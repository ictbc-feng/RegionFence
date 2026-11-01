// SPDX-License-Identifier: GPL-3.0-or-later

// rfreplay replays fetched mainnet blocks with geth's serial path and with
// the parallel executor (RegionFence, or the OCC and PCC baselines on the
// same engine), checks them against the chain and against each other, and
// reports timings.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"regionfence/engine"
)

func main() {
	var (
		dir       = flag.String("dir", "../data/state", "directory with <block>.json.gz fixtures")
		from      = flag.Uint64("from", 0, "first block (inclusive, 0 = all)")
		to        = flag.Uint64("to", 0, "last block (inclusive, 0 = all)")
		blockList = flag.String("blocks", "", "file with one block number per line: run only these (within -from/-to), in ascending order")
		threads   = flag.String("threads", "", "comma-separated worker counts for the parallel execution (empty = serial check only)")
		repeat    = flag.Int("repeat", 1, "timed repetitions per configuration (median reported)")
		out       = flag.String("out", "", "write per-window JSON lines here")
		positions = flag.String("positions", "", "write intra-transaction access positions per block here")
		window    = flag.Int("window", 1, "execute this many consecutive blocks as one window (later blocks read what earlier ones wrote; the engine admits them as the commit frontier advances)")
		cc        = flag.String("cc", "regionfence", "concurrency control: regionfence (learned fences at region boundaries) | occ (speculate, validate, re-execute) | pcc (wait at the first access for lower running transactions that touched the key)")
		gogc      = flag.Int("gogc", -1, "GC percent (debug.SetGCPercent; -1 = leave GOGC alone, 0 = off)")
		memLimit  = flag.Int64("memlimit", 0, "soft memory limit in MiB (debug.SetMemoryLimit; 0 = unchanged)")
		noSerial  = flag.Bool("noserial", false, "do not time the serial execution of each window (serial_ns = 0), so that with -repeat the process time beyond loading is the parallel phase alone")
		timeline  = flag.String("timeline", "", "write the execution timeline here")
		noCross   = flag.Bool("no-cross-block", false, "give every block a signature table of its own: nothing learned crosses a block")
		noWaits   = flag.Bool("no-learned-waits", false, "take no fence wait on what was learned")
	)
	flag.Parse()
	var baseline regionfence.Baseline // -cc occ, pcc: a block at a time, on their own executors
	switch *cc {
	case "regionfence":
		regionfence.Signatures = regionfence.NewSignatureTable()
	case "occ":
		baseline = regionfence.BaselineOCC
	case "pcc":
		baseline = regionfence.BaselinePCC
	default:
		fmt.Fprintln(os.Stderr, "-cc must be regionfence, occ or pcc")
		os.Exit(2)
	}
	if err := regionfence.Configure(regionfence.Options{Timeline: *timeline, NoCrossBlock: *noCross, NoLearnedWaits: *noWaits}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *gogc >= 0 {
		if *gogc == 0 {
			debug.SetGCPercent(-1)
		} else {
			debug.SetGCPercent(*gogc)
		}
	}
	if *memLimit > 0 {
		debug.SetMemoryLimit(*memLimit << 20)
	}

	files, err := listFixtures(*dir, *from, *to, *blockList)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	hashes := loadHashes(filepath.Join(*dir, "hashes.json"))
	lookup := func(n uint64) common.Hash { return hashes[n] }

	workers := parseInts(*threads)
	var sink *os.File
	if *out != "" {
		if sink, err = os.Create(*out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer sink.Close()
	}
	var ok, bad, txs, parBad int
	totalSerial := time.Duration(0)
	totalPar := make(map[int]time.Duration)
	type item struct {
		fx  *regionfence.Fixture
		env *regionfence.Env
		res *regionfence.Result
	}
	var batch []item
	flush := func(items []item) {
		if len(items) == 0 {
			return
		}
		fx := items[0].fx
		env := items[0].env
		envs := make([]*regionfence.Env, len(items))
		for i, it := range items {
			envs[i] = it.env
		}
		if len(items) > 1 {
			env = regionfence.NewWindowEnv(envs)
		}
		var lastRes *regionfence.Result
		row := map[string]any{"block": fx.Number, "window": len(items), "txs": len(env.Messages)}
		var txNs []int64
		serialTime := time.Duration(0)
		for _, it := range items {
			if *noSerial {
				break
			}
			var ns []int64
			serialTime += median(*repeat, func() time.Duration {
				start := time.Now()
				r, err := regionfence.ExecuteSerial(it.fx, it.env)
				if err != nil {
					panic(err)
				}
				ns = r.TxNs
				return time.Since(start)
			})
			txNs = append(txNs, ns...)
		}
		row["tx_ns"] = txNs
		totalSerial += serialTime
		row["serial_ns"] = serialTime.Nanoseconds()
		// Every run of this window starts from what was learned before the
		// window, whatever ran before it; the last run's state is what the
		// next window starts from.
		var learned *regionfence.Learned
		if *cc == "regionfence" {
			learned = regionfence.SaveLearned()
		}
		for _, p := range workers {
			var stats regionfence.Stats
			var failed bool
			byWall := make(map[time.Duration]regionfence.Stats, *repeat)
			walls := make([]int64, 0, *repeat)
			d := median(*repeat, func() time.Duration {
				var pres []*regionfence.Result
				var st regionfence.Stats
				var err error
				if learned == nil {
					pres, st, err = regionfence.ExecuteBaselineWindow(baseline, envs, p)
				} else {
					learned.Restore()
					pres, st, err = regionfence.Execute(env, p)
				}
				regionfence.FlushTimeline() // after the measurements
				if err != nil {
					fmt.Fprintf(os.Stderr, "block %d P=%d: %v\n", fx.Number, p, err)
					failed = true
					return 0
				}
				if lastRes != nil {
					lastRes.Release() // the previous repetition's window
				}
				lastRes = pres[0]
				differs := false
				for i, it := range items {
					if m := regionfence.CompareResults(it.fx, it.res, pres[i]); len(m) > 0 {
						report(fmt.Sprintf("block %d P=%d: %d differences from serial", it.fx.Number, p, len(m)), m)
						failed, differs = true, true
					}
				}
				if differs {
					// what the window committed on that the final state
					// does not confirm: the reads behind the difference
					stale := pres[0].StaleReads()
					fmt.Printf("window at block %d P=%d: %d committed reads are stale\n", fx.Number, p, len(stale))
					for i, line := range stale {
						if i >= 200 {
							break
						}
						fmt.Println("    STALE", line)
					}
				}
				stats = st
				byWall[st.Wall] = st
				walls = append(walls, st.Wall.Nanoseconds())
				return st.Wall
			})
			if measured, ok := byWall[d]; ok {
				stats = measured
			}
			if failed {
				parBad++
			}
			totalPar[p] += d
			key := func(name string) string { return fmt.Sprintf("p%d_%s", p, name) }
			row[key("ns")] = d.Nanoseconds()
			row[key("walls")] = walls // every repetition's, in run order
			row[key("aborts")] = stats.Aborts
			row[key("exec")] = stats.Executions
			row[key("block_ns")] = stats.BlockNs
			row[key("commit_ns")] = stats.CommitNs
			if *cc == "regionfence" {
				row[key("sig_lookups")] = stats.SignatureLookups
				row[key("sig_waits")] = stats.SignatureWaits
				row[key("sig_hits")] = stats.SignatureHits
			}
			if stats.WindowWidth != nil {
				row[key("window_width")] = stats.WindowWidth
			}
		}
		if lastRes != nil {
			lastRes.Release()
		}
		if sink != nil {
			blob, _ := json.Marshal(row)
			sink.Write(append(blob, '\n'))
		}
	}
	for _, path := range files {
		fx, err := regionfence.LoadFixture(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			bad++
			continue
		}
		hashes[fx.Number] = fx.Hash
		hashes[fx.Number-1] = fx.Header.ParentHash
		env, err := regionfence.NewEnv(fx, params.MainnetChainConfig, lookup)
		if err != nil {
			fmt.Fprintf(os.Stderr, "block %d: %v\n", fx.Number, err)
			bad++
			continue
		}
		res, err := regionfence.ExecuteSerial(fx, env)
		if err != nil {
			fmt.Fprintf(os.Stderr, "block %d: %v\n", fx.Number, err)
			bad++
			continue
		}
		mism := regionfence.CheckAgainstChain(fx, res)
		txs += len(fx.Txs)
		if len(mism) == 0 {
			ok++
		} else {
			bad++
			report(fmt.Sprintf("block %d: %d mismatches with the chain", fx.Number, len(mism)), mism)
			continue
		}
		if *positions != "" {
			pos, err := regionfence.RecordPositions(fx, env)
			if err != nil {
				fmt.Fprintf(os.Stderr, "block %d positions: %v\n", fx.Number, err)
			} else {
				os.MkdirAll(*positions, 0o755)
				blob, _ := json.Marshal(pos)
				os.WriteFile(filepath.Join(*positions, fmt.Sprintf("%d.json", fx.Number)), blob, 0o644)
			}
		}
		if len(workers) == 0 {
			continue
		}
		if len(batch) > 0 && (batch[len(batch)-1].fx.Number+1 != fx.Number || len(batch) >= *window) {
			flush(batch)
			batch = nil
		}
		batch = append(batch, item{fx, env, res})
	}
	flush(batch)
	fmt.Printf("serial replay: %d blocks match the chain, %d differ (%d txs)\n", ok, bad, txs)
	if len(workers) > 0 {
		fmt.Printf("parallel vs serial: %d block-configurations differ\n", parBad)
		base := totalPar[workers[0]]
		for _, p := range workers {
			fmt.Printf("  P=%-3d total %8.1f ms  speedup vs own P=%d %.2fx  vs geth serial %.2fx\n", p,
				float64(totalPar[p].Microseconds())/1000, workers[0],
				float64(base)/float64(totalPar[p]), float64(totalSerial)/float64(totalPar[p]))
		}
		fmt.Printf("  geth serial total %8.1f ms\n", float64(totalSerial.Microseconds())/1000)
	}
}

func report(title string, mism []regionfence.Mismatch) {
	fmt.Println(title)
	for i, m := range mism {
		if i >= 5 {
			break
		}
		fmt.Println("   ", m.What)
	}
}

func median(n int, f func() time.Duration) time.Duration {
	ds := make([]time.Duration, n)
	for i := range ds {
		ds[i] = f()
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	if n%2 == 0 {
		return (ds[n/2-1] + ds[n/2]) / 2 // not the larger of two
	}
	return ds[n/2]
}

func parseInts(s string) []int {
	var out []int
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			if v, err := strconv.Atoi(f); err == nil && v > 0 {
				out = append(out, v)
			}
		}
	}
	return out
}

// loadHashes reads the block hashes preceding each window.
func loadHashes(path string) map[uint64]common.Hash {
	out := make(map[uint64]common.Hash)
	blob, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	var raw map[string]common.Hash
	if err := json.Unmarshal(blob, &raw); err != nil {
		fmt.Fprintln(os.Stderr, "hashes.json:", err)
		return out
	}
	for k, v := range raw {
		if n, err := strconv.ParseUint(k, 10, 64); err == nil {
			out[n] = v
		}
	}
	return out
}

func listFixtures(dir string, from, to uint64, listFile string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var only map[uint64]bool
	if listFile != "" {
		data, err := os.ReadFile(listFile)
		if err != nil {
			return nil, err
		}
		only = map[uint64]bool{}
		for _, f := range strings.Fields(string(data)) {
			n, err := strconv.ParseUint(strings.TrimPrefix(f, "#"), 10, 64)
			if err == nil && !strings.HasPrefix(f, "#") {
				only[n] = true
			}
		}
	}
	type item struct {
		n    uint64
		path string
	}
	var items []item
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json.gz") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, ".json.gz"), 10, 64)
		if err != nil || (from != 0 && n < from) || (to != 0 && n > to) || (only != nil && !only[n]) {
			continue
		}
		items = append(items, item{n, filepath.Join(dir, name)})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].n < items[j].n })
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.path
	}
	return out, nil
}
