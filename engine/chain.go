// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"hash/maphash"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// The chains of hot keys run region by region.
//
// On a key that many transactions of a window read and change in turn (a
// pool's reserves, a token's balance of the pool), each hop of the chain would
// cost a whole transaction: the next one either waits for the writer's
// publication at its finish, or runs ahead on the old value and runs again
// from the start once the writer publishes. Region by region, a hop costs
// only the critical section: the reader waits at the start of its region (its
// first access to the key) and the writer publishes the key at the end of its
// region (its last write of the key), while both run the rest of their
// execution in parallel. Execution stays one transaction per goroutine: only
// the concurrency control works below the transaction.
//
// Every decision is taken from what the two layers of learning measure. A cost
// is a blend of the two things a delay can cost, by the measured busy share of
// the slots: while the processors are busy, the wall is the work they do, and
// processor time counts; while they are idle, the wall is the chain of
// commits, and delays to commits count. So the same learning holds back at few
// cores and pipelines at many, with nothing set per core count.
//
//   - The writer releases a hot key at its last write. Which write is the
//     last is learned per key from the writes of the incarnations that
//     finished (chainRates, kept across windows); the release pays when that
//     probability outweighs what a failed release costs its readers:
//     superseded by a later write, or its incarnation discarded before its
//     publication (costs.chain, from this machine's measured costs). Whether
//     the value may still move when a transaction below the writer runs again
//     is not weighed: the publication would carry the same value with the same
//     chance. The hand-off at the end of the writer's region is what a lock
//     released at the end of the region gives, without waiting for the
//     writer's commit.
//   - The reader waits at the start of its region for the writers the key
//     signatures name.
//   - A re-execution waits where the incarnation before it failed: on a key
//     that invalidated it, also for the lower transactions that have read
//     the key and not finished (the in-block evidence, not the features).
//   - A reader waiting on a writer that is running keeps polling on its
//     processor while idle slots outnumber the pollers, so the hop does not
//     pay a wake-up on an idle core. With fewer idle slots it sleeps.

// chainWrites bounds the writes of one key in one incarnation that the
// sketch tells apart: from the last bucket on, a write is never taken for the
// last one (memory, not policy: one counter per bucket).
const chainWrites = 8

// chainRates is, by storage key (a sketch, kept across windows), how many
// times the incarnations that finished wrote the key: bucket i counts those
// that wrote it i+1 times (the last bucket, that many or more).
var chainRates chainRate

const chainRateSlots = 1 << 14

var chainSeed = maphash.MakeSeed()

type chainRate struct {
	n [chainRateSlots][chainWrites]atomic.Uint32
	g [chainWrites]atomic.Uint32 // the population: incarnations of any hot key by its writes, a key's prior
}

func (c *chainRate) slot(k Key) uint64 { return maphash.Comparable(chainSeed, k) % chainRateSlots }

// add records an incarnation that wrote k n times.
func (c *chainRate) add(k Key, n int) {
	if n <= 0 {
		return
	}
	b := &c.n[c.slot(k)]
	var sum uint32
	for i := range b {
		sum += b[i].Load()
	}
	if sum >= 64 { // forget: halve them all (races only blur the counts)
		for i := range b {
			b[i].Store(b[i].Load() / 2)
		}
	}
	b[min(n, chainWrites)-1].Add(1)
	var gsum uint32
	for i := range c.g {
		gsum += c.g[i].Load()
	}
	if gsum >= 1<<12 {
		for i := range c.g {
			c.g[i].Store(c.g[i].Load() / 2)
		}
	}
	c.g[min(n, chainWrites)-1].Add(1)
}

// last is the probability that the n-th write of k in an incarnation is its
// last one: of the incarnations that wrote k at least n times, the share that
// wrote it exactly n times. A key's prior, worth two incarnations, is the
// same share over the population of hot keys (g), so that a key seen little
// starts where hot keys usually are, not at one half.
func (c *chainRate) last(k Key, n int) float64 {
	if n >= chainWrites {
		return 0
	}
	share := func(b *[chainWrites]atomic.Uint32) (at, beyond float64) {
		at = float64(b[n-1].Load())
		for i := n - 1; i < chainWrites; i++ {
			beyond += float64(b[i].Load())
		}
		return
	}
	gat, gbeyond := share(&c.g)
	prior := (gat + 1) / (gbeyond + 2)
	at, beyond := share(&c.n[c.slot(k)])
	return (at + 2*prior) / (beyond + 2)
}

// chainState is a copy of the per-key write counts (chainRates), for a
// harness that runs a window several times from the same state, as it does
// with the signature table.
type chainState struct {
	n [chainRateSlots][chainWrites]uint32
	g [chainWrites]uint32
	// the learned release threshold and re-execution cost: a repeat of the
	// window starts from them as they were, not from the last repeat's
	threshold uint64
	reexec    int64
}

// saveChain copies chainRates.
func saveChain() *chainState {
	t := new(chainState)
	for i := range t.n {
		for w := range t.n[i] {
			t.n[i][w] = chainRates.n[i][w].Load()
		}
	}
	for w := range t.g {
		t.g[w] = chainRates.g[w].Load()
	}
	t.threshold, t.reexec = costs.chain.Load(), chainCost.reexec.Load()
	return t
}

// Restore puts the copy back.
func (t *chainState) Restore() {
	if t == nil {
		return
	}
	for i := range t.n {
		for w := range t.n[i] {
			chainRates.n[i][w].Store(t.n[i][w])
		}
	}
	for w := range t.g {
		chainRates.g[w].Store(t.g[w])
	}
	costs.chain.Store(t.threshold)
	chainCost.reexec.Store(t.reexec)
}

// chainCosts are the measured costs the chain's decisions weigh, folded in at
// the end of each window (process-wide, an exponential average over windows).
type chainCosts struct {
	reexec atomic.Int64 // ns of a re-execution, per abort: the run of an incarnation
}

var chainCost chainCosts

func foldNs(v *atomic.Int64, x float64) {
	if x <= 0 {
		return
	}
	if old := v.Load(); old > 0 {
		x = 0.75*float64(old) + 0.25*x
	}
	v.Store(max(int64(x), 1))
}

// learnChain folds a window's measured costs into the chain's decisions; u is
// its idle share.
func learnChain(st *Stats, u float64) {
	if st.Aborts < 8 || st.ReexecNs <= 0 {
		return
	}
	reexec := float64(st.ReexecNs) / float64(st.Aborts)
	foldNs(&chainCost.reexec, reexec)
	// The release: q is that this write of the key is its last. Not releasing
	// when it is delays the readers of the key by the time from the release
	// to the writer's publication (per kept release, times the readers it
	// served: those it woke and those that read it while the writer ran on),
	// which costs only while the processors are idle (B). A release fails
	// when it is superseded, or when its incarnation is discarded before its
	// publication (went: the share that was not, which neither the write
	// counts nor pfin see); its readers then run again, a re-execution each
	// (A): processor time while the processors are busy, a delay to a commit
	// while they are idle. Without the release those readers would have
	// waited for the writer's next incarnation instead. The release itself
	// costs the writer c, paid whatever the outcome. Releasing pays when
	// q B > c + (1 - q) A, that is q >= (A + c) / (A + B), scaled by went.
	if st.ReleaseKept >= 8 && st.ReleaseGainNs > 0 && st.Releases > 0 {
		failed := st.ReleaseSuperseded + st.ReleaseAborted
		victims := float64(st.ReleaseVictims+st.ReleaseAbortVictims) / float64(max(failed, 1))
		served := float64(st.WaitByRelease+st.ReleaseReads) / float64(st.Releases)
		went := 1 - float64(st.ReleaseAborted)/float64(st.Releases)
		a, b := victims*reexec, u*served*float64(st.ReleaseGainNs)/float64(st.ReleaseKept)
		c := float64(st.ReleaseNs) / float64(st.Releases)
		if b <= c {
			foldScaled(&costs.chain, 1, 1, 0.5) // no outcome pays: as never as the sketches allow
		} else {
			foldScaled(&costs.chain, a+c, b-c, went)
		}
	}
}

// chainReleaseThreshold is the learned probability of being the last write
// from which a hot key is released.
func chainReleaseThreshold() float64 { return min(loadThreshold(&costs.chain), sketchSure) }

// noteFailed remembers that storage key k invalidated an incarnation of r
// (txMu[r] held): r's later incarnations wait there for the lower
// transactions that have read it and not finished.
func (ex *Executor) noteFailed(r int32, k Key) {
	if ex.failKeys == nil || k.Kind != kindStorage {
		return
	}
	if !containsKey(ex.failKeys[r], k) {
		ex.failKeys[r] = append(ex.failKeys[r], k)
	}
}

// noteRisky remembers, when an incarnation of r is discarded or invalidated
// after it ran (txMu[r] held), every storage key it read from a version of a
// transaction that has not committed: the reads that can move under it again.
// A retry sets its fences from the previous incarnation's exact reads, since
// an invalidation often hits an incarnation that is itself a re-execution.
func (ex *Executor) noteRisky(r int32, run *incarnation) {
	if ex.failKeys == nil || run == nil {
		return
	}
	front := ex.committed.Load()
	for k, o := range run.reads {
		if k.Kind == kindStorage && o.tx >= front && o.inc != boundaryInc && !containsKey(ex.failKeys[r], k) {
			ex.failKeys[r] = append(ex.failKeys[r], k)
		}
	}
}

// failedTouchers adds to cands the lower transactions that have read k in
// this window and not finished, named by the read-modify-write rule's learned
// probability (a reader of a contended key mostly changes it): k invalidated
// an earlier incarnation of j, or that incarnation read it from a version
// that may move, so they are the writers the in-block evidence points at.
func (ex *Executor) failedTouchers(j int32, k Key, cands []writerCandidate, minP float64) []writerCandidate {
	num, den := 1.0, 2.0 // even prior without a table
	if ex.signatures != nil {
		t := ex.tableFor(j)
		num = 2 * t.rmwPrior() // the population's rate while the key is not in the table
		if n, d, ok := t.rmwProb(k); ok {
			num, den = n, d
		}
	}
	if num < minP*den {
		return cands // below the threshold, as writerCandidates drops the features' candidates
	}
	front := ex.committed.Load()
	for _, i := range ex.lowerTouchers(j, k) {
		if i < front || ex.finished[i].Load() {
			continue
		}
		dup := false
		for x := range cands {
			if cands[x].tx == i {
				if num*cands[x].den > cands[x].num*den {
					cands[x].num, cands[x].den = num, den
				}
				dup = true
				break
			}
		}
		if !dup {
			cands = append(cands, writerCandidate{tx: i, fi: -1, num: num, den: den})
		}
	}
	// nearest first, as writerCandidates orders them
	for a := 1; a < len(cands); a++ {
		for b := a; b > 0 && cands[b].tx > cands[b-1].tx; b-- {
			cands[b], cands[b-1] = cands[b-1], cands[b]
		}
	}
	return cands
}

// chainIdle says whether a poller may keep its processor: free slots
// outnumber the pollers (itself included in n) and the busy slots and the
// pollers leave a processor.
func (ex *Executor) chainIdle(n int32) bool {
	free := ex.scheduler.freeA.Load()
	return free >= n && int32(ex.scheduler.workers)-free+n <= ex.procs
}

// spinChain polls ch, the publication of the running writer j waits for, on
// j's processor while the slots it would leave idle are idle anyway; it
// reports whether ch closed meanwhile (j then holds its slot as if it had
// never blocked). It stops when the writer blocks itself (the wait will be
// long) or when the idle slots run out, and the caller sleeps as before.
func (ex *Executor) spinChain(j int32, ch <-chan struct{}, why int, t0 time.Time) bool {
	w := ex.waitOn[j].Load() - 1
	if w < 0 || ex.waitOn[w].Load() != 0 || ex.status[w].get() != statusRunning {
		return false
	}
	if !ex.chainIdle(spinners.Load() + 1) {
		return false
	}
	n := spinners.Add(1)
	if !ex.chainIdle(n) {
		spinners.Add(-1)
		return false
	}
	defer spinners.Add(-1)
	for i := 0; ; i++ {
		select {
		case <-ch:
			t1 := time.Now()
			ex.blocked[j].Add(t1.Sub(t0).Nanoseconds())
			if timelineFile != nil {
				ex.timeline(j, "B %d %d %d %d %d %d %d\n", ex.blockOf(j).Fixture.Number, j, why, t0.Sub(ex.start).Nanoseconds(), t1.Sub(ex.start).Nanoseconds(), t1.Sub(ex.start).Nanoseconds(), w)
			}
			ex.checkStale(j)
			return true
		default:
		}
		if i&63 == 63 && (!ex.chainIdle(spinners.Load()) || ex.waitOn[w].Load() != 0 || ex.status[w].get() != statusRunning || ex.stale[j].Load()) {
			// stop: unless the writer has just published (it leaves running
			// before its publication wakes the waiters), sleep as before
			select {
			case <-ch:
				i = -1 // take it on the next turn of the loop
				continue
			default:
			}
			return false
		}
	}
}

// releaseChain releases hot key k with value val, written last with at
// least the learned threshold's probability, unless the cost model's probes
// are spent.
func (r *versionReader) releaseChain(k Key, val common.Hash) {
	if r.ex.probe(probeChain, &costs.chain) {
		r.release(k, Value{Slot: val})
	}
}

// flushHeld releases the held hot keys once a reader waits for this writer
// (chainWaiters): it is called at the writer's state accesses.
func (r *versionReader) flushHeld() {
	if r.ex.chainWaiters[r.j].Load() == 0 {
		return
	}
	for k, v := range r.held {
		delete(r.held, k)
		r.releaseChain(k, v)
	}
}

// countReleaseVictims counts the readers of the releases of j's incarnation
// run, which is being discarded before its publication: they run again with
// it (a release failing this way costs what a superseded one does, learnChain).
func (ex *Executor) countReleaseVictims(j int32, run *incarnation) {
	n := 0
	for k := range run.released {
		if e := ex.store.get(k); e != nil {
			e.mu.Lock()
			n += e.countReadersOf(j)
			e.mu.Unlock()
		}
	}
	ex.add(j, counterReleaseAbortVictims, int64(n))
}

// chainWrite counts a write of a hot key and releases the key when this write
// is learned to be its last one in the transaction: the end of the writer's
// region on the key. A write that puts back the value the transaction started
// with is not released (its readers have that value already). The release goes
// out only while a reader waits for this writer; otherwise the value is held
// and goes out at the writer's next state access after a reader starts waiting
// (flushHeld), or not at all.
func (s *fenceStateDB) chainWrite(ref slotRef, k Key, value common.Hash) {
	r := s.r
	if r.ex.signatures == nil || !r.ex.tableFor(r.j).maybe(k) {
		return
	}
	if s.hotWrites == nil {
		s.hotWrites = make(map[slotRef]uint8)
	}
	n := s.hotWrites[ref]
	if n < 255 {
		n++
	}
	s.hotWrites[ref] = n
	if len(r.held) > 0 {
		delete(r.held, k) // written again: what was held was not the last write
		r.flushHeld()
	}
	if value == s.StateDB.GetCommittedState(ref.addr, ref.slot) {
		return
	}
	if chainRates.last(k, int(n)) < chainReleaseThreshold() {
		return
	}
	if r.ex.chainWaiters[r.j].Load() == 0 {
		if r.held == nil {
			r.held = make(map[Key]common.Hash, 2)
		}
		r.held[k] = value // nobody waits for this writer yet
		return
	}
	r.releaseChain(k, value)
}
