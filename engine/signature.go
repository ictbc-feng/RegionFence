// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"encoding/binary"
	"math"
	"math/bits"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
)

// Key signatures predict, at the moment a fence needs it, the writers of a
// key: a transaction is about to read a key known to be contended; which of
// the lower transactions that have not finished will change it?
//
// The reader's side of an edge is then exact (it is reading the key now), and
// only the writers are predicted. A key remembers the features its writers
// carried — recipient, selector, the addresses in the calldata and the
// access list, and two conjunctions — and, per feature, how many finished
// transactions carrying it changed the key and how many did not. The swaps on
// one pool share the token address in their calldata whatever router they
// came through, which a (contract, selector) pair cannot express.
//
// There are no admission rules and no thresholds on a feature's count. A
// (key, feature) pair is a Bernoulli trial per finished carrier, and what the
// fence uses is its probability: the posterior mean of a Beta whose prior is
// learned at two levels. A feature's own record on the other keys that
// remember it comes first: a token whose carriers changed every pool slot
// seen so far starts a new slot near certainty, a router that did not starts
// it near zero. Behind that stands the record of the feature's kind and of
// how common it is in the window, taken from the first trials of all such
// pairs. A reader waits for an unfinished lower carrier while that
// probability is at least waitThreshold, the one number left: what a wait in vain
// costs against a missed dependency. Each wait that turns out to be in vain
// counts against the feature at once, so a reader stops by itself; and a
// feature whose carriers stop changing the key loses its record as soon as
// the run of misses is one its record makes improbable.
//
// Nothing is planned ahead, so nothing arrives late. A key enters the table
// when an invalidation shows it is contended, and that knowledge is usable by
// the next reader, in this block or any later one.

// feature is a hashed feature of a transaction, known before it runs. Its top
// three bits are its kind.
type feature uint64

const (
	featureTo uint64 = iota
	featureSelector
	featureArg
	featureFromTo
	featureArgTo
	featureKinds
)

// maxFeatureArgs bounds the calldata addresses of one kind (aligned words,
// packed, access list) that become features of one transaction.
const maxFeatureArgs = 8

func mixFeature(h, kind uint64) feature {
	h ^= (kind + 1) * 0x9E3779B97F4A7C15
	h ^= h >> 30
	h *= 0xBF58476D1CE4E5B9
	h ^= h >> 27
	h *= 0x94D049BB133111EB
	h ^= h >> 31
	return feature(h>>3 | kind<<61)
}

func (f feature) kind() int { return int(f >> 61) }

func addressBits(a []byte) uint64 {
	return binary.LittleEndian.Uint64(a[0:8]) ^ bits.RotateLeft64(binary.LittleEndian.Uint64(a[8:16]), 23) ^ uint64(binary.LittleEndian.Uint32(a[16:20]))*0xD6E8FEB86659FD93
}

// txFeatures lists the features of a message. known, if not nil, is the set of
// addresses seen to own changed storage: an address of it anywhere in the
// calldata is a feature, not only one in an aligned 32-byte word (a swap
// path packs its tokens).
func txFeatures(msg *core.Message, known *addressSet) []feature {
	out := make([]feature, 0, 12)
	from := addressBits(msg.From[:])
	var to uint64
	if msg.To != nil {
		to = addressBits(msg.To[:])
		out = append(out, mixFeature(to, featureTo), mixFeature(bits.RotateLeft64(from, 17)^to, featureFromTo))
		if len(msg.Data) >= 4 {
			out = append(out, mixFeature(to^uint64(binary.BigEndian.Uint32(msg.Data))<<32, featureSelector))
		}
	}
	var seen [3 * maxFeatureArgs]uint64
	args := 0
	add := func(a uint64, limit int, pair bool) {
		for _, s := range seen[:args] {
			if s == a {
				return
			}
		}
		if args >= limit {
			return
		}
		seen[args] = a
		args++
		out = append(out, mixFeature(a, featureArg))
		if pair && msg.To != nil {
			out = append(out, mixFeature(bits.RotateLeft64(a, 17)^to, featureArgTo))
		}
	}
	for o := 4; o+32 <= len(msg.Data); o += 32 {
		if w := msg.Data[o : o+32]; isAddressWord(common.Hash(w)) {
			add(addressBits(w[12:]), maxFeatureArgs, true)
		}
	}
	for i := range msg.AccessList {
		add(addressBits(msg.AccessList[i].Address[:]), 2*maxFeatureArgs, false)
	}
	if known != nil {
		for o := 0; o+20 <= len(msg.Data); o++ {
			if a := addressBits(msg.Data[o : o+20]); known.has(a) {
				add(a, 3*maxFeatureArgs, false)
			}
		}
	}
	return out
}

// addressSet is a Bloom filter of addresses (three probes into 2^21 bits: with
// tens of thousands of addresses a random 20-byte window passes about once in
// 40000, so a kilobyte of calldata yields a spurious feature every forty
// transactions).
type addressSet [1 << 15]atomic.Uint64

func (s *addressSet) probes(a uint64) [3]uint64 {
	h := uint64(mixFeature(a, 7))
	return [3]uint64{h, h >> 21, h >> 42}
}

func (s *addressSet) add(a uint64) {
	for _, x := range s.probes(a) {
		w, bit := &s[(x>>6)%uint64(len(s))], uint64(1)<<(x&63)
		if w.Load()&bit == 0 {
			w.Or(bit)
		}
	}
}

func (s *addressSet) has(a uint64) bool {
	for _, x := range s.probes(a) {
		if s[(x>>6)%uint64(len(s))].Load()&(1<<(x&63)) == 0 {
			return false
		}
	}
	return true
}

const (
	signatureShards     = 64
	signatureBloomWords = 1 << 12 // 2^18 bits
	signatureSlots      = 24      // features a key remembers: memory, not policy (see credit)
	recentWriters       = 8       // writers remembered per key, so that each is counted once
	signatureHalfLife   = 256     // trials of a pair after which its evidence is halved
	candidateScan       = 16      // candidates gathered for one read (the fence stops long before by itself)
	rarityBuckets       = 9       // rarity classes of a feature: bits.Len of its carriers in the window, capped
	priorTrials         = 4       // first trials of a pair that teach its kind's prior
	priorStrength       = 2.0     // weight of a prior, in trials
)

// waitThreshold is the probability from which a reader waits for a named writer:
// a decision of the cost model (cost.go, learnWait). Waiting for a writer
// that then does not change the key costs the reader the wait in vain (A),
// which costs only while the processors are idle; not waiting for one that
// does costs the reader a re-execution (B). It waits when q >= A / (A + B),
// one half until a window has measured both.
func waitThreshold() float64 {
	return min(loadThreshold(&costs.wait), sketchSure)
}

// learnWait folds a window's measured costs into waitThreshold; u is its idle
// share.
func learnWait(st *Stats, u float64) {
	if st.Aborts < 8 || st.ReexecNs <= 0 || st.VainWaits < 8 || st.VainWaitNs <= 0 {
		return
	}
	reexec := float64(st.ReexecNs) / float64(st.Aborts)
	vain := float64(st.VainWaitNs) / float64(st.VainWaits)
	foldScaled(&costs.wait, u*vain, reexec, 1)
}

// changeTestAlpha is the significance level of the change test on a pair (prob).
const changeTestAlpha = 0.05

// Signatures is the key-signature table the executors learn into and fence by (nil:
// off). It outlives windows: that is the cross-block learning.
var Signatures *SignatureTable

// SignatureTable maps contended keys to the features of their writers.
type SignatureTable struct {
	bloom        [signatureBloomWords]atomic.Uint64 // contended keys
	featureBloom [signatureBloomWords]atomic.Uint64 // tracked features
	known        addressSet                         // addresses seen to own changed storage
	shards       [signatureShards]signatureShard
	carriers     [signatureShards]carrierShard
	runs         atomic.Int64 // executors that used the table (writer ids)

	groupsMu sync.Mutex
	groups   [featureKinds * rarityBuckets][2]float64 // per kind and rarity: first-trial hits and misses of its pairs (groupsMu)
	// groupMean is each group's smoothed hit rate, (hits+1)/(hits+misses+2), as
	// float bits (0: no trial yet, 1/2). Every lookup of a key's writers reads
	// it; it changes only when a feature completes its first trials, which
	// takes groupsMu, so that a lookup does not.
	groupMean [featureKinds * rarityBuckets]atomic.Uint64

	// The read-modify-write rule's population: of the finished readers of
	// any contended key, how many changed it (halved at 1<<12 trials). A
	// key's own record starts from this rate, not from one half.
	rmwHitsG, rmwSeenG atomic.Int64
}

// rmwPrior is the population's read-modify-write rate (rmwHitsG), with a
// uniform prior of its own.
func (t *SignatureTable) rmwPrior() float64 {
	return (float64(t.rmwHitsG.Load()) + 1) / (float64(t.rmwSeenG.Load()) + 2)
}

type signatureShard struct {
	mu sync.Mutex
	m  map[Key]*keySignature
	_  [40]byte
}

type carrierShard struct {
	mu sync.RWMutex
	m  map[feature]*carrier
	_  [32]byte
}

// carrier is what the table knows of a feature across keys.
type carrier struct {
	n       atomic.Uint64 // finished transactions that carried it, since it was tracked
	hits    atomic.Int64  // of the pairs it is part of: carriers that changed the pair's key after it was admitted
	pairs   atomic.Int64  // keys remembering it
	sumBase atomic.Int64  // sum over those pairs of n at admission, less one (their trials are pairs*(n-1) - sumBase)
}

type keySignature struct {
	features []signatureFeature
	// Of the finished transactions that read the key, how many changed it
	// (the read-modify-write rule).
	rmwHits, rmwSeen float64
	recent           [recentWriters]int64
	next             uint8
}

type signatureFeature struct {
	f         feature
	hits      float64 // carriers that changed the key, the one that brought the feature included
	base      float64 // the feature's carriers when it was admitted, less one
	last      float64 // the feature's carriers at the last one that changed the key
	c         *carrier
	group     uint8 // kind and rarity when admitted
	graduated bool  // its first trials have been added to its group
}

// NewSignatureTable returns an empty table.
func NewSignatureTable() *SignatureTable {
	t := new(SignatureTable)
	for i := range t.shards {
		t.shards[i].m = make(map[Key]*keySignature)
		t.carriers[i].m = make(map[feature]*carrier)
	}
	return t
}

// Clone returns an independent copy (measurement runs that must start from
// the same knowledge).
func (t *SignatureTable) Clone() *SignatureTable {
	c := NewSignatureTable()
	counters := make(map[*carrier]*carrier)
	for i := range t.carriers {
		sh := &t.carriers[i]
		sh.mu.RLock()
		for f, n := range sh.m {
			cn := new(carrier)
			cn.n.Store(n.n.Load())
			cn.hits.Store(n.hits.Load())
			cn.pairs.Store(n.pairs.Load())
			cn.sumBase.Store(n.sumBase.Load())
			c.carriers[i].m[f] = cn
			counters[n] = cn
		}
		sh.mu.RUnlock()
	}
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.Lock()
		for k, ks := range sh.m {
			cp := *ks
			cp.features = append([]signatureFeature(nil), ks.features...)
			for j := range cp.features {
				cp.features[j].c = counters[cp.features[j].c]
			}
			c.shards[i].m[k] = &cp
		}
		sh.mu.Unlock()
	}
	for i := range t.bloom {
		c.bloom[i].Store(t.bloom[i].Load())
		c.featureBloom[i].Store(t.featureBloom[i].Load())
	}
	for i := range t.known {
		c.known[i].Store(t.known[i].Load())
	}
	t.groupsMu.Lock()
	c.groups = t.groups
	for i := range t.groupMean {
		c.groupMean[i].Store(t.groupMean[i].Load())
	}
	t.groupsMu.Unlock()
	c.runs.Store(t.runs.Load())
	c.rmwHitsG.Store(t.rmwHitsG.Load())
	c.rmwSeenG.Store(t.rmwSeenG.Load())
	return c
}

func keyBits(k Key) uint64 {
	return binary.LittleEndian.Uint64(k.Slot[24:32]) ^ bits.RotateLeft64(binary.LittleEndian.Uint64(k.Slot[0:8]), 29) ^ addressBits(k.Addr[:])
}

func bloomSet(b *[signatureBloomWords]atomic.Uint64, h uint64) {
	for _, x := range [2]uint64{h, h >> 32} {
		w, bit := &b[(x>>6)%signatureBloomWords], uint64(1)<<(x&63)
		if w.Load()&bit == 0 {
			w.Or(bit)
		}
	}
}

func bloomHas(b *[signatureBloomWords]atomic.Uint64, h uint64) bool {
	return b[(h>>6)%signatureBloomWords].Load()&(1<<(h&63)) != 0 && b[(h>>38)%signatureBloomWords].Load()&(1<<((h>>32)&63)) != 0
}

// maybe reports whether k may be in the table (no lock, no false negative).
func (t *SignatureTable) maybe(k Key) bool {
	return bloomHas(&t.bloom, uint64(mixFeature(keyBits(k), 6)))
}

func (t *SignatureTable) shard(k Key) *signatureShard { return &t.shards[keyBits(k)%signatureShards] }

// carrier returns the counters of f, tracking f from now on if it was not;
// fresh reports that.
func (t *SignatureTable) carrier(f feature) (c *carrier, fresh bool) {
	sh := &t.carriers[uint64(f)%signatureShards]
	sh.mu.RLock()
	c = sh.m[f]
	sh.mu.RUnlock()
	if c != nil {
		return c, false
	}
	sh.mu.Lock()
	if c = sh.m[f]; c == nil {
		c, fresh = new(carrier), true
		sh.m[f] = c
		bloomSet(&t.featureBloom, uint64(f))
	}
	sh.mu.Unlock()
	return c, fresh
}

// carried counts one finished transaction with features fs.
func (t *SignatureTable) carried(fs []feature) {
	for _, f := range fs {
		if !bloomHas(&t.featureBloom, uint64(f)) {
			continue
		}
		sh := &t.carriers[uint64(f)%signatureShards]
		sh.mu.RLock()
		c := sh.m[f]
		sh.mu.RUnlock()
		if c != nil {
			c.n.Add(1)
		}
	}
}

// prob returns the probability that the next carrier of s's feature changes
// s's key, as numerator and denominator: the caller counts the waits it has
// just made in vain by adding them to the denominator. The shard of the key
// is held.
func (t *SignatureTable) prob(s *signatureFeature) (num, den float64) {
	n := float64(s.c.n.Load())
	hits := s.hits - 1 // the transaction that brought the feature selected it: not a trial
	seen := max(n-s.base, s.hits) - 1
	if gap := n - s.last; hits >= 1 && gap >= 1 && seen > gap {
		// Has the pair changed? A run of carriers that did not change the
		// key, which its record before the run makes less likely than
		// changeTestAlpha, is taken as a change, and that record is dropped. A
		// feature that was nearly certain is distrusted after two misses; a
		// doubtful one keeps the benefit of the doubt longer.
		if rate := min(hits/(seen-gap+1), 0.999); math.Pow(1-rate, gap) < changeTestAlpha {
			shift := n - 1 - gap - s.base
			s.c.hits.Add(-int64(hits))
			s.c.sumBase.Add(int64(shift))
			s.hits, s.base = 1, s.base+shift
			hits, seen = 0, gap
		}
	}
	if !s.graduated && seen >= priorTrials {
		// its first trials teach its kind what such a feature is worth, once
		// (s.graduated is the key's: its shard lock is held)
		s.graduated = true
		h := hits * priorTrials / seen
		t.groupsMu.Lock()
		g := &t.groups[s.group]
		g[0] += h
		g[1] += priorTrials - h
		t.groupMean[s.group].Store(math.Float64bits((g[0] + 1) / (g[0] + g[1] + 2)))
		t.groupsMu.Unlock()
	}
	m := 0.5
	if b := t.groupMean[s.group].Load(); b != 0 {
		m = math.Float64frombits(b)
	}
	// the feature's record on its other keys, drawn towards its kind's
	fh := float64(s.c.hits.Load())
	fn := max(float64(s.c.pairs.Load())*(n-1)-float64(s.c.sumBase.Load()), fh)
	m = (max(fh-hits, 0) + priorStrength*m) / (max(fn-seen, 0) + priorStrength)
	return hits + priorStrength*m, seen + priorStrength
}

// credit records that the transaction with id wid, features fs (of rarity
// classes rar) changed k. With insert the key enters the table if it is not
// there (it was just seen to be contended); without, only keys already in
// the table count.
func (t *SignatureTable) credit(k Key, wid int64, fs []feature, rar []uint8, insert bool) {
	t.creditAhead(k, wid, fs, rar, insert, 0)
}

// creditAhead is credit by a writer that is not yet counted among its
// features' carriers (ahead = 1): learnSignatures counts it after its credits, so
// that a reader never sees it as a carrier that did not change the key. Seen
// between the two, the carrier looked like a miss, and a feature every
// carrier had used to change the key lost its record (prob): on a counter
// every transaction increments, readers stopped waiting for its writers.
func (t *SignatureTable) creditAhead(k Key, wid int64, fs []feature, rar []uint8, insert bool, ahead float64) {
	if !insert && !t.maybe(k) {
		return
	}
	sh := t.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	ks := sh.m[k]
	if ks == nil {
		if !insert {
			return
		}
		ks = new(keySignature)
		for i := range ks.recent {
			ks.recent[i] = -1
		}
		sh.m[k] = ks
		bloomSet(&t.bloom, uint64(mixFeature(keyBits(k), 6)))
	}
	for _, r := range ks.recent {
		if r == wid {
			// this writer was counted (an invalidation credited it before
			// it finished); it is the last carrier that changed the key
			if ahead > 0 {
				for _, f := range fs {
					for x := range ks.features {
						if s := &ks.features[x]; s.f == f {
							s.last = float64(s.c.n.Load()) + ahead
						}
					}
				}
			}
			return
		}
	}
	ks.recent[ks.next] = wid
	ks.next = (ks.next + 1) % recentWriters
features:
	for i, f := range fs {
		for x := range ks.features {
			if s := &ks.features[x]; s.f == f {
				s.hits++
				s.c.hits.Add(1)
				n := float64(s.c.n.Load()) + ahead
				s.last = n
				if seen := max(n-s.base, s.hits) - 1; seen >= signatureHalfLife {
					// old evidence counts half: the ratio stays, a change shows sooner
					drop, shift := (s.hits-1)/2, seen/2
					s.hits -= drop
					s.base += shift
					s.c.hits.Add(-int64(drop))
					s.c.sumBase.Add(int64(shift))
				}
				continue features
			}
		}
		if len(ks.features) >= signatureSlots {
			// Memory, not policy: a full signature keeps the features most
			// writers carried. The oldest feature seen on one writer only
			// makes room; if every feature has been seen on two, the
			// newcomer is dropped.
			victim := -1
			for x := range ks.features {
				if ks.features[x].hits < 2 {
					victim = x
					break
				}
			}
			if victim < 0 {
				continue
			}
			v := &ks.features[victim]
			v.c.pairs.Add(-1)
			v.c.sumBase.Add(-int64(v.base))
			ks.features = append(ks.features[:victim], ks.features[victim+1:]...)
		}
		c, fresh := t.carrier(f)
		if fresh {
			c.n.Store(uint64(1 - ahead)) // this writer, once counted
		}
		base := float64(c.n.Load()) + ahead - 1
		c.pairs.Add(1)
		c.sumBase.Add(int64(base))
		ks.features = append(ks.features, signatureFeature{f: f, hits: 1, base: base, last: base + 1, c: c, group: uint8(f.kind()*rarityBuckets + int(min(rar[i], rarityBuckets-1)))})
	}
}

// namingFeature is a feature that names the writers of a key with probability
// num/den.
type namingFeature struct {
	f        feature
	num, den float64
}

// readBy counts a finished transaction that read k, and whether it changed it.
func (t *SignatureTable) readBy(k Key, changed bool) {
	sh := t.shard(k)
	sh.mu.Lock()
	if ks := sh.m[k]; ks != nil {
		ks.rmwSeen++
		if changed {
			ks.rmwHits++
			t.rmwHitsG.Add(1)
		}
		if ks.rmwSeen >= signatureHalfLife {
			ks.rmwSeen, ks.rmwHits = ks.rmwSeen/2, ks.rmwHits/2
		}
		if v := t.rmwSeenG.Add(1); v >= 1<<12 && t.rmwSeenG.CompareAndSwap(v, v/2) {
			// forget: the goroutine that halved the trials halves the hits, once
			for {
				h := t.rmwHitsG.Load()
				if t.rmwHitsG.CompareAndSwap(h, h/2) {
					break
				}
			}
		}
	}
	sh.mu.Unlock()
}

// rmwProb is the probability that a transaction that has read k goes on to
// change it (the read-modify-write rule's posterior), and whether k is known.
func (t *SignatureTable) rmwProb(k Key) (num, den float64, known bool) {
	sh := t.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if ks := sh.m[k]; ks != nil {
		return ks.rmwHits + priorStrength*t.rmwPrior(), ks.rmwSeen + priorStrength, true
	}
	return 0, 0, false
}

// names appends the features that name k's writers with probability at least
// minP to out; rmw is the same for "a transaction that has read k goes on to
// change it" (zero numerator if below).
func (t *SignatureTable) names(k Key, out []namingFeature, minP float64) (_ []namingFeature, rmw namingFeature, known bool) {
	sh := t.shard(k)
	sh.mu.Lock()
	if ks := sh.m[k]; ks != nil {
		known = true
		for i := range ks.features {
			if num, den := t.prob(&ks.features[i]); num >= minP*den {
				out = append(out, namingFeature{ks.features[i].f, num, den})
			}
		}
		if num, den := ks.rmwHits+priorStrength*t.rmwPrior(), ks.rmwSeen+priorStrength; num >= minP*den && ks.rmwSeen >= 1 {
			rmw = namingFeature{num: num, den: den}
		}
	}
	sh.mu.Unlock()
	return out, rmw, known
}

// buildFeatureIndex computes the features of every message of the window, per
// feature the ascending list of transactions carrying it, and per
// transaction the rarity class of each of its features (bits.Len of the
// number of carriers in the window, capped).
func buildFeatureIndex(msgs []*core.Message, known *addressSet) ([][]feature, map[feature][]int32, [][]uint8) {
	n := len(msgs)
	features := make([][]feature, n)
	workers := max(1, min(8, runtime.GOMAXPROCS(0), n/256))
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := w; j < n; j += workers {
				features[j] = txFeatures(msgs[j], known)
			}
		}(w)
	}
	wg.Wait()
	// one map per goroutine, each taking the features that hash to it
	parts := make([]map[feature][]int32, workers)
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			m := make(map[feature][]int32, n*4/workers)
			for j, fs := range features {
				for _, f := range fs {
					if int(uint64(f)>>20&0xffff)%workers == w {
						m[f] = append(m[f], int32(j))
					}
				}
			}
			parts[w] = m
		}(w)
	}
	wg.Wait()
	idx := parts[0]
	for _, m := range parts[1:] {
		for f, ids := range m {
			idx[f] = ids
		}
	}
	rar := make([][]uint8, n)
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := w; j < n; j += workers {
				r := make([]uint8, len(features[j]))
				for i, f := range features[j] {
					r[i] = uint8(min(bits.Len(uint(len(idx[f]))), rarityBuckets-1))
				}
				rar[j] = r
			}
		}(w)
	}
	wg.Wait()
	return features, idx, rar
}

// blockTable is the signature table of block b.
func (ex *Executor) blockTable(b int32) *SignatureTable {
	if ex.blockTables == nil {
		return ex.signatures
	}
	return ex.blockTables[b]
}

// tableFor is the signature table of j's block.
func (ex *Executor) tableFor(j int32) *SignatureTable { return ex.blockTable(ex.blockIndex[j]) }

// signatureID identifies a transaction of this executor in the table's writer lists.
func (ex *Executor) signatureID(j int32) int64 { return ex.signatureRun<<32 | int64(j) }

// writerCandidate is an unfinished lower transaction a key's signature names as a
// writer: with probability num/den, by the feature at index fi of the
// signature's naming features (-1: the read-modify-write rule).
type writerCandidate struct {
	tx       int32
	fi       int8
	num, den float64
}

// writerCandidates lists the writers of k that j should consider at probability
// floor minP.
func (ex *Executor) writerCandidates(j int32, k Key, minP float64) []writerCandidate {
	if ex.signatures == nil || k.Kind != kindStorage {
		return nil
	}
	t := ex.tableFor(j)
	if !t.maybe(k) {
		return nil
	}
	var out []writerCandidate
	add := func(c writerCandidate) {
		for x := range out {
			if out[x].tx == c.tx {
				if c.num*out[x].den > out[x].num*c.den {
					out[x] = c
				}
				return
			}
		}
		out = append(out, c)
	}
	var buf [signatureSlots]namingFeature
	names, rmw, _ := t.names(k, buf[:0], minP)
	if len(names) == 0 && rmw.num == 0 {
		return nil
	}
	ex.add(j, counterSignatureLookups, 1)
	front := ex.committed.Load()
	if rmw.num > 0 {
		for _, i := range ex.lowerTouchers(j, k) {
			if i >= front && !ex.finished[i].Load() {
				add(writerCandidate{tx: i, fi: -1, num: rmw.num, den: rmw.den})
			}
		}
	}
	for fi, nm := range names {
		ids := ex.featureIndex[nm.f]
		taken := 0
		for p := sort.Search(len(ids), func(x int) bool { return ids[x] >= j }) - 1; p >= 0 && ids[p] >= front && taken < candidateScan; p-- {
			if i := ids[p]; !ex.finished[i].Load() {
				taken++
				add(writerCandidate{tx: i, fi: int8(fi), num: nm.num, den: nm.den})
			}
		}
	}
	return sortCands(out, candidateScan)
}

// sortCands orders a reader's candidates nearest first, at most scan of them
// (nil: none).
func sortCands(out []writerCandidate, scan int) []writerCandidate {
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(a, b int) bool { return out[a].tx > out[b].tx })
	if len(out) > scan {
		out = out[:scan]
	}
	return out
}

// signatureClass says what the table knew of k when writer invalidated reader r on
// it: 0 the key was not in it, 1 a feature of the writer named it, 2 the key
// was known but its signature did not name the writer.
func (ex *Executor) signatureClass(r int32, k Key, writer int32) int {
	t := ex.tableFor(r)
	if !t.maybe(k) {
		return 0
	}
	var buf [signatureSlots]namingFeature
	names, _, known := t.names(k, buf[:0], waitThreshold())
	if !known {
		return 0
	}
	for _, nm := range names {
		for _, f := range ex.features[writer] {
			if f == nm.f {
				return 1
			}
		}
	}
	return 2
}

// noteContended records that reader r was invalidated on k by writer: k is
// contended, and writer changed it. The other keys that writer changed in the
// same contract enter with it: the contended object is rarely one slot (a
// pool's price, liquidity and fee counters go together).
func (ex *Executor) noteContended(r int32, k Key, writer int32) {
	if ex.signatures == nil || k.Kind != kindStorage || writer < 0 {
		return
	}
	id, fs, rar := ex.signatureID(writer), ex.features[writer], ex.featureRarity[writer]
	t := ex.tableFor(r)
	t.credit(k, id, fs, rar, true)
	if ws := ex.changedKeys[writer].Load(); ws != nil {
		for _, s := range *ws {
			if s != k && s.Addr == k.Addr {
				t.credit(s, id, fs, rar, true)
			}
		}
	}
}

// learnSignatures feeds a finished incarnation of j into the table, once per
// transaction: its features as carriers, the contended keys it changed, and
// whether it changed the contended keys it read.
func (ex *Executor) learnSignatures(j int32, run *incarnation) {
	if ex.signatures == nil {
		return
	}
	t := ex.tableFor(j)
	ex.judgeSignatureWaits(j, run)
	if ex.signatureLearned[j].Swap(true) {
		return
	}
	fs, rar := ex.features[j], ex.featureRarity[j]
	id := ex.signatureID(j)
	var changed []Key
	var last common.Address
	for i := range run.writes {
		k := run.writes[i].key
		if k.Kind != kindStorage {
			continue
		}
		if seen, ok := run.vals[k]; ok && seen == fieldOf(kindStorage, run.writes[i].val) {
			// It put back what it read (a re-entrancy lock, an unchanged
			// price): no reader above is invalidated by this, so for the
			// key's signature this carrier did not write.
			continue
		}
		changed = append(changed, k)
		t.creditAhead(k, id, fs, rar, false, 1)
		if k.Addr != last {
			last = k.Addr
			t.known.add(addressBits(k.Addr[:])) // its address anywhere in a later transaction's calldata is a feature
		}
	}
	if len(changed) > 0 {
		ex.changedKeys[j].Store(&changed)
	}
	t.carried(fs) // after its credits (creditAhead)
	for k := range run.reads {
		if k.Kind == kindStorage && t.maybe(k) {
			t.readBy(k, containsKey(changed, k))
		}
	}
}

// judgeSignatureWaits looks at the signature waits of a finished incarnation that
// were for a transaction that did not change the key. Such a wait cost the
// reader its duration only if nothing the reader then read was published
// while it waited: otherwise it would have been invalidated without it. The
// count and the time of the waits in vain in that sense are what a measured
// waitThreshold needs (Stats.VainWaits).
func (ex *Executor) judgeSignatureWaits(j int32, run *incarnation) {
	for i := range run.waits {
		w := &run.waits[i]
		if !w.vain {
			continue
		}
		useful := false
		for _, o := range run.reads {
			if o.tx >= 0 {
				if p := ex.publishedNs[o.tx].Load(); p > w.t0 && p <= w.t1 {
					useful = true
					break
				}
			}
		}
		if !useful {
			ex.add(j, counterVainWaits, 1)
			ex.add(j, counterVainWaitNs, w.ns)
		}
	}
}

func isAddressWord(w common.Hash) bool {
	for _, b := range w[:12] {
		if b != 0 {
			return false
		}
	}
	return w[12] != 0 || w[13] != 0 || w[14] != 0 || w[15] != 0
}
