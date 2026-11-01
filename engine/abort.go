// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"hash/maphash"
	"sync/atomic"
	"time"
)

// Aborting an incarnation. A running incarnation whose reads went stale is
// discarded (discard, by its own worker); an executed one that fails
// validation or is invalidated after it finished is aborted and queued again
// (abortTx). Either way the transaction's incarnation number moves on and
// the versions the incarnation wrote stay in the store, labelled.

// discard drops the current incarnation of j: its versions are labelled
// (markEstimates) and the next incarnation starts.
func (ex *Executor) discard(j int32) {
	ex.txMu[j].Lock()
	ex.discardTx(j)
	ex.txMu[j].Unlock()
}

// discardTx is discard with txMu[j] held.
func (ex *Executor) discardTx(j int32) {
	atomic.AddInt64(&ex.stats.Aborts, 1)
	ex.abortNs[j].Store(time.Now().UnixNano())
	ex.inc[j].Add(1)
	ex.status[j].set(statusAborted)
	ex.markEstimates(j)
}

// abortTx discards the executed incarnation of f and queues f again; txMu[f]
// held.
func (ex *Executor) abortTx(f int32) {
	if run := ex.runs[f]; run != nil {
		atomic.AddInt64(&ex.stats.WastedNs, run.execNs)
	}
	ex.abortNs[f].Store(time.Now().UnixNano())
	ex.inc[f].Add(1)
	ex.status[f].set(statusAborted)
	ex.finished[f].Store(false)
	ex.lowerFinishedFront(f)
	ex.markEstimates(f)
	ex.signal[f] = make(chan struct{})
	ex.scheduler.push(f)
}

// markEstimates labels each key the aborted incarnation wrote by why its
// value is in doubt, instead of marking them all ESTIMATE:
//
//   - markFailedKey: the key whose stale read invalidated the incarnation
//     (abortKey) and which it also wrote. The next incarnation reads it anew
//     and mostly writes another value; readers wait for it.
//   - markRewrites: another key whose doubted values, learned by key across
//     windows (rewriteRates), mostly differ from the committed ones; readers
//     wait for it.
//   - tentative (no mark): every other key. The version stays visible as it
//     was; readers read it and it is validated like any read (by value, so a
//     republication of the same value keeps them). Such a version is told
//     apart by its incarnation being older than the writer's current one
//     (version.tentative).
//

// estimateMark is the label markEstimates puts on a version.
type estimateMark uint8

const (
	markNone      estimateMark = iota // not an ESTIMATE: current or tentative
	markFailedKey                     // the key whose stale read invalidated the writer
	markRewrites                      // learned to change when the writer runs again
)

func (m estimateMark) String() string {
	switch m {
	case markFailedKey:
		return "failed_key"
	case markRewrites:
		return "rewrites"
	}
	return "none"
}

// estimate says whether readers must wait for the writer's next incarnation.
func (v version) estimate() bool { return v.mark != markNone }

// tentative says whether v was left by an invalidated incarnation of its
// writer without an ESTIMATE label; cur is the writer's current incarnation.
func (v version) tentative(cur int32) bool {
	return v.tx >= 0 && v.mark == markNone && v.inc != boundaryInc && v.inc != predictedInc && v.inc>>seqBits < cur
}

// markEstimates labels the versions f's aborted incarnation wrote and, for a
// labelled key that is likely enough to change (learnCascade), invalidates
// its readers at once.
func (ex *Executor) markEstimates(f int32) {
	for _, k := range ex.written[f] {
		mark, rates := markRewrites, rewriteRates
		if fk, _ := fenceKey(k); fk == ex.abortKey[f] {
			mark, rates = markFailedKey, failedRates
		}
		ex.noteDoubt(f, k, mark == markFailedKey)
		q := rates.changeProb(k)
		if q < estimateThreshold() || !ex.probe(probeEstimate, &costs.estimate) {
			continue // left tentative
		}
		if mark == markRewrites {
			ex.add(f, counterRewritesLabelled, 1)
		}
		e := ex.store.get(k)
		if e == nil {
			continue
		}
		cascade := q >= cascadeThreshold() && ex.probe(probeCascade, &costs.cascade)
		var victims []readRec
		e.mu.Lock()
		e.markEstimate(f, mark)
		if cascade {
			for _, r := range e.readers {
				if r.orig.tx == f {
					victims = append(victims, r)
				}
			}
		}
		e.mu.Unlock()
		ex.noteChange(f, k)
		if len(victims) > 0 {
			// The readers stay registered: one whose invalidation finds it
			// between states is invalidated again by the republication.
			// Another goroutine, as txMu[f] is held here.
			go func(k Key) {
				for _, v := range victims {
					ex.invalidate(v, k, f)
				}
			}(k)
		}
	}
}

// A version an invalidated incarnation left is in doubt until its writer
// commits: the rates learn whether the committed value differs. A
// republication is not evidence enough: one by an incarnation that read a
// value still in doubt is itself transient, and on a counter every transaction
// increments such republications go back and forth.
type doubt struct {
	k      Key
	inc    int32 // the incarnation that wrote the value
	failed bool  // the key that invalidated it (failedRates)
	val    Value
}

// noteDoubt remembers the value f's invalidated incarnation left at k, unless
// it was noted already or there is none; txMu[f] held. A discarded run that
// had not published leaves the earlier one.
func (ex *Executor) noteDoubt(f int32, k Key, failed bool) {
	v, ok := ex.store.latestBelow(k, f+1)
	if !ok || v.tx != f {
		return
	}
	for _, d := range ex.doubts[f] {
		if d.k == k && d.inc == v.inc {
			return
		}
	}
	ex.doubts[f] = append(ex.doubts[f], doubt{k: k, inc: v.inc, failed: failed, val: v.val})
}

// judgeDoubts compares, at f's commit, the values its invalidated incarnations
// left with the committed ones; txMu[f] held.
func (ex *Executor) judgeDoubts(f int32, run *incarnation) {
	for _, d := range ex.doubts[f] {
		same := false
		for _, w := range run.writes {
			if w.key == d.k {
				same = sameValue(d.k.Kind, d.val, w.val)
				break
			}
		}
		if d.failed {
			failedRates.observe(d.k, same)
		} else {
			rewriteRates.observe(d.k, same)
		}
	}
	ex.doubts[f] = nil
}

// rewriteRates is, by key (a sketch kept across windows), how often a value an
// invalidated incarnation left at a key, other than the invalidating one,
// differed from the committed value.
var rewriteRates = &rewriteRate{pooled: true}

// failedRates is the same for the key that invalidated the incarnation, which
// goes through the same decision with its own rate. Both are consulted at
// aborts only, so their priors are learned; the volatile balances' rate is
// consulted on every balance read and keeps the even one.
var failedRates = &rewriteRate{pooled: true}

const (
	rewriteRateSlots    = 1 << 14 // keys
	rewriteAddressSlots = 1 << 12 // contracts and accounts
)

var rewriteSeed = maphash.MakeSeed()

// rewriteRate counts at three levels: the key, its address and all keys. A
// key with few observations borrows its address's rate as the prior, and an
// address the rate of all keys; the even prior (one half) is left only at
// the top, since most labels are decided for keys not yet observed.
type rewriteRate struct {
	seen, changed               [rewriteRateSlots]atomic.Uint32
	addressSeen, addressChanged [rewriteAddressSlots]atomic.Uint32
	allSeen, allChanged         atomic.Uint32
	pooled                      bool // the key's prior is learned (else even); see failedRates
}

// rateState is a copy of a rewriteRate's counts (Learned).
type rateState struct {
	seen, changed               [rewriteRateSlots]uint32
	addressSeen, addressChanged [rewriteAddressSlots]uint32
	allSeen, allChanged         uint32
}

func (s *rewriteRate) save() *rateState {
	t := new(rateState)
	for i := range s.seen {
		t.seen[i], t.changed[i] = s.seen[i].Load(), s.changed[i].Load()
	}
	for i := range s.addressSeen {
		t.addressSeen[i], t.addressChanged[i] = s.addressSeen[i].Load(), s.addressChanged[i].Load()
	}
	t.allSeen, t.allChanged = s.allSeen.Load(), s.allChanged.Load()
	return t
}

func (s *rewriteRate) restore(t *rateState) {
	for i := range s.seen {
		s.seen[i].Store(t.seen[i])
		s.changed[i].Store(t.changed[i])
	}
	for i := range s.addressSeen {
		s.addressSeen[i].Store(t.addressSeen[i])
		s.addressChanged[i].Store(t.addressChanged[i])
	}
	s.allSeen.Store(t.allSeen)
	s.allChanged.Store(t.allChanged)
}

func (s *rewriteRate) slot(k Key) uint64 {
	return maphash.Comparable(rewriteSeed, k) % rewriteRateSlots
}

func (s *rewriteRate) addressSlot(k Key) uint64 {
	return maphash.Comparable(rewriteSeed, k.Addr) % rewriteAddressSlots
}

func (s *rewriteRate) observe(k Key, same bool) {
	i := s.slot(k)
	rateObserve(&s.seen[i], &s.changed[i], same)
	if s.pooled {
		a := s.addressSlot(k)
		rateObserve(&s.addressSeen[a], &s.addressChanged[a], same)
		rateObserve(&s.allSeen, &s.allChanged, same)
	}
}

// rateObserve counts one observation, forgetting by halving both counts
// (races only blur them).
func rateObserve(seen, changed *atomic.Uint32, same bool) {
	if seen.Add(1) >= 64 {
		seen.Store(32)
		changed.Store(changed.Load() / 2)
	}
	if !same {
		changed.Add(1)
	}
}

// changeProb is the posterior mean of the share of k's doubted values that
// the commit changed: from its address's rate, which is from the rate of all
// keys, which is from the even prior, each weighing as two observations.
func (s *rewriteRate) changeProb(k Key) float64 {
	q := 0.5
	if s.pooled {
		q = rateMean(s.allSeen.Load(), s.allChanged.Load(), q)
		a := s.addressSlot(k)
		q = rateMean(s.addressSeen[a].Load(), s.addressChanged[a].Load(), q)
	}
	i := s.slot(k)
	return rateMean(s.seen[i].Load(), s.changed[i].Load(), q)
}

func rateMean(seen, changed uint32, prior float64) float64 {
	return (float64(changed) + 2*prior) / (float64(seen) + 2)
}

// The labelling is a decision of the cost model (cost.go): q is that the key
// changes when the writer runs again (changeProb). Leaving a key that then
// changes tentative costs its readers a re-execution each (B): the readers
// that took the tentative value are invalidated by the republication.
// Labelling a key that then keeps its value costs its readers the wait for
// the writer's republication (A), which costs only while the processors are
// idle. Labelling pays when q >= A / (A + B).

// estimateThreshold is the learned change probability from which a key is
// labelled ESTIMATE.
func estimateThreshold() float64 {
	return min(loadThreshold(&costs.estimate), sketchSure)
}

// learnEstimate folds a window's measured costs into the labelling; u is its
// idle share.
func learnEstimate(st *Stats, u float64) {
	if st.Aborts < 8 || st.ReexecNs <= 0 || st.TentativeChanged < 8 || st.RewritesLabelled < 8 || st.EstimateWaits <= 0 {
		return
	}
	reexec := float64(st.ReexecNs) / float64(st.Aborts)
	b := float64(st.TentativeAborts) / float64(st.TentativeChanged) * reexec
	wait := float64(st.EstimateWaitNs) / float64(st.EstimateWaits)
	a := u * float64(st.EstimateRewriteWaits) / float64(st.RewritesLabelled) * wait
	foldScaled(&costs.estimate, a, b, 1)
}

// A labelled key's readers that took the aborted incarnation's value are
// invalidated by the republication if it changes the key. Left to it, a
// cascade advances one re-execution per hop: the reader is invalidated only
// once its writer ran again, and its own readers once it did, which makes a
// window serial on a counter every transaction increments. Invalidating them
// at once wastes a re-execution when the key keeps its value (A, slot time
// while the processors are busy: (1-u) of it), and waiting delays a doomed
// reader by the time from the abort to the republication (B, measured on the
// cascades left to it). The readers are invalidated when the key's change
// probability q >= A / (A + B).

// cascadeThreshold is the learned change probability from which a labelled
// key's readers are invalidated at the abort.
func cascadeThreshold() float64 {
	return min(loadThreshold(&costs.cascade), sketchSure)
}

// learnCascade folds a window's measured costs into the decision; u is its
// idle share.
func learnCascade(st *Stats, u float64) {
	if st.Aborts < 8 || st.ReexecNs <= 0 || st.Cascades < 8 {
		return
	}
	a := (1 - u) * float64(st.ReexecNs) / float64(st.Aborts)
	b := float64(st.CascadeNs) / float64(st.Cascades)
	foldScaled(&costs.cascade, a, b, 1)
}
