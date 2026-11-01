// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"container/heap"
	"sync/atomic"
	"time"
)

// The reader's waits: an incarnation running on its transaction's slot
// blocks through the executor, which gives the slot up while it sleeps.

// checkStale unwinds the incarnation if it became stale.
func (r *versionReader) checkStale() { r.ex.checkStale(r.j) }

func (r *versionReader) waitPublished(w int32, k Key) int64 { return r.ex.waitPublished(r.j, w, k) }

func (r *versionReader) waitWriter(i int32, mark estimateMark) { r.ex.waitWriter(r.j, i, mark) }

func (r *versionReader) waitFrontier() { r.ex.waitFrontier(r.j) }

// waited says whether a transaction waits for j's publication: asleep
// (sleepers) or polling (chainWaiters).
func (ex *Executor) waited(j int32) bool {
	return ex.sleepers[j].Load() > 0 || (ex.chainWaiters != nil && ex.chainWaiters[j].Load() > 0)
}

// publishedCurrent says whether w's current incarnation has installed its versions:
// a wait for its publication is over as soon as they are, not when the
// incarnation is marked executed, whether or not it wrote the key. Between the
// two the finisher does the in-block feedback, its pre-registration and the
// committer's slice, on every hop of a chain whose reader waits. A discard
// moves inc on.
func (ex *Executor) publishedCurrent(w int32) bool {
	return ex.publishedInc[w].Load() == ex.inc[w].Load()+1
}

// spinners counts the goroutines polling right now.
var spinners atomic.Int32

// broadcast wakes everyone waiting for a publication of j; txMu[j] held.
func (ex *Executor) broadcast(j int32) {
	ex.publishedNs[j].Store(time.Now().UnixNano())
	if timelineFile != nil {
		ex.timeline(j, "P %d %d %d\n", ex.blockOf(j).Fixture.Number, j, time.Since(ex.start).Nanoseconds())
	}
	close(ex.progress[j])
	ex.progress[j] = make(chan struct{})
}

// checkStale unwinds the running incarnation of j if it has become stale.
func (ex *Executor) checkStale(j int32) {
	if ex.stale[j].Load() {
		panic(errAbortIncarnation)
	}
}

// Reasons a transaction blocks.
const (
	waitFence    = iota // WAIT fence at the first access
	waitFrontier        // a beneficiary's balance, a volatile balance's writer
	waitEstimate        // read hit an ESTIMATE (either label) of an aborted writer
)

// block gives up j's worker slot until ch is closed.
func (ex *Executor) block(j int32, ch <-chan struct{}, why int) {
	t0 := time.Now()
	if why == waitFence && ex.chainSpin != nil && ex.chainSpin[j].Load() && ex.spinChain(j, ch, why, t0) {
		return // the writer published while j polled on its processor
	}
	if why == waitFence || why == waitEstimate {
		// a wait for w's publication (not for a commit): w's finisher hands over
		if w := ex.waitOn[j].Load() - 1; w >= 0 {
			ex.sleepers[w].Add(1)
			defer ex.sleepers[w].Add(-1)
		}
	}
	ex.scheduler.release()
	<-ch
	t1 := time.Now()
	ex.scheduler.acquire(j)
	t2 := time.Now()
	if why == waitEstimate {
		atomic.AddInt64(&ex.stats.EstimateWaitNs, t1.Sub(t0).Nanoseconds())
	}
	ex.blocked[j].Add(t2.Sub(t0).Nanoseconds())
	if timelineFile != nil {
		ex.timeline(j, "B %d %d %d %d %d %d %d\n", ex.blockOf(j).Fixture.Number, j, why, t0.Sub(ex.start).Nanoseconds(), t1.Sub(ex.start).Nanoseconds(), t2.Sub(ex.start).Nanoseconds(), ex.waitOn[j].Load()-1)
	}
	ex.checkStale(j)
}

// countEstimateWait counts a reader's wait on an ESTIMATE by its label.
func (ex *Executor) countEstimateWait(mark estimateMark) {
	atomic.AddInt64(&ex.stats.EstimateWaits, 1)
	if mark == markRewrites {
		atomic.AddInt64(&ex.stats.EstimateRewriteWaits, 1)
	}
}

// waitEstimateKey blocks j until the ESTIMATE (labelled mark) that writer w
// left on k is no longer the version j sees (w released or published k
// again), or w finished. Waiting on the writer's signal alone would last until
// the flip at the end of its next incarnation, so that neither the release of
// the key at the end of the writer's region nor the early publication of its
// versions woke the reader, and a chain of re-executions would pay each
// writer's whole run. So the reader waits on the writer's progress (what every
// release and publication broadcasts).
func (ex *Executor) waitEstimateKey(j, w int32, k Key, mark estimateMark) {
	ex.countEstimateWait(mark)
	if ex.chainOn {
		// the re-executing writer releases a hot key at its last write only
		// while somebody waits for it (chain.go): without this, a reader
		// waiting on its ESTIMATE would be invisible to it and the release
		// would stay held until its finish
		ex.chainWaiters[w].Add(1)
		defer ex.chainWaiters[w].Add(-1)
	}
	for {
		if ex.finished[w].Load() || ex.publishedCurrent(w) {
			return
		}
		ex.txMu[w].Lock()
		if ex.finished[w].Load() || ex.publishedCurrent(w) {
			ex.txMu[w].Unlock()
			return
		}
		ch := ex.progress[w]
		ex.txMu[w].Unlock()
		if v, ok := ex.store.latestBelow(k, j); !ok || v.tx != w || !v.estimate() {
			return
		}
		ex.waitOn[j].Store(w + 1)
		ex.block(j, ch, waitEstimate)
		ex.waitOn[j].Store(0)
	}
}

// waitWriter blocks transaction j until writer i republishes: a balance chain
// holds an ESTIMATE (labelled mark) of i.
func (ex *Executor) waitWriter(j, i int32, mark estimateMark) {
	ex.countEstimateWait(mark)
	ex.txMu[i].Lock()
	ch := ex.signal[i]
	ex.txMu[i].Unlock()
	ex.waitOn[j].Store(i + 1)
	ex.block(j, ch, waitEstimate)
	ex.waitOn[j].Store(0)
}

// lowerTouchers returns the lower transactions whose running incarnation has
// touched k.
func (ex *Executor) lowerTouchers(j int32, k Key) []int32 {
	var out []int32
	kinds := []keyKind{kindStorage}
	if k.Kind != kindStorage {
		kinds = []keyKind{kindBalance, kindNonce, kindCode}
	}
	for _, kind := range kinds {
		e := ex.store.get(Key{Addr: k.Addr, Slot: k.Slot, Kind: kind})
		if e == nil {
			continue
		}
		e.mu.Lock()
		for _, r := range e.readers {
			if r.tx < j && !containsWriter(out, r.tx) {
				out = append(out, r.tx)
			}
		}
		e.mu.Unlock()
	}
	return out
}

// writerWrote reports whether the current incarnation of the finished writer w
// wrote fence key k.
func (ex *Executor) writerWrote(w int32, k Key) bool {
	ex.txMu[w].Lock()
	defer ex.txMu[w].Unlock()
	if ex.status[w].get() != statusExecuted && ex.status[w].get() != statusCommitted {
		return false
	}
	if ex.runs[w] == nil {
		return false
	}
	for _, x := range ex.written[w] {
		if fk, ok := fenceKey(x); ok && fk == k {
			return true
		}
	}
	return false
}

// waitFrontier blocks transaction j until every lower transaction committed.
func (ex *Executor) waitFrontier(j int32) {
	for {
		ch := ex.frontierReach(j)
		if ch == nil {
			return
		}
		ex.block(j, ch, waitFrontier)
	}
}

// frontierReach returns a channel closed once the frontier reaches at, or nil
// if it already has. Waiters are woken individually, not on every commit.
func (ex *Executor) frontierReach(at int32) chan struct{} {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if ex.frontier >= at {
		return nil
	}
	ch := make(chan struct{})
	heap.Push(&ex.frontierWaiters, frontierWaiter{at, ch})
	return ch
}

type frontierWaiter struct {
	at int32
	ch chan struct{}
}

type frontierHeap []frontierWaiter

func (h frontierHeap) Len() int           { return len(h) }
func (h frontierHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h frontierHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *frontierHeap) Push(x any)        { *h = append(*h, x.(frontierWaiter)) }
func (h *frontierHeap) Pop() any {
	old := *h
	w := old[len(old)-1]
	*h = old[:len(old)-1]
	return w
}

// waitPublished blocks transaction j until writer w has published k (early) or
// finished its current incarnation (WAIT fence). A visible version of w that
// is not an ESTIMATE ends the wait, also one an invalidated incarnation of w
// left tentative (markEstimates): the reader reads it as it would have
// without the wait.
func (ex *Executor) waitPublished(j, w int32, k Key) (blocked int64) {
	counted := false
	var t0 time.Time
	defer func() {
		if counted {
			blocked = time.Since(t0).Nanoseconds()
			if timelineFile != nil {
				ex.timeline(j, "K %d %d %d %d %s %s %d\n", ex.blockOf(j).Fixture.Number, j, w, k.Kind, k.Addr.Hex(), k.Slot.Hex(), time.Since(ex.start).Nanoseconds())
			}
			if ex.chainOn {
				ex.chainWaiters[w].Add(-1)
			}
		}
	}()
	for {
		if ex.finished[w].Load() || ex.publishedCurrent(w) {
			return
		}
		ex.txMu[w].Lock()
		if ex.finished[w].Load() || ex.publishedCurrent(w) {
			ex.txMu[w].Unlock()
			return
		}
		ch := ex.progress[w]
		ex.txMu[w].Unlock()
		if v, ok := ex.store.latestBelow(k, w+1); ok && v.tx == w && !v.estimate() {
			if counted {
				atomic.AddInt64(&ex.stats.WaitByRelease, 1)
			}
			return
		}
		if !counted {
			t0 = time.Now()
			counted = true
			if ex.chainOn {
				ex.chainWaiters[w].Add(1) // w's held releases go out at its next state access (flushHeld)
			}
		}
		ex.waitOn[j].Store(w + 1)
		ex.block(j, ch, waitFence)
		ex.waitOn[j].Store(0)
	}
}

// fenceWait is a signature wait that blocked for ns, from t0 to t1; vain: for
// a transaction that did not change the key (judgeSignatureWaits).
type fenceWait struct {
	ns     int64
	vain   bool
	t0, t1 int64
}

// volatileBalances is, by balance key, how often a re-execution that wrote
// it again wrote another value; unlike rewriteRates it counts the key that
// invalidated the writer too.
var volatileBalances rewriteRate

// Waiting on a volatile balance's chain is a decision of the cost model
// (cost.go): q is that the balance changes when its writer runs again
// (volatileBalances). Waiting when it would not have changed costs the
// reader the wait (A), only while the processors are idle; reading on when
// it does costs the reader a re-execution (B). It waits when q >= A / (A + B),
// one half until a window has measured both.
func volatileThreshold() float64 {
	return min(loadThreshold(&costs.volatile), sketchSure)
}

// learnVolatile folds a window's measured costs into volatileThreshold; u is
// its idle share.
func learnVolatile(st *Stats, u float64) {
	if st.Aborts < 8 || st.ReexecNs <= 0 || st.VolatileWaits < 8 || st.VolatileWaitNs <= 0 {
		return
	}
	reexec := float64(st.ReexecNs) / float64(st.Aborts)
	wait := float64(st.VolatileWaitNs) / float64(st.VolatileWaits)
	foldScaled(&costs.volatile, u*wait, reexec, 1)
}

// volatileWait is the writer a reader at j (block b) of balance key bk waits
// for: walking from the uncommitted writer of the version it would see down
// the writers' own reads of bk, the first that has not finished, or the
// first finished one whose read of bk is no longer current by value (a
// stale link). A stale link's writer is doomed (the committer would reject
// it): it is invalidated here if nobody has yet, and
// the reader waits for its next incarnation. A link stale only by version
// (the value it read is still the one there) is walked through. w is -1
// when the walk reaches a committed version, the pre-state or a writer that
// did not read bk. done is closed when w's current incarnation finishes
// (signal); why says where the walk stopped (vw...) and depth how many
// writers it passed.
func (ex *Executor) volatileWait(bk Key, j, b int32) (w int32, done <-chan struct{}, why, depth int) {
	w = ex.volatileWriter(bk, j, b)
	if w < 0 {
		return -1, nil, walkNone, 0
	}
	for ; depth < 64; depth++ {
		ex.txMu[w].Lock()
		if !ex.finished[w].Load() {
			ch := ex.signal[w]
			ex.txMu[w].Unlock()
			return w, ch, walkUnfinished, depth
		}
		run := ex.runs[w]
		o, read := run.reads[bk]
		switch {
		case !read:
			ex.txMu[w].Unlock()
			return -1, nil, walkUnread, depth
		case o.tx < 0 || o.inc == boundaryInc || o.inc == predictedInc || o.tx < ex.committed.Load():
			ex.txMu[w].Unlock()
			return -1, nil, walkCommitted, depth
		}
		if v, ok := ex.store.latestBelow(bk, w); !ok || v.tx != o.tx || v.inc != o.inc || v.estimate() {
			if !ex.readCurrent(w, bk, o, run.vals) {
				if ex.status[w].get() == statusExecuted {
					// nobody invalidated it yet: do it now rather than at the frontier
					writer := int32(-1)
					if ok {
						writer = v.tx
					}
					ex.countAbort(w, readRec{tx: w, inc: ex.inc[w].Load(), orig: o}, bk, writer, "volwait")
					ex.noteFailed(w, bk)
					ex.noteRisky(w, run)
					atomic.AddInt64(&ex.stats.Aborts, 1)
					ex.abortTx(w)
				}
				ch := ex.signal[w] // the next incarnation's
				ex.txMu[w].Unlock()
				return w, ch, walkStale, depth
			}
		}
		ex.txMu[w].Unlock()
		w = o.tx
	}
	return -1, nil, walkDeep, depth
}

// Where volatileWait's walk stopped.
const (
	walkNone       = iota // no uncommitted version below the reader
	walkCommitted         // the chain reached a committed version, the pre-state or a boundary
	walkUnread            // a finished writer on it did not read the key
	walkStale             // a finished writer's read of the key is no longer current by value (the reader waits for its next incarnation)
	walkUnfinished        // a writer on it has not finished (the reader waits)
	walkDeep              // 64 writers deep
)

// volatileWriter is the uncommitted transaction whose version of balance key
// bk a reader at j (block b) would see first, or -1 when that version is
// committed, a boundary, a prediction, the pre-state or an ESTIMATE (the
// reader waits for an ESTIMATE anyway).
func (ex *Executor) volatileWriter(bk Key, j, b int32) int32 {
	_, top, ok, est := ex.store.balanceBelow(bk, j, b)
	if est != nil || !ok || top.tx < 0 || top.inc == boundaryInc || top.inc == predictedInc || top.tx < ex.committed.Load() {
		return -1
	}
	return top.tx
}
