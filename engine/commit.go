// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"container/heap"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// The commit side of the executor: the dedicated committer drives the commit
// frontier in index order, validating what push invalidation did not cover
// (unregistered reads, account fields, predicates, block-boundary values),
// crediting the beneficiaries' fees and correcting the next block's
// boundary; finishers validate their own reads ahead of it while it is busy.

// wakeCommitter tells the committer that j, its frontier transaction, has
// executed, if it went to sleep waiting for that.
func (ex *Executor) wakeCommitter(j int32) {
	if ex.committed.Load() == j && ex.commitAsleep.Load() {
		select {
		case ex.commitWake <- struct{}{}:
		default:
		}
	}
}

// committer drives the commit frontier for the whole window on a goroutine of
// its own, locked to its thread. The commit of a window is one sequential
// activity whoever performs it: left to whichever finisher came by, it would
// run on a different core every few transactions, on cold caches, waiting for
// a thread to wake before every stretch. Waiting for the frontier transaction,
// the committer polls while a processor is spare (commitPollOn) and sees the
// transaction the moment it has executed; otherwise it sleeps and the frontier
// transaction's finisher wakes it (awaitExecuted).
func (ex *Executor) committer() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	ex.mu.Lock()
	for {
		ex.commitLoopLocked()
		if due := ex.rebaseQ; len(due) > 0 {
			ex.rebaseQ = nil
			ex.mu.Unlock()
			for _, b := range due {
				ex.rebase(b)
			}
			ex.mu.Lock()
			for _, b := range due {
				ex.rebased[b] = true
			}
			continue // the frontier was held at the block boundary
		}
		if int(ex.frontier) >= ex.n || ex.err != nil {
			ex.mu.Unlock()
			return
		}
		f := ex.frontier
		ex.mu.Unlock()
		ex.scheduler.kick() // the frontier moved: the run-ahead bound may release work
		if !ex.awaitExecuted(f) {
			return
		}
		ex.mu.Lock()
	}
}

// awaitExecuted returns once the frontier transaction f has an executed
// incarnation (true) or the window stopped (false). It polls while a processor
// is spare, then sleeps until f's finisher wakes it.
func (ex *Executor) awaitExecuted(f int32) bool {
	spinning := ex.startCommitSpin()
	for i := 0; ; i++ {
		if ex.status[f].get() == statusExecuted {
			ex.endCommitSpin(spinning)
			return true
		}
		if spinning && i&63 != 0 {
			continue
		}
		if spinning && ex.commitPollOn() {
			select {
			case <-ex.done:
				ex.endCommitSpin(spinning)
				return false
			default:
			}
			continue
		}
		ex.endCommitSpin(spinning)
		select {
		case <-ex.done:
			return false
		default:
		}
		ex.commitAsleep.Store(true)
		if ex.status[f].get() == statusExecuted { // it executed before the flag was up
			ex.commitAsleep.Store(false)
			return true
		}
		select {
		case <-ex.commitWake:
		case <-ex.done:
			ex.commitAsleep.Store(false)
			return false
		}
		ex.commitAsleep.Store(false)
		spinning = ex.startCommitSpin()
	}
}

// commitPollOn says whether the committer may poll for the frontier
// transaction: a processor is spare while the busy workers and the polling
// goroutines (the committer among them) hold fewer than all of them, one being
// left for the rest. Each time the committer sleeps, the frontier
// transaction's finisher has to wake its thread, a system call on the critical
// path.
func (ex *Executor) commitPollOn() bool {
	busy := int32(ex.scheduler.workers) - ex.scheduler.freeA.Load() + spinners.Load()
	return busy < ex.procs
}

// startCommitSpin says whether the committer may poll, counted among the
// spinners like any poller. Its poll is a tight loop that never yields: polls
// that yield would queue on the Go scheduler's lock.
func (ex *Executor) startCommitSpin() bool {
	spinners.Add(1)
	if !ex.commitPollOn() {
		spinners.Add(-1)
		return false
	}
	return true
}

func (ex *Executor) endCommitSpin(spinning bool) {
	if spinning {
		spinners.Add(-1)
	}
}

// boundaryInc stamps the versions of a block boundary (store.boundaries).
const boundaryInc int32 = 1 << 30

// rebase installs, once every block before b committed, the values block b's
// pre-state snapshot holds for keys that earlier blocks of the window wrote,
// where they differ: what happened between the blocks outside transactions
// (withdrawals, block rewards, system calls). Readers that already read the
// superseded value are invalidated.
func (ex *Executor) rebase(b int32) {
	bk := ex.blocks[b]
	if bk.First <= 0 {
		return
	}
	vals := make(map[Key]Value)
	// only the keys that changed between the blocks (BlockEnv.Between) can
	// differ from the versions the window's transactions left
	for _, bv := range bk.Between {
		cur, ok := ex.store.latestBelowInBlock(bv.Key, bk.First, b-1)
		if !ok || cur.estimate() || fieldOf(bv.Key.Kind, cur.val) == bv.Field {
			continue // no in-window version (the snapshot is what readers see), or equal
		}
		vals[bv.Key] = bv.Val
	}
	if len(vals) == 0 {
		return
	}
	// Install the layer first, then invalidate: a reader of block b that read
	// an older value before the install is registered or validated at commit;
	// one that reads after it sees the boundary value.
	ex.store.setBoundaries(b, vals)
	if ex.keyGeneration != nil {
		storeMax(&ex.changeAll, ex.changeGeneration.Add(1))
	}
	for k, val := range vals {
		for _, r := range ex.store.boundaryStale(b, k, val) {
			ex.invalidate(r, k, bk.First-1)
		}
	}
}

// recheckOK re-validates j's reads while its account-field loads were
// flagged, outside the global lock, and reports whether the incarnation is
// still current.
func (ex *Executor) recheckOK(j int32, run *incarnation) bool {
	for {
		if ex.stale[j].Load() {
			return false
		}
		if !ex.recheck[j].Swap(false) {
			return true
		}
		if !ex.readsCurrent(j, run) {
			return false
		}
	}
}

// commitLoopLocked commits executed transactions in block order while their
// reads are still current. The first stale one is aborted and re-executed; as
// every lower transaction is committed by then, its next incarnation is exact.
// Only the committer runs it. Whatever stopped commitNext, the frontier
// transaction is looked at once more before the committer goes on to wait.
func (ex *Executor) commitLoopLocked() {
	tc := time.Now()
	for ex.commitNext() || ex.frontierReady() {
	}
	ex.stats.CommitNs += time.Since(tc).Nanoseconds()
}

// frontierReady reports whether the frontier transaction can be validated
// now: it is executed and its block's boundary corrections are done (ex.mu
// held).
func (ex *Executor) frontierReady() bool {
	f := ex.frontier
	if int(f) >= ex.n || ex.err != nil {
		return false
	}
	if b := ex.blockIndex[f]; b > 0 && f == ex.blocks[b].First && !ex.rebased[b] {
		return false
	}
	_, executed := ex.executedRun(f)
	return executed
}

// commitNext validates and commits the frontier transaction and reports
// whether it did; if not, the frontier cannot move for now (ex.mu held,
// released while validating).
func (ex *Executor) commitNext() bool {
	if int(ex.frontier) >= ex.n {
		return false
	}
	f := ex.frontier
	if b := ex.blockIndex[f]; b > 0 && f == ex.blocks[b].First && !ex.rebased[b] {
		return false // block b's boundary corrections first (see rebase)
	}
	run, executed := ex.executedRun(f)
	if !executed {
		return false
	}
	// Validate outside ex.mu: every transaction below f is committed, so
	// no lower version can appear meanwhile; f's own status is re-checked
	// under its lock before it commits.
	ex.mu.Unlock()
	ahead, helperBusy := ex.checkedAhead(run)
	var current bool
	if ahead {
		current = ex.pendingList(f, run.pendingBeneficiary)
	} else {
		current = ex.pendingCurrent(f, run)
	}
	ex.mu.Lock()
	ex.txMu[f].Lock()
	if ex.status[f].get() != statusExecuted || ex.runs[f] != run {
		ex.txMu[f].Unlock()
		return false
	}
	if !current {
		if timelineFile != nil {
			ex.timelineDiscard(f, run, "commit")
		}
		atomic.AddInt64(&ex.stats.Aborts, 1)
		ex.abortTx(f)
		ex.txMu[f].Unlock()
		return false
	}
	if !(ahead && run.aheadPredicates) && !ex.predicatesHold(f, run) {
		if timelineFile != nil {
			ex.timelineDiscard(f, run, "predicate")
		}
		atomic.AddInt64(&ex.stats.Aborts, 1)
		ex.predFail[f].Store(true)
		ex.abortTx(f)
		ex.txMu[f].Unlock()
		return false
	}
	ex.materialize(f, run)
	if run.rejected != nil {
		// A rejection with current reads means the block is invalid, or
		// that something the message depends on went stale without the
		// validation noticing. Every lower transaction is committed, so one
		// more incarnation runs against final state (with exact balances):
		// only if that one is rejected as well is the block invalid. The
		// first rejection is reported with everything it read, and the
		// retry with what it read differently, so that one occurrence
		// identifies the read that validation missed.
		if _, again := ex.rejectedRuns[f]; !again {
			ex.reportRejected(f, run)
			if ex.rejectedRuns == nil {
				ex.rejectedRuns = make(map[int32]*incarnation)
			}
			ex.rejectedRuns[f] = run
			atomic.AddInt64(&ex.stats.Aborts, 1)
			ex.predFail[f].Store(true)
			ex.abortTx(f)
			ex.txMu[f].Unlock()
			return false
		}
		ex.txMu[f].Unlock()
		ex.failLocked(fmt.Errorf("tx %d: %w", f, run.rejected))
		return false
	}
	if old, was := ex.rejectedRuns[f]; was {
		ex.reportRejectRetry(f, old, run)
		delete(ex.rejectedRuns, f)
	}
	ex.creditFee(f, run)
	ex.judgeDoubts(f, run)
	ex.status[f].set(statusCommitted)
	if !helperBusy {
		putPending(run)
	}
	ex.txMu[f].Unlock()
	if ex.firstRunNs != nil {
		ex.chargeCommitLocked(f)
	}
	ex.frontier++
	ex.committed.Store(ex.frontier)
	if ex.firstRunNs != nil {
		ex.applyWidthLocked()
	} else {
		ex.scheduler.setLimit(ex.frontier + ex.runAheadBound) // the bound is a distance from the frontier
	}
	if timelineFile != nil {
		ex.timeline(f, "C %d %d %d\n", ex.blockOf(f).Fixture.Number, f, time.Since(ex.start).Nanoseconds())
	}
	if ex.lookaheadNext > 0 && ex.frontier >= ex.lookaheadNext {
		ex.adjustLookaheadLocked()
	}
	if bk := ex.blockOf(f); f == bk.Last {
		b := ex.blockIndex[f]
		ex.drainMaterialize(f) // the block's results and the next boundary read absolute balances
		ex.blockNs[b] = time.Since(ex.start).Nanoseconds()
		// the next block's boundary, and those of any empty blocks after it
		// (an empty block has no last transaction to commit)
		for nb := b + 1; int(nb) < len(ex.blocks); nb++ {
			ex.rebaseQ = append(ex.rebaseQ, nb)
			if ex.blocks[nb].First <= ex.blocks[nb].Last {
				break
			}
			ex.blockNs[nb] = ex.blockNs[b]
		}
	}
	for len(ex.frontierWaiters) > 0 && ex.frontierWaiters[0].at <= ex.frontier {
		close(heap.Pop(&ex.frontierWaiters).(frontierWaiter).ch)
	}
	if int(ex.frontier) == ex.n && ex.err == nil {
		close(ex.done)
	}
	return true
}

// readsCurrent checks that every observed read of run still resolves to the
// same value. A read that registered in the version store (registered) is kept
// current by push: a lower writer publishing a different version, a withdrawn
// version or a changed block boundary invalidates the reader at once
// (staleReaders, readersOf, boundaryStale), so only the unregistered reads can
// have gone stale and only they are looked up. Only registered storage reads
// qualify (of a version written in the window or of the pre-state, whose
// record sameCheck compares against the block's base field): account-field
// reads are weak (invalidate drops the record of a reader that had not used
// the field yet, and a summed balance depends on versions below the one it
// registered on), predicted reads depend on the deltas their assumed writers
// publish, and a read of a block boundary's value is not followed by
// boundaryStale. The wipe key a storage read also reads is validated as usual.
func (ex *Executor) readsCurrent(j int32, run *incarnation) bool {
	for k, o := range run.reads {
		if k.Kind == kindStorage && o.inc != predictedInc && o.inc != boundaryInc {
			if _, ok := run.registered[k]; ok {
				continue
			}
		}
		if usedField(run.used[k.Addr], k.Kind) && !ex.readCurrent(j, k, o, run.vals) {
			return false
		}
	}
	return true
}

// readCurrent reports whether j's read of k (which observed origin o) still
// resolves to the same value. Keys that cannot be compared by value, or reads
// whose value was not tracked, fall back to the version.
func (ex *Executor) readCurrent(j int32, k Key, o origin, vals map[Key]common.Hash) bool {
	seen, tracked := vals[k]
	return ex.readCurrentField(j, k, o, seen, tracked)
}

// readCurrentField is readCurrent given the field the read observed (seen, if
// tracked) instead of the incarnation's value map.
func (ex *Executor) readCurrentField(j int32, k Key, o origin, seen common.Hash, tracked bool) bool {
	v, ok := ex.store.latestBelow(k, j)
	if ok && v.estimate() {
		return false
	}
	now := baseOrigin
	if ok {
		now = origin{v.tx, v.inc}
	}
	if o.inc == predictedInc {
		// a predicted read never matches a version by identity: it is
		// current if the value it assumed is the one there now
		return tracked && ex.comparable(k, j) && seen == ex.servedField(j, k, now, v.val)
	}
	if k.Kind == kindBalance && tracked {
		// A balance read is validated by the value it saw: the version it
		// registered on may be unchanged while the chain of relative
		// versions below it (or the absolute version anchoring it) was
		// rewritten by a re-executed lower writer, and the committer
		// turns relative versions into absolute ones under the same
		// identity. Reads that summed a chain are always tracked.
		cur, _, _, est := ex.balanceAt(k, j)
		return est == nil && cur.Bytes32() == seen
	}
	if now == o {
		return true
	}
	if !ex.comparable(k, j) {
		return false
	}
	if o.tx >= 0 && !tracked {
		return false
	}
	if o.tx < 0 {
		seen = ex.store.baseField(k, ex.blockIndex[j])
	}
	if k.Kind == kindBalance && ok && v.val.Rel {
		// the field of a relative version is its delta, not a balance: a
		// sweep of the whole base balance has the base as its magnitude
		cur, _, _, est := ex.balanceAt(k, j)
		return est == nil && cur.Bytes32() == seen
	}
	return seen == ex.servedField(j, k, now, v.val)
}

// comparable reports whether j's reads of k can be validated by value.
func (ex *Executor) comparable(k Key, j int32) bool {
	switch k.Kind {
	case kindWipe:
		return false
	case kindStorage:
		if ex.wipes.Load() == 0 {
			return true // no account was deleted in this block (the common case)
		}
		return ex.store.firstWriter(Key{Addr: k.Addr, Kind: kindWipe}, j) >= j
	}
	return true
}

// readField is the field a recorded read observed.
func (ex *Executor) readField(j int32, k Key, o origin, vals map[Key]common.Hash) common.Hash {
	if o.tx < 0 {
		return ex.store.baseField(k, ex.blockIndex[j])
	}
	return vals[k]
}

// servedField is the field of k served from origin o (the pre-state if o.tx < 0).
func (ex *Executor) servedField(j int32, k Key, o origin, val Value) common.Hash {
	if o.tx < 0 {
		return ex.store.baseField(k, ex.blockIndex[j])
	}
	return fieldOf(k.Kind, val)
}

// sameField returns the predicate "this registered reader observed the field
// that val holds" for putVersionNode, or nil if k cannot be compared by value.
func (ex *Executor) sameField(k Key, val Value, sc *sameCheck) *sameCheck {
	if k.Kind == kindWipe {
		return nil
	}
	wiped := int32(ex.n)
	if k.Kind == kindStorage && ex.wipes.Load() > 0 {
		wiped = ex.store.firstWriter(Key{Addr: k.Addr, Kind: kindWipe}, wiped)
	}
	*sc = sameCheck{ex: ex, k: k, f: fieldOf(k.Kind, val), wiped: wiped}
	return sc
}

// sameCheck is the predicate "this registered reader observed the field a new
// version holds" (a struct rather than a closure: it is built per publication).
type sameCheck struct {
	ex    *Executor
	k     Key
	f     common.Hash
	wiped int32
	bases map[int32]common.Hash // pre-state field per reader block
}

func (c *sameCheck) ok(r readRec) bool {
	if r.tx > c.wiped {
		return false
	}
	if r.orig.tx >= 0 {
		return r.val == c.f
	}
	blk := c.ex.blockIndex[r.tx]
	base, ok := c.bases[blk]
	if !ok {
		if c.bases == nil {
			c.bases = make(map[int32]common.Hash, 2)
		}
		base = c.ex.store.baseField(c.k, blk)
		c.bases[blk] = base
	}
	return base == c.f
}

// creditFee credits the committing transaction's fee to its block's
// beneficiary. The fees of a block are summed outside the version store
// (feeSum) and written to it once, with the block's last transaction; until
// then a balance of the beneficiary resolved for a transaction of the block
// is the store's plus that sum (balanceAt). A version per transaction on the
// one key would make every credit copy the key's version list as soon as a
// higher transaction had published a change of that balance.
func (ex *Executor) creditFee(j int32, run *incarnation) {
	b := ex.blockIndex[j]
	bk := ex.blocks[b]
	k := Key{Addr: bk.Coinbase, Kind: kindBalance}
	acc := ex.feeSum[b].Load()
	for _, w := range run.writes {
		if w.key == k {
			// The transaction changed this balance itself. Its version is
			// an absolute value by now (written as one from the exact
			// balance it observed, or made one by materialize), with the
			// fees so far in it: they are in the store from here on.
			acc = nil
			break
		}
	}
	if acc == nil || !run.fee.IsZero() {
		sum := new(uint256.Int)
		if acc != nil {
			sum.Set(acc)
		}
		acc = sum.Add(sum, &run.fee)
		ex.feeSum[b].Store(acc)
	}
	if j < bk.Last {
		ex.feeAtFrontier[j+1] = acc // read once the frontier is there (feeLayer)
		return
	}
	if !acc.IsZero() {
		ex.putFee(j, run, k, acc)
	}
	ex.feeSum[b].Store(nil)
}

// putFee writes fee into the beneficiary's balance as a version of transaction
// j, on top of any change j made itself.
func (ex *Executor) putFee(j int32, run *incarnation, k Key, fee *uint256.Int) {
	bal, _, _, _ := ex.store.balanceBelowPlus(k, j+1, ex.blockIndex[j], fee)
	e := ex.store.getOrCreate(k)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.put(version{tx: j, inc: stamp(run.inc, feeSeq), val: Value{Balance: bal}})
	ex.markAccountWritten(k)
	if !containsKey(ex.written[j], k) {
		ex.written[j] = append(ex.written[j], k)
	}
}

// feeLayer returns the fees to add to the store's balance of k for a reader
// at j: those of j's block credited so far if k is that block's beneficiary
// (nil: none). Once the frontier has reached j it is the sum up to j exactly;
// before, whatever has been credited, a speculative view like any other.
func (ex *Executor) feeLayer(k Key, j int32) *uint256.Int {
	b := ex.blockIndex[j]
	if k.Kind != kindBalance || k.Addr != ex.blocks[b].Coinbase {
		return nil
	}
	if ex.committed.Load() >= j {
		return ex.feeAtFrontier[j]
	}
	return ex.feeSum[b].Load()
}

// balanceAt is store.balanceBelow for a reader at j with the beneficiary's
// uncredited fees added (feeLayer).
func (ex *Executor) balanceAt(k Key, j int32) (bal *uint256.Int, top version, hasTop bool, est *version) {
	return ex.store.balanceBelowPlus(k, j, ex.blockIndex[j], ex.feeLayer(k, j))
}

// pendingRead is a read the committer validates: its key and the origin it saw.
type pendingRead struct {
	k       Key
	o       origin
	val     common.Hash // the field the read observed, if tracked (the incarnation's vals)
	tracked bool
	unused  bool // an account field never observed: skipped
}

// pendingPool recycles the committer's lists of reads to validate: built on the
// finisher and read once by the committer, they would otherwise be allocated
// afresh and grown read by read for every transaction.
var pendingPool sync.Pool

func getPending(n int) []pendingRead {
	if p, ok := pendingPool.Get().(*[]pendingRead); ok && cap(*p) >= n {
		return (*p)[:0]
	}
	return make([]pendingRead, 0, max(n, 32))
}

// putPending returns a committed incarnation's list (the committer holds
// ex.mu; nothing reads the list after the commit).
func putPending(run *incarnation) {
	if cap(run.pending) > 0 {
		p := run.pending[:0]
		pendingPool.Put(&p)
		run.pending = nil
	}
}

// commitWork prepares, on the finisher, everything the serial commit loop
// would otherwise compute per transaction while touching the whole read map:
// the list of reads that still need validating at commit (the unregistered
// ones, plus account-field, predicted and boundary-value reads, see
// readsCurrent) and the relative writes. The committer then reads a short
// slice and a few fields of the incarnation, not structures other cores wrote.
func (ex *Executor) commitWork(j int32, run *incarnation) {
	if cap(run.pending) == 0 {
		run.pending = getPending(len(run.reads))
	}
	run.pending, run.pendingWipes, run.pendingBeneficiary = run.pending[:0], run.pendingWipes[:0], run.pendingBeneficiary[:0]
	for k, o := range run.reads {
		if k.Kind == kindStorage && o.inc != predictedInc && o.inc != boundaryInc {
			if _, ok := run.registered[k]; ok {
				continue
			}
		}
		// The entry carries the observed field and whether the field was
		// observed at all, so that the committer reads this slice and the
		// version store and none of the incarnation's maps.
		pr := pendingRead{k: k, o: o, unused: !usedField(run.used[k.Addr], k.Kind)}
		pr.val, pr.tracked = run.vals[k]
		// An account field loaded with the account and never observed is not
		// validated (the observed mask is final once the incarnation ran),
		// and a storage-wipe marker can only have changed if some account was
		// deleted in the window: the committer looks at those only then
		// (pendingCurrent).
		if k.Kind == kindWipe {
			run.pendingWipes = append(run.pendingWipes, pr)
			continue
		}
		if pr.unused {
			continue
		}
		if k.Kind == kindBalance && ex.coinbases[k.Addr] {
			run.pendingBeneficiary = append(run.pendingBeneficiary, pr)
		} else {
			run.pending = append(run.pending, pr)
		}
	}
	// the relative balance versions the committer makes absolute (materialize)
	run.relativeWrites = run.relativeWrites[:0]
	for i, w := range run.writes {
		if w.key.Kind == kindBalance && w.val.Rel {
			run.relativeWrites = append(run.relativeWrites, int32(i))
		}
	}
}

// pendingCurrent is readsCurrent over the reads commitWork left for the
// committer.
func (ex *Executor) pendingCurrent(j int32, run *incarnation) bool {
	return ex.pendingList(j, run.pending) && ex.pendingList(j, run.pendingBeneficiary) &&
		(ex.wipes.Load() == 0 || ex.pendingList(j, run.pendingWipes))
}

// pendingList validates a list of j's pending reads.
func (ex *Executor) pendingList(j int32, list []pendingRead) bool {
	for i := range list {
		pr := &list[i]
		if pr.unused {
			continue
		}
		if pr.o.tx < 0 && pr.k.Kind != kindStorage && !ex.accountMaybeWritten(pr.k) && ex.feeLayer(pr.k, j) == nil {
			continue // the pre-state of a field without a version in this window
		}
		if !ex.readCurrentField(j, pr.k, pr.o, pr.val, pr.tracked) {
			if timelineFile != nil {
				_, registered := ex.runs[j].registered[pr.k]
				ex.timeline(j, "V %d %d %d %d %s %d %t %t\n", ex.blockOf(j).Fixture.Number, j, ex.runs[j].inc, pr.k.Kind, pr.k.Addr.Hex(), pr.o.tx, registered, pr.k.Addr == ex.env.Messages[j].From)
			}
			return false
		}
	}
	return true
}

// sameValue compares the field of kind k held by two values.
func sameValue(k keyKind, a, b Value) bool {
	switch k {
	case kindBalance:
		return a.Balance != nil && b.Balance != nil && a.Balance.Eq(b.Balance)
	case kindStorage:
		return a.Slot == b.Slot
	}
	return false
}

// preRegister moves the commit-time validation of j's unregistered storage
// reads to the finisher while the commit loop is busy: each such read is
// checked against the current version under the entry's lock and, if
// still current, registered, so that the committer finds it covered by push
// (readsCurrent skips registered storage reads) and a later lower writer
// invalidates j directly. It returns false if a read is already stale (the
// incarnation is discarded, as the committer would have done). While the
// commit loop is idle the committer validates as before: the work moves to
// the finishers exactly when the serial loop is the bottleneck, and the
// shared-line writes it costs are paid only then.
func (ex *Executor) preRegister(j int32, run *incarnation) bool {
	if ex.lowerFinished(j) {
		return true // nothing below j can change what it read but an abort, which its commit finds
	}
	for k, o := range run.reads {
		if k.Kind != kindStorage || o.inc == predictedInc || o.inc == boundaryInc {
			continue
		}
		if _, ok := run.registered[k]; ok {
			continue
		}
		val, tracked := run.vals[k]
		switch ex.store.validateRegister(k, j, run.inc, o, val, tracked, ex.comparable(k, j)) {
		case 1:
			run.registered[k] = struct{}{}
		case -1:
			return false
		}
	}
	return true
}

// lowerFinished says whether every transaction below j has finished. A read
// registers only while a lower transaction has not finished: registering is
// how a lower writer that publishes later invalidates the reader at once, and
// once every lower transaction has published an executed incarnation, the
// value can only change if one of them is aborted and runs again, which the
// reader's validation at commit finds as well. preRegister follows the same
// rule: when an abort sends a transaction back, the front drops below the
// reader and the finisher validates and registers again.
func (ex *Executor) lowerFinished(j int32) bool { return j <= ex.finishedFront.Load() }

// advanceFinishedFront moves the finished front past the transactions that have
// finished. A race with lowerFinishedFront can leave it one past an unfinished
// transaction; that only costs a registration, the commit validates.
func (ex *Executor) advanceFinishedFront() {
	for {
		f := ex.finishedFront.Load()
		if int(f) >= ex.n || !ex.finished[f].Load() {
			return
		}
		if ex.finishedFront.CompareAndSwap(f, f+1) && !ex.finished[f].Load() {
			ex.lowerFinishedFront(f)
			return
		}
	}
}

// lowerFinishedFront moves the finished front back to f, whose executed
// incarnation was aborted.
func (ex *Executor) lowerFinishedFront(f int32) {
	for {
		cur := ex.finishedFront.Load()
		if cur <= f || ex.finishedFront.CompareAndSwap(cur, f) {
			return
		}
	}
}

// predicatesHold re-evaluates the transfer sufficiency checks of f against
// the committed balances (every lower transaction is committed).
func (ex *Executor) predicatesHold(f int32, run *incarnation) bool {
	for _, p := range run.predicates {
		k := Key{Addr: p.addr, Kind: kindBalance}
		bal, _, _, est := ex.balanceAt(k, f)
		if est != nil {
			return false // only ahead of the commit (prevalidate.go): a lower transaction aborted
		}
		if p.own != nil {
			bal = new(uint256.Int).Set(bal)
			if p.ownNeg {
				if bal.Lt(p.own) {
					bal.Clear()
				} else {
					bal.Sub(bal, p.own)
				}
			} else {
				bal.Add(bal, p.own)
			}
		}
		if (bal.Cmp(p.amount) >= 0) != p.ok {
			return false
		}
	}
	return true
}
