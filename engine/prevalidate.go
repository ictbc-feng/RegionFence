// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"encoding/binary"
	"math/bits"
	"sync/atomic"
)

// Validation ahead of the commit. The commit loop mostly confirms, in bursts
// of transactions that ran while the frontier transaction was missing, and its
// validation rarely fails. Helpers confirm those transactions ahead of the
// committer.
//
// A check of j done at generation g holds at j's commit if no key it depends
// on changed since. Every change of a key's versions (a publication, a
// release, a removal, a label) draws a new generation into keyGeneration, by the
// key's hash, after it is visible, and every change below j is noted before
// j's commit: the changes of a transaction end before it is executed. A check
// that read generation g before a change saw at worst the older state and
// g < the change's: it is refused. A check that read it after saw the change.
// Changes of other keys are told apart by the hash and most changes above j
// by the helpers' cursor (noteChange). What is left of both, a collision or a
// change between j and the cursor, only costs the commit its own validation.
// A change of a wipe key, which decides how storage reads compare, and a
// boundary correction concern every key (changeAll). Materialize keeps every sum.
// What the committer itself changes as it commits, the fee layer of the
// beneficiaries, is left to it: reads of a beneficiary's balance (pendingBeneficiary) and
// predicates on one are never checked ahead.
//
// A finisher checking its own transaction would do so while the transactions
// below it still run. So the check waits until everything below j has
// executed: a helper stops at the first transaction that has not, and the
// frontier transaction's publication, which refuses all that was checked
// before it, is what starts it.

// validationHelpers is the number of helpers: the committer's loop is mostly the
// checks, and two keep ahead of what is left of it.
const validationHelpers = 2

// What a helper did with an incarnation (incarnation.aheadState).
const (
	aheadIdle = iota
	aheadBusy
	aheadOK
	aheadDone // refused, or the committer has taken the incarnation
)

// keyGenerationSize is the size of keyGeneration, a power of 2.
const keyGenerationSize = 1 << 16

func keyGenerationSlot(k Key) uint64 {
	h := binary.LittleEndian.Uint64(k.Addr[12:]) ^ bits.RotateLeft64(binary.LittleEndian.Uint64(k.Slot[24:]), 21) ^ uint64(k.Kind)
	return h * 0x9e3779b97f4a7c15 >> (64 - 16)
}

// storeMax raises v to g: generations are drawn and stored apart.
func storeMax(v *atomic.Int64, g int64) {
	for {
		old := v.Load()
		if old >= g || v.CompareAndSwap(old, g) {
			return
		}
	}
}

// noteChange draws a generation for j's change of k's versions, now visible.
// A change at or above the helpers' cursor concerns no check made so far: the
// check of anything above it starts after the cursor passes j, so it sees the
// change.
func (ex *Executor) noteChange(j int32, k Key) {
	if ex.keyGeneration == nil || j >= ex.aheadNext.Load() {
		return
	}
	g := ex.changeGeneration.Add(1)
	if k.Kind == kindWipe {
		storeMax(&ex.changeAll, g)
		return
	}
	storeMax(&ex.keyGeneration[keyGenerationSlot(k)], g)
}

// unchangedSince says whether nothing run's check at generation g depends on
// has changed since.
func (ex *Executor) unchangedSince(run *incarnation, g int64) bool {
	if ex.changeAll.Load() > g {
		return false
	}
	for i := range run.pending {
		if ex.keyGeneration[keyGenerationSlot(run.pending[i].k)].Load() > g {
			return false
		}
	}
	for i := range run.pendingWipes {
		if ex.keyGeneration[keyGenerationSlot(run.pendingWipes[i].k)].Load() > g {
			return false
		}
	}
	if run.aheadPredicates {
		for _, p := range run.predicates {
			if ex.keyGeneration[keyGenerationSlot(Key{Addr: p.addr, Kind: kindBalance})].Load() > g {
				return false
			}
		}
	}
	return true
}

// wanted says whether a helper should run: on a spare processor, or on a
// worker's when the committer is behind. With every worker busy, a helper
// costs a worker's output, and that is worth nothing only once the executed
// transactions ahead of the committer span a whole round of the workers.
func (ex *Executor) wanted() bool {
	if ex.chainIdle(spinners.Load() + 1) {
		return true
	}
	t := int(ex.committed.Load()) + ex.scheduler.workers
	return t < ex.n && ex.executed(int32(t))
}

// validator is a helper's loop for the window: while wanted it checks what has
// become checkable, and it polls for more on a spare processor. Otherwise it
// sleeps, and the committer checks for itself.
func (ex *Executor) validator() {
	for {
		select {
		case <-ex.done:
			return
		default:
		}
		if ex.wanted() && ex.helpValidate() {
			continue
		}
		if n := spinners.Add(1); !ex.chainIdle(n) {
			spinners.Add(-1)
			select {
			case <-ex.aheadKick:
			case <-ex.done:
				return
			}
			continue
		}
		for i := 0; i < 1024; i++ {
			if _, _, ok := ex.nextCheck(); ok {
				break
			}
		}
		spinners.Add(-1)
	}
}

// executed says whether t has executed or committed.
func (ex *Executor) executed(t int32) bool {
	s := ex.status[t].get()
	return s == statusExecuted || s == statusCommitted
}

// nextCheck is the lowest transaction above the frontier no helper has
// claimed, with the cursor it was found at, if it can be checked now: it and
// the frontier transaction have executed.
func (ex *Executor) nextCheck() (i, cursor int32, ok bool) {
	f := ex.committed.Load()
	if int(f) >= ex.n || !ex.executed(f) {
		return 0, 0, false
	}
	cursor = ex.aheadNext.Load()
	i = max(cursor, f+1)
	if int(i) >= ex.n || ex.status[i].get() != statusExecuted {
		return 0, 0, false
	}
	return i, cursor, true
}

// helpValidate checks one transaction ahead and reports whether it found one.
func (ex *Executor) helpValidate() bool {
	i, cursor, ok := ex.nextCheck()
	if !ok {
		return false
	}
	if !ex.aheadNext.CompareAndSwap(cursor, i+1) {
		return true
	}
	run, ok := ex.executedRun(i)
	if !ok || !run.aheadState.CompareAndSwap(aheadIdle, aheadBusy) {
		return true
	}
	g := ex.changeGeneration.Load()
	st := int32(aheadDone)
	if ex.checkAhead(i, run) {
		st = aheadOK
		run.aheadGeneration = g
	}
	run.aheadState.CompareAndSwap(aheadBusy, st)
	return true
}

// checkAhead validates j's reads and, unless one is on a beneficiary, its
// predicates. A stale read is left for the commit to find: it decides.
func (ex *Executor) checkAhead(j int32, run *incarnation) bool {
	for i := range run.pending {
		pr := &run.pending[i]
		if pr.unused || (pr.o.tx < 0 && pr.k.Kind != kindStorage && !ex.accountMaybeWritten(pr.k)) {
			continue
		}
		if !ex.readCurrentField(j, pr.k, pr.o, pr.val, pr.tracked) {
			return false
		}
	}
	if ex.wipes.Load() > 0 {
		for i := range run.pendingWipes {
			pr := &run.pendingWipes[i]
			if !ex.readCurrentField(j, pr.k, pr.o, pr.val, pr.tracked) {
				return false
			}
		}
	}
	for _, p := range run.predicates {
		if ex.coinbases[p.addr] {
			return true
		}
	}
	run.aheadPredicates = true
	return ex.predicatesHold(j, run)
}

// checkedAhead says whether f's commit can use a check done ahead; it takes
// the incarnation from the helpers. busy: one may still be reading it.
func (ex *Executor) checkedAhead(run *incarnation) (ok, busy bool) {
	if ex.keyGeneration == nil {
		return false, false
	}
	switch run.aheadState.Swap(aheadDone) {
	case aheadBusy:
		return false, true
	case aheadOK:
		return ex.unchangedSince(run, run.aheadGeneration), false
	}
	return false, false
}

// kickValidators wakes the helpers that had no processor to poll on, if they
// are wanted: the frontier transaction has finished, or one a round ahead.
func (ex *Executor) kickValidators() {
	if !ex.wanted() {
		return
	}
	for range validationHelpers {
		select {
		case ex.aheadKick <- struct{}{}:
		default:
			return
		}
	}
}
