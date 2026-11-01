// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import "github.com/holiman/uint256"

// Materialize, which turns a committed transaction's relative balance
// versions into absolute ones, runs off the serial commit loop. Every reader
// of a balance sums the relative versions above the newest absolute one below
// it (store.balanceBelow, balanceAt), so a relative version left for a while
// reads the same: the readers, the committer's validation and predicates, and
// the fee credit see the same balance either way, over a chain a little longer.
//
// The committed transactions are materialized in order by the finishers:
// after its commit attempt, a finisher claims the committed transactions not
// yet materialized, one by one (materializeNext), until it reaches the frontier. Three
// things need the absolute versions and get them synchronously:
//   - a block's beneficiary: creditFee takes the fees so far as being in the
//     store from the version of a transaction that changed the beneficiary's
//     balance, so the committer still materializes those itself;
//   - a block's results (the result view reads the newest version's balance)
//     and the next block's boundary corrections (rebase compares the newest
//     version below the boundary with the snapshot): at a block's last
//     commit the committer finishes what the finishers have not
//     (drainMaterialize), usually nothing.

// helpMaterialize materializes, on a finisher, the committed transactions that
// are not yet, in order of claim, until it reaches the frontier.
func (ex *Executor) helpMaterialize() {
	for {
		i := ex.materializeNext.Load()
		if i >= ex.committed.Load() {
			return
		}
		if !ex.materializeNext.CompareAndSwap(i, i+1) {
			continue
		}
		ex.materializeOne(i)
	}
}

// materializeOne materializes committed transaction i (claimed) and moves
// materializedUpTo past every transaction materialized in a row.
func (ex *Executor) materializeOne(i int32) {
	if run := ex.runs[i]; run != nil && len(run.relativeWrites) > 0 {
		ex.materializeRest(i, run)
	}
	ex.materializedFlag[i].Store(true)
	ex.advanceMatDone()
}

// advanceMatDone moves materializedUpTo past every transaction materialized in a row.
func (ex *Executor) advanceMatDone() {
	for {
		d := ex.materializedUpTo.Load()
		if int(d) >= ex.n || !ex.materializedFlag[d].Load() {
			return
		}
		ex.materializedUpTo.CompareAndSwap(d, d+1) // whoever moves it, look again
	}
}

// drainMaterialize makes sure every transaction up to f (committed) is
// materialized: the committer does what is not yet done itself, also what a
// finisher has claimed and not finished (ex.mu held; materializing takes no
// lock but the entries'). Materializing a committed transaction again is
// harmless: every version below it is final, so it writes the same absolute
// value, and waiting for the claims in progress would tie the block's end to a
// claimant that may be descheduled or queued on an entry lock.
func (ex *Executor) drainMaterialize(f int32) {
	if ex.materializedUpTo.Load() > f {
		return
	}
	for {
		i := ex.materializeNext.Load()
		if i > f || ex.materializeNext.CompareAndSwap(i, f+1) {
			break // the finishers claim nothing up to f from here on
		}
	}
	for i := ex.materializedUpTo.Load(); i <= f; i++ {
		if !ex.materializedFlag[i].Load() {
			if run := ex.runs[i]; run != nil && len(run.relativeWrites) > 0 {
				ex.materializeRest(i, run)
			}
			ex.materializedFlag[i].Store(true)
		}
	}
	ex.advanceMatDone()
}

// materialize turns the relative balance versions of the blocks' beneficiaries
// that the committed transaction f wrote into absolute ones (every lower
// version is final now); the rest is done off the commit loop.
func (ex *Executor) materialize(f int32, run *incarnation) { ex.materializeSome(f, run, true) }

// materializeRest turns f's remaining relative balance versions (all but the
// beneficiaries') into absolute ones.
func (ex *Executor) materializeRest(f int32, run *incarnation) { ex.materializeSome(f, run, false) }

func (ex *Executor) materializeSome(f int32, run *incarnation, coinbases bool) {
	for _, i := range run.relativeWrites { // prepared by the finisher: the committer does not walk the write set
		w := run.writes[i]
		if ex.coinbases[w.key.Addr] != coinbases {
			continue
		}
		// (for the block's beneficiary with its fees so far: creditFee
		// takes them as being in the store from this version on)
		bal, _, _, _ := ex.balanceAt(w.key, f)
		abs := new(uint256.Int).Set(bal)
		if w.val.Neg {
			if abs.Lt(w.val.Balance) {
				abs.Clear()
			} else {
				abs.Sub(abs, w.val.Balance)
			}
		} else {
			abs.Add(abs, w.val.Balance)
		}
		e := ex.store.getOrCreate(w.key)
		e.mu.Lock()
		e.put(version{tx: f, inc: run.final, val: Value{Balance: abs}})
		e.mu.Unlock()
	}
}
