// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

// Learned is what the executor carries from one window to the next: the key
// signature table, the hot keys' write counts and the adaptive width. A
// benchmark that runs the same window several times restores it before
// each run, so that every run starts from what was learned before the
// window and the last run's state is what the next window starts from.
type Learned struct {
	signatures                        *SignatureTable
	chain                             *chainState
	width                             *widthState
	estimate, wait, volatile, cascade uint64 // costs
	rewrites, failed, balances        *rateState
}

// SaveLearned captures the current cross-window state.
func SaveLearned() *Learned {
	return &Learned{signatures: Signatures, chain: saveChain(), width: saveWidth(),
		estimate: costs.estimate.Load(), wait: costs.wait.Load(), volatile: costs.volatile.Load(), cascade: costs.cascade.Load(),
		rewrites: rewriteRates.save(), failed: failedRates.save(), balances: volatileBalances.save()}
}

// Restore makes the captured state current again (the signature table as a
// copy, so that the capture stays intact for the next run).
func (l *Learned) Restore() {
	if l.signatures != nil {
		Signatures = l.signatures.Clone()
	}
	l.chain.Restore()
	l.width.Restore()
	costs.estimate.Store(l.estimate)
	costs.wait.Store(l.wait)
	costs.volatile.Store(l.volatile)
	costs.cascade.Store(l.cascade)
	rewriteRates.restore(l.rewrites)
	failedRates.restore(l.failed)
	volatileBalances.restore(l.balances)
}
