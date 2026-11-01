// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"math"
	"sync/atomic"
)

// The cost of a decision of the concurrency control, learned online.
// A decision bets on an event of learned probability q and weighs what each
// outcome costs: acting costs A when the event does not happen, not acting
// costs B when it does, so acting pays exactly when q >= A / (A + B). A and B
// are measured on this machine, at this core count, on this workload, folded
// in at the end of each window (process-wide, an exponential average over
// the windows); q comes from a per-key estimator. Before a window has
// measured both costs, the decision uses the even prior q >= 1/2.
//
// Costs are slot time weighed by the window's idle share u: with every slot
// busy a wait costs little and a re-execution its whole run; with most slots
// idle a wait costs nearly its duration. The decisions taken this way: the
// chain's release of a hot key at its learned last write (learnChain), a
// fence's wait for a named writer (waitThreshold, learnWait), the ESTIMATE label
// on the keys of an aborted incarnation, the failed key included
// (learnEstimate), the invalidation of their readers at the abort
// (learnCascade), and the wait on a volatile balance's chain
// (learnVolatile).
type costModel struct {
	chain    atomic.Uint64 // float64 bits of the learned A / (A + B); 0: nothing measured yet
	estimate atomic.Uint64 // the same for labelling a key of an aborted incarnation ESTIMATE (learnEstimate)
	wait     atomic.Uint64 // the same for waiting at a fence for a named writer (learnWait, waitThreshold)
	volatile atomic.Uint64 // the same for waiting on a volatile balance's chain (learnVolatile)
	cascade  atomic.Uint64 // the same for invalidating a labelled key's readers at the abort (learnCascade)
}

var costs costModel

func loadThreshold(v *atomic.Uint64) float64 {
	if b := v.Load(); b != 0 {
		return math.Float64frombits(b)
	}
	return 0.5
}

// foldScaled folds A / (A + B) / scale, at most 1 (never act).
func foldScaled(v *atomic.Uint64, a, b, scale float64) {
	if a < 0 || b <= 0 || scale <= 0 {
		return
	}
	t := min(a/(a+b)/scale, 1)
	if old := v.Load(); old != 0 {
		t = 0.75*math.Float64frombits(old) + 0.25*t
	}
	v.Store(math.Float64bits(max(t, math.SmallestNonzeroFloat64))) // 0 bits mean nothing measured
}

// sketchSure is the share a per-key sketch gives a key whose every remembered
// outcome was the event: they forget by halving at 64, so at least 33 in 34.
// A learned threshold stops there: the surest keys still act, which keeps
// measuring the costs, so that the threshold comes back down when the costs
// change (another core count, another workload).
const sketchSure = 33.0 / 34

// When a learned threshold reaches sketchSure a window acts at most
// probeBudget times on the decision, so that its costs stay measured.
const probeBudget = 16

const (
	probeChain = iota
	probeEstimate
	probeCascade
	probeDecisions
)

// probe says whether the decision may act on a key that passes the
// threshold: always while acting can pay, within the window's budget once
// the costs say it never does.
func (ex *Executor) probe(i int, v *atomic.Uint64) bool {
	if loadThreshold(v) < sketchSure {
		return true
	}
	return ex.probes[i].Add(-1) >= 0
}

// idleShare is the share of the window's slot time that had nothing to run
// (u above).
func idleShare(st *Stats, workers int) float64 {
	if workers <= 0 || st.Wall <= 0 {
		return 1
	}
	return min(max(float64(st.IdleNs)/(float64(workers)*float64(st.Wall.Nanoseconds())), 0), 1)
}

// foldCosts folds in the window's measurements.
func (ex *Executor) foldCosts(st *Stats) {
	u := idleShare(st, ex.scheduler.workers)
	if ex.chainOn {
		learnChain(st, u)
	}
	learnEstimate(st, u)
	learnCascade(st, u)
	learnWait(st, u)
	learnVolatile(st, u)
}
