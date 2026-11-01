// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"sync"
	"sync/atomic"
	"time"
)

// The adaptive window. A sliding window of W blocks: the blocks from the
// commit frontier's block up to W-1 past it may run; the next block waits
// until the frontier's block moves. The harness hands over one range with no
// barrier, and W is decided at the end of every block by what one block more
// or one block less would have changed while the block committed, both as
// wall time:
//
//   - one more (the window was too shallow): the slot time left free while
//     the bound alone held back queued work (scheduler.boundIdle), over the
//     workers, at the share of the outermost block's first runs that
//     committed as they ran. That much more work would have been done.
//   - one less (too deep): the time the chain of commits waited on a
//     transaction running again whose first run was in the outermost block
//     of the window when it started. Without that block it would not have
//     run that early. Re-runs of transactions that ran nearer the frontier
//     happen at any width and do not count.
//
// W moves by one block toward the larger, between one block and the range,
// and stays when no block is left beyond the window. One block less also
// saves what the outermost block adds to every run's time and to the
// re-execution waits anywhere in the window (widthFit). The width carries over
// to the next range.
const initialWidth = 16

var lastWidth atomic.Int32

// widthBoundLocked is the run-ahead bound that admits the W blocks from
// the frontier's block.
func (ex *Executor) widthBoundLocked() int32 {
	if int(ex.frontier) >= ex.n {
		return max(ex.runAheadBound, 1)
	}
	edge := int32(ex.n)
	if e := int(ex.blockIndex[ex.frontier]) + int(ex.width); e < len(ex.blocks) {
		edge = ex.blocks[e].First
	}
	return max(edge-ex.frontier, 1)
}

// applyWidthLocked moves the bound to the current width (after a commit).
func (ex *Executor) applyWidthLocked() {
	ex.setRunAheadBound(ex.widthBoundLocked())
	ex.scheduler.setLimit(ex.frontier + ex.runAheadBound) // the next block's transactions may run
}

// noteFirstRun notes, at the first start of j, whether j's block was the
// outermost of the window then.
func (ex *Executor) noteFirstRun(j int32) {
	ahead := ex.blockIndex[j] - ex.blockIndex[min(ex.committed.Load(), int32(ex.n)-1)]
	ex.outerFirstRun[j].Store(ahead >= 1 && ahead >= ex.widthShared.Load()-1)
}

// chargeCommitLocked charges the wait before the commit of f, and at the
// end of f's block decides the width: one block more is worth the slot time
// left idle by the bound (in wall time, as far as outermost first runs hold);
// one block less saves the waits on re-executions of outermost first runs,
// what the outermost block adds to every run's time (charged on the
// interval's waits, which are runs) and what it adds to the waits on
// re-executions anywhere in the window. The last two are the slopes, over
// the admitted span, that widthFit learns from the block-end intervals.
func (ex *Executor) chargeCommitLocked(f int32) {
	now := time.Now().UnixNano()
	outer, re := ex.outerFirstRun[f].Load(), ex.inc[f].Load() > 0
	if stall := now - ex.lastCommitNs; ex.lastCommitNs > 0 && stall > 0 {
		ex.intervalStallNs += stall
		if re {
			ex.intervalRerunStallNs += stall
			if outer {
				ex.deepNs += stall
			}
		}
	}
	ex.lastCommitNs = now
	ex.intervalCommits++
	ex.intervalSpan += int64(ex.runAheadBound)
	if outer {
		if ex.outerRuns++; re {
			ex.outerReruns++
		}
		if ex.outerRuns >= 256 {
			ex.outerRuns, ex.outerReruns = ex.outerRuns/2, ex.outerReruns/2
		}
	}
	bk := ex.blockOf(f)
	if f != bk.Last {
		return
	}
	bound := ex.scheduler.boundIdleTime().Nanoseconds()
	ok := float64(ex.outerRuns-ex.outerReruns+1) / float64(ex.outerRuns+2)
	ex.shallowNs = int64(float64(bound-ex.boundIdleSeen) / float64(ex.scheduler.workers) * ok)
	ex.boundIdleSeen = bound

	// the interval's point for the fits, and the outermost block's share
	nIv := float64(ex.intervalCommits)
	span := float64(ex.intervalSpan) / nIv
	runN, runNs := ex.intervalRuns.Swap(0), ex.intervalRunNs.Swap(0)
	run := 0.0
	if runN > 0 {
		run = float64(runNs) / float64(runN)
	}
	reWait := float64(ex.intervalRerunStallNs) / nIv
	e := int(ex.blockIndex[f]) + 1 + int(ex.width) // the block one more would admit
	outerTx := float64(bk.Last - bk.First + 1)
	if e < len(ex.blocks) {
		outerTx = float64(ex.blocks[e].Last - ex.blocks[e].First + 1)
	}
	slowNs, reNs := widthFit.cost(span, run, reWait, outerTx, float64(ex.intervalStallNs), nIv)
	deep := ex.deepNs + slowNs + reNs
	switch {
	case e >= len(ex.blocks):
	case ex.shallowNs > deep:
		ex.width = min(ex.width+1, int32(len(ex.blocks)))
	case deep > ex.shallowNs:
		ex.width = max(ex.width-1, 1)
	}
	ex.shallowNs, ex.deepNs = 0, 0
	ex.intervalCommits, ex.intervalStallNs, ex.intervalRerunStallNs, ex.intervalSpan = 0, 0, 0, 0
	ex.stats.WindowWidth = append(ex.stats.WindowWidth, ex.width)
	ex.widthShared.Store(ex.width)
	lastWidth.Store(ex.width)
}

// lineFit is a least-squares line fitted with exponential forgetting.
type lineFit struct{ w, x, y, xx, xy float64 }

// fitForgetting is the weight each new point takes from the old ones.
const fitForgetting = 1.0 / 32

func (r *lineFit) add(x, y float64) {
	k := 1 - fitForgetting
	r.w, r.x, r.y, r.xx, r.xy = r.w*k+1, r.x*k+x, r.y*k+y, r.xx*k+x*x, r.xy*k+x*y
}

// slope is the fitted slope, 0 until the points are enough and spread, and
// never below 0 (a wider span is never taken to make runs cheaper).
func (r *lineFit) slope() float64 {
	d := r.w*r.xx - r.x*r.x
	if r.w < 4 || d <= 1e-9*r.w*r.xx {
		return 0
	}
	return max(0, (r.w*r.xy-r.x*r.y)/d)
}

// widthFits are the slopes over the admitted span (transactions) of a first
// run's time (ns) and of the wait per commit on re-executions (ns),
// process-wide.
type widthFits struct {
	mu      sync.Mutex
	run, re lineFit
}

var widthFit widthFits

// cost adds the interval's point and prices what the outermost block adds:
// the runs it slows (as a share of a run, on the interval's waits) and the
// waits on re-executions it adds over the interval's commits.
func (a *widthFits) cost(span, run, reWait, outerTx, waits, commits float64) (slowNs, reNs int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if run > 0 {
		a.run.add(span, run)
	}
	a.re.add(span, reWait)
	if run > 0 {
		slowNs = int64(min(1, a.run.slope()*outerTx/run) * waits)
	}
	reNs = int64(a.re.slope() * outerTx * commits)
	return slowNs, reNs
}

// widthState is a copy of the width's process-wide state, for a harness that
// runs a window several times from the same state (as chainState).
type widthState struct {
	width   int32
	run, re lineFit
}

// saveWidth copies the width's state.
func saveWidth() *widthState {
	t := new(widthState)
	t.width = lastWidth.Load()
	widthFit.mu.Lock()
	t.run, t.re = widthFit.run, widthFit.re
	widthFit.mu.Unlock()
	return t
}

// Restore puts the copy back.
func (t *widthState) Restore() {
	if t == nil {
		return
	}
	lastWidth.Store(t.width)
	widthFit.mu.Lock()
	widthFit.run, widthFit.re = t.run, t.re
	widthFit.mu.Unlock()
}
