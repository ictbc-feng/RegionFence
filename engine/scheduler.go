// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"container/heap"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// scheduler hands P worker slots to transactions. A slot holder runs one
// transaction at a time and, when it finishes, picks the next one itself, so
// dispatching a transaction does not wake another goroutine. Only a transaction
// that blocks inside its execution (a fence, an ESTIMATE, the frontier) gives
// its slot away, and competes for one again when it resumes.
//
// Queued transactions are dispatched lowest index first.
//
// slotLock guards the scheduler and the worker pool. Every transaction takes
// each at least twice and holds it for some tens of nanoseconds. A sync.Mutex
// parks a goroutine that does not get the lock after a short spin, and waking
// it again goes through the Go scheduler, which becomes the bottleneck at high
// core counts. This lock polls instead and offers its processor to others only
// once it has polled for long (a holder that lost its processor).
//
// Under the race detector it is always a sync.Mutex. The detector turns every
// atomic access into an operation on a lock of its own, and a dozen waiters
// polling one word keep that lock from the holder's releasing store. What the
// race tests check is the scheduler's data under the lock, and that is the
// same with either.
type slotLock struct {
	v  atomic.Int32
	mu sync.Mutex
	_  [48]byte
}

var schedMutex = raceBuild

// spinDelay busies the caller for about n iterations without touching
// shared memory.
func spinDelay(n int) int {
	x := 0
	for i := 0; i < n; i++ {
		x += i
	}
	return x
}

func (l *slotLock) Lock() {
	if schedMutex {
		l.mu.Lock()
		return
	}
	if l.v.CompareAndSwap(0, 1) {
		return
	}
	// A waiter that fails to take the lock waits a doubling number of empty
	// iterations before it looks again (up to about a microsecond), and
	// offers its processor only once it has waited a millisecond.
	t0 := time.Now()
	d := 8
	for i := 1; ; i++ {
		if l.v.Load() == 0 && l.v.CompareAndSwap(0, 1) {
			return
		}
		spinDelay(d)
		if d < 1024 {
			d <<= 1
		}
		if i&63 == 0 && time.Since(t0) > time.Millisecond {
			runtime.Gosched() // the holder lost its processor
		}
	}
}

func (l *slotLock) Unlock() {
	if schedMutex {
		l.mu.Unlock()
		return
	}
	l.v.Store(0)
}

type scheduler struct {
	mu      slotLock
	free    int
	freeA   atomic.Int32 // free, for readers without mu
	workers int
	waiters waiterHeap // resumed transactions waiting for a slot
	queued  []bool     // transactions waiting to (re-)execute
	lo      int32      // no queued transaction below lo
	hi      int32      // no queued transaction at or above hi
	nq      int
	spawn   func(j int32) // starts a worker (holding a slot) on j
	// Bounded run-ahead: no transaction at or above limit is dispatched
	// (noLimit: unbounded). Slots that would run one idle until the limit
	// rises (kick). The limit is an index, not a distance from the frontier:
	// it moves when the window's edge does, so a commit that leaves it in
	// place takes no lock here (it took this lock, the one every worker
	// queues on, for every transaction).
	limit     atomic.Int32
	idle      time.Duration // integral of free slots over time
	boundIdle time.Duration // ... while work existed that the pipeline's depth held back: queued beyond the bound, or in a block not yet admitted (more)
	more      func() bool   // reports whether blocks remain to be admitted; nil = none
	last      time.Time
	// onDrained, if set, is called (under mu, must not block) whenever a
	// dispatch empties the queue: a window admits its next block then.
	onDrained func()
}

// tickLocked accounts the free slots up to now; call it before changing free.
func (s *scheduler) tickLocked() {
	now := time.Now()
	if !s.last.IsZero() {
		d := time.Duration(s.free) * now.Sub(s.last)
		s.idle += d
		if s.free > 0 && s.limit.Load() != noLimit && ((s.nq > 0 && s.lowestLocked() >= s.limit.Load()) || (s.nq == 0 && s.more != nil && s.more())) {
			s.boundIdle += d
		}
	}
	s.last = now
}

// boundIdleTime returns the slot time left free while the run-ahead bound
// alone held back queued transactions (the cost of too little run-ahead).
func (s *scheduler) boundIdleTime() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickLocked()
	return s.boundIdle
}

// idleTime returns the slot time spent free so far.
func (s *scheduler) idleTime() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickLocked()
	return s.idle
}

func newScheduler(workers, n int, spawn func(int32)) *scheduler {
	s := &scheduler{free: workers, workers: workers, queued: make([]bool, n), spawn: spawn}
	s.freeA.Store(int32(workers))
	s.limit.Store(noLimit)
	return s
}

// setFree changes the number of free slots (mu held) and its lock-free mirror.
func (s *scheduler) setFree(n int) {
	s.free = n
	s.freeA.Store(int32(n))
}

// push queues transaction j, starting a worker right away if a slot is free.
func (s *scheduler) push(j int32) {
	s.mu.Lock()
	if s.free > 0 && len(s.waiters) == 0 && j < s.limit.Load() {
		s.tickLocked()
		s.setFree(s.free - 1)
		s.mu.Unlock()
		s.spawn(j)
		return
	}
	s.queued[j] = true
	s.nq++
	if j < s.lo {
		s.lo = j
	}
	if j >= s.hi {
		s.hi = j + 1
	}
	s.mu.Unlock()
}

// pushRange queues transactions [lo, hi) and starts workers on free slots.
func (s *scheduler) pushRange(lo, hi int32) {
	s.mu.Lock()
	for j := lo; j < hi; j++ {
		if !s.queued[j] {
			s.queued[j] = true
			s.nq++
		}
	}
	if lo < s.lo {
		s.lo = lo
	}
	if hi > s.hi {
		s.hi = hi
	}
	var start []int32
	s.tickLocked()
	for s.free > 0 && s.dispatchableLocked() {
		s.setFree(s.free - 1)
		start = append(start, s.popLocked())
	}
	s.mu.Unlock()
	for _, j := range start {
		s.spawn(j)
	}
}

// noLimit is the run-ahead limit of an unbounded scheduler.
const noLimit = math.MaxInt32

// setLimit moves the run-ahead limit and dispatches what a higher limit
// releases.
func (s *scheduler) setLimit(l int32) {
	if old := s.limit.Swap(l); l > old {
		s.kick()
	}
}

// dispatchable reports whether the lowest queued transaction is within the
// run-ahead bound.
func (s *scheduler) dispatchableLocked() bool {
	if s.nq == 0 {
		return false
	}
	return s.lowestLocked() < s.limit.Load()
}

// kick dispatches queued transactions that the run-ahead bound released.
func (s *scheduler) kick() {
	if s.limit.Load() == noLimit {
		return
	}
	s.mu.Lock()
	var start []int32
	for s.free > 0 && len(s.waiters) == 0 && s.dispatchableLocked() {
		s.tickLocked()
		s.setFree(s.free - 1)
		start = append(start, s.popLocked())
	}
	s.mu.Unlock()
	for _, j := range start {
		s.spawn(j)
	}
}

// popLocked removes and returns the queued transaction to dispatch next.
func (s *scheduler) popLocked() int32 {
	pick := s.lowestLocked()
	s.queued[pick] = false
	s.nq--
	if s.nq == 0 && s.onDrained != nil {
		s.onDrained()
	}
	return pick
}

// next is called by a slot holder that finished its transaction. It returns the
// next transaction to run, or false if the slot went to a resumed transaction
// with a lower index or there is nothing to do.
func (s *scheduler) next() (int32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiterFirstLocked() {
		w := heap.Pop(&s.waiters).(waiter)
		close(w.ch)
		return 0, false
	}
	if !s.dispatchableLocked() {
		s.tickLocked()
		s.setFree(s.free + 1)
		return 0, false
	}
	return s.popLocked(), true
}

// waiterFirstLocked reports whether a slot that comes free goes to the lowest
// resumed transaction rather than to the queue: it is below everything
// queued, or nothing queued may be dispatched (the queue is empty or held
// back by the run-ahead bound). A slot is never left free while a resumed
// transaction waits for one: nothing but a later hand-over would serve it,
// and with every worker idle (the frontier is driven by the committer, not
// by a slot holder) there would be none, nor would kick dispatch past it.
func (s *scheduler) waiterFirstLocked() bool {
	return len(s.waiters) > 0 && (s.nq == 0 || s.waiters[0].idx < s.lowestLocked() || !s.dispatchableLocked())
}

func (s *scheduler) lowestLocked() int32 {
	for !s.queued[s.lo] {
		s.lo++
	}
	return s.lo
}

// release gives the slot of a transaction that is about to block to the lowest
// resumed transaction or queued one.
func (s *scheduler) release() {
	s.mu.Lock()
	if s.waiterFirstLocked() {
		w := heap.Pop(&s.waiters).(waiter)
		s.mu.Unlock()
		close(w.ch)
		return
	}
	if s.dispatchableLocked() {
		j := s.popLocked()
		s.mu.Unlock()
		s.spawn(j)
		return
	}
	s.tickLocked()
	s.setFree(s.free + 1)
	s.mu.Unlock()
}

// acquire waits for a slot for a transaction that resumes after blocking.
func (s *scheduler) acquire(idx int32) {
	s.mu.Lock()
	if s.free > 0 {
		s.tickLocked()
		s.setFree(s.free - 1)
		s.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	heap.Push(&s.waiters, waiter{idx, ch})
	s.mu.Unlock()
	<-ch
}

type waiter struct {
	idx int32
	ch  chan struct{}
}

type waiterHeap []waiter

func (h waiterHeap) Len() int           { return len(h) }
func (h waiterHeap) Less(i, j int) bool { return h[i].idx < h[j].idx }
func (h waiterHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *waiterHeap) Push(x any)        { *h = append(*h, x.(waiter)) }
func (h *waiterHeap) Pop() any {
	old := *h
	w := old[len(old)-1]
	*h = old[:len(old)-1]
	return w
}

// workerPool runs transactions on a bounded set of long-lived goroutines
// instead of starting one per dispatch. A goroutine that runs out of work
// parks in the pool with its (grown) stack and is handed the next transaction
// by the dispatcher; a fresh goroutine is started only when the pool is empty.
// With GODEBUG=gcshrinkstackoff=1 parked stacks keep their size, so the EVM's
// deep call chains stop paying stack growth on every transaction.
type workerPool struct {
	mu      slotLock
	idle    []chan task
	maxIdle int
}

type task struct {
	ex *Executor
	j  int32
}

// pool is the process-wide worker pool: executors of consecutive windows reuse
// the same goroutines and their stacks.
var pool = &workerPool{maxIdle: 1024}

// dispatch runs j on ex, on a parked goroutine if one is available.
func (p *workerPool) dispatch(ex *Executor, j int32) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		ch := p.idle[n-1] // the one that went idle last: the likeliest to be awake still
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		ch <- task{ex, j}
		return
	}
	p.mu.Unlock()
	go p.loop(task{ex, j})
}

func (p *workerPool) loop(t task) {
	ch := make(chan task, 1)
	for {
		t.ex.worker(t.j)
		p.mu.Lock()
		if len(p.idle) >= p.maxIdle {
			p.mu.Unlock()
			return
		}
		p.idle = append(p.idle, ch)
		p.mu.Unlock()
		t = <-ch
	}
}
