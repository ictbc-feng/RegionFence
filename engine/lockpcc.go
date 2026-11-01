// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// Pessimistic concurrency control by runtime locks: strict two-phase locking
// on field-level keys, shared to read and exclusive to write, with a shared
// lock upgraded by a write. The block order decides conflicts (wound-wait): a
// transaction waits for a lower holder and wounds a higher one, which stops
// and runs again. So a wait always points down, nothing can deadlock, and a
// transaction that reached the lowest place is never wounded. Locks go at
// commit, in block order, so a read sees committed values only: no ESTIMATE,
// no validation. Writes stay in the StateDB until commit. A blocked
// transaction gives its slot up.
//
// Fees: the committer adds them to the beneficiary's balance, and observing
// that balance waits until every lower transaction has committed.

type pccEntry struct {
	mu      sync.Mutex
	holders []pccHold
	waiters []int32
}

type pccHold struct {
	tx, inc int32
	x       bool
}

type pccShard struct {
	mu   sync.RWMutex
	lock map[Key]*pccEntry
	data map[Key]pccValue
	_    [32]byte
}

// pccValue is a committed value and its writer.
type pccValue struct {
	val Value
	tx  int32
}

type pccTx struct {
	wound     atomic.Int32 // 1 + the incarnation wounded
	wake      chan struct{}
	executed  atomic.Pointer[pccDone]
	committed atomic.Bool
	baseTxCounters
	_ [64]byte
}

// pccDone is an incarnation waiting for its commit.
type pccDone struct {
	run   *baseRun
	locks map[Key]uint8
}

type lockPCC struct {
	*baseExec
	txs      []pccTx
	shards   [baseShards]pccShard
	codes    sync.Map
	commitMu sync.Mutex
	frontier atomic.Int32
	slots    pccSlots
	errMu    sync.Mutex
	err      error
	stop     chan struct{}
	commitNs int64
}

func executeLockPCC(env *Env, workers int) (*Result, Stats, error) {
	b := &lockPCC{txs: make([]pccTx, len(env.Messages)), stop: make(chan struct{})}
	b.baseExec = newBaseExec(env, b)
	for i := range b.shards {
		b.shards[i].lock = make(map[Key]*pccEntry, 8)
		b.shards[i].data = make(map[Key]pccValue, 8)
	}
	for j := range b.txs {
		b.txs[j].wake = make(chan struct{}, 1)
	}
	b.slots.free = workers
	var st Stats
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(b.n)
	for j := range b.n { // in block order: a slot goes to the lowest that needs one
		b.slots.acquire(int32(j))
		go func() {
			defer wg.Done()
			b.runTx(int32(j))
		}()
	}
	wg.Wait()
	st.Wall = time.Since(start)
	st.CommitNs = b.commitNs
	res := &Result{State: &baseView{f: b, base: b.bk.Base}}
	for j := range b.txs {
		tx := &b.txs[j]
		tx.foldInto(&st)
		if b.err == nil {
			res.Txs = append(res.Txs, tx.executed.Load().run.out)
		}
	}
	if b.err != nil {
		return nil, st, b.err
	}
	return res, st, nil
}

func (b *lockPCC) fail(err error) {
	b.errMu.Lock()
	defer b.errMu.Unlock()
	if b.err == nil {
		b.err = err
		close(b.stop)
	}
}

func (b *lockPCC) failed() bool {
	select {
	case <-b.stop:
		return true
	default:
		return false
	}
}

// runTx runs transaction j, which holds a slot, until it commits. A refused
// message waits for its commit like any incarnation: a lower transaction that
// changes what it read wounds it, and only at the frontier is a refusal final.
func (b *lockPCC) runTx(j int32) {
	tx := &b.txs[j]
	for inc := int32(0); ; inc++ {
		if b.failed() {
			b.slots.release()
			return
		}
		run, reader := b.execute(j, inc, &tx.baseTxCounters)
		if run.err != nil {
			b.fail(fmt.Errorf("tx %d: %w", j, run.err))
			b.slots.release()
			return
		}
		if !run.stopped {
			run.stopped = !b.settle(reader, run)
		}
		if run.stopped {
			tx.aborts.Add(1)
			b.releaseLocks(j, inc, reader.locks)
			continue
		}
		tx.executed.Store(&pccDone{run, reader.locks})
		b.tryCommit()
		b.slots.release()
		if b.awaitCommit(tx, inc) {
			return
		}
		tx.aborts.Add(1)
		done := tx.executed.Swap(nil)
		b.releaseLocks(j, inc, done.locks)
		b.slots.acquire(j)
	}
}

// settle takes the exclusive locks of writes without a lock so far (a nonce
// raised by the EVM itself, an account's deletion). false: wounded.
func (b *lockPCC) settle(r *baseReader, run *baseRun) (ok bool) {
	defer func() {
		if p := recover(); p != nil {
			if p != errBaseAbort {
				panic(p)
			}
			ok = false
		}
	}()
	for _, w := range run.writes {
		b.lock(r, w.key, true)
	}
	return b.txs[r.j].wound.Load() != r.inc+1
}

// awaitCommit parks the executed incarnation: true when it committed, false
// when it was wounded.
func (b *lockPCC) awaitCommit(tx *pccTx, inc int32) bool {
	for {
		if tx.committed.Load() {
			return true
		}
		if tx.wound.Load() == inc+1 {
			return false
		}
		select {
		case <-tx.wake:
		case <-b.stop:
			return true
		}
	}
}

// tryCommit commits, in block order, what is executed at the frontier. A
// wound can only come from below, so nothing at the frontier is wounded later.
func (b *lockPCC) tryCommit() {
	b.commitMu.Lock()
	defer b.commitMu.Unlock()
	t0 := time.Now()
	f := int(b.frontier.Load())
	f0 := f
	for ; f < b.n; f++ {
		tx := &b.txs[f]
		d := tx.executed.Load()
		if d == nil || tx.wound.Load() == d.run.inc+1 {
			break
		}
		if d.run.rejected != nil {
			b.fail(fmt.Errorf("tx %d: %w", f, d.run.rejected))
			break
		}
		b.commit(int32(f), d)
		b.frontier.Store(int32(f + 1))
		tx.committed.Store(true)
		pccWake(tx)
		if f+1 < b.n {
			pccWake(&b.txs[f+1]) // it may wait for the frontier
		}
	}
	if f > f0 {
		b.commitNs += time.Since(t0).Nanoseconds()
	}
}

func pccWake(tx *pccTx) {
	select {
	case tx.wake <- struct{}{}:
	default:
	}
}

func (b *lockPCC) commit(j int32, d *pccDone) {
	for _, w := range d.run.writes {
		if w.key.Kind == kindCode && w.val.Code != nil {
			b.codes.Store(w.val.CodeHash, w.val.Code)
		}
		b.put(w.key, pccValue{w.val, j})
	}
	if !d.run.fee.IsZero() {
		k := Key{Addr: b.bk.Coinbase, Kind: kindBalance}
		bal := new(uint256.Int)
		if v, ok := b.get(k); ok {
			bal.Set(v.val.Balance)
		} else if acc := b.bk.Base.accountShared(k.Addr); acc != nil {
			bal.Set(acc.Balance)
		}
		b.put(k, pccValue{Value{Balance: bal.Add(bal, &d.run.fee)}, j})
	}
	b.releaseLocks(j, d.run.inc, d.locks)
	putMap(&originMaps, d.run.reads)
	putMap(&usedMaps, d.run.used)
	d.run.reads, d.run.used = nil, nil
}

func (b *lockPCC) get(k Key) (pccValue, bool) {
	s := &b.shards[baseShard(k)]
	s.mu.RLock()
	v, ok := s.data[k]
	s.mu.RUnlock()
	return v, ok
}

func (b *lockPCC) put(k Key, v pccValue) {
	s := &b.shards[baseShard(k)]
	s.mu.Lock()
	s.data[k] = v
	s.mu.Unlock()
}

func (b *lockPCC) entry(k Key) *pccEntry {
	s := &b.shards[baseShard(k)]
	s.mu.RLock()
	e := s.lock[k]
	s.mu.RUnlock()
	if e != nil {
		return e
	}
	s.mu.Lock()
	if e = s.lock[k]; e == nil {
		e = new(pccEntry)
		s.lock[k] = e
	}
	s.mu.Unlock()
	return e
}

// stopIfWounded unwinds an incarnation that a lower transaction wounded.
func (b *lockPCC) stopIfWounded(r *baseReader) {
	if b.txs[r.j].wound.Load() == r.inc+1 || b.failed() {
		panic(errBaseAbort)
	}
}

// lock implements baseControl: the lock wound-wait grants, blocking without a slot
// while a lower transaction holds it in a conflicting mode.
func (b *lockPCC) lock(r *baseReader, k Key, x bool) {
	want := uint8(1)
	if x {
		want = 2
	}
	if r.locks[k] >= want {
		return
	}
	b.stopIfWounded(r)
	if r.locks == nil {
		r.locks = make(map[Key]uint8, 16)
	}
	tx := &b.txs[r.j]
	e := b.entry(k)
	for {
		e.mu.Lock()
		lower := false
		for _, h := range e.holders {
			if h.tx < r.j && (x || h.x) {
				lower = true
				break
			}
		}
		if !lower {
			e.grant(b, r, x)
			e.wakeNext(b)
			e.mu.Unlock()
			r.locks[k] = want
			return
		}
		if !containsWriter(e.waiters, r.j) {
			e.waiters = append(e.waiters, r.j)
		}
		e.mu.Unlock()
		b.park(r)
		if tx.wound.Load() == r.inc+1 || b.failed() {
			e.mu.Lock()
			e.waiters = removeTx(e.waiters, r.j)
			e.wakeNext(b)
			e.mu.Unlock()
			panic(errBaseAbort)
		}
		e.mu.Lock()
		e.waiters = removeTx(e.waiters, r.j)
		e.mu.Unlock()
	}
}

// grant wounds the higher holders in conflict and records the hold (e.mu held).
func (e *pccEntry) grant(b *lockPCC, r *baseReader, x bool) {
	kept := e.holders[:0]
	mine := false
	for _, h := range e.holders {
		switch {
		case h.tx == r.j:
			if h.inc == r.inc {
				mine = true
				h.x = h.x || x
				kept = append(kept, h)
			}
		case h.tx > r.j && (x || h.x):
			t := &b.txs[h.tx]
			t.wound.Store(h.inc + 1)
			pccWake(t)
		default:
			kept = append(kept, h)
		}
	}
	e.holders = kept
	if !mine {
		e.holders = append(e.holders, pccHold{r.j, r.inc, x})
	}
}

// wakeNext wakes the lowest waiter, which looks at the lock again (e.mu held).
func (e *pccEntry) wakeNext(b *lockPCC) {
	if len(e.waiters) == 0 {
		return
	}
	low := e.waiters[0]
	for _, w := range e.waiters {
		low = min(low, w)
	}
	e.waiters = removeTx(e.waiters, low)
	pccWake(&b.txs[low])
}

func removeTx(s []int32, j int32) []int32 {
	for i, x := range s {
		if x == j {
			s[i] = s[len(s)-1]
			return s[:len(s)-1]
		}
	}
	return s
}

// releaseLocks drops an incarnation's holds; a wound may have dropped some.
func (b *lockPCC) releaseLocks(j, inc int32, locks map[Key]uint8) {
	for k := range locks {
		e := b.entry(k)
		e.mu.Lock()
		for i, h := range e.holders {
			if h.tx == j && h.inc == inc {
				e.holders[i] = e.holders[len(e.holders)-1]
				e.holders = e.holders[:len(e.holders)-1]
				break
			}
		}
		e.wakeNext(b)
		e.mu.Unlock()
	}
}

// park gives the slot up until the transaction is woken.
func (b *lockPCC) park(r *baseReader) {
	tx := &b.txs[r.j]
	b.slots.release()
	select {
	case <-tx.wake:
	case <-b.stop:
	}
	b.slots.acquire(r.j)
}

// read implements baseControl: the newest committed value.
func (b *lockPCC) read(r *baseReader, k Key) (Value, origin, bool) {
	v, ok := b.get(k)
	if !ok {
		return Value{}, baseOrigin, false
	}
	return v.val, origin{v.tx, 0}, true
}

// observed implements baseControl: shared locks on the fields, then the load. An
// account loaded before its lock must be what the lock protects now.
func (b *lockPCC) observed(r *baseReader, addr common.Address, bits uint8) {
	fields := [...]struct {
		bit  uint8
		kind keyKind
	}{{usedBalance, kindBalance}, {usedNonce, kindNonce}, {usedCode, kindCode}}
	for _, f := range fields {
		if bits&f.bit == 0 || r.used[addr]&f.bit != 0 {
			continue
		}
		k := Key{Addr: addr, Kind: f.kind}
		if f.kind == kindBalance && addr == b.bk.Coinbase {
			b.awaitFrontier(r)
		}
		b.lock(r, k, false)
		la, loaded := r.loadedAcc[addr]
		if !loaded {
			continue
		}
		v, ok := b.get(k)
		if !ok {
			continue // not written in the block: the load holds
		}
		same := false
		switch f.kind {
		case kindBalance:
			same = v.val.Balance.Eq(la.balance)
		case kindNonce:
			same = v.val.Nonce == la.nonce
		case kindCode:
			same = v.val.CodeHash == la.codeHash || (v.val.CodeHash == (common.Hash{}) && la.codeHash == types.EmptyCodeHash)
		}
		if !same {
			panic(errBaseAbort)
		}
		r.reads[k] = origin{v.tx, 0}
	}
}

// awaitFrontier waits until every lower transaction has committed.
func (b *lockPCC) awaitFrontier(r *baseReader) {
	for b.frontier.Load() < r.j {
		b.stopIfWounded(r)
		b.park(r)
	}
}

func (b *lockPCC) deployed(h common.Hash) []byte {
	if c, ok := b.codes.Load(h); ok {
		return c.([]byte)
	}
	return nil
}

// final implements baseFinal.
func (b *lockPCC) final(k Key) (Value, int32, bool) {
	v, ok := b.get(k)
	if !ok {
		return Value{}, -1, false
	}
	return v.val, v.tx, true
}

// pccSlots are the threads: acquire waits for one, the lowest transaction
// first.
type pccSlots struct {
	mu    sync.Mutex
	free  int
	queue pccQueue
}

type pccWaiter struct {
	j  int32
	ch chan struct{}
}

type pccQueue []pccWaiter

func (q pccQueue) Len() int           { return len(q) }
func (q pccQueue) Less(a, b int) bool { return q[a].j < q[b].j }
func (q pccQueue) Swap(a, b int)      { q[a], q[b] = q[b], q[a] }
func (q *pccQueue) Push(x any)        { *q = append(*q, x.(pccWaiter)) }
func (q *pccQueue) Pop() any {
	old := *q
	w := old[len(old)-1]
	*q = old[:len(old)-1]
	return w
}

func (s *pccSlots) acquire(j int32) {
	s.mu.Lock()
	if s.free > 0 {
		s.free--
		s.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	heap.Push(&s.queue, pccWaiter{j, ch})
	s.mu.Unlock()
	<-ch
}

func (s *pccSlots) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queue.Len() > 0 {
		close(heap.Pop(&s.queue).(pccWaiter).ch)
		return
	}
	s.free++
}
