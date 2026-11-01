// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// Block-STM (Gelashvili et al., PPoPP'23), the scheduler of its Algorithms 4
// and 5 and the MVMemory of its Algorithm 2, with the names of the paper. The
// threads take tasks from two cursors, the lower first, validation first when
// they meet. An incarnation writes to MVMemory only when it ends, nothing
// interrupts it, and a validation reads its read set back and compares
// versions. A read of an ESTIMATE ends the incarnation with a dependency on
// the writer; it runs again from the start after the writer's next execution.
//
// The one addition: fees. An incarnation records its fee apart (stmTx.fee), and
// a read of the beneficiary's balance sums the fees of the lower transactions
// into its read set, with a dependency on the first one missing. This keeps
// the algorithm as it is: no commit index, no waiting for one.

const (
	stmReady uint8 = iota
	stmExecuting
	stmExecuted
	stmAborting
)

type stmVersion struct{ idx, inc int32 }

// stmWrite is an entry of MVMemory: est turns it into an ESTIMATE.
type stmWrite struct {
	stmVersion
	est atomic.Bool
	val Value
}

// stmEntry is a location's entries, ascending by transaction. A slice is never
// changed once stored, so a read takes no lock.
type stmEntry struct {
	mu   sync.Mutex
	vers atomic.Pointer[[]*stmWrite]
}

type stmShard struct {
	mu sync.RWMutex
	m  map[Key]*stmEntry
	_  [32]byte
}

type stmFee struct {
	inc int32
	amt uint256.Int
}

// stmReads is a recorded read set; it does not change once stored.
type stmReads struct {
	reads []stmRead
	feeLo int32
	fees  []int32
}

type stmRead struct {
	k Key
	o origin
}

type stmTx struct {
	mu      sync.Mutex // txn_status
	inc     int32
	status  uint8
	depMu   sync.Mutex // txn_dependency
	deps    []int32
	written []Key // last_written_locations: only the thread that owns the status touches it
	reads   atomic.Pointer[stmReads]
	run     atomic.Pointer[baseRun]
	fee     atomic.Pointer[stmFee]
	baseTxCounters
	_ [64]byte
}

type stmTask struct {
	kind uint8 // 0: none
	v    stmVersion
}

const (
	stmNone uint8 = iota
	stmExec
	stmValidate
)

type blockSTM struct {
	*baseExec
	n32             int32
	beneficiary     common.Address
	beneficiaryBase *uint256.Int
	executionIdx    paddedInt64
	validationIdx   paddedInt64
	decreaseCnt     paddedInt64
	activeTasks     paddedInt64
	done            atomic.Bool
	txs             []stmTx
	shards          [baseShards]stmShard
	codes           sync.Map
	errMu           sync.Mutex
	err             error
}

func executeBlockSTM(env *Env, workers int) (*Result, Stats, error) {
	b := &blockSTM{n32: int32(len(env.Messages)), txs: make([]stmTx, len(env.Messages))}
	b.baseExec = newBaseExec(env, b)
	b.beneficiary = b.bk.Coinbase
	b.beneficiaryBase = new(uint256.Int)
	if acc := b.bk.Base.accountShared(b.beneficiary); acc != nil {
		b.beneficiaryBase = acc.Balance
	}
	for i := range b.shards {
		b.shards[i].m = make(map[Key]*stmEntry, 8)
	}
	var st Stats
	start := time.Now()
	if b.n > 0 {
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b.thread()
			}()
		}
		wg.Wait()
	}
	st.Wall = time.Since(start)
	res := &Result{State: &baseView{f: b, base: b.bk.Base}}
	for j := range b.txs {
		b.txs[j].foldInto(&st)
		if b.err == nil {
			run := b.txs[j].run.Load()
			if run.rejected != nil {
				b.err = fmt.Errorf("tx %d: %w", j, run.rejected)
				continue
			}
			res.Txs = append(res.Txs, run.out)
		}
	}
	if b.err != nil {
		return nil, st, b.err
	}
	return res, st, nil
}

func (b *blockSTM) fail(err error) {
	b.errMu.Lock()
	if b.err == nil {
		b.err = err
	}
	b.errMu.Unlock()
	b.done.Store(true)
}

// thread is the loop of Algorithm 1.
func (b *blockSTM) thread() {
	var task stmTask
	for !b.done.Load() {
		switch task.kind {
		case stmExec:
			task = b.tryExecute(task.v)
		case stmValidate:
			task = b.needsReexecution(task.v)
		}
		if task.kind == stmNone {
			task = b.nextTask()
		}
		if task.kind == stmNone {
			runtime.Gosched()
		}
	}
}

func (b *blockSTM) tryExecute(v stmVersion) stmTask {
	tx := &b.txs[v.idx]
	for {
		run, reader := b.execute(v.idx, v.inc, &tx.baseTxCounters)
		if run.err != nil {
			b.fail(fmt.Errorf("tx %d: %w", v.idx, run.err))
			return stmTask{}
		}
		if run.stopped {
			tx.aborts.Add(1)
			if b.addDependency(v.idx, reader.dep) {
				return stmTask{}
			}
			continue // the dependency resolved meanwhile
		}
		wroteNew := b.record(v, run, reader)
		return b.finishExecution(v.idx, v.inc, wroteNew)
	}
}

func (b *blockSTM) needsReexecution(v stmVersion) stmTask {
	valid := b.validateReadSet(v.idx)
	aborted := !valid && b.tryValidationAbort(v.idx, v.inc)
	if aborted {
		b.convertWritesToEstimates(v.idx)
		b.txs[v.idx].aborts.Add(1)
	}
	return b.finishValidation(v.idx, aborted)
}

// The scheduler.

func (b *blockSTM) decreaseExecutionIdx(t int64) {
	lowerTo(&b.executionIdx.Int64, t)
	b.decreaseCnt.Add(1)
}

func (b *blockSTM) decreaseValidationIdx(t int64) {
	lowerTo(&b.validationIdx.Int64, t)
	b.decreaseCnt.Add(1)
}

func lowerTo(a *atomic.Int64, t int64) {
	for {
		cur := a.Load()
		if cur <= t || a.CompareAndSwap(cur, t) {
			return
		}
	}
}

func (b *blockSTM) checkDone() {
	observed := b.decreaseCnt.Load()
	n := int64(b.n)
	if min(b.executionIdx.Load(), b.validationIdx.Load()) >= n && b.activeTasks.Load() == 0 && observed == b.decreaseCnt.Load() {
		b.done.Store(true)
	}
}

func (b *blockSTM) tryIncarnate(idx int64) (stmVersion, bool) {
	if idx < int64(b.n) {
		tx := &b.txs[idx]
		tx.mu.Lock()
		if tx.status == stmReady {
			tx.status = stmExecuting
			inc := tx.inc
			tx.mu.Unlock()
			return stmVersion{int32(idx), inc}, true
		}
		tx.mu.Unlock()
	}
	b.activeTasks.Add(-1)
	return stmVersion{}, false
}

func (b *blockSTM) nextVersionToExecute() (stmVersion, bool) {
	if b.executionIdx.Load() >= int64(b.n) {
		b.checkDone()
		return stmVersion{}, false
	}
	b.activeTasks.Add(1)
	return b.tryIncarnate(b.executionIdx.Add(1) - 1)
}

func (b *blockSTM) nextVersionToValidate() (stmVersion, bool) {
	if b.validationIdx.Load() >= int64(b.n) {
		b.checkDone()
		return stmVersion{}, false
	}
	b.activeTasks.Add(1)
	idx := b.validationIdx.Add(1) - 1
	if idx < int64(b.n) {
		tx := &b.txs[idx]
		tx.mu.Lock()
		inc, status := tx.inc, tx.status
		tx.mu.Unlock()
		if status == stmExecuted {
			return stmVersion{int32(idx), inc}, true
		}
	}
	b.activeTasks.Add(-1)
	return stmVersion{}, false
}

func (b *blockSTM) nextTask() stmTask {
	if b.validationIdx.Load() < b.executionIdx.Load() {
		if v, ok := b.nextVersionToValidate(); ok {
			return stmTask{stmValidate, v}
		}
	} else if v, ok := b.nextVersionToExecute(); ok {
		return stmTask{stmExec, v}
	}
	return stmTask{}
}

func (b *blockSTM) addDependency(idx, blocking int32) bool {
	dep := &b.txs[blocking]
	dep.depMu.Lock()
	dep.mu.Lock()
	executed := dep.status == stmExecuted
	dep.mu.Unlock()
	if executed {
		dep.depMu.Unlock()
		return false
	}
	tx := &b.txs[idx]
	tx.mu.Lock()
	tx.status = stmAborting
	tx.mu.Unlock()
	dep.deps = append(dep.deps, idx)
	dep.depMu.Unlock()
	b.activeTasks.Add(-1)
	return true
}

func (b *blockSTM) setReadyStatus(idx int32) {
	tx := &b.txs[idx]
	tx.mu.Lock()
	tx.inc++
	tx.status = stmReady
	tx.mu.Unlock()
}

func (b *blockSTM) resumeDependencies(deps []int32) {
	if len(deps) == 0 {
		return
	}
	low := deps[0]
	for _, d := range deps {
		b.setReadyStatus(d)
		low = min(low, d)
	}
	b.decreaseExecutionIdx(int64(low))
}

func (b *blockSTM) finishExecution(idx, inc int32, wroteNew bool) stmTask {
	tx := &b.txs[idx]
	tx.mu.Lock()
	tx.status = stmExecuted
	tx.mu.Unlock()
	tx.depMu.Lock()
	deps := tx.deps
	tx.deps = nil
	tx.depMu.Unlock()
	b.resumeDependencies(deps)
	if b.validationIdx.Load() > int64(idx) {
		if wroteNew {
			b.decreaseValidationIdx(int64(idx))
		} else {
			return stmTask{stmValidate, stmVersion{idx, inc}}
		}
	}
	b.activeTasks.Add(-1)
	return stmTask{}
}

func (b *blockSTM) tryValidationAbort(idx, inc int32) bool {
	tx := &b.txs[idx]
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.inc == inc && tx.status == stmExecuted {
		tx.status = stmAborting
		return true
	}
	return false
}

func (b *blockSTM) finishValidation(idx int32, aborted bool) stmTask {
	if aborted {
		b.setReadyStatus(idx)
		b.decreaseValidationIdx(int64(idx) + 1)
		if b.executionIdx.Load() > int64(idx) {
			if v, ok := b.tryIncarnate(int64(idx)); ok {
				return stmTask{stmExec, v}
			}
			return stmTask{} // tryIncarnate gave the task up
		}
	}
	b.activeTasks.Add(-1)
	return stmTask{}
}

// MVMemory.

func (b *blockSTM) entry(k Key, create bool) *stmEntry {
	s := &b.shards[baseShard(k)]
	s.mu.RLock()
	e := s.m[k]
	s.mu.RUnlock()
	if e != nil || !create {
		return e
	}
	s.mu.Lock()
	if e = s.m[k]; e == nil {
		e = new(stmEntry)
		s.m[k] = e
	}
	s.mu.Unlock()
	return e
}

// put installs the entry of a transaction, replacing an earlier one.
func (e *stmEntry) put(v *stmWrite) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var old []*stmWrite
	if p := e.vers.Load(); p != nil {
		old = *p
	}
	at, same := stmSearch(old, v.idx)
	if same {
		next := make([]*stmWrite, len(old))
		copy(next, old)
		next[at] = v
		e.vers.Store(&next)
		return
	}
	next := make([]*stmWrite, len(old)+1)
	copy(next, old[:at])
	next[at] = v
	copy(next[at+1:], old[at:])
	e.vers.Store(&next)
}

func (e *stmEntry) remove(idx int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.vers.Load()
	if p == nil {
		return
	}
	old := *p
	if at, same := stmSearch(old, idx); same {
		next := make([]*stmWrite, 0, len(old)-1)
		next = append(append(next, old[:at]...), old[at+1:]...)
		e.vers.Store(&next)
	}
}

// stmSearch returns the position of the first entry not below idx.
func stmSearch(vers []*stmWrite, idx int32) (int, bool) {
	lo, hi := 0, len(vers)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if vers[m].idx < idx {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(vers) && vers[lo].idx == idx
}

// mvRead returns the entry of the highest transaction below idx (nil: none).
func (b *blockSTM) mvRead(k Key, idx int32) *stmWrite {
	e := b.entry(k, false)
	if e == nil {
		return nil
	}
	p := e.vers.Load()
	if p == nil {
		return nil
	}
	at, _ := stmSearch(*p, idx)
	if at == 0 {
		return nil
	}
	return (*p)[at-1]
}

// record is MVMemory.record: apply_write_set, the RCU update of the written
// locations, and the read set.
func (b *blockSTM) record(v stmVersion, run *baseRun, reader *baseReader) bool {
	tx := &b.txs[v.idx]
	for _, w := range run.writes {
		if w.key.Kind == kindCode && w.val.Code != nil {
			b.codes.Store(w.val.CodeHash, w.val.Code)
		}
		b.entry(w.key, true).put(&stmWrite{stmVersion: v, val: w.val})
	}
	wroteNew := false
	next := make([]Key, len(run.writes))
	for i, w := range run.writes {
		next[i] = w.key
		wroteNew = wroteNew || !containsKey(tx.written, w.key)
	}
	for _, k := range tx.written {
		if !containsKey(next, k) {
			b.entry(k, true).remove(v.idx)
		}
	}
	tx.written = next
	rs := &stmReads{reads: make([]stmRead, 0, len(run.reads))}
	if run.used[b.beneficiary]&usedBalance != 0 {
		rs.feeLo, rs.fees = reader.feeLo, reader.fees
	}
	for k, o := range run.reads {
		if run.dependsOn(k) {
			rs.reads = append(rs.reads, stmRead{k, o})
		}
	}
	tx.reads.Store(rs)
	tx.fee.Store(&stmFee{inc: v.inc, amt: run.fee})
	tx.run.Store(run)
	putMap(&originMaps, run.reads)
	putMap(&usedMaps, run.used)
	run.reads, run.used = nil, nil
	return wroteNew
}

func (b *blockSTM) convertWritesToEstimates(idx int32) {
	tx := &b.txs[idx]
	for _, k := range tx.written {
		if v := b.mvRead(k, idx+1); v != nil && v.idx == idx {
			v.est.Store(true)
		}
	}
	tx.fee.Store(nil)
}

func (b *blockSTM) validateReadSet(idx int32) bool {
	rs := b.txs[idx].reads.Load()
	if rs == nil {
		return false
	}
	for i := range rs.reads {
		rd := &rs.reads[i]
		v := b.mvRead(rd.k, idx)
		if v == nil {
			if rd.o.tx >= 0 {
				return false
			}
			continue
		}
		if v.est.Load() || v.idx != rd.o.tx || v.inc != rd.o.inc {
			return false
		}
	}
	for i, inc := range rs.fees {
		if f := b.txs[rs.feeLo+int32(i)].fee.Load(); f == nil || f.inc != inc {
			return false
		}
	}
	return true
}

// read implements baseControl: MVMemory.read, for the beneficiary's balance with
// the fees of the lower transactions. An ESTIMATE ends the incarnation.
func (b *blockSTM) read(r *baseReader, k Key) (Value, origin, bool) {
	v := b.mvRead(k, r.j)
	if v != nil && v.est.Load() {
		r.dep = v.idx
		panic(errBaseAbort)
	}
	if k.Kind == kindBalance && k.Addr == b.beneficiary {
		return b.readBeneficiary(r, v)
	}
	if v == nil {
		return Value{}, baseOrigin, false
	}
	return v.val, origin{v.idx, v.inc}, true
}

func (b *blockSTM) readBeneficiary(r *baseReader, v *stmWrite) (Value, origin, bool) {
	bal := new(uint256.Int).Set(b.beneficiaryBase)
	o := baseOrigin
	lo := int32(0)
	if v != nil {
		bal.Set(v.val.Balance)
		o = origin{v.idx, v.inc}
		lo = v.idx
	}
	fees := make([]int32, r.j-lo)
	for i := r.j - 1; i >= lo; i-- {
		f := b.txs[i].fee.Load()
		if f == nil {
			r.dep = i
			panic(errBaseAbort)
		}
		fees[i-lo] = f.inc
		bal.Add(bal, &f.amt)
	}
	r.feeLo, r.fees = lo, fees
	return Value{Balance: bal}, o, true
}

func (b *blockSTM) lock(*baseReader, Key, bool)                 {}
func (b *blockSTM) observed(*baseReader, common.Address, uint8) {}
func (b *blockSTM) deployed(h common.Hash) []byte {
	if c, ok := b.codes.Load(h); ok {
		return c.([]byte)
	}
	return nil
}

// final implements baseFinal.
func (b *blockSTM) final(k Key) (Value, int32, bool) {
	v := b.mvRead(k, b.n32)
	if k.Kind == kindBalance && k.Addr == b.beneficiary {
		bal := new(uint256.Int).Set(b.beneficiaryBase)
		lo, tx := int32(0), int32(-1)
		if v != nil {
			bal.Set(v.val.Balance)
			lo, tx = v.idx, v.idx
		}
		for i := lo; i < b.n32; i++ {
			f := b.txs[i].fee.Load()
			bal.Add(bal, &f.amt)
		}
		return Value{Balance: bal}, tx, true
	}
	if v == nil {
		return Value{}, -1, false
	}
	return v.val, v.idx, true
}
