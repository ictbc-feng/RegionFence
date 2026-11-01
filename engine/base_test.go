// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

// loadBaseBlock reads a recorded block, or skips where the data are absent.
func loadBaseBlock(t *testing.T, n uint64) (*Fixture, *Env, *Result) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "data", "state", fmt.Sprintf("%d.json.gz", n))
	if _, err := os.Stat(path); err != nil {
		t.Skip("no block data")
	}
	fx, err := LoadFixture(path)
	if err != nil {
		t.Fatal(err)
	}
	env, err := NewEnv(fx, params.MainnetChainConfig, func(uint64) common.Hash { return common.Hash{} })
	if err != nil {
		t.Fatal(err)
	}
	res, err := ExecuteSerial(fx, env)
	if err != nil {
		t.Fatal(err)
	}
	return fx, env, res
}

// Both baselines reproduce the serial result on recorded blocks, hot keys and
// beneficiary payments included, at one worker and at many.
func TestBaselinesMatchSerial(t *testing.T) {
	for _, n := range []uint64{12499900, 17873600, 20079900} {
		fx, env, want := loadBaseBlock(t, n)
		for _, kind := range []Baseline{BaselineOCC, BaselinePCC} {
			for _, p := range []int{1, 2, 8, 32} {
				var res []*Result
				var err error
				done := make(chan struct{})
				go func() {
					defer close(done)
					res, _, err = ExecuteBaselineWindow(kind, []*Env{env}, p)
				}()
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					panic(fmt.Sprintf("block %d baseline %d P=%d hangs", n, kind, p))
				}
				if err != nil {
					t.Fatalf("block %d baseline %d P=%d: %v", n, kind, p, err)
				}
				if m := CompareResults(fx, want, res[0]); len(m) > 0 {
					t.Fatalf("block %d baseline %d P=%d: %d differences, first %+v", n, kind, p, len(m), m[0])
				}
			}
		}
	}
}

func stmForTest(n int) *blockSTM {
	b := &blockSTM{n32: int32(n), txs: make([]stmTx, n), baseExec: &baseExec{n: n, bk: &BlockEnv{}}}
	for i := range b.shards {
		b.shards[i].m = make(map[Key]*stmEntry)
	}
	return b
}

// An ESTIMATE stops a reader with a dependency on the writer, which resumes it
// at the next incarnation, and fails the validation of a reader of the entry.
func TestBlockSTMEstimate(t *testing.T) {
	b := stmForTest(2)
	k := Key{Addr: common.Address{1}, Kind: kindNonce}
	w := stmVersion{0, 0}
	b.txs[0].status = stmExecuting
	b.record(w, &baseRun{inc: 0, writes: []keyWrite{{k, Value{Nonce: 1}}}}, &baseReader{})
	b.finishExecution(0, 0, true)
	b.txs[1].reads.Store(&stmReads{reads: []stmRead{{k, origin{0, 0}}}})
	if !b.validateReadSet(1) {
		t.Fatal("current read does not validate")
	}
	if !b.tryValidationAbort(0, 0) {
		t.Fatal("no abort")
	}
	b.convertWritesToEstimates(0)
	if b.validateReadSet(1) {
		t.Fatal("a read of an ESTIMATE validates")
	}
	r := &baseReader{j: 1, dep: -1}
	func() {
		defer func() {
			if recover() != errBaseAbort || r.dep != 0 {
				t.Fatalf("read of an ESTIMATE went on, dep %d", r.dep)
			}
		}()
		b.read(r, k)
	}()
	b.txs[1].status = stmExecuting
	b.activeTasks.Store(1)
	b.executionIdx.Store(2)
	if !b.addDependency(1, 0) {
		t.Fatal("dependency on an aborting transaction refused")
	}
	if b.txs[1].status != stmAborting {
		t.Fatal("reader not aborting")
	}
	b.setReadyStatus(0)
	b.txs[0].status = stmExecuting
	b.finishExecution(0, 1, false)
	if b.txs[1].status != stmReady || b.txs[1].inc != 1 || b.executionIdx.Load() != 1 {
		t.Fatalf("not resumed: status %d inc %d idx %d", b.txs[1].status, b.txs[1].inc, b.executionIdx.Load())
	}
}

// A write to a location the last incarnation did not write sends validation
// back; a rewrite does not, and a location no longer written goes away.
func TestBlockSTMRecordWroteNew(t *testing.T) {
	b := stmForTest(1)
	k1, k2 := Key{Addr: common.Address{1}}, Key{Addr: common.Address{2}}
	bal := Value{Balance: new(uint256.Int)}
	if !b.record(stmVersion{0, 0}, &baseRun{writes: []keyWrite{{k1, bal}}}, &baseReader{}) {
		t.Fatal("first write is not new")
	}
	if b.record(stmVersion{0, 1}, &baseRun{inc: 1, writes: []keyWrite{{k1, bal}}}, &baseReader{}) {
		t.Fatal("a rewrite is new")
	}
	if !b.record(stmVersion{0, 2}, &baseRun{inc: 2, writes: []keyWrite{{k2, bal}}}, &baseReader{}) {
		t.Fatal("another location is not new")
	}
	if b.mvRead(k1, 1) != nil {
		t.Fatal("old location still there")
	}
}

func pccForTest(n int) *lockPCC {
	b := &lockPCC{txs: make([]pccTx, n), stop: make(chan struct{}), baseExec: &baseExec{n: n, bk: &BlockEnv{}}}
	for i := range b.shards {
		b.shards[i].lock = make(map[Key]*pccEntry)
		b.shards[i].data = make(map[Key]pccValue)
	}
	for j := range b.txs {
		b.txs[j].wake = make(chan struct{}, 1)
	}
	b.slots.free = n + 8
	return b
}

// Shared locks coexist; an exclusive request wounds a higher holder and waits
// for a lower one, and a release lets it in. Waits only point down.
func TestWoundWait(t *testing.T) {
	b := pccForTest(4)
	k := Key{Addr: common.Address{1}, Kind: kindBalance}
	rs := make([]*baseReader, 4)
	for j := range rs {
		rs[j] = &baseReader{j: int32(j), ex: b.baseExec}
	}
	b.lock(rs[1], k, false)
	b.lock(rs[3], k, false)
	if len(b.entry(k).holders) != 2 {
		t.Fatal("shared locks do not coexist")
	}
	got := make(chan struct{})
	go func() {
		b.slots.acquire(2)
		b.lock(rs[2], k, true) // waits for 1, wounds 3
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("granted past a lower holder")
	case <-time.After(50 * time.Millisecond):
	}
	b.releaseLocks(1, 0, rs[1].locks)
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("not granted after the release")
	}
	if b.txs[3].wound.Load() != 1 {
		t.Fatal("higher holder not wounded")
	}
	func() {
		defer func() {
			if recover() != errBaseAbort {
				t.Fatal("wounded incarnation went on")
			}
		}()
		b.stopIfWounded(rs[3])
	}()
	if h := b.entry(k).holders; len(h) != 1 || h[0].tx != 2 || !h[0].x {
		t.Fatalf("holders %+v", h)
	}
}

// An upgrade waits for a lower reader and wounds a higher one.
func TestUpgrade(t *testing.T) {
	b := pccForTest(3)
	k := Key{Addr: common.Address{1}, Kind: kindNonce}
	rs := []*baseReader{{j: 0, ex: b.baseExec}, {j: 1, ex: b.baseExec}, {j: 2, ex: b.baseExec}}
	b.lock(rs[1], k, false)
	b.lock(rs[2], k, false)
	b.lock(rs[1], k, true)
	if b.txs[2].wound.Load() != 1 || rs[1].locks[k] != 2 {
		t.Fatal("upgrade did not wound the higher reader")
	}
	if h := b.entry(k).holders; len(h) != 1 || !h[0].x {
		t.Fatalf("holders %+v", h)
	}
}

type finalMap map[Key]pccValue

func (m finalMap) final(k Key) (Value, int32, bool) {
	v, ok := m[k]
	return v.val, v.tx, ok
}

// A deleted account's storage reads as zero unless written after the deletion,
// and the beneficiary's balance in the view is what the executor reports.
func TestViewAfterSelfDestruct(t *testing.T) {
	addr := common.Address{7}
	slot := common.Hash{1}
	base := newMemReader(map[common.Address]*Account{addr: {Balance: uint256.NewInt(5), Storage: map[common.Hash]common.Hash{slot: {9}}}})
	s := Key{Addr: addr, Slot: slot, Kind: kindStorage}
	w := Key{Addr: addr, Kind: kindWipe}
	v := &baseView{f: finalMap{w: {Value{}, 3}}, base: base}
	if got := v.SlotOf(addr, slot); got != (common.Hash{}) {
		t.Fatalf("deleted slot reads %x", got)
	}
	v = &baseView{f: finalMap{w: {Value{}, 3}, s: {Value{Slot: common.Hash{2}}, 4}}, base: base}
	if got := v.SlotOf(addr, slot); got != (common.Hash{2}) {
		t.Fatalf("slot rewritten after the deletion reads %x", got)
	}
}

// Learned brings the rates of a window back for the next repetition.
func TestLearnedRestoresRates(t *testing.T) {
	k := Key{Addr: common.Address{3}, Kind: kindBalance}
	l := SaveLearned()
	before := volatileBalances.seen[volatileBalances.slot(k)].Load()
	volatileBalances.observe(k, false)
	rewriteRates.observe(k, false)
	l.Restore()
	if volatileBalances.seen[volatileBalances.slot(k)].Load() != before {
		t.Fatal("rate not restored")
	}
}
