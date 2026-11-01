// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

// a helper works on a worker's processor only once a round is waiting
func TestHelperWanted(t *testing.T) {
	ex := &Executor{n: 8, status: make([]paddedStatus, 8), procs: 2}
	ex.scheduler = newScheduler(2, 8, func(int32) {})
	ex.scheduler.freeA.Store(0)
	if ex.wanted() {
		t.Fatal("wanted with no spare processor and nothing waiting")
	}
	ex.status[2].set(statusExecuted)
	if !ex.wanted() {
		t.Fatal("not wanted a round behind")
	}
}

// a check is refused by a change of a key it read, not of another
func TestRefusedByOwnKeys(t *testing.T) {
	ex := &Executor{keyGeneration: make([]atomic.Int64, keyGenerationSize)}
	ex.aheadNext.Store(5)
	a, b := Key{Slot: common.Hash{31: 1}, Kind: kindStorage}, Key{Slot: common.Hash{31: 2}, Kind: kindStorage}
	if keyGenerationSlot(a) == keyGenerationSlot(b) {
		t.Fatal("slots collide")
	}
	run := &incarnation{pending: []pendingRead{{k: a}}}
	ex.noteChange(1, b)
	g := ex.changeGeneration.Load()
	ex.noteChange(1, b)
	if !ex.unchangedSince(run, g) {
		t.Fatal("refused by another key")
	}
	ex.noteChange(1, a)
	if ex.unchangedSince(run, g) {
		t.Fatal("kept over a change of its key")
	}
	g = ex.changeGeneration.Load()
	ex.noteChange(5, a) // at the cursor: any check of it or above comes later
	if !ex.unchangedSince(run, g) {
		t.Fatal("refused by a change above the checks")
	}
	ex.noteChange(1, Key{Kind: kindWipe})
	if ex.unchangedSince(run, g) {
		t.Fatal("kept over a wipe")
	}
}

func balFix() (*Executor, Key) {
	addr := common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
	base := newMemReader(map[common.Address]*Account{addr: {Balance: uint256.NewInt(1000)}})
	ex := &Executor{n: 4, blockIndex: make([]int32, 4), store: newStore([]*memReader{base}, []int32{0}),
		blocks: []*BlockEnv{{First: 0, Last: 3}}, coinbases: map[common.Address]bool{}}
	return ex, Key{Addr: addr, Kind: kindBalance}
}

// a lower transaction's ESTIMATE on the balance, a predicate with an own change
func TestPredicateOnEstimate(t *testing.T) {
	ex, k := balFix()
	e := ex.store.getOrCreate(k)
	e.mu.Lock()
	e.put(version{tx: 1, inc: 0, mark: markRewrites, val: Value{Balance: uint256.NewInt(5), Rel: true}})
	e.mu.Unlock()
	run := &incarnation{predicates: []balancePredicate{{addr: k.Addr, amount: uint256.NewInt(1), ok: true, own: uint256.NewInt(7), ownNeg: true}}}
	defer func() { t.Logf("recovered: %v", recover()) }()
	t.Logf("predicatesHold=%v", ex.predicatesHold(2, run))
}

// i swept the whole balance as a relative debit; j read the pre-state, untracked
func TestRelativeVersionComparedAsBalance(t *testing.T) {
	ex, k := balFix()
	e := ex.store.getOrCreate(k)
	e.mu.Lock()
	e.put(version{tx: 1, inc: 0, val: Value{Balance: uint256.NewInt(1000), Rel: true, Neg: true}})
	e.mu.Unlock()
	bal, _, _, _ := ex.balanceAt(k, 2)
	if cur := ex.readCurrentField(2, k, baseOrigin, common.Hash{}, false); bal.Sign() != 0 || cur {
		t.Fatalf("balance %v, read of 1000 current: %v", bal, cur)
	}
}
