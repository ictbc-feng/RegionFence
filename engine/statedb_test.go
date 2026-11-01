// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// A beneficiary's account fence does not cover fees credited later by the
// committer: observing it must check again.
func TestObserveEagerBeneficiaryBalance(t *testing.T) {
	for _, tc := range []struct {
		name        string
		beneficiary bool
		loaded      uint64
		abort       bool
	}{
		{"stale beneficiary", true, 100, true},
		{"exact beneficiary", true, 150, false},
		{"ordinary account", false, 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := common.Address{1}
			base := newMemReader(map[common.Address]*Account{
				addr: {Balance: uint256.NewInt(100), Nonce: 3},
			})
			ex := &Executor{
				store:         newStore([]*memReader{base}, []int32{0}),
				blockIndex:    []int32{0, 0},
				blocks:        []*BlockEnv{{Coinbase: common.Address{2}}},
				coinbases:     map[common.Address]bool{addr: tc.beneficiary},
				blocked:       make([]paddedInt64, 2),
				stale:         make([]paddedBool, 2),
				counters:      make([]counterShard, 64),
				feeAtFrontier: []*uint256.Int{nil, uint256.NewInt(50)},
				frontier:      1,
			}
			if tc.beneficiary {
				ex.blocks[0].Coinbase = addr
			}
			ex.committed.Store(1)
			ex.finishedFront.Store(1)
			r := newVersionReader(ex, 1, 0)
			r.loadedAcc[addr] = loadedAccount{balance: uint256.NewInt(tc.loaded), nonce: 3}
			var caught any
			func() {
				defer func() { caught = recover() }()
				r.observeBalance(&fenceStateDB{}, addr)
			}()
			if tc.abort {
				if caught != errAbortIncarnation || !ex.stale[1].Load() {
					t.Fatalf("stale eager balance was not invalidated: panic=%v stale=%v", caught, ex.stale[1].Load())
				}
			} else if caught != nil || ex.stale[1].Load() {
				t.Fatalf("valid balance invalidated: panic=%v stale=%v", caught, ex.stale[1].Load())
			}
			if r.fenced[addr] != tc.beneficiary {
				t.Fatalf("balance observed=%v, want %v", r.fenced[addr], tc.beneficiary)
			}
			r.releaseExecMaps()
			putMap(&originMaps, r.reads)
			putMap(&hashMaps, r.vals)
			putMap(&registeredMaps, r.registered)
			r.releaseSets()
		})
	}
}

// The rebase: an account whose deferred fence found a newer
// balance moves onto it, keeping the incarnation's own change, through an
// offset the StateDB's journal cannot undo; it refuses when the nonce moved,
// when the account exists only by its balance, or when its own debit no
// longer fits.
func TestRebaseAccount(t *testing.T) {
	addr := common.Address{7}
	for _, tc := range []struct {
		name     string
		nonce    uint64 // loaded
		loaded   uint64 // balance loaded
		own      int64  // the incarnation's unobserved change since
		newBal   uint64
		newNonce uint64
		ok       bool
		want     uint64 // the balance the incarnation then sees
	}{
		{name: "credit kept on a higher balance", nonce: 3, loaded: 100, own: 30, newBal: 150, newNonce: 3, ok: true, want: 180},
		{name: "debit kept on a lower balance", nonce: 3, loaded: 100, own: -20, newBal: 50, newNonce: 3, ok: true, want: 30},
		{name: "debit no longer covered", nonce: 3, loaded: 100, own: -80, newBal: 50, newNonce: 3},
		{name: "nonce moved", nonce: 3, loaded: 100, newBal: 120, newNonce: 4},
		{name: "exists by its balance only", nonce: 0, loaded: 100, newBal: 120, newNonce: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sdb, err := state.New(types.EmptyRootHash, state.NewDatabaseForTesting())
			if err != nil {
				t.Fatal(err)
			}
			sdb.SetBalance(addr, uint256.NewInt(tc.loaded), tracing.BalanceChangeUnspecified)
			sdb.SetNonce(addr, tc.nonce, tracing.NonceChangeUnspecified)
			if tc.own > 0 {
				sdb.AddBalance(addr, uint256.NewInt(uint64(tc.own)), tracing.BalanceChangeTransfer)
			} else if tc.own < 0 {
				sdb.SubBalance(addr, uint256.NewInt(uint64(-tc.own)), tracing.BalanceChangeTransfer)
			}
			s := &fenceStateDB{StateDB: sdb, r: &versionReader{}, used: map[common.Address]uint8{}}
			la := loadedAccount{balance: uint256.NewInt(tc.loaded), nonce: tc.nonce, codeHash: types.EmptyCodeHash, exists: true}
			inner := sdb.GetBalance(addr).Clone()
			snap := sdb.Snapshot()
			ok := s.rebase(addr, &la, uint256.NewInt(tc.newBal), tc.newNonce)
			if ok != tc.ok {
				t.Fatalf("rebase = %v, want %v", ok, tc.ok)
			}
			if !sdb.GetBalance(addr).Eq(inner) {
				t.Fatal("the rebase wrote into the StateDB")
			}
			if !ok {
				if !s.balanceOf(addr).Eq(inner) {
					t.Fatal("a refused rebase changed the balance seen")
				}
				return
			}
			sdb.RevertToSnapshot(snap) // a call that observed the balance reverts
			if got := s.balanceOf(addr).Uint64(); got != tc.want {
				t.Fatalf("balance seen %d, want %d", got, tc.want)
			}
			if la.balance.Uint64() != tc.newBal {
				t.Fatalf("loaded record %v, want %d", la.balance, tc.newBal)
			}
		})
	}
}

// A reader of a volatile balance waits for the uncommitted writer
// of the version it would see, and for nobody once that writer committed, for
// an ESTIMATE (it waits there anyway) or for the pre-state.
func TestVolatileWriter(t *testing.T) {
	addr := common.Address{9}
	base := newMemReader(map[common.Address]*Account{addr: {Balance: uint256.NewInt(1000), Nonce: 1}})
	ex := &Executor{n: 8, blockIndex: make([]int32, 8), store: newStore([]*memReader{base}, []int32{0})}
	bk := Key{Addr: addr, Kind: kindBalance}
	if w := ex.volatileWriter(bk, 5, 0); w != -1 {
		t.Fatalf("pre-state: writer %d, want none", w)
	}
	e := ex.store.getOrCreate(bk)
	e.put(version{tx: 2, inc: stamp(0, 1), val: Value{Balance: uint256.NewInt(900)}})
	if w := ex.volatileWriter(bk, 5, 0); w != 2 {
		t.Fatalf("uncommitted writer: %d, want 2", w)
	}
	if w := ex.volatileWriter(bk, 2, 0); w != -1 {
		t.Fatalf("the writer itself sees the pre-state: %d", w)
	}
	ex.committed.Store(3)
	if w := ex.volatileWriter(bk, 5, 0); w != -1 {
		t.Fatalf("committed writer: %d, want none", w)
	}
	ex.committed.Store(0)
	e.markEstimate(2, markFailedKey)
	if w := ex.volatileWriter(bk, 5, 0); w != -1 {
		t.Fatalf("ESTIMATE: %d, want none (the reader waits there anyway)", w)
	}
	var rates rewriteRate
	if rates.changeProb(bk) > 0.5 {
		t.Fatal("an unseen key is not volatile")
	}
	for range 8 {
		rates.observe(bk, false)
	}
	if rates.changeProb(bk) <= 0.5 {
		t.Fatal("a balance rewritten with other values eight times is volatile")
	}
}

// The volatile balance's chain: the reader waits for the first writer down bk's chain of
// reads that has not finished, and for nobody once the chain is finished down
// to a committed version or the pre-state. A finished writer whose read is
// stale only by version is walked through.
func TestVolatileWait(t *testing.T) {
	addr := common.Address{9}
	base := newMemReader(map[common.Address]*Account{addr: {Balance: uint256.NewInt(1000), Nonce: 1}})
	ex := &Executor{n: 8, blockIndex: make([]int32, 8), store: newStore([]*memReader{base}, []int32{0}), blocks: []*BlockEnv{{}},
		txMu: make([]paddedMutex, 8), finished: make([]paddedBool, 8), runs: make([]*incarnation, 8), signal: make([]chan struct{}, 8)}
	bk := Key{Addr: addr, Kind: kindBalance}
	e := ex.store.getOrCreate(bk)
	e.put(version{tx: 2, inc: stamp(0, 1), val: Value{Balance: uint256.NewInt(900)}})
	e.put(version{tx: 4, inc: stamp(0, 1), val: Value{Balance: uint256.NewInt(800)}})
	finish := func(w int32, o origin) {
		ex.runs[w] = &incarnation{reads: map[Key]origin{bk: o}}
		ex.finished[w].Store(true)
	}
	if w, _, why, _ := ex.volatileWait(bk, 6, 0); w != 4 || why != walkUnfinished {
		t.Fatalf("newest writer running: wait for %d, want 4", w)
	}
	finish(4, origin{2, stamp(0, 1)})
	if w, _, why, depth := ex.volatileWait(bk, 6, 0); w != 2 || why != walkUnfinished || depth != 1 {
		t.Fatalf("the writer it read from running: wait for %d, want 2", w)
	}
	finish(2, baseOrigin)
	if w, _, why, _ := ex.volatileWait(bk, 6, 0); w != -1 || why != walkCommitted {
		t.Fatalf("chain finished down to the pre-state: wait for %d", w)
	}
	ex.finished[2].Store(false)
	ex.committed.Store(3)
	if w, _, why, _ := ex.volatileWait(bk, 6, 0); w != -1 || why != walkCommitted {
		t.Fatalf("chain reaching a committed version: wait for %d", w)
	}
	ex.committed.Store(0)
	// 4 read an incarnation of 2 that is no longer there, but the balance it
	// saw (900) is still the one below 4: only the version changed, the walk
	// goes on to 2 (running)
	finish(4, origin{2, stamp(1, 1)})
	ex.runs[4].vals = map[Key]common.Hash{bk: uint256.NewInt(900).Bytes32()}
	if w, _, why, depth := ex.volatileWait(bk, 6, 0); w != 2 || why != walkUnfinished || depth != 1 {
		t.Fatalf("read stale by version only: wait for %d (why %d, depth %d), want 2", w, why, depth)
	}
}
