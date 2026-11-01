// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// retentionFixture is a one-block store whose pre-state holds slot k with a
// non-zero field, and the executor state sameField needs.
func retentionFixture(n int) (*Executor, Key, common.Hash) {
	addr := common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
	slot := common.HexToHash("0x01")
	field := common.HexToHash("0x2a")
	base := newMemReader(map[common.Address]*Account{addr: {Storage: map[common.Hash]common.Hash{slot: field}}})
	ex := &Executor{n: n, blockIndex: make([]int32, n), store: newStore([]*memReader{base}, []int32{0})}
	return ex, Key{Addr: addr, Slot: slot, Kind: kindStorage}, field
}

// publishTo is putVersionNode without the invalidation: it installs the version
// and returns the readers it made stale.
func publishTo(ex *Executor, k Key, writer, st int32, val Value) []readRec {
	var sc sameCheck
	same := ex.sameField(k, val, &sc)
	e := ex.store.getOrCreate(k)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.put(version{tx: writer, inc: st, val: val})
	return e.staleReaders(writer, st, 0, same)
}

// A reader registered on the pre-state and kept across a same-value
// publication must still be invalidated by a later publication of another
// value. Registered storage reads are not validated at commit (readsCurrent),
// so a reader wrongly kept here commits a stale read. The record of a
// pre-state reader carries no field; left unset, the next comparison would run
// against the zero hash, and a writer clearing the slot would look like one
// that kept its value.
func TestSameValueRetentionThenClear(t *testing.T) {
	for _, tc := range []struct {
		name      string
		second    int32 // the writer that clears the slot
		secondInc int32
	}{
		{"writer between", 3, 0},
		{"same writer re-executed", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex, k, field := retentionFixture(8)
			const reader = 5
			if _, ok := ex.store.readRegistered(k, reader, 0, false); ok {
				t.Fatal("the reader should have resolved to the pre-state")
			}
			if victims := publishTo(ex, k, 2, stamp(0, 1), Value{Slot: field}); len(victims) != 0 {
				t.Fatalf("same-value publication: victims %d, want none", len(victims))
			}
			victims := publishTo(ex, k, tc.second, stamp(tc.secondInc, 1), Value{})
			if len(victims) != 1 || victims[0].tx != reader {
				t.Fatalf("clearing publication: victims %+v, want reader %d invalidated", victims, reader)
			}
		})
	}
}

// The retained reader stays retained while writers keep publishing the field
// it observed, and a boundary value equal to that field leaves it alone while
// a different one (also zero) invalidates it.
func TestSameValueRetentionKeepsObservedField(t *testing.T) {
	ex, k, field := retentionFixture(8)
	const reader = 5
	ex.store.readRegistered(k, reader, 0, false)
	for w := int32(1); w <= 3; w++ {
		if victims := publishTo(ex, k, w, stamp(0, 1), Value{Slot: field}); len(victims) != 0 {
			t.Fatalf("writer %d republishing the observed field: victims %d", w, len(victims))
		}
	}
	ex.store.firsts = []int32{0, 4} // the reader's block starts at index 4
	if out := ex.store.boundaryStale(1, k, Value{Slot: field}); len(out) != 0 {
		t.Fatalf("boundary value equal to the observed field invalidated %d readers", len(out))
	}
	if out := ex.store.boundaryStale(1, k, Value{}); len(out) != 1 || out[0].tx != reader {
		t.Fatalf("zero boundary value: stale readers %+v, want reader %d", out, reader)
	}
}
