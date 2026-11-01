// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// The release learns how many times finished incarnations wrote a hot key: a
// key always written once is soon taken as written last.
func TestChainRatesLearn(t *testing.T) {
	saved := saveChain()
	t.Cleanup(saved.Restore)
	(&chainState{}).Restore()
	k := Key{Addr: common.Address{7}, Slot: common.Hash{1}, Kind: kindStorage}
	if p := chainRates.last(k, 1); p != 0.5 {
		t.Fatalf("prior %v, want 0.5", p)
	}
	for i := 0; i < 40; i++ {
		chainRates.add(k, 1)
	}
	if p := chainRates.last(k, 1); p < 0.9 {
		t.Fatalf("after 40 single writes: %v", p)
	}
}

func TestLineFit(t *testing.T) {
	var r lineFit
	if r.slope() != 0 {
		t.Fatal("empty fit has a slope")
	}
	for i := 0; i < 64; i++ {
		x := float64(100 + 50*(i%5))
		r.add(x, 3*x+7)
	}
	if s := r.slope(); s < 2.99 || s > 3.01 {
		t.Fatalf("slope %v, want 3", s)
	}
	var d lineFit
	for i := 0; i < 64; i++ {
		x := float64(100 + 50*(i%5))
		d.add(x, 1000-x)
	}
	if d.slope() != 0 {
		t.Fatal("a falling fit must price 0")
	}
	var c lineFit
	for i := 0; i < 64; i++ {
		c.add(200, float64(i))
	}
	if c.slope() != 0 {
		t.Fatal("no spread in x must price 0")
	}
}

// With NoCrossBlock a transaction's table is its block's, and the shared one without it.
func TestTableFor(t *testing.T) {
	shared, a, b := NewSignatureTable(), NewSignatureTable(), NewSignatureTable()
	ex := &Executor{signatures: shared, blockIndex: []int32{0, 0, 1}}
	if ex.tableFor(2) != shared {
		t.Fatal("without block tables a transaction uses the shared table")
	}
	ex.blockTables = []*SignatureTable{a, b}
	if ex.tableFor(1) != a || ex.tableFor(2) != b {
		t.Fatal("a transaction uses its block's table")
	}
}
