// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
)

func sigMsg(from, to byte, sel uint32, args ...byte) *core.Message {
	t := common.Address{19: to}
	data := []byte{byte(sel >> 24), byte(sel >> 16), byte(sel >> 8), byte(sel)}
	for _, a := range args {
		var w [32]byte
		w[12], w[31] = 0xaa, a // an address word: twelve zero bytes, then a non-zero prefix
		data = append(data, w[:]...)
	}
	return &core.Message{From: common.Address{19: from}, To: &t, Data: data}
}

func hasFeature(fs []feature, f feature) bool {
	for _, x := range fs {
		if x == f {
			return true
		}
	}
	return false
}

// Two swaps of one token through different routers share the token's feature
// and nothing else; a small integer argument is not an address; an address
// packed inside the calldata is a feature once it is known to own storage,
// and so is an address of the access list.
func TestTxFeats(t *testing.T) {
	a := txFeatures(sigMsg(1, 10, 0x11111111, 7), nil)
	b := txFeatures(sigMsg(2, 11, 0x22222222, 7, 8), nil)
	shared := 0
	for _, f := range a {
		if hasFeature(b, f) {
			shared++
		}
	}
	if shared != 1 {
		t.Fatalf("features shared by two routers swapping one token: %d, want 1", shared)
	}
	plain := sigMsg(1, 10, 0x11111111)
	plain.Data = append(plain.Data, make([]byte, 31)...)
	plain.Data = append(plain.Data, 5) // uint256(5)
	base := len(txFeatures(sigMsg(1, 10, 0x11111111), nil))
	if n := len(txFeatures(plain, nil)); n != base {
		t.Fatalf("an integer argument became a feature: %d features, want %d", n, base)
	}
	token := common.Address{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	packed := sigMsg(1, 10, 0x11111111)
	packed.Data = append(append(packed.Data, 0xfe, 0xfe, 0xfe), token[:]...) // not on a word boundary
	want := mixFeature(addressBits(token[:]), featureArg)
	if hasFeature(txFeatures(packed, nil), want) || hasFeature(txFeatures(packed, new(addressSet)), want) {
		t.Fatal("a packed address that is not known to own storage became a feature")
	}
	known := new(addressSet)
	known.add(addressBits(token[:]))
	if !hasFeature(txFeatures(packed, known), want) {
		t.Fatal("a known address packed inside the calldata is not a feature")
	}
	listed := sigMsg(1, 10, 0x11111111)
	listed.AccessList = types.AccessList{{Address: token}}
	if !hasFeature(txFeatures(listed, nil), want) {
		t.Fatal("an address of the access list is not a feature")
	}
	if f := mixFeature(42, featureArgTo); f.kind() != int(featureArgTo) {
		t.Fatalf("kind of a feature: %d, want %d", f.kind(), featureArgTo)
	}
}

func named(tab *SignatureTable, k Key, f feature) (float64, bool) {
	names, _, _ := tab.names(k, nil, waitThreshold())
	for _, nm := range names {
		if nm.f == f {
			return nm.num / nm.den, true
		}
	}
	return 0, false
}

// A feature names a key's writers while most of its carriers change the key:
// the token of a pool does once a second swap has come, the router that also
// carries everything else does not, and the token stops when its carriers
// stop changing the key.
func TestSigTableProbability(t *testing.T) {
	tab := NewSignatureTable()
	k := Key{Addr: common.Address{1}, Slot: common.Hash{2}, Kind: kindStorage}
	token, router := mixFeature(42, featureArg), mixFeature(7, featureTo)
	swap, other := []feature{token, router}, []feature{router}
	rar := []uint8{2, 8}

	if tab.maybe(k) {
		t.Fatal("an empty table knows the key")
	}
	tab.credit(k, 1, swap, rar, false)
	if tab.maybe(k) {
		t.Fatal("a write of a key that is not contended entered the table")
	}
	tab.credit(k, 1, swap, rar, true) // invalidated a reader: contended
	for range 10 {
		tab.carried(other) // through the same router, not touching the key
	}
	tab.carried(swap)
	tab.credit(k, 2, swap, rar, false)
	tab.credit(k, 2, swap, rar, false) // the same writer again: counted once
	if p, ok := named(tab, k, token); !ok || p < 0.6 {
		t.Fatalf("after two swaps the token names the writers: named %v, probability %.2f", ok, p)
	}
	if p, ok := named(tab, k, router); ok {
		t.Fatalf("the router, carried by ten transactions that did not change the key, names its writers (%.2f)", p)
	}
	for range 10 {
		tab.carried(swap) // the token's carriers stop changing the key
	}
	if p, ok := named(tab, k, token); ok {
		t.Fatalf("the token still names the writers after ten carriers that did not change the key (%.2f)", p)
	}
}

// A feature that was nearly certain is distrusted after a short run of
// carriers that did not change the key, long before its record would have
// averaged down; with the change test off it still names the writers.
func TestSigTableChange(t *testing.T) {
	tab := NewSignatureTable()
	k := Key{Addr: common.Address{7}, Slot: common.Hash{8}, Kind: kindStorage}
	f, rar := []feature{mixFeature(9, featureSelector)}, []uint8{3}
	tab.credit(k, 1, f, rar, true)
	for id := int64(2); id <= 60; id++ {
		tab.carried(f)
		tab.credit(k, id, f, rar, false)
	}
	if p, ok := named(tab, k, f[0]); !ok || p < 0.9 {
		t.Fatalf("sixty carriers all changed the key: named %v at %.2f", ok, p)
	}
	for range 3 {
		tab.carried(f) // the mint is sold out: its calls no longer change the counter
	}
	if p, ok := named(tab, k, f[0]); ok {
		t.Fatalf("after three misses in a row a feature that never missed still names the writers (%.2f)", p)
	}
	tab.carried(f)
	tab.credit(k, 100, f, rar, false)
	tab.carried(f)
	tab.credit(k, 101, f, rar, false)
	if _, ok := named(tab, k, f[0]); !ok {
		t.Fatal("the carriers change the key again: the feature should name the writers again")
	}
}

// A feature's record on the keys that already remember it is its prior on a
// new key: a token that changed every pool slot so far names the writers of
// the next slot at once, a feature of the same kind without a record does
// not start as high.
func TestSigTableRecord(t *testing.T) {
	tab := NewSignatureTable()
	token, fresh := mixFeature(42, featureArg), mixFeature(43, featureArg)
	rar := []uint8{2}
	for slot := byte(1); slot <= 3; slot++ {
		k := Key{Addr: common.Address{1}, Slot: common.Hash{slot}, Kind: kindStorage}
		tab.credit(k, 1, []feature{token}, rar, true)
	}
	for id := int64(2); id <= 9; id++ {
		tab.carried([]feature{token})
		for slot := byte(1); slot <= 3; slot++ {
			tab.credit(Key{Addr: common.Address{1}, Slot: common.Hash{slot}, Kind: kindStorage}, id, []feature{token}, rar, false)
		}
	}
	next := Key{Addr: common.Address{1}, Slot: common.Hash{9}, Kind: kindStorage}
	tab.credit(next, 10, []feature{token, fresh}, []uint8{2, 2}, true)
	pt, okt := named(tab, next, token)
	pf, _ := named(tab, next, fresh)
	if !okt || pt < 0.85 {
		t.Fatalf("a token with a perfect record on three slots starts a fourth at %.2f (named %v), want at least 0.85", pt, okt)
	}
	if pf >= pt {
		t.Fatalf("a feature without a record starts as high as one with a perfect record: %.2f against %.2f", pf, pt)
	}
}

// The read-modify-write rule holds while most finished readers of the key
// changed it, and a copy of the table is independent of the original.
func TestSigTableRMWAndClone(t *testing.T) {
	tab := NewSignatureTable()
	k := Key{Addr: common.Address{3}, Slot: common.Hash{4}, Kind: kindStorage}
	f, rar := []feature{mixFeature(1, featureArg)}, []uint8{1}
	tab.credit(k, 1, f, rar, true)
	for range 4 {
		tab.readBy(k, true)
	}
	if _, rmw, _ := tab.names(k, nil, waitThreshold()); rmw.num == 0 {
		t.Fatal("four of four readers changed the key: the rule should hold")
	}
	cp := tab.Clone()
	for range 20 {
		tab.readBy(k, false)
	}
	if _, rmw, _ := tab.names(k, nil, waitThreshold()); rmw.num != 0 {
		t.Fatal("four of twenty-four readers changed the key: the rule should not hold")
	}
	if _, rmw, _ := cp.names(k, nil, waitThreshold()); rmw.num == 0 {
		t.Fatal("the copy followed the original")
	}
	other := Key{Addr: common.Address{5}, Slot: common.Hash{6}, Kind: kindStorage}
	cp.credit(other, 2, f, rar, true)
	if tab.maybe(other) || !cp.maybe(other) {
		t.Fatal("a key added to the copy: only the copy should know it")
	}
	before, _ := named(cp, k, f[0])
	for range 20 {
		tab.carried(f)
	}
	if after, ok := named(cp, k, f[0]); !ok || after != before {
		t.Fatalf("carriers counted in the original changed the copy's probability: %.2f -> %.2f", before, after)
	}
}
