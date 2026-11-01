// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// TestChainRateLast checks the learned probability that the n-th write of a
// key is its last one: a key written once per transaction, a lock-like key
// written three times (taken, then the value, then released), and the
// forgetting that lets a key's pattern change.
func TestChainRateLast(t *testing.T) {
	var c chainRate
	once := Key{Addr: common.HexToAddress("0x01"), Slot: common.HexToHash("0x01"), Kind: kindStorage}
	thrice := Key{Addr: common.HexToAddress("0x02"), Slot: common.HexToHash("0x02"), Kind: kindStorage}
	if p := c.last(once, 1); p != 0.5 {
		t.Fatalf("unseen key: last(1) = %v, want the uniform prior 0.5", p)
	}
	for range 30 {
		c.add(once, 1)
		c.add(thrice, 3)
	}
	if p := c.last(once, 1); p < 0.95 {
		t.Errorf("written once 30 times: last(1) = %v, want >= 0.95", p)
	}
	if p := c.last(thrice, 1); p > 0.05 {
		t.Errorf("written three times: last(1) = %v, want <= 0.05", p)
	}
	if p := c.last(thrice, 2); p > 0.05 {
		t.Errorf("written three times: last(2) = %v, want <= 0.05", p)
	}
	if p := c.last(thrice, 3); p < 0.95 {
		t.Errorf("written three times: last(3) = %v, want >= 0.95", p)
	}
	if p := c.last(thrice, chainWrites); p != 0 {
		t.Errorf("last(%d) = %v, want 0 (never taken for the last)", chainWrites, p)
	}
	// the key's transactions start writing it twice: the counts forget
	for range 200 {
		c.add(once, 2)
	}
	if p := c.last(once, 1); p > 0.1 {
		t.Errorf("after the change: last(1) = %v, want <= 0.1", p)
	}
	if p := c.last(once, 2); p < 0.9 {
		t.Errorf("after the change: last(2) = %v, want >= 0.9", p)
	}
}

// TestChainRatePopulationPrior checks that a key seen for the first time
// starts from what the population of hot keys does, not from one half.
func TestChainRatePopulationPrior(t *testing.T) {
	var c chainRate
	for i := range 200 {
		c.add(Key{Addr: common.BigToAddress(common.Big1), Slot: common.BigToHash(big.NewInt(int64(i))), Kind: kindStorage}, 1)
	}
	fresh := Key{Addr: common.HexToAddress("0x99"), Slot: common.HexToHash("0x99"), Kind: kindStorage}
	if p := c.last(fresh, 1); p < 0.9 {
		t.Errorf("unseen key among keys written once: last(1) = %v, want >= 0.9", p)
	}
}
