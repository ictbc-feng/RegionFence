// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

// Env holds what every executor needs to run the transactions of a fixture:
// the chain rules, the block context and the decoded messages.
type Env struct {
	Config       *params.ChainConfig
	Rules        params.Rules
	BlockContext vm.BlockContext
	Messages     []*core.Message
	Base         *memReader
	// An execution window: the blocks the messages belong to (one for a
	// single block) and each message's block index.
	Blocks     []*BlockEnv
	BlockIndex []int32
}

// BlockEnv is one block of an execution window.
type BlockEnv struct {
	Fixture     *Fixture
	Context     vm.BlockContext
	Rules       params.Rules
	Base        *memReader
	Coinbase    common.Address
	First, Last int32 // its transactions' window indices (Last < First if empty)
	// Between lists the keys whose value at the start of this block is not
	// the one the window's earlier blocks left them with, and that value:
	// what happened between blocks outside transactions (withdrawals, block
	// rewards, system calls). Set by NewWindowEnv; see Executor.rebase.
	Between []BoundaryValue
}

// BoundaryValue is one key of a block's pre-state that changed outside the
// window's transactions, with the value the block starts from.
type BoundaryValue struct {
	Key   Key
	Field common.Hash // the value as a read observes it (fieldOf)
	Val   Value
}

// NewWindowEnv concatenates consecutive blocks into one execution window:
// transactions keep their own block context, and a later block reads what
// earlier blocks of the window wrote before falling back to its own pre-state.
func NewWindowEnv(envs []*Env) *Env {
	w := &Env{Config: envs[0].Config, Rules: envs[0].Rules, BlockContext: envs[0].BlockContext, Base: envs[0].Base}
	for _, e := range envs {
		b := *e.Blocks[0]
		b.First = int32(len(w.Messages))
		b.Last = b.First + int32(len(e.Messages)) - 1
		w.Blocks = append(w.Blocks, &b)
		for range e.Messages {
			w.BlockIndex = append(w.BlockIndex, int32(len(w.Blocks)-1))
		}
		w.Messages = append(w.Messages, e.Messages...)
	}
	setBetween(w.Blocks)
	return w
}

// setBetween derives, from the blocks' pre-state snapshots and the recorded
// effects of their transactions, which keys change between the blocks of a
// window outside transactions. A key's value at the end of a block is what
// the block's transactions left (Post), or the block's own pre-state value if
// they did not change it; if the next snapshot that holds the key shows
// another value, something other than a transaction changed it in between.
// These changes are inputs of the blocks, not results of the execution: a
// node applies them itself (withdrawals from the block body, system calls).
// The replay has no such inputs, so it reads them off the snapshots, here,
// when the window is built, rather than by comparing every key of a block's
// pre-state with the version store while the commit frontier waits at the
// block boundary. Beneficiaries of earlier blocks are always listed: their
// fee credits are not in the recorded effects.
func setBetween(blocks []*BlockEnv) {
	if len(blocks) < 2 {
		return
	}
	end := make(map[Key]common.Hash) // value at the end of the latest earlier block that held the key
	slots := make(map[common.Address][]common.Hash)
	coinbases := make(map[common.Address]bool)
	for i, b := range blocks {
		for addr, acc := range b.Base.accounts {
			check := func(k Key, field common.Hash, val Value) {
				was, seen := end[k]
				if i > 0 && ((seen && was != field) || (k.Kind == kindBalance && coinbases[addr])) {
					b.Between = append(b.Between, BoundaryValue{k, field, val})
				}
				if !seen && k.Kind == kindStorage {
					slots[addr] = append(slots[addr], k.Slot)
				}
				end[k] = field
			}
			check(Key{Addr: addr, Kind: kindBalance}, acc.Balance.Bytes32(), Value{Balance: acc.Balance.Clone()})
			check(Key{Addr: addr, Kind: kindNonce}, uint256.NewInt(acc.Nonce).Bytes32(), Value{Nonce: acc.Nonce})
			for slot, val := range acc.Storage {
				check(Key{Addr: addr, Slot: slot, Kind: kindStorage}, val, Value{Slot: val})
			}
		}
		for addr, pa := range b.Fixture.Post {
			if pa.Deleted {
				end[Key{Addr: addr, Kind: kindBalance}] = common.Hash{}
				end[Key{Addr: addr, Kind: kindNonce}] = common.Hash{}
				for _, slot := range slots[addr] {
					end[Key{Addr: addr, Slot: slot, Kind: kindStorage}] = common.Hash{}
				}
				continue
			}
			if pa.Balance != nil {
				end[Key{Addr: addr, Kind: kindBalance}] = pa.Balance.Bytes32()
			}
			if pa.Nonce != nil {
				end[Key{Addr: addr, Kind: kindNonce}] = uint256.NewInt(*pa.Nonce).Bytes32()
			}
			for slot, val := range pa.Storage {
				k := Key{Addr: addr, Slot: slot, Kind: kindStorage}
				if _, seen := end[k]; !seen {
					slots[addr] = append(slots[addr], slot)
				}
				end[k] = val
			}
		}
		coinbases[b.Coinbase] = true
	}
}

// HashLookup resolves historical block hashes for the BLOCKHASH opcode.
type HashLookup func(number uint64) common.Hash

// NewEnv prepares the execution environment of a fixture. Sender recovery happens
// here, outside of any timed section.
func NewEnv(fx *Fixture, config *params.ChainConfig, hashes HashLookup) (*Env, error) {
	h := fx.Header
	var baseFee, blobBaseFee *big.Int
	if h.BaseFee != nil {
		baseFee = new(big.Int).Set(h.BaseFee)
	}
	if h.ExcessBlobGas != nil {
		blobBaseFee = eip4844.CalcBlobFee(config, h)
	}
	var random *common.Hash
	if h.Difficulty.Sign() == 0 {
		random = &h.MixDigest
	}
	var slotNum uint64
	if h.SlotNumber != nil {
		slotNum = *h.SlotNumber
	}
	number := h.Number.Uint64()
	blockCtx := vm.BlockContext{
		CanTransfer: canTransfer, // a predicate on the RegionFence state, core.CanTransfer elsewhere
		Transfer:    core.Transfer,
		GetHash: func(n uint64) common.Hash {
			if n >= number || hashes == nil {
				return common.Hash{}
			}
			return hashes(n)
		},
		Coinbase:         h.Coinbase,
		BlockNumber:      new(big.Int).Set(h.Number),
		Time:             h.Time,
		Difficulty:       new(big.Int).Set(h.Difficulty),
		BaseFee:          baseFee,
		BlobBaseFee:      blobBaseFee,
		GasLimit:         h.GasLimit,
		Random:           random,
		SlotNum:          slotNum,
		CostPerStateByte: params.CostPerStateByte,
	}
	signer := types.MakeSigner(config, h.Number, h.Time)
	msgs := make([]*core.Message, len(fx.Txs))
	for i, tx := range fx.Txs {
		msg, err := core.TransactionToMessage(tx, signer, baseFee)
		if err != nil {
			return nil, fmt.Errorf("tx %d: %w", i, err)
		}
		msgs[i] = msg
	}
	rules := config.Rules(h.Number, h.Difficulty.Sign() == 0, h.Time)
	base := newMemReader(fx.Pre)
	env := &Env{
		Config:       config,
		Rules:        rules,
		BlockContext: blockCtx,
		Messages:     msgs,
		Base:         base,
		BlockIndex:   make([]int32, len(msgs)),
	}
	env.Blocks = []*BlockEnv{{Fixture: fx, Context: blockCtx, Rules: rules, Base: base, Coinbase: h.Coinbase, First: 0, Last: int32(len(msgs)) - 1}}
	return env, nil
}

// TxOutcome is what an executor reports per transaction.
type TxOutcome struct {
	GasUsed uint64
	Failed  bool
}

// Result is the outcome of executing a fixture's transactions.
type Result struct {
	Txs   []TxOutcome
	State StateView
	TxNs  []int64 // per-transaction wall time (serial execution only)
	ex    *Executor
}

// StateView exposes the post-state for comparison.
type StateView interface {
	BalanceHex(addr common.Address) string
	NonceOf(addr common.Address) uint64
	SlotOf(addr common.Address, slot common.Hash) common.Hash
	Exists(addr common.Address) bool
}

type stateDBView struct{ db *state.StateDB }

func (v stateDBView) BalanceHex(a common.Address) string { return v.db.GetBalance(a).Hex() }
func (v stateDBView) NonceOf(a common.Address) uint64    { return v.db.GetNonce(a) }
func (v stateDBView) SlotOf(a common.Address, s common.Hash) common.Hash {
	return v.db.GetState(a, s)
}
func (v stateDBView) Exists(a common.Address) bool { return v.db.Exist(a) && !v.db.Empty(a) }

// ExecuteSerial runs the transactions one after another on a plain StateDB, the
// way geth's StateProcessor does, minus the block-level system calls and
// finalisation (execution-layer comparison only).
func ExecuteSerial(fx *Fixture, env *Env) (*Result, error) {
	db, err := state.NewWithReader(types.EmptyRootHash, state.NewDatabaseForTesting(), env.Base)
	if err != nil {
		return nil, err
	}
	evm := vm.NewEVM(env.BlockContext, db, env.Config, vm.Config{})
	gp := core.NewGasPool(fx.Header.GasLimit)
	res := &Result{Txs: make([]TxOutcome, len(env.Messages)), State: stateDBView{db}, TxNs: make([]int64, len(env.Messages))}
	for i, msg := range env.Messages {
		start := time.Now()
		db.SetTxContext(fx.Txs[i].Hash(), i, uint32(i+1))
		out, err := core.ApplyMessage(evm, msg, gp)
		if err != nil {
			return nil, fmt.Errorf("block %d tx %d: %w", fx.Number, i, err)
		}
		db.Finalise(env.Rules)
		res.TxNs[i] = time.Since(start).Nanoseconds()
		res.Txs[i] = TxOutcome{GasUsed: out.UsedGas, Failed: out.Failed()}
	}
	return res, nil
}

// Mismatch describes one difference between an execution and the chain.
type Mismatch struct {
	What string
}

// CheckAgainstChain compares an execution with the on-chain receipts and the
// post-state the tracer recorded.
func CheckAgainstChain(fx *Fixture, res *Result) []Mismatch {
	var out []Mismatch
	for i, r := range fx.Receipts {
		got := res.Txs[i]
		if got.GasUsed != r.GasUsed {
			out = append(out, Mismatch{fmt.Sprintf("tx %d gasUsed %d want %d", i, got.GasUsed, r.GasUsed)})
		}
		if (r.Status == 1) == got.Failed {
			out = append(out, Mismatch{fmt.Sprintf("tx %d status failed=%v want status %d", i, got.Failed, r.Status)})
		}
	}
	for addr, pa := range fx.Post {
		if addr == fx.Header.Coinbase {
			continue // fee credit to the beneficiary; checked separately if needed
		}
		if pa.Deleted {
			if res.State.Exists(addr) {
				out = append(out, Mismatch{fmt.Sprintf("%s should be deleted", addr.Hex())})
			}
			continue
		}
		if pa.Balance != nil && res.State.BalanceHex(addr) != pa.Balance.Hex() {
			out = append(out, Mismatch{fmt.Sprintf("%s balance %s want %s", addr.Hex(), res.State.BalanceHex(addr), pa.Balance.Hex())})
		}
		if pa.Nonce != nil && res.State.NonceOf(addr) != *pa.Nonce {
			out = append(out, Mismatch{fmt.Sprintf("%s nonce %d want %d", addr.Hex(), res.State.NonceOf(addr), *pa.Nonce)})
		}
		for slot, want := range pa.Storage {
			if got := res.State.SlotOf(addr, slot); got != want {
				out = append(out, Mismatch{fmt.Sprintf("%s slot %s = %s want %s", addr.Hex(), slot.Hex(), got.Hex(), want.Hex())})
			}
		}
	}
	return out
}

// CompareResults checks that two executions of the same fixture agree on every
// transaction outcome and on every key the block writes.
func CompareResults(fx *Fixture, a, b *Result) []Mismatch {
	var out []Mismatch
	for i := range a.Txs {
		if a.Txs[i] != b.Txs[i] {
			out = append(out, Mismatch{fmt.Sprintf("tx %d outcome %+v vs %+v", i, a.Txs[i], b.Txs[i])})
		}
	}
	for addr, pa := range fx.Post {
		if a.State.BalanceHex(addr) != b.State.BalanceHex(addr) {
			out = append(out, Mismatch{fmt.Sprintf("%s balance %s vs %s", addr.Hex(), a.State.BalanceHex(addr), b.State.BalanceHex(addr))})
		}
		if a.State.NonceOf(addr) != b.State.NonceOf(addr) {
			out = append(out, Mismatch{fmt.Sprintf("%s nonce differs", addr.Hex())})
		}
		for slot := range pa.Storage {
			if a.State.SlotOf(addr, slot) != b.State.SlotOf(addr, slot) {
				out = append(out, Mismatch{fmt.Sprintf("%s slot %s differs", addr.Hex(), slot.Hex())})
			}
		}
	}
	return out
}
