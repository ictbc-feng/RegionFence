// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"
)

// The baselines of the evaluation are two small executors of their own, not
// modes of the RegionFence engine: pure Block-STM (blockstm.go) and pure
// runtime locking (lockpcc.go), block by block. They share what is here: the
// EVM, field-level keys, the mask of the fields a transaction observed, the
// difference of a finished StateDB against what it read, and the deferred
// beneficiary fee, without which every transaction would write one balance
// and the block would be serial. Nothing the engine has to speed itself up
// comes along: no window, no learning, no prevalidation, no predicted nonces,
// no relative balances or balance predicates, no lazy account loads.

// Baseline selects the concurrency control of ExecuteBaselineWindow.
type Baseline int

const (
	BaselineOCC Baseline = iota
	BaselinePCC
)

// errBaseAbort unwinds an incarnation that cannot go on: it read an ESTIMATE
// (Block-STM), was wounded, or found a loaded field stale (PCC).
var errBaseAbort = errors.New("regionfence: baseline incarnation stopped")

// ExecuteBaselineWindow runs the blocks one after the other, each on its own,
// on exactly p workers: Wall is the sum of the blocks' walls, the time between
// them is not in it.
func ExecuteBaselineWindow(kind Baseline, envs []*Env, p int) ([]*Result, Stats, error) {
	out := make([]*Result, len(envs))
	var total Stats
	for i, env := range envs {
		var (
			res *Result
			st  Stats
			err error
		)
		if kind == BaselineOCC {
			res, st, err = executeBlockSTM(env, p)
		} else {
			res, st, err = executeLockPCC(env, p)
		}
		total.add(&st)
		total.BlockNs = append(total.BlockNs, st.Wall.Nanoseconds())
		if err != nil {
			return nil, total, fmt.Errorf("block %d: %w", env.Blocks[0].Fixture.Number, err)
		}
		out[i] = res
	}
	return out, total, nil
}

// add sums the counters a baseline fills.
func (s *Stats) add(o *Stats) {
	s.Wall += o.Wall
	s.Executions += o.Executions
	s.Aborts += o.Aborts
	s.CommitNs += o.CommitNs
}

// baseTxCounters is the common counters of a transaction, added to by whichever
// thread works on it.
type baseTxCounters struct {
	exec, aborts atomic.Int64
}

func (c *baseTxCounters) foldInto(s *Stats) {
	s.Executions += c.exec.Load()
	s.Aborts += c.aborts.Load()
}

// baseControl is what the two baselines do differently at a state access.
type baseControl interface {
	// read returns the value of k visible to the incarnation (ok false: the
	// block's pre-state) and the version it came from.
	read(r *baseReader, k Key) (Value, origin, bool)
	// lock takes a lock on k before a read or a write; Block-STM has none.
	lock(r *baseReader, k Key, x bool)
	// observed runs when the incarnation first depends on the given fields of
	// a loaded account.
	observed(r *baseReader, addr common.Address, bits uint8)
	// deployed returns code a transaction of the block deployed.
	deployed(h common.Hash) []byte
}

// baseRun is the outcome of one incarnation.
type baseRun struct {
	inc      int32
	reads    map[Key]origin // with used: what the incarnation depends on (see dependsOn)
	used     map[common.Address]uint8
	writes   []keyWrite
	fee      uint256.Int
	out      TxOutcome
	stopped  bool  // unwound by errBaseAbort
	rejected error // the message was refused
	err      error
}

// dependsOn reports whether a read matters: an account's fields are loaded
// together, and the transaction depends on those it observed.
func (run *baseRun) dependsOn(k Key) bool {
	return usedField(run.used[k.Addr], k.Kind)
}

// baseExec is the state of one block's execution shared by both baselines.
type baseExec struct {
	env   *Env
	bk    *BlockEnv
	n     int
	db    state.Database
	cc    baseControl
	start time.Time
}

func newBaseExec(env *Env, cc baseControl) *baseExec {
	return &baseExec{env: env, bk: env.Blocks[0], n: len(env.Messages), db: state.NewDatabaseForTesting(), cc: cc}
}

// baseReader is the state.Reader behind one incarnation's StateDB. It is used
// by a single goroutine.
type baseReader struct {
	ex     *baseExec
	j, inc int32

	reads      map[Key]origin
	readVals   map[Key]Value
	loadedAcc  map[common.Address]loadedAccount
	loadedSlot map[slotRef]common.Hash
	used       map[common.Address]uint8
	locks      map[Key]uint8 // PCC: 1 shared, 2 exclusive
	fees       []int32       // Block-STM: the incarnations of the fees summed into the beneficiary's balance
	feeLo      int32         // ... from this transaction up
	dep        int32         // Block-STM: the transaction the incarnation stopped on
}

func newBaseReader(ex *baseExec, j, inc int32) *baseReader {
	return &baseReader{
		ex: ex, j: j, inc: inc, dep: -1,
		reads:      originMaps.Get().(map[Key]origin),
		readVals:   valueMaps.Get().(map[Key]Value),
		loadedAcc:  accountMaps.Get().(map[common.Address]loadedAccount),
		loadedSlot: slotMaps.Get().(map[slotRef]common.Hash),
	}
}

// get serves k once per incarnation; the first value is its snapshot.
func (r *baseReader) get(k Key, lock bool) (Value, origin, bool) {
	if lock {
		r.ex.cc.lock(r, k, false)
	}
	if o, seen := r.reads[k]; seen {
		v, ok := r.readVals[k]
		return v, o, ok
	}
	v, o, ok := r.ex.cc.read(r, k)
	r.reads[k] = o
	if ok {
		r.readVals[k] = v
	}
	return v, o, ok
}

func (r *baseReader) baseAccount(addr common.Address) *types.StateAccount {
	return r.ex.bk.Base.accountShared(addr)
}

// Account implements state.StateReader: all the account's fields, each from
// its own key.
func (r *baseReader) Account(addr common.Address) (*types.StateAccount, error) {
	base := r.baseAccount(addr)
	la := loadedAccount{balance: new(uint256.Int), codeHash: types.EmptyCodeHash}
	if base != nil {
		la.balance, la.nonce, la.codeHash = base.Balance, base.Nonce, common.BytesToHash(base.CodeHash)
	}
	if v, _, ok := r.get(Key{Addr: addr, Kind: kindBalance}, false); ok {
		la.balance = v.Balance
	}
	if v, _, ok := r.get(Key{Addr: addr, Kind: kindNonce}, false); ok {
		la.nonce = v.Nonce
	}
	if v, _, ok := r.get(Key{Addr: addr, Kind: kindCode}, false); ok {
		la.codeHash = v.CodeHash
	}
	_, wiped, _ := r.get(Key{Addr: addr, Kind: kindWipe}, true)
	la.exists = !(la.nonce == 0 && la.balance.IsZero() && la.codeHash == types.EmptyCodeHash)
	r.loadedAcc[addr] = la
	if !la.exists {
		return nil, nil
	}
	root := types.EmptyRootHash
	if base != nil && wiped.tx < 0 {
		root = base.Root
	}
	return &types.StateAccount{Nonce: la.nonce, Balance: la.balance.Clone(), Root: root, CodeHash: la.codeHash.Bytes()}, nil
}

// Storage implements state.StateReader.
func (r *baseReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	v, so, ok := r.get(Key{Addr: addr, Slot: slot, Kind: kindStorage}, true)
	_, wo, _ := r.get(Key{Addr: addr, Kind: kindWipe}, true)
	var val common.Hash
	switch {
	case wo.tx > so.tx:
		// deleted after the last write to this slot
	case ok:
		val = v.Slot
	default:
		val, _ = r.ex.bk.Base.storageAt(addr, slot)
	}
	r.loadedSlot[slotRef{addr, slot}] = val
	return val, nil
}

// Has implements state.ContractCodeReader.
func (r *baseReader) Has(addr common.Address, codeHash common.Hash) bool {
	return len(r.Code(addr, codeHash)) > 0
}

// Code implements state.ContractCodeReader.
func (r *baseReader) Code(addr common.Address, codeHash common.Hash) []byte {
	if code := r.ex.cc.deployed(codeHash); code != nil {
		return code
	}
	return r.ex.bk.Base.Code(addr, codeHash)
}

// CodeSize implements state.ContractCodeReader.
func (r *baseReader) CodeSize(addr common.Address, codeHash common.Hash) int {
	return len(r.Code(addr, codeHash))
}

// baseStateDB wraps the StateDB of one incarnation: it records the account
// fields the transaction observes, the slots it writes, and keeps the
// beneficiary's fee out of the state.
type baseStateDB struct {
	*state.StateDB
	r         *baseReader
	coinbase  common.Address
	fee       uint256.Int
	used      map[common.Address]uint8
	written   map[slotRef]struct{}
	recreated map[common.Address]bool
}

// use records that the transaction observes these fields of addr. An account
// not loaded yet is locked first, so that the load is current.
func (s *baseStateDB) use(addr common.Address, bits uint8) {
	if s.used[addr]&bits == bits {
		return
	}
	s.r.ex.cc.observed(s.r, addr, bits)
	s.used[addr] |= bits
}

func (s *baseStateDB) exclusive(k Key) { s.r.ex.cc.lock(s.r, k, true) }

func (s *baseStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	if addr == s.coinbase && reason == tracing.BalanceIncreaseRewardTransactionFee {
		s.fee.Add(&s.fee, amount)
		return uint256.Int{}
	}
	if !amount.IsZero() { // a zero-value transfer touches the account but does not depend on its balance
		s.use(addr, usedBalance)
		s.exclusive(Key{Addr: addr, Kind: kindBalance})
	}
	return s.StateDB.AddBalance(addr, amount, reason)
}

func (s *baseStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	if !amount.IsZero() {
		if addr == s.coinbase && !s.fee.IsZero() {
			pending := s.fee
			s.fee.Clear()
			s.AddBalance(addr, &pending, tracing.BalanceChangeTransfer)
		}
		s.use(addr, usedBalance)
		s.exclusive(Key{Addr: addr, Kind: kindBalance})
	}
	return s.StateDB.SubBalance(addr, amount, reason)
}

func (s *baseStateDB) GetBalance(addr common.Address) *uint256.Int {
	s.use(addr, usedBalance)
	if addr == s.coinbase && !s.fee.IsZero() {
		return new(uint256.Int).Add(s.StateDB.GetBalance(addr), &s.fee)
	}
	return s.StateDB.GetBalance(addr)
}

func (s *baseStateDB) GetNonce(addr common.Address) uint64 {
	s.use(addr, usedNonce)
	return s.StateDB.GetNonce(addr)
}

func (s *baseStateDB) SetNonce(addr common.Address, nonce uint64, reason tracing.NonceChangeReason) {
	s.use(addr, usedNonce)
	s.exclusive(Key{Addr: addr, Kind: kindNonce})
	s.StateDB.SetNonce(addr, nonce, reason)
}

func (s *baseStateDB) GetCodeHash(addr common.Address) common.Hash {
	s.use(addr, usedCode)
	return s.StateDB.GetCodeHash(addr)
}

func (s *baseStateDB) GetCode(addr common.Address) []byte {
	s.use(addr, usedCode)
	return s.StateDB.GetCode(addr)
}

func (s *baseStateDB) GetCodeSize(addr common.Address) int {
	s.use(addr, usedCode)
	return s.StateDB.GetCodeSize(addr)
}

func (s *baseStateDB) SetCode(addr common.Address, code []byte, reason tracing.CodeChangeReason) []byte {
	s.use(addr, usedCode)
	s.exclusive(Key{Addr: addr, Kind: kindCode})
	return s.StateDB.SetCode(addr, code, reason)
}

func (s *baseStateDB) SetState(addr common.Address, key, value common.Hash) common.Hash {
	s.exclusive(Key{Addr: addr, Slot: key, Kind: kindStorage})
	s.written[slotRef{addr, key}] = struct{}{}
	return s.StateDB.SetState(addr, key, value)
}

// useExistence records the fields an existence or emptiness check depends on:
// an account with code exists whatever its balance, one with a nonce too.
func (s *baseStateDB) useExistence(addr common.Address) {
	switch {
	case s.StateDB.GetCodeHash(addr) != types.EmptyCodeHash && s.StateDB.GetCodeHash(addr) != (common.Hash{}):
		s.use(addr, usedCode)
	case s.StateDB.GetNonce(addr) != 0:
		s.use(addr, usedCode|usedNonce)
	default:
		s.use(addr, usedAll)
	}
}

func (s *baseStateDB) Exist(addr common.Address) bool {
	s.useExistence(addr)
	return s.StateDB.Exist(addr)
}

func (s *baseStateDB) Empty(addr common.Address) bool {
	s.useExistence(addr)
	return s.StateDB.Empty(addr)
}

func (s *baseStateDB) CreateAccount(addr common.Address) {
	s.use(addr, usedAll)
	if s.recreated == nil {
		s.recreated = make(map[common.Address]bool, 1)
	}
	s.recreated[addr] = true
	s.StateDB.CreateAccount(addr)
}

func (s *baseStateDB) CreateContract(addr common.Address) {
	s.use(addr, usedAll)
	s.StateDB.CreateContract(addr)
}

func (s *baseStateDB) SelfDestruct(addr common.Address) {
	s.use(addr, usedBalance)
	s.exclusive(Key{Addr: addr, Kind: kindBalance})
	s.StateDB.SelfDestruct(addr)
}

// collectWrites diffs the finalised state against what the reader served.
func (s *baseStateDB) collectWrites() []keyWrite {
	r := s.r
	out := make([]keyWrite, 0, 2*len(r.loadedAcc)+len(s.written))
	inner := s.StateDB
	for addr, la := range r.loadedAcc {
		if !inner.Exist(addr) {
			if la.exists {
				out = append(out,
					keyWrite{Key{Addr: addr, Kind: kindBalance}, Value{Balance: new(uint256.Int)}},
					keyWrite{Key{Addr: addr, Kind: kindNonce}, Value{}},
					keyWrite{Key{Addr: addr, Kind: kindCode}, Value{CodeHash: types.EmptyCodeHash}},
					keyWrite{Key{Addr: addr, Kind: kindWipe}, Value{}})
			}
			continue
		}
		if bal := inner.GetBalance(addr); !bal.Eq(la.balance) {
			out = append(out, keyWrite{Key{Addr: addr, Kind: kindBalance}, Value{Balance: bal.Clone()}})
		}
		if nonce := inner.GetNonce(addr); nonce != la.nonce {
			out = append(out, keyWrite{Key{Addr: addr, Kind: kindNonce}, Value{Nonce: nonce}})
		}
		if ch := inner.GetCodeHash(addr); ch != la.codeHash {
			out = append(out, keyWrite{Key{Addr: addr, Kind: kindCode}, Value{CodeHash: ch, Code: inner.GetCode(addr)}})
		}
	}
	seen := writtenMaps.Get().(map[slotRef]struct{})
	defer putMap(&writtenMaps, seen)
	emit := func(ref slotRef, before common.Hash) {
		if _, dup := seen[ref]; dup {
			return
		}
		seen[ref] = struct{}{}
		if !inner.Exist(ref.addr) {
			return // covered by the account wipe
		}
		if now := inner.GetState(ref.addr, ref.slot); now != before {
			out = append(out, keyWrite{Key{Addr: ref.addr, Slot: ref.slot, Kind: kindStorage}, Value{Slot: now}})
		}
	}
	for ref := range s.written {
		emit(ref, r.loadedSlot[ref])
	}
	if len(s.recreated) > 0 {
		for ref, before := range r.loadedSlot {
			if s.recreated[ref.addr] {
				emit(ref, before)
			}
		}
	}
	return out
}

// execute runs one incarnation. It writes nothing shared: the outcome is
// recorded by the caller.
func (ex *baseExec) execute(j, inc int32, counters *baseTxCounters) (run *baseRun, reader *baseReader) {
	counters.exec.Add(1)
	reader = newBaseReader(ex, j, inc)
	var sdb *baseStateDB
	defer func() {
		if p := recover(); p != nil {
			if p != errBaseAbort {
				panic(p)
			}
			run = &baseRun{inc: inc, stopped: true}
		}
		putMap(&valueMaps, reader.readVals)
		putMap(&accountMaps, reader.loadedAcc)
		putMap(&slotMaps, reader.loadedSlot)
		reader.readVals, reader.loadedAcc, reader.loadedSlot = nil, nil, nil
		if sdb != nil {
			putMap(&writtenMaps, sdb.written)
		}
		if run.stopped {
			putMap(&originMaps, reader.reads)
			if sdb != nil {
				putMap(&usedMaps, sdb.used)
			}
			reader.reads = nil
		}
	}()
	var inner *state.StateDB
	if v := stateDBs.Get(); v != nil {
		inner = v.(*state.StateDB)
		inner.ResetForReader(types.EmptyRootHash, ex.db, reader)
	} else {
		var err error
		if inner, err = state.NewWithReader(types.EmptyRootHash, ex.db, reader); err != nil {
			return &baseRun{err: err}, reader
		}
	}
	bk := ex.bk
	sdb = &baseStateDB{
		StateDB: inner, r: reader, coinbase: bk.Coinbase,
		used:    usedMaps.Get().(map[common.Address]uint8),
		written: writtenMaps.Get().(map[slotRef]struct{}),
	}
	reader.used = sdb.used
	evm := vm.NewEVM(bk.Context, sdb, ex.env.Config, vm.Config{})
	evm.SetJumpDestCache(jumpDests)
	defer evm.Release()
	sdb.SetTxContext(bk.Fixture.Txs[j].Hash(), int(j), uint32(j+1))
	res, err := core.ApplyMessage(evm, ex.env.Messages[j], core.NewGasPool(bk.Fixture.Header.GasLimit))
	if err != nil {
		return &baseRun{inc: inc, reads: reader.reads, used: sdb.used, out: TxOutcome{Failed: true}, rejected: err}, reader
	}
	sdb.Finalise(bk.Rules)
	writes := sdb.collectWrites()
	inner.ResetForReader(types.EmptyRootHash, ex.db, nil)
	stateDBs.Put(inner)
	return &baseRun{inc: inc, reads: reader.reads, used: sdb.used, writes: writes, fee: sdb.fee,
		out: TxOutcome{GasUsed: res.UsedGas, Failed: res.Failed()}}, reader
}

// baseShards hashes a key to a shard.
const baseShards = 1 << 10

func baseShard(k Key) int {
	return (int(k.Addr[18])<<8 ^ int(k.Addr[19]) ^ int(k.Slot[30])<<7 ^ int(k.Slot[31]) ^ int(k.Kind)<<4) & (baseShards - 1)
}

// baseFinal is what the view of a finished block asks the executor.
type baseFinal interface {
	final(k Key) (Value, int32, bool) // the block's last value of k and its writer
}

// baseView exposes the post-state of a baseline's block.
type baseView struct {
	f    baseFinal
	base *memReader
}

func (v *baseView) account(addr common.Address) loadedAccount {
	la := loadedAccount{balance: new(uint256.Int), codeHash: types.EmptyCodeHash}
	if b := v.base.accountShared(addr); b != nil {
		la.balance, la.nonce, la.codeHash = b.Balance, b.Nonce, common.BytesToHash(b.CodeHash)
	}
	if val, _, ok := v.f.final(Key{Addr: addr, Kind: kindBalance}); ok {
		la.balance = val.Balance
	}
	if val, _, ok := v.f.final(Key{Addr: addr, Kind: kindNonce}); ok {
		la.nonce = val.Nonce
	}
	if val, _, ok := v.f.final(Key{Addr: addr, Kind: kindCode}); ok {
		la.codeHash = val.CodeHash
	}
	la.exists = !(la.nonce == 0 && la.balance.IsZero() && la.codeHash == types.EmptyCodeHash)
	return la
}

func (v *baseView) BalanceHex(addr common.Address) string { return v.account(addr).balance.Hex() }
func (v *baseView) NonceOf(addr common.Address) uint64    { return v.account(addr).nonce }
func (v *baseView) Exists(addr common.Address) bool       { return v.account(addr).exists }

func (v *baseView) SlotOf(addr common.Address, slot common.Hash) common.Hash {
	sv, stx, sok := v.f.final(Key{Addr: addr, Slot: slot, Kind: kindStorage})
	_, wtx, wok := v.f.final(Key{Addr: addr, Kind: kindWipe})
	if wok && (!sok || wtx > stx) {
		return common.Hash{}
	}
	if sok {
		return sv.Slot
	}
	val, _ := v.base.storageAt(addr, slot)
	return val
}
