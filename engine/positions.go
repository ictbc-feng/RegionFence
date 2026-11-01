// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/holiman/uint256"
)

// TxPositions records, for one transaction, the instruction index at which each
// state key is first accessed and last written (-1 if never written). Keys use
// the rwsets naming: "a:<addr>" for an account's balance, "s:<addr>:<slot>".
type TxPositions struct {
	I   int                 `json:"i"`
	N   uint64              `json:"n"`
	Gas uint64              `json:"gas"`
	K   map[string][2]int64 `json:"k"`
}

type positionRecorder struct {
	pos  uint64
	keys map[string][2]int64
}

func (p *positionRecorder) touch(key string, write bool) {
	e, ok := p.keys[key]
	if !ok {
		e = [2]int64{int64(p.pos), -1}
	}
	if write {
		e[1] = int64(p.pos)
	}
	p.keys[key] = e
}

func accountKey(a common.Address) string { return fmt.Sprintf("a:0x%x", a.Bytes()) }

func slotKey(a common.Address, slot uint256.Int) string {
	b := slot.Bytes32()
	return fmt.Sprintf("s:0x%x:0x%x", a.Bytes(), b[:])
}

func stackBack(data []uint256.Int, n int) (uint256.Int, bool) {
	if len(data) <= n {
		return uint256.Int{}, false
	}
	return data[len(data)-1-n], true
}

func (p *positionRecorder) onOpcode(pc uint64, op byte, gas, cost uint64, scope tracing.OpContext, rData []byte, depth int, err error) {
	p.pos++
	data := scope.StackData()
	switch vm.OpCode(op) {
	case vm.SLOAD:
		if slot, ok := stackBack(data, 0); ok {
			p.touch(slotKey(scope.Address(), slot), false)
		}
	case vm.SSTORE:
		if slot, ok := stackBack(data, 0); ok {
			p.touch(slotKey(scope.Address(), slot), true)
		}
	case vm.BALANCE:
		if a, ok := stackBack(data, 0); ok {
			p.touch(accountKey(common.Address(a.Bytes20())), false)
		}
	case vm.SELFBALANCE:
		p.touch(accountKey(scope.Address()), false)
	case vm.CALL, vm.CALLCODE:
		to, ok1 := stackBack(data, 1)
		value, ok2 := stackBack(data, 2)
		if ok1 && ok2 && !value.IsZero() {
			p.touch(accountKey(scope.Address()), true)
			p.touch(accountKey(common.Address(to.Bytes20())), true)
		}
	}
}

// RecordPositions replays the fixture serially with an opcode tracer. The
// sender's account is recorded as read at position 0 and written at the end
// (gas refund).
func RecordPositions(fx *Fixture, env *Env) ([]TxPositions, error) {
	db, err := state.NewWithReader(types.EmptyRootHash, state.NewDatabaseForTesting(), env.Base)
	if err != nil {
		return nil, err
	}
	rec := &positionRecorder{}
	hooks := &tracing.Hooks{OnOpcode: rec.onOpcode}
	evm := vm.NewEVM(env.BlockContext, db, env.Config, vm.Config{Tracer: hooks})
	gp := core.NewGasPool(fx.Header.GasLimit)
	out := make([]TxPositions, len(env.Messages))
	for i, msg := range env.Messages {
		rec.pos, rec.keys = 0, make(map[string][2]int64)
		rec.touch(accountKey(msg.From), false)
		if msg.To != nil && msg.Value.Sign() != 0 {
			rec.touch(accountKey(*msg.To), true)
		}
		db.SetTxContext(fx.Txs[i].Hash(), i, uint32(i+1))
		res, err := core.ApplyMessage(evm, msg, gp)
		if err != nil {
			return nil, fmt.Errorf("block %d tx %d: %w", fx.Number, i, err)
		}
		db.Finalise(env.Rules)
		sender := accountKey(msg.From)
		e := rec.keys[sender]
		e[1] = int64(rec.pos) // the sender's balance is final after the refund
		rec.keys[sender] = e
		out[i] = TxPositions{I: i, N: rec.pos, Gas: res.UsedGas, K: rec.keys}
	}
	return out, nil
}
