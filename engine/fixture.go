// SPDX-License-Identifier: LGPL-3.0-or-later

// Replay fixtures: a block together with the pre-state its transactions
// observe and the post-state they produce, as fetched from a tracing node.

package regionfence

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
)

// Account is a pre-state account: only the storage slots the block touches are
// present.
type Account struct {
	Balance  *uint256.Int
	Nonce    uint64
	Code     []byte
	CodeHash common.Hash
	Storage  map[common.Hash]common.Hash
}

// PostAccount is the expected value of every account field and slot written by
// the block's transactions. Nil fields were not changed.
type PostAccount struct {
	Balance     *uint256.Int
	Nonce       *uint64
	Deleted     bool
	CodeChanged bool
	Storage     map[common.Hash]common.Hash
}

// ReceiptExpect is the on-chain outcome of a transaction.
type ReceiptExpect struct {
	GasUsed uint64
	Status  uint64
}

// Fixture is one replayable block.
type Fixture struct {
	Number   uint64
	Hash     common.Hash
	Header   *types.Header
	Txs      types.Transactions
	Pre      map[common.Address]*Account
	Post     map[common.Address]*PostAccount
	Receipts []ReceiptExpect
}

type rawAccount struct {
	Balance *hexutil.U256     `json:"balance"`
	Nonce   uint64            `json:"nonce"`
	Code    *string           `json:"code"`
	Storage map[string]string `json:"storage"`
}

type rawPostAccount struct {
	Balance     *hexutil.U256     `json:"balance"`
	Nonce       *uint64           `json:"nonce"`
	Storage     map[string]string `json:"storage"`
	Deleted     bool              `json:"deleted"`
	CodeChanged bool              `json:"codeChanged"`
}

type rawReceipt struct {
	GasUsed hexutil.Uint64 `json:"gasUsed"`
	Status  hexutil.Uint64 `json:"status"`
}

type rawFixture struct {
	Block    json.RawMessage           `json:"block"`
	Pre      map[string]rawAccount     `json:"pre"`
	Post     map[string]rawPostAccount `json:"post"`
	Receipts []rawReceipt              `json:"receipts"`
}

type rawBlock struct {
	Hash         common.Hash          `json:"hash"`
	Transactions []*types.Transaction `json:"transactions"`
}

// LoadFixture reads data/state/<number>.json.gz; code blobs are resolved from
// the sibling code/ directory.
func LoadFixture(path string) (*Fixture, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	var raw rawFixture
	if err := json.NewDecoder(gz).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	header := new(types.Header)
	if err := json.Unmarshal(raw.Block, header); err != nil {
		return nil, fmt.Errorf("header of %s: %w", path, err)
	}
	var blk rawBlock
	if err := json.Unmarshal(raw.Block, &blk); err != nil {
		return nil, fmt.Errorf("transactions of %s: %w", path, err)
	}
	fx := &Fixture{
		Number: header.Number.Uint64(),
		Hash:   blk.Hash,
		Header: header,
		Txs:    blk.Transactions,
		Pre:    make(map[common.Address]*Account, len(raw.Pre)),
		Post:   make(map[common.Address]*PostAccount, len(raw.Post)),
	}
	codeDir := filepath.Join(filepath.Dir(path), "code")
	for addr, ra := range raw.Pre {
		acc := &Account{Nonce: ra.Nonce, CodeHash: types.EmptyCodeHash, Storage: parseStorage(ra.Storage)}
		acc.Balance = new(uint256.Int)
		if ra.Balance != nil {
			acc.Balance = (*uint256.Int)(ra.Balance)
		}
		if ra.Code != nil && *ra.Code != "" {
			blob, err := os.ReadFile(filepath.Join(codeDir, *ra.Code+".hex"))
			if err != nil {
				return nil, fmt.Errorf("code %s: %w", *ra.Code, err)
			}
			code, err := hexutil.Decode(strings.TrimSpace(string(blob)))
			if err != nil {
				return nil, fmt.Errorf("code %s: %w", *ra.Code, err)
			}
			acc.Code = code
			acc.CodeHash = crypto.Keccak256Hash(code)
		}
		fx.Pre[common.HexToAddress(addr)] = acc
	}
	for addr, rp := range raw.Post {
		pa := &PostAccount{Nonce: rp.Nonce, Deleted: rp.Deleted, CodeChanged: rp.CodeChanged, Storage: parseStorage(rp.Storage)}
		if rp.Balance != nil {
			pa.Balance = (*uint256.Int)(rp.Balance)
		}
		fx.Post[common.HexToAddress(addr)] = pa
	}
	for _, r := range raw.Receipts {
		fx.Receipts = append(fx.Receipts, ReceiptExpect{GasUsed: uint64(r.GasUsed), Status: uint64(r.Status)})
	}
	if len(fx.Receipts) != len(fx.Txs) {
		return nil, fmt.Errorf("%s: %d receipts for %d transactions", path, len(fx.Receipts), len(fx.Txs))
	}
	return fx, nil
}

func parseStorage(in map[string]string) map[common.Hash]common.Hash {
	out := make(map[common.Hash]common.Hash, len(in))
	for k, v := range in {
		out[common.HexToHash(k)] = common.HexToHash(v)
	}
	return out
}

// memReader serves the fixture pre-state as a state.Reader. It is read-only and
// safe for concurrent use.
type memReader struct {
	accounts map[common.Address]*Account
	codes    map[common.Hash][]byte
	shared   sync.Map // address -> *types.StateAccount, built once (accountShared)
}

// storageMarker is reported as the storage root of accounts with known storage,
// so that create-collision checks see a non-empty storage.
var storageMarker = common.HexToHash("0x01")

func newMemReader(pre map[common.Address]*Account) *memReader {
	r := &memReader{accounts: pre, codes: make(map[common.Hash][]byte)}
	for _, acc := range pre {
		if len(acc.Code) > 0 {
			r.codes[acc.CodeHash] = acc.Code
		}
	}
	return r
}

// Account implements state.StateReader. Accounts that are empty in the sense of
// EIP-161 are reported as non-existent; the tracer does not distinguish them, and
// it also lists zero-valued slots it saw being read.
func (r *memReader) Account(addr common.Address) (*types.StateAccount, error) {
	acc := r.accounts[addr]
	if acc == nil || (acc.Nonce == 0 && acc.Balance.IsZero() && len(acc.Code) == 0) {
		return nil, nil
	}
	root := types.EmptyRootHash
	for _, v := range acc.Storage {
		if v != (common.Hash{}) {
			root = storageMarker
			break
		}
	}
	return &types.StateAccount{
		Nonce:    acc.Nonce,
		Balance:  acc.Balance.Clone(),
		Root:     root,
		CodeHash: acc.CodeHash.Bytes(),
	}, nil
}

// accountShared is Account built once per account and shared: the version
// store's readers of the pre-state only read it.
func (r *memReader) accountShared(addr common.Address) *types.StateAccount {
	if v, ok := r.shared.Load(addr); ok {
		return v.(*types.StateAccount)
	}
	acc, _ := r.Account(addr)
	v, _ := r.shared.LoadOrStore(addr, acc)
	return v.(*types.StateAccount)
}

// has reports whether the snapshot holds the account at all (touched by the block).
func (r *memReader) has(addr common.Address) bool { return r.accounts[addr] != nil }

// storageAt returns the snapshot's value of a slot and whether the slot is in it.
func (r *memReader) storageAt(addr common.Address, slot common.Hash) (common.Hash, bool) {
	if acc := r.accounts[addr]; acc != nil {
		v, ok := acc.Storage[slot]
		return v, ok
	}
	return common.Hash{}, false
}

// Storage implements state.StateReader.
func (r *memReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	if acc := r.accounts[addr]; acc != nil {
		return acc.Storage[slot], nil
	}
	return common.Hash{}, nil
}

// Has implements state.ContractCodeReader.
func (r *memReader) Has(addr common.Address, codeHash common.Hash) bool {
	_, ok := r.codes[codeHash]
	return ok
}

// Code implements state.ContractCodeReader.
func (r *memReader) Code(addr common.Address, codeHash common.Hash) []byte {
	return r.codes[codeHash]
}

// CodeSize implements state.ContractCodeReader.
func (r *memReader) CodeSize(addr common.Address, codeHash common.Hash) int {
	return len(r.codes[codeHash])
}
