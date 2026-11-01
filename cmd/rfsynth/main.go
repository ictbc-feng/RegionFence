// SPDX-License-Identifier: GPL-3.0-or-later

// rfsynth writes synthetic replay fixtures: blocks of transactions against
// one contract (Synth.sol) whose access pattern is set by flags, so that each
// mechanism of the concurrency control can be measured on a workload that
// exercises it alone. The blocks are executed serially by the engine's own
// serial executor to produce the post-state and receipts, block after block
// on one evolving state, and are written in the format of the mainnet
// fixtures (data/state): rfreplay replays them with all its checks.
//
//	rfsynth -out data/synth/indep -fn work -loops 50
//	rfsynth -out data/synth/hot -fn work -hotfrac 1 -hotkeys 1
//	rfsynth -out data/synth/lowprob -fn work -hotfrac 0.1 -hotkeys 1
//	rfsynth -out data/synth/zipf -fn transfer -recipients 1000 -zipf 1.2
//	rfsynth -out data/synth/mix -fn defimix -hotfrac 0.2 -writefrac 0.5 -n 16 -pos 20 -pad 400
package main

import (
	"compress/gzip"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "embed"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	"regionfence/engine"
)

// synthCode is the runtime code of Synth.sol (solc 0.8.29 --optimize
// --evm-version cancun --no-cbor-metadata --bin-runtime).
//
//go:embed synth.bin-runtime
var synthCode string

var (
	synthAddr = common.HexToAddress("0x5157000000000000000000000000000000000001")
	miner     = common.HexToAddress("0x5157000000000000000000000000000000000c0b")
	hotPool   = common.HexToAddress("0x5157000000000000000000000000000000000b01")
	selWork   = []byte{0x3a, 0x35, 0xcd, 0x77}

	selTransfer = selector("transfer(address,uint256,uint256,uint256)")
	selRead     = selector("priceRead(address,uint256,uint256,uint256)")
	selSwap     = selector("swap(address,uint256,uint256,uint256,uint256)")
	selMixXfer  = selector("xfer(address,uint256,uint256,uint256,uint256)")
)

const (
	slotOwn  = 0  // mapping(address => uint256) own
	slotHot  = 1  // uint256[64] hot
	slotBal  = 65 // mapping(address => uint256) bal
	slotRes  = 66 // mapping(address => uint256) reserves
	slotPriv = 67 // mapping(address => mapping(uint256 => uint256)) priv

	swapIn = 1_000_000_000_000_000_000 // 1e18 into a pool of 1e30 per side: the price moves, the pool never drains
)

func selector(sig string) []byte { return crypto.Keccak256([]byte(sig))[:4] }

func privSlot(a common.Address, i uint64) common.Hash {
	base := mapSlot(a, slotPriv)
	return crypto.Keccak256Hash(word(i), base[:])
}

func mapSlot(a common.Address, base uint64) common.Hash {
	var buf [64]byte
	copy(buf[12:32], a[:])
	binary.BigEndian.PutUint64(buf[56:], base)
	return crypto.Keccak256Hash(buf[:])
}

func word(v uint64) []byte {
	var w [32]byte
	binary.BigEndian.PutUint64(w[24:], v)
	return w[:]
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// zipfTable returns the cumulative distribution of Zipf(theta) over n ranks:
// P(rank k) is proportional to (k+1)^-theta, theta >= 0, unlike rand.Zipf
// which needs theta > 1.
func zipfTable(theta float64, n int) []float64 {
	cdf := make([]float64, n)
	sum := 0.0
	for k := range cdf {
		sum += math.Pow(float64(k+1), -theta)
		cdf[k] = sum
	}
	for k := range cdf {
		cdf[k] /= sum
	}
	return cdf
}

func addrWord(a common.Address) []byte {
	var w [32]byte
	copy(w[12:], a[:])
	return w[:]
}

type sender struct {
	key   *ecdsa.PrivateKey
	addr  common.Address
	nonce uint64
}

func main() {
	var (
		out        = flag.String("out", "", "directory for <number>.json.gz and code/")
		blocks     = flag.Int("blocks", 100, "blocks")
		perBlock   = flag.Int("txs", 200, "transactions per block")
		from       = flag.Uint64("from", 30_000_000, "first block number")
		fn         = flag.String("fn", "work", "work | transfer | defimix")
		loops      = flag.Uint64("loops", 20, "work: hash rounds per call (the transaction's length)")
		hotFrac    = flag.Float64("hotfrac", 0, "work: share of calls that increment a shared counter; defimix: share of transactions on the hot pool")
		hotKeys    = flag.Int("hotkeys", 1, "work: shared counters the incrementing calls pick from, uniformly")
		hotFirst   = flag.Bool("hotfirst", false, "work: increment before the hash rounds instead of after")
		nSenders   = flag.Int("senders", 0, "distinct senders, each sending in turn (0: a fresh sender per transaction)")
		recipients = flag.Int("recipients", 1000, "transfer: recipients")
		zipfS      = flag.Float64("zipf", 1.2, "transfer: Zipf exponent of the recipients (>= 0)")
		writeFrac  = flag.Float64("writefrac", 0.5, "defimix: share of the hot-pool transactions that swap (the rest priceRead)")
		accesses   = flag.Uint64("n", 16, "defimix: distinct storage slots per transaction (>= 3)")
		pos        = flag.Uint64("pos", 20, "defimix, transfer: position of the hot access, % of the transaction's rounds and private slots before it")
		pad        = flag.Uint64("pad", 0, "defimix, transfer: hash rounds per transaction")
		seed       = flag.Int64("seed", 1, "random seed")
	)
	flag.Parse()
	if *out == "" || *hotKeys < 1 || *hotKeys > 64 || *pos > 100 || *accesses < 3 {
		flag.Usage()
		os.Exit(2)
	}
	if err := os.MkdirAll(filepath.Join(*out, "code"), 0o755); err != nil {
		fatal(err)
	}
	code := common.FromHex(strings.TrimSpace(synthCode))
	codeName := crypto.Keccak256Hash(code).Hex()
	if err := os.WriteFile(filepath.Join(*out, "code", codeName+".hex"), []byte(hexutil.Encode(code)), 0o644); err != nil {
		fatal(err)
	}
	rng := rand.New(rand.NewSource(*seed))
	signer := types.LatestSignerForChainID(params.MainnetChainConfig.ChainID)

	// the world: every account and slot any block has touched, at its latest value
	world := map[common.Address]*regionfence.Account{
		synthAddr: {Balance: new(uint256.Int), Code: code, CodeHash: crypto.Keccak256Hash(code), Storage: map[common.Hash]common.Hash{}},
		miner:     {Balance: new(uint256.Int), CodeHash: types.EmptyCodeHash, Storage: map[common.Hash]common.Hash{}},
	}
	rich := new(uint256.Int).Lsh(uint256.NewInt(1), 100)
	newSender := func(i int) *sender {
		var sk [32]byte
		binary.BigEndian.PutUint64(sk[24:], uint64(*seed)<<32|uint64(i)+1)
		key, err := crypto.ToECDSA(sk[:])
		if err != nil {
			fatal(err)
		}
		s := &sender{key: key, addr: crypto.PubkeyToAddress(key.PublicKey)}
		world[s.addr] = &regionfence.Account{Balance: new(uint256.Int).Set(rich), CodeHash: types.EmptyCodeHash, Storage: map[common.Hash]common.Hash{}}
		// starting values, so that no write creates a slot
		world[synthAddr].Storage[mapSlot(s.addr, slotOwn)] = common.BigToHash(big.NewInt(int64(i) + 1))
		world[synthAddr].Storage[mapSlot(s.addr, slotBal)] = common.BigToHash(new(big.Int).Lsh(big.NewInt(1), 64))
		if *fn == "defimix" {
			for j := uint64(0); j < *accesses; j++ {
				world[synthAddr].Storage[privSlot(s.addr, j)] = common.BigToHash(big.NewInt(int64(j) + 1))
			}
		}
		return s
	}
	var pool []*sender
	if *nSenders > 0 {
		for i := 0; i < *nSenders; i++ {
			pool = append(pool, newSender(i))
		}
	}
	for i := 0; i < 64; i++ {
		world[synthAddr].Storage[common.BigToHash(big.NewInt(slotHot+int64(i)))] = common.BigToHash(big.NewInt(1))
	}
	var rcpt []common.Address
	var zipf []float64
	if *fn == "transfer" {
		for i := 0; i < *recipients; i++ {
			a := common.BigToAddress(new(big.Int).Add(big.NewInt(0x5157<<16), big.NewInt(int64(i))))
			rcpt = append(rcpt, a)
			world[synthAddr].Storage[mapSlot(a, slotBal)] = common.BigToHash(big.NewInt(1))
		}
		zipf = zipfTable(*zipfS, *recipients)
	}
	if *fn == "defimix" {
		r := new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
		world[synthAddr].Storage[mapSlot(hotPool, slotRes)] = common.BigToHash(new(big.Int).Or(new(big.Int).Lsh(r, 128), r))
	}

	baseFee := big.NewInt(params.GWei)
	parent := common.Hash{}
	next := 0
	for b := 0; b < *blocks; b++ {
		number := *from + uint64(b)
		header := &types.Header{
			ParentHash:  parent,
			UncleHash:   types.EmptyUncleHash,
			Coinbase:    miner,
			TxHash:      types.EmptyTxsHash,
			ReceiptHash: types.EmptyReceiptsHash,
			Difficulty:  new(big.Int),
			Number:      new(big.Int).SetUint64(number),
			GasLimit:    uint64(*perBlock) * 3_000_000,
			Time:        1_750_000_000 + 12*uint64(b), // Prague, before Osaka
			Extra:       []byte("rfsynth"),
			BaseFee:     new(big.Int).Set(baseFee),
		}
		header.MixDigest = crypto.Keccak256Hash(word(number))
		zero := uint64(0)
		header.BlobGasUsed, header.ExcessBlobGas = &zero, &zero
		header.ParentBeaconRoot = &common.Hash{}
		header.WithdrawalsHash = &types.EmptyWithdrawalsHash
		header.RequestsHash = &types.EmptyRequestsHash

		// the block's transactions, and the accounts and slots they touch
		touched := map[common.Address]map[common.Hash]bool{synthAddr: {}, miner: {}}
		touch := func(a common.Address, slots ...common.Hash) {
			if touched[a] == nil {
				touched[a] = map[common.Hash]bool{}
			}
			for _, s := range slots {
				touched[a][s] = true
			}
		}
		var txs types.Transactions
		for t := 0; t < *perBlock; t++ {
			var s *sender
			if len(pool) > 0 {
				s = pool[next%len(pool)]
			} else {
				s = newSender(next)
			}
			next++
			touch(s.addr)
			var data []byte
			var gas uint64
			switch *fn {
			case "work":
				h := uint64(0)
				if rng.Float64() < *hotFrac {
					h = uint64(rng.Intn(*hotKeys)) + 1
					touch(synthAddr, common.BigToHash(big.NewInt(int64(slotHot+h-1))))
				}
				first := uint64(0)
				if *hotFirst {
					first = 1
				}
				data = append(append(append(append([]byte{}, selWork...), word(*loops)...), word(h)...), word(first)...)
				touch(synthAddr, mapSlot(s.addr, slotOwn))
				gas = 100_000 + 200*(*loops)
			case "transfer":
				to := rcpt[sort.SearchFloat64s(zipf, rng.Float64())]
				data = concat(selTransfer, addrWord(to), word(1), word(*pad), word(*pos))
				touch(synthAddr, mapSlot(s.addr, slotBal), mapSlot(to, slotBal))
				gas = 100_000 + 200*(*pad)
			case "defimix":
				// both draws on every transaction: settings differ only in their effect
				hot, write := rng.Float64() < *hotFrac, rng.Float64() < *writeFrac
				priv := *accesses - 2
				switch {
				case hot && write:
					data = concat(selSwap, addrWord(hotPool), word(swapIn), word(*accesses), word(*pad), word(*pos))
					touch(synthAddr, mapSlot(hotPool, slotRes))
				case hot:
					data = concat(selRead, addrWord(hotPool), word(*accesses), word(*pad), word(*pos))
					touch(synthAddr, mapSlot(hotPool, slotRes))
				default:
					// a recipient no other transaction names, so that a block without the pool has no conflict
					to := common.BigToAddress(big.NewInt(0x5158<<32 + int64(next)))
					world[synthAddr].Storage[mapSlot(to, slotBal)] = common.BigToHash(big.NewInt(1))
					data = concat(selMixXfer, addrWord(to), word(1), word(*accesses), word(*pad), word(*pos))
					touch(synthAddr, mapSlot(s.addr, slotBal), mapSlot(to, slotBal))
					priv--
				}
				touch(synthAddr, mapSlot(s.addr, slotOwn))
				for j := uint64(0); j < priv; j++ {
					touch(synthAddr, privSlot(s.addr, j))
				}
				gas = 100_000 + 10_000*(*accesses) + 200*(*pad)
			default:
				fatal(fmt.Errorf("unknown -fn %q", *fn))
			}
			tx, err := types.SignNewTx(s.key, signer, &types.DynamicFeeTx{
				ChainID: params.MainnetChainConfig.ChainID, Nonce: s.nonce, GasTipCap: big.NewInt(params.GWei),
				GasFeeCap: big.NewInt(3 * params.GWei), Gas: gas, To: &synthAddr, Data: data,
			})
			if err != nil {
				fatal(err)
			}
			s.nonce++
			txs = append(txs, tx)
		}

		// the pre-state: what the block touches, at its current value
		pre := map[common.Address]*regionfence.Account{}
		for a, slots := range touched {
			w := world[a]
			acc := &regionfence.Account{Balance: new(uint256.Int).Set(w.Balance), Nonce: w.Nonce, Code: w.Code, CodeHash: w.CodeHash, Storage: map[common.Hash]common.Hash{}}
			for s := range slots {
				acc.Storage[s] = w.Storage[s]
			}
			pre[a] = acc
		}
		fx := &regionfence.Fixture{Number: number, Header: header, Txs: txs, Pre: pre, Post: map[common.Address]*regionfence.PostAccount{}}
		env, err := regionfence.NewEnv(fx, params.MainnetChainConfig, nil)
		if err != nil {
			fatal(fmt.Errorf("block %d: %w", number, err))
		}
		res, err := regionfence.ExecuteSerial(fx, env)
		if err != nil {
			fatal(fmt.Errorf("block %d: %w", number, err))
		}
		var gasUsed uint64
		for i, o := range res.Txs {
			if o.Failed {
				fatal(fmt.Errorf("block %d tx %d failed", number, i))
			}
			gasUsed += o.GasUsed
			fx.Receipts = append(fx.Receipts, regionfence.ReceiptExpect{GasUsed: o.GasUsed, Status: 1})
		}
		header.GasUsed = gasUsed
		fx.Hash = header.Hash()

		// the post-state: every touched field that changed, carried into the world
		for a, slots := range touched {
			w := world[a]
			pa := &regionfence.PostAccount{Storage: map[common.Hash]common.Hash{}}
			bal, err := uint256.FromHex(res.State.BalanceHex(a))
			if err != nil {
				fatal(err)
			}
			if !bal.Eq(w.Balance) {
				pa.Balance = bal
				w.Balance = bal
			}
			if n := res.State.NonceOf(a); n != w.Nonce {
				pa.Nonce = &n
				w.Nonce = n
			}
			for s := range slots {
				if v := res.State.SlotOf(a, s); v != w.Storage[s] {
					pa.Storage[s] = v
					w.Storage[s] = v
				}
			}
			if pa.Balance != nil || pa.Nonce != nil || len(pa.Storage) > 0 {
				fx.Post[a] = pa
			}
		}
		if err := write(filepath.Join(*out, fmt.Sprintf("%d.json.gz", number)), fx, codeName); err != nil {
			fatal(err)
		}
		parent = fx.Hash
	}
	fmt.Printf("%d blocks of %d transactions in %s\n", *blocks, *perBlock, *out)
}

// write stores fx in the fixture format LoadFixture reads.
func write(path string, fx *regionfence.Fixture, codeName string) error {
	hj, err := json.Marshal(fx.Header)
	if err != nil {
		return err
	}
	var block map[string]any
	if err := json.Unmarshal(hj, &block); err != nil {
		return err
	}
	block["hash"] = fx.Hash
	block["transactions"] = fx.Txs
	type rawAccount struct {
		Balance *hexutil.U256     `json:"balance"`
		Nonce   uint64            `json:"nonce"`
		Code    *string           `json:"code"`
		Storage map[string]string `json:"storage"`
	}
	type rawPost struct {
		Balance *hexutil.U256     `json:"balance,omitempty"`
		Nonce   *uint64           `json:"nonce,omitempty"`
		Storage map[string]string `json:"storage"`
	}
	type rawReceipt struct {
		GasUsed hexutil.Uint64 `json:"gasUsed"`
		Status  hexutil.Uint64 `json:"status"`
	}
	storage := func(m map[common.Hash]common.Hash) map[string]string {
		out := make(map[string]string, len(m))
		for k, v := range m {
			out[k.Hex()] = v.Hex()
		}
		return out
	}
	pre := map[string]rawAccount{}
	for a, acc := range fx.Pre {
		ra := rawAccount{Balance: (*hexutil.U256)(acc.Balance), Nonce: acc.Nonce, Storage: storage(acc.Storage)}
		if len(acc.Code) > 0 {
			name := codeName
			ra.Code = &name
		}
		pre[a.Hex()] = ra
	}
	post := map[string]rawPost{}
	for a, pa := range fx.Post {
		post[a.Hex()] = rawPost{Balance: (*hexutil.U256)(pa.Balance), Nonce: pa.Nonce, Storage: storage(pa.Storage)}
	}
	var receipts []rawReceipt
	for _, r := range fx.Receipts {
		receipts = append(receipts, rawReceipt{GasUsed: hexutil.Uint64(r.GasUsed), Status: hexutil.Uint64(r.Status)})
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	if err := json.NewEncoder(gz).Encode(map[string]any{"block": block, "pre": pre, "post": post, "receipts": receipts}); err != nil {
		return err
	}
	return gz.Close()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "rfsynth:", err)
	os.Exit(1)
}
