// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

const (
	statusReady uint8 = iota
	statusRunning
	statusExecuted
	statusAborted
	statusCommitted
)

// errAbortIncarnation unwinds an incarnation whose reads became stale while it
// was running; execute recovers it and the transaction starts over.
var errAbortIncarnation = errors.New("regionfence: incarnation invalidated")

// A version stamp combines the writer's incarnation with a per-incarnation
// sequence number, so one incarnation can publish a key several times (early
// releases) and readers can tell the publications apart.
const (
	seqBits  = 12
	feeSeq   = 1<<seqBits - 1 // stamp of the fee credit to the beneficiary
	finalSeq = feeSeq - 1     // upper bound for publication sequence numbers
)

func stamp(inc, seq int32) int32 { return inc<<seqBits | seq }

type released struct {
	stamp int32
	val   Value
	t     int64 // when (UnixNano), for the gain a kept release brought (costModel)
}

// incarnation is the outcome of one execution attempt of a transaction.
type incarnation struct {
	inc                int32
	reads              map[Key]origin
	registered         map[Key]struct{}    // reads registered in the version store
	pending            []pendingRead       // reads the committer still has to validate (finish builds it; see commitWork)
	pendingWipes       []pendingRead       // storage-wipe markers read, validated only if an account was deleted in the window
	pendingBeneficiary []pendingRead       // reads of a beneficiary's balance: the committer's alone (prevalidate.go)
	aheadState         atomic.Int32        // aheadIdle...: the check ahead (prevalidate.go)
	aheadGeneration    int64               // the generation it was done at
	aheadPredicates    bool                // it covered the predicates
	relativeWrites     []int32             // indices in writes of the relative balance versions (commitWork)
	vals               map[Key]common.Hash // fields read from versions
	used               map[common.Address]uint8
	writes             []keyWrite
	released           map[Key]released // slots published early by RELEASE
	seq                int32            // publications so far
	fee                uint256.Int
	predicates         []balancePredicate // transfer sufficiency checks, validated at commit
	final              int32              // stamp of the final publication (publish)
	execNs             int64              // running time of this incarnation, blocked time excluded
	waits              []fenceWait
	out                TxOutcome
	aborted            bool  // unwound because it became stale
	err                error // infrastructure failure
	// rejected is set when the message itself was rejected (e.g. a stale nonce).
	// It is only an error if the incarnation's reads turn out to be current.
	rejected error
}

// Executor runs the transactions of one window in parallel under RegionFence.
type Executor struct {
	env             *Env
	n               int
	store           *store
	codes           sync.Map // code hash -> code deployed in this window
	blocks          []*BlockEnv
	blockIndex      []int32
	coinbases       map[common.Address]bool // beneficiaries of the window's blocks
	beneficiaryFrom []int32                 // per block: first window index sent by its beneficiary (n if none)
	prevSender      []int32                 // per transaction: the nearest lower transaction of the same sender in the window (-1 if none)
	// Fees of a block's committed transactions that are not in the version
	// store yet (creditFee): feeSum per block, the running sum, and feeAtFrontier
	// per transaction, the sum when the frontier reached it.
	feeSum        []atomic.Pointer[uint256.Int]
	feeAtFrontier []*uint256.Int
	// accountWritten is a Bloom filter of the account fields (balance, nonce,
	// code) that received a version in this window (see accountMaybeWritten).
	accountWritten [accountBloomWords]atomic.Uint64
	// Key signatures (signature.go): the table, this executor's id in its
	// writer lists, every transaction's features, the transactions carrying
	// each feature (ascending, read-only once built) and whether a
	// transaction has been learned from.
	signatures       *SignatureTable
	signatureRun     int64
	blockTables      []*SignatureTable // with NoCrossBlock: a table per block in place of signatures (tableFor)
	features         [][]feature
	featureIndex     map[feature][]int32
	featureRarity    [][]uint8
	signatureLearned []atomic.Bool
	// changedKeys: the storage keys a transaction's learned incarnation
	// changed (siblings of a key it invalidates a reader on).
	changedKeys []atomic.Pointer[[]Key]
	blockNs     []int64 // when each block's last transaction committed
	rebaseQ     []int32 // blocks whose boundary corrections are due
	rebased     []bool  // ... and done (the commit frontier waits for them)
	// adaptive lookahead state (under ex.mu)
	lookaheadNext  int32 // frontier at which the bound is next adjusted
	runAheadBound  int32
	runAheadShared atomic.Int32  // runAheadBound for readers outside ex.mu (admission)
	runAheadGrew   chan struct{} // runAheadBound grew: admission looks again (buffer 1)
	runAheadFloor  int32         // floor of the bound: a block's worth of transactions
	// the adaptive width (window.go; under ex.mu but firstRunNs): the width in
	// blocks, when each transaction first started, the last commit and the
	// block's shallow and deep waits so far
	width                  int32
	widthShared            atomic.Int32 // width for readers without ex.mu
	firstRunNs             []paddedInt64
	outerFirstRun          []atomic.Bool // first run in the window's outermost block
	lastCommitNs           int64
	outerRuns, outerReruns int64 // outermost first runs committed, those of them run again (decayed)
	boundIdleSeen          int64 // scheduler.boundIdle at the last decision
	shallowNs, deepNs      int64

	// the block-end interval: commits, their waits (all, on re-executions), the
	// admitted span summed over commits, first runs (ns, count)
	intervalCommits, intervalStallNs, intervalRerunStallNs, intervalSpan int64
	intervalRunNs, intervalRuns                                          atomic.Int64

	start     time.Time
	scheduler *scheduler
	committed atomic.Int32 // mirror of frontier readable without ex.mu
	db        state.Database
	jumps     *sharedJumpDests
	blocked   []paddedInt64  // blocked time of each transaction (written by its goroutine only)
	counters  []counterShard // per-transaction counters, 64 cache lines (see counterShard)
	predFail  []paddedBool   // a predicate of the transaction failed at commit: its next incarnation checks balances for real
	// rejectedRuns holds the incarnation of a transaction whose message was
	// rejected although its reads validated; the transaction is run once
	// more before the block is declared invalid (commitNext; ex.mu held).
	rejectedRuns map[int32]*incarnation

	mu sync.Mutex
	// Per-transaction state. txMu[j] guards the fields of transaction j that
	// are written by more than one goroutine (its own, invalidators, the
	// committer): status, inc, runs, written, signal, progress. status and
	// inc are also atomics so other transactions can read them without the
	// lock. ex.mu is left to the commit frontier and window-level state.
	txMu             []paddedMutex
	status           []paddedStatus
	inc              []paddedInt32
	stale            []paddedBool    // current incarnation read a value that is no longer current
	recheck          []paddedBool    // an account field it loaded changed; validate before publishing
	signal           []chan struct{} // closed when the current incarnation publishes its writes
	progress         []chan struct{} // closed on every publication (release or finish)
	done             chan struct{}   // closed when every transaction committed (or on failure)
	err              error           // first failure
	finished         []paddedBool    // the current incarnation has published its final writes
	frontier         int32           // every tx below it is committed
	commitWake       chan struct{}   // wakes the committer when it sleeps (buffer 1)
	commitAsleep     atomic.Bool     // the committer is about to sleep or sleeping
	procs            int32           // GOMAXPROCS when the window started
	publishedNs      []paddedInt64   // when each transaction last woke its waiters (broadcast)
	released         bool            // Result.Release done
	frontierWaiters  frontierHeap    // transactions blocked until the frontier reaches a threshold
	runs             []*incarnation
	written          [][]Key                      // keys holding a version of each tx
	wipes            atomic.Int64                 // account deletions published in this window
	abortNs          []paddedInt64                // when each transaction's last incarnation was invalidated (abort.go)
	waitOn           []paddedInt32                // 1 + the transaction each blocked transaction waits for (0: not blocked on one)
	keyGeneration    []atomic.Int64               // prevalidate.go: by key hash, the generation of the last change (nil: off)
	changeGeneration atomic.Int64                 // the last generation drawn
	changeAll        atomic.Int64                 // ... by a change that concerns any key
	aheadNext        atomic.Int32                 // the first transaction no helper has claimed
	aheadKick        chan struct{}                // wakes helpers without a processor to poll on
	sleepers         []paddedInt32                // transactions blocked on each one's publication (the hand-off in finish)
	finishedFront    atomic.Int32                 // every transaction below it has finished (an executed incarnation published), see lowerFinished
	probes           [probeDecisions]atomic.Int32 // costModel: acts left to this window on a decision learned never to pay (probe)
	chainOn          bool                         // the chain runs: signatures and more than one worker (with one, nothing runs beside a writer to take its release)
	publishedInc     []atomic.Int32               // 1 + the incarnation whose versions are installed, before it is marked executed (publishedCurrent)
	materializeNext  atomic.Int32                 // materialize.go: the next committed transaction whose relative balances are to be made absolute (claimed by CAS)
	materializedUpTo atomic.Int32                 // materialize.go: every transaction below it is materialized
	materializedFlag []atomic.Bool                // materialize.go: per transaction, materialized (helpers finish out of order)
	failKeys         [][]Key                      // chain.go: per transaction, the storage keys that invalidated its incarnations (under txMu)
	abortKey         []Key                        // the fence key of the transaction's last invalidation (txMu)
	doubts           [][]doubt                    // the values invalidated incarnations left, judged at the commit (abort.go; txMu)
	chainWaiters     []atomic.Int32               // chain.go: per writer, the transactions waiting for its publication of a key (waitPublished)
	chainSpin        []atomic.Bool                // chain.go: the transaction is in a signature wait, which may poll (spinChain)

	stats Stats
}

// Execute runs the transactions of an execution window (one or more
// consecutive blocks, see NewWindowEnv) and returns one result per block.
// The blocks are admitted one at a time: block b enters once the commit
// frontier is within the run-ahead bound of its first transaction, or the
// workers run out of work, so the depth of the pipeline follows the work
// (the adaptive width, window.go).
func Execute(env *Env, workers int) ([]*Result, Stats, error) {
	n := len(env.Messages)
	bases := make([]*memReader, len(env.Blocks))
	firsts := make([]int32, len(env.Blocks))
	coinbases := make(map[common.Address]bool, len(env.Blocks))
	beneficiaryFrom := make([]int32, len(env.Blocks))
	for i, b := range env.Blocks {
		bases[i], firsts[i] = b.Base, b.First
		coinbases[b.Coinbase] = true
		beneficiaryFrom[i] = int32(n)
		for j := b.First; j <= b.Last; j++ {
			if env.Messages[j].From == b.Coinbase {
				beneficiaryFrom[i] = j
				break
			}
		}
	}
	ex := &Executor{
		env:             env,
		n:               n,
		beneficiaryFrom: beneficiaryFrom,
		prevSender:      prevSameSender(env.Messages),
		feeSum:          make([]atomic.Pointer[uint256.Int], len(env.Blocks)),
		feeAtFrontier:   make([]*uint256.Int, n),
		store:           newStore(bases, firsts),
		blocks:          env.Blocks,
		blockIndex:      env.BlockIndex,
		coinbases:       coinbases,
		blockNs:         make([]int64, len(env.Blocks)),
		rebased:         make([]bool, len(env.Blocks)),
		db:              state.NewDatabaseForTesting(),
		jumps:           jumpDests,
		txMu:            make([]paddedMutex, n),
		status:          make([]paddedStatus, n),
		inc:             make([]paddedInt32, n),
		stale:           make([]paddedBool, n),
		recheck:         make([]paddedBool, n),
		signal:          make([]chan struct{}, n),
		progress:        make([]chan struct{}, n),
		done:            make(chan struct{}),
		finished:        make([]paddedBool, n),
		runs:            make([]*incarnation, n),
		written:         make([][]Key, n),
		blocked:         make([]paddedInt64, n),
		abortNs:         make([]paddedInt64, n),
		runAheadGrew:    make(chan struct{}, 1),
		publishedNs:     make([]paddedInt64, n),
		procs:           int32(runtime.GOMAXPROCS(0)),
		waitOn:          make([]paddedInt32, n),
		sleepers:        make([]paddedInt32, n),
		counters:        make([]counterShard, 64),
		predFail:        make([]paddedBool, n),
	}
	ex.probes[probeChain].Store(probeBudget)
	ex.probes[probeEstimate].Store(probeBudget)
	ex.probes[probeCascade].Store(probeBudget)
	ex.materializedFlag = make([]atomic.Bool, n)
	ex.publishedInc = make([]atomic.Int32, n)
	ex.abortKey = make([]Key, n)
	ex.doubts = make([][]doubt, n)
	if workers > 1 && Signatures != nil { // the chain runs on the signature table (its hot keys and writers)
		ex.chainOn = true
		ex.failKeys = make([][]Key, n)
		ex.chainSpin = make([]atomic.Bool, n)
		ex.chainWaiters = make([]atomic.Int32, n)
	}
	if workers > 1 {
		ex.keyGeneration = make([]atomic.Int64, keyGenerationSize)
		ex.aheadKick = make(chan struct{}, validationHelpers)
	}
	if Signatures != nil {
		ex.signatures, ex.signatureRun = Signatures, Signatures.runs.Add(1)
		if options.NoCrossBlock {
			ex.blockTables = make([]*SignatureTable, len(ex.blocks))
			for b := range ex.blockTables {
				ex.blockTables[b] = NewSignatureTable()
			}
		}
		ex.features, ex.featureIndex, ex.featureRarity = buildFeatureIndex(env.Messages, &Signatures.known)
		ex.signatureLearned = make([]atomic.Bool, n)
		ex.changedKeys = make([]atomic.Pointer[[]Key], n)
	}
	// no boundary corrections before the first block with transactions
	for b := range ex.blocks {
		ex.rebased[b] = true
		if ex.blocks[b].First <= ex.blocks[b].Last {
			break
		}
	}
	for i := range n {
		ex.signal[i] = make(chan struct{})
		ex.progress[i] = make(chan struct{})
	}
	ex.scheduler = newScheduler(workers, n, func(j int32) { pool.dispatch(ex, j) })
	ex.initRunAhead(workers)
	start := time.Now()
	ex.start = start
	if n == 0 {
		close(ex.done)
	}
	var driver sync.WaitGroup
	if n > 0 {
		ex.commitWake = make(chan struct{}, 1)
		driver.Add(1)
		go func() {
			defer driver.Done()
			ex.committer()
		}()
	}
	if ex.keyGeneration != nil && n > 0 {
		for range validationHelpers {
			driver.Add(1)
			go func() {
				defer driver.Done()
				ex.validator()
			}()
		}
	}
	ex.admit(&driver)
	<-ex.done
	driver.Wait()
	ex.stats.Wall = time.Since(start)
	ex.stats.BlockNs = ex.blockNs
	ex.stats.IdleNs = ex.scheduler.idleTime().Nanoseconds()
	ex.foldCounters()
	ex.foldCosts(&ex.stats)
	if ex.err != nil {
		return nil, ex.stats, fmt.Errorf("block %d: %w", env.Blocks[0].Fixture.Number, ex.err)
	}
	out := make([]*Result, len(env.Blocks))
	for b := range env.Blocks {
		out[b] = ex.blockResult(b)
	}
	return out, ex.stats, nil
}

// initRunAhead sets the run-ahead bound the window starts with: a block is
// admitted only once everything within the bound is dispatched.
func (ex *Executor) initRunAhead(workers int) {
	n := ex.n
	// The floor is a block's worth of transactions: below that the
	// pipeline could not even hold the next block.
	if nb := 0; len(ex.blocks) > 0 {
		for _, b := range ex.blocks {
			if b.First <= b.Last {
				nb++
			}
		}
		if nb > 0 {
			ex.runAheadFloor = int32(n / nb)
		}
	}
	ex.runAheadBound = max(int32(4*workers), 2*ex.runAheadFloor)
	if workers > 1 { // with one worker nothing runs ahead to bound
		ex.firstRunNs = make([]paddedInt64, n)
		ex.outerFirstRun = make([]atomic.Bool, n)
		ex.width = lastWidth.Load()
		if ex.width <= 0 {
			ex.width = initialWidth
		}
		ex.widthShared.Store(ex.width)
		ex.runAheadBound = ex.widthBoundLocked()
	}
	ex.runAheadShared.Store(ex.runAheadBound)
	ex.scheduler.limit.Store(ex.runAheadBound) // the frontier is at 0
	ex.lookaheadNext = lookaheadEvery
}

// admit starts the window's admission on a goroutine of driver: a block enters
// when the run-ahead bound reaches its first transaction, or when the queue
// runs dry.
func (ex *Executor) admit(driver *sync.WaitGroup) {
	drained := make(chan struct{}, 1)
	ex.scheduler.onDrained = func() {
		select {
		case drained <- struct{}{}:
		default:
		}
	}
	var nextAdmit atomic.Int32 // the first block not admitted yet
	ex.scheduler.more = func() bool { return int(nextAdmit.Load()) < len(ex.blocks) }
	admit := func(b int) {
		bk := ex.blocks[b]
		nextAdmit.Store(int32(b + 1))
		if bk.First <= bk.Last {
			ex.scheduler.pushRange(bk.First, bk.Last+1)
		}
	}
	driver.Add(1)
	go func() {
		defer driver.Done()
		admit(0)
		for b := 1; b < len(ex.blocks); b++ {
			bk := ex.blocks[b]
			// The depth of the pipeline is the run-ahead bound's: the
			// block is admitted as soon as its first transaction is within the
			// bound of the commit frontier, so that dispatch never
			// starves while the controller allows depth; an empty queue
			// admits it regardless (nothing else could run). It waits for
			// the frontier the block needs, not for every commit, which
			// would take ex.mu and a wake-up of the committer per commit.
			for {
				need := bk.First - ex.runAheadShared.Load()
				if ex.committed.Load() >= need {
					break
				}
				ch := ex.frontierReach(need)
				if ch == nil {
					continue
				}
				select {
				case <-ch:
				case <-ex.runAheadGrew:
				case <-drained:
					ch = nil
				case <-ex.done:
					return
				}
				if ch == nil {
					break
				}
			}
			admit(b)
		}
	}()
}

// setRunAheadBound sets the run-ahead bound (ex.mu held); admission waits for
// the frontier a block needs under it, so a larger bound wakes it.
func (ex *Executor) setRunAheadBound(v int32) {
	grew := v > ex.runAheadBound
	ex.runAheadBound = v
	ex.runAheadShared.Store(v)
	if grew {
		select {
		case ex.runAheadGrew <- struct{}{}:
		default:
		}
	}
}

// blockResult is the result of block b: its post-state view and outcomes.
func (ex *Executor) blockResult(b int) *Result {
	bk := ex.blocks[b]
	res := &Result{State: &versionView{ex: ex, upto: bk.Last + 1, blk: int32(b)}, ex: ex}
	for j := bk.First; j <= bk.Last; j++ {
		res.Txs = append(res.Txs, ex.runs[j].out)
	}
	return res
}

// blockOf returns the block transaction j belongs to.
func (ex *Executor) blockOf(j int32) *BlockEnv { return ex.blocks[ex.blockIndex[j]] }

// prevSameSender returns, per message, the index of the nearest lower message
// of the same sender (-1 if none): the only writers of a sender's nonce.
func prevSameSender(msgs []*core.Message) []int32 {
	prev := make([]int32, len(msgs))
	last := make(map[common.Address]int32, len(msgs))
	for j, m := range msgs {
		if p, ok := last[m.From]; ok {
			prev[j] = p
		} else {
			prev[j] = -1
		}
		last[m.From] = int32(j)
	}
	return prev
}

// upfrontCost is the balance core's buyGas requires of the sender before
// anything runs: gas limit times fee cap (the gas price before EIP-1559),
// the value, and the blob gas at its fee cap. ok is false on overflow, where
// buyGas rejects the message whatever the balance.
func upfrontCost(msg *core.Message, cancun bool) (cost *uint256.Int, ok bool) {
	cost = new(uint256.Int).SetUint64(msg.GasLimit)
	price := msg.GasPrice
	if msg.GasFeeCap != nil {
		price = msg.GasFeeCap
	}
	if _, overflow := cost.MulOverflow(cost, price); overflow {
		return nil, false
	}
	if msg.Value != nil {
		if _, overflow := cost.AddOverflow(cost, msg.Value); overflow {
			return nil, false
		}
	}
	if cancun && len(msg.BlobHashes) > 0 {
		blob := new(uint256.Int).SetUint64(uint64(len(msg.BlobHashes) * params.BlobTxBlobGasPerBlob))
		if _, overflow := blob.MulOverflow(blob, msg.BlobGasFeeCap); overflow {
			return nil, false
		}
		if _, overflow := cost.AddOverflow(cost, blob); overflow {
			return nil, false
		}
	}
	return cost, true
}

// lazyCoinbase reports whether transaction j may treat addr, its block's
// beneficiary, lazily: the beneficiary has not sent a
// transaction below j, has no code, and provably exists in the pre-state.
// The returned account is that pre-state.
func (ex *Executor) lazyCoinbase(j int32, addr common.Address) (*types.StateAccount, bool) {
	b := ex.blockIndex[j]
	if addr != ex.blocks[b].Coinbase || j >= ex.beneficiaryFrom[b] {
		return nil, false
	}
	acc, _ := ex.store.baseAccount(addr, b)
	if acc == nil || (acc.Balance.IsZero() && acc.Nonce == 0) {
		return nil, false
	}
	if h := common.BytesToHash(acc.CodeHash); h != types.EmptyCodeHash && h != (common.Hash{}) {
		return nil, false
	}
	return acc, true
}

// worker holds a slot and runs transactions until the scheduler has nothing
// for it (or gives its slot to a resumed transaction).
func (ex *Executor) worker(j int32) {
	for {
		ex.runTx(j)
		var ok bool
		if j, ok = ex.scheduler.next(); !ok {
			return
		}
	}
}

// runTx executes transaction j until an incarnation is published. If that
// incarnation is later aborted, the transaction is queued again.
func (ex *Executor) runTx(j int32) {
	for {
		ex.txMu[j].Lock()
		inc := ex.inc[j].Load()
		ex.status[j].set(statusRunning)
		ex.stale[j].Store(false)
		ex.recheck[j].Store(false)
		ex.waitOn[j].Store(0) // a wait the last incarnation unwound from
		if ex.chainSpin != nil {
			ex.chainSpin[j].Store(false) // a wait the last incarnation unwound from
		}
		ex.txMu[j].Unlock()

		if ex.firstRunNs != nil && inc == 0 {
			ex.firstRunNs[j].Store(time.Now().UnixNano())
			ex.noteFirstRun(j)
		}
		t0, b0 := time.Now(), ex.blocked[j].Load()
		run := ex.execute(j, inc)
		run.execNs = time.Since(t0).Nanoseconds() - (ex.blocked[j].Load() - b0)
		if ex.firstRunNs != nil && inc == 0 {
			ex.intervalRunNs.Add(run.execNs)
			ex.intervalRuns.Add(1)
		}

		if timelineFile != nil {
			ex.timeline(j, "S %d %d %d %d %d\n", ex.blockOf(j).Fixture.Number, j, inc, t0.Sub(ex.start).Nanoseconds(), time.Since(ex.start).Nanoseconds())
		}
		if inc != 0 {
			ex.add(j, counterReexecNs, run.execNs)
		}
		if run.err != nil {
			ex.mu.Lock()
			ex.failLocked(fmt.Errorf("tx %d: %w", j, run.err))
			ex.mu.Unlock()
			return
		}
		ok := ex.finish(j, run)
		if timelineFile != nil {
			ex.timeline(j, "F %d %d %d %d %t\n", ex.blockOf(j).Fixture.Number, j, inc, time.Since(ex.start).Nanoseconds(), ok)
		}
		if ok {
			return
		}
		atomic.AddInt64(&ex.stats.WastedNs, run.execNs)
		if ex.failKeys != nil {
			ex.txMu[j].Lock()
			ex.noteRisky(j, run)
			ex.txMu[j].Unlock()
		}
		run.release() // discarded: nothing references its maps
	}
}

// failLocked stops the execution with an error. Transactions still blocked are
// abandoned.
func (ex *Executor) failLocked(err error) {
	if ex.err == nil {
		ex.err = err
		close(ex.done)
	}
}

// execute runs one incarnation against the multi-version store.
func (ex *Executor) execute(j, inc int32) (run *incarnation) {
	ex.add(j, counterExecutions, 1)
	reader := newVersionReader(ex, j, inc)
	if ex.failKeys != nil {
		ex.txMu[j].Lock()
		reader.failed = ex.failKeys[j] // appended to under txMu only: this header stays valid
		ex.txMu[j].Unlock()
	}
	defer func() {
		reader.releaseSets()
		if r := recover(); r != nil {
			if r != errAbortIncarnation {
				panic(r)
			}
			atomic.AddInt64(&ex.stats.ReleaseAborted, int64(len(reader.released)))
			run = &incarnation{inc: inc, aborted: true, released: reader.released, reads: reader.reads, registered: reader.registered}
		}
	}()
	var inner *state.StateDB
	if v := stateDBs.Get(); v != nil {
		inner = v.(*state.StateDB)
		inner.ResetForReader(types.EmptyRootHash, ex.db, reader)
	} else {
		var err error
		if inner, err = state.NewWithReader(types.EmptyRootHash, ex.db, reader); err != nil {
			return &incarnation{err: err}
		}
	}
	bk := ex.blockOf(j)
	sdb := newFenceStateDB(inner, reader, bk.Coinbase, ex.env.Messages[j].From)
	sdb.exactBalances = ex.predFail[j].Load()
	if !sdb.exactBalances {
		if cost, ok := upfrontCost(ex.env.Messages[j], bk.Rules.IsCancun); ok {
			sdb.senderRel, sdb.upfront, reader.senderRel = true, cost, true
		}
	}
	reader.used = sdb.used
	evm := vm.NewEVM(bk.Context, sdb, ex.env.Config, vm.Config{})
	evm.SetJumpDestCache(ex.jumps)
	defer evm.Release() // return the stack arena to the pool, also on abort
	lj := int(j - bk.First)
	sdb.SetTxContext(bk.Fixture.Txs[lj].Hash(), lj, uint32(lj+1))
	res, err := core.ApplyMessage(evm, ex.env.Messages[j], core.NewGasPool(bk.Fixture.Header.GasLimit))
	if err != nil {
		// A speculative read can make a valid transaction look invalid (e.g. a
		// stale nonce); it is retried once validation catches the stale read.
		// The predicates go with it: a gas purchase refused against a
		// speculative balance is decided by its predicate at commit.
		return &incarnation{inc: inc, reads: reader.reads, registered: reader.registered, vals: reader.vals, used: sdb.used, released: reader.released,
			seq: reader.seq, predicates: sdb.predicates, out: TxOutcome{Failed: true}, rejected: err}
	}
	sdb.Finalise(bk.Rules)
	writes := sdb.collectWrites()
	reader.releaseExecMaps()
	putMap(&writtenMaps, sdb.written)
	inner.ResetForReader(types.EmptyRootHash, ex.db, nil) // drop references before pooling
	stateDBs.Put(inner)
	return &incarnation{
		vals:       reader.vals,
		inc:        inc,
		reads:      reader.reads,
		registered: reader.registered,
		used:       sdb.used,
		writes:     writes,
		released:   reader.released,
		seq:        reader.seq,
		waits:      reader.waits,
		fee:        sdb.fee,
		predicates: sdb.predicates,
		out:        TxOutcome{GasUsed: res.UsedGas, Failed: res.Failed()},
	}
}

// finish publishes an incarnation's writes and tries to advance the frontier.
// It returns false if the incarnation was invalidated while running, in which
// case the caller re-executes right away.
func (ex *Executor) finish(j int32, run *incarnation) bool {
	if run.aborted || !ex.recheckOK(j, run) {
		if timelineFile != nil {
			ex.timelineDiscard(j, run, map[bool]string{true: "unwound", false: "recheck"}[run.aborted])
		}
		if ex.chainOn && len(run.released) > 0 {
			ex.countReleaseVictims(j, run)
		}
		ex.discard(j)
		return false
	}
	ex.publish(j, run)
	// The versions are in: whoever waits for them goes on now, not after the
	// feedback, the pre-registration and the committer's slice below (a
	// reader that did not wait can read them already). A wait for a key this
	// incarnation did not write ends here too (publishedCurrent), as it would at
	// the flip.
	ex.publishedInc[j].Store(run.inc + 1)
	if ex.waited(j) {
		ex.txMu[j].Lock()
		ex.broadcast(j)
		ex.txMu[j].Unlock()
	}
	ex.learnSignatures(j, run)
	if !ex.recheckOK(j, run) {
		if timelineFile != nil {
			ex.timelineDiscard(j, run, "published")
		}
		ex.discard(j) // invalidated while publishing
		return false
	}
	if !ex.preRegister(j, run) {
		if timelineFile != nil {
			ex.timelineDiscard(j, run, "preregister")
		}
		ex.discard(j) // a read went stale since it ran
		return false
	}
	ex.commitWork(j, run)
	ex.txMu[j].Lock()
	if ex.stale[j].Load() {
		if timelineFile != nil {
			ex.timelineDiscard(j, run, "flip")
		}
		ex.discardTx(j)
		ex.txMu[j].Unlock()
		return false
	}
	ex.runs[j] = run
	ex.status[j].set(statusExecuted)
	ex.finished[j].Store(true)
	ex.advanceFinishedFront()
	woke := ex.sleepers[j].Load() > 0
	close(ex.signal[j])
	ex.broadcast(j)
	ex.txMu[j].Unlock()
	if f := ex.committed.Load(); ex.keyGeneration != nil && (f == j || int(j-f) >= ex.scheduler.workers) {
		ex.kickValidators()
	}
	// The hand-off: a finisher whose publication woke a blocked transaction
	// yields its processor, so that the woken goroutine, which the Go
	// scheduler queued next on this processor, runs at once on this core,
	// with the writer's versions in its caches and without waking a thread on
	// a sleeping core. On a dependency chain every hop would pay that wake-up.
	// The frontier transaction's finisher yields only after the commits it
	// drives.
	front := woke && ex.committed.Load() == j
	if woke && !front {
		runtime.Gosched()
	}
	ex.wakeCommitter(j)
	ex.helpMaterialize() // the committed transactions' relative balances, off the commit loop
	if front {
		runtime.Gosched()
	}
	return true
}

// lookaheadEvery is the commit interval of the adaptive lookahead controller.
const lookaheadEvery = 32

// adjustLookaheadLocked sets the run-ahead bound by what the window has paid
// for so far: slot time left idle while the bound alone held back queued work
// (too shallow) against running time thrown away in discarded incarnations
// (too deep). The bound grows while idling costs more than waste and shrinks
// otherwise, by a step proportional to the imbalance, never less than a
// slot's worth. Both are measured times of the same unit, so no threshold is
// involved, and the whole window's balance names the regime where one
// interval's would chase the delayed, noisy effect of each change. With more
// than one worker the width moves the bound instead (applyWidthLocked).
func (ex *Executor) adjustLookaheadLocked() {
	ex.lookaheadNext = ex.frontier + lookaheadEvery
	if ex.firstRunNs != nil {
		return
	}
	idle := float64(ex.scheduler.boundIdleTime().Nanoseconds())
	wasted := float64(atomic.LoadInt64(&ex.stats.WastedNs))
	bal := (idle - wasted) / (idle + wasted + 1)
	step := int32(float64(ex.runAheadBound) * bal / 2)
	if w := int32(ex.scheduler.workers); step > -w && step < w {
		if bal >= 0 {
			step = w
		} else {
			step = -w
		}
	}
	lo, hi := int32(2*ex.scheduler.workers), int32(ex.n)
	ex.setRunAheadBound(min(hi, max(lo, ex.runAheadBound+step)))
	ex.scheduler.setLimit(ex.frontier + ex.runAheadBound)
}

// invalidate tells incarnation rec.inc of rec.tx that a read went stale. For a
// weak read (an account field) it first checks whether rec.tx used the field
// and whether the value changed.
func (ex *Executor) invalidate(rec readRec, k Key, writer int32) {
	r, inc := rec.tx, rec.inc
	ex.txMu[r].Lock()
	defer ex.txMu[r].Unlock()
	if ex.inc[r].Load() != inc {
		return
	}
	switch ex.status[r].get() {
	case statusRunning:
		if rec.weak {
			ex.recheck[r].Store(true)
			if timelineFile != nil {
				ex.timeline(r, "A %d %d %d weak %d %d %d %d %d %s %t\n", ex.blockOf(r).Fixture.Number, r, inc, k.Kind, writer, rec.orig.tx, ex.inc[max(writer, 0)].Load(), time.Since(ex.start).Nanoseconds(), k.Addr.Hex(), k.Addr == ex.env.Messages[r].From)
			}
		} else {
			ex.countAbort(r, rec, k, writer, "running")
			ex.noteFailed(r, k)
			ex.stale[r].Store(true)
		}
	case statusExecuted:
		if rec.weak {
			run := ex.runs[r]
			if !usedField(run.used[k.Addr], k.Kind) {
				return
			}
			if o, ok := run.reads[k]; ok && ex.readCurrent(r, k, o, run.vals) {
				return
			}
		}
		ex.countAbort(r, rec, k, writer, "executed")
		ex.noteFailed(r, k)
		ex.noteRisky(r, ex.runs[r])
		atomic.AddInt64(&ex.stats.Aborts, 1)
		ex.abortTx(r)
	}
}

// putVersionNode installs node, allocated by the caller, as j's version of k
// and invalidates the readers it made stale; it returns their number.
func (ex *Executor) putVersionNode(j int32, k Key, node *version) int {
	st, val := node.inc, node.val
	if k.Kind == kindWipe {
		ex.wipes.Add(1)
	}
	var sc sameCheck
	same := ex.sameField(k, val, &sc)
	if val.Rel {
		same = nil // a relative version changes every reader's sum
	}
	ex.markAccountWritten(k) // before the version is visible
	e := ex.store.getOrCreate(k)
	e.mu.Lock()
	e.putNode(node)
	victims := e.staleReaders(j, st, ex.committed.Load(), same)
	e.mu.Unlock()
	ex.noteChange(j, k)
	for _, v := range victims {
		if v.orig.tx == j && v.orig.inc>>seqBits < st>>seqBits && ex.inc[v.tx].Load() == v.inc {
			// a cascade the abort left to this republication (learnCascade)
			if t := ex.abortNs[j].Load(); t != 0 {
				ex.add(v.tx, counterCascades, 1)
				ex.add(v.tx, counterCascadeNs, time.Now().UnixNano()-t)
			}
		}
		ex.invalidate(v, k, j)
	}
	return len(victims)
}

// removeVersion drops j's version of k and invalidates the readers that saw it.
func (ex *Executor) removeVersion(j int32, k Key) int {
	e := ex.store.get(k)
	if e == nil {
		return 0
	}
	e.mu.Lock()
	e.remove(j)
	victims := e.readersOf(j)
	e.mu.Unlock()
	ex.noteChange(j, k)
	for _, v := range victims {
		ex.invalidate(v, k, j)
	}
	return len(victims)
}

// release publishes a slot while its writer is still running (RELEASE fence).
func (ex *Executor) release(j, st int32, k Key, val Value, again bool) {
	atomic.AddInt64(&ex.stats.Releases, 1)
	if timelineFile != nil {
		ex.timeline(j, "R %d %d %d %s %s %d\n", ex.blockOf(j).Fixture.Number, j, k.Kind, k.Addr.Hex(), k.Slot.Hex(), time.Since(ex.start).Nanoseconds())
	}
	victims := ex.putVersionNode(j, k, &version{tx: j, inc: st, val: val, rel: true})
	if again {
		atomic.AddInt64(&ex.stats.ReleaseSuperseded, 1)
		atomic.AddInt64(&ex.stats.ReleaseVictims, int64(victims))
	}
	ex.txMu[j].Lock()
	if !containsKey(ex.written[j], k) {
		ex.written[j] = append(ex.written[j], k)
	}
	ex.broadcast(j)
	ex.txMu[j].Unlock()
}

func containsKey(keys []Key, k Key) bool {
	for _, x := range keys {
		if x == k {
			return true
		}
	}
	return false
}

// publish installs the incarnation's final writes (keeping early releases whose
// value did not change) and drops every other version the transaction holds.
func (ex *Executor) publish(j int32, run *incarnation) {
	final := stamp(run.inc, min(run.seq+1, finalSeq))
	run.final = final
	// the keys an earlier incarnation of j published (only j's own
	// incarnations, one after another, change them)
	ex.txMu[j].Lock()
	stale := ex.written[j]
	failed := ex.abortKey[j]
	ex.txMu[j].Unlock()
	keys := make([]Key, 0, len(run.writes))
	nodes := make([]version, len(run.writes)) // the incarnation's versions, one allocation
	for i, w := range run.writes {
		if w.key.Kind == kindCode && len(w.val.Code) > 0 {
			ex.codes.Store(w.val.CodeHash, w.val.Code)
		}
		keys = append(keys, w.key)
		if run.inc > 0 && len(stale) > 0 && containsKey(stale, w.key) {
			if ov, ok := ex.store.latestBelow(w.key, j+1); ok && ov.tx == j {
				fk, _ := fenceKey(w.key)
				same := sameValue(w.key.Kind, ov.val, w.val)
				if fk != failed && !same && !ov.estimate() { // left tentative (markEstimates)
					ex.add(j, counterTentativeChanged, 1)
				}
				if w.key.Kind == kindBalance {
					volatileBalances.observe(w.key, same) // the invalidating key too (waitVolatile)
				}
			}
		}
		rel, wasReleased := run.released[w.key]
		if wasReleased && sameValue(w.key.Kind, rel.val, w.val) {
			atomic.AddInt64(&ex.stats.ReleaseKept, 1)
			if rel.t > 0 {
				atomic.AddInt64(&ex.stats.ReleaseGainNs, time.Now().UnixNano()-rel.t)
			}
			continue // the early release was already the final value
		}
		nodes[i] = version{tx: j, inc: final, val: w.val}
		victims := ex.putVersionNode(j, w.key, &nodes[i])
		if wasReleased {
			atomic.AddInt64(&ex.stats.ReleaseSuperseded, 1)
			atomic.AddInt64(&ex.stats.ReleaseVictims, int64(victims))
		}
	}
	ex.txMu[j].Lock()
	ex.written[j] = keys
	ex.txMu[j].Unlock()
	if len(stale) == 0 {
		return // a first incarnation: nothing earlier to withdraw
	}
	var fresh map[Key]struct{}
	if len(stale)*len(keys) > 256 {
		fresh = make(map[Key]struct{}, len(keys))
		for _, k := range keys {
			fresh[k] = struct{}{}
		}
	}
	for _, k := range stale {
		kept := false
		if fresh != nil {
			_, kept = fresh[k]
		} else {
			kept = containsKey(keys, k)
		}
		if !kept {
			victims := ex.removeVersion(j, k)
			if _, ok := run.released[k]; ok {
				atomic.AddInt64(&ex.stats.ReleaseSuperseded, 1)
				atomic.AddInt64(&ex.stats.ReleaseVictims, int64(victims))
			}
		}
	}
}

// The committer validates the account fields a transaction observed (the
// code of every contract it called, nonces, balances it looked at) by looking
// each one up, most of them fields nothing in the window wrote. A read that
// resolved to the pre-state of a field no transaction has given a version is
// current whatever happens later, so the publishers mark the fields they write
// in a Bloom filter and the committer looks the rest up no more.
const accountBloomWords = 1 << 14

func accountBloomBits(k Key) [2]uint32 {
	a := binary.LittleEndian.Uint64(k.Addr[4:12]) ^ uint64(k.Kind)*0x9e3779b97f4a7c15
	b := binary.LittleEndian.Uint64(k.Addr[12:20])*0xff51afd7ed558ccd ^ uint64(k.Kind)<<7
	return [2]uint32{uint32(a % (accountBloomWords * 64)), uint32(b % (accountBloomWords * 64))}
}

// markAccountWritten records that account field k has a version in this window.
func (ex *Executor) markAccountWritten(k Key) {
	if k.Kind == kindStorage {
		return
	}
	for _, bit := range accountBloomBits(k) {
		w := &ex.accountWritten[bit/64]
		if m := uint64(1) << (bit % 64); w.Load()&m == 0 {
			w.Or(m)
		}
	}
}

// accountMaybeWritten reports whether account field k may have a version in
// this window (false: certainly none).
func (ex *Executor) accountMaybeWritten(k Key) bool {
	for _, bit := range accountBloomBits(k) {
		if ex.accountWritten[bit/64].Load()&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// versionView exposes the committed post-state of one block of an execution.
type versionView struct {
	ex   *Executor
	upto int32 // versions below this window index
	blk  int32 // the block whose pre-state is the base
}

func (v *versionView) account(addr common.Address) loadedAccount {
	n := v.upto
	la := loadedAccount{balance: new(uint256.Int), codeHash: types.EmptyCodeHash}
	if base, _ := v.ex.store.baseAccount(addr, v.blk); base != nil {
		la.balance, la.nonce, la.codeHash = base.Balance, base.Nonce, common.BytesToHash(base.CodeHash)
	}
	if ver, ok := v.ex.store.latestBelowInBlock(Key{Addr: addr, Kind: kindBalance}, n, v.blk); ok {
		la.balance = ver.val.Balance
	}
	if ver, ok := v.ex.store.latestBelowInBlock(Key{Addr: addr, Kind: kindNonce}, n, v.blk); ok {
		la.nonce = ver.val.Nonce
	}
	if ver, ok := v.ex.store.latestBelowInBlock(Key{Addr: addr, Kind: kindCode}, n, v.blk); ok {
		la.codeHash = ver.val.CodeHash
	}
	la.exists = !(la.nonce == 0 && la.balance.IsZero() && la.codeHash == types.EmptyCodeHash)
	return la
}

func (v *versionView) BalanceHex(addr common.Address) string { return v.account(addr).balance.Hex() }
func (v *versionView) NonceOf(addr common.Address) uint64    { return v.account(addr).nonce }
func (v *versionView) Exists(addr common.Address) bool       { return v.account(addr).exists }

func (v *versionView) SlotOf(addr common.Address, slot common.Hash) common.Hash {
	n := v.upto
	sv, sok := v.ex.store.latestBelowInBlock(Key{Addr: addr, Slot: slot, Kind: kindStorage}, n, v.blk)
	wv, wok := v.ex.store.latestBelowInBlock(Key{Addr: addr, Kind: kindWipe}, n, v.blk)
	if wok && (!sok || wv.tx > sv.tx) {
		return common.Hash{}
	}
	if sok {
		return sv.val.Slot
	}
	return v.ex.store.baseStorage(addr, slot, v.blk)
}

// sharedJumpDests is a JUMPDEST analysis cache shared by all incarnations of a
// block, like the one a serial EVM keeps across the block's transactions.
type sharedJumpDests struct{ m sync.Map }

// jumpDests is the process-wide JUMPDEST analysis cache (by code hash); a
// per-window cache recomputed every block's bitmaps.
var jumpDests = new(sharedJumpDests)

// stateDBs recycles geth StateDBs between incarnations (see ResetForReader).
var stateDBs sync.Pool

func (c *sharedJumpDests) Load(h common.Hash) (vm.BitVec, bool) {
	if v, ok := c.m.Load(h); ok {
		return v.(vm.BitVec), true
	}
	return nil, false
}

func (c *sharedJumpDests) Store(h common.Hash, vec vm.BitVec) { c.m.Store(h, vec) }

// Release returns the window's version store and the committed incarnations'
// maps to their pools. Call it once the results have been compared; their
// State must not be read afterwards. Safe to call more than once, and on any
// of the window's block results.
func (r *Result) Release() {
	ex := r.ex
	if ex == nil || ex.err != nil {
		return
	}
	ex.mu.Lock()
	defer ex.mu.Unlock()
	if ex.released {
		return
	}
	ex.released = true
	for _, run := range ex.runs {
		if run != nil {
			run.release()
		}
	}
	ex.store.recycle()
}

type paddedBool struct {
	atomic.Bool
	_ [60]byte
}

type paddedInt64 struct {
	atomic.Int64
	_ [56]byte
}

// executedRun returns f's executed incarnation, if it is waiting to commit.
func (ex *Executor) executedRun(f int32) (*incarnation, bool) {
	ex.txMu[f].Lock()
	defer ex.txMu[f].Unlock()
	if ex.status[f].get() != statusExecuted {
		return nil, false
	}
	return ex.runs[f], true
}

type paddedMutex struct {
	sync.Mutex
	_ [56]byte
}

// paddedStatus is a transaction's status, written under its txMu and
// readable without it.
type paddedStatus struct {
	v atomic.Uint32
	_ [60]byte
}

func (p *paddedStatus) get() uint8  { return uint8(p.v.Load()) }
func (p *paddedStatus) set(s uint8) { p.v.Store(uint32(s)) }

type paddedInt32 struct {
	atomic.Int32
	_ [60]byte
}
