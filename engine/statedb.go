// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"fmt"
	"strings"
	"sync"
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

// Account fields a transaction actually observed.
const (
	usedBalance uint8 = 1 << iota
	usedNonce
	usedCode
	usedAll = usedBalance | usedNonce | usedCode
)

type loadedAccount struct {
	exists   bool
	balance  *uint256.Int
	nonce    uint64
	codeHash common.Hash
}

type slotRef struct {
	addr common.Address
	slot common.Hash
}

// versionReader is the state.Reader behind the StateDB of one incarnation of one
// transaction: it serves the newest version written by a lower transaction (or
// the pre-state) and records what it served. It is used by a single goroutine.
type versionReader struct {
	ex     *Executor
	j, inc int32
	blk    int32 // the block of j

	reads      map[Key]origin
	registered map[Key]struct{} // reads registered in the version store (invalidated by push; see readsCurrent)
	readVals   map[Key]Value    // value of each key read from a version (absent: pre-state), for repeated reads
	loadedAcc  map[common.Address]loadedAccount
	loadedSlot map[slotRef]common.Hash

	used map[common.Address]uint8 // fields observed, shared with the fenceStateDB
	vals map[Key]common.Hash      // fields served by versions
	// senderRel: the sender is not an observed account in this incarnation
	// (fenceStateDB.senderRel): its fences are deferred like any other
	// account's and its nonce may be served from the transaction itself.
	senderRel bool
	released  map[Key]released
	pred      map[Key]predictedRead // values to serve from an assumption (predictNonce)
	// deferred holds the accounts whose WAIT / retry fences were not run when
	// the account was loaded: loading an account for its code, or for a
	// transfer the transaction never observes, depends on no writer of its
	// balance. The fence runs at the first observation of the balance
	// (observeBalance), and the balance the StateDB holds is refreshed then.
	deferred      map[common.Address]bool
	fenced        map[common.Address]bool         // accounts whose deferred fence has run (observed)
	balanceOffset map[common.Address]*uint256.Int // what a rebased account's balance adds to the StateDB's (fenceStateDB.rebase)
	// loadRegistration narrows registration while an account is loaded (lazy loads):
	// loadAll registers as usual, loadNoReg not at all, loadUncommitted
	// only a version an uncommitted writer published.
	loadRegistration uint8
	seq              int32
	waits            []fenceWait

	// failed: the storage keys that invalidated the transaction's earlier
	// incarnations (chain.go).
	failed []Key
	held   map[Key]common.Hash // hot keys past their learned last write, not released while nobody waited (flushHeld)
}

// Per-incarnation maps are recycled (clear keeps the buckets): the maps of a
// discarded incarnation right away, those an executed incarnation keeps for
// validation when the window's results are released (Result.Release).
var (
	originMaps     = sync.Pool{New: func() any { return make(map[Key]origin, 32) }}
	valueMaps      = sync.Pool{New: func() any { return make(map[Key]Value, 32) }}
	hashMaps       = sync.Pool{New: func() any { return make(map[Key]common.Hash, 32) }}
	accountMaps    = sync.Pool{New: func() any { return make(map[common.Address]loadedAccount, 8) }}
	slotMaps       = sync.Pool{New: func() any { return make(map[slotRef]common.Hash, 32) }}
	usedMaps       = sync.Pool{New: func() any { return make(map[common.Address]uint8, 8) }}
	registeredMaps = sync.Pool{New: func() any { return make(map[Key]struct{}, 16) }}
	writtenMaps    = sync.Pool{New: func() any { return make(map[slotRef]struct{}, 8) }}
)

func putMap[M ~map[K]V, K comparable, V any](pool *sync.Pool, m M) {
	if m != nil {
		clear(m)
		pool.Put(m)
	}
}

func newVersionReader(ex *Executor, j, inc int32) *versionReader {
	r := &versionReader{
		ex: ex, j: j, inc: inc, blk: ex.blockIndex[j],
		reads:      originMaps.Get().(map[Key]origin),
		registered: registeredMaps.Get().(map[Key]struct{}),
		readVals:   valueMaps.Get().(map[Key]Value),
		loadedAcc:  accountMaps.Get().(map[common.Address]loadedAccount),
		loadedSlot: slotMaps.Get().(map[slotRef]common.Hash),
		vals:       hashMaps.Get().(map[Key]common.Hash),
	}
	return r
}

// releaseExecMaps returns the maps only the execution itself needed.
func (r *versionReader) releaseExecMaps() {
	putMap(&valueMaps, r.readVals)
	putMap(&accountMaps, r.loadedAcc)
	putMap(&slotMaps, r.loadedSlot)
	r.readVals, r.loadedAcc, r.loadedSlot = nil, nil, nil
}

// release returns the maps an incarnation kept for validation.
func (run *incarnation) release() {
	putMap(&originMaps, run.reads)
	putMap(&hashMaps, run.vals)
	putMap(&usedMaps, run.used)
	putMap(&registeredMaps, run.registered)
	run.reads, run.vals, run.used, run.registered = nil, nil, nil, nil
}

// fence runs at a region boundary, the first access to k: the transaction
// waits for the unfinished lower writers the key signatures name (and, on a
// key that invalidated an earlier incarnation, for the lower transactions
// that read it).
func (r *versionReader) fence(k Key) {
	// Key signature: k is known to be contended and some of its writers'
	// features name unfinished lower transactions with enough probability:
	// wait for them, nearest first, down to the first that did write k. A wait
	// that turns out to be in vain counts against the feature that named it at
	// once, so the reader gives up on a feature after as many misses as its
	// evidence allows: no cap on the writers per key is needed.
	var vain [signatureSlots + 1]float64 // by naming feature (+1: the read-modify-write rule at 0)
	pstar := waitThreshold()
	// failed: k invalidated an earlier incarnation of this transaction.
	chain := r.ex.chainOn && k.Kind == kindStorage
	failed := chain && len(r.failed) > 0 && containsKey(r.failed, k)
	signatures := r.ex.writerCandidates(r.j, k, pstar)
	if failed {
		signatures = r.ex.failedTouchers(r.j, k, signatures, pstar)
	}
	if options.NoLearnedWaits {
		signatures = nil
	}
	if timelineFile != nil && k.Kind == kindStorage {
		var b strings.Builder
		for _, c := range r.ex.writerCandidates(r.j, k, 0) {
			fmt.Fprintf(&b, " %d:%.3f:%d:%t", c.tx, c.num/c.den, c.fi, r.ex.finished[c.tx].Load())
		}
		r.ex.timeline(r.j, "N %d %d %d %s %s %d %t%s\n", r.ex.blockOf(r.j).Fixture.Number, r.j, r.inc, k.Addr.Hex(), k.Slot.Hex(), time.Since(r.ex.start).Nanoseconds(), r.ex.signatures != nil && r.ex.tableFor(r.j).maybe(k), b.String())
	}
	for _, c := range signatures {
		w := c.tx
		if r.ex.finished[w].Load() {
			continue
		}
		if v := vain[c.fi+1]; v > 0 && c.num < pstar*(c.den+v) {
			continue
		}
		if c.num < pstar*c.den {
			continue // below the threshold: run past it
		}
		r.ex.add(r.j, counterSignatureWaits, 1)
		t0 := time.Now().UnixNano()
		if chain {
			r.ex.chainSpin[r.j].Store(true) // block may poll for a running writer (spinChain)
		}
		ns := r.waitPublished(w, k)
		if chain {
			r.ex.chainSpin[r.j].Store(false)
		}
		wrote := r.ex.writerWrote(w, k)
		released := false
		if !wrote && !r.ex.finished[w].Load() {
			// the wait ended on w's early release of k (chain.go): w wrote
			// it, and the reader has what it waited for; the wait is not in
			// vain and the writers below w need no wait
			if v, ok := r.ex.store.latestBelow(k, r.j); ok && v.tx == w && !v.estimate() {
				wrote, released = true, true
			}
		}
		changed := released
		if ws := r.ex.changedKeys[w].Load(); ws != nil && !released {
			changed = containsKey(*ws, k)
		}
		t1 := time.Now().UnixNano()
		if ns > 0 {
			r.waits = append(r.waits, fenceWait{ns: ns, vain: !changed, t0: t0, t1: t1})
		}
		if changed {
			r.ex.add(r.j, counterSignatureHits, 1)
		}
		if timelineFile != nil {
			r.ex.timeline(r.j, "D %d %d %d %d %s %s %d %d %.3f %t\n", r.ex.blockOf(r.j).Fixture.Number, r.j, r.inc, w, k.Addr.Hex(), k.Slot.Hex(), t1-r.ex.start.UnixNano(), ns, c.num/c.den, changed)
		}
		if wrote {
			break
		}
		vain[c.fi+1]++
	}
}

// predictedRead is a value served to this incarnation from an assumption (a
// sender's nonce, predictNonce) rather than from a published version.
type predictedRead struct {
	val   Value
	field common.Hash
	top   int32
	base  int32
	set   []int32
}

// release publishes the current value of k early, before the incarnation
// finishes (a hot key at its learned last write, chain.go).
func (r *versionReader) release(k Key, val Value) {
	r.checkStale()
	if r.seq+1 >= finalSeq {
		return // leave the rest to the final publication
	}
	r.seq++
	st := stamp(r.inc, r.seq)
	if r.released == nil {
		r.released = make(map[Key]released)
	}
	_, again := r.released[k]
	t0 := time.Now().UnixNano()
	r.released[k] = released{stamp: st, val: val, t: t0}
	r.ex.release(r.j, st, k, val, again)
	atomic.AddInt64(&r.ex.stats.ReleaseNs, time.Now().UnixNano()-t0)
}

// read returns the value of k visible to this transaction, waiting while the
// visible version is an ESTIMATE left by an aborted writer.
func (r *versionReader) read(k Key) (Value, origin, bool) {
	r.checkStale()
	if len(r.held) > 0 {
		r.flushHeld()
	}
	if o, seen := r.reads[k]; seen {
		// The first read registered this incarnation as a reader of k: a
		// lower writer publishing later invalidates it, so the first value is
		// the incarnation's snapshot and repeated reads need not touch the
		// store (nor can they observe a second version).
		v, ok := r.readVals[k]
		return v, o, ok
	}
	if p, ok := r.pred[k]; ok {
		r.ex.store.registerPredicted(k, r.j, r.inc, p.top, p.base, p.set, p.field)
		o := origin{p.top, predictedInc}
		r.record(k, o, p.val)
		r.readVals[k] = p.val
		return p.val, o, true
	}
	for {
		weak := k.Kind == kindBalance || k.Kind == kindNonce || k.Kind == kindCode
		// A read registers (a write to the key's shared line) while a lower
		// transaction has not finished: a re-execution of that writer then
		// invalidates this reader at once instead of at its own commit.
		v, ok := r.ex.store.latestBelow(k, r.j)
		uncommitted := ok && v.tx >= 0 && v.inc != boundaryInc && v.tx >= r.ex.committed.Load()
		reg := !r.ex.lowerFinished(r.j)
		switch r.loadRegistration {
		case loadNoReg:
			reg = false
		case loadUncommitted:
			reg = uncommitted
		}
		if reg {
			v, ok = r.ex.store.readRegistered(k, r.j, r.inc, weak)
			if !(ok && v.estimate()) {
				// registered (readRegistered registers nothing when the
				// visible version is an ESTIMATE: the read is retried below)
				r.registered[k] = struct{}{}
			}
		}
		if !ok {
			r.record(k, baseOrigin, v.val)
			return Value{}, baseOrigin, false
		}
		if v.estimate() {
			r.ex.waitEstimateKey(r.j, v.tx, k, v.mark) // until w's next incarnation releases or publishes k
			continue
		}
		o := origin{v.tx, v.inc}
		r.record(k, o, v.val)
		r.readVals[k] = v.val
		// a version its writer published is not a release's
		if !r.ex.finished[v.tx].Load() && r.ex.chainOn && !r.ex.publishedCurrent(v.tx) {
			r.ex.add(r.j, counterReleaseReads, 1)
		}
		return v.val, o, true
	}
}

// record remembers the version a first read of k observed and the field it
// served. read checks that k was not read before and returns the first value
// for every repeated read (the incarnation's snapshot), so a key never
// resolves to two versions in one incarnation.
func (r *versionReader) record(k Key, o origin, val Value) {
	r.reads[k] = o
	if o.tx >= 0 {
		r.vals[k] = fieldOf(k.Kind, val)
	}
}

// Account implements state.StateReader.
func (r *versionReader) Account(addr common.Address) (*types.StateAccount, error) {
	bk := Key{Addr: addr, Kind: kindBalance}
	sender := addr == r.ex.env.Messages[r.j].From
	// A beneficiary's balance is exact only once every lower transaction
	// has committed (fees are credited at commit). Loading its account does
	// not observe that balance: a transfer to it is a relative change, a
	// transaction it sends checks a predicate. So the wait for the frontier
	// is deferred with the account's fences to the first observation of the
	// balance (observeBalance): waiting at every load would chain each block's
	// last transaction, the builder's payment, behind the commit loop.
	if r.fenced[addr] {
		// observed already: the fence ran (observeBalance) and this is a reload
		// (from the chain's values: a rebase's offset no longer applies)
		delete(r.balanceOffset, addr)
	} else if sender && !r.senderRel {
		// (without senderRel the sender's balance and nonce are observed by every transaction)
		r.fence(bk)
	} else {
		if r.deferred == nil {
			r.deferred = newAddressSet()
		}
		r.deferred[addr] = true
	}
	base, _ := r.ex.store.baseAccount(addr, r.blk)
	la := loadedAccount{balance: new(uint256.Int), codeHash: types.EmptyCodeHash}
	if base != nil {
		la.balance, la.nonce, la.codeHash = base.Balance, base.Nonce, common.BytesToHash(base.CodeHash)
	}
	lazy := r.deferred[addr] && !r.fenced[addr]
	if lazy {
		r.loadRegistration = loadNoReg
	} else {
		r.waitVolatile(bk) // fenced at load: the balance is observed
	}
	la.balance = r.resolveBalance(bk, la.balance)
	nk := Key{Addr: addr, Kind: kindNonce}
	if sender && r.senderRel {
		r.predictNonce(nk)
	}
	if v, _, ok := r.read(nk); ok {
		la.nonce = v.Nonce
	}
	r.loadRegistration = loadAll
	if v, _, ok := r.read(Key{Addr: addr, Kind: kindCode}); ok {
		la.codeHash = v.CodeHash
	}
	if lazy {
		r.loadRegistration = loadUncommitted
	}
	_, wiped, _ := r.read(Key{Addr: addr, Kind: kindWipe})
	r.loadRegistration = loadAll
	la.exists = !(la.nonce == 0 && la.balance.IsZero() && la.codeHash == types.EmptyCodeHash)
	r.loadedAcc[addr] = la
	if !la.exists {
		return nil, nil
	}
	root := types.EmptyRootHash
	if base != nil && wiped.tx < 0 {
		root = base.Root
	}
	return &types.StateAccount{
		Nonce:    la.nonce,
		Balance:  la.balance.Clone(),
		Root:     root,
		CodeHash: la.codeHash.Bytes(),
	}, nil
}

// Lazy loads: loading an account whose fences are deferred does not register
// the transaction as a reader of the balance and nonce, and registers it for
// the wipe marker only when an uncommitted writer published the version it
// sees. The code registers as before: it is used at once (a call runs it), and
// a contract a lower transaction creates in the window would otherwise be
// found stale only at commit, on the frontier. A hot contract is loaded by
// most transactions that call it while they never observe its balance, so
// registering every load would put them all on its entry locks. Nothing is
// lost: an observed account field is validated at commit whether it registered
// or not, and the first observation of a deferred account reads its balance
// and nonce again, registered (observeBalance); a field never observed is no
// dependency.

// baseNonce is addr's nonce before the transaction's block.
func (r *versionReader) baseNonce(addr common.Address) uint64 {
	if acc, _ := r.ex.store.baseAccount(addr, r.blk); acc != nil {
		return acc.Nonce
	}
	return 0
}

const (
	loadAll = iota
	loadNoReg
	loadUncommitted
)

// The per-incarnation sets of deferred and fenced accounts come from a pool: at
// high core counts the Go heap's locks, taken when allocations need fresh
// spans, are a large lock delay of the executions.
var addressSets = sync.Pool{New: func() any { return make(map[common.Address]bool, 4) }}

func newAddressSet() map[common.Address]bool { return addressSets.Get().(map[common.Address]bool) }

// releaseSets returns the reader's account sets to the pool: they are not
// used after the incarnation ran.
func (r *versionReader) releaseSets() {
	for _, m := range []map[common.Address]bool{r.deferred, r.fenced} {
		if m != nil {
			clear(m)
			addressSets.Put(m)
		}
	}
	r.deferred, r.fenced = nil, nil
}

// predictNonce stashes the sender's nonce for the read that follows, if a
// lower transaction of the same sender has not committed: the nonce the
// transaction must find is the one it carries, and only the sender's own
// transactions change it. Like any predicted read it is registered against
// the writers it assumes and validated by its value, at their publication
// and at commit; a block whose nonces do not line up fails there and the
// next incarnation reads the store. A previous transaction that has
// published but not committed counts too: read as a version, its nonce
// would tie this transaction to that incarnation.
func (r *versionReader) predictNonce(nk Key) {
	msg := r.ex.env.Messages[r.j]
	p := r.ex.prevSender[r.j]
	front := r.ex.committed.Load()
	if msg.SkipNonceChecks || p < front {
		return // nothing of the sender in flight below: the ordinary read is final
	}
	// the uncommitted transactions of the sender, down to the nearest committed one
	set := []int32{p}
	base := r.ex.prevSender[p]
	for base >= front {
		set = append(set, base)
		base = r.ex.prevSender[base]
	}
	if r.pred == nil {
		r.pred = make(map[Key]predictedRead)
	}
	val := Value{Nonce: msg.Nonce}
	r.pred[nk] = predictedRead{val: val, field: fieldOf(kindNonce, val), top: p, base: base, set: set}
}

// resolveBalance reads the balance key: the newest version below the
// transaction, or, if that is a relative change, the sum of the chain above
// the newest absolute value (waiting for an ESTIMATE in it); the sum is
// remembered as the value seen, which the committer validates by value.
func (r *versionReader) resolveBalance(bk Key, base *uint256.Int) *uint256.Int {
	for {
		v, _, ok := r.read(bk)
		fees := r.ex.feeLayer(bk, r.j)
		if fees == nil {
			if !ok {
				return base
			}
			if !v.Rel {
				return v.Balance
			}
		}
		// (with the beneficiary's fees of this block so far, see Executor.creditFee)
		bal, _, _, est := r.ex.store.balanceBelowPlus(bk, r.j, r.blk, fees)
		if est != nil {
			r.waitWriter(est.tx, est.mark)
			continue
		}
		r.vals[bk] = bal.Bytes32()
		return bal
	}
}

// observeBalance runs the account fences deferred when addr was loaded, now
// that the transaction observes its balance or nonce (the fields the
// account key's writers change; code loads never trigger it). If waiting
// changed what the chain serves, the incarnation has acted on a stale load
// (its StateDB holds the old values, and a sub-call may have branched on
// them): it is unwound and re-executed, like an inconsistent read, and its
// next incarnation loads the account after the writers it waited for.
func (r *versionReader) observeBalance(s *fenceStateDB, addr common.Address) {
	if r.fenced[addr] {
		return
	}
	beneficiary := r.ex.coinbases[addr]
	if _, loaded := r.loadedAcc[addr]; beneficiary && !loaded {
		// a beneficiary's balance is about to be observed and its account is
		// not loaded yet: wait for the frontier first, so that the load below
		// is exact and the incarnation is not unwound for a stale one
		r.waitFrontier()
	}
	// An eager account fence does not cover fees credited at commit.
	if !r.deferred[addr] && !beneficiary {
		if _, loaded := r.loadedAcc[addr]; loaded {
			return // loaded with its fence run
		}
		s.StateDB.GetBalance(addr) // load it now: Account runs or defers the fence
		if !r.deferred[addr] {
			return
		}
	}
	delete(r.deferred, addr)
	if r.fenced == nil {
		r.fenced = newAddressSet()
	}
	bk := Key{Addr: addr, Kind: kindBalance}
	r.fence(bk)
	if beneficiary {
		r.waitFrontier() // fees of every lower transaction are in (see Account)
	}
	r.fenced[addr] = true
	la, ok := r.loadedAcc[addr]
	if !ok {
		return
	}
	// the first load was not a dependency: forget it and read again
	nk := Key{Addr: addr, Kind: kindNonce}
	for _, k := range []Key{bk, nk} {
		delete(r.reads, k)
		delete(r.readVals, k)
		delete(r.vals, k)
	}
	base, nonce := new(uint256.Int), uint64(0)
	if acc, _ := r.ex.store.baseAccount(addr, r.blk); acc != nil {
		base, nonce = acc.Balance, acc.Nonce
	}
	r.waitVolatile(bk)
	bal := r.resolveBalance(bk, base)
	if v, _, ok := r.read(nk); ok {
		nonce = v.Nonce
	}
	if bal.Eq(la.balance) && nonce == la.nonce {
		return
	}
	if !beneficiary && s.rebase(addr, &la, bal, nonce) {
		r.loadedAcc[addr] = la
		return
	}
	r.ex.stale[r.j].Store(true)
	panic(errAbortIncarnation)
}

// waitVolatile waits, before the incarnation observes balance key bk, for the
// writers of bk's chain below it to settle when bk's republications mostly
// change its value (volatileBalances): until each writer from the
// newest uncommitted version down has finished on a read of bk that is still
// current, down to a committed version (Executor.volatileWait); a finished
// writer whose read went stale is waited for until its next incarnation
// finishes. A version such a chain leads to is replaced only if something
// other than bk moves. A wait for the writer's commit instead would be a wait
// for the whole frontier.
func (r *versionReader) waitVolatile(bk Key) {
	if volatileBalances.changeProb(bk) < volatileThreshold() {
		return
	}
	for range 64 {
		w, done, why, depth := r.ex.volatileWait(bk, r.j, r.blk)
		if timelineFile != nil {
			r.ex.timeline(r.j, "W %d %d %d %d %d %s %d\n", r.ex.blockOf(r.j).Fixture.Number, r.j, r.inc, why, depth, bk.Addr.Hex(), time.Since(r.ex.start).Nanoseconds())
		}
		if w < 0 {
			return
		}
		t0 := time.Now()
		r.ex.add(r.j, counterVolatileWaits, 1)
		r.ex.block(r.j, done, waitFrontier) // until w's (next) incarnation finishes, not its first publication
		r.ex.add(r.j, counterVolatileWaitNs, time.Since(t0).Nanoseconds())
		r.checkStale()
	}
}

// rebase moves the account the incarnation loaded (la) onto the balance the
// chain serves now, instead of unwinding it. Until this first
// observation the incarnation has only changed the balance by transfers it
// did not observe (checked at commit as predicates): its state holds the
// loaded balance plus its own change, so the balance it should see is the new
// one plus that change. The difference is kept as an offset (balanceOffset) that
// every look at the balance adds (balanceOf), not written into the StateDB:
// a write there is journaled in the current call and would be undone if that
// call reverts, while the loaded record would not. The loaded record moves
// to the new balance, so the change it publishes stays its own. Only the
// balance is rebased, and only for an account whose existence does not
// depend on it (a nonce or code): emptiness, and the deletion of an empty
// account at the end of the transaction, are decided on the StateDB's own
// balance. Otherwise the incarnation is unwound as before.
func (s *fenceStateDB) rebase(addr common.Address, la *loadedAccount, bal *uint256.Int, nonce uint64) bool {
	if s.StateDB == nil || nonce != la.nonce || (la.nonce == 0 && la.codeHash == types.EmptyCodeHash) {
		return false
	}
	cur := s.balanceOf(addr)
	if bal.Lt(la.balance) && cur.Lt(new(uint256.Int).Sub(la.balance, bal)) {
		return false // its own debits exceed the new balance
	}
	d := new(uint256.Int).Sub(bal, la.balance) // modulo 2^256: a decrease wraps, and so does the sum
	if o := s.r.balanceOffset[addr]; o != nil {
		d.Add(d, o)
	}
	if s.r.balanceOffset == nil {
		s.r.balanceOffset = make(map[common.Address]*uint256.Int, 1)
	}
	s.r.balanceOffset[addr] = d
	la.balance = bal.Clone()
	return true
}

// balanceOf is addr's balance in the StateDB plus the offset of a rebase
// (rebase).
func (s *fenceStateDB) balanceOf(addr common.Address) *uint256.Int {
	b := s.StateDB.GetBalance(addr)
	if o := s.r.balanceOffset[addr]; o != nil {
		return new(uint256.Int).Add(b, o)
	}
	return b
}

// Storage implements state.StateReader.
func (r *versionReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	r.fence(Key{Addr: addr, Slot: slot, Kind: kindStorage})
	v, so, ok := r.read(Key{Addr: addr, Slot: slot, Kind: kindStorage})
	_, wo, _ := r.read(Key{Addr: addr, Kind: kindWipe})
	var val common.Hash
	switch {
	case wo.tx > so.tx:
		// deleted after the last write to this slot
	case ok:
		val = v.Slot
	default:
		val = r.ex.store.baseStorage(addr, slot, r.blk)
	}
	r.loadedSlot[slotRef{addr, slot}] = val
	return val, nil
}

// Has implements state.ContractCodeReader.
func (r *versionReader) Has(addr common.Address, codeHash common.Hash) bool {
	return len(r.Code(addr, codeHash)) > 0
}

// Code implements state.ContractCodeReader.
func (r *versionReader) Code(addr common.Address, codeHash common.Hash) []byte {
	if code, ok := r.ex.codes.Load(codeHash); ok {
		return code.([]byte)
	}
	return r.ex.store.baseCode(addr, codeHash)
}

// CodeSize implements state.ContractCodeReader.
func (r *versionReader) CodeSize(addr common.Address, codeHash common.Hash) int {
	return len(r.Code(addr, codeHash))
}

// fenceStateDB wraps the StateDB of one incarnation. It records which account
// fields the transaction observed, remembers slots written without a prior read,
// and keeps the beneficiary's fee credit out of the state.
type fenceStateDB struct {
	*state.StateDB
	r         *versionReader
	coinbase  common.Address
	sender    common.Address // its balance changes again at the gas refund
	fee       uint256.Int
	used      map[common.Address]uint8
	written   map[slotRef]struct{}
	hotWrites map[slotRef]uint8 // how many times it wrote each hot key (chainWrite)
	// Balances changed by transfers the transaction never observes (any
	// account but the sender and the beneficiary) are published as relative
	// versions; a transfer's "enough balance" check is recorded as a
	// predicate and validated at commit instead of being a read of the value.
	moved map[common.Address]bool // accounts with unobserved transfers
	// recreated holds the accounts CreateAccount replaced in this
	// incarnation: a fresh object, so a slot the incarnation read there can
	// read differently without being written (collectWrites)
	recreated     map[common.Address]bool
	predicates    []balancePredicate
	exactBalances bool // after a predicate failed at commit: check balances for real
	// senderRel: the sender is treated like any other account whose balance
	// the transaction does not observe. The one look core takes at it, the
	// gas purchase's "balance covers gas limit times fee cap plus value",
	// has a bound known from the message (upfront) and is recorded as a
	// predicate; the gas, the value and the refund are then unobserved
	// transfers, published as one relative change. Together with the nonce
	// served from the message (versionReader.predictNonce) a transaction no
	// longer depends on the previous one of its sender. Off after a failed
	// predicate (exactBalances).
	senderRel   bool
	upfront     *uint256.Int
	upfrontDone bool // the gas purchase's check has been answered
}

// balancePredicate is a sufficiency check the EVM made before a transfer: balance of
// addr >= amount held (ok) when the incarnation ran against its speculative
// view; the committer re-evaluates it against the committed balance.
type balancePredicate struct {
	addr   common.Address
	amount *uint256.Int
	ok     bool
	own    *uint256.Int // the transaction's own net change to addr before the check (nil: none)
	ownNeg bool
}

// canTransfer replaces core.CanTransfer in the block context: on the
// RegionFence state it records a predicate instead of reading the balance.
func canTransfer(db vm.StateDB, addr common.Address, amount *uint256.Int) bool {
	if s, ok := db.(*fenceStateDB); ok {
		return s.canTransfer(addr, amount)
	}
	return core.CanTransfer(db, addr, amount)
}

func (s *fenceStateDB) canTransfer(addr common.Address, amount *uint256.Int) bool {
	relSender := addr == s.sender && s.senderRel
	if (!relSender && (addr == s.sender || addr == s.coinbase)) || s.exactBalances || s.used[addr]&usedBalance != 0 {
		return s.GetBalance(addr).Cmp(amount) >= 0 // observed anyway
	}
	cur := s.balanceOf(addr)
	ok := cur.Cmp(amount) >= 0
	// the transaction's own earlier transfers to / from addr are part of what
	// the EVM compared; the committer adds them to the committed balance
	p := balancePredicate{addr: addr, amount: new(uint256.Int).Set(amount), ok: ok}
	if la, has := s.r.loadedAcc[addr]; has {
		if cur.Gt(la.balance) {
			p.own = new(uint256.Int).Sub(cur, la.balance)
		} else if cur.Lt(la.balance) {
			p.own, p.ownNeg = new(uint256.Int).Sub(la.balance, cur), true
		}
	}
	s.predicates = append(s.predicates, p)
	return ok
}

func newFenceStateDB(inner *state.StateDB, r *versionReader, coinbase, sender common.Address) *fenceStateDB {
	return &fenceStateDB{
		StateDB:  inner,
		r:        r,
		coinbase: coinbase,
		sender:   sender,
		used:     usedMaps.Get().(map[common.Address]uint8),
		written:  writtenMaps.Get().(map[slotRef]struct{}),
	}
}

func (s *fenceStateDB) use(addr common.Address, bits uint8) { s.used[addr] |= bits }

func (s *fenceStateDB) AddBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	if addr == s.coinbase && reason == tracing.BalanceIncreaseRewardTransactionFee {
		s.fee.Add(&s.fee, amount)
		return uint256.Int{}
	}
	if addr == s.coinbase && !amount.IsZero() {
		if _, ok := s.r.ex.lazyCoinbase(s.r.j, addr); ok {
			// a transfer to the beneficiary: credited with the fee at commit
			s.fee.Add(&s.fee, amount)
			return uint256.Int{}
		}
	}
	// A zero-value transfer touches the account but does not depend on its balance.
	if amount.IsZero() {
		return s.StateDB.AddBalance(addr, amount, reason)
	}
	if (addr != s.sender || s.senderRel) && !s.exactBalances {
		// a transfer in: the transaction does not observe the balance, the
		// change is published as a relative version
		if s.moved == nil {
			s.moved = make(map[common.Address]bool, 2)
		}
		s.moved[addr] = true
		return s.StateDB.AddBalance(addr, amount, reason)
	}
	s.r.observeBalance(s, addr)
	s.use(addr, usedBalance)
	return s.StateDB.AddBalance(addr, amount, reason)
}

func (s *fenceStateDB) SubBalance(addr common.Address, amount *uint256.Int, reason tracing.BalanceChangeReason) uint256.Int {
	if amount.IsZero() {
		return s.StateDB.SubBalance(addr, amount, reason)
	}
	if addr == s.coinbase && !s.fee.IsZero() {
		// the beneficiary pays out after being credited lazily: make the
		// pending credit explicit first (exact, waits for the frontier)
		pending := s.fee
		s.fee.Clear()
		s.StateDB.AddBalance(addr, &pending, tracing.BalanceChangeTransfer)
	}
	if relSender := addr == s.sender && s.senderRel; (relSender || (addr != s.sender && addr != s.coinbase)) && !s.exactBalances {
		// a transfer out after a predicate check (canTransfer, or the gas
		// purchase's for the sender): the value itself was not observed
		if s.moved == nil {
			s.moved = make(map[common.Address]bool, 2)
		}
		s.moved[addr] = true
		return s.StateDB.SubBalance(addr, amount, reason)
	}
	s.r.observeBalance(s, addr)
	s.use(addr, usedBalance)
	return s.StateDB.SubBalance(addr, amount, reason)
}

func (s *fenceStateDB) GetBalance(addr common.Address) *uint256.Int {
	if addr == s.sender && s.senderRel && !s.upfrontDone {
		// the gas purchase asks whether the balance covers the upfront cost:
		// a predicate, not an observation of the value
		s.upfrontDone = true
		if cur := s.balanceOf(addr); s.used[addr]&usedBalance == 0 && cur.Cmp(s.upfront) >= 0 {
			s.predicates = append(s.predicates, balancePredicate{addr: addr, amount: s.upfront, ok: true})
			return cur
		}
		// The balance in view does not cover the cost: a lower transaction
		// that funds the sender has probably not published yet. Recording
		// "insufficient" as a predicate would have the message refused now
		// and run again at the frontier; observing the balance instead runs
		// the sender's fences
		// and reads it again.
		s.senderRel, s.r.senderRel = false, false
	}
	s.r.observeBalance(s, addr)
	s.use(addr, usedBalance)
	if addr == s.coinbase && !s.fee.IsZero() {
		// exact (the inner read waits for the frontier) plus what this
		// transaction has credited lazily so far
		return new(uint256.Int).Add(s.StateDB.GetBalance(addr), &s.fee)
	}
	return s.balanceOf(addr)
}

func (s *fenceStateDB) GetNonce(addr common.Address) uint64 {
	if addr == s.sender && s.senderRel {
		// validated by value like every observed field; no fence runs for it
		// (the balance is not observed, the nonce is the message's own)
		s.use(addr, usedNonce)
		return s.StateDB.GetNonce(addr)
	}
	if acc, ok := s.lazyBeneficiary(addr); ok {
		return acc.Nonce
	}
	s.r.observeBalance(s, addr) // the nonce is written by the same writers as the balance (the account's own transactions)
	s.use(addr, usedNonce)
	return s.StateDB.GetNonce(addr)
}

func (s *fenceStateDB) GetCodeHash(addr common.Address) common.Hash {
	if _, ok := s.lazyBeneficiary(addr); ok {
		return types.EmptyCodeHash
	}
	s.use(addr, usedCode)
	return s.StateDB.GetCodeHash(addr)
}

func (s *fenceStateDB) GetCode(addr common.Address) []byte {
	if _, ok := s.lazyBeneficiary(addr); ok {
		return nil
	}
	s.use(addr, usedCode)
	return s.StateDB.GetCode(addr)
}

func (s *fenceStateDB) GetCodeSize(addr common.Address) int {
	if _, ok := s.lazyBeneficiary(addr); ok {
		return 0
	}
	s.use(addr, usedCode)
	return s.StateDB.GetCodeSize(addr)
}

func (s *fenceStateDB) SetCode(addr common.Address, code []byte, reason tracing.CodeChangeReason) []byte {
	s.use(addr, usedCode)
	return s.StateDB.SetCode(addr, code, reason)
}

func (s *fenceStateDB) SetState(addr common.Address, key, value common.Hash) common.Hash {
	if s.r.ex.chainOn {
		s.chainWrite(slotRef{addr, key}, Key{Addr: addr, Slot: key, Kind: kindStorage}, value)
	}
	s.written[slotRef{addr, key}] = struct{}{}
	return s.StateDB.SetState(addr, key, value)
}

// lazyBeneficiary answers block-invariant questions about the beneficiary (existence,
// nonce, code) from the pre-state without loading its account, which would
// wait for the commit frontier. See Executor.lazyCoinbase.
func (s *fenceStateDB) lazyBeneficiary(addr common.Address) (*types.StateAccount, bool) {
	if addr != s.coinbase {
		return nil, false
	}
	return s.r.ex.lazyCoinbase(s.r.j, addr)
}

// useExistence records the fields an existence or emptiness check depends on:
// an account with code exists whatever its balance, one with a nonce too.
func (s *fenceStateDB) useExistence(addr common.Address) {
	switch {
	case s.StateDB.GetCodeHash(addr) != types.EmptyCodeHash && s.StateDB.GetCodeHash(addr) != (common.Hash{}):
		s.use(addr, usedCode)
	case s.StateDB.GetNonce(addr) != 0:
		if s.r.baseNonce(addr) != 0 {
			// It existed with a nonce before the block, and a nonce only
			// grows: the answer does not depend on the value read. An
			// account without code is never deleted (EIP-161 removes
			// empty ones only, and without code nothing can self-destruct
			// it), nor can a creation reach it (a non-zero nonce is a
			// collision).
			s.use(addr, usedCode)
			break
		}
		s.use(addr, usedCode|usedNonce)
	default:
		s.r.observeBalance(s, addr) // emptiness depends on the balance
		s.use(addr, usedAll)
	}
}

func (s *fenceStateDB) Exist(addr common.Address) bool {
	if _, ok := s.lazyBeneficiary(addr); ok {
		return true
	}
	s.useExistence(addr)
	return s.StateDB.Exist(addr)
}

func (s *fenceStateDB) Empty(addr common.Address) bool {
	if _, ok := s.lazyBeneficiary(addr); ok {
		return false // balance or nonce is non-zero in the pre-state and only grows
	}
	s.useExistence(addr)
	return s.StateDB.Empty(addr)
}

func (s *fenceStateDB) CreateAccount(addr common.Address) {
	s.r.observeBalance(s, addr)
	s.use(addr, usedAll)
	if s.recreated == nil {
		s.recreated = make(map[common.Address]bool, 1)
	}
	s.recreated[addr] = true
	s.StateDB.CreateAccount(addr)
}

func (s *fenceStateDB) CreateContract(addr common.Address) {
	s.r.observeBalance(s, addr)
	s.use(addr, usedAll)
	s.StateDB.CreateContract(addr)
}

func (s *fenceStateDB) SelfDestruct(addr common.Address) {
	s.r.observeBalance(s, addr)
	if s.r.balanceOffset[addr] != nil {
		// a rebased account (rebase): the StateDB zeroes its own balance,
		// and a revert would bring it back without the offset
		s.r.ex.stale[s.r.j].Store(true)
		panic(errAbortIncarnation)
	}
	s.use(addr, usedBalance)
	s.StateDB.SelfDestruct(addr)
}

type keyWrite struct {
	key Key
	val Value
}

// collectWrites diffs the finalised state against what the reader served and
// returns every changed item. Reverted writes are absent because the StateDB
// already rolled them back.
func (s *fenceStateDB) collectWrites() []keyWrite {
	for ref, n := range s.hotWrites {
		chainRates.add(Key{Addr: ref.addr, Slot: ref.slot, Kind: kindStorage}, int(n)) // how many times it wrote the hot key
	}
	// one allocation: an account changes a field or two, a written slot one
	out := make([]keyWrite, 0, 2*len(s.r.loadedAcc)+len(s.written))
	inner := s.StateDB
	for addr, la := range s.r.loadedAcc {
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
		if bal := s.balanceOf(addr); !bal.Eq(la.balance) {
			if s.moved[addr] && s.used[addr]&usedBalance == 0 {
				// unobserved transfers: publish the change, not the value
				if bal.Gt(la.balance) {
					out = append(out, keyWrite{Key{Addr: addr, Kind: kindBalance}, Value{Balance: new(uint256.Int).Sub(bal, la.balance), Rel: true}})
				} else {
					out = append(out, keyWrite{Key{Addr: addr, Kind: kindBalance}, Value{Balance: new(uint256.Int).Sub(la.balance, bal), Rel: true, Neg: true}})
				}
			} else {
				out = append(out, keyWrite{Key{Addr: addr, Kind: kindBalance}, Value{Balance: bal.Clone()}})
			}
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
	// A slot changes only by SetState (written) or with its account: deleted
	// (the wipe above covers it) or replaced by CreateAccount (recreated).
	// The slots only read elsewhere are not looked at again.
	for ref := range s.written {
		emit(ref, s.r.loadedSlot[ref]) // what the reader served, zero if it never read it
	}
	if len(s.recreated) > 0 {
		for ref, before := range s.r.loadedSlot {
			if s.recreated[ref.addr] {
				emit(ref, before)
			}
		}
	}
	return out
}
