// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// keyKind distinguishes the independently versioned parts of the state. Account
// fields are versioned separately so that, e.g., calling a contract (which needs
// its code) does not conflict with a transaction changing its balance.
type keyKind uint8

const (
	kindBalance keyKind = iota
	kindNonce
	kindCode
	kindStorage
	kindWipe // the account was deleted: all its storage reads as zero afterwards
)

// Key names one versioned state item.
type Key struct {
	Addr common.Address
	Slot common.Hash
	Kind keyKind
}

// Value is the payload of a version; which field is meaningful depends on the
// key kind.
type Value struct {
	Balance  *uint256.Int
	Nonce    uint64
	CodeHash common.Hash
	Code     []byte
	Slot     common.Hash
	// Rel marks a relative balance version: Balance is the magnitude of the
	// change (Neg: a decrease) the writer made without observing the balance
	// (a transfer in or out). A reader sums the relative versions above the
	// newest absolute one (store.balanceBelow); the committer turns a
	// relative version into an absolute one when its writer commits.
	Rel, Neg bool
}

// origin identifies the version a read observed; tx == -1 is the pre-state.
type origin struct {
	tx, inc int32
}

var baseOrigin = origin{tx: -1}

type version struct {
	tx, inc int32
	mark    estimateMark // set on a version an invalidated incarnation left (abort.go)
	rel     bool         // released by its writer while it ran (not its publication)
	val     Value
}

// readRec remembers that incarnation inc of tx read this key and saw orig.
// Weak records are account fields loaded together with the account; whether the
// transaction depends on them is only known from its observed-field mask.
type readRec struct {
	tx, inc int32
	orig    origin
	weak    bool
	src     uint8       // what the version read was (sourceBase...)
	val     common.Hash // field observed (unset for pre-state reads, see store.baseField)
	// predicted reads (orig.inc == predictedInc): the published version the
	// prediction started from and the writers whose deltas it assumed
	pbase int32
	pset  []int32
}

// The sources of a read, reported with an invalidation on the timeline: the
// pre-state or a boundary, a predicted value, and a version published or
// released by a first or a later incarnation of its writer. sourceTentative is
// assigned at the invalidation: the incarnation that wrote the version read
// had already been invalidated (a value left tentative, or replaced since).
const (
	sourceBase = iota
	sourcePredicted
	sourceTentative
	sourcePublished
	sourcePublishedRerun
	sourceReleased
	sourceReleasedRerun
)

func sourceOf(v version, ok bool) uint8 {
	if !ok || v.inc == boundaryInc {
		return sourceBase
	}
	s := uint8(sourcePublished)
	if v.rel {
		s = sourceReleased
	}
	if v.inc>>seqBits >= 1 {
		s++
	}
	return s
}

func (r readRec) assumed(w int32) bool {
	for _, x := range r.pset {
		if x == w {
			return true
		}
	}
	return false
}

type entry struct {
	mu sync.Mutex
	// vers is the ascending version list, published as a whole (under mu):
	// a reader takes one atomic load of the header and never writes the
	// entry's cache lines, so a read of a key nobody writes in the window
	// costs no shared write at any core count. Elements are pointers and a
	// published list is never modified below its length: appending a version
	// (the common case, publication in index order) writes the slot past the
	// length readers hold and publishes a longer header; an insert below the
	// top, a replacement or a removal copies the pointers (8 bytes each)
	// into a fresh list. readers stays under mu; only reads that must be
	// invalidated early register (see versionReader.read).
	vers    atomic.Pointer[[]*version]
	readers []readRec
}

// load returns the current version list (never modified below its length).
func (e *entry) load() []*version {
	if p := e.vers.Load(); p != nil {
		return *p
	}
	return nil
}

func (e *entry) publish(vs []*version) { e.vers.Store(&vs) }

// staleReaders returns the readers above writer that did not observe version
// (writer, inc): they read an older version, or an older incarnation of writer.
// Their records are dropped; a re-execution registers again. same reports
// whether a reader observed the field the new version holds: such readers stay
// registered, now as readers of the new version. Records of readers below
// frontier (committed: nothing can invalidate them any more) are dropped on the
// way, so that a hot key's reader list does not grow with the window.
func (e *entry) staleReaders(writer, inc, frontier int32, same *sameCheck) []readRec {
	var out []readRec
	live := e.readers[:0]
	for _, r := range e.readers {
		if r.tx < frontier {
			continue
		}
		if r.tx > writer && (r.orig.tx < writer || (r.orig.tx == writer && r.orig.inc != inc) ||
			// a predicted read: a writer it did not assume published in its range
			(r.orig.inc == predictedInc && writer > r.pbase && writer < r.orig.tx && !r.assumed(writer))) {
			if same != nil && same.ok(r) {
				// The record now stands for a read of (writer, inc) and must
				// carry the field it observed: a pre-state reader's record has
				// none, and compared as the zero hash a later writer clearing
				// the key would look like one keeping its value. The reader
				// would stay registered on a stale read, which the committer
				// does not look up (readsCurrent).
				r.orig, r.val = origin{writer, inc}, same.f
				live = append(live, r)
				continue
			}
			out = append(out, r)
			continue
		}
		live = append(live, r)
	}
	e.readers = live
	return out
}

// countReadersOf is the number of registered readers that read writer's
// versions (e.mu held).
func (e *entry) countReadersOf(writer int32) int {
	n := 0
	for _, r := range e.readers {
		if r.orig.tx == writer {
			n++
		}
	}
	return n
}

// readersOf returns and drops the readers that observed a version of writer.
func (e *entry) readersOf(writer int32) []readRec {
	var out []readRec
	kept := e.readers[:0]
	for _, r := range e.readers {
		if r.orig.tx == writer {
			out = append(out, r)
			continue
		}
		kept = append(kept, r)
	}
	e.readers = kept
	return out
}

// put installs v (replacing the writer's previous version); e.mu held.
func (e *entry) put(v version) { e.putNode(&v) }

// putNode installs node, which the entry then owns (nothing changes a
// version once it is in the store: an ESTIMATE is a copy).
func (e *entry) putNode(node *version) {
	old := e.load()
	i := sort.Search(len(old), func(i int) bool { return old[i].tx >= node.tx })
	v := node
	switch {
	case i == len(old) && cap(old) > len(old):
		vs := old[:i+1] // readers hold headers of length i: the slot is theirs to ignore
		vs[i] = node
		e.publish(vs)
	case i < len(old) && old[i].tx == v.tx:
		vs := make([]*version, len(old), cap(old))
		copy(vs, old)
		vs[i] = node
		e.publish(vs)
	default:
		vs := make([]*version, len(old)+1, 2*(len(old)+1))
		copy(vs, old[:i])
		vs[i] = node
		copy(vs[i+1:], old[i:])
		e.publish(vs)
	}
}

// markEstimate marks tx's version as an ESTIMATE of the given kind; e.mu held.
func (e *entry) markEstimate(tx int32, mark estimateMark) {
	old := e.load()
	i := sort.Search(len(old), func(i int) bool { return old[i].tx >= tx })
	if i < len(old) && old[i].tx == tx && !old[i].estimate() {
		vs := make([]*version, len(old), cap(old))
		copy(vs, old)
		v := *old[i]
		v.mark = mark
		vs[i] = &v
		e.publish(vs)
	}
}

// remove drops tx's version; e.mu held.
func (e *entry) remove(tx int32) {
	old := e.load()
	i := sort.Search(len(old), func(i int) bool { return old[i].tx >= tx })
	if i < len(old) && old[i].tx == tx {
		vs := make([]*version, len(old)-1, cap(old))
		copy(vs, old[:i])
		copy(vs[i:], old[i+1:])
		e.publish(vs)
	}
}

const storeShards = 256

// store is the multi-version state of one block, sharded by key.
type store struct {
	shards [storeShards]struct {
		m    sync.Map // Key -> *entry; lock-free lookup of existing keys
		mu   sync.Mutex
		free []*entry // recycled entries (see recycle), under mu
	}
	bases  []*memReader // pre-state snapshot of each block of the window
	firsts []int32      // window index of each block's first transaction
	// boundaries[b] holds, for keys that earlier blocks of the window wrote,
	// the value block b starts from when it differs from the last version
	// (withdrawals, block rewards, system calls): a layer between block b-1's
	// versions and block b's own, installed as a whole when block b-1 commits
	// (boundaryMu serialises installers; readers take one atomic load).
	boundaryMu sync.Mutex
	boundaries []atomic.Pointer[map[Key]Value]
	// boundaryKeys is the set of keys with a boundary value in any block (small:
	// withdrawals, rewards, system calls). A lookup of any other key needs no
	// walk over the window's blocks: its newest version below the reader, or
	// the pre-state. Replaced as a whole when boundaries are installed.
	boundaryKeys atomic.Pointer[map[Key]struct{}]
	// Which blocks' pre-state snapshots hold each account / slot (ascending):
	// a base read binary-searches instead of walking the blocks.
	baseAccounts map[common.Address][]int32
	baseSlots    map[slotRef][]baseSlotValue
}

// baseSlotValue is a slot's value in the pre-state snapshot of block b: the
// index keeps the values (the snapshots do not change during the window), so
// a base read is one map lookup.
type baseSlotValue struct {
	b int32
	v common.Hash
}

// storePool recycles version stores between windows: the shard maps keep
// their buckets and the entries keep the capacity of their version and reader
// slices, so a window does not grow them from zero again.
var storePool sync.Pool

func newStore(bases []*memReader, firsts []int32) *store {
	var s *store
	if v := storePool.Get(); v != nil {
		s = v.(*store)
	} else {
		s = &store{}
	}
	s.bases, s.firsts, s.boundaries = bases, firsts, make([]atomic.Pointer[map[Key]Value], len(bases))
	s.boundaryKeys.Store(nil)
	s.baseAccounts, s.baseSlots = nil, nil
	if len(bases) > 1 {
		s.baseAccounts = make(map[common.Address][]int32)
		s.baseSlots = make(map[slotRef][]baseSlotValue)
		for b, base := range bases {
			for addr, acc := range base.accounts {
				s.baseAccounts[addr] = append(s.baseAccounts[addr], int32(b))
				for slot, v := range acc.Storage {
					ref := slotRef{addr, slot}
					s.baseSlots[ref] = append(s.baseSlots[ref], baseSlotValue{int32(b), v})
				}
			}
		}
	}
	return s
}

// latestAtOrBelow returns the highest block index in bs (ascending) that is
// <= blk, or -1.
func latestAtOrBelow(bs []int32, blk int32) int32 {
	i := sort.Search(len(bs), func(i int) bool { return bs[i] > blk })
	if i == 0 {
		return -1
	}
	return bs[i-1]
}

// recycle returns the store to the pool. The caller guarantees no goroutine
// still uses it (the window is complete and its results have been consumed).
func (s *store) recycle() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.m.Range(func(_, v any) bool {
			e := v.(*entry)
			e.vers.Store(nil)
			clear(e.readers) // drop the records' references (pset slices)
			e.readers = e.readers[:0]
			sh.free = append(sh.free, e)
			return true
		})
		sh.m.Clear()
	}
	s.bases, s.firsts, s.boundaries = nil, nil, nil
	s.baseAccounts, s.baseSlots = nil, nil
	s.boundaryKeys.Store(nil)
	storePool.Put(s)
}

func shardOf(k Key) int {
	return int(k.Addr[19]^k.Addr[7]^k.Slot[31]^k.Slot[13]^byte(k.Kind)) % storeShards
}

func (s *store) get(k Key) *entry {
	if v, ok := s.shards[shardOf(k)].m.Load(k); ok {
		return v.(*entry)
	}
	return nil
}

func (s *store) getOrCreate(k Key) *entry {
	sh := &s.shards[shardOf(k)]
	if v, ok := sh.m.Load(k); ok {
		return v.(*entry)
	}
	sh.mu.Lock()
	var e *entry
	if n := len(sh.free); n > 0 {
		e = sh.free[n-1]
		sh.free = sh.free[:n-1]
	} else {
		e = new(entry)
	}
	sh.mu.Unlock()
	if v, loaded := sh.m.LoadOrStore(k, e); loaded {
		sh.mu.Lock()
		sh.free = append(sh.free, e) // lost the race: keep the spare
		sh.mu.Unlock()
		return v.(*entry)
	}
	return e
}

// readRegistered looks up the newest version of k below j and, unless it is an
// ESTIMATE, registers (j, inc) as a reader of it so that a later lower write
// can invalidate the read.
func (s *store) readRegistered(k Key, j, inc int32, weak bool) (version, bool) {
	e := s.getOrCreate(k)
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := s.lookup(e, k, j, s.blockOf(j), -1)
	if ok && v.estimate() {
		return v, ok
	}
	o := baseOrigin
	if ok {
		o = origin{v.tx, v.inc}
	}
	rec := readRec{tx: j, inc: inc, orig: o, weak: weak, src: sourceOf(v, ok)}
	if ok {
		rec.val = fieldOf(k.Kind, v.val)
		if v.val.Rel {
			rec.val = summedField // the reader saw a sum, not this version's field
		}
	}
	if e.readers == nil {
		e.readers = make([]readRec, 0, 8)
	}
	e.readers = append(e.readers, rec)
	return v, ok
}

// validateRegister checks, under the entry's lock, that j's earlier read of
// k (which observed origin o and, if tracked, field val) still resolves to
// the same version or value, and registers the read against the current
// version if so. It returns 1 when registered, -1 when the read is stale,
// and 0 when it could not decide (an ESTIMATE is visible): the committer
// then validates as usual. Finishers use it to move the commit-time
// validation of their own reads off the serial commit loop.
func (s *store) validateRegister(k Key, j, inc int32, o origin, val common.Hash, tracked, byValue bool) int {
	e := s.getOrCreate(k)
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := s.lookup(e, k, j, s.blockOf(j), -1)
	if ok && v.estimate() {
		return 0
	}
	now := baseOrigin
	if ok {
		now = origin{v.tx, v.inc}
	}
	if now != o {
		// a different version: current only when it holds the field the
		// read saw (the base field for a pre-state read)
		if !byValue || !ok || v.val.Rel {
			return -1
		}
		if o.tx < 0 {
			val, tracked = s.baseField(k, s.blockOf(j)), true
		}
		if !tracked || fieldOf(k.Kind, v.val) != val {
			return -1
		}
	}
	rec := readRec{tx: j, inc: inc, orig: now, src: sourceOf(v, ok)}
	if ok {
		rec.val = fieldOf(k.Kind, v.val)
	}
	if e.readers == nil {
		e.readers = make([]readRec, 0, 8)
	}
	e.readers = append(e.readers, rec)
	return 1
}

// blockOf returns the block of window index j.
func (s *store) blockOf(j int32) int32 {
	b := sort.Search(len(s.firsts), func(i int) bool { return s.firsts[i] > j }) - 1
	if b < 0 {
		b = 0
	}
	return int32(b)
}

// boundary returns block b's boundary value of k, if set.
func (s *store) boundary(b int32, k Key) (Value, bool) {
	if m := s.boundaries[b].Load(); m != nil {
		v, ok := (*m)[k]
		return v, ok
	}
	return Value{}, false
}

// latestBelow looks up the version of k a transaction at window index j reads:
// the newest version written by its own block below j, else its block's
// boundary value, else the same from the block before, and so on.
func (s *store) latestBelow(k Key, j int32) (version, bool) {
	return s.latestBelowInBlock(k, j, s.blockOf(j))
}

// latestBelowInBlock is latestBelow for a reader in block b (j may be the index
// right after the block for its post-state).
func (s *store) latestBelowInBlock(k Key, j, b int32) (version, bool) {
	return s.latestBelowSkip(k, j, b, -1)
}

// latestBelowSkip is latestBelowInBlock ignoring the version written by skip and
// ESTIMATEs below it (ESTIMATEs of the reader's own block are returned, so
// that readers wait for them). It takes no lock.
func (s *store) latestBelowSkip(k Key, j, b, skip int32) (version, bool) {
	return s.lookup(s.get(k), k, j, b, skip)
}

// lookup is the block-aware lookup on the current version list of e (which
// may be nil): the newest version of k in [firsts[b], j), else block b's
// boundary value, else the same for the block before. It only reads.
func (s *store) lookup(e *entry, k Key, j, b, skip int32) (version, bool) {
	var vs []*version
	if e != nil {
		vs = e.load()
	}
	if bk := s.boundaryKeys.Load(); bk == nil || !hasKey(*bk, k) {
		// no boundary layer anywhere for k: the newest version below j in
		// any block, else the pre-state
		i := sort.Search(len(vs), func(i int) bool { return vs[i].tx >= j })
		for i--; i >= 0; i-- {
			v := *vs[i]
			if v.tx == skip || (skip >= 0 && v.estimate()) {
				continue
			}
			return v, true
		}
		return version{}, false
	}
	for {
		if e != nil {
			i := sort.Search(len(vs), func(i int) bool { return vs[i].tx >= j })
			for i--; i >= 0 && vs[i].tx >= s.firsts[b]; i-- {
				v := *vs[i]
				if v.tx == skip || (skip >= 0 && v.estimate()) {
					continue
				}
				return v, true
			}
		}
		if bv, ok := s.boundary(b, k); ok {
			return version{tx: s.firsts[b] - 1, inc: boundaryInc, val: bv}, true
		}
		if b == 0 {
			return version{}, false
		}
		j = s.firsts[b]
		b--
	}
}

// setBoundaries installs block b's boundary values (all at once: readers see
// the new layer with their next lookup). The caller then collects the readers
// each value made stale with boundaryStale.
func (s *store) setBoundaries(b int32, vals map[Key]Value) {
	s.boundaryMu.Lock()
	defer s.boundaryMu.Unlock()
	m := make(map[Key]Value, len(vals))
	if old := s.boundaries[b].Load(); old != nil {
		for k, v := range *old {
			m[k] = v
		}
	}
	for k, v := range vals {
		m[k] = v
	}
	s.boundaries[b].Store(&m)
	keys := make(map[Key]struct{}, len(vals))
	if old := s.boundaryKeys.Load(); old != nil {
		for k := range *old {
			keys[k] = struct{}{}
		}
	}
	for k := range vals {
		keys[k] = struct{}{}
	}
	s.boundaryKeys.Store(&keys)
}

func hasKey(m map[Key]struct{}, k Key) bool {
	_, ok := m[k]
	return ok
}

// boundaryStale returns the readers of block b or later that read an older
// value of k instead of the boundary value val (to be invalidated); they are
// dropped from the entry.
func (s *store) boundaryStale(b int32, k Key, val Value) []readRec {
	e := s.get(k)
	if e == nil {
		return nil
	}
	f := fieldOf(k.Kind, val)
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []readRec
	kept := e.readers[:0]
	for _, r := range e.readers {
		if r.tx >= s.firsts[b] && r.orig.tx < s.firsts[b] && r.orig.inc != boundaryInc && r.val != f {
			out = append(out, r)
			continue
		}
		kept = append(kept, r)
	}
	e.readers = kept
	return out
}

// summedField is the recorded field of a read that summed a relative chain:
// no absolute field equals it, so same-value retention never keeps such a
// reader (its sum has to be validated by value).
var summedField = common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")

// balanceBelow resolves the balance of account key k (kind balance) as a
// reader at window index j of block b sees it: the relative versions
// published below j, summed on top of the newest absolute version among them
// (else the block boundary value, else the pre-state). The pre-state anchor
// is that of the lowest block whose relative version was summed: a later
// block's pre-state already contains the earlier block's changes, so summing
// an earlier block's versions on top of it would count them twice. top is the
// newest version below j (what the reader registers on). If a version in the
// chain is an ESTIMATE, est is set and the reader must wait for its writer.
func (s *store) balanceBelow(k Key, j, b int32) (bal *uint256.Int, top version, hasTop bool, est *version) {
	return s.balanceBelowPlus(k, j, b, nil)
}

// balanceBelowPlus is balanceBelow with extra (nil: nothing) added as one
// more credit of the chain: a debit covered only by it must not be cut off
// at zero before it is added (the fees of a block's beneficiary that are not
// in the store yet, see Executor.feeLayer).
func (s *store) balanceBelowPlus(k Key, j, b int32, extra *uint256.Int) (bal *uint256.Int, top version, hasTop bool, est *version) {
	e := s.get(k)
	var vs []*version
	if e != nil {
		vs = e.load()
	}
	pos, neg := new(uint256.Int), new(uint256.Int)
	if extra != nil {
		pos.Set(extra)
	}
	anchor := b // block whose pre-state anchors the chain when no absolute version does
	finish := func(abs *uint256.Int) *uint256.Int {
		out := new(uint256.Int).Add(abs, pos)
		if out.Lt(neg) { // a speculative chain can be inconsistent; validation catches it
			return new(uint256.Int)
		}
		return out.Sub(out, neg)
	}
	base := func() *uint256.Int {
		if acc, _ := s.baseAccount(k.Addr, anchor); acc != nil {
			return acc.Balance
		}
		return new(uint256.Int)
	}
	walk := s.boundaryKeys.Load() != nil && hasKey(*s.boundaryKeys.Load(), k)
	i := sort.Search(len(vs), func(i int) bool { return vs[i].tx >= j }) - 1
	for {
		for ; i >= 0 && (!walk || vs[i].tx >= s.firsts[b]); i-- {
			v := vs[i]
			if v.estimate() {
				return nil, top, hasTop, v
			}
			if !hasTop {
				top, hasTop = *v, true
			}
			if !v.val.Rel {
				return finish(v.val.Balance), top, hasTop, nil
			}
			if walk {
				anchor = b
			} else {
				anchor = s.blockOf(v.tx)
			}
			if v.val.Neg {
				neg.Add(neg, v.val.Balance)
			} else {
				pos.Add(pos, v.val.Balance)
			}
		}
		if !walk || b == 0 {
			return finish(base()), top, hasTop, nil
		}
		if bv, ok := s.boundary(b, k); ok {
			if !hasTop {
				top, hasTop = version{tx: s.firsts[b] - 1, inc: boundaryInc, val: bv}, true
			}
			return finish(bv.Balance), top, hasTop, nil
		}
		b--
	}
}

// registerPredicted registers (j, inc) as a reader of k that saw a predicted
// field, attributed to writer top (origin inc predictedInc).
func (s *store) registerPredicted(k Key, j, inc, top, base int32, set []int32, field common.Hash) {
	e := s.getOrCreate(k)
	e.mu.Lock()
	e.readers = append(e.readers, readRec{tx: j, inc: inc, orig: origin{top, predictedInc}, src: sourcePredicted, val: field, pbase: base, pset: set})
	e.mu.Unlock()
}

// firstWriter returns the lowest transaction holding a version of k, or n if
// there is none.
func (s *store) firstWriter(k Key, n int32) int32 {
	e := s.get(k)
	if e == nil {
		return n
	}
	if vs := e.load(); len(vs) > 0 {
		return vs[0].tx
	}
	return n
}

// fieldOf encodes the part of a value that a read of kind k observes, so that
// two reads of k can be compared by value. A balance fits 32 bytes exactly.
func fieldOf(k keyKind, v Value) common.Hash {
	switch k {
	case kindBalance:
		if v.Balance == nil {
			return common.Hash{}
		}
		return v.Balance.Bytes32()
	case kindNonce:
		return uint256.NewInt(v.Nonce).Bytes32()
	case kindCode:
		return v.CodeHash
	case kindStorage:
		return v.Slot
	}
	return common.Hash{}
}

// baseAccount is the pre-state account a transaction of block blk reads when no
// earlier transaction of the window wrote it: the snapshot of the latest block
// up to blk that touched the account (later snapshots include what happened
// between blocks outside transactions, e.g. withdrawals).
func (s *store) baseAccount(addr common.Address, blk int32) (*types.StateAccount, error) {
	if s.baseAccounts != nil {
		if b := latestAtOrBelow(s.baseAccounts[addr], blk); b >= 0 {
			return s.bases[b].accountShared(addr), nil
		}
		return nil, nil
	}
	for b := blk; b >= 0; b-- {
		if s.bases[b].has(addr) {
			return s.bases[b].accountShared(addr), nil
		}
	}
	return nil, nil
}

// baseStorage is the pre-state value of a slot for a transaction of block blk.
func (s *store) baseStorage(addr common.Address, slot common.Hash, blk int32) common.Hash {
	if s.baseSlots != nil {
		vs := s.baseSlots[slotRef{addr, slot}] // ascending by block
		for i := len(vs) - 1; i >= 0; i-- {
			if vs[i].b <= blk {
				return vs[i].v
			}
		}
		return common.Hash{}
	}
	for b := blk; b >= 0; b-- {
		if v, ok := s.bases[b].storageAt(addr, slot); ok {
			return v
		}
	}
	return common.Hash{}
}

// baseCode looks a code hash up in every snapshot of the window.
func (s *store) baseCode(addr common.Address, codeHash common.Hash) []byte {
	for _, b := range s.bases {
		if code := b.Code(addr, codeHash); len(code) > 0 {
			return code
		}
	}
	return nil
}

// baseField is the field of k in the pre-state of block blk, as versionReader
// serves it when no transaction wrote k.
func (s *store) baseField(k Key, blk int32) common.Hash {
	if k.Kind == kindStorage {
		return s.baseStorage(k.Addr, k.Slot, blk)
	}
	v := Value{Balance: new(uint256.Int), CodeHash: types.EmptyCodeHash}
	if acc, _ := s.baseAccount(k.Addr, blk); acc != nil {
		v.Balance, v.Nonce, v.CodeHash = acc.Balance, acc.Nonce, common.BytesToHash(acc.CodeHash)
	}
	return fieldOf(k.Kind, v)
}

// fenceKey maps a versioned key to the key fences are placed on.
func fenceKey(k Key) (Key, bool) {
	switch k.Kind {
	case kindStorage:
		return k, true
	case kindBalance, kindNonce, kindCode:
		return Key{Addr: k.Addr, Kind: kindBalance}, true
	}
	return Key{}, false
}

// predictedInc marks, in a reader record's origin, a read served from an
// assumption rather than a published version: a sender's nonce taken from
// the transaction itself (versionReader.predictNonce).
const predictedInc int32 = -1

func containsWriter(ws []int32, w int32) bool {
	for _, x := range ws {
		if x == w {
			return true
		}
	}
	return false
}
