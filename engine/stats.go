// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"sync/atomic"
	"time"
)

// Stats summarises one parallel execution: what rfreplay reports and what the
// cost model (cost.go) folds in at the end of each window.
type Stats struct {
	Wall             time.Duration
	Executions       int64   // incarnations started
	Aborts           int64   // incarnations discarded (stale or failed validation)
	BlockNs          []int64 // completion time of each block
	CommitNs         int64   // whole commit loop, under ex.mu
	WindowWidth      []int32 // the adaptive width in blocks after each block
	SignatureLookups int64   // first reads of a contended key whose signature named writers
	SignatureWaits   int64   // ... waits for a named writer that had not finished
	SignatureHits    int64   // ... that turned out to have changed the key's value

	ReexecNs int64 // later incarnations, running
	WastedNs int64 // running time of incarnations that were discarded
	IdleNs   int64 // slot time with nothing to run

	// Early releases of hot keys (chain.go).
	Releases            int64 // early publications
	ReleaseKept         int64 // ... that were the final value
	ReleaseSuperseded   int64 // ... overwritten or withdrawn by the same incarnation
	ReleaseAborted      int64 // ... made by incarnations that were later discarded
	ReleaseVictims      int64 // readers invalidated when a release was superseded
	ReleaseAbortVictims int64 // readers of releases whose incarnation was discarded before its publication
	ReleaseReads        int64 // reads served by a released value of a writer still running
	ReleaseGainNs       int64 // time from each kept release to its writer's publication
	ReleaseNs           int64 // time the writers spent publishing early releases
	WaitByRelease       int64 // blocked WAIT fences that a release ended

	// Readers' waits on an ESTIMATE, and what the labels on the keys of
	// invalidated incarnations did (abort.go).
	EstimateWaits        int64 // reads that waited on an ESTIMATE
	EstimateRewriteWaits int64 // ... of those, on a key labelled markRewrites
	EstimateWaitNs       int64 // their blocked time
	RewritesLabelled     int64 // keys labelled markRewrites
	TentativeChanged     int64 // keys left tentative that the next incarnation changed
	TentativeAborts      int64 // invalidations of readers that had taken a tentative value
	Cascades             int64 // readers a republication invalidated that had read an earlier incarnation of the writer
	CascadeNs            int64 // their time from that incarnation's invalidation

	VolatileWaits  int64 // waits for a volatile balance's writers
	VolatileWaitNs int64 // ... and their time
	VainWaits      int64 // signature waits for a transaction that did not change the key
	VainWaitNs     int64 // ... and their time
}

const (
	abortCascade        = iota // storage: it had read the writer's earlier incarnation, which a re-execution replaced
	abortNew                   // storage, it ran past the writer: the key was not in the signature table
	abortNamed                 // ... the table named the writer at the waiting threshold
	abortUnnamed               // ... the table knew the key, but not this writer
	abortAccountCascade        // an account field (balance, nonce, code), read from the writer's earlier incarnation
	abortAccountPast           // an account field, it ran past the writer
)

// countAbort records an invalidation of r on k. A storage invalidation also
// teaches the signature table that k is contended. The signatureClass call there
// has an effect of its own: looking the key's features up runs the change
// test on their records (SignatureTable.prob).
func (ex *Executor) countAbort(r int32, rec readRec, k Key, writer int32, where string) {
	class, src, failedKey := ex.classifyAbort(r, rec, k, writer)
	if timelineFile != nil {
		ex.timeline(r, "A %d %d %d %s %d %d %d %d %d %s %t %s %d %d %t false\n", ex.blockOf(r).Fixture.Number, r, ex.inc[r].Load(), where, k.Kind, writer, rec.orig.tx, ex.inc[max(writer, 0)].Load(), time.Since(ex.start).Nanoseconds(), k.Addr.Hex(), k.Addr == ex.env.Messages[r].From, k.Slot.Hex(), class, src, failedKey)
	}
	if writer >= 0 && writer < r && ex.signatures != nil && k.Kind == kindStorage {
		ex.signatureClass(r, k, writer)
		ex.noteContended(r, k, writer)
	}
}

// classifyAbort classifies an invalidation for the timeline, remembers its
// fence key and counts the readers of tentative values (txMu[r] held).
func (ex *Executor) classifyAbort(r int32, rec readRec, k Key, writer int32) (class, src int, failedKey bool) {
	cascade := writer >= 0 && rec.orig.tx == writer
	class = abortAccountPast
	switch {
	case k.Kind != kindStorage && cascade:
		class = abortAccountCascade
	case k.Kind != kindStorage:
	case cascade:
		class = abortCascade
	case ex.signatures == nil || writer < 0 || writer >= r:
		class = abortUnnamed
	default:
		class = abortNew + ex.signatureClass(r, k, writer)
	}
	if fk, ok := fenceKey(k); ok {
		ex.abortKey[r] = fk
	}
	src = int(rec.src)
	if o := rec.orig; o.tx >= 0 && o.inc != predictedInc && o.inc != boundaryInc && o.inc>>seqBits < ex.inc[o.tx].Load() {
		src = sourceTentative
		ex.add(r, counterTentativeAborts, 1)
	}
	if ex.failKeys != nil {
		if fk, ok := fenceKey(k); ok && containsKey(ex.failKeys[r], fk) {
			failedKey = true
		}
	}
	return class, src, failedKey
}

// Counters every transaction updates live in 64 cache lines indexed by
// transaction, not in one Stats field every core writes: a shared counter
// line would be a coherence transfer per increment. foldCounters sums them
// into Stats.
const (
	counterExecutions = iota
	counterReexecNs
	counterSignatureLookups
	counterSignatureWaits
	counterSignatureHits
	counterVainWaits
	counterVainWaitNs
	counterReleaseReads
	counterReleaseAbortVictims
	counterRewritesLabelled
	counterTentativeChanged
	counterTentativeAborts
	counterVolatileWaits
	counterVolatileWaitNs
	counterCascades
	counterCascadeNs
	counterKinds
)

type counterShard struct {
	v [counterKinds]int64
	_ [(128 - counterKinds*8%128) % 128]byte // to a cache-line pair, whatever the number of counters
}

func (ex *Executor) add(j int32, c int, n int64) {
	atomic.AddInt64(&ex.counters[j&63].v[c], n)
}

func (ex *Executor) foldCounters() {
	var sum [counterKinds]int64
	for i := range ex.counters {
		for c := range sum {
			sum[c] += atomic.LoadInt64(&ex.counters[i].v[c])
		}
	}
	st := &ex.stats
	st.Executions = sum[counterExecutions]
	st.ReexecNs = sum[counterReexecNs]
	st.SignatureLookups = sum[counterSignatureLookups]
	st.SignatureWaits = sum[counterSignatureWaits]
	st.SignatureHits = sum[counterSignatureHits]
	st.VainWaits = sum[counterVainWaits]
	st.VainWaitNs = sum[counterVainWaitNs]
	st.ReleaseReads = sum[counterReleaseReads]
	st.ReleaseAbortVictims = sum[counterReleaseAbortVictims]
	st.RewritesLabelled = sum[counterRewritesLabelled]
	st.TentativeChanged = sum[counterTentativeChanged]
	st.TentativeAborts = sum[counterTentativeAborts]
	st.VolatileWaits = sum[counterVolatileWaits]
	st.VolatileWaitNs = sum[counterVolatileWaitNs]
	st.Cascades = sum[counterCascades]
	st.CascadeNs = sum[counterCascadeNs]
}
