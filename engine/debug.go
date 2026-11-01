// SPDX-License-Identifier: LGPL-3.0-or-later

package regionfence

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Options are the run-wide settings of the engine; Configure sets them before
// the first execution.
type Options struct {
	Timeline       string // file for the execution timeline ("": none)
	NoCrossBlock   bool   // every block learns into a table of its own, nothing reaches another block
	NoLearnedWaits bool   // no fence waits on what was learned: readers run past every named transaction
}

var options Options

// Configure sets the engine's options and opens the timeline file.
func Configure(o Options) error {
	options = o
	if o.Timeline == "" {
		return nil
	}
	f, err := os.Create(o.Timeline)
	if err != nil {
		return err
	}
	timelineFile = f
	return nil
}

// The timeline has one line per event, with times in ns since the window
// started and hexadecimal addresses and slots. Its parser depends on these
// formats:
//
//	S block tx inc t-start t-end                 an incarnation ran
//	F block tx inc t ok                          ... and was published (ok) or discarded
//	B block tx why t-block t-wake t-slot writer  it blocked (why: waitFence...), woke, had a slot again; writer -1: none
//	C block tx t                                 it committed
//	P block tx t                                 it woke the waiters for its publications
//	R block tx kind addr slot t                  it released a key while running
//	K block tx writer kind addr slot t           it waited at a key for writer (written when the wait ended)
//	N block tx inc addr slot t known [ tx:p:fi:finished]...
//	                                             a fence's candidates at probability 0; known: the key is in the table
//	D block tx inc writer addr slot t ns p changed
//	                                             the outcome of a fence wait
//	W block tx inc why depth addr t              where a walk of a volatile balance's chain stopped (why: walkNone...)
//	A block tx inc weak kind writer origin writer-inc t addr sender
//	                                             a weak read of a running incarnation went stale
//	A block tx inc where kind writer origin writer-inc t addr sender slot class src failed same
//	                                             an invalidation (where: running, executed, volwait; class:
//	                                             abortCascade...; src: sourceBase...; failed: the key invalidated
//	                                             the transaction before; same: always false)
//	V block tx inc kind addr origin registered sender
//	                                             the read that failed validation at commit
//	X block tx inc reason t                      the finisher or committer discarded an incarnation
var timelineFile *os.File

// The timeline is buffered in memory, in shards by transaction, and written
// out by FlushTimeline after the window's timing: one lock and a file write
// per event would slow the run and put that lock on the committer's path.
type timelineShard struct {
	mu  sync.Mutex
	buf []byte
	_   [32]byte
}

var timelineShards [64]timelineShard

func (ex *Executor) timeline(j int32, format string, args ...any) {
	if timelineFile == nil {
		return
	}
	sh := &timelineShards[j&63]
	sh.mu.Lock()
	sh.buf = fmt.Appendf(sh.buf, format, args...)
	sh.mu.Unlock()
}

// timelineDiscard notes why incarnation run of j was discarded.
func (ex *Executor) timelineDiscard(j int32, run *incarnation, why string) {
	ex.timeline(j, "X %d %d %d %s %d\n", ex.blockOf(j).Fixture.Number, j, run.inc, why, time.Since(ex.start).Nanoseconds())
}

// FlushTimeline writes the buffered timeline out; the caller does so outside
// the timed run.
func FlushTimeline() {
	if timelineFile == nil {
		return
	}
	for i := range timelineShards {
		sh := &timelineShards[i]
		sh.mu.Lock()
		timelineFile.Write(sh.buf)
		sh.buf = sh.buf[:0]
		sh.mu.Unlock()
	}
}

func usedField(mask uint8, kind keyKind) bool {
	switch kind {
	case kindBalance:
		return mask&usedBalance != 0
	case kindNonce:
		return mask&usedNonce != 0
	case kindCode:
		return mask&usedCode != 0
	}
	return true
}

// StaleReads describes the committed reads of the window that the final
// state does not confirm: what a run that differs from the serial execution
// committed on. Call it before Release; incarnations already released are
// skipped.
func (res *Result) StaleReads() []string {
	if res == nil || res.ex == nil {
		return nil
	}
	ex := res.ex
	var out []string
	for j, run := range ex.runs {
		if run == nil {
			continue
		}
		for k, o := range run.reads {
			if usedField(run.used[k.Addr], k.Kind) && !ex.readCurrent(int32(j), k, o, run.vals) {
				out = append(out, ex.describeRead(int32(j), k, o, run))
			}
		}
	}
	sort.Strings(out)
	return out
}

// reportRejected writes the incarnation of f whose message was rejected with
// validated reads: the error, every read and the transfer predicates.
func (ex *Executor) reportRejected(f int32, run *incarnation) {
	bk := ex.blockOf(f)
	var b strings.Builder
	fmt.Fprintf(&b, "REJECT block %d tx %d (window index %d) inc %d: %v; %d reads, %d predicates, exact balances %v\n",
		bk.Fixture.Number, f-bk.First, f, run.inc, run.rejected, len(run.reads), len(run.predicates), ex.predFail[f].Load())
	for k, o := range run.reads {
		fmt.Fprintf(&b, "REJECT   read used-mask %d %s\n", run.used[k.Addr], ex.describeRead(f, k, o, run))
	}
	for _, p := range run.predicates {
		fmt.Fprintf(&b, "REJECT   predicate %s >= %v was %v (own change %v, negative %v)\n", p.addr.Hex(), p.amount, p.ok, p.own, p.ownNeg)
	}
	os.Stderr.WriteString(b.String())
}

// reportRejectRetry writes, once the incarnation after a rejected one
// validated, the reads the two resolved differently.
func (ex *Executor) reportRejectRetry(f int32, old, run *incarnation) {
	bk := ex.blockOf(f)
	var b strings.Builder
	fmt.Fprintf(&b, "REJECT-RETRY block %d tx %d (window index %d): incarnation %d was rejected (%v), incarnation %d is not\n",
		bk.Fixture.Number, f-bk.First, f, old.inc, old.rejected, run.inc)
	for k, o := range old.reads {
		no, again := run.reads[k]
		was, now := ex.readField(f, k, o, old.vals), ex.readField(f, k, no, run.vals)
		if !again || o != no || was != now {
			fmt.Fprintf(&b, "REJECT-RETRY   kind %d %s slot %s used-mask %d: was %d/%d field %x, read again %v as %d/%d field %x\n",
				k.Kind, k.Addr.Hex(), k.Slot.Hex(), old.used[k.Addr], o.tx, o.inc, was, again, no.tx, no.inc, now)
		}
	}
	os.Stderr.WriteString(b.String())
}

// describeRead renders j's read of k: what it observed, what the store
// resolves it to now, whether it registered, and j's reader records on the
// key's entry.
func (ex *Executor) describeRead(j int32, k Key, o origin, run *incarnation) string {
	bk := ex.blockOf(j)
	now, nowField := "pre-state", ex.store.baseField(k, ex.blockIndex[j])
	if cur, ok := ex.store.latestBelow(k, j); ok {
		now, nowField = fmt.Sprintf("%d/%d", cur.tx, cur.inc), fieldOf(k.Kind, cur.val)
		switch {
		case cur.estimate():
			now += " (ESTIMATE " + cur.mark.String() + ")"
		case cur.tentative(ex.inc[cur.tx].Load()):
			now += " (tentative)"
		}
	}
	_, registered := run.registered[k]
	var recs []string
	if e := ex.store.get(k); e != nil {
		e.mu.Lock()
		for _, r := range e.readers {
			if r.tx == j {
				recs = append(recs, fmt.Sprintf("{inc %d origin %d/%d field %x weak %v}", r.inc, r.orig.tx, r.orig.inc, r.val, r.weak))
			}
		}
		e.mu.Unlock()
	}
	return fmt.Sprintf("block %d tx %d (window index %d) inc %d key %s slot %s kind %d: saw %d/%d field %x, now %s field %x, registered %v, records %v",
		bk.Fixture.Number, j-bk.First, j, run.inc, k.Addr.Hex(), k.Slot.Hex(), k.Kind, o.tx, o.inc, ex.readField(j, k, o, run.vals), now, nowField, registered, recs)
}
