// SPDX-License-Identifier: LGPL-3.0-or-later

// Package regionfence executes the transactions of consecutive Ethereum blocks
// in parallel with RegionFence, a concurrency control whose unit is the region:
// the part of a transaction's execution between two fence points, a fence
// point being the transaction's first access to a key. Execution stays one
// whole transaction per goroutine; what happens below the transaction is the
// concurrency control: where a reader waits, where a writer releases a key,
// and what is validated at commit.
//
// The engine is a multi-version store (mvstore.go) with push invalidation and
// in-order commit (commit.go), slot scheduling with an adaptive run-ahead
// (scheduler.go, window.go), and per-incarnation state readers that apply the
// fences (statedb.go). What the fences do is learned online, within the block
// and across blocks, and decided from learned probabilities and measured costs
// (cost.go):
//
//   - key signatures name the writers of a contended key from features the
//     transactions carry before they run (signature.go);
//   - on hot keys a reader waits at the start of its region and the writer
//     releases the key at its learned last write (chain.go);
//   - the keys of an invalidated incarnation are marked ESTIMATE where they
//     mostly change when it runs again (abort.go), and readers of a volatile
//     balance wait for the stale link of its writer chain (wait.go);
//   - the window's width in blocks follows what one block more or less would
//     have cost the chain of commits (window.go).
//
// Correctness never depends on a prediction: reads carry the version they saw,
// a writer's publication invalidates its stale readers, and the committer
// validates by value every read that push did not cover. Validation ahead of
// the commit and the off-loop materialization of relative balances
// (prevalidate.go, materialize.go) keep the serial commit loop short. The
// evaluation's baselines are separate executors (base.go, blockstm.go,
// lockpcc.go).
//
// cmd/rfreplay replays fetched mainnet blocks through Execute and compares
// every block with geth's serial execution (serial.go) and with the chain.
package regionfence
