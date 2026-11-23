// SPDX-License-Identifier: MIT

// This file holds the bound on an aggregate's initial group capacity under a
// memory budget (#1706, ADR-0006's "a charge for memory that does not exist
// yet" rule applied to the aggregate).
package exec

// presizeBound caps an initial group capacity n at the aggregate's estimated
// input rows when the aggregate runs under a memory budget.
//
// Every index, key mirror and flat accumulator array an aggregate allocates
// up front is charged at its capacity, through reconcileGroupMemory's
// forced "spill tracking" charge, on the first batch. The unhinted default is
// 4096 slots, which on the typed-generic path (a STRING key beside an INT64
// key) is 458,796 bytes — 88% of a 512 KiB budget — for a table that will
// hold two groups. A partitioned aggregate runs one clone per worker, and two
// grouped LATERAL bodies run two such aggregates concurrently (each join
// build runs on its own goroutine), so six clones held 2.75 MB of capacity at
// once, drained it to run files and released it. Whether a hash join's
// first arrival reservation landed inside that window or after it was the
// query's verdict: `memory budget exceeded (used=1376347, requested=24692,
// budget=524288, of which forced=1376347 by "spill tracking")`, decided by
// the scheduler (#1706, the third site).
//
// A group needs an input row, so capacity beyond the input can never be
// filled. The input count is the planner's ESTIMATE (InputRowHint), so a low
// estimate costs a few doublings and never an answer: every structure capped
// here grows on demand. Without a budget, or without an estimate, n is
// returned unchanged — the unbudgeted engine allocates exactly what it did.
//
// A partitioned aggregate (cloneNDVDivisor = k > 1) is one of k sinks that
// each own a hash partition of the key space: the primary (workerSinks[0])
// and k-1 clones. Each one's share of the estimate is about rows/k, so the
// cap is that share, rounded UP (a positive estimate never becomes the zero
// that means "unknown"). The division happens HERE, once, for the primary and
// the clones alike: the primary keeps the whole estimate in InputRowHint and
// the clones keep it in presizeRowCap, both undivided, and every other reader
// of either (the upward hint in resolveIndices, the flat-accumulator hint,
// inputRowBound) still sees the whole estimate. The share is a sizing
// heuristic, not a bound on a partition's groups: under skew one partition can
// receive every group, and it grows — and is charged for growing — exactly as
// an aggregate whose estimate was low does. Before this, every one of k
// partitions sized for the WHOLE estimate (#1706: seven clones of a
// 5,000-row LATERAL body each force-charged a 4,096-slot pool chunk for one
// group, and a hash join's first reservation refused or not by how many of
// them had been released). That narrows the window in which a join's first
// reservation races partitions' releases; it does not remove the class.
func (h *HashAggregate) presizeBound(n int) int {
	rows := h.InputRowHint
	if rows <= 0 {
		rows = h.presizeRowCap
	}
	if d := int64(h.cloneNDVDivisor); rows > 0 && d > 1 {
		rows = (rows-1)/d + 1 // ceiling, without the overflow of rows+d-1
	}
	if rows <= 0 || int64(n) <= rows || h.Spill == nil {
		return n
	}
	if t := h.Spill.Tracker(); t == nil || t.Budget() <= 0 {
		return n
	}
	return int(rows)
}
