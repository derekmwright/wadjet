# Kernel exact int64 batch sum

Source: internal/engine/exec/kernel/agg.go — func sumSliceExactInt64(data []int64, nulls *batch.Bitmap, sel []uint32, vecLen int, acc *Accumulator) (int64, int64) {, moved 2026-09-11 (#1026)

sumSliceExactInt64 sums an INT64 vector in 128 BITS and reports whether the
batch's true total fits back into int64. It replaces the per-row branch the
first cut of this check used.

The 128-bit form is the CHEAPEST EXACT one measured, and it is cheaper for
the shape of the work rather than the width. A per-row
`over = over || (sum^s)&(v^s) < 0` puts a BRANCH on the loop-carried sum;
this is an ADD/ADC pair whose carry feeds a SECOND accumulator and whose sign
count feeds a THIRD, so three independent chains issue together.
BenchmarkBatchSumInt64, medians of 7 runs at -benchtime=3000x:

	unchecked base (no exactness at all)      517 ns
	per-row branch (8d34890b)               1 257 ns   +143%
	this                                      954 ns    +85%

Every alternative the round-3 review named was measured and is slower or no
better: the branchless `overBits |=` (~1 100), a magnitude probe proving the
batch cannot wrap before running the untouched base loop (~1 300 — the probe
is a second full pass and no cheaper than the sum), and two- and four-lane
unrollings of this loop (~1 015). The residual is not a spelling problem: the
base loop is already ISSUE-limited at about 1.1 cycles per row (load, add,
index, branch), so any exactness at all costs roughly a cycle per row. What
is left is a bounded cost on a kernel that, since #784, serves computed
integer arguments and TIMESTAMP/DURATION sums — a bare SUM over an integer
COLUMN takes the Int128 carrier or the widened int32 loop below.

It is also the more correct reading. The per-row form was STICKY — a running
total that left the int64 range and came back failed a query whose answer was
representable — where this one asks the question once, of the number the
batch actually produced. The cross-batch fold in ResolveBatchSum keeps the
conservative reading, because there the running total IS the answer so far.

(hi, lo) is the exact two's-complement 128-bit sum; it fits in int64 exactly
when hi is int64(lo)'s sign extension.
