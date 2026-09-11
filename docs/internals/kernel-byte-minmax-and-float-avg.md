# Kernel byte minmax and float avg

Source: internal/engine/exec/kernel/agg_string_avg.go — func minRowString(acc *Accumulator, vec *batch.Vector, row int) {, moved 2026-09-11 (#1026)
Superseded: Bare integer SUM/AVG now use exact accumulator typing; only isAvgFloatAccumType selects the float AVG path. CIDR MIN/MAX has a dedicated inet-order updater, not raw text order.

Byte-backed and BOOL MIN/MAX kernels, and overflow-safe AVG kernels.

MIN/MAX over string/bytes columns previously resolved to a nil updater —
the aggregate silently skipped accumulation and returned NULL (ClickBench
Q22, MIN(URL)). Comparisons use the zero-copy string view; only a new
best value is copied out of the batch buffer (StringValue).

The same nil updater kept answering NULL for the other three BytesColumn
types — IPV6, CIDR and UUID — and for BOOL, long after STRING and BYTES
were fixed, because the resolvers named two types where five share the
storage (#417). The byte order is the one kernel/sort.go already gives all
five, so MIN agrees with the first row of an ORDER BY over the same column.
The accumulator records WHICH of the five it holds (Accumulator.StrType),
because the raw bytes of an IPV6 or a UUID are not text and only round-trip
into their own vector as []byte.

AVG over int64-class columns previously shared the int64 SUM kernel; the
running sum wrapped for large values (AVG(UserID) over hash-like IDs) and
the mean came out garbage. AVG-designated kernels accumulate float64
instead — the result is a float64 mean anyway, and the relative error of
float64 accumulation is far below any cross-engine tolerance. SUM keeps
its exact int64 semantics. int32-class inputs keep the exact int64 sum
(they cannot overflow it). These kernels set IsFloat so finalize, merge,
and partial-spill all route through the float sum.
