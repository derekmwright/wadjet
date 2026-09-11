# Window exact cell reader

Source: internal/engine/exec/window_decimal_agg.go — windowExactCells, moved 2026-09-11 (#1026)

windowExactCells is one input column read as EXACT Int128 cells, resolved
ONCE per partition (ADR-0002's typed-kernel rule: resolve the type once,
then dispatch to a typed reader, never per row).

Three carriers reach it and they are the three the engine stores an exactly
summable number in: a DECIMAL's Int128 array, an int64 array, and an int32
array (INT32 and — since #953 — the int4-domain PORT and PROTOCOL). The
DECIMAL arm hands back the stored cell untouched, which is what keeps the
#586 path byte-identical; the integer arms widen at scale 0, which is
exactly what kernel.sumRowInt64Decimal does for the GROUPED spelling
(#784).

MEASURED RESIDUAL, and it is a filing candidate for a perf arc rather than a
correctness question. Reading a DECIMAL cell through this struct rather than
indexing the slice directly costs the exact-DECIMAL window path — #586's,
which predates #987 — 11.6% to 19.4% on BenchmarkWindowFrameSlide's DECIMAL
cells (p <= 0.001, benchstat, -count=6 twice a side, quiet box), against a
float64 control that moves +3% to +5% with no code change of its own.
Allocations are unchanged: 1 alloc/op, 4 KiB, every cell, both sides.

An earlier number is in the tree's history and is WRONG: 90f4651f's body
records 3.3%-9.0% from the first measurement round, and the re-measure on a
quiet box is the range above (#987 review, P2). The reader is what lets one
accumulator serve the DECIMAL and the integer arms, so removing it means
two accumulators again; an attempt to make it neutral in-place could not
show neutrality against that control and was reverted rather than shipped.
