# Kernel decimal filter residual cache

Source: internal/engine/exec/kernel/compare.go — func compareFilterDecimal(op CompareOp, value any) FilterKernel {, moved 2026-09-11 (#1026)

compareFilterDecimal compares a DECIMAL column against a constant.

Without this arm ResolveFilterKernel returned nil for DECIMAL, and every
caller read that as "the column does not exist": `WHERE dec_col <> 5.0005`
failed the query with `filter column "c_dec" does not exist in the input
schema` whenever the predicate reached the operator-level filter instead of
the scan (#401).

The comparison is EXACT, per ADR-0012 — PostgreSQL compares numeric against
a numeric literal at full precision. The column's values live at its own
scale, so the constant is truncated to that scale and the truncation's
RESIDUAL is carried: with a DECIMAL(18,4) column, `> 2499.5074494849528`
must still exclude the row holding exactly 2499.5074, which comparing
against the truncated constant alone would admit.

The literal cannot be resolved once at RESOLVE time — the scale comes off
the vector, and a kernel resolved from one batch can be handed the next one
— so it is memoized BY SCALE inside the closure, the way inFilterDecimal
memoizes its set for the same reason. A column's scale does not change
across the batches of one query, so the parse runs once and every later
batch reads two fields.

Unsynchronized deliberately, on inFilterDecimal's own argument and for the
same reason: the single caller is KernelFilter.Execute, and
KernelFilter.Clone returns a fresh KernelFilter with `kern` nil, so every
parallel worker resolves its own closure. That is already required by the
operator's other per-instance scratch (`outSel`); this adds no new
constraint. It is NOT ColumnCompare's predicate, which Filter.Clone DOES
share and which is why that one carries no mutable state at all.
