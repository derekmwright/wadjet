# Decimal arithmetic execution paths

Source: internal/engine/expr/binop_decimal.go — type decimalOperand interface {, moved 2026-09-11 (#1026)

The DECIMAL mode of BinOpNumeric: `+ - * / %` computed EXACTLY on the Int128
carrier instead of through float64 (ADR-0024 items 3 and 4, #555).

Before this, every arithmetic expression with a DECIMAL operand resolved
float mode — `operandIsInt` accepts only INT32/INT64 — so `d_2 - d_4` over
12.75 and 12.7500 answered -9.999999999976694e-05 where the exact difference
is 0, and `d / d` answered 1 where PostgreSQL answers 0.99999215690465.

The mode is resolved once per node against the first batch, beside the int
and float ones and for the same reason: a column's type does not exist until
a batch arrives. What it needs beyond the type is the operands' (p,s), and
that comes from two places — the vector carries the SCALE and the batch
SCHEMA carries the precision.

Two execution paths, both exact and both required to agree (the two-path
rule of ADR-0018 §3):

  - the BOXED path (Eval), which every row-at-a-time consumer takes and
    which the stage DAG takes for every projection. It answers the value's
    rendered TEXT, exactly as a DECIMAL COLUMN's box is — so every consumer
    that already knows how to read a decimal box (the comparison layer, the
    group-key encoder, Vector.SetValueChecked) reads a computed decimal the
    same way, with no new box type to teach them.
  - the VECTORIZED path (EvalDecimalVec), which writes unscaled carriers
    straight into the projection's DECIMAL vector with no boxing and no
    allocation.
