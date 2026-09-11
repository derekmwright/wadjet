# Window accumulator type correction

Source: internal/engine/exec/window_decimal_agg.go — windowAccOutputType, moved 2026-09-11 (#1026)

windowAccOutputType is Window.retypeValueColumns' rule for SUM and AVG: the
output type is the ACCUMULATOR's, not the input's. A DECIMAL input makes it
DECIMAL; an INTEGER input takes PostgreSQL's own result type, which is
exec.IntegerAccOutputType — bigint for sum(int4), numeric for sum(int8) and
for avg of either; everything else keeps FLOAT64.

The last clause is not decoration. A stage spec built before this change, or
a planner declaration resolved against a different scan, can declare DECIMAL
over an input that is not one; writing float sums into a DECIMAL vector's
Int128 array would produce values off by a power of ten with nothing to
report it, so the declaration is corrected DOWN as well as up.

The INTEGER arm is #987 and #813: until it existed, `SUM(int8) OVER ()`
accumulated in float64, so past 2^53 the total depended on the ORDER the
rows arrived in — the same query answered 9007201419001868 or
9007201419001864 on the same data — while `SUM(int8) GROUP BY` next to it
answered exactly. One question, two spellings, two numbers. It asks
IntegerAccOutputType rather than repeating the rule so that the grouped
declaration, this one and the planner's window declaration cannot drift.

`declared` is the spec's own type, and it decides ONE case this correction
must not touch: a SUM whose plan says bigint over an int64-carried input.
Every integer expression in this engine computes in int64 (ADR-0024's
widening), so the input VECTOR of `SUM(CASE WHEN … THEN 1 ELSE 0 END)
OVER ()` is indistinguishable from `SUM(int8_col + 0) OVER ()`'s — while the
PLAN, which still has the argument's syntax, can tell them apart and says
bigint for the first (physical.windowComputedArgDecl and
physical.integerAccArgWidth, #987 review B1).
Widening it back to numeric here would undo that and put the window's OID
at 1700 where its grouped twin's is 20. Both arms accumulate in the same
Int128 and the bigint arm refuses a total that does not fit rather than
wrapping, so keeping the narrower declaration costs no exactness.
