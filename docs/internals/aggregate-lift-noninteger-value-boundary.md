# Aggregate lift noninteger value boundary

Source: internal/planner/logical/const_arith_agg_typed.go — caaLiftIsSafe, moved 2026-09-11 (#1026)

EVERY non-integer column declines, and the reason is not a
disposition — it is a VALUE.

IEEE addition is not associative, so `SUM(f + k)` and
`SUM(f) + k*COUNT(f)` are different numbers whenever the summands
span enough magnitude to cancel. Over `f = 1e16, 1, 1, 1, 1`,
PostgreSQL 17.11 answers 1.0000000000000008e+16 for `SUM(f+1)` and
3.0000000000000016e+16 for `SUM(f*3)`; the lifted forms answer
…004e+16 and 3e+16. The first cut of this pass lifted FLOAT64 on the
grounds that "float arithmetic never refuses" — true of this engine,
and beside the point: the lift is an identity over VALUES or it is
not applied, and over a float it is not (round-1 review, B1).

FLOAT32 declines for a sharper version of the same thing: the
per-row multiplication widens each value to a double before it is
accumulated while `SUM(c_f32)` accumulates at float4's width, so the
two forms use a different ACCUMULATOR — `SUM(c_f32 * 2)` answered
1383.1428577005863 per-row and 1383.142822265625 lifted over the
type matrix's 100 rows.

DECIMAL declines too, unchanged from #841: the engine's 128-bit
carrier can refuse where PostgreSQL answers, and the lifted and
per-row forms round at different scales.
