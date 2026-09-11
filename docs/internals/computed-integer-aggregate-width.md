# Computed integer aggregate width

Source: internal/planner/physical/agg_integer_width.go — aggInputIsWideInteger, moved 2026-09-11 (#1026)

```go
// The WIDTH of a COMPUTED integer aggregate argument (#841's second half).
//
// PostgreSQL's SUM rule is by INPUT WIDTH: `sum(int2|int4)` is bigint, because
// there is a wider integer to grow into, and `sum(int8)` is NUMERIC, because
// there is not. A BARE column already gets that rule here
// (aggIntegerOutputType). A COMPUTED argument did not: every integer
// expression declares INT64 in this engine (ADR-0024's recorded widening), so
// aggOutputFromInputDecl could not tell `SUM(CASE WHEN … THEN 1 ELSE 0 END)`
// — TPC-H Q12's shape, int4 in PostgreSQL and bigint under SUM — from
// `SUM(bigint_col + 0)`, which is numeric there. It read them all as int4,
// keeping Q12's OID and leaving the int8 case as a residual: the total sums
// into an int64 carrier and a query PostgreSQL answers becomes 22003.
//
// That residual was invisible while `rewriteConstArithAggs` was lifting the
// constant out — `SUM(b + 0)` ran as `SUM(b) + 0*COUNT(b)` over a BARE column,
// which takes the exact path — and it surfaced the moment the lift stopped
// moving refusals. It is the same question #841 asks: one expression, one
// disposition, whichever position it is written in.
//
// The width is recoverable from the AST plus the column declarations, which is
// what this walk does. It answers "wide" ONLY for an expression that provably
// carries an int8-domain operand, and everything else keeps the int4 reading
// it had — so the change is confined to shapes that can be pointed at, and no
// declaration moves on a shape this walk cannot see through.
//
//	SUM(CASE WHEN … THEN 1 ELSE 0 END)   not wide → bigint   (PostgreSQL: bigint)
//	SUM(int32_col * 2)                   not wide → bigint   (PostgreSQL: bigint)
//	SUM(int64_col + 0)                   WIDE     → numeric  (PostgreSQL: numeric)
//	SUM(row_number_slot * 2)             WIDE     → numeric  (PostgreSQL: numeric)
//	SUM(9223372036854775807 * x)         WIDE     → numeric  (the literal is int8)
//	SUM(int64_col::bigint)               WIDE     → numeric  (the CAST's target)
//	SUM(int64_col::int4)                 not wide → bigint   (the cast narrows)
//
// It is asked by BOTH spellings — `aggComputedInputDecl` for `GROUP BY` and
// `windowComputedArgDecl` for `OVER (…)` — so an arm added here moves the two
// together by construction. That is why the CAST arm closes one divergence in
// two places at once, and why a missing arm is a divergence in two places at
// once: `SUM(bigint_col::bigint)` read as int4 in both.
//
// NOT covered, deliberately, and recorded rather than guessed at: PORT and
// PROTOCOL under ARITHMETIC. Both are int4-domain and a BARE one takes int4's
// result types (exec.IntegerAccOutputType, #953), but `c_port * 1` is
// evaluated on the FLOAT path — `expr.operandIsInt` keeps the network types
// there on purpose, and `intArithAllInt` mirrors it so the declaration cannot
// promise an integer the kernel will not produce. Answering "int4-domain" here
// alone would be that promise. See ADR-0012's #953 entry for the mechanism and
// the pinned cells.
```
