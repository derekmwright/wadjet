# Planner arithmetic mode stamp

Source: internal/engine/expr/int_domain.go — func StampArithMode(e Expr, integer bool) {, moved 2026-09-11 (#1026)

StampArithMode tells a compiled arithmetic node what the PLANNER decided its
output type is, so the runtime does not decide it a second time.

The two decisions are `physical.intArithAllInt` (which picks the output
VECTOR) and `expr.operandIsInt` (which picks the KERNEL), and they were two
hand-maintained walks over two representations of one expression. When they
disagree the value is not merely mislabelled: a float computed under an INT64
declaration is TRUNCATED into the vector, and at the edge it WRAPS. That is
how `LEAST(c_i64, 1.5) * 3` answered 4 for PostgreSQL's 4.5 and
`(CASE … ELSE 1.5 END) * <int8 max>` answered MinInt64 (round-1 review, B3).

So the planner stamps, and `intArm.resolve` returns the stamped answer
instead of re-deriving one. `integer` is the planner's claim that the output
vector is INT64.

The stamp cannot manufacture an integer out of a value that is not one:
`BinOp.intArith` still reads both boxes through `toInt64Safe` and returns
ok=false for anything else, so a stamp of `true` over a decimal or a float
box falls through to the float arm exactly as an unstamped node would. What
it removes is the case where the runtime says integer and the planner did
not — the direction that used to leave a right value under a declaration
nothing enforced.

Only the TOP node of a projection is stamped, which is the only one whose
answer meets a materialized vector; a nested node is an input to this
decision and keeps deriving its own.
