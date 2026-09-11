# Fractional choice literal trigger

Source: internal/engine/expr/rettype.go — func fractionalLitTriggersFold(decided []DeclType) bool {, moved 2026-09-11 (#1026)

fractionalLitTriggersFold reports whether a numeric literal with a
FRACTIONAL spelling, beside at least one arm that is not a constant, must
put a choice construct on the DECIMAL rung.

It is the one exception to "a constant contributes to the fold and never
triggers it" (ADR-0024), and it is here because the alternative is a wrong
VALUE rather than a wrong type. `LEAST(c_i64, 1.5)` is `numeric` on
PostgreSQL 17.11 and answers 1.5; declaring it INT64 — which the integer
rung's `typed[0]` did — builds an int64 vector, and the 1.5 the evaluator
produces is TRUNCATED into it. Arithmetic over it then made the truncation
worse: `LEAST(c_i64, 1.5) * 3` was 4 for the server's 4.5, and
`(CASE … ELSE 1.5 END) * <int8 max>` was MinInt64 for an exact numeric
(round-1 review, B3).

Two conditions, and the second is what keeps the deferral this narrows:

  - the literal's SPELLING has a non-zero scale. `COALESCE(i32, 2)` is
    integer in PostgreSQL too and is untouched.
  - at least one arm is NOT a constant. With every arm constant there is
    nothing to resolve the literal against, and `GREATEST(-2.5, -7.5)` keeps
    the FLOAT64 a bare numeric literal declares — ADR-0024's literal
    deferral, unchanged, and the case CommonDeclType's allLiterals clause
    returns before reaching here anyway.

expr.decimalArmFold makes the identical call over the COMPILED arms, so the
vector the plan builds and the box the runtime hands it stay one decision.
