# Compiled decimal choice fold

Source: internal/engine/expr/binop_decimal.go — fracLitArmTriggersFold, moved 2026-09-11 (#1026)

decimalArmFold is the common DECIMAL type of a set of alternatives one value
is chosen from — ADR-0024 item 2's rule, applied to a compiled tree.

Every arm that can PRODUCE a value must have an exact fixed-point form, and
at least one must be a genuine DECIMAL. An arm that has no such form — a
float, a string, an expression this layer cannot type — makes the result a
different type or arrives at its own scale and would be read at the fold's,
so it declines the whole fold. An INTEGER arm participates: an integer is
DECIMAL(10,0)/(19,0) and a numeric literal its own spelling (ADR-0024
item 2), which is what makes `COALESCE(d, 0)` numeric here as it is in
PostgreSQL (#695). A NULL literal is skipped, for the reason CommonDeclType
skips it — it names no type and produces no value.

expr.CommonDeclType folds the same alternatives over their DECLARED types,
and the two must agree: a plan that declares DECIMAL for an expression the
runtime boxes as an integer hands the store a carrier instead of a value.
fracLitArmTriggersFold is expr.CommonDeclType's fractionalLitTriggersFold
over the COMPILED arms: a numeric literal whose spelling has a non-zero
scale, beside at least one arm that is not a constant, puts the choice on
the DECIMAL rung. See that function for why this one exception to
ADR-0024's "a constant never triggers the fold" exists.
