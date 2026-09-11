# Integer expression domain contract

Source: internal/engine/expr/int_domain.go — func castIsInt(e *Cast) bool { return IsIntegerCastDest(e.DestType) }, moved 2026-09-11 (#1026)

The integer DOMAIN of an expression is a property of its TYPE, not of the
SYNTAX that produced its operands (#849, ADR-0024 item 2).

`c_i64 * <int8 max>` raises 22003 here and on PostgreSQL 17.11. Put ANY of
CAST, a function or a choice construct around the same column and the
expression used to answer 9.223399706970886e+24 as a float64 — the same
wrong value projected, filtered, grouped and summed, where the server raises
`bigint out of range` in every one of those positions. The shapes that do
NOT overflow were wrong in the same way with the defect invisible:
`CAST(v AS BIGINT) * 2` answered 200 under OID 701 where PostgreSQL declares
bigint (measured on the wire, round 0).

The mechanism was the NODE CHOICE. compileBinOp builds the typed
BinOpNumeric only when both operands satisfy Float64Expr AND Int64Expr;
*Cast, *Case, *Coalesce, *decimalScalarFn and a polymorphic *FuncCall
satisfy neither, so every one of those pairs fell to the generic BinOp,
whose `+ - * %` arms read both sides through ToFloat64. Only `/` had an
integer arm, added by #369 for exactly this node and exactly these operands.

The predicates below are the RUNTIME MIRROR of physical.intArithAllInt's
declared-type tail: the planner declares INT64 for what these accept, so the
two must recognise the same trees or a declaration promises an integer the
kernel does not produce — which is not a theoretical worry. The first cut of
this fix moved only the planner, and `ABS(i) * <int8 max>` then computed
9.2e24 in float64 and STORED it into the INT64 vector the declaration had
asked for, answering MinInt64: a wrapped number wearing the right type,
which is worse than the float it replaced.
