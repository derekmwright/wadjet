# Whole day date arithmetic boundary

Source: internal/engine/expr/expr_temporal_cast.go — BinOp.dateArith, moved 2026-09-11 (#1026)

dateArith is `date - date` and `date ± n`, the two shapes BinOp.Eval must
recognize once CAST produces a real date (#340).

Operands resolve through temporalOperand, so every form the engine has for a
date is accepted on equal terms: a DATE/TIMESTAMP column, a CAST to one, and
a date-shaped string — which is what a DATE column looks like when the
catalog declares it VARCHAR, as the TPC-H fixtures do. `l_receiptdate -
l_shipdate` is exactly that shape, and it answered NULL on every row.

	date - date → the whole number of days between them (DuckDB: BIGINT)
	date ± n    → the date n days away, as epoch days (DuckDB: DATE)

Both are gated on the operands being whole DAYS. An instant carrying a clock
declines and falls through to the arithmetic below, because
timestamp-minus-timestamp is an INTERVAL in SQL and this engine has no
interval column type to answer with — inventing a unit here is the mistake
#319 and #322 were about. `date ± INTERVAL` is not handled here either: that
is intervalShift, which keeps the rendered-string result #322 pinned for it.

ok=false means "not date arithmetic" and leaves the caller's numeric path
untouched — including the case where an operand is a string that does not
parse as a date, which is how `'BUILDING' - 1` keeps its old answer.
