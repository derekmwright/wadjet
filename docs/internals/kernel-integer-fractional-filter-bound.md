# Kernel integer fractional filter bound

Source: internal/engine/exec/kernel/compare.go — func IntFilterBound(v any, op CompareOp) (int64, CompareOp, IntBoundVerdict, IntConstStatus) {, moved 2026-09-11 (#1026)

IntFilterBound resolves an integer column's filter constant AND its operator
together, which is what a NON-INTEGRAL constant needs and Int64FilterConst
alone cannot give (#704).

`int64(3.5)` is 3, so `c = 3.5` matched the row holding 3 and `c IN (3.5)`
matched it too; Go truncates TOWARD ZERO, so `c = -0.5` matched the row
holding 0. PostgreSQL compares `bigint = numeric` exactly and answers no
rows for all three. The typemx measurement in the arc brief read 0 for the
INT64 column only because no row of it holds 3 — `c_i64 = 1000003.5` matched
one, which is the same defect one fixture row away.

For an integer column c and a constant f with a fraction, floor(f) = n:

	c =  f  ->  no row          c <> f  ->  every non-NULL row
	c >  f  ->  c >  n          c >= f  ->  c >  n
	c <  f  ->  c <= n          c <= f  ->  c <= n

The same rewrite answers a constant OUTSIDE int64 entirely (±Infinity
included, which is why the infinities need no arm of their own): there the
verdict is the whole column's, one way or the other. A NaN constant declines
— the caller raises rather than comparing against an implementation-defined
conversion — and no SQL spelling reaches this with one, since a quoted 'NaN'
is read by the integer grammar and refused there.

Every non-float box delegates to Int64FilterConst with the operator
unchanged, so the ordinary path is exactly what it was.
