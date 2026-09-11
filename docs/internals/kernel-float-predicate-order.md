# Kernel float predicate order

Source: internal/engine/exec/kernel/float_order.go — type FloatOrdered interface{ ~float32 | ~float64 }, moved 2026-09-11 (#1026)

--- The same order, as the six SQL predicates ---

A predicate is not free to disagree with the comparator. PostgreSQL's `=`,
`<`, `>` … over float8/float4 are the operators of the total order above,
not IEEE754's: `'NaN' = 'NaN'` is TRUE, `'NaN' > 'Infinity'` is TRUE, and
`-0.0 = 0.0` is TRUE (verified against live postgres:17-alpine). Go's own
operators are IEEE754, so `WHERE f = f` dropped the NaN rows and
`WHERE f > 1e300` dropped them too, while `ORDER BY f` and `GROUP BY f`
had already been taught to place NaN greatest and fold it into one value
(#446/#459, ADR-0012 item 8).

The forms below are the CHEAP spellings of that rule, not
`CompareFloat64(a,b) <op> 0`: each is the plain IEEE operator plus, at most,
one self-inequality test that only runs when the plain operator already said
no. On data with no NaN — which is all of TPC-H and ClickBench — that extra
test is a predictable never-taken branch. The self-inequality `a != a` is
the NaN test; math.IsNaN is the same instruction behind a call, and this is
the innermost loop of every float filter.

	Eq  a = b   both equal, or both NaN
	Ne  a <> b  the negation of Eq
	Lt  a < b   plain, or (b is NaN and a is not: NaN is greatest)
	Le  a <= b  plain, or b is NaN (everything is <= NaN)
	Gt  a > b   plain, or (a is NaN and b is not)
	Ge  a >= b  plain, or a is NaN (NaN is >= everything, itself included)

Against a CONSTANT the cost is not "at most one extra test" but ZERO, and
resolveFloatConstPred below is where that is spent: with a non-NaN c,
`a > c || a is NaN` is exactly `!(a <= c)` and `a >= c || a is NaN` is
exactly `!(a < c)` — one machine comparison each, the same one the IEEE
kernel issued, with the sense flipped. (Both hold because `a <= c` and
`a < c` are FALSE for a NaN a, which is the whole reason the naive
spellings needed a second test.)
