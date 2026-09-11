# Kernel quoted numeric literal domain

Source: internal/engine/exec/kernel/numeric_literal.go — type NumConstStatus = IntConstStatus, moved 2026-09-11 (#1026)

This file is the ONE rule for a QUOTED (unknown-typed) literal meeting a
NUMERIC column, parameterized by the column's TypeID (#646).

PostgreSQL types an unknown-typed literal FROM the operand it meets and
coerces it with THAT TYPE'S OWN INPUT FUNCTION — at every comparison site,
and with no widening anywhere. Verified with EXPLAIN VERBOSE on
postgres:17-alpine over a `real` column:

	r = '3.1'                  ->  (r = '3.1'::real)
	r IN ('3.1')               ->  (r = '3.1'::real)
	r IN ('3.1','7.1')         ->  (r = ANY ('{3.1,7.1}'::real[]))
	r BETWEEN '3.1' AND '100'  ->  (r >= '3.1'::real) AND (r <= '100'::real)
	CASE WHEN r < '3.1'        ->  (r < '3.1'::real)
	CASE r WHEN '3.1'          ->  CASE r WHEN '3.1'::real
	GREATEST(r, '3.1')         ->  GREATEST(r, '3.1'::real)
	NULLIF(r, '3.1')           ->  NULLIF(r, '3.1'::real)
	r IS DISTINCT FROM '3.1'   ->  (r IS DISTINCT FROM '3.1'::real)

That is the OPPOSITE direction from an UNQUOTED numeric literal, which is
`numeric` and drags the comparison up to float8 (`r = 3.1` is `r =
'3.1'::double precision`, #631) — so `r = 3.1` answers 0 rows over a column
holding real(3.1) and `r = '3.1'` answers 1. Both spellings are live in the
oracle corpus for exactly that reason, and the two kernels stay separate:
the box's Go type is what tells them apart, a `string` for the quoted
spelling and a float64/int64 for the numeric one, which is the one thing a
box CAN say about a literal that its declaration cannot (ADR-0012 item 8 is
about a VALUE's order, not about which literal the user wrote).

What this replaces is a silent zero. `toFloat64` has no string arm at all,
so every quoted constant against a FLOAT column read as 0.0: `real = '3.1'`
matched the row holding 0.0, `real = 'abc'` matched it too, `real IN
('3.1','7.1')` matched nothing, and `f > '-Infinity'` asked `> 0.0` and
dropped every negative row — the float rung of #463's silent-sentinel
ladder, which #536 closed for the integer family and #574 for BOOL.
