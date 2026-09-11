# Integer average lift rounding

Source: internal/planner/logical/const_arith_agg_typed.go — caaLiftIsSafe, moved 2026-09-11 (#1026)

AVG over an integer is numeric(38,4) — a value ROUNDED to four
decimals — so what the lift may do to it depends on the operator,
and only one of the two is an identity.

`AVG(col ± k)` → `AVG(col) ± k` is exact for an INTEGER k, and the
reason is that rounding commutes with adding an integer:
round(s/n, 4) + k = round(s/n + k, 4) for integral k, because the
shift moves no digit past the fourth decimal. Measured on
PostgreSQL 17.11 over 1, 2, 4: `avg(m+1)` and `avg(m)+1` are both
3.3333333333333333.

`AVG(col * k)` → `AVG(col) * k` is NOT. It rounds to four decimals
BEFORE the multiply, so the last digit is lost for any k that is not
a power of two — and PostgreSQL itself shows the rewrite is not an
identity: over the same three rows `avg(x*3)` is 7.0000000000000000
and `avg(x)*3` is 6.9999999999999999. This engine answered 6.9999
where the server answers 7.0000 (round-1 review, B2). The digit-count
bound below never asked the question; it only asked whether the
result FIT.

The identity that does hold for `*` is `(k*SUM(col))/COUNT(col)` with
ONE division at the end, and it is not taken here: the lifted form's
DECLARED type would then be the division's rather than the AVG's, so
the two arms of the invariance oracle would render the same number at
two scales. Declining costs the `AVG(col * k)` shape and nothing
else — Q30's shape is SUM.
