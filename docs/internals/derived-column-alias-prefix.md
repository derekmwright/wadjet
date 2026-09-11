# Derived column alias prefix

Source: internal/planner/logical/builder.go — applyColumnAliases, moved 2026-09-11 (#1026)

applyColumnAliases renames a subquery's output columns positionally, the way
`(SELECT …) AS b(kk, nn)` and `WITH c(kk, nn) AS (…)` do.

PostgreSQL's arity rules, measured live on postgres:17-alpine over a
two-column derived table:

	AS b(kk, nn)         → columns kk, nn
	AS b(kk)             → columns kk, n — FEWER aliases rename a PREFIX
	AS b(kk, nn, extra)  → 42P10 `table "b" has 2 columns available but
	                       3 columns specified`

The CTE arm used to apply the list only when the counts matched EXACTLY and
drop it in silence otherwise, so both of the mismatches above answered under
the wrong names.

A subquery whose SELECT list carries a `*` is left alone: the star's width
is a catalog question this layer cannot ask (ExpandStarProjections answers
it later), so neither the rename nor the arity refusal can be made
truthfully here. Guessing would rename the wrong columns, which is a wrong
answer rather than a missing one.

It rewrites the DERIVED TABLE's own SELECT ALIASES rather than stacking a
rename Project above the finished plan. `AS b(kk)` means exactly what
`SELECT s AS kk` means, and the spelling that already worked on every path
is the one with the alias inside. A CTE takes applyColumnAliasProject
instead, because its body SQL is re-read by consumers a rewritten SELECT
list would be invisible to.
