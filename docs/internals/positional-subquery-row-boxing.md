# Positional subquery row boxing

Source: internal/planner/physical/subquery_pipeline.go — subqueryRowsPerColumn, moved 2026-09-11 (#1026)

subqueryRowsPerColumn is the sink's rows with ONE MAP ENTRY PER OUTPUT
COLUMN, whatever the columns are called.

PostgreSQL lets two output columns share a name — `SELECT ABS(a), ABS(b)`
is two columns both called `abs`, and so is `SELECT x AS v, y AS v` — and
`exec.CollectSink.convert` boxes a row into a map KEYED BY NAME, so the
second column overwrites the first and the map holds ONE entry for TWO
columns. Its own `ToRowValues` comment records that as lossy.

Every consumer that reduces a subquery's row asks the map how many columns
there are: `expr.ScalarSubqueryValue`, `InSubquery.resolveSlow`,
`CorrelatedInSubquery.EvalBoolNull` and `materializeInSubquery` all count
`len(row)`. With the map collapsed, a two-column subquery counted as ONE and
walked straight through the 42601 refusal PostgreSQL raises for it —
`SELECT (SELECT ABS(a), ABS(b) FROM decpair WHERE id = 1)` answered
`12.7500` on every arm and every door, and its IN twin answered a row count.

The count belongs to the SCHEMA, which is positional and cannot collapse,
so the disambiguation happens HERE, once, at the seam every one of those
consumers is fed from — rather than in four reducers that would each need a
schema they are not given. `CollectSink.ToRowValues` already materializes
the positional form for exactly this case and returns nil when the names are
unique, which is the ordinary shape and costs nothing.

The suffix is `:N`, the column's position: a colon cannot appear in an
identifier the binder resolves, so a disambiguated key can collide with
nothing.

Every consumer of THESE rows iterates rather than reading a value by name,
and the one that comes closest (`materializeInSubquery`) refuses anything
but a single column first — but "no consumer reads by name" is NOT true of
the runner's rows in general, and saying so would be the false claim a
reviewer found: the RECURSIVE-CTE materialization keys its working row by
name, so a duplicate-name column list already collapsed there before this
pass existed. That path does not come through here, it is broken on both
sides of this change, and it has its own filing; recorded so the next
reader does not take the narrow statement for the wide one.
