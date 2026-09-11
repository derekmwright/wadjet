# Aggregate input group key materialization

Source: internal/planner/physical/window_alias_respell.go — aggInputAliasIsAggregateGroupKey, moved 2026-09-11 (#1026)

aggInputAliasIsAggregateGroupKey reports whether the derived alias's DEFINING
EXPRESSION is a GROUP BY key of the aggregate below — the one case in which
the producer emits a column under that expression's TEXT, and so the one case
in which the aggregate's argument is a bare NAME spelled that way.

It is the third answer to "is this name materialized here", and the three
differ in WHAT the producer calls the value: nothing materializes it
(substitute the expression), a join or an ordering materializes it under the
ALIAS, an aggregate materializes a GROUP BY key under its expression's TEXT.

The first draft asked only "is there an aggregate below", and that was far
too wide. `SELECT SUM(v) FROM (SELECT SUM(a) * 2 AS v FROM t GROUP BY s) x`
has an aggregate below, but `SUM(a) * 2` is not a group key — the aggregate
emits `s` and `__agg_0`, and the alias is arithmetic OVER an aggregate
output. Spelling the argument `__agg_0 * 2` handed the operator a name it
cannot look up, and both DAG arms hard-failed after three attempts with
`aggregate input "__agg_0 * 2" is not a column of its input (input has: s,
__agg_0)` — a query PostgreSQL answers 105.98 and ff7c3f19 answered on every
arm. The expression has to be COMPUTED there, which is the first answer.

Matching on the GROUP BY list is what separates the two: the DISTINCT rewrite
puts the whole projected expression in it (`SELECT DISTINCT a * 2 AS v`
groups by `a * 2`), and arithmetic over an aggregate output never appears
there.
