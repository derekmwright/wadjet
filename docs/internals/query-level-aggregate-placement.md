# Query level aggregate placement

Source: internal/planner/logical/agg_placement.go — checkAggregatePlacement, moved 2026-09-11 (#1026)

checkAggregatePlacement enforces PostgreSQL's placement rules for aggregate
and grouping operations AT THIS QUERY LEVEL, before anything is planned:

	SELECT g FROM t WHERE SUM(h) > 1 GROUP BY g
	  ERROR: aggregate functions are not allowed in WHERE            (42803)
	SELECT g FROM t WHERE GROUPING(g) = 0 GROUP BY ROLLUP(g)
	  ERROR: grouping operations are not allowed in WHERE            (42803)
	SELECT a.g FROM t a JOIN t b ON SUM(a.h) = 0 GROUP BY a.g
	  ERROR: aggregate functions are not allowed in JOIN conditions  (42803)
	SELECT SUM(GROUPING(g)) FROM t GROUP BY ROLLUP(g)
	  ERROR: aggregate function calls cannot be nested               (42803)

(every message and SQLSTATE transcribed from PostgreSQL 17.11).

WHERE runs BEFORE grouping, so no aggregate's output and no grouping-set
membership exists there to read; an aggregate call in that position is a
question the query cannot ask. Both were answered SILENTLY before this
check — `WHERE SUM(h) > 1` and `WHERE GROUPING(g) = 0` each returned ZERO
ROWS, because the reference resolved to nothing and a filter admits only
TRUE — and `SUM(GROUPING(g))` aggregated over a column nothing populated
and returned a column of NULLs. A wrong number in place of an error is the
regression the correctness protocol's rule 8 forbids, and #804's parser
widening reached two of these positions, so the rule that covers them is
one rule, not a GROUPING special case.

Scope is deliberately THIS query level:

  - A subquery is its own level and its aggregates are legal there —
    `WHERE h > (SELECT AVG(h) FROM t)` is ordinary SQL, and PostgreSQL
    accepts it. plansql.FindAllAggregates does not descend into subquery
    nodes, which is what makes the scan level-local.
  - A WINDOW column is skipped: `SUM(COUNT(*)) OVER ()` is legal in
    PostgreSQL (a window function OVER an aggregate), and the builder
    already hoists aggregates out of a window's own spec terms. Refusing
    it here would invent a rule PostgreSQL does not have.
