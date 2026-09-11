# Subquery aggregate placement boundary

Source: internal/planner/logical/agg_placement.go — checkSubqueryAggregatePlacement, moved 2026-09-11 (#1026)

checkSubqueryAggregatePlacement applies the level-local rule to the
SUBQUERIES this level contains, at THEIR level (#809, #601).

The scan above deliberately does not descend into a subquery, and the
reason it gives is right — a subquery is its own level, and `WHERE h >
(SELECT AVG(h) FROM t)` is ordinary SQL. What it left uncovered is the
subquery's OWN level, which nothing else reaches when the planner takes the
subquery apart rather than running it: `SELECT b.w_i32 FROM numwidth b
WHERE SUM(b.w_i32) > 0` is refused by PostgreSQL with 42803, and by this
engine too when the subquery is EXECUTED (its Runner plans it, and the scan
above fires at that level) — but a decorrelated IN builds the inner plan
straight from the parsed subquery, so the aggregate reached a Filter and
`a.w_i32 NOT IN (that)` answered every row of numwidth in silence. The DAG
half of #809 is the same gap wearing the other hat: `WHERE h > (SELECT
AVG(x.h) FROM collslot x WHERE SUM(x.h) > 0)` reached the worker as filter
TEXT and failed with "subqueries require a SubqueryRunner" and no SQLSTATE
at all, while the single-process arm gave PostgreSQL's 42803.

THE BOUNDARY, and it is where PostgreSQL and this engine really differ: an
aggregate inside a subquery may belong to the OUTER level, and PostgreSQL
accepts it there — measured live, `HAVING (SELECT MAX(d.k) FROM typemx_dim
d WHERE d.k = SUM(typemx.g)) > 0` answers rows. This engine does not answer
that shape on ANY path, at this arc's base or at its tip: the subquery is
re-run standalone and refused at its own level with the same 42803. So the
test is not "does it belong to this level" — nothing here has a schema to
resolve a bare name with — but "does it name a relation this subquery does
NOT provide". An aggregate that does is left to the runner, exactly as
before, so nothing that could one day answer is refused earlier because of
this; everything else is the subquery's own and is refused HERE, where the
error carries PostgreSQL's SQLSTATE and reaches both distribution arms.
