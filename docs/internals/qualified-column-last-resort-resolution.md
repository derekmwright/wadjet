# Qualified column last resort resolution

Source: internal/engine/expr/expr_leaf.go — if idx = uniqueQualifiedColumn(b, parts[1]); idx >= 0 {, moved 2026-09-11 (#1026)

A QUALIFIED reference the stream spells under a DIFFERENT qualifier.

The mirror of the bare branch above, and the same disagreement one
spelling over. `QualifyAllBuildCols` renames every build column to the
build's TABLE alias, so a CTE or derived table on the build side of a
self-join publishes `decpair.dv` while every consumer above the join
spells it with the arm's own alias:

	WITH c AS (SELECT id, SUM(f) * 2 AS dv FROM t GROUP BY id)
	SELECT COUNT(*) FROM t x JOIN c ON c.id = x.id JOIN t y ON c.id = y.id
	WHERE c.dv > 1
	-- PostgreSQL 6 · single 6 · both DAG arms 0, in silence (#762)

The BARE spelling of that same predicate (`WHERE dv > 1`) already
answered 6, through the branch above — which is what says the
qualifier is the whole of it and not the carrying.

physical.columnResolves has accepted this direction since #656 (its
last loop compares the two names by their bare parts), so the
planner's checker believed in a resolution the evaluator did not
implement, and every check waved the plan through. Implementing it
removes that disagreement rather than adding a special case — the same
argument the bare branch above was added under.

Last resort and UNAMBIGUOUS ONLY: two arms that both spell it
(`p.w` and `q.w` on one stream, with `x.w` asked for) decline and keep
the loud failure rather than guessing an arm, which is the direction
#742 is about.
