# Outer join residual match contract

Source: internal/planner/logical/join_predicates.go — routeOuterJoinOnResiduals, moved 2026-09-11 (#1026)

routeOuterJoinOnResiduals moves every ON-clause conjunct of a LEFT, RIGHT
or FULL OUTER join that is not a bare-column equality out of JoinCond and
into JoinFilter — the join's residual predicate, evaluated by the executor
on the combined (probe row + candidate build row) BEFORE a key match is
accepted (#358).

This is the placement liftInnerJoinOnResiduals explicitly could not use:
an outer join's ON runs before the NULL-padding, so a residual lifted above
the join deletes the very rows the join preserves, and one pushed into a
preserved side's scan deletes the rows owed back unmatched (the
FullJoinOnConjunctBuildSide shape). On the probe it is exact for every
disposition: a probe row whose candidates all fail the residual is simply
unmatched — LEFT/FULL still emit it NULL-padded — and a build row counts as
matched only when some probe row passed key AND residual, which is what the
RIGHT/FULL unmatched flush consults.

Runs AFTER pushdownPredicates so extractJoinCondPredicates has already
pushed the conjuncts that have a strictly better home (a LEFT join's
build-side conjunct filters that scan directly; same for a RIGHT join's
probe side). What remains is exactly what had no legal home before:
cross-side non-equalities, expression-operand equalities, and any single-
sided conjunct of a FULL join. Conjuncts that fail to parse stay in
JoinCond, where the physical planner's key parser still refuses them
loudly.

When no conjunct survives as a key pair the join becomes keyless: JoinCond
empties and the executor degenerates to one all-rows candidate chain, with
the residual doing the whole of the work (`LEFT JOIN r ON n.x = r.y + 3`).
