# Count distinct two level rewrite

Source: internal/planner/logical/count_distinct_rewrite.go — rewriteCountDistinctTwoLevel, moved 2026-09-11 (#1026)

rewriteCountDistinctTwoLevel rewrites an aggregate containing exactly one
COUNT(DISTINCT x) into two stacked aggregates, so the distinct set rides
the typed GROUP BY fast paths instead of per-group distinct-set maps:

	Aggregate{GroupBy: K, [COUNT(DISTINCT x), SUM(a), COUNT(*), ...]}
	⇒ Aggregate{GroupBy: K, [COUNT(x), SUM(__tl_sum0), SUM(__tl_cnt1), ...]}
	    → Aggregate{GroupBy: K+[x], [SUM(a) AS __tl_sum0, COUNT(*) AS __tl_cnt1, ...]}

Level 1 groups by (K, x): one row per distinct (key, x) pair, with the
other aggregates decomposed into re-aggregable partials. Level 2 groups
by K: COUNT(x) over level-1 rows IS the distinct count (COUNT skips the
NULL-x row, matching COUNT(DISTINCT) semantics), and the partials
recombine (COUNT→SUM of counts, SUM→SUM of sums, MIN/MAX pass through).

Why: profile-attributed (2026-08-17 c6a telemetry). The distinct-set
path pays per-row map inserts into per-group Go maps; both levels of
the rewritten form land on the typed SoA aggregation paths and the
off-heap arena. As a structural bonus, rewritten shapes no longer carry
AggExpr.Distinct, so distributed plans take the ordinary parallel
two-phase aggregate stages instead of the #291 single-task
RawInputAggregate route (#294's ask, delivered by construction).

Scope (v1):
  - Exactly one distinct aggregate; it must be COUNT(DISTINCT col) over
    a bare column (approx_distinct keeps its sketch path; expressions
    and multi-distinct fall through).
  - Every other aggregate is a decomposable simple: COUNT(*)/COUNT(col),
    SUM, MIN, MAX, AVG over bare columns or expressions (the expression
    evaluates at level 1, exactly as it would have in the original).
    AVG decomposes into SUM+COUNT partials with a division projection
    above level 2 (ClickBench Q10's AVG(ResolutionWidth)).
  - No grouping sets.
