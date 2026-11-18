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
Ungrouped over zero input rows, level 1 has no groups and a SUM of
counts answers NULL where COUNT answers 0, so the ungrouped form
finalizes each recombined COUNT through COALESCE(…, 0) in the same
projection above level 2 that AVG's division uses (#1584). Grouped, a
group exists only when a row does, so no group sums zero rows.

A recombining SUM (of COUNT partials, or of SUM partials) is marked
AggExpr.Recombines and declares its partial's own type, which is what the
original aggregate declares: bigint for a COUNT and a SUM over an int4,
numeric for a SUM over an int8 or a numeric. Declared as a SUM over its
bigint input it said numeric (#1696). AVG's two legs are not marked; its
division declares the AVG. Arithmetic over the recombined aggregate reads
the declaration, so it is now PostgreSQL's integer arithmetic:
`SUM(x) / 3` beside a COUNT(DISTINCT) is bigint 46 where it was numeric
46.666667, and `COUNT(*) * 4000000000000000000` raises 22003.

Not when two outputs share a name and the finalizing projection is built
(`COUNT(*) AS a, COUNT(DISTINCT x) AS a`): that projection reads level 2
by name and so reads the first same-named column for both. On the
single-process path the second output's declaration disagreeing with
that column is what refuses the query (the #361 store guard); declared
bigint beside the bigint distinct count, the refusal would become the
COUNT answered twice. There the recombined aggregates keep SUM's own
declaration and the statement behaves exactly as before; telling the two
outputs apart is #1692's shared-name line.

On the stage DAG that projection rides the level-2 final aggregate stage
as its ProjectExprs (dagplan absorbAggregateOutputProjection). The
COALESCE is published under the same name as the SUM it wraps, so a
projection the stage planner declined would leave the bare SUM's NULL in
place with nothing downstream to notice; the planner therefore carries
the rewrite's `coalesce` call for a Project marked PreservesAggOutputs
(rewriteFinalizationSource), where it declines every function call in a
user's SELECT list (#1692).

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
