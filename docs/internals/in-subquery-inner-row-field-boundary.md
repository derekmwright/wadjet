# In subquery inner row field boundary

Source: internal/planner/logical/optimizer.go — tryDecorrelateInSubquery, moved 2026-09-11 (#1026)

A ROW FIELD PATH as the INNER key — the mirror of the outer-key decline
above, and #866.

`d.b IN (SELECT c_row.b FROM typemx_nested)` names a FIELD of a ROW
column. The semi join's build side is the subquery's own plan, which
emits the ROW column `c_row` and no column called `b`, so
exec.HashJoin resolved the build key to -1 — the degenerate
all-rows-equal key — and the join answered the rows whose OUTER key is
NULL while dropping the one row that matches. Measured against live
PostgreSQL 17 over the same rows: PG answers `did = 6`, the
single-process and spilled arms answered `8, 9` (decpair's two
NULL-keyed rows, which `NULL IN (…)` must EXCLUDE), the DAG answered
NOTHING, and the shuffled arm failed loudly with `partitioned shuffle:
key "c_row.b" not in schema`. The NOT IN twin was wrong on all four
arms: seven rows for PostgreSQL's NONE (the membership set contains
NULLs, so PostgreSQL's three-valued rule admits nothing).

The test is that the QUALIFIER names no relation this subquery reads.
That is exactly what a field path is here — the subquery is
uncorrelated by construction at this point, so a qualifier that is not
a relation is a ROW column — and it needs no catalog, which the inner
plan does not have annotated yet at this point in the walk.

MATERIALIZING IT INTO A `__path_N` SLOT WAS BUILT IN ARC J1 AND
WITHDRAWN. The projection publishes the path, the semi join keys on the
slot, and the SINGLE-PROCESS and SPILLED arms then answer PostgreSQL's
`did = 6` and its NOT IN's no rows — but the stage DAG answered ZERO
rows and its NOT IN twin every row, SILENTLY, because no stage
materializes the slot: `absorbComputedSubqueryProjection` is the pass
that would, and the semi join's build side reaches it as
`Distinct → Project → Project → Scan` (dedupSemiAntiBuildSide's dedup),
where its own resolvability check declines. A plan-time refusal keyed
on the slot was tried too and cannot fire: the join's build dep is an
`exchange-replicate` whose column list is empty, so `carrierInputColumns`
reports the input as UN-MODELLED and every carrier assert skips it.
Closing #866 needs the stage model to describe an exchange's payload,
which is its own arc — written up in J1's report.

Declining leaves the IN where it was: an ordinary filter predicate,
whose subquery runs as written and whose field path resolves through
ADR-0022 rule 1's vectorized filters. It is the same answer #482, #516
and #769 take for a shape this rewrite cannot NAME.
