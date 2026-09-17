# Join arm aliases

Source: internal/planner/physical/join_keys.go — joinArmAlias, moved 2026-09-11 (#1026)

```go
// joinArmAlias is the name the ENCLOSING QUERY calls a join arm — which is
// what a qualified reference above the join is written against, and therefore
// the only alias a join may qualify that arm's duplicate columns with.
//
// For a base table and for a derived table it is `findScanAlias`, because
// `BuildFromTable`'s `setSubtreeAlias` stamps a derived alias onto every scan
// below it. A CTE reference records its name on the SUBTREE ROOT instead
// (`Node.CTEName`, plus `Node.CTERefAlias` for the name one reference gives it
// in `FROM c AS x`) — deliberately, so two relations comma-joined inside the
// CTE body keep separate identities (see subtreeNamesRelation) — and reading
// only the scan below it returned the CTE's underlying TABLE:
//
//	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
//	SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv
//	FROM (SELECT id, b - 100 AS dv FROM decpair) p
//	JOIN decpair x ON p.id = x.id JOIN c ON c.id = p.id
//	JOIN decpair y ON c.id = y.id ORDER BY x.id
//	-- PostgreSQL cdv 25.50, pdv -87.2500 (two different columns)
//	-- before: `c.dv` answered p's -87.2500 on every arm
//
// The join qualified c's column as `decpair.dv` while p's stayed bare, so
// `c.dv` matched neither spelling exactly, fell through to the resolver's
// qualifier strip, and bound the SIBLING arm's bare `dv`. Naming the arm `c`
// makes the exact match the one that wins, and leaves p's `p.dv` on the
// bare-strip path it already took.
// It has TWO answers, because the two engines hand the join two different
// STREAMS and a name describes a stream.
//
// On the single-process pipeline the arm's own Project is a real operator: the
// build side the join receives is the arm's OUTPUT — `id`, `w` — and no inner
// relation's columns are in it at all, so the ONE name the enclosing query
// writes is the only name those columns can answer to.
//
// On the stage DAG a Project emits NO STAGE (ADR-0025), so the stream the join
// receives is the arm's RAW inner columns — `d92` and `j.d92`, one per
// relation inside it — and the arm's name describes none of them: which of the
// two the arm publishes is exactly what the un-materialized Project knows and
// the stage does not. Qualifying them by the arm there put `m.d92` on the
// column the arm did NOT select, and every consumer read the wrong one.
//
// So `joinArmAlias` is the MATERIALIZED answer and `stageBuildTableAlias` is
// the raw one, and each engine's resolvers use its own — which is what makes
// the declaration and the value agree on each path (#773, #706 round 2).
```

## Amendment, 2026-09-14 (#1102, arc R2)

There is a THIRD case, and it is the materialized answer on the DAG too: an arm
a SET OPERATION terminates.

A set operation composes a NEW relation out of what its arms emit — the stage
projects every arm onto the operation's own column list (`Stage.UnionArms`'
per-arm projections) — so the stream the join receives is one column per SELECT
item of the operation and nothing of any scan below it. `stageBuildTableAlias`
answered the first scan it found there, and the join then qualified the arm's
duplicate `id` as `lat_ord.id`:

    SELECT a.id FROM (SELECT id FROM lat_ord UNION SELECT id FROM lat_ord) a
    JOIN lat_item b ON b.order_id = a.id
    -- PostgreSQL 17.11 and both single-process arms: 1|1|2|2
    -- the three DAG arms, before: 1|2|3|4 (lat_item's ids)

`a.id` matched neither spelling exactly, fell through to the resolver's
qualifier strip, and bound the OTHER arm's bare `id`. Which side builds is a
cost decision, so the same statement answered correctly whenever the plan chose
the other relation for the build.

`dagplan.setOpArmPublishesItsOwnList` makes such an arm a MATERIALIZED arm, so
`joinArmAlias` names it — the one name the enclosing query writes. It requires
the arm to HAVE a name: an unnamed one keeps the scan's spelling, which is
strictly better than no qualification at all.

Gate: `coordinator.TestR2AJoinArmIsKeyedAndNamedTheSameOnEveryArm`, the
`{union-all,union,intersect,except}*` rows of the join-arm table, five arms.

## Amendment, 2026-09-14 (arc R2 round 2)

And a FOURTH case, which is the third one's twin: an arm whose SELECT list the
AGGREGATE ABSORPTION materialized onto its stage
(`absorbAggregateOutputProjection`). The stream there is the aggregate's own
relation — its keys under the names `exec.PublishedGroupKeyNames` decides and
its outputs under the planner's — so the one name the enclosing query writes
describes all of it, and the scan below it describes none of it. Two copies of
one grouped block were both qualified by the same inner scan name and each
reference bound the PROBE's copy: every row came back paired with itself.

NOT under a DEPENDENT join. A decorrelated LATERAL's arm is a plan OF the outer
side's rows rather than a relation the query wrote (ADR-0026 §3c), and naming it
by the enclosing alias moved `MAX` over a CTE inside a LATERAL from
PostgreSQL's declared scale on the DAG (12.7500) to the single-process arm's
12.75 — a wrong declaration on the wire traded for a right row set, which is
not a trade. Measured both ways in `TestKnownSetOperationTwoPathSplits`.
