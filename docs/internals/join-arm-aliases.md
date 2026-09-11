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
