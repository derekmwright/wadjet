# Join evaluated column payloads

Source: internal/planner/physical/join_carried_columns.go — ensureJoinCarriesEvaluatedColumns, moved 2026-09-11 (#1026)

```go
// A join's exchange carries every column the join stage will EVALUATE.
//
// A join stage's `Columns` is an OutputFilter and its input exchanges'
// `Columns` are payload manifests: both NARROW what arrives (ADR-0025, "A
// stage's Columns list is a FILTER, not a promise"). They are computed from
// the join node's `NeededColumns` at stage emission — before
// `attachScanSelectProjections` decides that this join is where the outer
// SELECT list gets computed, and before `resolveFilterAliasSpelling` decides
// how a WHERE above the join is spelled. So a name that only those late
// passes introduce is absent from every list the payload is built from, and
// the shuffle drops the column the fragment is about to read:
//
//	SELECT p.w AS pw, q.w AS qw, r.w AS rw
//	  FROM (SELECT id, SUM(b) OVER () AS w FROM t) p
//	  JOIN (SELECT id, SUM(a) OVER () AS w FROM t) q ON p.id = q.id
//	  JOIN (SELECT id, MIN(a) OVER () AS w FROM t) r ON p.id = r.id
//
// The three window slots are what the join's projection reads (`pw=__win_0`,
// `qw=__win_1`, `rw=__win_2`) and the exchanges carried `[id r.id q.id p.id]`
// — `column "__win_0" does not exist in the input schema`, on a query
// PostgreSQL answers. The same gap is silent rather than loud whenever the
// missing name resolves to SOMETHING ELSE on the stream, which is the shape
// #700 was filed for: the exchange carried the CTE's alias while the
// predicate had been re-spelled to the base column, so the filter was UNKNOWN
// on every row and the query answered zero.
//
// The repair is to close the loop rather than to widen the payload
// everywhere: after the late passes have settled what each join stage
// evaluates, union those column references back into the join's own
// OutputFilter and into its input exchanges' manifests. Only a stage that
// really carries a filter or a projection is touched, so a plan with neither
// is byte-identical, and an already-empty list is left empty — for both kinds
// of list, empty means "carry everything" and narrowing it here would be the
// defect in the other direction.
//
// Widening cannot invent a column: a payload naming something an arm does not
// have is ignored (the manifest is applied per side and both sides already
// receive the union of the two, which is why the two-arm spelling of the
// shape above happened to work). The name-resolvability question — does
// anything at all produce this? — stays with the checks in carrier_assert.go,
// which run after this pass and refuse the plan when the answer is no.
```
