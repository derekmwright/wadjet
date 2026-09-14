# Gather output rename resolution

Source: internal/planner/physical/output_rename_resolve.go — resolveOutputRenameSource, moved 2026-09-11 (#1026)

```go
// resolveOutputRenameSource maps an OutputRename SOURCE that names a nested
// subquery's alias back to the column the DAG's streams actually carry (#385).
//
// walkStages treats an ordinary Project as a passthrough — it emits no stage
// — so a subquery's rename never happens anywhere on the DAG: every stream
// carries SOURCE column names, and each consumer compensates by resolving
// aliases back through the plan (resolveShuffleKey for join keys,
// resolveAggInputName for aggregate inputs, resolveSortKeyColumn for ORDER BY
// terms). The GATHER is the consumer this helper compensates for: when the
// outer SELECT merely forwards a subquery's alias (`SELECT k FROM (SELECT
// r_regionkey AS k FROM region) t`), extractOutputRenames reads the outermost
// Project and produces {From: k, To: k} — but no stage ever emitted a column
// named k, so applyOutputRenames could not resolve the source, degraded to
// its rename-only fallback, and the client saw the full upstream width under
// source names.
//
// The walk starts at the child of the outermost Project (whose list the
// renames came from) and substitutes at most once per Project — a projection
// list is simultaneous, so `b AS a, a AS b` must not chase itself — while
// descending through order/cardinality-preserving wrappers. Chained renames
// across NESTED Projects (`SELECT a FROM (SELECT b AS a FROM (SELECT c AS b
// ...))`) do resolve level by level.
//
// Three stop conditions mirror the sibling resolvers:
//   - a COMPUTED alias (Projection.Column == "") stops the walk: the value
//     has no source column to resolve to, and the #383/#169 machinery
//     materializes it into the producing fragment under the alias itself;
//   - an Aggregate stops the walk: its outputs are its own GroupBy /
//     OutputCol names, and descending past it would resolve against the
//     wrong schema (#355's aggStageRenames already handles group keys the
//     aggregate itself had to resolve);
//   - a Join recurses into both output-visible children (probe side only for
//     semi/anti), first substitution wins.
//
// See resolveOutputRenameSource / resolveOutputRenameSourceForGather below for
// the one place the two callers disagree.
```

## Amendment, 2026-09-14 (arc R2)

**The walk comes back out of an arm with the column the block's projection
reads — and where the block wrote a BARE source name, the arm identity is gone
with it.** Two copies of one block then resolve to one name:

    SELECT a.k, a.p, b.k, b.p
    FROM (SELECT order_id AS k, product AS p FROM lat_item ORDER BY amount) a
    JOIN (SELECT order_id AS k, product AS p FROM lat_item ORDER BY amount) b
    ON b.k = a.k

Both `a.k` and `b.k` resolved to `order_id`, which binds the PROBE side's copy,
so every row came back paired with ITSELF on the three DAG arms where
PostgreSQL pairs each row of one arm with each matching row of the other.

`buildArmQualified` puts the BUILD arm's own name back on a source column the
walk resolved to a bare one, for the GATHER's rename alone. The join qualifies
its build's duplicate columns by that arm's name, so the spelling it puts back
is the one the stream really carries; where the column is not a duplicate the
stream carries it bare and `exec.ColumnIndexFallback` strips the qualifier on
the miss, which is the same column. The PROBE arm needs nothing — its columns
keep their bare names, which is what a resolution that lost the qualifier had
already bound.

It applies only where the arm holds exactly ONE relation and computes no
relation of its own — no aggregate, no set operation, no window. A block over
two relations would name a raw inner column after the arm, which is #773's
wrong value from the other side; an aggregate or a set operation publishes a
relation of its OWN, whose identity is the arm's name and not the scan's, and
those arms keep the resolution they have (the residue is pinned in
`coordinator.arc_r2_pins_test.go` with that mechanism).
