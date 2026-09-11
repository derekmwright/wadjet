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
