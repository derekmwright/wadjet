# Filter and projection carriers

Source: internal/planner/physical/filter_carrier.go — filterCarrierIndex, moved 2026-09-11 (#1026)

```go
// Where a Filter and a Project land on the stage DAG, and what happens when
// nothing there can run them.
//
// walkStages lowers a logical Filter by appending its predicate text to the
// stage it has just emitted, and a logical Project by emitting nothing at all
// — a Project is a pass-through on the DAG, and the gather's OutputRenames
// recover the SELECT list at the end. Both shortcuts hold only while the
// stage underneath is one that RUNS what it is handed. When it is not, the
// predicate or the projection is attached to a stage that ignores the field,
// or to a stage a later pass deletes, and the query answers WITHOUT it —
// silently, because no operator ever sees a name it cannot resolve (#656).
//
// Three things close that:
//
//   - stageEvaluatesFilter names, per stage type, whether the coordinator's
//     fragment builder emits an OpFilter for Stage.FilterExprs — and, for the
//     types whose projection runs ABOVE that filter, whether a projection is
//     already attached. It is the planner-side mirror of the fragment
//     builders, the way projectableProducer is for Stage.ProjectExprs.
//
//   - filterCarrierIndex gives a predicate a stage that will run it: the last
//     emitted stage when that stage qualifies, and otherwise a StageProject
//     of its own, inserted above it. Nothing is ever attached to a stage that
//     will not evaluate it.
//
//   - resolveFilterAliasSpelling settles WHICH of the predicate's two
//     spellings the carrying stage can evaluate, once every pass that can put
//     an alias-naming projection on a fragment has run.
//
// Two gates hold it, and between them they cover the class rather than the
// seven shapes. ValidateNativeDAGShape refuses, on every distributed query, a
// plan that populates either field on a stage whose fragment ignores it — the
// PLACEMENT half. TestStageDAGCarriesEveryFilterAndProjection asserts that
// every predicate stage emission attached is still readable off some stage
// after every rewriting pass — the CONSERVATION half, which is what a
// carrier-deleting pass breaks.
```
