# Project stage ordering visibility

Source: internal/planner/physical/project_stage_insert.go — orderingSurvivesAProjectStage, moved 2026-09-11 (#1026)

```go
// orderingSurvivesAProjectStage reports whether inserting a StageProject
// between the producer at producerIdx and its consumers keeps the producer's
// ORDERING visible to them.
//
// A stage's ordering is read off the DIRECT dependency: the coordinator asks
// what its gather's dependency is and whether that stage is ordered, and the
// worker's merge does the same one level down. A projection inserted between
// the two hides it — the new stage is a `project`, it declares no SortKeys,
// and a Tasks=1 fragment concatenating several ordered input files could not
// truthfully declare any, because concatenation is not a merge.
//
// So a producer that carries its own fused ordering keeps its consumers. The
// symptom otherwise is the sharpest kind of silent: the right rows in the
// wrong sequence. `SELECT a.s_suppkey AS lo, b.s_suppkey AS hi FROM supplier
// a JOIN supplier b ON … ORDER BY lo, hi` came back as a correct 9-row
// multiset with the ORDER BY ignored, because the projection renaming
// `a.s_suppkey` to `lo` moved in between the join's fused sort and the gather.
//
// A producer with NO ordering of its own has nothing to lose, and that is the
// case the insertion exists for: an aggregate, a union or a DISTINCT that
// collapses its input and cannot evaluate the SELECT list itself.
```
