# Fragment output stream model

Source: internal/planner/physical/stage_stream_model.go — streamCol / stage stream model, moved 2026-09-11 (#1026)

```go
// What a stage's fragment SHIPS, per column, with the arm it came from.
//
// `stageEmittedColumns` answers a weaker question and answers it for one stage
// at a time: which names appear in this stage's own lists. That is enough for
// a reachability check and it is NOT enough to resolve a GROUP BY key, for two
// reasons this model exists to fix (#795):
//
//   - a JOIN's output is not either side's column list. The executor emits the
//     probe's columns, then the build's with every DUPLICATE name QUALIFIED by
//     its owning alias (`joinOutputSchemaWithMapping`), so a stream really can
//     carry `w` and `y.w` at once — and ADR-0026 §4a's claim that "a join
//     stream carries `w`, never `y.w`" was a fact about the MODEL, not about
//     the engine.
//   - a chained link carries its OWN `Columns` as that link's output filter,
//     so a fused chain's real output is the LAST link's list and not the
//     stage's. Reading the stage's list refused a CTE shape the DAG was
//     executing correctly.
//
// Nothing here infers from node kinds. Every rule below mirrors a line of the
// executor: the qualification rule is `joinOutputSchemaWithMapping`'s, the
// filter rule is its output-filter loop including both halves of the
// qualified↔bare fallback, and a pass-through stage forwards what its
// dependency ships.
```
