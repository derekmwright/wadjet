# Join arm projection materialization

Source: internal/planner/physical/join_input_projection.go — absorbJoinArmProjection, moved 2026-09-11 (#1026)

```go
// absorbJoinArmProjection materializes a derived arm's computed SELECT list
// onto the JOIN stage that produces its rows (#780).
//
// Neither branch above can reach this shape. The scan branch needs the arm to
// emit exactly one scan stage, and an arm that is itself a join emits two or
// more; the value is computed over the JOINED stream anyway, which no single
// scan carries. So on the DAG the column existed nowhere — `walkStages` emits
// no stage for the arm's Project, and every consumer above it re-resolved the
// bare name against the arm's RAW inner columns, where `a` is the scan's
// column and not `g.a * 3`. That is a WRONG VALUE, silently, on both DAG arms.
//
// The target is the arm's TERMINAL stage, which is the one stream the
// enclosing query sees. The passthrough is written from the stage-stream
// model (stage_stream_model.go), not from the stage's column lists: a join's
// output is neither side's list — the executor qualifies a duplicate build
// column with its owning alias and DROPS one it cannot qualify — and the
// model mirrors `joinOutputSchemaWithMapping` line for line, so what is
// passed through is what the fragment really ships.
//
// The arm's own aliases are EXCLUDED from the passthrough, exactly as the
// scan branch strips them from the read set: `g.a * 3 AS a` over an arm that
// also carries a raw `a` is one name and two values, and the one the arm
// PUBLISHES is the computed one. That is the whole of ADR-0025's arm
// doctrine, applied to the stream rather than to the name.
```
