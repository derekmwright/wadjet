# Unpublished star block refusal

Source: internal/planner/physical/lateral_projection_refusal.go — ErrLateralProjectionDistributed, moved 2026-09-11 (#1026)

```go
// ErrLateralProjectionDistributed marks a plan the stage DAG refuses because a
// STAR reads a derived block whose PROJECTION no stage could publish.
//
// The refusal's TRIGGER changed with arc K3 (#984) and its DISPOSITION did not.
// It used to fire on a name test — "is every column the block publishes a
// column of the stream, once" — and every shape that failed it was handed to
// the coordinator-local pipeline, because a Project emits no stage and the
// star would otherwise publish the stream. A stage carries the block's own
// projection now (starReadBlockProjections / publishBlockProjection), so
// almost every one of those shapes runs distributed; what is left is the set
// the pass DECLINED, and this refusal names exactly that set:
//
//	starReadBlocks  the blocks a star reads whose projection is not the
//	                stage's column list — the candidates
//	publishedBlocks the ones a stage really carries
//	the difference  a star reading a relation the plan cannot state
//
// The two ways a candidate is declined, and why each is a decline rather than
// a guess:
//
//   - a COMPUTED item whose type the plan cannot state. `ARRAY[COUNT(*)]`
//     inside a CASE with a NULL arm decides nothing (expr.Undecided), and a
//     projection materialized under a type the empty side of the same join
//     declares differently is ADR-0010's `one stage's files describe one
//     relation` — the loud failure this route exists to spare the client.
//   - a producer that cannot carry the projection at all: its specs do not
//     resolve against what it emits, or a StageProject above it would hide an
//     ordering its consumer reads off its direct dependency.
//
// Asking the question AFTER stage generation is what makes it exact. A
// plan-time name test cannot know whether the pass will succeed, and a refusal
// that fires where the pass would have worked takes an ordinary distributed
// query off the DAG for nothing.
```
