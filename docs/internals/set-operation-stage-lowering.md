# Set operation stage lowering

Source: internal/planner/physical/set_op_stages.go — emitSetOpStages, moved 2026-09-11 (#1026)

```go
// emitSetOpStages lowers a set-operation node onto the stage DAG.
//
// walkStages used to walk both arms and emit nothing else, on the comment
// "each side runs independently; merge results at the end" — and nothing
// merged. The terminal gather then attached to whichever arm happened to be
// emitted last, so `SELECT r_regionkey FROM region UNION ALL SELECT
// r_regionkey FROM region` answered with five rows carrying r_regionkey,
// r_name and r_comment: one arm, unprojected, at half the row count (#346).
//
// What is emitted here:
//
//	UNION ALL  → one StageUnion. Arm i is dispatched as task i, reads its
//	             arm's whole output, and projects it onto the result column
//	             names and types; the stage's files are therefore the
//	             concatenation.
//	UNION      → the same StageUnion plus a GroupByAll final_aggregate that
//	             dedups the concatenation. The dedup is Singleton: correct,
//	             but one task holds the whole distinct set (see the note on
//	             emitSetOpDedup).
//	INTERSECT  → the same StageUnion with per-arm TAG columns appended
//	EXCEPT       (arm 0 rows carry (1,0), arm 1 rows (0,1)), then a grouped
//	             counting final_aggregate: GROUP BY the full result row,
//	             SUM the tags. The distribution pass inserts an
//	             exchange-repartition on the full row between the two
//	             (StageUnion is RoundRobin, a grouped final requires
//	             ClusteredOn its group keys), so equal rows from both arms
//	             — NULLs included, the shuffle hash marks them
//	             deterministically — meet in one partition and each
//	             partition is independently answerable. The stage's SetOp
//	             marker makes its fragment append an emit operator that
//	             turns each group's (countA, countB) into rows per the
//	             operation's rule and drops the tags. See
//	             emitSetOpCountingStage.
```
