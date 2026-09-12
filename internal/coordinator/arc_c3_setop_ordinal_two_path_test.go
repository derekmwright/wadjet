package coordinator

import (
	"context"
	"testing"
	"time"
)

// A SET OPERATION'S RESULT COLUMNS ARE ADDRESSED BY POSITION — #1022, five
// arms, every answer measured on live postgres:17-alpine.
//
// A set operation's result columns are named by its LEFTMOST arm, and two of
// them may carry the same name:
//
//	SELECT order_id AS amount, amount FROM lat_item
//	UNION SELECT id, total FROM lat_ord ORDER BY 1, 2 DESC
//
// publishes `amount` twice. PostgreSQL 17.11 answers seven rows
// `1,150 | 1,100 | 1,50 | 2,200 | 2,125 | 2,75 | 3,0`. Two independent defects
// stood between wadjet and that, and the cell with NO ORDER BY AT ALL is what
// separates them:
//
//  1. THE DEDUP KEY bound by NAME. A distinct UNION's dedup is a `GroupByAll`
//     hash aggregate, which resolves its key set from the live schema and then
//     looked each key back up by name — so both keys bound column one, the
//     operation deduplicated `(order_id, order_id)`, and the two DAG arms
//     answered THREE rows whose second column carried the first's values under
//     the first's declared type. `INTERSECT` and `EXCEPT` take the same shape
//     through `emitSetOpCountingStage`'s key list and answered 0 rows for
//     PostgreSQL's 4. The key set is now addressed by POSITION —
//     `HashAggregate.GroupByColIdx`, the group-key twin of
//     `AggColumn.InputColIdx` (#575) — which is what the positions ARE by
//     construction: every arm is projected onto the result column list, in
//     order, so that the arms are one schema (ADR-0026 §3a).
//
//  2. THE ORDINAL lost its POSITION twice on the way to the sort.
//     `resolveSetOpOrderBy` rewrites `ORDER BY <n>` to the leftmost arm's name
//     for item n but, unlike `resolveOrderBy` beside it, recorded no
//     `Ordinal`; and `buildSetOpPlan` built the Sort's keys by hand rather
//     than through `orderExprFor`, so even a recorded one would not have
//     reached `OrderExpr.SlotPos`. Both keys therefore reached the sort
//     spelled `amount` and bound the first — the single-process arms' seven
//     right rows with key 2 never applied, which no unordered comparison can
//     see. `sortKeySlotPos` now answers a position over a set operation with
//     no further proof (`sortInputSetOpWidth`), because the operation's output
//     IS its result column list.
//
// The census is the issue's matrix: UNION / UNION ALL / INTERSECT / EXCEPT ×
// `ORDER BY 1`, `1, 2 DESC`, `2, 1`, `2 DESC, 1` × a named list with a
// duplicated output name and a star, plus a 5000-row pair so the DAG really
// fans the arms across tasks, plus the row COUNTS, which is the half of #1022
// that has nothing to do with the sort.
func TestC3ASetOperationOrdinalsBindTheirOwnSlots(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := c3Arms(t, ctx)

	// The filing's own pair: two output columns called `amount`, from two
	// different relations, at two different declared types.
	const a = "SELECT order_id AS amount, amount FROM lat_item "
	const b = "SELECT id, total FROM lat_ord"
	// The same left arm against an OVERLAPPING right one, so INTERSECT and
	// EXCEPT have rows to answer with.
	const c = "SELECT order_id, amount FROM lat_item WHERE amount >= 75"
	// The STAR spelling, over a relation whose three columns have distinct
	// names — the arm shape `setOpArmProjection` reads from ScanColumns
	// rather than from a Project.
	const s1 = "SELECT * FROM lat_ord "
	const s2 = "SELECT * FROM lat_ord WHERE id < 3"
	// AT SCALE: 5000 rows over four files, two output columns called `id`.
	const w = "SELECT g AS id, id FROM typemx "
	const w2 = "SELECT g, id FROM typemx WHERE id < 3"

	const dup2 = "cols=[amount:INT64 amount:FLOAT64] "
	const star3 = "cols=[id:INT64 customer:STRING total:FLOAT64] "
	const dupW = "cols=[id:INT32 id:INT64] "

	c3RunDecl(t, arms, []c3Case{
		// ---------- the filing's shape, and the three total orders over it
		{
			name: "1022 UNION, ORDER BY 1, 2 DESC",
			sql:  a + "UNION " + b + " ORDER BY 1, 2 DESC",
			want: dup2 + "rows=7 | 1,150 | 1,100 | 1,50 | 2,200 | 2,125 | 2,75 | 3,0",
		},
		{
			name: "1022 UNION, ORDER BY 2, 1",
			sql:  a + "UNION " + b + " ORDER BY 2, 1",
			want: dup2 + "rows=7 | 3,0 | 1,50 | 2,75 | 1,100 | 2,125 | 1,150 | 2,200",
		},
		{
			name: "1022 UNION, ORDER BY 2 DESC, 1",
			sql:  a + "UNION " + b + " ORDER BY 2 DESC, 1",
			want: dup2 + "rows=7 | 2,200 | 1,150 | 2,125 | 1,100 | 2,75 | 1,50 | 3,0",
		},
		{
			// THE ROW COUNT, with no ORDER BY anywhere: the dedup-key half of
			// #1022 on its own. Three rows for seven, before any sort key
			// existed to be mis-bound.
			name: "1022 the same UNION with no ORDER BY at all, counted",
			sql:  "SELECT COUNT(*) AS n FROM (" + a + "UNION " + b + ") t",
			want: "cols=[n:INT64] rows=1 | 7",
		},
		{
			name: "1022 UNION ALL, ORDER BY 1, 2 DESC",
			sql:  a + "UNION ALL " + b + " ORDER BY 1, 2 DESC",
			want: dup2 + "rows=7 | 1,150 | 1,100 | 1,50 | 2,200 | 2,125 | 2,75 | 3,0",
		},
		{
			name: "1022 EXCEPT, ORDER BY 1, 2 DESC",
			sql:  a + "EXCEPT " + b + " ORDER BY 1, 2 DESC",
			want: dup2 + "rows=4 | 1,100 | 1,50 | 2,125 | 2,75",
		},
		{
			// INTERSECT over the disjoint pair: no rows, and the columns are
			// still declared (arc N1: an empty column list is never an answer).
			name: "1022 INTERSECT over the disjoint pair",
			sql:  a + "INTERSECT " + b + " ORDER BY 1, 2 DESC",
			want: dup2 + "rows=0",
		},

		// ---------- the overlapping pair, so INTERSECT and EXCEPT have rows
		{
			name: "1022 INTERSECT with rows, ORDER BY 1, 2 DESC",
			sql:  a + "INTERSECT " + c + " ORDER BY 1, 2 DESC",
			want: dup2 + "rows=3 | 1,100 | 2,125 | 2,75",
		},
		{
			name: "1022 INTERSECT with rows, ORDER BY 2 DESC, 1",
			sql:  a + "INTERSECT " + c + " ORDER BY 2 DESC, 1",
			want: dup2 + "rows=3 | 2,125 | 1,100 | 2,75",
		},
		{
			name: "1022 EXCEPT with rows, ORDER BY 2, 1",
			sql:  a + "EXCEPT " + c + " ORDER BY 2, 1",
			want: dup2 + "rows=1 | 1,50",
		},
		{
			name: "1022 UNION ALL of the overlapping pair, ORDER BY 2 DESC, 1",
			sql:  a + "UNION ALL " + c + " ORDER BY 2 DESC, 1",
			want: dup2 + "rows=7 | 2,125 | 2,125 | 1,100 | 1,100 | 2,75 | 2,75 | 1,50",
		},

		{
			// The ALL spellings of the counting operation: same key list,
			// same positions, the multiset rule instead of the membership
			// one.
			name: "1022 INTERSECT ALL, ORDER BY 2 DESC, 1",
			sql:  a + "INTERSECT ALL " + c + " ORDER BY 2 DESC, 1",
			want: dup2 + "rows=3 | 2,125 | 1,100 | 2,75",
		},
		{
			name: "1022 EXCEPT ALL, ORDER BY 2 DESC, 1",
			sql:  a + "EXCEPT ALL " + c + " ORDER BY 2 DESC, 1",
			want: dup2 + "rows=1 | 1,50",
		},

		// ---------- THREE ARMS. `A UNION B UNION C` parses left-deep, so arm
		// 1 of the outer operation is itself a set operation and its result
		// names come from the whole chain's leftmost arm — the recursion
		// `setOpOutputNames` and `setOpArmProjection` already make, asserted
		// here with the duplicated name in play.
		{
			name: "1022 three arms, ORDER BY 1, 2 DESC",
			sql:  a + "UNION " + b + " UNION " + c + " ORDER BY 1, 2 DESC",
			want: dup2 + "rows=7 | 1,150 | 1,100 | 1,50 | 2,200 | 2,125 | 2,75 | 3,0",
		},
		{
			name: "1022 three arms UNION ALL, ORDER BY 2 DESC, 1",
			sql:  a + "UNION ALL " + b + " UNION ALL " + c + " ORDER BY 2 DESC, 1",
			want: dup2 + "rows=10 | 2,200 | 1,150 | 2,125 | 2,125 | 1,100 | 1,100 | " +
				"2,75 | 2,75 | 1,50 | 3,0",
		},

		// ---------- the STAR spelling
		{
			name: "1022 star UNION, ORDER BY 1",
			sql:  s1 + "UNION " + s2 + " ORDER BY 1",
			want: star3 + "rows=3 | 1,Alice,150 | 2,Bob,200 | 3,Carol,0",
		},
		{
			name: "1022 star UNION, ORDER BY 3 DESC, 1",
			sql:  s1 + "UNION " + s2 + " ORDER BY 3 DESC, 1",
			want: star3 + "rows=3 | 2,Bob,200 | 1,Alice,150 | 3,Carol,0",
		},
		{
			name: "1022 star UNION ALL, ORDER BY 3 DESC, 1",
			sql:  s1 + "UNION ALL " + s2 + " ORDER BY 3 DESC, 1",
			want: star3 + "rows=5 | 2,Bob,200 | 2,Bob,200 | 1,Alice,150 | 1,Alice,150 | 3,Carol,0",
		},
		{
			name: "1022 star INTERSECT, ORDER BY 3 DESC, 1",
			sql:  s1 + "INTERSECT " + s2 + " ORDER BY 3 DESC, 1",
			want: star3 + "rows=2 | 2,Bob,200 | 1,Alice,150",
		},
		{
			name: "1022 star EXCEPT, ORDER BY 1",
			sql:  s1 + "EXCEPT " + s2 + " ORDER BY 1",
			want: star3 + "rows=1 | 3,Carol,0",
		},

		// ---------- AT SCALE: 5000 rows across four files, `id` twice
		{
			name: "1022 at 5000 rows, ORDER BY 1, 2 DESC",
			sql:  w + "UNION " + w2 + " ORDER BY 1, 2 DESC LIMIT 5",
			want: dupW + "rows=5 | 0,4998 | 0,4984 | 0,4977 | 0,4970 | 0,4963",
		},
		{
			name: "1022 at 5000 rows, ORDER BY 2, 1",
			sql:  w + "UNION " + w2 + " ORDER BY 2, 1 LIMIT 5",
			want: dupW + "rows=5 | 0,0 | 1,1 | 2,2 | 3,3 | 4,4",
		},
		{
			name: "1022 at 5000 rows, the UNION's row count",
			sql:  "SELECT COUNT(*) AS n FROM (" + w + "UNION " + w2 + ") t",
			want: "cols=[n:INT64] rows=1 | 5000",
		},
		{
			name: "1022 at 5000 rows, the EXCEPT's row count",
			sql:  "SELECT COUNT(*) AS n FROM (" + w + "EXCEPT " + w2 + ") t",
			want: "cols=[n:INT64] rows=1 | 4997",
		},
		{
			name: "1022 at 5000 rows, the INTERSECT's row count",
			sql:  "SELECT COUNT(*) AS n FROM (" + w + "INTERSECT " + w2 + ") t",
			want: "cols=[n:INT64] rows=1 | 3",
		},
	})
}
