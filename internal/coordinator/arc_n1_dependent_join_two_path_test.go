package coordinator

import (
	"context"
	"testing"
	"time"
)

// A DEPENDENT JOIN IS NOT REORDERABLE — #1008's second half, four arms, every
// answer measured on live postgres:17-alpine.
//
// A decorrelated LATERAL is a join the PLANNER manufactured, and the node
// carries the rules that make it correct: the slot it minted and drops
// (`HiddenJoinCols`), the pad marker, the empty-input defaults (ADR-0026 §3c).
// `reorderJoins` treated it as an ordinary inner join:
//
//   - `flattenJoinChain` walked THROUGH it, so two laterals over one outer
//     flattened to THREE relations and `costBasedJoinReorder` REBUILT the
//     chain with `NewJoin` — which carries none of those rules. `SELECT *
//     FROM lat_ord o JOIN LATERAL (…) s ON true JOIN LATERAL (…) s2 ON true`
//     published `__key_0` and `__key_1` in the client's relation, and the
//     re-hung conditions keyed a STRING against the integer correlation
//     column.
//   - the two-way swap exchanged the sides, and for a manufactured join the
//     side order IS the answer: PostgreSQL publishes the outer relation's
//     columns and then the lateral's.
//
// So this gate is about the star's COLUMN LIST and its ORDER.
// TestN1AGroupedLateralAnswersItsRows holds the values.
//
// THE BOUNDARY IS A CLAIM: the ordinary inner join is still reordered — the
// two controls at the end are a plain two-way join and a three-way one, whose
// column lists this change must not touch — and #988's own shape (independent
// UNGROUPED laterals, whose joins are LEFT and so were never reorderable)
// must not move either.
func TestN1ATwoGroupedLateralsPublishTheirOwnColumns(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const fiveCols = "cols=[id:INT64 customer:STRING total:FLOAT64 p:STRING q:STRING]"
	const eightRows = " rows=8 | " +
		"1,Alice,150,Gadget,Gadget | 1,Alice,150,Gadget,Widget | " +
		"1,Alice,150,Widget,Gadget | 1,Alice,150,Widget,Widget | " +
		"2,Bob,200,Doohickey,Doohickey | 2,Bob,200,Doohickey,Widget | " +
		"2,Bob,200,Widget,Doohickey | 2,Bob,200,Widget,Widget"

	f1Run(t, arms, []f1Case{
		{
			// #1008's exact shape. `cols=[] rows=0` on both single-process
			// arms at base, and the loud STRING/integer key-path mismatch on
			// both DAG arms.
			name: "1008 a star over two grouped laterals",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"JOIN LATERAL (SELECT i2.product AS q FROM lat_item i2 WHERE i2.order_id = o.id GROUP BY i2.product) s2 ON true " +
				"ORDER BY o.id, p, q",
			want: fiveCols + eightRows,
		},
		{
			// ONE grouped lateral: the reorder's two-way SWAP alone, with no
			// chain to rebuild. It published `p, id, customer, total` — the
			// right values under a column ORDER PostgreSQL does not have.
			name: "1008 a star over one grouped lateral",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"ORDER BY o.id, p",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 p:STRING] rows=4 | " +
				"1,Alice,150,Gadget | 1,Alice,150,Widget | 2,Bob,200,Doohickey | 2,Bob,200,Widget",
		},
		{
			// A grouped inner that ALSO aggregates: the values were right at
			// base and the LIST was not — `__key_0` and `__key_1` reached the
			// client, and the lateral's columns came first. Reserved names are
			// what ADR-0026 §3c exists to keep out of a client's relation.
			name: "1008 a star over two grouped laterals that also aggregate",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p, COUNT(*) AS n FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"JOIN LATERAL (SELECT i2.product AS q, SUM(i2.amount) AS sm FROM lat_item i2 WHERE i2.order_id = o.id GROUP BY i2.product) s2 ON true " +
				"ORDER BY o.id, p, q",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 p:STRING n:INT64 q:STRING sm:FLOAT64] rows=8 | " +
				"1,Alice,150,Gadget,1,Gadget,100 | 1,Alice,150,Gadget,1,Widget,50 | " +
				"1,Alice,150,Widget,1,Gadget,100 | 1,Alice,150,Widget,1,Widget,50 | " +
				"2,Bob,200,Doohickey,1,Doohickey,125 | 2,Bob,200,Doohickey,1,Widget,75 | " +
				"2,Bob,200,Widget,1,Doohickey,125 | 2,Bob,200,Widget,1,Widget,75",
		},
		{
			// Two DIFFERENT tables, so the chain rebuild is not a property of
			// reading one table twice.
			name: "1008 a star over two grouped laterals on different tables",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"JOIN LATERAL (SELECT z.customer AS c FROM lat_ord z WHERE z.id = o.id GROUP BY z.customer) s2 ON true " +
				"ORDER BY o.id, p, c",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 p:STRING c:STRING] rows=4 | " +
				"1,Alice,150,Gadget,Alice | 1,Alice,150,Widget,Alice | " +
				"2,Bob,200,Doohickey,Bob | 2,Bob,200,Widget,Bob",
		},
		{
			// NESTED: the second lateral correlates on the first's OUTPUT, so
			// a reorder that puts it first places the inner before the
			// relation it depends on.
			name: "1008 a star over nested grouped laterals",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"JOIN LATERAL (SELECT i2.amount AS am FROM lat_item i2 WHERE i2.product = s.p GROUP BY i2.amount) s2 ON true " +
				"ORDER BY o.id, p, am",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 p:STRING am:FLOAT64] rows=6 | " +
				"1,Alice,150,Gadget,100 | 1,Alice,150,Widget,50 | 1,Alice,150,Widget,75 | " +
				"2,Bob,200,Doohickey,125 | 2,Bob,200,Widget,50 | 2,Bob,200,Widget,75",
		},
		{
			// The LEFT spelling is not reorderable for a reason of its own
			// (outer join order is significant), so it holds the boundary from
			// the other side: right at base's LIST, and its VALUES are the
			// grouping fix's.
			name: "1008 a star over two LEFT grouped laterals",
			sql: "SELECT * FROM lat_ord o " +
				"LEFT JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"LEFT JOIN LATERAL (SELECT i2.product AS q FROM lat_item i2 WHERE i2.order_id = o.id GROUP BY i2.product) s2 ON true " +
				"ORDER BY o.id, p, q",
			want: fiveCols + " rows=9 | " +
				"1,Alice,150,Gadget,Gadget | 1,Alice,150,Gadget,Widget | " +
				"1,Alice,150,Widget,Gadget | 1,Alice,150,Widget,Widget | " +
				"2,Bob,200,Doohickey,Doohickey | 2,Bob,200,Doohickey,Widget | " +
				"2,Bob,200,Widget,Doohickey | 2,Bob,200,Widget,Widget | " +
				"3,Carol,0,NULL,NULL",
		},
		{
			// CONTROL: #988's own shape — two INDEPENDENT ungrouped laterals,
			// whose manufactured joins are LEFT and were never reorderable.
			// Right at base, and this change must not move it.
			name: "control: a star over two independent ungrouped laterals",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"JOIN LATERAL (SELECT MIN(amount) AS mn FROM lat_item i2 WHERE i2.order_id = o.id) s2 ON true " +
				"ORDER BY o.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64 mn:FLOAT64] rows=3 | " +
				"1,Alice,150,100,50 | 2,Bob,200,125,75 | 3,Carol,0,NULL,NULL",
		},
		{
			// CONTROL: an ORDINARY two-way inner join, which the cost model
			// still reorders. Its column list is what it was at base — this
			// change narrows the rule to MANUFACTURED joins and nothing else.
			name: "control: an ordinary two-way inner join",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 " +
				"o.id:INT64 customer:STRING total:FLOAT64] rows=4 | " +
				"1,1,Widget,50,1,Alice,150 | 2,1,Gadget,100,1,Alice,150 | " +
				"3,2,Widget,75,2,Bob,200 | 4,2,Doohickey,125,2,Bob,200",
		},
		{
			// A GROUPED LATERAL CARRYING ITS OWN `ORDER BY`. The rows are
			// PostgreSQL's on every arm — the grouping fix answers it — and on
			// the DAG arms the minted slot RIDES OUT to the client beside
			// them. `stageHiddenPositions` looks for the slot's ordinal in
			// what the lateral's STAGE publishes, and with a Sort of the
			// lateral's own between the projection and the join that list is
			// not the projection's, so the join drops nothing. Not closed
			// here: it is the DAG's stage-stream model (ADR-0026 §3c's
			// distributed half), and it belongs with the arc that gives a
			// lateral's own ORDER BY / LIMIT its per-outer-row meaning, below.
			name: "1008 boundary: a grouped lateral with its own ORDER BY leaks the slot on the DAG",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id " +
				"GROUP BY i.product ORDER BY i.product) s ON true ORDER BY o.id, p",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 p:STRING] rows=4 | " +
				"1,Alice,150,Gadget | 1,Alice,150,Widget | 2,Bob,200,Doohickey | 2,Bob,200,Widget",
			wantDag: "cols=[id:INT64 customer:STRING total:FLOAT64 __key_0:INT64 p:STRING] rows=4 | " +
				"1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | " +
				"2,Bob,200,2,Doohickey | 2,Bob,200,2,Widget",
			wantDagshuf: "cols=[id:INT64 customer:STRING total:FLOAT64 __key_0:INT64 p:STRING] rows=4 | " +
				"1,Alice,150,1,Gadget | 1,Alice,150,1,Widget | " +
				"2,Bob,200,2,Doohickey | 2,Bob,200,2,Widget",
			why: "the lateral's own Sort sits between its projection and the join, " +
				"so the stage publishes a list stageHiddenPositions cannot find the " +
				"slot's ordinal in; the ROWS are PostgreSQL's on all four arms",
		},
		{
			// THE SAME WITH `LIMIT 1`, a wrong answer on every arm, DEFERRED
			// with its mechanism. PostgreSQL evaluates a lateral per outer
			// row, so its LIMIT bounds each one — two rows here, one per
			// order. The decorrelation makes the lateral ONE relation joined
			// once, so the LIMIT bounds the whole of it and a single row
			// survives. Repairing it means the bound travelling with the
			// correlation key (a per-key top-N), which is ADR-0021's territory
			// and not a boundary this arc can move; a bounded repair would put
			// a plausible number where an obvious one is.
			name: "1008 boundary: a grouped lateral's own LIMIT is not per outer row",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id " +
				"GROUP BY i.product ORDER BY i.product LIMIT 1) s ON true ORDER BY o.id, p",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 p:STRING] rows=1 | " +
				"2,Bob,200,Doohickey",
			wantDag: "cols=[id:INT64 customer:STRING total:FLOAT64 __key_0:INT64 p:STRING] rows=1 | " +
				"2,Bob,200,2,Doohickey",
			wantDagshuf: "cols=[id:INT64 customer:STRING total:FLOAT64 __key_0:INT64 p:STRING] rows=1 | " +
				"2,Bob,200,2,Doohickey",
			why: "PostgreSQL 17 answers TWO rows — 1,Alice,150,Gadget and " +
				"2,Bob,200,Doohickey — because a lateral's LIMIT bounds each outer " +
				"row's evaluation; the decorrelated form bounds the whole relation " +
				"once. DEFERRED: the bound has to travel with the correlation key",
		},
		{
			// CONTROL: an ordinary THREE-way inner join, the shape
			// costBasedJoinReorder itself owns.
			name: "control: an ordinary three-way inner join",
			sql: "SELECT COUNT(*) AS n FROM lat_ord o " +
				"JOIN lat_item i ON i.order_id = o.id JOIN lat_item j ON j.order_id = o.id",
			want: "cols=[n:INT64] rows=1 | 8",
		},
	})
}
