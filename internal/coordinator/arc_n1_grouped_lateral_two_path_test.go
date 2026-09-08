package coordinator

import (
	"context"
	"testing"
	"time"
)

// n1RoutedLocal is the disposition every cell below MEASURES on both DAG
// arms: a lateral join whose SELECT list NAMES its columns is refused by the
// stage planner and answered by the coordinator's in-process path
// (`UnreachableOutputLocalRoutes`). It is recorded rather than wished away —
// rows alone cannot tell an executed query from a routed one — and the STAR
// spelling in TestN1ATwoGroupedLateralsPublishTheirOwnColumns is the cell that
// really runs as stages.
var n1RoutedLocal = map[string]string{
	"dag":     "unreachable output +1",
	"dagshuf": "unreachable output +1",
}

// A LATERAL WHOSE INNER BLOCK **GROUPS** IS AN AGGREGATE TO THE LOWERING —
// #1008, four arms, every answer measured on live postgres:17-alpine.
//
// `buildLateralSubquery` decided "is this an aggregate?" by walking the
// SELECT list for an aggregate CALL, while `BuildFromSelect` — the very next
// thing it calls — builds an Aggregate node on `hasAgg || len(GroupBy) > 0`.
// A block that GROUPS without calling one falls in the gap:
//
//	JOIN LATERAL (SELECT i.product AS p FROM lat_item i
//	              WHERE i.order_id = o.id GROUP BY i.product) s ON true
//
// The lowering minted the correlation key into the select list as `__key_0`
// and did NOT add it to the GROUP BY, so the aggregate below published ONE
// key column — the product, a STRING — under the slot the join keys on. Both
// single-process arms answered ZERO ROWS (and, being a star over a join, no
// columns at all) where PostgreSQL 17 answers four; both DAG arms were the
// loud `join key "s.__key_0" is STRING on the probe side and the build side
// took the integer key path` (#615). The LEFT spelling padded every outer row
// instead: three all-NULL rows for PostgreSQL's nine.
//
// THE CELLS NAME THEIR COLUMNS. The star's column LIST over two joins is the
// other half of #1008 and it belongs to the reorder fix
// (`TestN1ATwoGroupedLateralsPublishTheirOwnColumns`); what this gate holds is
// the VALUES, which the grouping decides.
//
// THE BOUNDARY IS A CLAIM: the ungrouped-aggregate lateral (`MAX(amount)`)
// went through this code before and after and must not move, and neither must
// the shape whose inner both GROUPS and aggregates — that one always looked
// like an aggregate to the walk.
func TestN1AGroupedLateralAnswersItsRows(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	f1Run(t, arms, []f1Case{
		{
			// #1008's own shape, with the columns named so the answer is the
			// VALUES rather than the star's list.
			name: "1008 two grouped laterals over one table",
			sql: "SELECT o.id, o.customer, s.p, s2.q FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"JOIN LATERAL (SELECT i2.product AS q FROM lat_item i2 WHERE i2.order_id = o.id GROUP BY i2.product) s2 ON true " +
				"ORDER BY o.id, s.p, s2.q",
			want: "cols=[id:INT64 customer:STRING p:STRING q:STRING] rows=8 | " +
				"1,Alice,Gadget,Gadget | 1,Alice,Gadget,Widget | 1,Alice,Widget,Gadget | 1,Alice,Widget,Widget | " +
				"2,Bob,Doohickey,Doohickey | 2,Bob,Doohickey,Widget | 2,Bob,Widget,Doohickey | 2,Bob,Widget,Widget",
			routed: n1RoutedLocal,
		},
		{
			// ONE grouped lateral: the defect is not a property of having two.
			// This cell is the smallest thing that reproduced it.
			name: "1008 one grouped lateral",
			sql: "SELECT o.id, o.customer, s.p FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"ORDER BY o.id, s.p",
			want: "cols=[id:INT64 customer:STRING p:STRING] rows=4 | " +
				"1,Alice,Gadget | 1,Alice,Widget | 2,Bob,Doohickey | 2,Bob,Widget",
			routed: n1RoutedLocal,
		},
		{
			// The LEFT spelling, where the wrong answer was not zero rows but
			// three PADDED ones — an outer row per order with every lateral
			// column NULL. Carol, the order with no items, is the only row
			// PostgreSQL pads.
			name: "1008 the LEFT JOIN LATERAL spelling",
			sql: "SELECT o.id, o.customer, s.p, s2.q FROM lat_ord o " +
				"LEFT JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"LEFT JOIN LATERAL (SELECT i2.product AS q FROM lat_item i2 WHERE i2.order_id = o.id GROUP BY i2.product) s2 ON true " +
				"ORDER BY o.id, s.p, s2.q",
			want: "cols=[id:INT64 customer:STRING p:STRING q:STRING] rows=9 | " +
				"1,Alice,Gadget,Gadget | 1,Alice,Gadget,Widget | 1,Alice,Widget,Gadget | 1,Alice,Widget,Widget | " +
				"2,Bob,Doohickey,Doohickey | 2,Bob,Doohickey,Widget | 2,Bob,Widget,Doohickey | 2,Bob,Widget,Widget | " +
				"3,Carol,NULL,NULL",
			routed: n1RoutedLocal,
		},
		{
			// Two DIFFERENT tables, so the shape is not a property of reading
			// one table twice.
			name: "1008 two grouped laterals over different tables",
			sql: "SELECT o.id, s.p, s2.c FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"JOIN LATERAL (SELECT z.customer AS c FROM lat_ord z WHERE z.id = o.id GROUP BY z.customer) s2 ON true " +
				"ORDER BY o.id, s.p, s2.c",
			want: "cols=[id:INT64 p:STRING c:STRING] rows=4 | " +
				"1,Gadget,Alice | 1,Widget,Alice | 2,Doohickey,Bob | 2,Widget,Bob",
			routed: n1RoutedLocal,
		},
		{
			// NESTED: the second grouped lateral correlates on the FIRST
			// one's output, so the two joins are a chain rather than
			// siblings and the second one's key is a STRING by rights.
			name: "1008 nested grouped laterals",
			sql: "SELECT o.id, s.p, s2.am FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"JOIN LATERAL (SELECT i2.amount AS am FROM lat_item i2 WHERE i2.product = s.p GROUP BY i2.amount) s2 ON true " +
				"ORDER BY o.id, s.p, s2.am",
			want: "cols=[id:INT64 p:STRING am:FLOAT64] rows=6 | " +
				"1,Gadget,100 | 1,Widget,50 | 1,Widget,75 | 2,Doohickey,125 | 2,Widget,50 | 2,Widget,75",
			routed: n1RoutedLocal,
		},
		{
			// CONTROL: an inner that both GROUPS and AGGREGATES always
			// looked like an aggregate to the walk, so it was right at base
			// and must stay right.
			name: "control: a grouped inner that also aggregates",
			sql: "SELECT o.id, s.p, s.n FROM lat_ord o " +
				"JOIN LATERAL (SELECT i.product AS p, COUNT(*) AS n FROM lat_item i WHERE i.order_id = o.id GROUP BY i.product) s ON true " +
				"ORDER BY o.id, s.p",
			want: "cols=[id:INT64 p:STRING n:INT64] rows=4 | " +
				"1,Gadget,1 | 1,Widget,1 | 2,Doohickey,1 | 2,Widget,1",
			routed: n1RoutedLocal,
		},
		{
			// CONTROL: the UNGROUPED aggregate lateral, which is what every
			// lateral gate in this package writes. Right at base, unmoved.
			name: "control: an ungrouped aggregate lateral",
			sql: "SELECT o.id, s.mx FROM lat_ord o " +
				"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"ORDER BY o.id",
			// …and it EXECUTES as stages, where every grouped cell above
			// routes local: the disposition is part of the control.
			want: "cols=[id:INT64 mx:FLOAT64] rows=3 | 1,100 | 2,125 | 3,NULL",
		},
	})
}
