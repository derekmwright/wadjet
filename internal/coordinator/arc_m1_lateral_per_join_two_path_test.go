package coordinator

import (
	"context"
	"testing"
	"time"
)

// EVERY LATERAL JOIN DROPS THE SLOT IT MINTED AND APPLIES ITS OWN DEFAULTS —
// #988, four arms, every answer measured on live postgres:17-alpine.
//
// A decorrelated LATERAL materializes its correlation key into `__key_N` so
// the join it manufactures has a column to key on, and the join drops that
// column from its output — except where the lateral is an UNGROUPED aggregate,
// where the marker is what the empty-input default operator ABOVE the join
// reads, so that operator drops it instead (`exec.LateralEmptyDefault`,
// ADR-0026 §3c).
//
// Both of those belong to the JOIN that manufactured the padded row, and
// `fuseStageChains` absorbs a 1:1 downstream join into its upstream stage as a
// `ChainedJoinSpec` — which carried the absorbed join's `HiddenJoinCols` but
// neither its pad marker nor its empty-input defaults. With two independent
// laterals over one table the second join is absorbed into the first's
// fragment, so exactly one drop and one default ran per QUERY:
//
//   - `SELECT * FROM lat_ord o JOIN LATERAL (SELECT MAX(amount) AS mx …) s ON
//     true JOIN LATERAL (SELECT MIN(amount) AS mn …) s2 ON true` published
//     `id, customer, total, mx, __key_1, mn` on both DAG arms where PostgreSQL
//     publishes five columns — a reserved name reaching the client.
//   - The SILENT half, which the filing does not mention: the second lateral's
//     empty-input DEFAULT was lost with it. `COUNT(*) + 1` came back NULL for
//     the order with no items, where PostgreSQL answers 1.
//
// `WADJET_STAGE_FUSION=0` answered PostgreSQL on both DAG arms at base, which
// is what localizes it to the fusion rather than to the lowering.
//
// THE BOUNDARY IS A CLAIM: one lateral was already right (its join keeps its
// own stage), and the `o.*` cell holds the other side — a qualified star names
// one relation and never sees the slot at all, so it must not move.
func TestM1AEveryLateralJoinDropsItsOwnSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const two = "FROM lat_ord o " +
		"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
		"JOIN LATERAL (SELECT MIN(amount) AS mn FROM lat_item i2 WHERE i2.order_id = o.id) s2 ON true "
	const fiveCols = "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64 mn:FLOAT64]"
	const fiveRows = " rows=3 | 1,Alice,150,100,50 | 2,Bob,200,125,75 | 3,Carol,0,NULL,NULL"

	f1Run(t, arms, []f1Case{
		{
			// #988's exact shape: two INDEPENDENT laterals over ONE table.
			name: "988 two independent laterals over one table publish five columns",
			sql:  "SELECT * " + two + "ORDER BY o.id",
			want: fiveCols + fiveRows,
		},
		{
			// THE SILENT HALF. An ungrouped aggregate over an empty input
			// still yields a row in PostgreSQL, so Carol — the order with no
			// items — takes `COUNT(*) + 1 = 1` in BOTH columns. The absorbed
			// join's defaults were dropped with its pad marker, so the second
			// column came back NULL: a wrong VALUE, not a leaked name.
			name: "988 the absorbed lateral's empty-input default is applied",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT COUNT(*) + 1 AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"JOIN LATERAL (SELECT COUNT(*) + 1 AS mn FROM lat_item i2 WHERE i2.order_id = o.id) s2 ON true " +
				"ORDER BY o.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mx:INT64 mn:INT64] rows=3 | " +
				"1,Alice,150,3,3 | 2,Bob,200,3,3 | 3,Carol,0,1,1",
		},
		{
			// THREE laterals: the chain absorbs twice, so a fix that carries
			// one absorbed join's rules and not the next one's fails here.
			// Both `__key_1` and `__key_2` reached the client at base.
			name: "988 three independent laterals over one table",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"JOIN LATERAL (SELECT MIN(amount) AS mn FROM lat_item i2 WHERE i2.order_id = o.id) s2 ON true " +
				"JOIN LATERAL (SELECT SUM(amount) AS sm FROM lat_item i3 WHERE i3.order_id = o.id) s3 ON true " +
				"ORDER BY o.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64 mn:FLOAT64 sm:FLOAT64] rows=3 | " +
				"1,Alice,150,100,50,150 | 2,Bob,200,125,75,200 | 3,Carol,0,NULL,NULL,NULL",
		},
		{
			// Two laterals over DIFFERENT tables, so the shape is not a
			// property of reading one table twice.
			name: "988 two laterals over different tables",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"JOIN LATERAL (SELECT COUNT(*) AS c FROM lat_ord z WHERE z.id = o.id) s2 ON true " +
				"ORDER BY o.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64 c:INT64] rows=3 | " +
				"1,Alice,150,100,1 | 2,Bob,200,125,1 | 3,Carol,0,NULL,1",
		},
		{
			// NESTED: the second lateral correlates on the FIRST one's output,
			// so the two joins are a real chain rather than two siblings.
			// Carol's `mx` is NULL, nothing matches it, and PostgreSQL's
			// COUNT over that empty input is 0.
			name: "988 nested laterals, the second correlating on the first",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"JOIN LATERAL (SELECT COUNT(*) AS c FROM lat_item i2 WHERE i2.amount = s.mx) s2 ON true " +
				"ORDER BY o.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64 c:INT64] rows=3 | " +
				"1,Alice,150,100,1 | 2,Bob,200,125,1 | 3,Carol,0,NULL,0",
		},
		{
			// A DERIVED star over the whole thing: the drop is below every
			// star, so one join-level fix covers the outer `*`, this one and
			// a CTE's (ADR-0026 §3c).
			name: "988 a derived star over two independent laterals",
			sql:  "SELECT * FROM (SELECT * " + two + ") d ORDER BY d.id",
			want: fiveCols + fiveRows,
		},
		{
			// A CTE's star, the third consumer of the same drop.
			name: "988 a CTE star over two independent laterals",
			sql:  "WITH c AS (SELECT * " + two + ") SELECT * FROM c ORDER BY id",
			want: fiveCols + fiveRows,
		},
		{
			// The LEFT spelling of both joins, which is what an outer row the
			// lateral matches nothing for takes anyway — the pad is the same
			// pad and the marker the same marker.
			name: "988 the LEFT JOIN LATERAL spelling",
			sql: "SELECT * FROM lat_ord o " +
				"LEFT JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"LEFT JOIN LATERAL (SELECT MIN(amount) AS mn FROM lat_item i2 WHERE i2.order_id = o.id) s2 ON true " +
				"ORDER BY o.id",
			want: fiveCols + fiveRows,
		},
		{
			// CONTROL: a QUALIFIED star names one relation, so it never
			// published the slot even at base. It must not move.
			name: "988 control: a qualified star beside two laterals",
			sql:  "SELECT o.*, s2.mn " + two + "ORDER BY o.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mn:FLOAT64] rows=3 | " +
				"1,Alice,150,50 | 2,Bob,200,75 | 3,Carol,0,NULL",
		},
		{
			// CONTROL: ONE lateral. Its join keeps a stage of its own, so its
			// drop and its default always ran — right at base on all four arms
			// and the cell that says the defect is the ABSORBED join.
			name: "988 control: one lateral was always right",
			sql: "SELECT * FROM lat_ord o " +
				"JOIN LATERAL (SELECT MAX(amount) AS mx FROM lat_item i WHERE i.order_id = o.id) s ON true " +
				"ORDER BY o.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 mx:FLOAT64] rows=3 | " +
				"1,Alice,150,100 | 2,Bob,200,125 | 3,Carol,0,NULL",
		},
	})
}
