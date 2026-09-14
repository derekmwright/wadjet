package coordinator

import (
	"context"
	"testing"
	"time"
)

// A STAR OVER A JOIN PUBLISHES THE QUERY, NOT THE PLAN — #997, #1012, #993,
// on FIVE arms, against PostgreSQL 17.11 measured live over this fixture.
//
// Every `want` below is PostgreSQL's answer. The rule they hold is one
// sentence: `SELECT *` over a join publishes every FROM arm's own column
// list, LEFT ARM FIRST in the clause's written order, duplicate names kept BY
// POSITION and never qualified. The engine used to publish the join
// OPERATOR's stream instead — probe columns then build columns, duplicates
// qualified by the build's alias — and which side probes is a COST decision,
// so one statement published a different relation as the data moved:
//
//   - #997  `SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id`
//     published `…, b.id, …` with no predicate and `…, a.id, …` under
//     `WHERE a.id < 100`, a predicate that changes no row.
//   - #1012 `SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id`
//     published `i`'s four columns before `o`'s three, on every arm.
//   - #993  the same three relations through a derived block published
//     `o.customer`, `o.total` on the SHUFFLE arm and `customer`, `total` on
//     the broadcast one — `markCoPathingSelfJoinBuilds` reads each join's
//     build dependency out of the ARM's stage DAG.
//
// All three have ONE producer and one fix: the star is expanded into the FROM
// clause's arms at Optimize step 1, BEFORE any pass that reorders a join, and
// the projection that publishes it sits above the ORDER BY and the LIMIT
// (logical/star_join_order.go). The join operator's qualification survives
// underneath as a RESOLUTION spelling, which is what every item binds through
// — ADR-0026 §2's pair of names, applied to a join's output.
//
// The table is the SEAM, enumerated once: {inner, left, right, full, cross,
// comma, self, three-way, derived block, CTE} × {no predicate, a selective
// one, a zero-row one, both FROM orders} × {`*`, `t.*`, `*` beside an item,
// a derived `*`} × {no sort, a written key, a positional key, DISTINCT,
// LIMIT} on five arms. `pgwire.TestO1TheWireDeclaresAStarJoinsOwnArms` is the
// same rule on the wire.
func TestO1AStarOverAJoinPublishesTheQueryNotThePlan(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up three embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := c1Arms(t, ctx)

	c1Run(t, arms, []c1Case{
		{
			name: "inner-none-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "inner-none-star-fromrev",
			sql:  "SELECT * FROM lat_item i JOIN lat_ord o ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,1,Widget,50,1,Alice,150 | 2,1,Gadget,100,1,Alice,150 | 3,2,Widget,75,2,Bob,200 | 4,2,Doohickey,125,2,Bob,200",
		},
		{
			name: "inner-sel-left-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id WHERE o.id < 100 ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "inner-sel-right-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id WHERE i.id < 100 ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "inner-zero-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id WHERE o.id < 0",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=0",
		},
		{
			name: "inner-none-qstar-left",
			sql:  "SELECT o.* FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY o.id, i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,Alice,150 | 1,Alice,150 | 2,Bob,200 | 2,Bob,200",
		},
		{
			name: "inner-none-qstar-right",
			sql:  "SELECT i.* FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,1,Widget,50 | 2,1,Gadget,100 | 3,2,Widget,75 | 4,2,Doohickey,125",
		},
		{
			name: "inner-none-two-qstars",
			sql:  "SELECT o.*, i.* FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "inner-none-star-beside",
			sql:  "SELECT *, o.id FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64] rows=4 | 1,Alice,150,1,1,Widget,50,1 | 1,Alice,150,2,1,Gadget,100,1 | 2,Bob,200,3,2,Widget,75,2 | 2,Bob,200,4,2,Doohickey,125,2",
		},
		{
			name: "left-none-star",
			sql:  "SELECT * FROM lat_ord o LEFT JOIN lat_item i ON i.order_id = o.id ORDER BY o.id, i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=5 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125 | 3,Carol,0,NULL,NULL,NULL,NULL",
		},
		{
			name: "left-none-star-fromrev",
			sql:  "SELECT * FROM lat_item i LEFT JOIN lat_ord o ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,1,Widget,50,1,Alice,150 | 2,1,Gadget,100,1,Alice,150 | 3,2,Widget,75,2,Bob,200 | 4,2,Doohickey,125,2,Bob,200",
		},
		{
			name: "left-zero-star",
			sql:  "SELECT * FROM lat_ord o LEFT JOIN lat_item i ON i.order_id = o.id WHERE o.id < 0",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=0",
		},
		{
			name: "right-none-star",
			sql:  "SELECT * FROM lat_item i RIGHT JOIN lat_ord o ON i.order_id = o.id ORDER BY o.id, i.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=5 | 1,1,Widget,50,1,Alice,150 | 2,1,Gadget,100,1,Alice,150 | 3,2,Widget,75,2,Bob,200 | 4,2,Doohickey,125,2,Bob,200 | NULL,NULL,NULL,NULL,3,Carol,0",
		},
		{
			name: "right-none-star-fromrev",
			sql:  "SELECT * FROM lat_ord o RIGHT JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "full-none-star",
			sql:  "SELECT * FROM lat_ord o FULL JOIN lat_item i ON i.order_id = o.id ORDER BY o.id, i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=5 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125 | 3,Carol,0,NULL,NULL,NULL,NULL",
		},
		{
			name: "cross-none-star",
			sql:  "SELECT * FROM lat_ord o CROSS JOIN lat_item i ORDER BY o.id, i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=12 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 1,Alice,150,3,2,Widget,75 | 1,Alice,150,4,2,Doohickey,125 | 2,Bob,200,1,1,Widget,50 | 2,Bob,200,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125 | 3,Carol,0,1,1,Widget,50 | 3,Carol,0,2,1,Gadget,100 | 3,Carol,0,3,2,Widget,75 | 3,Carol,0,4,2,Doohickey,125",
		},
		{
			name: "comma-pred-star",
			sql:  "SELECT * FROM lat_ord o, lat_item i WHERE i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "self-none-star",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | 3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			name: "self-sel-star",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id WHERE a.id < 100 ORDER BY a.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | 3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			name: "self-zero-star",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id WHERE a.id < 0",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=0",
		},
		{
			name: "self-ord-none-star",
			sql:  "SELECT * FROM lat_ord a JOIN lat_ord b ON a.id = b.id ORDER BY a.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=3 | 1,Alice,150,1,Alice,150 | 2,Bob,200,2,Bob,200 | 3,Carol,0,3,Carol,0",
		},
		{
			name: "self-none-qstar-b",
			sql:  "SELECT b.* FROM lat_item a JOIN lat_item b ON a.id = b.id ORDER BY b.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,1,Widget,50 | 2,1,Gadget,100 | 3,2,Widget,75 | 4,2,Doohickey,125",
		},
		{
			name: "self-none-orderby-dup",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id ORDER BY a.order_id, a.amount, b.amount",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | 3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			name: "three-none-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_ord o2 ON o2.id = i.order_id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50,1,Alice,150 | 1,Alice,150,2,1,Gadget,100,1,Alice,150 | 2,Bob,200,3,2,Widget,75,2,Bob,200 | 2,Bob,200,4,2,Doohickey,125,2,Bob,200",
		},
		{
			name: "three-none-star-fromrev",
			sql:  "SELECT * FROM lat_item i JOIN lat_ord o ON i.order_id = o.id JOIN lat_ord o2 ON o2.id = i.order_id ORDER BY i.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,1,Widget,50,1,Alice,150,1,Alice,150 | 2,1,Gadget,100,1,Alice,150,1,Alice,150 | 3,2,Widget,75,2,Bob,200,2,Bob,200 | 4,2,Doohickey,125,2,Bob,200,2,Bob,200",
		},
		{
			name: "three-sel-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_ord o2 ON o2.id = i.order_id WHERE o.id < 100 ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50,1,Alice,150 | 1,Alice,150,2,1,Gadget,100,1,Alice,150 | 2,Bob,200,3,2,Widget,75,2,Bob,200 | 2,Bob,200,4,2,Doohickey,125,2,Bob,200",
		},
		{
			name: "three-zero-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_ord o2 ON o2.id = i.order_id WHERE o.id < 0",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=0",
		},
		{
			name: "three-none-qstar",
			sql:  "SELECT o2.* FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_ord o2 ON o2.id = i.order_id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64] rows=4 | 1,Alice,150 | 1,Alice,150 | 2,Bob,200 | 2,Bob,200",
		},
		{
			name: "derived-join-body-star",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT * FROM lat_item i JOIN lat_ord o2 ON o2.id = i.order_id) s ON s.order_id = o.id ORDER BY s.product, s.amount",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 2,Bob,200,4,2,Doohickey,125,2,Bob,200 | 1,Alice,150,2,1,Gadget,100,1,Alice,150 | 1,Alice,150,1,1,Widget,50,1,Alice,150 | 2,Bob,200,3,2,Widget,75,2,Bob,200",
			pin: map[string]string{
				"single":       "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64 o.id:INT64 o.customer:STRING o.total:FLOAT64] rows=4 | 4,2,Doohickey,125,2,Bob,200,2,Bob,200 | 2,1,Gadget,100,1,Alice,150,1,Alice,150 | 1,1,Widget,50,1,Alice,150,1,Alice,150 | 3,2,Widget,75,2,Bob,200,2,Bob,200",
				"spilled512k":  "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64 o.id:INT64 o.customer:STRING o.total:FLOAT64] rows=4 | 4,2,Doohickey,125,2,Bob,200,2,Bob,200 | 2,1,Gadget,100,1,Alice,150,1,Alice,150 | 1,1,Widget,50,1,Alice,150,1,Alice,150 | 3,2,Widget,75,2,Bob,200,2,Bob,200",
				"dag":          "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 o2.id:INT64 customer:STRING total:FLOAT64 o.id:INT64 o.customer:STRING o.total:FLOAT64] rows=4 | 4,2,Doohickey,125,2,Bob,200,2,Bob,200 | 2,1,Gadget,100,1,Alice,150,1,Alice,150 | 1,1,Widget,50,1,Alice,150,1,Alice,150 | 3,2,Widget,75,2,Bob,200,2,Bob,200",
				"dag-shuffled": "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 o2.id:INT64 o2.customer:STRING o2.total:FLOAT64 o.id:INT64 o.customer:STRING o.total:FLOAT64] rows=4 | 4,2,Doohickey,125,2,Bob,200,2,Bob,200 | 2,1,Gadget,100,1,Alice,150,1,Alice,150 | 1,1,Widget,50,1,Alice,150,1,Alice,150 | 3,2,Widget,75,2,Bob,200,2,Bob,200",
				"dag-morsel4":  "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 o2.id:INT64 customer:STRING total:FLOAT64 o.id:INT64 o.customer:STRING o.total:FLOAT64] rows=4 | 4,2,Doohickey,125,2,Bob,200,2,Bob,200 | 2,1,Gadget,100,1,Alice,150,1,Alice,150 | 1,1,Widget,50,1,Alice,150,1,Alice,150 | 3,2,Widget,75,2,Bob,200,2,Bob,200",
			},
			why: "DECLINED, with the mechanism: the derived block publishes TWO columns " +
				"named `id` — its own body is a star over a join — and every item of an " +
				"expanded star is a QUALIFIED REFERENCE, so `s.id` binds the first of the " +
				"two and the second column would carry the first's VALUES. A wrong value " +
				"is worse than a wrong name, so the star is left to read the stream: this " +
				"cell keeps the values it had at 0193c4e9, in the order it had them. " +
				"Closing it needs a block's column addressed by POSITION.",
		},
		{
			name: "derived-join-body-list",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT i.id AS iid, i.order_id, o2.customer FROM lat_item i JOIN lat_ord o2 ON o2.id = i.order_id) s ON s.order_id = o.id ORDER BY s.iid",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 iid:INT64 order_id:INT64 customer:STRING] rows=4 | 1,Alice,150,1,1,Alice | 1,Alice,150,2,1,Alice | 2,Bob,200,3,2,Bob | 2,Bob,200,4,2,Bob",
			pin: map[string]string{
				"dag":         `ERR sort: key column "s.iid" does not exist in the input schema`,
				"dag-morsel4": `ERR sort: key column "s.iid" does not exist in the input schema`,
			},
			why: "PRE-EXISTING and LOUD, on the BROADCAST arms only — identical at " +
				"0193c4e9, where the shuffle arm failed the same way and now answers. The " +
				"sort key is the derived block's own alias `s.iid` and the stage carrying " +
				"the sort publishes the block's `iid`: a derived-alias sort-key binding, " +
				"not a star's list. The COLUMN LIST this gate is about is right on the " +
				"three arms that answer.",
			routed: map[string]string{"dag-shuffled": "UnreachableOutput +1"},
		},
		{
			name: "derived-join-body-qstar",
			sql:  "SELECT s.* FROM lat_ord o JOIN (SELECT i.id AS iid, i.order_id, o2.customer FROM lat_item i JOIN lat_ord o2 ON o2.id = i.order_id) s ON s.order_id = o.id ORDER BY s.iid",
			want: "cols=[iid:INT64 order_id:INT64 customer:STRING] rows=4 | 1,1,Alice | 2,1,Alice | 3,2,Bob | 4,2,Bob",
			routed: map[string]string{
				"dag": "UnreachableOutput +1", "dag-shuffled": "UnreachableOutput +1",
				"dag-morsel4": "UnreachableOutput +1",
			},
		},
		{
			name: "derived-join-alone-star",
			sql:  "SELECT * FROM (SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id) s ORDER BY s.order_id, s.product",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,2,1,Gadget,100 | 1,Alice,150,1,1,Widget,50 | 2,Bob,200,4,2,Doohickey,125 | 2,Bob,200,3,2,Widget,75",
		},
		{
			name: "cte-join-body-star",
			sql:  "WITH c AS (SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id) SELECT * FROM c ORDER BY c.order_id, c.product",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,2,1,Gadget,100 | 1,Alice,150,1,1,Widget,50 | 2,Bob,200,4,2,Doohickey,125 | 2,Bob,200,3,2,Widget,75",
		},
		{
			name: "cte-joined-star",
			sql:  "WITH c AS (SELECT * FROM lat_item) SELECT * FROM lat_ord o JOIN c ON c.order_id = o.id ORDER BY c.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "paren-bushy-star",
			sql:  "SELECT * FROM lat_ord o JOIN (lat_item i JOIN lat_ord o3 ON o3.id = i.order_id) ON i.order_id = o.id ORDER BY i.id",
			want: "ERR parsing derived table: parsing SQL: expected SELECT",
			why: "NOT an identity question: the PARSER reads a parenthesised FROM item as " +
				"a derived table, so a parenthesised JOIN is refused before any list is " +
				"expanded. Identical at 0193c4e9; PostgreSQL answers the three arms in " +
				"written order.",
		},
		{
			name: "three-way-star-beside",
			sql:  "SELECT *, i.amount FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_ord o2 ON o2.id = i.order_id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50,1,Alice,150,50 | 1,Alice,150,2,1,Gadget,100,1,Alice,150,100 | 2,Bob,200,3,2,Widget,75,2,Bob,200,75 | 2,Bob,200,4,2,Doohickey,125,2,Bob,200,125",
		},
		{
			name: "inner-none-star-limit",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id LIMIT 2",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=2 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100",
		},
		{
			name: "inner-ordinal-sort",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY 4",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "inner-ordinal-sort-desc",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY 4 DESC",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 2,Bob,200,4,2,Doohickey,125 | 2,Bob,200,3,2,Widget,75 | 1,Alice,150,2,1,Gadget,100 | 1,Alice,150,1,1,Widget,50",
		},
		{
			name: "inner-sort-second-arm",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.amount DESC",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 2,Bob,200,4,2,Doohickey,125 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 1,Alice,150,1,1,Widget,50",
		},
		{
			name: "self-sort-both-arms",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.order_id = b.order_id ORDER BY a.id, b.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=8 | 1,1,Widget,50,1,1,Widget,50 | 1,1,Widget,50,2,1,Gadget,100 | 2,1,Gadget,100,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | 3,2,Widget,75,3,2,Widget,75 | 3,2,Widget,75,4,2,Doohickey,125 | 4,2,Doohickey,125,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			name: "self-sort-second-arm",
			sql:  "SELECT * FROM lat_item a JOIN lat_item b ON a.order_id = b.order_id ORDER BY b.id, a.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=8 | 1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,1,1,Widget,50 | 1,1,Widget,50,2,1,Gadget,100 | 2,1,Gadget,100,2,1,Gadget,100 | 3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,3,2,Widget,75 | 3,2,Widget,75,4,2,Doohickey,125 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			name: "inner-sort-limit-desc",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id DESC LIMIT 2",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=2 | 2,Bob,200,4,2,Doohickey,125 | 2,Bob,200,3,2,Widget,75",
		},
		{
			name: "distinct-star-join",
			sql:  "SELECT DISTINCT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
		},
		{
			name: "inner-where-both-sides",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id WHERE o.total > 100 AND i.amount < 100 ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=2 | 1,Alice,150,1,1,Widget,50 | 2,Bob,200,3,2,Widget,75",
		},
		{
			name: "three-ordinal-sort",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id JOIN lat_ord o2 ON o2.id = i.order_id ORDER BY 4 DESC",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING total:FLOAT64] rows=4 | 2,Bob,200,4,2,Doohickey,125,2,Bob,200 | 2,Bob,200,3,2,Widget,75,2,Bob,200 | 1,Alice,150,2,1,Gadget,100,1,Alice,150 | 1,Alice,150,1,1,Widget,50,1,Alice,150",
		},
		{
			name: "inner-star-group",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id GROUP BY o.id, o.customer, o.total, i.id, i.order_id, i.product, i.amount ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
			pin: map[string]string{
				"single":       "cols=[o.id:INT64 customer:STRING total:FLOAT64 i.id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
				"spilled512k":  "cols=[o.id:INT64 customer:STRING total:FLOAT64 i.id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
				"dag":          "cols=[o.id:INT64 customer:STRING total:FLOAT64 i.id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
				"dag-shuffled": "cols=[o.id:INT64 customer:STRING total:FLOAT64 i.id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
				"dag-morsel4":  "cols=[o.id:INT64 customer:STRING total:FLOAT64 i.id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
			},
			why: "NOT a join's list: the star's source here is the AGGREGATE, which " +
				"publishes its GROUP BY keys under the spellings the query wrote (`o.id`, " +
				"`i.id`) rather than the relations' own names. Identical at 0193c4e9 on " +
				"all five arms; a star over an aggregate is its own question.",
		},

		// ------------------------------------------------------------------
		// ROUND 2 — the two dimensions the first census never varied (review
		// P2): a derived arm's ROOT (plain / Sort / LIMIT / DISTINCT / set
		// operation / GROUP BY) and its ITEM KIND (aliased column, plain
		// column, unaliased expression, unaliased aggregate, literal, CAST).
		//
		// The ROOT dimension was a real hole: a block is not always its own
		// projection, and stopping at the root made the star read the JOIN's
		// stream for every sorted, limited or DISTINCT arm — #997's
		// divergence, one node above where the first pass looked for it. Those
		// cells agree with PostgreSQL on all five arms now.
		//
		// The ITEM KIND dimension is the seam's other half and it DECLINES:
		// an expanded item addresses its column by NAME, so an arm whose
		// PUBLISHED name is not the name its producer EMITS cannot be stated
		// (ADR-0026 §9). Those cells keep the values they had at 0193c4e9.
		{
			name: "root-plain-aliased",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id AS k, product AS p FROM lat_item) s ON s.k = o.id ORDER BY s.k",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 p:STRING] rows=3 | 1,Alice,150,1,Widget | 2,Bob,200,2,Gadget | 3,Carol,0,3,Widget",
		},
		{
			name: "root-plain-plaincols",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id, product FROM lat_item) s ON s.id = o.id ORDER BY s.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 product:STRING] rows=3 | 1,Alice,150,1,Widget | 2,Bob,200,2,Gadget | 3,Carol,0,3,Widget",
		},
		{
			name: "root-sort",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id, customer FROM lat_ord ORDER BY id) a ON a.id = o.id ORDER BY a.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 customer:STRING] rows=3 | 1,Alice,150,1,Alice | 2,Bob,200,2,Bob | 3,Carol,0,3,Carol",
		},
		{
			name: "root-limit",
			sql:  "SELECT * FROM (SELECT id, customer FROM lat_ord ORDER BY id LIMIT 2) a JOIN lat_item i ON i.order_id = a.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,1,1,Widget,50 | 1,Alice,2,1,Gadget,100 | 2,Bob,3,2,Widget,75 | 2,Bob,4,2,Doohickey,125",
		},
		{
			name: "root-limit-sel",
			sql:  "SELECT * FROM (SELECT id, customer FROM lat_ord ORDER BY id LIMIT 2) a JOIN lat_item i ON i.order_id = a.id WHERE i.id < 100 ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,1,1,Widget,50 | 1,Alice,2,1,Gadget,100 | 2,Bob,3,2,Widget,75 | 2,Bob,4,2,Doohickey,125",
		},
		{
			name: "root-limit-fromrev",
			sql:  "SELECT * FROM lat_item i JOIN (SELECT id, customer FROM lat_ord ORDER BY id LIMIT 2) a ON i.order_id = a.id ORDER BY i.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 id:INT64 customer:STRING] rows=4 | 1,1,Widget,50,1,Alice | 2,1,Gadget,100,1,Alice | 3,2,Widget,75,2,Bob | 4,2,Doohickey,125,2,Bob",
		},
		{
			name: "root-distinct",
			sql:  "SELECT * FROM (SELECT DISTINCT order_id FROM lat_item) a JOIN lat_ord o ON o.id = a.order_id ORDER BY a.order_id",
			want: "cols=[order_id:INT64 id:INT64 customer:STRING total:FLOAT64] rows=2 | 1,1,Alice,150 | 2,2,Bob,200",
		},
		{
			name: "root-distinct-fromrev",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT DISTINCT order_id FROM lat_item) a ON o.id = a.order_id ORDER BY a.order_id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64] rows=2 | 1,Alice,150,1 | 2,Bob,200,2",
		},
		{
			name: "root-setop",
			sql:  "SELECT * FROM (SELECT id FROM lat_ord UNION ALL SELECT id FROM lat_ord) a JOIN lat_item i ON i.order_id = a.id ORDER BY i.id, a.id",
			want: "cols=[id:INT64 id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=8 | 1,1,1,Widget,50 | 1,1,1,Widget,50 | 1,2,1,Gadget,100 | 1,2,1,Gadget,100 | 2,3,2,Widget,75 | 2,3,2,Widget,75 | 2,4,2,Doohickey,125 | 2,4,2,Doohickey,125",
		},
		{
			name: "root-groupby",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT order_id, COUNT(*) AS n FROM lat_item GROUP BY order_id) s ON s.order_id = o.id ORDER BY s.order_id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 n:INT64] rows=2 | 1,Alice,150,1,2 | 2,Bob,200,2,2",
		},
		{
			name: "root-cte-limit",
			sql:  "WITH c AS (SELECT id, customer FROM lat_ord ORDER BY id LIMIT 2) SELECT * FROM c JOIN lat_item i ON i.order_id = c.id ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | 1,Alice,1,1,Widget,50 | 1,Alice,2,1,Gadget,100 | 2,Bob,3,2,Widget,75 | 2,Bob,4,2,Doohickey,125",
		},
		{
			name: "item-expr",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id AS k, amount * 2 FROM lat_item) s ON s.k = o.id ORDER BY s.k",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:FLOAT64] rows=3 | 1,Alice,150,1,100 | 2,Bob,200,2,200 | 3,Carol,0,3,150",
			// CLOSED by the PAIR (round-2 review, B1): the arm publishes this
			// item under PostgreSQL's name while both engines EMIT it under its
			// own expression text, and a star item carries BOTH — it resolves
			// by the producer's spelling and publishes PostgreSQL's name
			// (`StarColumn`, ADR-0026 §9). It read NULL under a STRING
			// declaration at 3842eaba and the VALUE the unexpanded star gave
			// at 0193c4e9 under the producer's name; it is PostgreSQL's now,
			// name, type and value, on all five arms.
		},
		{
			name: "item-func",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id AS k, UPPER(product) FROM lat_item) s ON s.k = o.id ORDER BY s.k",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 upper:STRING] rows=3 | 1,Alice,150,1,WIDGET | 2,Bob,200,2,GADGET | 3,Carol,0,3,WIDGET",
		},
		{
			name: "item-literal",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id AS k, 1 FROM lat_item) s ON s.k = o.id ORDER BY s.k",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:INT32] rows=3 | 1,Alice,150,1,1 | 2,Bob,200,2,1 | 3,Carol,0,3,1",
			pin: map[string]string{
				"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:INT64] rows=3 | 1,Alice,150,1,1 | 2,Bob,200,2,1 | 3,Carol,0,3,1",
				"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:INT64] rows=3 | 1,Alice,150,1,1 | 2,Bob,200,2,1 | 3,Carol,0,3,1",
				"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:INT64] rows=3 | 1,Alice,150,1,1 | 2,Bob,200,2,1 | 3,Carol,0,3,1",
				"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:INT64] rows=3 | 1,Alice,150,1,1 | 2,Bob,200,2,1 | 3,Carol,0,3,1",
				"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:INT64] rows=3 | 1,Alice,150,1,1 | 2,Bob,200,2,1 | 3,Carol,0,3,1",
			},
			why: "NOT a star question, and the only thing left in it is the DECLARED TYPE: " +
				"the VALUE and the NAME are PostgreSQL's on all five arms, and a bare " +
				"integer literal is declared INT64 here where PostgreSQL declares int4. " +
				"That is the numeric-literal typing rule (ADR-0024), identical for the " +
				"same literal outside a star.",
		},
		{
			name: "item-agg-unaliased",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT order_id, COUNT(*) FROM lat_item GROUP BY order_id) s ON s.order_id = o.id ORDER BY s.order_id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 count:INT64] rows=2 | 1,Alice,150,1,2 | 2,Bob,200,2,2",
			// CLOSED by the PAIR (round-2 review, B1): the arm publishes this
			// item under PostgreSQL's name while both engines EMIT it under its
			// own expression text, and a star item carries BOTH — it resolves
			// by the producer's spelling and publishes PostgreSQL's name
			// (`StarColumn`, ADR-0026 §9). It read NULL under a STRING
			// declaration at 3842eaba and the VALUE the unexpanded star gave
			// at 0193c4e9 under the producer's name; it is PostgreSQL's now,
			// name, type and value, on all five arms.
		},
		{
			name: "item-sum-unaliased",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT order_id, SUM(amount) FROM lat_item GROUP BY order_id) s ON s.order_id = o.id ORDER BY s.order_id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 sum:FLOAT64] rows=2 | 1,Alice,150,1,150 | 2,Bob,200,2,200",
			// CLOSED by the PAIR (round-2 review, B1): the arm publishes this
			// item under PostgreSQL's name while both engines EMIT it under its
			// own expression text, and a star item carries BOTH — it resolves
			// by the producer's spelling and publishes PostgreSQL's name
			// (`StarColumn`, ADR-0026 §9). It read NULL under a STRING
			// declaration at 3842eaba and the VALUE the unexpanded star gave
			// at 0193c4e9 under the producer's name; it is PostgreSQL's now,
			// name, type and value, on all five arms.
		},
		{
			name: "item-cast",
			sql:  "SELECT * FROM lat_ord o JOIN (SELECT id AS k, CAST(amount AS BIGINT) FROM lat_item) s ON s.k = o.id ORDER BY s.k",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 amount:INT64] rows=3 | 1,Alice,150,1,50 | 2,Bob,200,2,100 | 3,Carol,0,3,75",
			// CLOSED by the PAIR (round-2 review, B1): the arm publishes this
			// item under PostgreSQL's name while both engines EMIT it under its
			// own expression text, and a star item carries BOTH — it resolves
			// by the producer's spelling and publishes PostgreSQL's name
			// (`StarColumn`, ADR-0026 §9). It read NULL under a STRING
			// declaration at 3842eaba and the VALUE the unexpanded star gave
			// at 0193c4e9 under the producer's name; it is PostgreSQL's now,
			// name, type and value, on all five arms.
		},
		{
			name: "item-expr-cte",
			sql:  "WITH c AS (SELECT id AS k, amount * 2 FROM lat_item) SELECT * FROM lat_ord o JOIN c ON c.k = o.id ORDER BY c.k",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:FLOAT64] rows=3 | 1,Alice,150,1,100 | 2,Bob,200,2,200 | 3,Carol,0,3,150",
			// CLOSED by the PAIR (round-2 review, B1): the arm publishes this
			// item under PostgreSQL's name while both engines EMIT it under its
			// own expression text, and a star item carries BOTH — it resolves
			// by the producer's spelling and publishes PostgreSQL's name
			// (`StarColumn`, ADR-0026 §9). It read NULL under a STRING
			// declaration at 3842eaba and the VALUE the unexpanded star gave
			// at 0193c4e9 under the producer's name; it is PostgreSQL's now,
			// name, type and value, on all five arms.
		},
		{
			name: "item-expr-derived-alone",
			sql:  "SELECT * FROM (SELECT * FROM lat_ord o JOIN (SELECT id AS k, amount * 2 FROM lat_item) s ON s.k = o.id) d ORDER BY d.k",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 k:INT64 ?column?:FLOAT64] rows=3 | 1,Alice,150,1,100 | 2,Bob,200,2,200 | 3,Carol,0,3,150",
			// CLOSED by the PAIR (round-2 review, B1): the arm publishes this
			// item under PostgreSQL's name while both engines EMIT it under its
			// own expression text, and a star item carries BOTH — it resolves
			// by the producer's spelling and publishes PostgreSQL's name
			// (`StarColumn`, ADR-0026 §9). It read NULL under a STRING
			// declaration at 3842eaba and the VALUE the unexpanded star gave
			// at 0193c4e9 under the producer's name; it is PostgreSQL's now,
			// name, type and value, on all five arms.
		},
		{
			name: "item-agg-cte",
			sql:  "WITH c AS (SELECT order_id, COUNT(*) FROM lat_item GROUP BY order_id) SELECT * FROM lat_ord o JOIN c ON c.order_id = o.id ORDER BY c.order_id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 count:INT64] rows=2 | 1,Alice,150,1,2 | 2,Bob,200,2,2",
			// THE SAME ITEM THROUGH THE OTHER TWO BLOCK SPELLINGS. The pair is
			// a property of the ITEM, so a CTE arm and a derived block over the
			// whole join publish `count` over a stream spelling it `count(*)`
			// exactly as the inline derived arm does — which is what makes it a
			// rule rather than one shape's repair (round-2 review, B1).
		},
		{
			name: "item-agg-derived-alone",
			sql:  "SELECT * FROM (SELECT * FROM lat_ord o JOIN (SELECT order_id, COUNT(*) FROM lat_item GROUP BY order_id) s ON s.order_id = o.id) d ORDER BY d.order_id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 count:INT64] rows=2 | 1,Alice,150,1,2 | 2,Bob,200,2,2",
			// The star is the DERIVED BLOCK's own, and the block republishes
			// what the join published: `count` reaches the client through two
			// star expansions, one inside the other.
		},
		{
			name: "item-expr-qstar-ctl",
			sql:  "SELECT s.* FROM lat_ord o JOIN (SELECT id AS k, amount * 2 FROM lat_item) s ON s.k = o.id ORDER BY s.k",
			want: "cols=[k:INT64 ?column?:FLOAT64] rows=3 | 1,100 | 2,200 | 3,150",
			pin: map[string]string{
				"dag":          "cols=[k:INT64 ?column?:STRING] rows=3 | 1,100 | 2,200 | 3,150",
				"dag-shuffled": "cols=[k:INT64 ?column?:STRING] rows=3 | 1,100 | 2,200 | 3,150",
				"dag-morsel4":  "cols=[k:INT64 ?column?:STRING] rows=3 | 1,100 | 2,200 | 3,150",
			},
			why: "CONTROL for the branch this arc does not own, and it MOVED with arc O2 " +
				"(#1077): a QUALIFIED star over an arm with an unaliased item answered " +
				"NULL on the single-process arms and was a loud `parse projection " +
				"\"s.?column?\"` on the three DAG ones at 0193c4e9. It answers " +
				"PostgreSQL's VALUES on all five arms now; what remains is the DECLARED " +
				"TYPE on the DAG arms — STRING where PostgreSQL declares float8 — which " +
				"is the qualified-star branch's own residual, not the bare star's.",
		},
		{
			name: "item-expr-named-ctl",
			sql:  "SELECT o.id, s.k FROM lat_ord o JOIN (SELECT id AS k, amount * 2 FROM lat_item) s ON s.k = o.id ORDER BY s.k",
			want: "cols=[id:INT64 k:INT64] rows=3 | 1,1 | 2,2 | 3,3",
			// The NAMED control routes local on the two broadcast-family DAG
			// arms (the SELECT list names a column no stage publishes — the
			// arm's unaliased item is not in the join's emitted set), which is
			// answer-preserving and identical at 0193c4e9. Recorded beside the
			// rows because a right-to-routed move is invisible to a value check.
			routed: map[string]string{
				"dag": "UnreachableOutput +1", "dag-shuffled": "UnreachableOutput +1",
				"dag-morsel4": "UnreachableOutput +1",
			},
		},
		{
			name: "lateral-arm-star",
			sql:  "SELECT * FROM lat_ord o, LATERAL (SELECT i.id, i.amount FROM lat_item i WHERE i.order_id = o.id) l ORDER BY o.id, l.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,50 | 1,Alice,150,2,100 | 2,Bob,200,3,75 | 2,Bob,200,4,125",
			pin: map[string]string{
				"single":       "cols=[id:INT64 customer:STRING total:FLOAT64 l.id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,50 | 1,Alice,150,2,100 | 2,Bob,200,3,75 | 2,Bob,200,4,125",
				"spilled512k":  "cols=[id:INT64 customer:STRING total:FLOAT64 l.id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,1,50 | 1,Alice,150,2,100 | 2,Bob,200,3,75 | 2,Bob,200,4,125",
				"dag":          "cols=[id:INT64 customer:STRING total:FLOAT64 i.id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,2,100 | 1,Alice,150,1,50 | 2,Bob,200,4,125 | 2,Bob,200,3,75",
				"dag-shuffled": "cols=[id:INT64 customer:STRING total:FLOAT64 i.id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,2,100 | 1,Alice,150,1,50 | 2,Bob,200,4,125 | 2,Bob,200,3,75",
				"dag-morsel4":  "cols=[id:INT64 customer:STRING total:FLOAT64 i.id:INT64 amount:FLOAT64] rows=4 | 1,Alice,150,2,100 | 1,Alice,150,1,50 | 2,Bob,200,4,125 | 2,Bob,200,3,75",
			},
			why: "DECLINED by design: a written LATERAL's subtree carries the correlation " +
				"slot the join drops (ADR-0026 §3c), so the star is left to read the " +
				"stream, and a join qualifies the build arm's duplicate. Arc L1's alias " +
				"stamp (#1111) reaches it " +
				"and it moves it HALFWAY: the lateral's subtree " +
				"root carries the alias the query wrote now, so the column is published " +
				"as `l.id` rather than the inner scan's `i.id` and `ORDER BY l.id` BINDS " +
				"— the ties are ordered. What is left is the QUALIFIER: PostgreSQL " +
				"publishes the bare `id`, because it does not qualify a duplicate at all.",
		},
		{
			name: "using-star",
			sql:  "SELECT * FROM lat_ord o JOIN lat_item i USING (id) ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING amount:FLOAT64] rows=3 | 1,Alice,150,1,Widget,50 | 2,Bob,200,1,Gadget,100 | 3,Carol,0,2,Widget,75",
			pin: map[string]string{
				"single":       "ERR SELECT * over a JOIN ... USING at position 40 is not supported",
				"spilled512k":  "ERR SELECT * over a JOIN ... USING at position 40 is not supported",
				"dag":          "ERR SELECT * over a JOIN ... USING at position 40 is not supported",
				"dag-shuffled": "ERR SELECT * over a JOIN ... USING at position 40 is not supported",
				"dag-morsel4":  "ERR SELECT * over a JOIN ... USING at position 40 is not supported",
			},
			why: "REFUSED, and it is the ONE place `every arm's own list, concatenated` is " +
				"not PostgreSQL's rule: USING MERGES the joined column into one output " +
				"column (PostgreSQL publishes six here, not seven). The list is knowable " +
				"now — the merge is not implemented — so the refusal states that rather " +
				"than the pre-arc `not resolvable here`. Pre-existing, identical at " +
				"0193c4e9 (#655).",
		},
		{
			name: "natural-star",
			sql:  "SELECT * FROM lat_ord o NATURAL JOIN lat_item i ORDER BY i.id",
			want: "cols=[id:INT64 customer:STRING total:FLOAT64 order_id:INT64 product:STRING amount:FLOAT64] rows=3 | 1,Alice,150,1,Widget,50 | 2,Bob,200,1,Gadget,100 | 3,Carol,0,2,Widget,75",
			pin: map[string]string{
				"single":       "ERR NATURAL JOIN is not supported at position 24",
				"spilled512k":  "ERR NATURAL JOIN is not supported at position 24",
				"dag":          "ERR NATURAL JOIN is not supported at position 24",
				"dag-shuffled": "ERR NATURAL JOIN is not supported at position 24",
				"dag-morsel4":  "ERR NATURAL JOIN is not supported at position 24",
			},
			why: "REFUSED at the PARSER, pre-existing and identical at 0193c4e9: NATURAL " +
				"JOIN is not implemented at all, and its star would merge the joined " +
				"columns the way USING does.",
		},
		{
			name: "derived-aliaslist",
			sql:  "SELECT * FROM (SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id) s (c1, c2, c3, c4, c5, c6, c7) ORDER BY c4",
			want: "cols=[c1:INT64 c2:STRING c3:FLOAT64 c4:INT64 c5:INT64 c6:STRING c7:FLOAT64] rows=4 | 1,Alice,150,1,1,Widget,50 | 1,Alice,150,2,1,Gadget,100 | 2,Bob,200,3,2,Widget,75 | 2,Bob,200,4,2,Doohickey,125",
			pin: map[string]string{
				"single":       "ERR renames the columns of a `SELECT *` this planner did not expand",
				"spilled512k":  "ERR renames the columns of a `SELECT *` this planner did not expand",
				"dag":          "ERR renames the columns of a `SELECT *` this planner did not expand",
				"dag-shuffled": "ERR renames the columns of a `SELECT *` this planner did not expand",
				"dag-morsel4":  "ERR renames the columns of a `SELECT *` this planner did not expand",
			},
			why: "REFUSED, pre-existing and identical at 0193c4e9: the column-alias list " +
				"wraps the block in a SECOND `SELECT *`, and that wrapper's star reads a " +
				"DERIVED TABLE — the branch this pass does not enumerate — so the width " +
				"the list renames is unknown. The star over the JOIN inside the block DID " +
				"expand, which is why the refusal's sentence names the state rather than " +
				"the rule it used to name.",
		},
	})
}
