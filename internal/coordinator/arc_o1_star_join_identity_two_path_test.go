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
	})
}
