package coordinator

import (
	"context"
	"testing"
	"time"
)

// A QUALIFIED ORDER BY TERM BINDS THE REFERENCE ITS QUALIFIER NAMES — #989,
// on FOUR arms, every answer measured on live postgres:17-alpine.
//
// `WITH q AS (SELECT order_id, amount FROM lat_item) SELECT * FROM q a JOIN q
// b ON b.order_id = a.order_id ORDER BY a.order_id, a.amount, b.amount` is a
// TOTAL order: every output row differs in the three keys, so exactly one
// sequence is legal and ADR-0013 lists no nondeterminism class that covers it.
// PostgreSQL answers
//
//	(1,50,1,50) (1,50,1,100) (1,100,1,50) (1,100,1,100)
//	(2,75,2,75) (2,75,2,125) (2,125,2,75) (2,125,2,125)
//
// and the single-process and spilled arms answered the trailing key INVERTED
// inside every `(a.order_id, a.amount)` peer group while both DAG arms
// answered PostgreSQL's order.
//
// The mechanism is one line: `buildSort` (and the top-N builder beside it)
// built each key as `cleanExpr(ob.Column)`, which STRIPS the table qualifier.
// The join publishes `[order_id amount b.order_id b.amount]` — the probe's
// columns bare, every DUPLICATE build column qualified by its owning alias —
// so `a.amount` and `b.amount` both became the key `amount`, and
// `exec.columnIndexFallback` bound both of them to the first column carrying
// it. The third key repeated the second and the residual sequence was the
// join's emission order. `sortKeyLocalColumn` keeps the spelling the query
// wrote, which is ADR-0026 §6 at the ORDER BY consumer.
//
// #905 is the same family one shape over: there the Sort's child IS a Project,
// so `sortKeyLocalSlotPos` gives the key a POSITION and the qualifier is not
// needed. A `SELECT *` has no Project to take a position from, which is why
// every cell below that reproduces the defect is a star.
//
// THE BOUNDARY IS A CLAIM, and the controls attempt it from both sides: the
// explicit-select-list spellings (D, F) were already right through #905's
// position and must not move; a star over a join of two DIFFERENT tables (J)
// has no duplicate bare name for the qualifier to disambiguate and must not
// move; the top-N builder (K) is the second sort site and carries the same
// key construction.
func TestL1AQualifiedOrderByTermBindsTheReferenceItNames(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const cte = "WITH q AS (SELECT order_id, amount FROM lat_item) "
	const starCols = "cols=[order_id:INT64 amount:FLOAT64 b.order_id:INT64 b.amount:FLOAT64]"

	f1Run(t, arms, []f1Case{
		{
			// #989's exact shape.
			name: "989 a CTE self-join star, a total ORDER BY across both references",
			sql: cte + "SELECT * FROM q a JOIN q b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: starCols + " rows=8 | 1,50,1,50 | 1,50,1,100 | 1,100,1,50 | 1,100,1,100 | " +
				"2,75,2,75 | 2,75,2,125 | 2,125,2,75 | 2,125,2,125",
		},
		{
			// DESC on the trailing key. This spelling AGREED at base on all
			// four arms — the join's emission order happens to be descending
			// in `b.amount` for this fixture — which is exactly why it is
			// here: a cell that passes for the wrong reason is what the
			// ASC cell above is measured against (protocol method 2).
			name: "989 the same with DESC on the trailing key",
			sql: cte + "SELECT * FROM q a JOIN q b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, a.amount, b.amount DESC",
			want: starCols + " rows=8 | 1,50,1,100 | 1,50,1,50 | 1,100,1,100 | 1,100,1,50 | " +
				"2,75,2,125 | 2,75,2,75 | 2,125,2,125 | 2,125,2,75",
		},
		{
			// The two references SWAPPED in the ORDER BY, so a run that binds
			// both keys to the first `amount` cannot pass in this direction
			// either.
			name: "989 the same with the second reference's key FIRST",
			sql: cte + "SELECT * FROM q a JOIN q b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, b.amount, a.amount",
			want: starCols + " rows=8 | 1,50,1,50 | 1,100,1,50 | 1,50,1,100 | 1,100,1,100 | " +
				"2,75,2,75 | 2,125,2,75 | 2,75,2,125 | 2,125,2,125",
		},
		{
			// A DERIVED-TABLE self-join, which shares no CTE body: the defect
			// is the star over a self-join, not the CTE deduplication the
			// filing suspected.
			name: "989 a derived-table self-join star",
			sql: "SELECT * FROM (SELECT order_id, amount FROM lat_item) a " +
				"JOIN (SELECT order_id, amount FROM lat_item) b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: starCols + " rows=8 | 1,50,1,50 | 1,50,1,100 | 1,100,1,50 | 1,100,1,100 | " +
				"2,75,2,75 | 2,75,2,125 | 2,125,2,75 | 2,125,2,125",
		},
		{
			// A BASE-TABLE self-join star: no CTE and no derived block at all.
			name: "989 a base-table self-join star",
			sql: "SELECT * FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 b.id:INT64 " +
				"b.order_id:INT64 b.product:STRING b.amount:FLOAT64] rows=8 | " +
				"1,1,Widget,50,1,1,Widget,50 | 1,1,Widget,50,2,1,Gadget,100 | " +
				"2,1,Gadget,100,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | " +
				"3,2,Widget,75,3,2,Widget,75 | 3,2,Widget,75,4,2,Doohickey,125 | " +
				"4,2,Doohickey,125,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			// THE BOUNDARY, attempted: a predicate on `a` makes `a` the
			// smaller relation, `reorderJoins` swaps the arms, and the join
			// qualifies `a` instead of `b`. The ORDER must not move, and the
			// fix cannot know which side was qualified — `columnIndexFallback`
			// tries the qualified spelling first and falls back to the bare
			// one, so `b.amount` binds the bare column when `a` is the
			// qualified side and the qualified column when `b` is.
			//
			// The COLUMN LIST here is #997's divergence and is asserted as
			// this tree has it: PostgreSQL publishes `id, order_id, product,
			// amount, id, order_id, product, amount` for BOTH this cell and
			// the one above, in the FROM clause's order. #997 is deferred
			// with its mechanism (see TestL1AStarOverAJoinPublishesThePlanNotTheQuery in
			// arc_l1_star_join_names_two_path_test.go)
			// and its fix restates this cell's `want`.
			name: "989 boundary: a predicate that swaps the arms leaves the ORDER alone",
			sql: "SELECT * FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id " +
				"WHERE a.id < 100 ORDER BY a.order_id, a.amount, b.amount",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 a.id:INT64 " +
				"a.order_id:INT64 a.product:STRING a.amount:FLOAT64] rows=8 | " +
				"1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,1,1,Widget,50 | " +
				"1,1,Widget,50,2,1,Gadget,100 | 2,1,Gadget,100,2,1,Gadget,100 | " +
				"3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,3,2,Widget,75 | " +
				"3,2,Widget,75,4,2,Doohickey,125 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			// THREE references to one CTE, so the middle key is neither the
			// first nor the last column of its name.
			name: "989 three references, the middle one's key last",
			sql: cte + "SELECT * FROM q a JOIN q b ON b.order_id=a.order_id " +
				"JOIN q c ON c.order_id=a.order_id WHERE a.order_id=1 " +
				"ORDER BY a.amount, c.amount, b.amount",
			want: "cols=[order_id:INT64 amount:FLOAT64 b.order_id:INT64 b.amount:FLOAT64 " +
				"c.order_id:INT64 c.amount:FLOAT64] rows=8 | " +
				"1,50,1,50,1,50 | 1,50,1,100,1,50 | 1,50,1,50,1,100 | 1,50,1,100,1,100 | " +
				"1,100,1,50,1,50 | 1,100,1,100,1,50 | 1,100,1,50,1,100 | 1,100,1,100,1,100",
		},
		{
			// The TOP-N builder is the second sort site and had the same key
			// construction (#905's lesson, re-attempted here for the star).
			name: "989 the same under a LIMIT (the top-N builder)",
			sql: cte + "SELECT * FROM q a JOIN q b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, a.amount, b.amount LIMIT 4",
			want: starCols + " rows=4 | 1,50,1,50 | 1,50,1,100 | 1,100,1,50 | 1,100,1,100",
		},
		{
			// CONTROL: the explicit SELECT list. Two outputs named `amount`,
			// bound by POSITION through #905's `sortKeyLocalSlotPos`, right
			// on every arm at base and unmoved by this fix.
			name: "989 control: the explicit select list binds by position (#905)",
			sql: "SELECT a.order_id, a.amount, b.amount FROM lat_item a " +
				"JOIN lat_item b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: "cols=[order_id:INT64 amount:FLOAT64 amount:FLOAT64] rows=8 | " +
				"1,50,50 | 1,50,100 | 1,100,50 | 1,100,100 | " +
				"2,75,75 | 2,75,125 | 2,125,75 | 2,125,125",
		},
		{
			// CONTROL: three same-named outputs in an explicit list.
			name: "989 control: three same-named outputs, the middle key last",
			sql: cte + "SELECT a.amount, b.amount, c.amount FROM q a " +
				"JOIN q b ON b.order_id=a.order_id JOIN q c ON c.order_id=a.order_id " +
				"WHERE a.order_id=1 ORDER BY a.amount, c.amount, b.amount",
			want: "cols=[amount:FLOAT64 amount:FLOAT64 amount:FLOAT64] rows=8 | " +
				"50,50,50 | 50,100,50 | 50,50,100 | 50,100,100 | " +
				"100,50,50 | 100,100,50 | 100,50,100 | 100,100,100",
		},
		{
			// A star over a join of two DIFFERENT tables, which is where the
			// defect is WIDER than the filing said: `lat_ord.id` and
			// `lat_item.id` are one bare name, the join publishes `[id
			// order_id product amount o.id customer total]`, and the leading
			// key `o.id DESC` bound lat_ITEM's id at base. Not a self-join,
			// not a CTE — every star over a join where the two relations
			// share ONE column name is the shape. This cell fails on revert
			// on the single and spilled arms.
			name: "989 a star over a join of two different tables sharing one name",
			sql: "SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id " +
				"ORDER BY o.id DESC, i.amount",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 o.id:INT64 " +
				"customer:STRING total:FLOAT64] rows=4 | " +
				"3,2,Widget,75,2,Bob,200 | 4,2,Doohickey,125,2,Bob,200 | " +
				"1,1,Widget,50,1,Alice,150 | 2,1,Gadget,100,1,Alice,150",
		},
	})
}
