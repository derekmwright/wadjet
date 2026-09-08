package coordinator

import (
	"context"
	"testing"
	"time"
)

// THE MERGE APPLIES THE QUERY'S ORDERING, ON EVERY ARM — #1002, four arms,
// every answer measured on live postgres:17-alpine.
//
// `SELECT DISTINCT * FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id
// ORDER BY a.order_id, a.amount, b.amount` is a TOTAL order: all eight output
// rows differ in the three keys, so exactly one sequence is legal and ADR-0013
// lists no nondeterminism class that covers one.
//
// A star DISTINCT over a self-join is the one user DISTINCT that reaches the
// coordinator un-deduplicated. `rewriteDistinctAsGroupBy` gives every other
// spelling a stage — a projection's items become GROUP BY keys, and a star
// over ONE relation takes `rewriteStarDistinct` — but a star over a self-join
// declines there (`starDistinctGroupKeys` refuses a name two scans publish,
// because collapsing two columns into one over-deduplicates), so `walkStages`
// passes the Distinct through (#163) and `dedupGatherResult` dedups and
// RE-SORTS at the coordinator.
//
// That re-sort bound each key by an exact lookup in `mergeColIdx` and
// `continue`d past a key that missed. The join publishes `[id order_id product
// amount b.id b.order_id b.product b.amount]` — the probe's columns bare, every
// DUPLICATE build column qualified by its owning alias — so neither
// `a.order_id` nor `a.amount` was a key of that map, both were dropped, and
// both DAG arms returned the rows sorted by `b.amount` alone: the LEADING key
// not applied at all. `mergeSortKeyIndices` binds through
// `exec.ColumnIndexFallback`, the resolver the single-process Sort (#989) and
// the DAG's own sort stage already bind through, and a key that still does not
// resolve is an ERROR rather than a silently different order.
//
// THE BOUNDARY IS A CLAIM and the cells attempt it from both sides: the
// non-DISTINCT twin and the single-relation star DISTINCT never reach this
// merge and must not move; the DESC and key-swapped spellings mean a run that
// ignores the key LIST cannot pass in one direction by accident; LIMIT takes
// the top-K heap instead of the full sort, which is the second comparator;
// OFFSET proves the truncation happens after the ordering; and the
// arm-swapping predicate proves the binding needs no model of which side of
// the join built.
func TestM1AMergedOrderIsTheQuerysOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const selfCols = "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 " +
		"b.id:INT64 b.order_id:INT64 b.product:STRING b.amount:FLOAT64]"
	const selfJoin = "SELECT DISTINCT * FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id "

	f1Run(t, arms, []f1Case{
		{
			// #1002's exact shape. PostgreSQL 17:
			//   (1,1,Widget,50)   × (1,1,Widget,50)
			//   (1,1,Widget,50)   × (2,1,Gadget,100)
			//   (2,1,Gadget,100)  × (1,1,Widget,50)
			//   (2,1,Gadget,100)  × (2,1,Gadget,100)
			//   (3,2,Widget,75)   × (3,2,Widget,75)   … and so on.
			name: "1002 a star DISTINCT over a self-join, a total ORDER BY across both references",
			sql:  selfJoin + "ORDER BY a.order_id, a.amount, b.amount",
			want: selfCols + " rows=8 | " +
				"1,1,Widget,50,1,1,Widget,50 | 1,1,Widget,50,2,1,Gadget,100 | " +
				"2,1,Gadget,100,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | " +
				"3,2,Widget,75,3,2,Widget,75 | 3,2,Widget,75,4,2,Doohickey,125 | " +
				"4,2,Doohickey,125,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			// DESC on the trailing key: what said the key list was MIS-ORDERED
			// rather than truncated. The two DAG arms answered
			// `(b.amount DESC, a.amount ASC)` here and `(b.amount ASC,
			// a.amount ASC)` above, which is one dropped leading key and one
			// surviving trailing one, not a truncation.
			name: "1002 the same with DESC on the trailing key",
			sql:  selfJoin + "ORDER BY a.order_id, a.amount, b.amount DESC",
			want: selfCols + " rows=8 | " +
				"1,1,Widget,50,2,1,Gadget,100 | 1,1,Widget,50,1,1,Widget,50 | " +
				"2,1,Gadget,100,2,1,Gadget,100 | 2,1,Gadget,100,1,1,Widget,50 | " +
				"3,2,Widget,75,4,2,Doohickey,125 | 3,2,Widget,75,3,2,Widget,75 | " +
				"4,2,Doohickey,125,4,2,Doohickey,125 | 4,2,Doohickey,125,3,2,Widget,75",
		},
		{
			// The two references SWAPPED, so a run that binds both `amount`
			// keys to one column cannot pass in this direction either.
			name: "1002 the same with the second reference's key first",
			sql:  selfJoin + "ORDER BY a.order_id, b.amount, a.amount",
			want: selfCols + " rows=8 | " +
				"1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,1,1,Widget,50 | " +
				"1,1,Widget,50,2,1,Gadget,100 | 2,1,Gadget,100,2,1,Gadget,100 | " +
				"3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,3,2,Widget,75 | " +
				"3,2,Widget,75,4,2,Doohickey,125 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			// A LIMIT takes the top-K HEAP rather than the full sort — the
			// second comparator, which read the same dropped-key indices.
			name: "1002 the same under a LIMIT (the top-K heap)",
			sql:  selfJoin + "ORDER BY a.order_id, a.amount, b.amount LIMIT 4",
			want: selfCols + " rows=4 | " +
				"1,1,Widget,50,1,1,Widget,50 | 1,1,Widget,50,2,1,Gadget,100 | " +
				"2,1,Gadget,100,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100",
		},
		{
			// An OFFSET with no LIMIT: the truncation is applied AFTER the
			// ordering, so a dropped key moves WHICH rows come back and not
			// only their sequence — a wrong ROW SET, not only a wrong order.
			// At base both DAG arms returned four rows PostgreSQL does not
			// return here.
			name: "1002 the same under an OFFSET (the skipped rows are the ordered ones)",
			sql:  selfJoin + "ORDER BY a.order_id, a.amount, b.amount OFFSET 4",
			want: selfCols + " rows=4 | " +
				"3,2,Widget,75,3,2,Widget,75 | 3,2,Widget,75,4,2,Doohickey,125 | " +
				"4,2,Doohickey,125,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			// THE BOUNDARY, attempted: a predicate on `a` makes `a` the
			// smaller relation, `reorderJoins` swaps the arms and the join
			// qualifies `a` instead of `b`. The ORDER must not move.
			// `ColumnIndexFallback` tries the qualified spelling first and the
			// bare one second, so `b.amount` binds the bare column when `a`
			// is the qualified side and the qualified one when `b` is — the
			// fix needs no model of which side built.
			//
			// The LEADING key is `b.amount`, which the swap leaves as the
			// stream's BARE `amount`: the old exact lookup missed it and
			// dropped the key that decides the whole sequence. A trailing
			// unresolved key can pass by accident of the dedup's emission
			// order, and did — this cell puts the missing key first so it
			// cannot.
			//
			// The COLUMN LIST here is #997's divergence, asserted as this tree
			// has it: PostgreSQL publishes `id, order_id, product, amount`
			// twice, in the FROM clause's order.
			name: "1002 boundary: a predicate that swaps the arms leaves the order alone",
			sql:  selfJoin + "WHERE a.id < 100 ORDER BY b.amount DESC, a.order_id, a.amount",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 a.id:INT64 " +
				"a.order_id:INT64 a.product:STRING a.amount:FLOAT64] rows=8 | " +
				"4,2,Doohickey,125,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125 | " +
				"2,1,Gadget,100,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | " +
				"3,2,Widget,75,3,2,Widget,75 | 3,2,Widget,75,4,2,Doohickey,125 | " +
				"1,1,Widget,50,1,1,Widget,50 | 1,1,Widget,50,2,1,Gadget,100",
			why: "#997: the star's column ORDER and its qualified side follow the PLAN, so " +
				"the arm-swapped spelling publishes `a.` where the unswapped one publishes " +
				"`b.`; PostgreSQL publishes the FROM arms in written order with duplicates " +
				"kept by position. The ROWS and their ORDER are PostgreSQL's on all four arms.",
		},
		{
			// TWO DIFFERENT TABLES that share one column name, so the shape is
			// not a self-join: `lat_ord.id` and `lat_item.id` are one bare
			// name and the leading key is the qualified one. PostgreSQL's rows
			// and order, in PostgreSQL's own sequence; the column ORDER is
			// #997's again.
			name: "1002 a star DISTINCT over two tables sharing one column name",
			sql: "SELECT DISTINCT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id " +
				"ORDER BY i.amount DESC, o.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64 o.id:INT64 " +
				"customer:STRING total:FLOAT64] rows=4 | " +
				"4,2,Doohickey,125,2,Bob,200 | 2,1,Gadget,100,1,Alice,150 | " +
				"3,2,Widget,75,2,Bob,200 | 1,1,Widget,50,1,Alice,150",
			why: "#997: PostgreSQL publishes `o` first (id, customer, total) then `i`; this " +
				"tree publishes the join operator's order. Same rows, same sequence.",
		},
		{
			// CONTROL: the NON-DISTINCT twin. It has a sort STAGE, never
			// reaches this merge, and was right on all four arms at base
			// (#989's headline shape). It must not move.
			name: "1002 control: the non-DISTINCT twin has a sort stage",
			sql: "SELECT * FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id " +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: selfCols + " rows=8 | " +
				"1,1,Widget,50,1,1,Widget,50 | 1,1,Widget,50,2,1,Gadget,100 | " +
				"2,1,Gadget,100,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | " +
				"3,2,Widget,75,3,2,Widget,75 | 3,2,Widget,75,4,2,Doohickey,125 | " +
				"4,2,Doohickey,125,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125",
		},
		{
			// CONTROL: a star DISTINCT over ONE relation. `rewriteStarDistinct`
			// reads the group keys off the scan, so this becomes a GROUP BY
			// with a stage of its own and never reaches the coordinator's
			// dedup either. Right at base, and the cell that says the fix is
			// scoped to the shape that declines that rewrite.
			name: "1002 control: a star DISTINCT over one relation is a GROUP BY",
			sql:  "SELECT DISTINCT * FROM lat_item a ORDER BY a.amount DESC, a.id",
			want: "cols=[id:INT64 order_id:INT64 product:STRING amount:FLOAT64] rows=4 | " +
				"4,2,Doohickey,125 | 2,1,Gadget,100 | 3,2,Widget,75 | 1,1,Widget,50",
		},
	})
}
