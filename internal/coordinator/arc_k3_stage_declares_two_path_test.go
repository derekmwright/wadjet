package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A DERIVED BLOCK A STAR READS PUBLISHES ITS OWN PROJECTION (#984).
//
// A Project emits no stage, so on the DAG a derived table's SELECT list was
// not a relation: the Aggregate or the Scan below it materialized, and a
// `SELECT *` above the join published THAT. Every consumer with a NAME
// compensated (resolveShuffleKey, resolveAggInputName, the gather's
// OutputRenames), and the star — the one consumer with no names, reading by
// POSITION — did not.
//
// Four spellings, all measured silently wrong on both DAG arms at v0.18.60
// and all four one question, "is the block's projection the stage's column
// list":
//
//	dup       SELECT order_id, order_id AS oid   `oid` GONE
//	rename    SELECT order_id AS k               published as `order_id`
//	agg-alias SELECT CAST(COUNT(*) AS VARCHAR) AS n   `__agg_0` reached the client
//	computed  SELECT amount * 2 AS d             `amount` reached the client too
//
// EVERY `want` is PostgreSQL 17's column SET and values over the same rows.
// The column ORDER is asserted as `single`'s, which is the join operator's own
// (probe side first) and diverges from PostgreSQL's FROM order on every arm —
// a PRE-EXISTING divergence recorded in ADR-0012, older than this arc and not
// what this issue is. What the cells assert is that the four arms describe ONE
// relation: same columns, same names, same values.
//
// THE CONTROLS ARE THE LOAD-BEARING HALF. A pass that materialized every
// derived block would pass every cell above and cost every ordinary
// distributed query a projection stage it does not need. So: a named SELECT
// list over each of the same blocks stays exactly as it was, a block that IS
// its stream is untouched, and the TPC-H stage-dump golden is byte-identical
// (22 queries, none of them a star).
func TestArcK3ADerivedBlockPublishesItsOwnProjection(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	for _, tc := range []struct {
		name, sql, want, wantDAG string
		// wantRouted says the DISTRIBUTED arms answer by handing the query to
		// the coordinator-local pipeline rather than by running the DAG. It is
		// asserted on EVERY cell, in both directions, because the ROWS cannot
		// tell the two apart and the route is not answer-preserving: the local
		// pipeline's ORDER BY is wrong for some shapes the DAG gets right, so
		// "right rows" is not evidence that a cell is well. Round 1 found two
		// shapes that had moved from executed to routed behind a green gate
		// that read only rows.
		wantRouted bool
		// wantUnreachableRoute is the OTHER route a cell here can take: the
		// SELECT-list reachability refusal (#656). A block whose own ORDER BY
		// keys on a column the block does NOT publish is refused there, not by
		// this arc's refusal, and a gate that watched only this arc's counter
		// called such a cell "executed" — which is how round 5's report came
		// to state a rule the tree does not have.
		wantUnreachableRoute bool
	}{
		// A SOURCE COLUMN PUBLISHED TWICE. The stream carries one column
		// called `order_id` and cannot answer to it twice; the projection
		// reads it twice, which is what PostgreSQL publishes.
		{name: "dup/derived",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, order_id AS oid ` +
				`FROM lat_item) s ON s.order_id = o.id ORDER BY o.id`,
			want: `order_id,oid,id,customer,total | 1,1,1,Alice,150 | 1,1,1,Alice,150 | ` +
				`2,2,2,Bob,200 | 2,2,2,Bob,200`},
		{name: "dup/cte",
			sql: `WITH q AS (SELECT order_id, order_id AS oid FROM lat_item) ` +
				`SELECT * FROM lat_ord o JOIN q ON q.order_id = o.id ORDER BY o.id`,
			want: `id,customer,total,order_id,oid | 1,Alice,150,1,1 | 1,Alice,150,1,1 | ` +
				`2,Bob,200,2,2 | 2,Bob,200,2,2`},
		// A RENAME the stream does not carry. The join KEYS on the alias
		// too, so this cell is also the proof that the key binds to what the
		// producer publishes rather than to the source it once resolved to —
		// without that the shuffle refused `key "order_id" not in schema`.
		{name: "rename/join-keys-on-the-alias",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id AS k, amount ` +
				`FROM lat_item) d ON d.k = o.id ORDER BY o.id, d.amount`,
			want: `k,amount,id,customer,total | 1,50,1,Alice,150 | 1,100,1,Alice,150 | ` +
				`2,75,2,Bob,200 | 2,125,2,Bob,200`},
		// AN ALIAS OVER AN AGGREGATE. The aggregate stage emits `__agg_0`
		// beside the computed `n`; a reserved slot reaching a client is worse
		// than a lost column, because no query can spell it to avoid it.
		{name: "agg-alias/having",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, CAST(COUNT(*) AS VARCHAR) ` +
				`AS n FROM lat_item GROUP BY order_id HAVING COUNT(*) > 1) s ` +
				`ON s.order_id = o.id ORDER BY o.id`,
			want: `id,customer,total,order_id,n | 1,Alice,150,1,2 | 2,Bob,200,2,2`},
		// A COMPUTED item. absorbComputedSubqueryProjection is deliberately
		// ADDITIVE — the computed column is appended and the source it reads
		// stays — so the star saw both.
		{name: "computed/source-does-not-leak",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, amount * 2 AS d ` +
				`FROM lat_item) s ON s.order_id = o.id ORDER BY o.id, d`,
			want: `order_id,d,id,customer,total | 1,100,1,Alice,150 | 1,200,1,Alice,150 | ` +
				`2,150,2,Bob,200 | 2,250,2,Bob,200`},

		// CONTROLS — none of these may move.
		{name: "ctl/block-is-its-stream",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id FROM lat_item) s ` +
				`ON s.order_id = o.id ORDER BY o.id`,
			want: `order_id,id,customer,total | 1,1,Alice,150 | 1,1,Alice,150 | ` +
				`2,2,Bob,200 | 2,2,Bob,200`},
		// The DAG orders the star's columns PROBE SIDE FIRST here where the
		// single-process arms put the outer table first. Same set, same
		// names, same values; a PRE-EXISTING per-arm column ORDER divergence
		// (ADR-0012's star-column-order entry) that this arc neither creates
		// nor closes, pinned per arm rather than described.
		{name: "ctl/derived-aggregate-is-its-stream",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, COUNT(*) AS n ` +
				`FROM lat_item GROUP BY order_id) s ON s.order_id = o.id ORDER BY o.id`,
			want:    `id,customer,total,order_id,n | 1,Alice,150,1,2 | 2,Bob,200,2,2`,
			wantDAG: `order_id,n,id,customer,total | 1,2,1,Alice,150 | 2,2,2,Bob,200`},
		{name: "ctl/no-join-above-the-block",
			sql: `SELECT * FROM (SELECT order_id, order_id AS oid FROM lat_item) s ` +
				`ORDER BY s.order_id, s.oid`,
			want: `order_id,oid | 1,1 | 1,1 | 2,2 | 2,2`},
		{name: "ctl/named-over-the-dup-block",
			sql: `SELECT s.order_id AS a, s.oid AS b, o.customer AS c FROM lat_ord o ` +
				`JOIN (SELECT order_id, order_id AS oid FROM lat_item) s ` +
				`ON s.order_id = o.id ORDER BY o.id, a`,
			want: `a,b,c | 1,1,Alice | 1,1,Alice | 2,2,Bob | 2,2,Bob`},
		{name: "ctl/named-over-the-rename-block",
			sql: `SELECT d.k AS kk, d.amount AS am FROM lat_ord o ` +
				`JOIN (SELECT order_id AS k, amount FROM lat_item) d ON d.k = o.id ORDER BY 1, 2`,
			want: `kk,am | 1,50 | 1,100 | 2,75 | 2,125`},
		{name: "ctl/named-over-the-agg-block",
			sql: `SELECT s.n AS nn FROM lat_ord o JOIN (SELECT order_id, ` +
				`CAST(COUNT(*) AS VARCHAR) AS n FROM lat_item GROUP BY order_id) s ` +
				`ON s.order_id = o.id ORDER BY 1`,
			want: `nn | 2 | 2`},
		// A SELF-JOIN's star, zero rows and non-empty, on four arms. Every
		// name is a duplicate, so every build column is qualified — and WHICH
		// side is the build is a COST decision, so the pair is one statement
		// under two predicates. PostgreSQL publishes the names by POSITION
		// (`id, order_id, product, amount` twice); this engine qualifies the
		// build side, on every arm and at every base — the star-column-naming
		// divergence ADR-0012 records, not something this arc moves.
		//
		// `981/ctl-a-self-join-still-qualifies` above is the same table joined
		// to itself under a DIFFERENT predicate and qualifies with `b.` where
		// these qualify with `a.`. Both are right, and together they are the
		// evidence for the sentence above: a pair that dropped the predicate
		// instead of changing it would be comparing two plans.
		{name: "selfjoin/star-with-rows",
			sql: `SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id ` +
				`WHERE a.id < 100 ORDER BY a.id`,
			want: `id,order_id,product,amount,a.id,a.order_id,a.product,a.amount | ` +
				`1,1,Widget,50,1,1,Widget,50 | 2,1,Gadget,100,2,1,Gadget,100 | ` +
				`3,2,Widget,75,3,2,Widget,75 | 4,2,Doohickey,125,4,2,Doohickey,125`},
		{name: "selfjoin/star-with-no-rows-declares-the-same",
			sql: `SELECT * FROM lat_item a JOIN lat_item b ON a.id = b.id ` +
				`WHERE a.id < 0 ORDER BY a.id`,
			want: `id,order_id,product,amount,a.id,a.order_id,a.product,a.amount`},

		{name: "ctl/a-plain-join-star",
			sql: `SELECT * FROM lat_ord o JOIN lat_item i ON i.order_id = o.id ` +
				`ORDER BY o.id, i.id`,
			want: `id,order_id,product,amount,o.id,customer,total | ` +
				`1,1,Widget,50,1,Alice,150 | 2,1,Gadget,100,1,Alice,150 | ` +
				`3,2,Widget,75,2,Bob,200 | 4,2,Doohickey,125,2,Bob,200`},

		// #980 — A LATERAL'S DEFAULTED COLUMN IS PART OF THE STAGE'S ONE
		// COLUMN SET. Each of these answered on the single arms and, at
		// v0.18.60, on the DAG arms only by ROUTING off it; with the route
		// gone they are the shape ADR-0010 refused (`one stage's files
		// describe one relation`) until the lateral join stage carried the
		// defaulted column in the relation it declares. Carol has no items, so
		// every cell exercises the empty-input default.
		{name: "980/count-plus-one",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT COUNT(*) + 1 AS n ` +
				`FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,n | 1,Alice,150,3 | 2,Bob,200,3 | 3,Carol,0,1`},
		{name: "980/coalesce-sum",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT COALESCE(SUM(amount), 0) ` +
				`AS n FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,n | 1,Alice,150,150 | 2,Bob,200,200 | 3,Carol,0,0`},
		// A BOOLEAN item, and it is not a spelling: parquet.TypeBool is the
		// ZERO TypeID, so a declaration carried as "type, and non-zero means
		// known" loses it — the fragment then guesses STRING for the column
		// while the empty side of the same join declares BOOL, and ADR-0010
		// refuses the pair. The known-ness travels beside the type for that.
		{name: "980/count-equals-zero-is-a-bool",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT COUNT(*) = 0 AS n ` +
				`FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,n | 1,Alice,150,false | 2,Bob,200,false | 3,Carol,0,true`},
		{name: "980/string-default",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT CAST(COUNT(*) AS VARCHAR) ` +
				`AS n FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,n | 1,Alice,150,2 | 2,Bob,200,2 | 3,Carol,0,0`},
		{name: "980/ctl-bare-count-is-its-stream",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT COUNT(*) AS n ` +
				`FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,n | 1,Alice,150,2 | 2,Bob,200,2 | 3,Carol,0,0`},

		// #981 — THE WIRE'S NAMES ARE THE PLAN'S NAMES. Two laterals over ONE
		// table made `markCoPathingSelfJoinBuilds` read the table through the
		// aggregates and mark both joins as a co-pathing SELF-JOIN, so every
		// build column was force-qualified and the client was handed `s.mx`,
		// `s2.mn` where PostgreSQL and the single-process path publish `mx`,
		// `mn`. An aggregate's output is not its table's columns, so the walk
		// stops there and the two builds are not a repeated scan.
		{name: "981/nested-laterals-publish-bare-names",
			sql: `SELECT * FROM lat_ord o JOIN LATERAL (SELECT MAX(amount) AS mx ` +
				`FROM lat_item WHERE order_id = o.id) s ON true JOIN LATERAL (` +
				`SELECT MIN(amount) AS mn FROM lat_item WHERE amount >= s.mx) s2 ` +
				`ON true ORDER BY o.id`,
			want: `id,customer,total,mx,mn | 1,Alice,150,100,NULL | 2,Bob,200,125,NULL | ` +
				`3,Carol,0,NULL,NULL`},
		{name: "981/nested-laterals-two-inner-items",
			sql: `SELECT * FROM lat_ord o JOIN LATERAL (SELECT MAX(amount) AS mx ` +
				`FROM lat_item WHERE order_id = o.id) s ON true JOIN LATERAL (` +
				`SELECT MIN(amount) AS mn, MAX(id) AS zz FROM lat_item WHERE amount >= s.mx) s2 ` +
				`ON true ORDER BY o.id`,
			want: `id,customer,total,mx,mn,zz | 1,Alice,150,100,NULL,NULL | ` +
				`2,Bob,200,125,NULL,NULL | 3,Carol,0,NULL,NULL,NULL`},
		// THE CONTROL FOR THE MARKING: a real self-join over one table still
		// qualifies, which is what the pass was written for (Q07's shape).
		{name: "981/ctl-a-self-join-still-qualifies",
			sql: `SELECT * FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id ` +
				`AND b.id > a.id ORDER BY a.id, b.id`,
			want: `id,order_id,product,amount,b.id,b.order_id,b.product,b.amount | ` +
				`1,1,Widget,50,2,1,Gadget,100 | 3,2,Widget,75,4,2,Doohickey,125`},
		// ------------------------------------------------------------------
		// A COMPUTED ITEM OVER A PRODUCER THAT MATERIALIZES IT IS ALREADY ON
		// THE STREAM (round 3). absorbComputedSubqueryProjection projects a
		// computed alias INTO the producing fragment for a scan, a window and
		// a join, so these columns were right at bb8635a4 and calling them
		// INTRODUCED took them off the DAG the moment this pass could not type
		// them. Each EXECUTES, counter +0, values identical at base.
		{name: "computed/scalar-subquery-item",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, ` +
				`(SELECT MAX(amount) FROM lat_item) AS sq FROM lat_item) s ` +
				`ON s.order_id = o.id ORDER BY o.id, sq`,
			want: `order_id,sq,id,customer,total | 1,125,1,Alice,150 | 1,125,1,Alice,150 | ` +
				`2,125,2,Bob,200 | 2,125,2,Bob,200`},
		{name: "computed/scalar-subquery-over-a-CTE",
			sql: `WITH q AS (SELECT MAX(amount) AS m FROM lat_item) SELECT * FROM lat_ord o ` +
				`JOIN (SELECT order_id, (SELECT m FROM q) AS sq FROM lat_item) s ` +
				`ON s.order_id = o.id ORDER BY o.id, sq`,
			want: `order_id,sq,id,customer,total | 1,125,1,Alice,150 | 1,125,1,Alice,150 | ` +
				`2,125,2,Bob,200 | 2,125,2,Bob,200`},
		{name: "computed/an-all-NULL-CASE",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, ` +
				`CASE WHEN order_id = 1 THEN NULL ELSE NULL END AS c FROM lat_item) s ` +
				`ON s.order_id = o.id ORDER BY o.id`,
			want: `order_id,c,id,customer,total | 1,NULL,1,Alice,150 | 1,NULL,1,Alice,150 | ` +
				`2,NULL,2,Bob,200 | 2,NULL,2,Bob,200`},
		{name: "computed/a-CASE-with-one-typed-arm",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, ` +
				`CASE WHEN order_id = 1 THEN 7 ELSE NULL END AS c FROM lat_item) s ` +
				`ON s.order_id = o.id ORDER BY o.id`,
			want: `order_id,c,id,customer,total | 1,7,1,Alice,150 | 1,7,1,Alice,150 | ` +
				`2,NULL,2,Bob,200 | 2,NULL,2,Bob,200`},
		{name: "computed/COALESCE-of-two-NULLs",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, COALESCE(NULL, NULL) AS c ` +
				`FROM lat_item) s ON s.order_id = o.id ORDER BY o.id`,
			want: `order_id,c,id,customer,total | 1,NULL,1,Alice,150 | 1,NULL,1,Alice,150 | ` +
				`2,NULL,2,Bob,200 | 2,NULL,2,Bob,200`},
		// The container cases. At bb8635a4 the DAG published the container's
		// SOURCE column beside it — five columns for PostgreSQL's four — and
		// the FILTERED spelling did too, because the predicate keeps the
		// source alive through pruning. Publishing the block's projection ends
		// both: the stage emits exactly what the block wrote.
		{name: "computed/a-container-over-a-plain-column",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, ARRAY[amount] AS a ` +
				`FROM lat_item) s ON s.order_id = o.id ORDER BY o.id`,
			want: `order_id,a,id,customer,total | 1,[50],1,Alice,150 | ` +
				`1,[100],1,Alice,150 | 2,[75],2,Bob,200 | 2,[125],2,Bob,200`},
		{name: "computed/a-two-element-container",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, ARRAY[amount, amount * 2] ` +
				`AS a FROM lat_item) s ON s.order_id = o.id ORDER BY o.id`,
			want: `order_id,a,id,customer,total | 1,[50 100],1,Alice,150 | ` +
				`1,[100 200],1,Alice,150 | 2,[75 150],2,Bob,200 | 2,[125 250],2,Bob,200`},
		{name: "computed/a-container-over-a-FILTERED-scan",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, ARRAY[amount] AS a ` +
				`FROM lat_item WHERE amount > 60) s ON s.order_id = o.id ORDER BY o.id, a`,
			want: `id,customer,total,order_id,a | 1,Alice,150,1,[100] | ` +
				`2,Bob,200,2,[125] | 2,Bob,200,2,[75]`},
		// A CONTAINER INSIDE A LATERAL, both join kinds. At bb8635a4 these
		// were ROUTED; at round 3 they read the lateral's stream and published
		// the correlation columns (dag) or failed under ADR-0010 (dagshuf).
		{name: "computed/a-container-inside-a-LEFT-lateral",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT ARRAY[amount] AS a ` +
				`FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id, a`,
			want: `id,customer,total,a | 1,Alice,150,[100] | 1,Alice,150,[50] | ` +
				`2,Bob,200,[125] | 2,Bob,200,[75] | 3,Carol,0,NULL`},
		{name: "computed/a-container-inside-an-INNER-lateral",
			sql: `SELECT * FROM lat_ord o JOIN LATERAL (SELECT ARRAY[amount] AS a ` +
				`FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id, a`,
			want: `a,id,customer,total | [100],1,Alice,150 | [50],1,Alice,150 | ` +
				`[125],2,Bob,200 | [75],2,Bob,200`},
		{name: "computed/an-all-NULL-CASE-inside-a-lateral",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT ` +
				`CASE WHEN amount > 60 THEN NULL ELSE NULL END AS c FROM lat_item ` +
				`WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,c | 1,Alice,150,NULL | 1,Alice,150,NULL | ` +
				`2,Bob,200,NULL | 2,Bob,200,NULL | 3,Carol,0,NULL`},
		// A SET-OP ARM is not a relation a star reads: the operation names its
		// arms. Marking one published its list onto the arm's own stage and
		// the union above then read columns that were no longer there —
		// `[<nil>]` for `[50]`, measured, in round 4's first cut.
		{name: "computed/a-container-in-a-set-op-arm",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, ARRAY[amount] AS a ` +
				`FROM lat_item UNION ALL SELECT order_id, ARRAY[amount] FROM lat_item ` +
				`WHERE amount > 1000) s ON s.order_id = o.id ORDER BY o.id, a`,
			want: `order_id,a,id,customer,total | 1,[100],1,Alice,150 | ` +
				`1,[50],1,Alice,150 | 2,[125],2,Bob,200 | 2,[75],2,Bob,200`},

		// THE SAME RULE where the block ALSO introduces a column, and it is
		// the sort key that decides, not the introduction: `ORDER BY id` keys
		// on a column this block does not publish, so it is reachability-
		// refused like the two above. The improvement is what it answers —
		// at bb8635a4 this lost `a2` silently on both DAG arms and the
		// `order_id AS oid` spelling failed LOUDLY (`sort: key column "oid"
		// does not exist`). The DAG now answers exactly what the
		// single-process arms answer, `__sortkey_0` included.
		{name: "sortkey/introducing-block-keeps-its-column",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, amount, amount AS a2 ` +
				`FROM lat_item ORDER BY id LIMIT 4) s ON s.order_id = o.id ` +
				`ORDER BY o.id, s.amount`,
			want: `order_id,amount,a2,__sortkey_0,id,customer,total | ` +
				`1,50,50,1,1,Alice,150 | 1,100,100,2,1,Alice,150 | ` +
				`2,75,75,3,2,Bob,200 | 2,125,125,4,2,Bob,200`,
			wantUnreachableRoute: true},

		// ------------------------------------------------------------------
		// THE BOUNDARY, SHAPE BY SHAPE (round 2). The route is NOT
		// answer-preserving — the coordinator-local pipeline's ORDER BY is
		// wrong for some shapes the DAG gets right — so it may take only what
		// was already WRONG or LOUD without it. Every `wantRouted: false` cell
		// below EXECUTES distributed; the three that route say why, each
		// measured at bb8635a4.
		//
		// A BLOCK WHOSE OWN `ORDER BY` KEYS ON A COLUMN IT DOES NOT PUBLISH is
		// refused by the SELECT-list reachability check (#656) and answered on
		// the coordinator-local pipeline — on BOTH DAG arms, with and without
		// a LIMIT, and whether or not the block also introduces a column. That
		// is the rule, and it is about the SORT KEY rather than about the
		// class: keying on a column the block DOES publish executes with both
		// counters at zero, the cell below.
		//
		// It publishes `__sortkey_0` on every arm — the sort below still reads
		// that key, so the projection cannot drop it. PostgreSQL sends five
		// columns; the sixth is this engine's own materialized ORDER BY term,
		// PRE-EXISTING on the single path, pinned in `want` rather than
		// exempted. bb8635a4's DAG published the scan's `amount` in its place.
		{name: "boundary/a-sort-key-the-block-does-not-publish",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item ` +
				`ORDER BY amount LIMIT 3) s ON s.order_id = o.id ORDER BY o.id, s.product`,
			want: `id,customer,total,order_id,product,__sortkey_0 | 1,Alice,150,1,Gadget,100 | ` +
				`1,Alice,150,1,Widget,50 | 2,Bob,200,2,Widget,75`,
			wantUnreachableRoute: true},
		// THE CONTROL, and the half that makes the rule a rule: the same block
		// keyed on a column it DOES publish executes distributed, both
		// counters at zero.
		{name: "boundary/a-sort-key-the-block-publishes",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, product FROM lat_item ` +
				`ORDER BY product LIMIT 3) s ON s.order_id = o.id ORDER BY o.id, s.product`,
			want: `id,customer,total,order_id,product | 1,Alice,150,1,Gadget | ` +
				`1,Alice,150,1,Widget | 2,Bob,200,2,Doohickey`},
		// EXECUTED and PostgreSQL's exact order at bb8635a4, and the local
		// pipeline got this ORDER BY wrong — so routing it turned a right
		// answer into a wrong one (round-1 B1). K3 PINNED the single arms'
		// order in `wantDAG` rather than describing it; #989 is that pin's
		// mechanism and arc L1 removed it, so all four arms now answer
		// PostgreSQL's order and the pin is DELETED as the fix's proof.
		{name: "boundary/a-twice-referenced-CTE-executes",
			sql: `WITH q AS (SELECT order_id, amount FROM lat_item) SELECT * FROM q a ` +
				`JOIN q b ON b.order_id = a.order_id ORDER BY a.order_id, a.amount, b.amount`,
			want: `order_id,amount,b.order_id,b.amount | 1,50,1,50 | 1,50,1,100 | ` +
				`1,100,1,50 | 1,100,1,100 | 2,75,2,75 | 2,75,2,125 | 2,125,2,75 | 2,125,2,125`},
		// LOUD on both DAG arms at bb8635a4. A CTE body is planned ONCE, so
		// the second reference's Project nodes never reach the publish hook —
		// the verdict is carried to them where the subtree is deduped. Its
		// #989 pin is deleted here for the same reason as the cell above.
		{name: "boundary/a-twice-referenced-CTE-with-a-rename-executes",
			sql: `WITH q AS (SELECT order_id AS k, amount FROM lat_item) SELECT * FROM q a ` +
				`JOIN q b ON b.k = a.k ORDER BY a.k, a.amount, b.amount`,
			want: `k,amount,b.k,b.amount | 1,50,1,50 | 1,50,1,100 | 1,100,1,50 | ` +
				`1,100,1,100 | 2,75,2,75 | 2,75,2,125 | 2,125,2,75 | 2,125,2,125`},
		// SQL's `unknown` DECIDES: PostgreSQL declares a bare NULL select item
		// `text`. Leaving it undecided routed a query that executed correctly
		// at bb8635a4 (round-1 B2).
		{name: "boundary/a-bare-NULL-item-is-text-and-executes",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, NULL AS c FROM lat_item) s ` +
				`ON s.order_id = o.id ORDER BY o.id, s.order_id`,
			want: `order_id,c,id,customer,total | 1,NULL,1,Alice,150 | 1,NULL,1,Alice,150 | ` +
				`2,NULL,2,Bob,200 | 2,NULL,2,Bob,200`},
		// SILENT WRONG at bb8635a4 (`order_id,amount` — both aliases lost).
		// It EXECUTES distributed, which round 1's docs denied.
		{name: "boundary/one-name-published-twice-executes",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id AS k, amount AS k ` +
				`FROM lat_item) s ON s.k = o.id ORDER BY o.id`,
			want: `k,k,id,customer,total | 1,50,1,Alice,150 | 1,100,1,Alice,150 | ` +
				`2,75,2,Bob,200 | 2,125,2,Bob,200`},

		// AN EMPTY DERIVED BUILD IS SHAPED BY WHAT THE BLOCK PUBLISHES. A
		// LEFT join over a derived side that matches NOTHING has no batch to
		// take a schema from, so the plan's declaration is the answer — and
		// read from the scan below the block it padded the row with the
		// table's own columns: eight for PostgreSQL's five, on the
		// single-process arms, at bb8635a4 and until round 2 (round-1 P2).
		{name: "boundary/an-empty-derived-build-declares-the-block",
			sql: `SELECT * FROM lat_ord o LEFT JOIN (SELECT order_id, id * 1.5 AS h ` +
				`FROM lat_item WHERE amount > 1000) s ON s.order_id = o.id ORDER BY o.id`,
			want: `id,customer,total,order_id,h | 1,Alice,150,NULL,NULL | ` +
				`2,Bob,200,NULL,NULL | 3,Carol,0,NULL,NULL`},

		// THE RESIDUE — TWO shapes, each ROUTED, each measured wrong or loud
		// at bb8635a4 without the route. This list is the one ADR-0026 §7 and
		// docs/sql-reference.md state, and it is complete.
		//
		// ROUTED at bb8635a4 and until round 4, when the type stopped being a
		// disposition: the block's projection is typed by the SAME inference
		// the single path uses, so a container over an aggregate is published
		// like any other item and the query EXECUTES.
		{name: "computed/a-container-over-an-aggregate",
			sql: `SELECT * FROM lat_ord o LEFT JOIN LATERAL (SELECT ARRAY[COUNT(*)] AS a ` +
				`FROM lat_item WHERE order_id = o.id) s ON true ORDER BY o.id`,
			want: `id,customer,total,a | 1,Alice,150,[2] | 2,Bob,200,[2] | 3,Carol,0,[0]`},
		// SILENT WRONG at bb8635a4: the DAG published `__agg_1` beside `sa`
		// and `sb`. An aggregate SELECT item is computed by the aggregate
		// stage, not by a projection above it.
		{name: "residue/a-bare-aggregate-alias-beside-a-computed-sibling",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, SUM(amount) AS sa, ` +
				`SUM(amount) * 1 AS sb FROM lat_item GROUP BY order_id) s ` +
				`ON s.order_id = o.id ORDER BY o.id`,
			want: `id,customer,total,order_id,sa,sb | 1,Alice,150,1,150,150 | ` +
				`2,Bob,200,2,200,200`,
			wantDAG: `order_id,sa,sb,id,customer,total | 1,150,150,1,Alice,150 | ` +
				`2,200,200,2,Bob,200`,
			wantRouted: true},
		// LOUD on both DAG arms at bb8635a4.
		{name: "residue/a-window-inside-the-block",
			sql: `SELECT * FROM lat_ord o JOIN (SELECT order_id, SUM(amount) OVER () AS w ` +
				`FROM lat_item) s ON s.order_id = o.id ORDER BY o.id, w`,
			want: `order_id,w,id,customer,total | 1,350,1,Alice,150 | 1,350,1,Alice,150 | ` +
				`2,350,2,Bob,200 | 2,350,2,Bob,200`,
			wantRouted: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				var routesBefore, uoBefore int64
				if arm.coord != nil {
					routesBefore = arm.coord.LateralProjectionLocalRoutes()
					uoBefore = arm.coord.UnreachableOutputLocalRoutes()
				}
				cols, rows, err := arm.run(tc.sql)
				if arm.coord != nil {
					routed := arm.coord.LateralProjectionLocalRoutes() > routesBefore
					if routed != tc.wantRouted {
						t.Fatalf("%s arm routed=%v, want %v — a block whose projection a "+
							"stage carries EXECUTES; only one the plan cannot state is "+
							"handed over, and only where it was wrong or loud before\n  SQL: %s",
							arm.name, routed, tc.wantRouted, tc.sql)
					}
					uo := arm.coord.UnreachableOutputLocalRoutes() > uoBefore
					if uo != tc.wantUnreachableRoute {
						t.Fatalf("%s arm reachability-routed=%v, want %v — a block whose "+
							"own ORDER BY keys on a column it does NOT publish is refused "+
							"by the SELECT-list reachability check (#656)\n  SQL: %s",
							arm.name, uo, tc.wantUnreachableRoute, tc.sql)
					}
				}
				if err != nil {
					t.Fatalf("%s arm: %v\n  want %s\n  SQL: %s", arm.name, err, tc.want, tc.sql)
				}
				got := e3Render(cols, rows)
				want := tc.want
				if arm.coord != nil && tc.wantDAG != "" {
					want = tc.wantDAG
				}
				if got != want {
					t.Fatalf("%s arm: %s\n  want %s (PostgreSQL 17's column set and values)\n  SQL: %s",
						arm.name, got, want, tc.sql)
				}
				// Nothing the planner minted for itself reaches a client —
				// unless the cell's own expectation names it, which is how a
				// PRE-EXISTING leak this arc does not touch is PINNED rather
				// than exempted: the day it stops leaking, the rendering
				// changes and the cell fails.
				for _, c := range cols {
					if strings.HasPrefix(strings.ToLower(c), "__") &&
						!strings.Contains(want, c) {
						t.Fatalf("%s arm published the reserved slot %q to the client\n  SQL: %s",
							arm.name, c, tc.sql)
					}
				}
			}
		})
	}
}
