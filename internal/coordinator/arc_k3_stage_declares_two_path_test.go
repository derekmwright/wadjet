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

	for _, tc := range []struct{ name, sql, want, wantDAG string }{
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
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				cols, rows, err := arm.run(tc.sql)
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
				for _, c := range cols {
					if strings.HasPrefix(strings.ToLower(c), "__") {
						t.Fatalf("%s arm published the reserved slot %q to the client\n  SQL: %s",
							arm.name, c, tc.sql)
					}
				}
			}
		})
	}
}
