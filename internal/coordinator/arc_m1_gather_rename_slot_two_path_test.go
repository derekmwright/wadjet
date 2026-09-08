package coordinator

import (
	"context"
	"testing"
	"time"
)

// A GATHER RENAME BINDS THE COLUMN ITS SOURCE NAMES — four arms, every answer
// measured on live postgres:17-alpine.
//
// A SELECT list of plain group-key references emits no stage on the DAG: the
// aggregate materializes and the gather's `OutputRename{From: "a.amount", To:
// "amount"}` maps each item back onto what that stage publishes. An aggregate
// publishes its keys under BOTH spellings — `exec.PublishedGroupKeyNames`
// strips a key's qualifier and reverts a colliding one, and the stream also
// carries the join's own names — so the gather saw
// `[order_id amount amount a.order_id a.amount b.amount]`.
//
// `classScopedMatch` rescanned for the BARE name whenever the exact spelling
// matched fewer than TWO columns, which is every uniquely-resolving qualified
// reference. `a.amount` and `b.amount` each matched exactly ONE column and
// both were re-bound to the FIRST bare `amount`, so
//
//	SELECT a.order_id, a.amount, b.amount FROM lat_item a
//	JOIN lat_item b ON b.order_id = a.order_id
//	GROUP BY a.order_id, a.amount, b.amount ORDER BY a.order_id, a.amount, b.amount
//
// returned eight rows whose THIRD column carried the SECOND's value on both
// DAG arms — right groups, wrong values — where PostgreSQL 17 and the two
// single-process arms answer eight distinct triples. The rescan runs only when
// the exact spelling matched NOTHING now, which is `exec.ColumnIndexFallback`'s
// order and the rule the rest of the engine binds by.
//
// THE BOUNDARY IS A CLAIM: the rescan exists for #785, where an item spelled
// through a derived table's alias (`u.g`) matches no column exactly and the
// stream carries the bare `g` twice — the aggregate's own output and its group
// key — and the CLASS decides. That case still has zero exact matches, so it
// still takes the rescan; `TestArcE3` holds it and is unmoved. The controls
// below hold the other side: an ALIASED select list is materialized as a
// projection and never reaches this resolver, and a plain non-grouped
// projection has no aggregate stage to rename over.
func TestM1AGatherRenameBindsWhatItsSourceNames(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const selfJoin = "FROM lat_item a JOIN lat_item b ON b.order_id = a.order_id "
	const eight = "cols=[order_id:INT64 amount:FLOAT64 amount:FLOAT64] rows=8 | " +
		"1,50,50 | 1,50,100 | 1,100,50 | 1,100,100 | " +
		"2,75,75 | 2,75,125 | 2,125,75 | 2,125,125"

	f1Run(t, arms, []f1Case{
		{
			// The headline shape: a GROUP BY over a self-join whose two
			// references publish one bare name.
			name: "a GROUP BY over a self-join binds each reference's own column",
			sql: "SELECT a.order_id, a.amount, b.amount " + selfJoin +
				"GROUP BY a.order_id, a.amount, b.amount " +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: eight,
		},
		{
			// The DISTINCT spelling, which `rewriteDistinctAsGroupBy` lowers
			// to exactly the tree above — so it reaches the same resolver by a
			// different door.
			name: "the DISTINCT spelling of the same query",
			sql: "SELECT DISTINCT a.order_id, a.amount, b.amount " + selfJoin +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: eight,
		},
		{
			// An AGGREGATE beside the keys, so the stage has `AggSpecs` and
			// the class rule has something to be about. The wrong binding was
			// the same here.
			name: "the same with an aggregate output beside the keys",
			sql: "SELECT a.order_id, a.amount, b.amount, COUNT(*) AS n " + selfJoin +
				"GROUP BY a.order_id, a.amount, b.amount " +
				"ORDER BY a.order_id, a.amount, b.amount",
			want: "cols=[order_id:INT64 amount:FLOAT64 amount:FLOAT64 n:INT64] rows=8 | " +
				"1,50,50,1 | 1,50,100,1 | 1,100,50,1 | 1,100,100,1 | " +
				"2,75,75,1 | 2,75,125,1 | 2,125,75,1 | 2,125,125,1",
		},
		{
			// THREE references to one table, so the middle key is neither the
			// first nor the last column of its name and a fix that binds "the
			// second bare match" instead of the reference's own column cannot
			// pass.
			name: "three references, three columns of one bare name",
			sql: "SELECT a.amount, b.amount, c.amount FROM lat_item a " +
				"JOIN lat_item b ON b.order_id=a.order_id " +
				"JOIN lat_item c ON c.order_id=a.order_id WHERE a.order_id=1 " +
				"GROUP BY a.amount, b.amount, c.amount ORDER BY a.amount, b.amount, c.amount",
			want: "cols=[amount:FLOAT64 amount:FLOAT64 amount:FLOAT64] rows=8 | " +
				"50,50,50 | 50,50,100 | 50,100,50 | 50,100,100 | " +
				"100,50,50 | 100,50,100 | 100,100,50 | 100,100,100",
		},
		{
			// TWO DIFFERENT tables sharing one column name — not a self-join,
			// so the shape is not a property of reading one table twice.
			name: "two different tables sharing one column name",
			sql: "SELECT o.id, i.id FROM lat_ord o JOIN lat_item i ON i.order_id = o.id " +
				"GROUP BY o.id, i.id ORDER BY o.id, i.id",
			want: "cols=[id:INT64 id:INT64] rows=4 | 1,1 | 1,2 | 2,3 | 2,4",
		},
		{
			// CONTROL: the ALIASED select list. Aliases make the list a
			// materialized projection, which never reaches this resolver —
			// right at base on all four arms and unmoved.
			name: "control: an aliased select list is a projection",
			sql: "SELECT a.order_id AS oid, a.amount AS aa, b.amount AS ba " + selfJoin +
				"GROUP BY a.order_id, a.amount, b.amount ORDER BY oid, aa, ba",
			want: "cols=[oid:INT64 aa:FLOAT64 ba:FLOAT64] rows=8 | " +
				"1,50,50 | 1,50,100 | 1,100,50 | 1,100,100 | " +
				"2,75,75 | 2,75,125 | 2,125,75 | 2,125,125",
		},
		{
			// CONTROL: no GROUP BY at all. There is no aggregate stage to
			// rename over, so the join's own output reaches the client and was
			// right at base.
			name: "control: the same projection with no GROUP BY",
			sql:  "SELECT a.amount, b.amount " + selfJoin + "ORDER BY a.amount, b.amount",
			want: "cols=[amount:FLOAT64 amount:FLOAT64] rows=8 | " +
				"50,50 | 50,100 | 75,75 | 75,125 | 100,50 | 100,100 | 125,75 | 125,125",
		},
	})
}
