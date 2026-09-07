package coordinator

import (
	"context"
	"testing"
	"time"
)

// A DUPLICATE OUTPUT NAME BINDS BY POSITION IN ORDER BY — #905, on FOUR arms.
//
// `WITH cte AS (SELECT id, a FROM decpair) SELECT a.id, b.id FROM cte a JOIN
// cte b ON a.a = b.a ORDER BY a.id, b.id` publishes two output columns called
// `id`. The single-process pipeline builds each sort key as
// `cleanExpr(ob.Column)`, which STRIPS the table qualifier, so both keys became
// `id` and columnIndexFallback bound both of them to the FIRST one: the second
// key was never applied and rows sharing an equal first key came back in an
// arbitrary order. PostgreSQL 17 answers `1,1 | 1,2 | 1,3 | 1,8 | 2,1 | …` and
// the single path answered `1,8 | 1,3 | 1,2 | 1,1 | 2,8 | …` — the right rows
// in the wrong SEQUENCE, which no multiset comparison can see.
//
// The stage DAG is right on this shape at the same commit, so the arms are the
// evidence in both directions: this is a single-process defect, and the fix
// (sortKeyLocalSlotPos) is wired into the single path's two sort builders only.
//
// The controls carry the boundary. `ORDER BY 1, 2` was already position-bound
// (#557) and must not move; an ALIASED spelling resolves by a name that IS
// unique and must not move; a key naming a column the SELECT list does not
// carry, an EXPRESSION key, and the SOURCE spelling of an aliased item all
// resolve outside the select list and must not move. The three shapes
// PostgreSQL REFUSES as ambiguous (`ORDER BY id` over two `id` outputs, an
// alias shadowing another column, two items sharing an alias) are answered
// here rather than refused — a superset divergence recorded in ADR-0012 — and
// they are asserted so the fix cannot change WHICH column they answer for.
func TestH2ADuplicateOutputNameOrdersByPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	const cte = "WITH cte AS (SELECT id, a FROM decpair) "
	const asc = "cols=[id:INT64 id:INT64] rows=19 | 1,1 | 1,2 | 1,3 | 1,8 | 2,1 | 2,2 | 2,3 | " +
		"2,8 | 3,1 | 3,2 | 3,3 | 3,8 | 4,4 | 5,5 | 6,6 | 8,1 | 8,2 | 8,3 | 8,8"

	f1Run(t, arms, []f1Case{
		{
			// #905's exact shape. The join condition is on `a`, NOT on `id`,
			// which is what makes the second key discriminate: over `a.id =
			// b.id` the two keys are redundant and every arm agrees whatever
			// they bind to.
			name: "905 a CTE self-join with two outputs named id",
			sql:  cte + "SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a ORDER BY a.id, b.id",
			want: asc,
		},
		{
			// The same with the second key DESC, so a run that ignores it
			// cannot pass by accident in either direction.
			name: "905 the same with the second key DESC",
			sql:  cte + "SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a ORDER BY a.id, b.id DESC",
			want: "cols=[id:INT64 id:INT64] rows=19 | 1,8 | 1,3 | 1,2 | 1,1 | 2,8 | 2,3 | 2,2 | " +
				"2,1 | 3,8 | 3,3 | 3,2 | 3,1 | 4,4 | 5,5 | 6,6 | 8,8 | 8,3 | 8,2 | 8,1",
		},
		{
			// The TOP-N builder is a second sort site and had the same key
			// construction: `ORDER BY … LIMIT` answered 1,8 | 1,2 | 1,1 | 1,3
			// — not even the first key's ties in one order.
			name: "905 the same under a LIMIT (the top-N builder)",
			sql:  cte + "SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a ORDER BY a.id, b.id LIMIT 6",
			want: "cols=[id:INT64 id:INT64] rows=6 | 1,1 | 1,2 | 1,3 | 1,8 | 2,1 | 2,2",
		},
		{
			name: "905 control: the ORDINAL spelling was already position-bound (#557)",
			sql:  cte + "SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a ORDER BY 1, 2",
			want: asc,
		},
		{
			name: "905 control: the ALIASED spelling resolves by a unique name",
			sql:  cte + "SELECT a.id AS aw, b.id AS bw FROM cte a JOIN cte b ON a.a = b.a ORDER BY aw, bw",
			want: "cols=[aw:INT64 bw:INT64] rows=19 | 1,1 | 1,2 | 1,3 | 1,8 | 2,1 | 2,2 | 2,3 | " +
				"2,8 | 3,1 | 3,2 | 3,3 | 3,8 | 4,4 | 5,5 | 6,6 | 8,1 | 8,2 | 8,3 | 8,8",
		},
		{
			name: "905 control: a key naming a column the SELECT list does not carry",
			sql:  "SELECT a FROM decpair ORDER BY id DESC",
			want: "cols=[a:DECIMAL(9,2)] rows=9 | NULL | 12.75 | NULL | 0.00 | 2.00 | -0.01 | " +
				"12.75 | 12.75 | 12.75",
		},
		{
			name: "905 control: an EXPRESSION key",
			sql:  "SELECT id*2, a FROM decpair ORDER BY id*2 DESC LIMIT 4",
			want: "cols=[?column?:INT64 a:DECIMAL(9,2)] rows=4 | 18,NULL | 16,12.75 | 14,NULL | 12,0.00",
		},
		{
			name: "905 control: the SOURCE spelling of an aliased item",
			sql:  "SELECT id AS k, a FROM decpair ORDER BY id DESC LIMIT 4",
			want: "cols=[k:INT64 a:DECIMAL(9,2)] rows=4 | 9,NULL | 8,12.75 | 7,NULL | 6,0.00",
		},
		{
			name: "905 control: a plain single-relation ORDER BY",
			sql:  "SELECT id, a FROM decpair ORDER BY a, id LIMIT 5",
			want: "cols=[id:INT64 a:DECIMAL(9,2)] rows=5 | 4,-0.01 | 6,0.00 | 5,2.00 | 1,12.75 | 2,12.75",
		},
		{
			// PostgreSQL 17: `ERROR: ORDER BY "id" is ambiguous`. wadjet
			// answers it — a PG-rejects-but-we-answer superset (ADR-0012) —
			// and WHICH column it answers for must not move: the first `id`.
			name: "905 superset: ORDER BY a bare duplicate output name",
			sql:  cte + "SELECT a.id, b.id FROM cte a JOIN cte b ON a.a = b.a ORDER BY id",
			want: "cols=[id:INT64 id:INT64] rows=19 | 1,8 | 1,3 | 1,2 | 1,1 | 2,8 | 2,3 | 2,2 | " +
				"2,1 | 3,8 | 3,3 | 3,2 | 3,1 | 4,4 | 5,5 | 6,6 | 8,8 | 8,3 | 8,2 | 8,1",
		},
		{
			// PostgreSQL 17: `ERROR: ORDER BY "id" is ambiguous` — the alias
			// `id` on the first item and the real column `id` on the second.
			name: "905 superset: an alias that shadows another output's column",
			sql:  "SELECT a AS id, id FROM decpair ORDER BY id LIMIT 4",
			want: "cols=[id:DECIMAL(9,2) id:INT64] rows=4 | -0.01,4 | 0.00,6 | 2.00,5 | 12.75,1",
		},
		{
			// PostgreSQL 17: `ERROR: ORDER BY "k" is ambiguous`. Two items
			// answer to one ALIAS, so the position is declined and the name
			// fallback stands.
			name: "905 superset: two items sharing one alias",
			sql:  "SELECT id AS k, a AS k FROM decpair ORDER BY k LIMIT 4",
			want: "cols=[k:INT64 k:DECIMAL(9,2)] rows=4 | 1,12.75 | 2,12.75 | 3,12.75 | 4,-0.01",
		},
		{
			// The ClickBench spelling #905 was found in, and the DAG's half of
			// the same class: with the CTE's column itself an ALIAS
			// (`SELECT id AS WatchID`), BOTH select items resolve to the CTE's
			// one source column `id`, so the join fragment's projection read
			// it twice and the second output carried the first arm's value —
			// 1,1 | 1,1 | 1,1 | 1,1 on both DAG arms where PostgreSQL has
			// 1,1 | 1,2 | 1,3 | 1,8, silently, and right on both local arms.
			// qualifySharedRenameSource re-attaches each item's own qualifier
			// when another item resolves the same bare source under a
			// different one.
			name: "905 the CTE column is itself an alias (the DAG half)",
			sql: "WITH cte AS (SELECT id AS WatchID, a FROM decpair) " +
				"SELECT a.WatchID, b.WatchID FROM cte a JOIN cte b ON a.a = b.a " +
				"ORDER BY a.WatchID, b.WatchID",
			want: "cols=[watchid:INT64 watchid:INT64] rows=19 | 1,1 | 1,2 | 1,3 | 1,8 | 2,1 | " +
				"2,2 | 2,3 | 2,8 | 3,1 | 3,2 | 3,3 | 3,8 | 4,4 | 5,5 | 6,6 | 8,1 | 8,2 | 8,3 | 8,8",
		},
		{
			name: "905 the aliased CTE spelling with the second key DESC",
			sql: "WITH cte AS (SELECT id AS WatchID, a FROM decpair) " +
				"SELECT a.WatchID, b.WatchID FROM cte a JOIN cte b ON a.a = b.a " +
				"ORDER BY a.WatchID, b.WatchID DESC LIMIT 6",
			want: "cols=[watchid:INT64 watchid:INT64] rows=6 | 1,8 | 1,3 | 1,2 | 1,1 | 2,8 | 2,3",
		},
		{
			// THREE references of one CTE, so a repair that only ever
			// distinguishes two qualifiers cannot pass.
			name: "905 three references of the same aliased CTE",
			sql: "WITH cte AS (SELECT id AS WatchID, a FROM decpair) " +
				"SELECT a.WatchID, b.WatchID, c.WatchID FROM cte a JOIN cte b ON a.a = b.a " +
				"JOIN cte c ON a.a = c.a ORDER BY a.WatchID, b.WatchID, c.WatchID LIMIT 8",
			want: "cols=[watchid:INT64 watchid:INT64 watchid:INT64] rows=8 | 1,1,1 | 1,1,2 | " +
				"1,1,3 | 1,1,8 | 1,2,1 | 1,2,2 | 1,2,3 | 1,2,8",
		},
		{
			// The boundary from the other side: ONE qualified rename, so the
			// source is not contested and the spelling must not be touched.
			name: "905 control: a single qualified rename over a self-join",
			sql: "WITH cte AS (SELECT id AS WatchID, a FROM decpair) " +
				"SELECT a.WatchID FROM cte a JOIN cte b ON a.a = b.a ORDER BY a.WatchID LIMIT 6",
			want: "cols=[watchid:INT64] rows=6 | 1 | 1 | 1 | 1 | 2 | 2",
		},
		{
			// Two qualifiers resolving to DIFFERENT sources: nothing is
			// contested and neither spelling moves.
			name: "905 control: two qualifiers, different sources",
			sql: "WITH cte AS (SELECT id AS k, a AS m FROM decpair) " +
				"SELECT a.k, b.m FROM cte a JOIN cte b ON a.m = b.m ORDER BY a.k, b.m LIMIT 6",
			want: "cols=[k:INT64 m:DECIMAL(9,2)] rows=6 | 1,12.75 | 1,12.75 | 1,12.75 | " +
				"1,12.75 | 2,12.75 | 2,12.75",
		},
	})
}
