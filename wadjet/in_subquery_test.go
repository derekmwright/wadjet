package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestInSubquerySemiJoinAnswersEveryAliasSpelling is #516's value gate on the
// single-process path — the path the bug was on, and the one most interactive
// queries take.
//
// `SELECT COUNT(*) FROM t a WHERE a.x IN (SELECT b.x FROM t b WHERE …)`
// answered 0 for every wide type while the stage DAG answered correctly and
// PostgreSQL agreed with the DAG. The cause was in the IN → semi-join
// lowering, not in any type: the rewrite named the join's inner key `b.x`,
// which the inner plan's Scan emits as `x`, and the executor's key-repair
// heuristic then swapped a pair that was not misassigned (on a self-IN the
// bare name is present on BOTH sides, so the repair's premise does not hold).
//
// Every expectation below was read off PostgreSQL 17 over the same fixture.
func TestInSubquerySemiJoinAnswersEveryAliasSpelling(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	cases := []struct {
		name string
		sql  string
		want int64
	}{
		// ids 0..499 are the 500 rows the subquery yields.
		{"both_relations_aliased", // the #516 repro
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id FROM mbtypes b WHERE b.id < 500)`, 500},
		{"inner_select_item_bare",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT id FROM mbtypes b WHERE b.id < 500)`, 500},
		{"neither_relation_aliased",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes WHERE id < 500)`, 500},
		{"inner_select_item_qualified_by_table_name",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT mbtypes.id FROM mbtypes WHERE mbtypes.id < 500)`, 500},
		{"inner_select_item_aliased",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id AS bid FROM mbtypes b WHERE b.id < 500)`, 500},
		{"not_in_anti_join",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id NOT IN (SELECT b.id FROM mbtypes b WHERE b.id < 500)`, 4500},
		// g is 0,1,2 with no NULLs, so every row's g is in the subquery's set.
		{"group_by_inner",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.g IN (SELECT b.g FROM mbtypes b GROUP BY b.g)`, 5000},
		{"group_by_having_inner",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.g IN (SELECT b.g FROM mbtypes b GROUP BY b.g HAVING COUNT(*) > 1)`, 5000},
		// c_i64 is NULL every 31st row, so the subquery's set covers
		// 500 - ceil(500/31) = 484 of the 500 rows below the bound, and each
		// value is unique — 484 outer rows match.
		{"wide_typed_column",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.c_i64 IN (SELECT b.c_i64 FROM mbtypes b WHERE b.id < 500)`, 484},
		// Shapes the rewrite now DECLINES rather than lowering to a key it
		// cannot name — the IN stays a subquery predicate and is answered.
		// Both used to fail the physical planner outright ("cannot be
		// represented as an equi-join key").
		{"ungrouped_aggregate_inner",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT MAX(b.id) FROM mbtypes b)`, 1},
		{"computed_inner_select_item",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id + 0 FROM mbtypes b WHERE b.id < 500)`, 500},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query error: %v\n  SQL: %s", err, tc.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1 (scalar COUNT)\n  SQL: %s", len(res.Rows), tc.sql)
			}
			if got := res.Rows[0]["n"]; got != tc.want {
				t.Errorf("COUNT(*) = %v, want %d (PostgreSQL 17)\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}

// TestInSubqueryHonorsTheSubqueryLimit is #482's value gate.
//
// `WHERE x IN (SELECT … LIMIT n)` matched against the FULL unbounded result
// set for any n, so the predicate selected every row. The semi join the IN
// lowers to has nowhere to put the bound — its build side IS the relation the
// subquery reads — so a bounded subquery is not decorrelated at all and is
// executed as written instead.
//
// Every entry carries an ORDER BY inside the subquery, because a bare LIMIT
// does not say WHICH rows it yields (ADR-0013's legal-nondeterminism list);
// the two bare-LIMIT entries below assert only what is determined regardless.
// Expectations read off PostgreSQL 17.
func TestInSubqueryHonorsTheSubqueryLimit(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	cases := []struct {
		name string
		sql  string
		want int64
	}{
		{"ordered_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes ORDER BY id LIMIT 3)`, 3},
		{"ordered_limit_offset",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes ORDER BY id LIMIT 3 OFFSET 5)`, 3},
		{"ordered_limit_aliased",
			`SELECT COUNT(*) AS n FROM mbtypes a WHERE a.id IN (SELECT b.id FROM mbtypes b ORDER BY b.id LIMIT 3)`, 3},
		{"not_in_ordered_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id NOT IN (SELECT id FROM mbtypes ORDER BY id LIMIT 3)`, 4997},
		// LIMIT 0 is a bound, not an absence (#481) — the membership set is
		// empty and nothing matches.
		{"limit_zero",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes ORDER BY id LIMIT 0)`, 0},
		// Bare LIMIT: WHICH three ids is unspecified, but id is unique, so
		// exactly three outer rows match whichever three they are.
		{"bare_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id IN (SELECT id FROM mbtypes LIMIT 3)`, 3},
		{"bare_not_in_limit",
			`SELECT COUNT(*) AS n FROM mbtypes WHERE id NOT IN (SELECT id FROM mbtypes LIMIT 3)`, 4997},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query error: %v\n  SQL: %s", err, tc.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1 (scalar COUNT)\n  SQL: %s", len(res.Rows), tc.sql)
			}
			if got := res.Rows[0]["n"]; got != tc.want {
				t.Errorf("COUNT(*) = %v, want %d (PostgreSQL 17)\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}

// jiOpen builds the two-relation fixture the joined-inner shapes need: a
// subquery that JOINS, with the two relations carrying a column of the SAME
// bare name (`x`) so the join's output can only spell one of them bare.
//
//	tt(id, x) = (1,10) (2,20) (3,30) (4,40)
//	uu(k, x)  = (1,10) (2,99)
//
// `uu c JOIN tt b ON b.id = c.k` yields two rows — (c.x=10, b.x=10) and
// (c.x=99, b.x=20) — so `c.x` and `b.x` are DIFFERENT membership sets and a
// query that reads the wrong one is visible in the count. tt is deliberately
// the larger relation, because which side reorderJoins puts on the probe is
// decided by estimated rows.
func jiOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	load := func(name string, cols []parquet.Column, rows []map[string]any) {
		t.Helper()
		sch := parquet.Schema{Columns: cols}
		if err := db.CreateTable(ctx, name, sch, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		ing := db.NewIngester(name, sch, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 8})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", name, err)
		}
	}
	load("tt",
		[]parquet.Column{{Name: "id", Type: parquet.TypeInt64}, {Name: "x", Type: parquet.TypeInt64}},
		[]map[string]any{
			{"id": int64(1), "x": int64(10)},
			{"id": int64(2), "x": int64(20)},
			{"id": int64(3), "x": int64(30)},
			{"id": int64(4), "x": int64(40)},
		})
	load("uu",
		[]parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "x", Type: parquet.TypeInt64}},
		[]map[string]any{
			{"k": int64(1), "x": int64(10)},
			{"k": int64(2), "x": int64(99)},
		})
	return db
}

// TestInSubqueryOverAJoinedInnerAgreesWithPostgres holds the line #516's fix
// has to stop at.
//
// innerSemiJoinKey strips a qualifier that names the subquery's LEADING
// relation, on the premise that the relation written first becomes the inner
// plan's bottom Scan and its columns come out of every node above it bare.
// That premise holds for a single-relation subquery and is FALSE the moment
// the subquery joins: which relation's columns a join emits bare is decided
// by reorderJoins from estimated row counts (Optimize step 73), long after
// decorrelateInSubqueries has named the key (step 36). Stripping `c.x` to `x`
// in `uu c JOIN tt b` with a small `uu` then answers over tt.x — 2 rows where
// PostgreSQL says 1, silently. So the strip is scoped to a subquery with no
// JOIN in it, and a joined inner keeps the spelling the user wrote.
//
// The `knownBug` entries are two OLDER defects this fixture is the first to
// see, both of them the same premise applied one layer over: #526, where a
// qualified item over a joined inner names a column the build schema does not
// carry (exec.HashJoin's key repair then swaps the pair on the false premise
// #516 documents, and the join matches nothing), and #527, where a correlated
// EXISTS over a joined inner strips its correlation column the same way and
// correlates on the wrong relation. They RUN and they LOG; the gate FAILS if
// one starts agreeing, because that is the proof the fix landed and the pin
// should go (ADR-0013 §Pins).
//
// Every want below is a live postgres:17-alpine transcript over this fixture.
func TestInSubqueryOverAJoinedInnerAgreesWithPostgres(t *testing.T) {
	ctx := context.Background()
	db := jiOpen(t)

	cases := []struct {
		name     string
		sql      string
		want     int64
		knownBug string
	}{
		// The regression this test exists for: lead-qualified item, small
		// lead relation, so reorderJoins moves the OTHER relation onto the
		// probe and a stripped key reads the wrong column.
		{name: "lead_qualified_small_lead",
			sql:  `SELECT COUNT(*) AS n FROM tt a WHERE a.x IN (SELECT c.x FROM uu c JOIN tt b ON b.id = c.k)`,
			want: 1},
		{name: "not_in_lead_qualified_small_lead",
			sql:  `SELECT COUNT(*) AS n FROM tt a WHERE a.x NOT IN (SELECT c.x FROM uu c JOIN tt b ON b.id = c.k)`,
			want: 3},
		// The same membership set reached with the relations written the
		// other way round, so the item qualifies the NON-leading relation.
		{name: "nonlead_qualified_big_lead",
			sql:  `SELECT COUNT(*) AS n FROM tt a WHERE a.x IN (SELECT c.x FROM tt b JOIN uu c ON b.id = c.k)`,
			want: 1},
		// #527: a correlated EXISTS over a joined inner takes
		// decorrelateExists + stripTableQualifiers, not this rewrite — and it
		// strips `c.x` to `x` on the same premise, so it correlates on b.x.
		// Both directions answer 2 of 4, which is what a wrong KEY looks
		// like: the semi join and the anti join agree with each other and
		// disagree with PostgreSQL.
		{name: "exists_over_joined_inner",
			sql:      `SELECT COUNT(*) AS n FROM tt a WHERE EXISTS (SELECT 1 FROM uu c JOIN tt b ON b.id = c.k WHERE c.x = a.x)`,
			want:     1,
			knownBug: "#527"},
		{name: "not_exists_over_joined_inner",
			sql:      `SELECT COUNT(*) AS n FROM tt a WHERE NOT EXISTS (SELECT 1 FROM uu c JOIN tt b ON b.id = c.k WHERE c.x = a.x)`,
			want:     3,
			knownBug: "#527"},
		// #526: the other relation's column. Wrong before #516, wrong after
		// it, wrong here — and wrong in BOTH directions of write order, which
		// is why it is not "the non-leading relation" but "the spelling the
		// join's output does not carry".
		{name: "nonlead_qualified_small_lead",
			sql:      `SELECT COUNT(*) AS n FROM tt a WHERE a.x IN (SELECT b.x FROM uu c JOIN tt b ON b.id = c.k)`,
			want:     2,
			knownBug: "#526"},
		{name: "lead_qualified_big_lead",
			sql:      `SELECT COUNT(*) AS n FROM tt a WHERE a.x IN (SELECT b.x FROM tt b JOIN uu c ON b.id = c.k)`,
			want:     2,
			knownBug: "#526"},
		{name: "not_in_nonlead_qualified_small_lead",
			sql:      `SELECT COUNT(*) AS n FROM tt a WHERE a.x NOT IN (SELECT b.x FROM uu c JOIN tt b ON b.id = c.k)`,
			want:     2,
			knownBug: "#526"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				if tc.knownBug != "" {
					t.Logf("PINNED %s: query error: %v\n  SQL: %s", tc.knownBug, err, tc.sql)
					return
				}
				t.Fatalf("query error: %v\n  SQL: %s", err, tc.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1 (scalar COUNT)\n  SQL: %s", len(res.Rows), tc.sql)
			}
			got := res.Rows[0]["n"]
			if tc.knownBug != "" {
				if got == tc.want {
					t.Errorf("PIN %s now AGREES with PostgreSQL (COUNT(*) = %v): delete the pin, it is the proof the fix landed\n  SQL: %s",
						tc.knownBug, got, tc.sql)
				} else {
					t.Logf("PINNED %s: COUNT(*) = %v, PostgreSQL says %d\n  SQL: %s", tc.knownBug, got, tc.want, tc.sql)
				}
				return
			}
			if got != tc.want {
				t.Errorf("COUNT(*) = %v, want %d (PostgreSQL 17)\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}
