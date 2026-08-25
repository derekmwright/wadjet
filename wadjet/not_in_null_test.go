package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// niOpen loads the three NULL fixtures #507 measured, plus a string-keyed twin
// of one of them so both key encodings are exercised (the integer and the
// serialized key paths reach the hash table by different code, and #459 is the
// last time they disagreed about NULL).
//
//	A — NULLs on both sides
//	B — non-null probe, NULL-poisoned list
//	C — NULL probe, clean list
func niOpen(t *testing.T) *DB {
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
	intCols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
	}
	strCols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}

	load("probe_a", intCols, []map[string]any{
		{"id": int64(1), "k": int64(1)},
		{"id": int64(2), "k": nil},
		{"id": int64(3), "k": int64(3)},
	})
	load("lst_a", intCols, []map[string]any{
		{"id": int64(1), "k": int64(1)},
		{"id": int64(2), "k": nil},
	})
	load("probe_b", intCols, []map[string]any{
		{"id": int64(1), "k": int64(1)},
		{"id": int64(2), "k": int64(2)},
		{"id": int64(3), "k": int64(3)},
	})
	load("lst_b", intCols, []map[string]any{
		{"id": int64(1), "k": int64(1)},
		{"id": int64(2), "k": nil},
	})
	load("probe_c", intCols, []map[string]any{
		{"id": int64(1), "k": int64(1)},
		{"id": int64(2), "k": nil},
		{"id": int64(3), "k": int64(3)},
	})
	load("lst_c", intCols, []map[string]any{
		{"id": int64(1), "k": int64(1)},
	})
	// String-keyed twin of case B.
	load("probe_s", strCols, []map[string]any{
		{"id": int64(1), "s": "a"},
		{"id": int64(2), "s": "b"},
		{"id": int64(3), "s": "c"},
	})
	load("lst_s", strCols, []map[string]any{
		{"id": int64(1), "s": "a"},
		{"id": int64(2), "s": nil},
	})
	return db
}

// TestNotInSubqueryIsThreeValuedOverNulls is #507's gate.
//
// `decorrelateInSubqueries` lowers NOT IN straight to an anti join, and an
// anti join asks a two-valued question: did this probe row match nothing.
// NOT IN's rule is three-valued — TRUE only when the key differs from EVERY
// value the subquery returns, FALSE when it equals one, and UNKNOWN (so WHERE
// drops the row) the moment the key itself is NULL or the subquery returned a
// NULL the key did not match on some other value. An anti join has no way to
// say UNKNOWN: a probe row with no matching build row is emitted whether the
// "no match" is a real difference or NULL trivially failing to equal anything.
//
// So a NULL anywhere in the list empties the whole answer, and a NULL probe
// key never survives. Both are decided by the BUILD side, which is why the
// operator carries them (exec.HashJoin.NullAwareAnti) rather than the planner
// rewriting the predicate.
//
// The IN and NOT EXISTS twins are the controls: a semi join IS the right
// two-valued question for IN, and NOT EXISTS is a different predicate that was
// already correct — if a fix to NOT IN moved either of them, it moved too
// much. Every want is a live postgres:17-alpine transcript over these rows.
func TestNotInSubqueryIsThreeValuedOverNulls(t *testing.T) {
	ctx := context.Background()
	db := niOpen(t)

	cases := []struct {
		name string
		sql  string
		want int64
	}{
		// A — NULLs on both sides. id=1 matches, id=2's own key is NULL, and
		// id=3 compares against the list's NULL: every row is UNKNOWN.
		{"a_not_in", `SELECT COUNT(*) AS n FROM probe_a p WHERE p.k NOT IN (SELECT l.k FROM lst_a l)`, 0},
		{"a_in", `SELECT COUNT(*) AS n FROM probe_a p WHERE p.k IN (SELECT l.k FROM lst_a l)`, 1},
		{"a_not_exists", `SELECT COUNT(*) AS n FROM probe_a p WHERE NOT EXISTS (SELECT 1 FROM lst_a l WHERE l.k = p.k)`, 2},
		{"a_exists", `SELECT COUNT(*) AS n FROM probe_a p WHERE EXISTS (SELECT 1 FROM lst_a l WHERE l.k = p.k)`, 1},

		// B — every probe key is non-null, but the list carries a NULL, so
		// only the row that MATCHES is decided; the rest go UNKNOWN.
		{"b_not_in", `SELECT COUNT(*) AS n FROM probe_b p WHERE p.k NOT IN (SELECT l.k FROM lst_b l)`, 0},
		{"b_in", `SELECT COUNT(*) AS n FROM probe_b p WHERE p.k IN (SELECT l.k FROM lst_b l)`, 1},
		{"b_not_exists", `SELECT COUNT(*) AS n FROM probe_b p WHERE NOT EXISTS (SELECT 1 FROM lst_b l WHERE l.k = p.k)`, 2},

		// C — clean list, one NULL probe key. The one row that genuinely
		// differs from every list value is the only survivor.
		{"c_not_in", `SELECT COUNT(*) AS n FROM probe_c p WHERE p.k NOT IN (SELECT l.k FROM lst_c l)`, 1},
		{"c_in", `SELECT COUNT(*) AS n FROM probe_c p WHERE p.k IN (SELECT l.k FROM lst_c l)`, 1},
		{"c_not_exists", `SELECT COUNT(*) AS n FROM probe_c p WHERE NOT EXISTS (SELECT 1 FROM lst_c l WHERE l.k = p.k)`, 2},

		// The control that says the anti join still ANSWERS: strip the NULL
		// out of the list and NOT IN keeps the two rows that differ.
		{"b_not_in_null_free_list",
			`SELECT COUNT(*) AS n FROM probe_b p WHERE p.k NOT IN (SELECT l.k FROM lst_b l WHERE l.k IS NOT NULL)`, 2},

		// An EMPTY subquery is the boundary of the rule, and the place a
		// NULL-guard goes one step too far. Both rules above are about a
		// COMPARISON, and an empty set offers none: `k NOT IN ()` is TRUE for
		// EVERY row — the NULL-keyed one included, because there is no value
		// for its comparison to be UNKNOWN about — and `k IN ()` is FALSE for
		// every row, NULL-keyed one included. Guarding only on "the list held
		// a NULL" answered 2 of 3 here.
		{"c_not_in_empty_set",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE p.k NOT IN (SELECT l.k FROM lst_c l WHERE l.k > 999)`, 3},
		{"c_in_empty_set",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE p.k IN (SELECT l.k FROM lst_c l WHERE l.k > 999)`, 0},
		// The same over an empty list on the STRING key path.
		{"s_not_in_empty_set",
			`SELECT COUNT(*) AS n FROM probe_s p WHERE p.s NOT IN (SELECT l.s FROM lst_s l WHERE l.s > 'zzz')`, 3},
		// A list that is not empty but holds ONLY NULLs is NOT the empty case
		// — it poisons, and the guard must tell the two apart.
		{"c_not_in_all_null_list",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE p.k NOT IN (SELECT l.k FROM lst_b l WHERE l.k IS NULL)`, 0},

		// The serialized-key path reaches the hash table by different code
		// than the integer one, and #459 is the last time the two disagreed
		// about NULL.
		{"s_not_in", `SELECT COUNT(*) AS n FROM probe_s p WHERE p.s NOT IN (SELECT l.s FROM lst_s l)`, 0},
		{"s_in", `SELECT COUNT(*) AS n FROM probe_s p WHERE p.s IN (SELECT l.s FROM lst_s l)`, 1},
		{"s_not_in_null_free_list",
			`SELECT COUNT(*) AS n FROM probe_s p WHERE p.s NOT IN (SELECT l.s FROM lst_s l WHERE l.s IS NOT NULL)`, 2},
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

	// The VALUES, not just the count: a right count with the wrong row is the
	// shape ADR-0013 §Pins warns about.
	res, err := db.Query(ctx, `SELECT p.id AS id FROM probe_c p WHERE p.k NOT IN (SELECT l.k FROM lst_c l) ORDER BY p.id`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["id"] != int64(3) {
		t.Errorf("NOT IN over case C returned %v, want the single row id=3 (PostgreSQL 17)", res.Rows)
	}
}

// #571. The same predicates over a subquery whose FROM is a DERIVED TABLE.
//
// The parser keeps a FROM-subquery as a table whose NAME is its own SQL text,
// and the plan BUILDER recognises that and recurses. The three decorrelation
// rewrites did not: each called NewScan on the text it was handed, producing a
// Scan of a table the catalog has never heard of. Such a scan is not an error
// — it yields ZERO batches — so the semi/anti join's build side was empty and
// `IN` answered nothing while `NOT IN` answered every row, silently.
//
// The NULL cases are why this belongs next to #507's gate rather than in a
// file of its own: an empty build side answers 0 for a poisoned NOT IN too,
// by accident, and the IN twin beside each one is what tells the accident from
// the rule. Every want is a live postgres:17-alpine transcript over these rows.
func TestSubqueryOverADerivedTableIsNotDecorrelatedIntoAnEmptyBuild(t *testing.T) {
	ctx := context.Background()
	db := niOpen(t)

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		// The live repro both #571 and #572 were filed from: a derived table
		// JOINED to a base relation, with a NULL reaching the list. The
		// subquery yields {1, NULL}: p.k=1 is excluded by the match, and the
		// other two rows compare against the NULL and go UNKNOWN.
		{"joined_derived_not_in",
			`SELECT COUNT(*) AS n FROM probe_a p WHERE p.k NOT IN
				(SELECT s.k FROM (SELECT l.k AS k, l.id AS id FROM lst_a l) s
				 JOIN probe_a b ON b.id = s.id)`, 0},
		{"joined_derived_in",
			`SELECT COUNT(*) AS n FROM probe_a p WHERE p.k IN
				(SELECT s.k FROM (SELECT l.k AS k, l.id AS id FROM lst_a l) s
				 JOIN probe_a b ON b.id = s.id)`, 1},

		// The plain derived table, no join. A clean list, so NOT IN keeps
		// exactly the row that differs from every value in it.
		{"derived_not_in",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE p.k NOT IN
				(SELECT s.k FROM (SELECT l.k AS k FROM lst_c l) s)`, 1},
		{"derived_in",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE p.k IN
				(SELECT s.k FROM (SELECT l.k AS k FROM lst_c l) s)`, 1},
		// An EMPTY derived set is a real answer, not an absence: NOT IN is
		// TRUE for every row including the NULL-keyed one, which is also
		// what an empty build side answered for the wrong reason.
		{"derived_not_in_empty_set",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE p.k NOT IN
				(SELECT s.k FROM (SELECT l.k AS k FROM lst_c l WHERE l.k > 999) s)`, 3},

		// The serialized-key path reaches the hash table by different code
		// than the integer one (#459).
		{"derived_not_in_string",
			`SELECT COUNT(*) AS n FROM probe_s p WHERE p.s NOT IN
				(SELECT s.v FROM (SELECT l.s AS v FROM lst_s l) s)`, 0},
		{"derived_in_string",
			`SELECT COUNT(*) AS n FROM probe_s p WHERE p.s IN
				(SELECT s.v FROM (SELECT l.s AS v FROM lst_s l) s)`, 1},

		// The other two rewrites that build their inner plan the same way:
		// correlated EXISTS and the scalar subquery.
		{"derived_exists",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE EXISTS
				(SELECT 1 FROM (SELECT l.k AS k FROM lst_c l) s WHERE s.k = p.k)`, 1},
		{"derived_not_exists",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE NOT EXISTS
				(SELECT 1 FROM (SELECT l.k AS k FROM lst_c l) s WHERE s.k = p.k)`, 2},
		{"derived_scalar_subquery",
			`SELECT COUNT(*) AS n FROM probe_c p WHERE p.k >
				(SELECT MAX(s.k) FROM (SELECT l.k AS k FROM lst_c l) s)`, 1},
	} {
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

	// The VALUES: a right count over the wrong row is what a count-only
	// assertion cannot see, and an empty build side gets one of these counts
	// right by accident.
	res, err := db.Query(ctx,
		`SELECT p.id AS id FROM probe_c p WHERE p.k NOT IN
			(SELECT s.k FROM (SELECT l.k AS k FROM lst_c l) s) ORDER BY p.id`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["id"] != int64(3) {
		t.Errorf("NOT IN over a derived-table list returned %v, want the single row id=3 "+
			"(PostgreSQL 17)", res.Rows)
	}
}
