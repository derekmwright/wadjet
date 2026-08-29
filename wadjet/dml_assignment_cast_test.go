package wadjet

import (
	"context"
	"fmt"
	"testing"

	"sort"

	"github.com/derekmwright/wadjet/internal/oracle/dmlassign"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The UPDATE arm of the SET-value matrix (internal/oracle/dmlassign): five
// value classes against four target types, every expectation read off
// postgres:17-alpine.
//
// The block this exists for is the DECIMAL one. An evaluated value is a VALUE,
// and ADR-0018 §4 defines a STORED integer box in a DECIMAL column as the
// already-unscaled CARRIER — so handing an evaluated int64 to
// DecimalValueFromBox reopened exactly the trap the #647 arc closed:
// `SET d = n` with n = 10 stored 0.10, `SET d = 1 + 1` stored 0.02, and both
// returned success (#678 review R1).
func TestDMLAssignmentCastFollowsPostgres(t *testing.T) {
	for _, tc := range dmlassign.Matrix() {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			db := assignmentDB(t)
			sql := "UPDATE mv SET " + tc.Set
			_, err := db.Execute(ctx, sql)
			assignmentCheck(t, ctx, db, sql, tc.Col, tc.Want, tc.State, err)
		})
	}
}

// The MERGE arm of the same matrix. It is a separate arm because MERGE
// resolves its values against a MERGED namespace and reads them out of a boxed
// row rather than off a batch, and that path had its own copy of the same
// defect — its direct-reference branch never reached the cast at all, so
// `SET d = s.k` with k = 7 stored 0.07.
func TestMergeAssignmentCastFollowsPostgres(t *testing.T) {
	for _, tc := range dmlassign.Matrix() {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			db := assignmentDB(t)
			sql := "MERGE INTO mv t USING mvs s ON t.id = s.id WHEN MATCHED THEN UPDATE SET " + tc.MergeSet()
			_, err := db.Execute(ctx, sql)
			assignmentCheck(t, ctx, db, sql, tc.Col, tc.MergeValue(), tc.MergeSQLState(), err)
		})
	}
}

// A MERGE names its columns in a namespace of TWO relations, and every name in
// it is resolved before the statement writes anything.
//
// `SET n = nosuchcol` used to store NULL and report success. `SET n = other.k`
// used to DROP the qualifier and store the source's k — for a relation the
// statement does not have. And an ON key naming nothing matched no row, so the
// MERGE reported success having silently done nothing (#678 review R2, and its
// residual 3). PostgreSQL raises 42703 / 42P01 / 42703 respectively.
func TestMergeResolvesEveryColumnItNames(t *testing.T) {
	const on = "MERGE INTO mv t USING mvs s ON "
	const matched = " WHEN MATCHED THEN UPDATE SET "
	for _, tc := range []struct {
		name  string
		sql   string
		state string
	}{
		{"SET value naming no column", on + "t.id = s.id" + matched + "n = nosuchcol", "42703"},
		{"SET value qualified by an unknown relation", on + "t.id = s.id" + matched + "n = other.k", "42P01"},
		{"SET value naming no column of the target", on + "t.id = s.id" + matched + "n = t.nosuchcol", "42703"},
		{"SET value naming no column of the source", on + "t.id = s.id" + matched + "n = s.nosuchcol", "42703"},
		{"SET value inside a function", on + "t.id = s.id" + matched + "n = ABS(nosuchcol)", "42703"},
		{"SET target naming no column", on + "t.id = s.id" + matched + "nosuchcol = 1", "42703"},
		{"ON key naming no column of the target", on + "t.nosuchcol = s.id" + matched + "n = 5", "42703"},
		{"ON key naming no column of the source", on + "t.id = s.nosuchcol" + matched + "n = 5", "42703"},
		// An ON qualifier naming NEITHER relation fails in parseOnKeys, before
		// checkOnKeys ever runs, so the code has to be carried at that site —
		// it used to fail there with no SQLSTATE at all (#678 re-review N2).
		{"ON key qualified by an unknown relation on the left",
			on + "zz.id = s.id" + matched + "n = 5", "42P01"},
		{"ON key qualified by an unknown relation on the right",
			on + "t.id = zz.id" + matched + "n = 5", "42P01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := assignmentDB(t)
			_, err := db.Execute(ctx, tc.sql)
			if err == nil {
				q, _ := db.Query(ctx, "SELECT id, n, d FROM mv")
				t.Fatalf("%s succeeded; want %s. The target is now %v", tc.sql, tc.state, q.Rows)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("SQLSTATE %q, want %q (err: %v)", got, tc.state, err)
			}
			// A refused MERGE wrote nothing.
			q, qerr := db.Query(ctx, "SELECT id, n, d FROM mv")
			if qerr != nil {
				t.Fatal(qerr)
			}
			if len(q.Rows) != 1 || fmt.Sprint(q.Rows[0]["n"]) != "10" || fmt.Sprint(q.Rows[0]["d"]) != "1.50" {
				t.Errorf("the refused MERGE left %v, want the original single row (n=10, d=1.50)", q.Rows)
			}
		})
	}
}

// An UNQUALIFIED name in a MERGE resolves in the merged namespace. PostgreSQL
// answers `SET n = k` with 7 — the source's k — and buildMergedRow's map has
// always held the same, so the resolver must agree with both rather than
// refuse the spelling.
func TestMergeUnqualifiedNameResolvesInTheMergedNamespace(t *testing.T) {
	ctx := context.Background()
	db := assignmentDB(t)
	sql := "MERGE INTO mv t USING mvs s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = k"
	if _, err := db.Execute(ctx, sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	q, err := db.Query(ctx, "SELECT n FROM mv")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(q.Rows[0]["n"]); got != "7" {
		t.Errorf("SET n = k stored %s, want 7 (the source's k, as PostgreSQL resolves it)", got)
	}
}

func assignmentCheck(t *testing.T, ctx context.Context, db *DB, sql, col, want, state string, err error) {
	t.Helper()
	if state != "" {
		if err == nil {
			q, _ := db.Query(ctx, "SELECT n, d, f, s FROM mv")
			t.Fatalf("%s succeeded; want %s. Row is now %v", sql, state, q.Rows)
		}
		if got := sqlerr.StateOf(err); got != state {
			t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", sql, got, state, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	q, qerr := db.Query(ctx, "SELECT n, d, f, s FROM mv")
	if qerr != nil {
		t.Fatal(qerr)
	}
	if len(q.Rows) != 1 {
		t.Fatalf("%d rows after %s, want 1: %v", len(q.Rows), sql, q.Rows)
	}
	if got := fmt.Sprint(q.Rows[0][col]); got != want {
		t.Errorf("%s stored %s = %s, want %s (PostgreSQL 17)", sql, col, got, want)
	}
}

func assignmentDB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, sql := range []string{dmlassign.TargetDDL, dmlassign.SourceDDL} {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{dmlassign.TargetRow, dmlassign.SourceRow} {
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

// A value whose TYPE the column cannot take is 42804 (datatype_mismatch) — not
// 22P02, which means "the text does not spell a value of that type", and not a
// bare ingest error with no SQLSTATE at all.
//
// BOOL is the case that reaches it. `SET n = b` failed at ingest.checkType with
// "expected integer, got bool" and no code; `SET d = b` reached
// DecimalValueFromBox's default and answered 22P02. PostgreSQL says 42804 for
// bigint, numeric and double precision alike — and ACCEPTS bool into text,
// storing 'true', which is why that row is a value and not a refusal (all four
// verified live on postgres:17-alpine, #678 re-review N3).
func TestDMLIncompatibleValueTypeIsDatatypeMismatch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		col   string
		state string
		want  string
	}{
		{name: "BOOL into INT64", sql: "UPDATE bz SET n = b", state: "42804"},
		{name: "BOOL into DECIMAL", sql: "UPDATE bz SET d = b", state: "42804"},
		{name: "BOOL into FLOAT64", sql: "UPDATE bz SET f = b", state: "42804"},
		{name: "BOOL into STRING is ACCEPTED", sql: "UPDATE bz SET s = b", col: "s", want: "true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query(ctx,
				"CREATE TABLE bz (id INT64, n INT64, d DECIMAL(9,2), f FLOAT64, s STRING, b BOOL)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx, "INSERT INTO bz VALUES (1, 10, 1.50, 2.5, 'ab', true)"); err != nil {
				t.Fatal(err)
			}
			_, execErr := db.Execute(ctx, tc.sql)
			if tc.state != "" {
				if execErr == nil {
					q, _ := db.Query(ctx, "SELECT n, d, f, s FROM bz")
					t.Fatalf("%s succeeded; want %s. Row is now %v", tc.sql, tc.state, q.Rows)
				}
				if got := sqlerr.StateOf(execErr); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, execErr)
				}
				return
			}
			if execErr != nil {
				t.Fatalf("%s: %v", tc.sql, execErr)
			}
			q, qerr := db.Query(ctx, "SELECT n, d, f, s FROM bz")
			if qerr != nil {
				t.Fatal(qerr)
			}
			if got := fmt.Sprint(q.Rows[0][tc.col]); got != tc.want {
				t.Errorf("%s stored %s = %s, want %s (PostgreSQL 17)", tc.sql, tc.col, got, tc.want)
			}
		})
	}
}

// One DECIMAL value must land in ONE partition directory however it was
// written.
//
// ingest.formatPartitionValue prints a DECIMAL box VERBATIM into the directory
// name, so the box a DML assignment hands on decides the path. Handing on the
// bare `10` an integer expression evaluates to, where an INSERT of the same
// value hands on `10.00`, put one value in two directories — no rows lost, but
// every scan of that value then has to read both, and a compaction of one does
// not see the other (#678 re-review N4). assignDecimalValue renders the
// canonical text at the column's scale, which is also what PostgreSQL prints
// (10::bigint::numeric(9,2) is 10.00).
func TestDecimalPartitionPathIsCanonicalWhicheverPathWroteIt(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "pz", schema, []string{"d"}); err != nil {
		t.Fatal(err)
	}
	// The INSERT path, writing the canonical spelling.
	if _, err := db.Execute(ctx, "INSERT INTO pz VALUES (1, 10, 10.00)"); err != nil {
		t.Fatal(err)
	}
	// The ASSIGNMENT path, writing the same value from an integer expression.
	if _, err := db.Execute(ctx, "INSERT INTO pz VALUES (2, 10, 1.00)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "UPDATE pz SET d = n WHERE id = 2"); err != nil {
		t.Fatal(err)
	}

	manifest, err := db.Catalog().GetManifest(ctx, "pz")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, part := range manifest.Partitions {
		if len(part.Files) == 0 {
			continue
		}
		seen[part.Values["d"]] = true
	}
	if seen["10"] {
		t.Errorf("the assignment path wrote partition d=10 while the INSERT path wrote d=10.00: "+
			"one value, two directories. Partitions holding files: %v", partitionValuesOf(seen))
	}
	if !seen["10.00"] {
		t.Errorf("no partition d=10.00; partitions holding files: %v", partitionValuesOf(seen))
	}
	// And the value still reads back correctly through both.
	q, err := db.Query(ctx, "SELECT id, d FROM pz WHERE d = 10.00 ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Rows) != 2 {
		t.Fatalf("%d rows hold d = 10.00, want 2 (one from each path): %v", len(q.Rows), q.Rows)
	}
}

// PIN: PostgreSQL rounds a float8 half to EVEN and a numeric half AWAY from
// zero, and this engine boxes both as float64 — so one rounding rule has to
// serve both, and half-away-from-zero is the one kept.
//
// TODO(#699): give the assignment cast the source's TYPE so each family gets
// its own rule. Until then the divergence is REAL and is pinned here rather
// than described in prose: the values below were read off postgres:17-alpine,
// and this test fails if the rule is ever changed in either direction — which
// is the point, because "we chose half-away-from-zero" is only true while
// something checks it.
//
//	        float8   numeric   wadjet
//	 2.5 ->    2         3        3     <- diverges from float8
//	-2.5 ->   -2        -3       -3     <- diverges from float8
//	 0.5 ->    0         1        1     <- diverges from float8
//	 3.5 ->    4         4        4        agrees with both
//	 1.5 ->    2         2        2        agrees with both
func TestFloatHalfRoundingIsPinnedToTheNumericRule(t *testing.T) {
	for _, tc := range []struct {
		f          string
		pgFloat8   string
		pgNumeric  string
		wadjetWant string
	}{
		{f: "2.5", pgFloat8: "2", pgNumeric: "3", wadjetWant: "3"},
		{f: "0 - 2.5", pgFloat8: "-2", pgNumeric: "-3", wadjetWant: "-3"},
		{f: "0.5", pgFloat8: "0", pgNumeric: "1", wadjetWant: "1"},
		{f: "3.5", pgFloat8: "4", pgNumeric: "4", wadjetWant: "4"},
		{f: "1.5", pgFloat8: "2", pgNumeric: "2", wadjetWant: "2"},
	} {
		t.Run(tc.f, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query(ctx, "CREATE TABLE rz (id INT64, n INT64)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx, "INSERT INTO rz VALUES (1, 0)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx, "UPDATE rz SET n = "+tc.f); err != nil {
				t.Fatalf("UPDATE rz SET n = %s: %v", tc.f, err)
			}
			q, qerr := db.Query(ctx, "SELECT n FROM rz")
			if qerr != nil {
				t.Fatal(qerr)
			}
			got := fmt.Sprint(q.Rows[0]["n"])
			if got != tc.wadjetWant {
				t.Fatalf("SET n = %s stored %s, want %s — the rounding rule moved. PostgreSQL: "+
					"float8 %s, numeric %s. Changing the rule needs TODO(#699) resolved and this "+
					"pin updated, not deleted", tc.f, got, tc.wadjetWant, tc.pgFloat8, tc.pgNumeric)
			}
			if tc.pgFloat8 != tc.pgNumeric && got != tc.pgNumeric {
				t.Errorf("SET n = %s stored %s; the kept rule is PostgreSQL's NUMERIC one (%s)",
					tc.f, got, tc.pgNumeric)
			}
		})
	}
}

// partitionValuesOf renders the partition values that hold files, sorted, so a
// failure names the directories rather than a map in random order.
func partitionValuesOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
