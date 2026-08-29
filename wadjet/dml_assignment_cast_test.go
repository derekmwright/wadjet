package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/dmlassign"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
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
			assignmentCheck(t, ctx, db, sql, tc.Col, tc.MergeValue(), tc.State, err)
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
