package wadjet

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// Re-updating one row DOUBLED it: 1 row, then 2, then 4 (#674).
//
// A merge-on-read table's live rows are its files minus its delete markers.
// The SELECT path has applied that filter all along; the DML match scans did
// not, so every UPDATE re-matched the copies its own earlier UPDATEs had
// superseded, re-ingested them beside the live one, and marked the source file
// again. Plain INT64 columns, the default path, no error anywhere.
//
// The gate is a COUNT taken after EACH statement rather than once at the end,
// because 1 → 2 → 4 is already visible at the second statement and a test that
// looks only at the end cannot say which one broke.
//
// MERGE's arm of the same rule is TestReMergeDoesNotMultiplyRows, and it lands
// with #676: MERGE's target scan indexes `SELECT *` order while its marker loop
// walks manifest order, so filtering that logical index cannot be VERIFIED
// until the two are the same walk. Fixing it here would have been a change no
// test could tell apart from the defect.
//
// The HTTP executors are a second copy of this code with the same defect;
// their twin of this test is internal/server.TestHTTPReUpdateDoesNotMultiplyRows.
func TestReUpdateDoesNotMultiplyRows(t *testing.T) {
	ctx := context.Background()
	db := visibilityDB(t)
	mustDDL(t, db, "CREATE TABLE u (id INT64, n INT64)")
	mustExec(t, db, "INSERT INTO u VALUES (1, 1)")
	mustExec(t, db, "INSERT INTO u VALUES (2, 100)")

	for i := 2; i <= 6; i++ {
		mustExec(t, db, fmt.Sprintf("UPDATE u SET n = %d WHERE id = 1", i))
		rows := mustRows(t, db, "SELECT id, n FROM u WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("after %d UPDATEs id=1 is %d rows, want 1: %v", i-1, len(rows), rows)
		}
		if got := fmt.Sprint(rows[0]["n"]); got != fmt.Sprint(i) {
			t.Errorf("after UPDATE %d, n = %v, want %d", i-1, rows[0]["n"], i)
		}
		// The row the statement did not touch is untouched, and counted once.
		other := mustRows(t, db, "SELECT id, n FROM u WHERE id = 2")
		if len(other) != 1 || fmt.Sprint(other[0]["n"]) != "100" {
			t.Fatalf("the untouched row is %v after %d UPDATEs, want one row with n=100", other, i-1)
		}
	}

	// Compaction rewrites the files and rebases the markers. The live row set
	// must be the same on the other side, and a further UPDATE must still not
	// multiply — compaction is where a wrong marker becomes permanent, since
	// it REPLACES its inputs.
	c := compaction.New(db.Catalog(), slog.Default(), compaction.Config{})
	if _, err := c.RewriteTable(ctx, "u"); err != nil {
		t.Fatalf("compacting: %v", err)
	}
	rows := mustRows(t, db, "SELECT id, n FROM u")
	if len(rows) != 2 {
		t.Fatalf("after compaction the table holds %d rows, want 2: %v", len(rows), rows)
	}
	mustExec(t, db, "UPDATE u SET n = 7 WHERE id = 1")
	rows = mustRows(t, db, "SELECT id, n FROM u WHERE id = 1")
	if len(rows) != 1 || fmt.Sprint(rows[0]["n"]) != "7" {
		t.Fatalf("after compaction and one more UPDATE, id=1 is %v, want one row with n=7", rows)
	}
}

// A DELETE after a chain of UPDATEs removes the LIVE copy and nothing else —
// the other half of the same filter. Before #674 the DELETE matched every
// superseded copy too, which left the right row set only because it deleted
// them all, and reported a row count nobody could explain.
func TestDeleteAfterReUpdateRemovesExactlyTheLiveRow(t *testing.T) {
	db := visibilityDB(t)
	mustDDL(t, db, "CREATE TABLE dv (id INT64, n INT64)")
	mustExec(t, db, "INSERT INTO dv VALUES (1, 1)")
	mustExec(t, db, "INSERT INTO dv VALUES (2, 2)")
	for i := 2; i <= 4; i++ {
		mustExec(t, db, fmt.Sprintf("UPDATE dv SET n = %d WHERE id = 1", i))
	}

	// The reported count is the number of LIVE rows removed, not the number of
	// physical copies.
	if got := mustExec(t, db, "DELETE FROM dv WHERE id = 1"); got != 1 {
		t.Errorf("DELETE reported %d rows affected, want 1 — a superseded copy is not a row", got)
	}
	rows := mustRows(t, db, "SELECT id, n FROM dv")
	if len(rows) != 1 || fmt.Sprint(rows[0]["id"]) != "2" {
		t.Fatalf("after the DELETE the table holds %v, want only id=2", rows)
	}
	if got := mustExec(t, db, "DELETE FROM dv WHERE id = 1"); got != 0 {
		t.Errorf("a second DELETE of the same row reported %d rows affected, want 0", got)
	}
}

func visibilityDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustDDL(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Query(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func mustExec(t *testing.T, db *DB, sql string) int64 {
	t.Helper()
	res, err := db.Execute(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return res.RowsAffected
}

func mustRows(t *testing.T, db *DB, sql string) []map[string]any {
	t.Helper()
	q, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return q.Rows
}
