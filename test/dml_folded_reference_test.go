package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// dfrFixture registers a table whose catalog schema carries the CamelCase
// column names a parquet dataset gives it. Every DML fixture in the tree
// spells its columns lower case, which is precisely why none of them could
// see the defect these gates exist for: with a lower-case schema the folded
// reference and the schema's spelling are the SAME STRING, so a site that
// carries the wrong one of the two is indistinguishable from a correct one.
func dfrFixture(tb testing.TB) (*wadjet.DB, context.Context) {
	tb.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "UserAgent", Type: parquet.TypeString},
	}}
	// Through the CATALOG: the DDL door folds a column name it MINTS, so a
	// CamelCase schema is one a parquet dataset or ingest brought in.
	if err := db.Catalog().CreateTable(ctx, "hits", schema, nil); err != nil {
		tb.Fatalf("create hits: %v", err)
	}
	rows := []map[string]any{
		{"WatchID": int64(1), "UserAgent": "old-1"},
		{"WatchID": int64(2), "UserAgent": "old-2"},
		{"WatchID": int64(3), "UserAgent": "old-3"},
	}
	ing := db.NewIngester("hits", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		tb.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		tb.Fatalf("flush: %v", err)
	}
	return db, ctx
}

// dfrRead reads the table back as (WatchID, UserAgent) pairs, ordered.
func dfrRead(tb testing.TB, db *wadjet.DB, ctx context.Context) [][2]any {
	tb.Helper()
	res, err := db.Query(ctx, `SELECT WatchID, UserAgent FROM hits ORDER BY WatchID`)
	if err != nil {
		tb.Fatalf("read back: %v", err)
	}
	out := make([][2]any, 0, len(res.Rows))
	for i := range res.Rows {
		c := res.Cells(i)
		out = append(out, [2]any{c[0], c[1]})
	}
	return out
}

func dfrWant(tb testing.TB, got [][2]any, want [][2]any, what string) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("%s: %d rows, want %d (%v)", what, len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			tb.Fatalf("%s: row %d is %v, want %v (all rows: %v)", what, i, got[i], want[i], got)
		}
	}
}

// TestUpdateResolvesFoldedReferences is the regression gate for an UPDATE
// that reports success and writes nothing.
//
// An unquoted identifier folds to lower case at the lexer (#731), so
// `SET UserAgent = 'NEW'` reaches `ResolveDMLSetClauses` as `useragent`. The
// LOOKUP already conceded the case — `byName` is keyed folded — but the
// resolved assignment carried the FOLDED name forward, and the map it writes
// into is `batch.RecordBatch.RowAt`, whose keys are the SCHEMA's spelling.
// So the replacement row grew a second key `useragent` beside an untouched
// `UserAgent`, the parquet writer's byte-exact `row[col.Name]` read the OLD
// value, the delete markers were committed anyway, and the client was told
// `UPDATE 1`.
//
// That is silent: the row count is right, no error is raised, and the value
// is the one that was already there. Reverting either half of the hunk fails
// the first two cells below.
func TestUpdateResolvesFoldedReferences(t *testing.T) {
	t.Run("constant SET on an unquoted CamelCase column", func(t *testing.T) {
		db, ctx := dfrFixture(t)
		res, err := db.Execute(ctx, `UPDATE hits SET UserAgent = 'NEW' WHERE WatchID = 1`)
		if err != nil {
			t.Fatalf("update: %v", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
		}
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "NEW"}, {int64(2), "old-2"}, {int64(3), "old-3"},
		}, "constant SET")
	})

	t.Run("computed SET on an unquoted CamelCase column", func(t *testing.T) {
		// The expression branch appends its own assignment, so it is a
		// SECOND site with the same defect and needs its own cell.
		db, ctx := dfrFixture(t)
		if _, err := db.Execute(ctx, `UPDATE hits SET WatchID = WatchID + 10 WHERE UserAgent = 'old-2'`); err != nil {
			t.Fatalf("update: %v", err)
		}
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "old-1"}, {int64(3), "old-3"}, {int64(12), "old-2"},
		}, "computed SET")
	})

	t.Run("MERGE matches its target on an unquoted CamelCase key", func(t *testing.T) {
		// A MERGE's ON condition is parsed as an EXPRESSION, so its column
		// names arrive folded while the target row is keyed by the catalog
		// schema. `ON hits.WatchID = s.k` therefore matched NOTHING: every
		// WHEN MATCHED clause was skipped and the source row fell through to
		// WHEN NOT MATCHED, which INSERTED a duplicate of a row that was
		// already there. The row count is not the tell — one row was written
		// either way.
		db, ctx := dfrFixture(t)
		if _, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 1 AS k) s ON hits.WatchID = s.k `+
			`WHEN MATCHED THEN UPDATE SET UserAgent = 'MERGED' `+
			`WHEN NOT MATCHED THEN INSERT (WatchID, UserAgent) VALUES (9, 'INSERTED')`); err != nil {
			t.Fatalf("merge: %v", err)
		}
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "MERGED"}, {int64(2), "old-2"}, {int64(3), "old-3"},
		}, "MERGE matched")
	})

	t.Run("MERGE inserts under the schema's column names", func(t *testing.T) {
		db, ctx := dfrFixture(t)
		if _, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 42 AS k) s ON hits.WatchID = s.k `+
			`WHEN MATCHED THEN UPDATE SET UserAgent = 'MERGED' `+
			`WHEN NOT MATCHED THEN INSERT (WatchID, UserAgent) VALUES (s.k, 'INSERTED')`); err != nil {
			t.Fatalf("merge: %v", err)
		}
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "old-1"}, {int64(2), "old-2"}, {int64(3), "old-3"}, {int64(42), "INSERTED"},
		}, "MERGE not matched")
	})

	t.Run("delimited SET target in the schema's own case", func(t *testing.T) {
		db, ctx := dfrFixture(t)
		if _, err := db.Execute(ctx, `UPDATE hits SET "UserAgent" = 'Q' WHERE WatchID = 3`); err != nil {
			t.Fatalf("update: %v", err)
		}
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "old-1"}, {int64(2), "old-2"}, {int64(3), "Q"},
		}, "delimited SET")
	})
}
