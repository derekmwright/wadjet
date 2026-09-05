package test

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
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

// dfrMixedFixture is dfrFixture's MIXED-case sibling: two columns the parquet
// file spells CamelCase and one it already spells folded.
//
// The mixed shape is the load-bearing part. A site that writes a row under the
// REFERENCE's spelling rather than the schema's loses exactly the columns
// whose two spellings differ, so on an all-CamelCase table the damage is
// total and on an all-lower-case table it is invisible; only a mixed schema
// shows the PARTIAL miss, where one column of a statement's own column list
// lands and the rest go NULL. Every column is nullable so that miss is a
// silently stored value rather than a not-null refusal — the quiet form.
func dfrMixedFixture(tb testing.TB) (*wadjet.DB, context.Context) {
	tb.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64, Nullable: true},
		{Name: "counterid", Type: parquet.TypeInt64, Nullable: true},
		{Name: "UserAgent", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.Catalog().CreateTable(ctx, "hits", schema, nil); err != nil {
		tb.Fatalf("create hits: %v", err)
	}
	rows := []map[string]any{
		{"WatchID": int64(1), "counterid": int64(10), "UserAgent": "old-1"},
		{"WatchID": int64(2), "counterid": int64(20), "UserAgent": "old-2"},
		{"WatchID": int64(3), "counterid": int64(30), "UserAgent": "old-3"},
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

// dfrRead3 reads the mixed table back as (WatchID, counterid, UserAgent).
func dfrRead3(tb testing.TB, db *wadjet.DB, ctx context.Context) [][3]any {
	tb.Helper()
	res, err := db.Query(ctx, `SELECT WatchID, counterid, UserAgent FROM hits ORDER BY WatchID`)
	if err != nil {
		tb.Fatalf("read back: %v", err)
	}
	out := make([][3]any, 0, len(res.Rows))
	for i := range res.Rows {
		c := res.Cells(i)
		out = append(out, [3]any{c[0], c[1], c[2]})
	}
	return out
}

func dfrWant3(tb testing.TB, got, want [][3]any, what string) {
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

// dfrRefused asserts a statement was REFUSED with PostgreSQL's SQLSTATE and
// that the message names the reference the statement actually wrote — the
// same two things the "column does not exist" path already answers with, so a
// client cannot tell the two causes apart and does not have to.
func dfrRefused(tb testing.TB, err error, state, names, what string) {
	tb.Helper()
	if err == nil {
		tb.Fatalf("%s: accepted; want %s naming %q", what, state, names)
	}
	if got := sqlerr.StateOf(err); got != state {
		tb.Fatalf("%s: SQLSTATE %q, want %q (error: %v)", what, got, state, err)
	}
	if !strings.Contains(err.Error(), names) {
		tb.Fatalf("%s: message %q does not name %q", what, err.Error(), names)
	}
}

// TestMergeActionsWriteUnderTheSchemasColumnNames is the regression gate for a
// MERGE that reports a row affected and writes nothing, and for one that
// stores a row with the columns it was given set to NULL.
//
// Both actions resolve their target column — `ev.targetColumn` — and then
// wrote the value back under the REFERENCE's spelling, `row[col]`, while the
// row is `readMergeTarget`'s `batch.RecordBatch.RowAt` (matched arm) or the
// map the ingester reads with a byte-exact `row[col.Name]` (not-matched arm).
// An unquoted identifier is folded (#731) and a parquet-born schema is not, so
// the two spellings differ on every CamelCase column and the write went into a
// key nothing reads.
//
// The existing MERGE cells above cannot see it: they spell each column exactly
// as the schema does, where the reference and the schema are the SAME STRING.
// These spell them the way an unquoted reference actually arrives.
func TestMergeActionsWriteUnderTheSchemasColumnNames(t *testing.T) {
	t.Run("MATCHED UPDATE with a folded SET column", func(t *testing.T) {
		// before: MERGE 1, and all three rows unchanged — the command tag was
		// a lie, which is worse than the honest MERGE 0 the ON key's own
		// fold used to produce.
		db, ctx := dfrMixedFixture(t)
		res, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 1 AS k) s ON hits.WatchID = s.k `+
			`WHEN MATCHED THEN UPDATE SET useragent = 'MERGED'`)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
		}
		dfrWant3(t, dfrRead3(t, db, ctx), [][3]any{
			{int64(1), int64(10), "MERGED"},
			{int64(2), int64(20), "old-2"},
			{int64(3), int64(30), "old-3"},
		}, "MATCHED folded SET")
	})

	t.Run("MATCHED UPDATE writing two columns, one of each spelling", func(t *testing.T) {
		// The PARTIAL miss on the matched arm: `counterid` is already folded,
		// so it landed either way and only `useragent` was lost. A statement
		// that half-worked is what makes this class survive review.
		db, ctx := dfrMixedFixture(t)
		if _, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 2 AS k) s ON hits.WatchID = s.k `+
			`WHEN MATCHED THEN UPDATE SET counterid = 99, useragent = 'BOTH'`); err != nil {
			t.Fatalf("merge: %v", err)
		}
		dfrWant3(t, dfrRead3(t, db, ctx), [][3]any{
			{int64(1), int64(10), "old-1"},
			{int64(2), int64(99), "BOTH"},
			{int64(3), int64(30), "old-3"},
		}, "MATCHED folded SET, two columns")
	})

	t.Run("NOT MATCHED INSERT with a folded column list", func(t *testing.T) {
		// before: MERGE 1 and the row stored as [<nil> 7 <nil>] — the one
		// column the schema already spells folded survived, both CamelCase
		// columns became NULL.
		db, ctx := dfrMixedFixture(t)
		res, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 42 AS k) s ON hits.WatchID = s.k `+
			`WHEN NOT MATCHED THEN INSERT (watchid, counterid, useragent) VALUES (42, 7, 'INSERTED')`)
		if err != nil {
			t.Fatalf("merge: %v", err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("RowsAffected = %d, want 1", res.RowsAffected)
		}
		dfrWant3(t, dfrRead3(t, db, ctx), [][3]any{
			{int64(1), int64(10), "old-1"},
			{int64(2), int64(20), "old-2"},
			{int64(3), int64(30), "old-3"},
			{int64(42), int64(7), "INSERTED"},
		}, "NOT MATCHED folded list")
	})

	t.Run("NOT MATCHED INSERT with a folded column list, NOT NULL schema", func(t *testing.T) {
		// The SAME miss under a declaration that catches it, and the reason
		// it is worth its own cell: the loud form names the wrong problem.
		// The row went to the ingester with `watchid` set and `WatchID`
		// absent, so the writer refused with
		// `null value in column "WatchID" violates not-null constraint` for a
		// column the statement had just supplied a value for — a refusal that
		// sends the reader looking at their VALUES list.
		db, ctx := dfrFixture(t)
		if _, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 42 AS k) s ON hits.WatchID = s.k `+
			`WHEN NOT MATCHED THEN INSERT (watchid, useragent) VALUES (42, 'INSERTED')`); err != nil {
			t.Fatalf("merge: %v", err)
		}
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "old-1"}, {int64(2), "old-2"}, {int64(3), "old-3"}, {int64(42), "INSERTED"},
		}, "NOT MATCHED folded list, NOT NULL schema")
	})

	t.Run("both actions in the schema's own spelling still write", func(t *testing.T) {
		// The control: the reference and the schema are the same string, so
		// this cell passes on both sides of the fix and pins that the fix
		// did not move the byte-exact case.
		db, ctx := dfrMixedFixture(t)
		if _, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 3 AS k) s ON hits.WatchID = s.k `+
			`WHEN MATCHED THEN UPDATE SET UserAgent = 'CTL' `+
			`WHEN NOT MATCHED THEN INSERT (WatchID, counterid, UserAgent) VALUES (43, 8, 'CTL-INS')`); err != nil {
			t.Fatalf("merge: %v", err)
		}
		if _, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 43 AS k) s ON hits.WatchID = s.k `+
			`WHEN MATCHED THEN UPDATE SET UserAgent = 'CTL' `+
			`WHEN NOT MATCHED THEN INSERT (WatchID, counterid, UserAgent) VALUES (43, 8, 'CTL-INS')`); err != nil {
			t.Fatalf("merge insert: %v", err)
		}
		dfrWant3(t, dfrRead3(t, db, ctx), [][3]any{
			{int64(1), int64(10), "old-1"},
			{int64(2), int64(20), "old-2"},
			{int64(3), int64(30), "CTL"},
			{int64(43), int64(8), "CTL-INS"},
		}, "schema-spelled control")
	})
}

// TestDelimitedDMLReferenceStaysByteExact is the regression gate for the write
// door accepting a DELIMITED identifier that is not the column's own bytes.
//
// The rule the read door already implements (batch.ResolveSchemaIndex, items
// 1-4) has two halves: an unquoted reference arrives FOLDED and may resolve
// case-insensitively against a parquet-born CamelCase schema, and a reference
// still carrying an upper-case letter can only have been DELIMITED, so it
// resolves byte-exact ONLY. Three DML sites kept only the first half — they
// lowercased the reference and looked it up in a map keyed by the fold — so
// `"USERAGENT"`, `"WATCHID"` and `hits."WATCHID"` all bound to columns
// PostgreSQL says do not exist:
//
//	UPDATE hits SET "USERAGENT" = 'X' WHERE WatchID = 1
//	  before: UPDATE 1, the value WRITTEN     after: 42703
//	UPDATE hits SET UserAgent = 'Z' WHERE "WATCHID" = 1
//	  before: UPDATE 0, silently matching nothing (the predicate DOES apply
//	          the rule, one layer down, and answered false for every row)
//	  after:  42703
//	MERGE ... ON hits."WATCHID" = s.k
//	  before: the MATCHED branch fires        after: 42703
//
// The two dispositions the write door had for a name that resolves to
// nothing — write to the wrong column, or match no row and report success —
// are now the ONE disposition a genuinely absent column always got, with the
// same SQLSTATE and the same message naming the reference as written.
func TestDelimitedDMLReferenceStaysByteExact(t *testing.T) {
	t.Run("delimited wrong-case SET target", func(t *testing.T) {
		db, ctx := dfrFixture(t)
		_, err := db.Execute(ctx, `UPDATE hits SET "USERAGENT" = 'X' WHERE WatchID = 1`)
		dfrRefused(t, err, "42703", "USERAGENT", "delimited wrong-case SET target")
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "old-1"}, {int64(2), "old-2"}, {int64(3), "old-3"},
		}, "refused SET wrote nothing")
	})

	t.Run("delimited wrong-case WHERE column, UPDATE", func(t *testing.T) {
		db, ctx := dfrFixture(t)
		_, err := db.Execute(ctx, `UPDATE hits SET UserAgent = 'Z' WHERE "WATCHID" = 1`)
		dfrRefused(t, err, "42703", "WATCHID", "delimited wrong-case UPDATE predicate")
	})

	t.Run("delimited wrong-case WHERE column, DELETE", func(t *testing.T) {
		db, ctx := dfrFixture(t)
		_, err := db.Execute(ctx, `DELETE FROM hits WHERE "WATCHID" = 3`)
		dfrRefused(t, err, "42703", "WATCHID", "delimited wrong-case DELETE predicate")
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "old-1"}, {int64(2), "old-2"}, {int64(3), "old-3"},
		}, "refused DELETE removed nothing")
	})

	t.Run("delimited wrong-case ON key", func(t *testing.T) {
		db, ctx := dfrFixture(t)
		_, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 1 AS k) s ON hits."WATCHID" = s.k `+
			`WHEN MATCHED THEN UPDATE SET UserAgent = 'MERGED'`)
		dfrRefused(t, err, "42703", "WATCHID", "delimited wrong-case ON key")
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "old-1"}, {int64(2), "old-2"}, {int64(3), "old-3"},
		}, "refused MERGE wrote nothing")
	})

	t.Run("a genuinely absent column is refused the same way", func(t *testing.T) {
		// The disposition these cells align the wrong-case names WITH. It
		// already held; it is asserted here so the alignment cannot be
		// achieved from the other end by loosening this one.
		db, ctx := dfrFixture(t)
		_, err := db.Execute(ctx, `UPDATE hits SET UserAgent = 'Z' WHERE nosuchcol = 1`)
		dfrRefused(t, err, "42703", "nosuchcol", "absent WHERE column")
		_, err = db.Execute(ctx, `UPDATE hits SET nosuchcol = 'Z' WHERE WatchID = 1`)
		dfrRefused(t, err, "42703", "nosuchcol", "absent SET target")
		_, err = db.Execute(ctx, `MERGE INTO hits USING (SELECT 1 AS k) s ON hits.nosuchcol = s.k `+
			`WHEN MATCHED THEN UPDATE SET UserAgent = 'M'`)
		dfrRefused(t, err, "42703", "nosuchcol", "absent ON key")
	})

	t.Run("delimited RIGHT-case references still resolve", func(t *testing.T) {
		// The other side of the boundary: byte-exact is not "refuse every
		// delimited name", it is "resolve the ones that match". A fix that
		// took this cell down would have narrowed the door instead of
		// correcting it.
		db, ctx := dfrFixture(t)
		if _, err := db.Execute(ctx, `UPDATE hits SET "UserAgent" = 'Q' WHERE "WatchID" = 3`); err != nil {
			t.Fatalf("delimited right-case UPDATE: %v", err)
		}
		if _, err := db.Execute(ctx, `MERGE INTO hits USING (SELECT 1 AS k) s ON hits."WatchID" = s.k `+
			`WHEN MATCHED THEN UPDATE SET UserAgent = 'MERGED'`); err != nil {
			t.Fatalf("delimited right-case ON key: %v", err)
		}
		dfrWant(t, dfrRead(t, db, ctx), [][2]any{
			{int64(1), "MERGED"}, {int64(2), "old-2"}, {int64(3), "Q"},
		}, "delimited right-case")
	})
}
