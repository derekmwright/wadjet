package wadjet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A refused UPDATE must be atomic over the WHOLE STATEMENT, not per file.
//
// The first fix made the re-ingest precede the marker commit, which closed the
// single-file case. It left a per-FILE window open: Ingest only BUFFERS, so a
// marker committed for file 1 inside the loop is durable while file 1's
// replacement rows are still in RAM, and a failure on file 2 returns without
// ever flushing. `UPDATE m SET n = 99` over a two-file table whose second file
// holds a legacy value past the column's precision therefore answered 22003
// with file 1's matched rows GONE (#647 re-review).
//
// Markers now accumulate across the statement, one FlushAll follows the loop,
// and one AddDeleteMarkers commits them only if it succeeded.
func TestFailedUpdateAcrossFilesLeavesEveryFileIntact(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The TABLE declares DECIMAL(9,2).
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "m", schema, nil); err != nil {
		t.Fatal(err)
	}

	// File 1 through the ordinary path.
	if _, err := db.Execute(ctx, "INSERT INTO m (id, n, d) VALUES (1, 1, 1.50)"); err != nil {
		t.Fatal(err)
	}
	// File 2 written directly at a WIDER declared precision and registered
	// under the table's schema: a legacy row from before the ingest check,
	// holding a value the table's own DECIMAL(9,2) cannot express.
	legacy := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 2, Nullable: true},
	}}
	writeLegacyFile(t, ctx, store, db.catalog, "m", legacy,
		[]map[string]any{{"id": int64(2), "n": int64(2), "d": "9999999999999999.99"}})

	before := updateRowSet(t, db, ctx)
	if len(before) != 2 {
		t.Fatalf("seeded %d rows, want 2: %v", len(before), before)
	}

	if _, err := db.Execute(ctx, "UPDATE m SET n = 99"); err == nil {
		t.Fatal("UPDATE over a file holding a value the column cannot express succeeded")
	} else if got := sqlerr.StateOf(err); got != "22003" {
		t.Fatalf("SQLSTATE %q, want 22003 (err: %v)", got, err)
	}

	after := updateRowSet(t, db, ctx)
	if len(after) != len(before) {
		t.Fatalf("%d rows after the REFUSED multi-file UPDATE, want %d — a file whose marker "+
			"committed before the statement failed lost its rows\n  before: %v\n  after:  %v",
			len(after), len(before), before, after)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("row %d is %q after the REFUSED UPDATE, want %q", i, after[i], before[i])
		}
	}
}

// The other way a statement fails after some files are done: the object store
// refusing the write. Nothing this statement matched may be deleted, because
// nothing it produced was ever stored.
func TestUpdateCommitsNoMarkersWhenTheFlushFails(t *testing.T) {
	ctx := context.Background()
	store := &failingDataStore{MemStore: objstore.NewMemStore()}
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "m", schema, nil); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		// One file per INSERT, so the UPDATE below spans three of them.
		if _, err := db.Execute(ctx, fmt.Sprintf("INSERT INTO m (id, n) VALUES (%d, %d)", i, i)); err != nil {
			t.Fatal(err)
		}
	}
	before := updateRowSet(t, db, ctx)
	if len(before) != 3 {
		t.Fatalf("seeded %d rows, want 3", len(before))
	}

	store.failParquet.Store(true)
	if _, err := db.Execute(ctx, "UPDATE m SET n = 99"); err == nil {
		t.Fatal("UPDATE succeeded while the object store refused every data write")
	}
	store.failParquet.Store(false)

	after := updateRowSet(t, db, ctx)
	if len(after) != len(before) {
		t.Fatalf("%d rows after an UPDATE whose flush failed, want %d — markers committed for "+
			"replacement rows that were never stored\n  before: %v\n  after:  %v",
			len(after), len(before), before, after)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("row %d is %q, want %q", i, after[i], before[i])
		}
	}
}

// A MERGE value past the target's declared (p, s) is refused where it is
// RESOLVED, not at the parquet leaf. resolveSetValue took a TypeID and
// swallowed every conversion failure — the quoted arm discarded the error and
// the literal arm answered the raw expression text (#647 re-review).
func TestMergeRefusesAValueTheTargetCannotHold(t *testing.T) {
	for _, tc := range []struct {
		name  string
		set   string
		state string
	}{
		{name: "literal past the precision", set: "d = 99999999999999999999.99", state: "22003"},
		{name: "quoted text naming no number", set: "d = 'abc'", state: "22P02"},
		{name: "quoted NaN", set: "d = 'NaN'", state: "22003"},
		{name: "source column past the precision", set: "d = s.v", state: "22003"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			if err := db.CreateTable(ctx, "u", parquet.Schema{Columns: []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
			}}, nil); err != nil {
				t.Fatal(err)
			}
			if err := db.CreateTable(ctx, "s", parquet.Schema{Columns: []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "v", Type: parquet.TypeString, Nullable: true},
			}}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx, "INSERT INTO u (id, d) VALUES (1, 1.50)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx, "INSERT INTO s (id, v) VALUES (1, '99999999999999999999.99')"); err != nil {
				t.Fatal(err)
			}

			_, err = db.Execute(ctx,
				"MERGE INTO u USING s ON u.id = s.id WHEN MATCHED THEN UPDATE SET "+tc.set)
			if err == nil {
				t.Fatalf("MERGE SET %s succeeded; want %s", tc.set, tc.state)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Fatalf("SQLSTATE %q, want %q (err: %v)", got, tc.state, err)
			}
			res, qerr := db.Query(ctx, "SELECT d FROM u")
			if qerr != nil {
				t.Fatal(qerr)
			}
			if len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["d"]) != "1.50" {
				t.Fatalf("the target changed under a REFUSED MERGE: %v", res.Rows)
			}
		})
	}
}

// --- helpers -------------------------------------------------------------

func updateRowSet(t *testing.T, db *DB, ctx context.Context) []string {
	t.Helper()
	res, err := db.Query(ctx, "SELECT id, n FROM m")
	if err != nil {
		t.Fatalf("reading the table back: %v", err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, fmt.Sprintf("id=%v n=%v", r["id"], r["n"]))
	}
	sort.Strings(out)
	return out
}

// writeLegacyFile registers a parquet file written under a DIFFERENT schema
// than the table declares — the shape a table carries after an older writer
// stored a value its column could not express.
func writeLegacyFile(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog,
	table string, fileSchema parquet.Schema, rows []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, fileSchema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	path := "tables/" + table + "/legacy_0001.parquet"
	if _, err := store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, table, map[string]string{}, "tables/"+table+"/", []catalog.FileEntry{{
		Path:      path,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatal(err)
	}
}

// failingDataStore refuses to write DATA files once armed, and is a TEST
// double only: it exists to make the object store fail the way a real one can
// mid-statement, which no production code path can be asked to simulate.
// Catalog metadata still writes, so the failure is the data write and nothing
// else.
type failingDataStore struct {
	*objstore.MemStore
	failParquet atomic.Bool
}

func (f *failingDataStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	if f.failParquet.Load() && strings.HasSuffix(key, ".parquet") {
		return "", errors.New("injected object-store failure")
	}
	return f.MemStore.Put(ctx, bucket, key, r, size, contentType)
}
