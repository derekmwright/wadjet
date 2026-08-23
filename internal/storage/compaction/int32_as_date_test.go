package compaction

import (
	"context"
	"io"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCompactTableRefusesInt32AsDate is the compaction-path half of #439's
// regression pin. A source file's footer can disagree with the catalog: here
// a leaf physically stores a bare INT32 (no DATE annotation, ordinary
// integers like 12345) while the table's catalog schema declares the column
// STRING. CoercibleTo used to admit that pairing and coerceDecoded rendered
// the integers as ISO dates via FormatDateDays, and #428 put that coercion on
// mergeAndWriteFiles's read-and-rewrite path — so a compaction pass replaced
// the inputs with a file holding fabricated dates ("2003-10-20") instead of
// the real values.
//
// After this fix, retypeFromCatalog refuses the pairing (INT32 is no longer
// in CoercibleTo's STRING arm), so ReadRowGroupAs — and therefore
// mergeAndWriteFiles — errors instead of coercing. CompactTable's pass loop
// currently swallows that error with a Warn and `continue` rather than
// returning it (#435, open, tracked separately): this test pins that
// EXISTING behavior — CompactTable reports no error and no partitions
// compacted for the drifted table — while asserting the property #435 exists
// to protect: a merge that cannot be read safely must not destroy the
// inputs. If #435 is fixed to surface the error instead, this test's
// no-error branch simply stops executing; either shape is correct so long as
// the inputs survive and nothing fabricated is written.
func TestCompactTableRefusesInt32AsDate(t *testing.T) {
	cat, store := setupTestCatalog(t)
	ctx := context.Background()

	// The CATALOG's authority: column "v" is STRING.
	catalogSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "driftv", catalogSchema, nil); err != nil {
		t.Fatal(err)
	}

	// The FILE's own footer disagrees: "v" is a bare INT32 leaf, no DATE
	// annotation, holding ordinary integers. This is the exact shape #439
	// fabricated dates from.
	fileSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt32, Nullable: true},
	}}
	rows := []map[string]any{
		{"id": int64(0), "v": int64(12345)},
		{"id": int64(1), "v": int64(1234)},
	}
	const path1 = "tables/driftv/chunk_0000.parquet"
	const path2 = "tables/driftv/chunk_0001.parquet"
	size1 := writeTestFile(t, store, "test-bucket", path1, fileSchema, rows)
	size2 := writeTestFile(t, store, "test-bucket", path2, fileSchema, rows)
	for _, e := range []catalog.FileEntry{
		{Path: path1, SizeBytes: size1, NumRows: int64(len(rows)), CreatedAt: time.Now().UTC()},
		{Path: path2, SizeBytes: size2, NumRows: int64(len(rows)), CreatedAt: time.Now().UTC()},
	} {
		if err := cat.AddFiles(ctx, "driftv", nil, "", []catalog.FileEntry{e}); err != nil {
			t.Fatal(err)
		}
	}

	cfg := DefaultConfig()
	cfg.MinFiles = 2 // both files are candidates for one merge pass
	result, err := New(cat, nil, cfg).CompactTable(ctx, "driftv")

	// See the doc comment: #435 currently swallows the merge error with a
	// Warn, so CompactTable itself reports (result, nil). Assert on whichever
	// of the two legal shapes actually happened, rather than pinning one.
	if err == nil {
		if result.PartitionsCompacted != 0 || result.FilesCreated != 0 || result.RowsMerged != 0 {
			t.Fatalf("CompactTable reported success compacting the drifted partition: %+v", result)
		}
	}

	// Whichever way the failure surfaced, the inputs must be exactly what
	// they were before: a merge that could not be read safely must not
	// replace them (#428/#429's lesson, restated).
	manifest, err := cat.GetManifest(ctx, "driftv")
	if err != nil {
		t.Fatal(err)
	}
	var gotPaths []string
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			gotPaths = append(gotPaths, f.Path)
		}
	}
	sort.Strings(gotPaths)
	wantPaths := []string{path1, path2}
	sort.Strings(wantPaths)
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("manifest files = %v, want the original inputs untouched %v", gotPaths, wantPaths)
	}

	// And the surviving file, read with the catalog's STRING schema, must
	// refuse — not answer with a fabricated date and not with "".
	rc, _, err := store.Get(ctx, "test-bucket", path1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := r.ReadRowsAs(catalogSchema.Columns, nil); err == nil {
		t.Fatalf("reading the surviving file through the catalog's STRING schema succeeded: %#v", got)
	}
}
