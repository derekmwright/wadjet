package compaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// countingStore counts Get calls per key, so a test can assert that a rewrite
// read each input file EXACTLY once — the property that makes RewriteTable
// terminate structurally rather than by a progress rule.
type countingStore struct {
	objstore.Store
	mu   sync.Mutex
	gets map[string]int
}

func newCountingStore() *countingStore {
	return &countingStore{Store: objstore.NewMemStore(), gets: map[string]int{}}
}

func (s *countingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	s.mu.Lock()
	s.gets[key]++
	s.mu.Unlock()
	return s.Store.Get(ctx, bucket, key)
}

func (s *countingStore) getCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

func setupCountingCatalog(t *testing.T) (*catalog.Catalog, *countingStore) {
	t.Helper()
	store := newCountingStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return cat, store
}

func rewriteTestSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}}
}

// TestRewriteTable_RewritesWhatCompactionWillNotTouch is the migration path
// ADR-0018's DECIMAL(p > 18) compatibility note depends on.
//
// "A compaction pass over the table is the whole migration" was not true of
// the code: shouldCompact refuses a partition with fewer than two files, with
// fewer than MinFiles, or whose average file is at or above MaxFileSizeBytes.
// A table that was already compacted — one big file per partition, which is
// what a healthy table looks like — is exactly the shape those floors reject,
// so the files needing the format migration most were the ones no pass would
// ever rewrite. RewriteTable is the explicit mode that does.
func TestRewriteTable_RewritesWhatCompactionWillNotTouch(t *testing.T) {
	schema := rewriteTestSchema()
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		// files in the one partition, and the SizeBytes each is reported as
		// in the manifest (the trigger reads the manifest, not the store).
		files       int
		reportSize  int64
		minFiles    int
		maxFileSize int64
	}{
		// The shape a migrated table is actually in: one already-compacted
		// file. shouldCompact's n < 2 floor refuses it forever.
		{"single_file", 1, 1024, 2, 32 << 20},
		// Below MinFiles: three files under a MinFiles of 10.
		{"below_min_files", 3, 1024, 10, 32 << 20},
		// Large files: over the average-size gate, so "worth merging" is
		// false even though the format migration still has to happen.
		{"large_files", 3, 512 << 20, 2, 32 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, store := setupCountingCatalog(t)
			table := "rw_" + tc.name
			if err := cat.CreateTable(ctx, table, schema, nil); err != nil {
				t.Fatal(err)
			}
			var inputs []string
			for i := 0; i < tc.files; i++ {
				path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, i)
				rows := []map[string]any{
					{"id": int64(i*2 + 0), "name": fmt.Sprintf("r%d", i*2)},
					{"id": int64(i*2 + 1), "name": nil},
				}
				writeTestFile(t, store, "test-bucket", path, schema, rows)
				if err := cat.AddFiles(ctx, table, nil, "", []catalog.FileEntry{
					{Path: path, SizeBytes: tc.reportSize, NumRows: int64(len(rows)), CreatedAt: time.Now().UTC()},
				}); err != nil {
					t.Fatal(err)
				}
				inputs = append(inputs, path)
			}

			cfg := DefaultConfig()
			cfg.MinFiles = tc.minFiles
			cfg.MaxFileSizeBytes = tc.maxFileSize
			cfg.DeleteGrace = -1 // delete immediately, so orphans are visible
			c := New(cat, nil, cfg)

			// First: ordinary compaction must still REFUSE this partition.
			// That is the floors doing their job, and the reason the rewrite
			// mode has to exist rather than the floors being loosened.
			before := manifestPaths(t, cat, table)
			cres, err := c.CompactTable(ctx, table)
			if err != nil {
				t.Fatalf("CompactTable: %v", err)
			}
			if cres.PartitionsCompacted != 0 || cres.FilesCreated != 0 {
				t.Fatalf("CompactTable compacted a partition below its floors: %+v", cres)
			}
			if got := manifestPaths(t, cat, table); !equalStrings(got, before) {
				t.Fatalf("CompactTable changed the manifest: %v -> %v", before, got)
			}

			// Now the rewrite: every file, once.
			done := make(chan *Result, 1)
			go func() {
				res, err := c.RewriteTable(ctx, table)
				if err != nil {
					t.Errorf("RewriteTable: %v", err)
				}
				done <- res
			}()
			var res *Result
			select {
			case res = <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("RewriteTable did not return — the rewrite is looping over its own output")
			}

			if res.FilesRemoved != tc.files {
				t.Errorf("FilesRemoved = %d, want every input (%d)", res.FilesRemoved, tc.files)
			}
			if res.FilesCreated < 1 {
				t.Errorf("FilesCreated = %d, want at least one output", res.FilesCreated)
			}
			if len(res.Failed) != 0 {
				t.Errorf("Failed = %+v, want none", res.Failed)
			}

			// Each input read exactly once: that is "rewritten once per
			// invocation" stated as a measurement, not as an assumption.
			for _, p := range inputs {
				if n := store.getCount(p); n != 1 {
					t.Errorf("input %s read %d times, want exactly 1", p, n)
				}
			}

			// None of the inputs survive in the manifest, and the outputs are
			// all new paths.
			after := manifestPaths(t, cat, table)
			for _, in := range inputs {
				for _, out := range after {
					if in == out {
						t.Errorf("input %s still in the manifest after a rewrite", in)
					}
				}
			}
			if len(after) != res.FilesCreated {
				t.Errorf("manifest holds %d files, rewrite reported creating %d", len(after), res.FilesCreated)
			}

			// And the data is intact.
			gotRows := readRewrittenRows(t, cat, table, schema)
			if len(gotRows) != tc.files*2 {
				t.Fatalf("read %d rows after the rewrite, want %d", len(gotRows), tc.files*2)
			}
		})
	}
}

// TestRewriteTable_MultipleGroupsPerPartition covers the shape where a
// partition holds more files than one memory-bounded group: every group is
// rewritten in the SAME invocation (compaction would need several passes),
// each input still read once, and the outputs land on distinct paths.
//
// The distinct-path assertion is not decoration: the output name's only
// distinguishing part is a nanosecond timestamp, and this is the one caller
// that emits several back to back. Two colliding names are not an error the
// store reports — the second Put overwrites the first and the first group's
// rows disappear with the manifest still pointing at the path.
func TestRewriteTable_MultipleGroupsPerPartition(t *testing.T) {
	ctx := context.Background()
	schema := rewriteTestSchema()
	cat, store := setupCountingCatalog(t)
	const table = "rw_groups"
	if err := cat.CreateTable(ctx, table, schema, nil); err != nil {
		t.Fatal(err)
	}

	const nFiles = 12
	var inputs []string
	for i := 0; i < nFiles; i++ {
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, i)
		rows := []map[string]any{{"id": int64(i), "name": fmt.Sprintf("r%d", i)}}
		writeTestFile(t, store, "test-bucket", path, schema, rows)
		if err := cat.AddFiles(ctx, table, nil, "", []catalog.FileEntry{
			// A large reported size keeps adaptivePassSize at MaxFilesPerPass.
			{Path: path, SizeBytes: 1 << 30, NumRows: 1, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, path)
	}

	cfg := DefaultConfig()
	cfg.MaxFilesPerPass = 5 // 12 files -> groups of 5, 5, 2
	cfg.DeleteGrace = -1
	res, err := New(cat, nil, cfg).RewriteTable(ctx, table)
	if err != nil {
		t.Fatal(err)
	}

	if res.FilesRemoved != nFiles {
		t.Errorf("FilesRemoved = %d, want %d", res.FilesRemoved, nFiles)
	}
	if res.FilesCreated != 3 {
		t.Errorf("FilesCreated = %d, want 3 groups of (5,5,2)", res.FilesCreated)
	}
	for _, p := range inputs {
		if n := store.getCount(p); n != 1 {
			t.Errorf("input %s read %d times, want exactly 1", p, n)
		}
	}

	after := manifestPaths(t, cat, table)
	if len(after) != 3 {
		t.Fatalf("manifest holds %d files, want 3: %v", len(after), after)
	}
	seen := map[string]bool{}
	for _, p := range after {
		if seen[p] {
			t.Fatalf("two manifest entries share the path %q — the second output overwrote the first", p)
		}
		seen[p] = true
	}

	rows := readRewrittenRows(t, cat, table, schema)
	if len(rows) != nFiles {
		t.Fatalf("read %d rows after the rewrite, want %d", len(rows), nFiles)
	}
}

// TestRewriteTable_ReportsAFailedPartitionAndKeepsGoing: the rewrite obeys the
// same report-and-continue contract as CompactTable.
func TestRewriteTable_ReportsAFailedPartitionAndKeepsGoing(t *testing.T) {
	ctx := context.Background()
	cat, store := setupTestCatalog(t)
	tableSchema := rewriteTestSchema()
	const table = "rw_mixed"
	if err := cat.CreateTable(ctx, table, tableSchema, nil); err != nil {
		t.Fatal(err)
	}
	// A STRING leaf under an INT64 "id": genuine drift, no exact repair.
	driftSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeString},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}}

	addPart := func(partPath string, values map[string]string, s parquet.Schema, id any) {
		path := fmt.Sprintf("tables/%s/%s/chunk.parquet", table, partPath)
		writeTestFile(t, store, "test-bucket", path, s, []map[string]any{{"id": id, "name": "x"}})
		if err := cat.AddFiles(ctx, table, values, "tables/"+table+"/"+partPath,
			[]catalog.FileEntry{{Path: path, SizeBytes: 1024, NumRows: 1, CreatedAt: time.Now().UTC()}}); err != nil {
			t.Fatal(err)
		}
	}
	addPart("d=bad", map[string]string{"d": "bad"}, driftSchema, "not-an-int")
	addPart("d=good", map[string]string{"d": "good"}, tableSchema, int64(7))

	cfg := DefaultConfig()
	cfg.DeleteGrace = -1
	res, err := New(cat, nil, cfg).RewriteTable(ctx, table)
	if err == nil {
		t.Fatalf("RewriteTable reported success over a partition it could not read: %+v", res)
	}
	var agg *CompactionFailed
	if !errors.As(err, &agg) {
		t.Fatalf("error %T is not *CompactionFailed: %v", err, err)
	}
	if len(agg.Failures) != 1 || !strings.Contains(agg.Failures[0].Partition, "d=bad") {
		t.Fatalf("Failures = %+v, want only the drifted partition", agg.Failures)
	}
	if !agg.Partial() {
		t.Error("Partial() is false, but the good partition was rewritten")
	}
	if res.FilesCreated != 1 {
		t.Errorf("FilesCreated = %d, want the good partition's one output", res.FilesCreated)
	}

	// The bad partition's input is untouched; the good one's is gone.
	paths := manifestPaths(t, cat, table)
	foundBad := false
	for _, p := range paths {
		if strings.Contains(p, "d=bad") {
			foundBad = true
		}
		if strings.Contains(p, "d=good") && strings.Contains(p, "chunk.parquet") {
			t.Errorf("the good partition's input survived the rewrite: %q", p)
		}
	}
	if !foundBad {
		t.Error("the failed partition's input was removed — a merge that could not run must not destroy its inputs")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// readRewrittenRows reads every manifest file of a table through the row path.
func readRewrittenRows(t *testing.T, cat *catalog.Catalog, table string, schema parquet.Schema) []map[string]any {
	t.Helper()
	ctx := context.Background()
	m, err := cat.GetManifest(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			rc, _, err := cat.ReadFile(ctx, f.Path)
			if err != nil {
				t.Fatalf("reading %s: %v", f.Path, err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			r, err := parquet.NewReaderFromBytes(data)
			if err != nil {
				t.Fatalf("opening %s: %v", f.Path, err)
			}
			rows, err := r.ReadRowsAs(schema.Columns, nil)
			if err != nil {
				t.Fatalf("reading rows of %s: %v", f.Path, err)
			}
			out = append(out, rows...)
		}
	}
	return out
}
