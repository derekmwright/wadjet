package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #483. A process holds more than one *Catalog over the same KV — standalone
// holds the coordinator's, the pgwire DB's, and a fresh one per worker
// pipeline task — and pgwire sends SELECT to the coordinator's while
// INSERT/UPDATE/DELETE and DDL go to the DB's. The manifest cache used to be
// validated by a 2-second wall clock and invalidated only by writes made
// through the same *Catalog value, so a write through one instance was
// invisible to a read through another for up to two seconds: back-to-back
// statements saw writes vanish, and DROP TABLE + CREATE TABLE of the same
// name answered out of the dropped incarnation's files.
//
// The contract these tests hold: a read through ANY instance reflects every
// write that has already returned through ANY instance, with no delay to wait
// out. That is a property of the KV revision, so no test here sleeps.
func twoCatalogsOverOneKV(t *testing.T) (writer, reader *Catalog) {
	t.Helper()
	kv := NewMemKV()
	store := objstore.NewMemStore()
	ctx := context.Background()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	writer = New(kv, store, "test")
	if err := writer.Init(ctx); err != nil {
		t.Fatalf("init writer catalog: %v", err)
	}
	reader = New(kv, store, "test")
	if err := reader.Init(ctx); err != nil {
		t.Fatalf("init reader catalog: %v", err)
	}
	return writer, reader
}

func fileEntry(path string, rows int64) FileEntry {
	return FileEntry{Path: path, SizeBytes: 1024, NumRows: rows, CreatedAt: time.Now().UTC()}
}

func manifestFileCount(t *testing.T, c *Catalog, table string) int {
	t.Helper()
	m, err := c.GetManifest(context.Background(), table)
	if err != nil {
		t.Fatalf("get manifest %s: %v", table, err)
	}
	n := 0
	for _, p := range m.Partitions {
		n += len(p.Files)
	}
	return n
}

func TestManifestReadSeesAnotherInstancesWriteImmediately(t *testing.T) {
	ctx := context.Background()
	writer, reader := twoCatalogsOverOneKV(t)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt64},
		{Name: "c1", Type: parquet.TypeString},
	}}
	if err := writer.CreateTable(ctx, "tp6", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := writer.AddFiles(ctx, "tp6", map[string]string{}, "tables/tp6/",
		[]FileEntry{fileEntry("tables/tp6/chunk_a.parquet", 1)}); err != nil {
		t.Fatalf("add first file: %v", err)
	}

	// The first read is what used to freeze the reader's view.
	if got := manifestFileCount(t, reader, "tp6"); got != 1 {
		t.Fatalf("first read: %d files, want 1", got)
	}

	if err := writer.AddFiles(ctx, "tp6", map[string]string{}, "tables/tp6/",
		[]FileEntry{fileEntry("tables/tp6/chunk_b.parquet", 1)}); err != nil {
		t.Fatalf("add second file: %v", err)
	}
	if got := manifestFileCount(t, reader, "tp6"); got != 2 {
		t.Fatalf("read after a second instance's write: %d files, want 2", got)
	}

	// Delete markers are manifest state too: merge-on-read DELETE reported
	// success and stayed invisible for the same reason (#483's opening repro).
	if err := writer.AddDeleteMarkers(ctx, "tp6", []DeleteMarker{{
		FilePath: "tables/tp6/chunk_a.parquet", RowIndices: []int64{0}, CreatedAt: time.Now().UTC(),
	}}); err != nil {
		t.Fatalf("add delete markers: %v", err)
	}
	m, err := reader.GetManifest(ctx, "tp6")
	if err != nil {
		t.Fatalf("get manifest after delete markers: %v", err)
	}
	if len(m.DeleteMarkers) != 1 {
		t.Fatalf("delete markers visible to the reader: %d, want 1", len(m.DeleteMarkers))
	}
}

func TestManifestReadDoesNotSurviveDropAndRecreate(t *testing.T) {
	ctx := context.Background()
	writer, reader := twoCatalogsOverOneKV(t)

	first := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt64},
		{Name: "c1", Type: parquet.TypeString},
	}}
	if err := writer.CreateTable(ctx, "repro2", first, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := writer.AddFiles(ctx, "repro2", map[string]string{}, "tables/repro2/",
		[]FileEntry{fileEntry("tables/repro2/chunk_old.parquet", 1)}); err != nil {
		t.Fatalf("add file: %v", err)
	}
	if got := manifestFileCount(t, reader, "repro2"); got != 1 {
		t.Fatalf("first incarnation: %d files, want 1", got)
	}

	if err := writer.DropTable(ctx, "repro2"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	// A dropped table has no manifest — not the dropped one's.
	if _, err := reader.GetManifest(ctx, "repro2"); err == nil {
		t.Fatal("manifest for a dropped table still resolves")
	}

	// Same name, DIFFERENT column type: the previous incarnation's file
	// stores c1 as STRING and would be refused at decode time if it leaked
	// into this incarnation's scan. Recreation must not carry any of it over.
	second := parquet.Schema{Columns: []parquet.Column{
		{Name: "c0", Type: parquet.TypeInt64},
		{Name: "c1", Type: parquet.TypeInt64},
	}}
	if err := writer.CreateTable(ctx, "repro2", second, nil); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if got := manifestFileCount(t, reader, "repro2"); got != 0 {
		t.Fatalf("recreated table: %d files, want 0 (the old incarnation's files leaked)", got)
	}
	if err := writer.AddFiles(ctx, "repro2", map[string]string{}, "tables/repro2/",
		[]FileEntry{fileEntry("tables/repro2/chunk_new.parquet", 1)}); err != nil {
		t.Fatalf("add file to recreated table: %v", err)
	}
	m, err := reader.GetManifest(ctx, "repro2")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	var paths []string
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			paths = append(paths, f.Path)
		}
	}
	if len(paths) != 1 || paths[0] != "tables/repro2/chunk_new.parquet" {
		t.Fatalf("recreated table's files = %v, want [tables/repro2/chunk_new.parquet]", paths)
	}
}

// The other half of the contract: the cache still has to BE a cache. The
// planner calls GetManifest several times per table per query (scan
// estimation, column annotation, metadata count/min-max, dynamic filters,
// the scan source's Init), and at SF100 a manifest is megabytes of JSON —
// re-decoding it per call is what the memo exists to avoid. While the
// revision holds, every call must hand back the same decoded value; the
// first call after a write must not.
func TestManifestDecodeIsMemoizedWithinARevision(t *testing.T) {
	ctx := context.Background()
	writer, reader := twoCatalogsOverOneKV(t)

	schema := parquet.Schema{Columns: []parquet.Column{{Name: "c0", Type: parquet.TypeInt64}}}
	if err := writer.CreateTable(ctx, "memo", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := writer.AddFiles(ctx, "memo", map[string]string{}, "tables/memo/",
		[]FileEntry{fileEntry("tables/memo/chunk_a.parquet", 1)}); err != nil {
		t.Fatalf("add file: %v", err)
	}

	first, err := reader.GetManifest(ctx, "memo")
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	again, err := reader.GetManifest(ctx, "memo")
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if first != again {
		t.Fatal("a repeated read at the same revision re-decoded the manifest")
	}

	if err := writer.AddFiles(ctx, "memo", map[string]string{}, "tables/memo/",
		[]FileEntry{fileEntry("tables/memo/chunk_b.parquet", 1)}); err != nil {
		t.Fatalf("add second file: %v", err)
	}
	after, err := reader.GetManifest(ctx, "memo")
	if err != nil {
		t.Fatalf("get after write: %v", err)
	}
	if after == first {
		t.Fatal("a read after a write handed back the previous revision's manifest")
	}
}

// statMax reads the c0 max out of an aggregate. Manifest stats round-trip
// through JSON, so an int64 comes back as a float64.
func statMax(t *testing.T, agg map[string]TableColumnStats) float64 {
	t.Helper()
	switch v := agg["c0"].MaxValue.(type) {
	case float64:
		return v
	case int64:
		return float64(v)
	default:
		t.Fatalf("c0 max has type %T, want a number", agg["c0"].MaxValue)
		return 0
	}
}

// The derived caches key on the manifest's KV revision for the same reason
// the manifest itself does: they used to key on UpdatedAt + file count, a
// content proxy computed from the reader's OWN cached manifest — so a second
// instance's write left both the manifest and everything derived from it
// frozen together.
func TestDerivedStatsCachesFollowTheManifestRevision(t *testing.T) {
	ctx := context.Background()
	writer, reader := twoCatalogsOverOneKV(t)

	schema := parquet.Schema{Columns: []parquet.Column{{Name: "c0", Type: parquet.TypeInt64}}}
	if err := writer.CreateTable(ctx, "stats", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	withStats := func(path string, minV, maxV int64) FileEntry {
		f := fileEntry(path, 10)
		f.ColumnStats = map[string]FileColumnStats{
			"c0": {MinValue: minV, MaxValue: maxV},
		}
		return f
	}
	if err := writer.AddFiles(ctx, "stats", map[string]string{}, "tables/stats/",
		[]FileEntry{withStats("tables/stats/chunk_a.parquet", 1, 10)}); err != nil {
		t.Fatalf("add file: %v", err)
	}

	agg, err := reader.AggregateColumnStats(ctx, "stats")
	if err != nil {
		t.Fatalf("aggregate stats: %v", err)
	}
	if got := statMax(t, agg); got != 10 {
		t.Fatalf("first aggregate: max = %v, want 10", got)
	}

	if err := writer.AddFiles(ctx, "stats", map[string]string{}, "tables/stats/",
		[]FileEntry{withStats("tables/stats/chunk_b.parquet", 11, 99)}); err != nil {
		t.Fatalf("add second file: %v", err)
	}
	agg, err = reader.AggregateColumnStats(ctx, "stats")
	if err != nil {
		t.Fatalf("aggregate stats after write: %v", err)
	}
	if got := statMax(t, agg); got != 99 {
		t.Fatalf("aggregate after a second instance's write: max = %v, want 99", got)
	}
}
