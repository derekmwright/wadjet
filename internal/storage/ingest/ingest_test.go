package ingest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/google/uuid"
)

var testSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "name", Type: parquet.TypeString},
	{Name: "year", Type: parquet.TypeString},
}}

const testBucket = "test-bucket"
const testTable = "events"

func setupCatalog(t *testing.T) (*catalog.Catalog, objstore.Store) {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, testTable, testSchema, []string{"year"}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return cat, store
}

func TestNew(t *testing.T) {
	cat, _ := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	if ing == nil {
		t.Fatal("expected non-nil ingester")
	}
	if ing.tableName != testTable {
		t.Fatalf("expected table %q, got %q", testTable, ing.tableName)
	}
	if ing.buffers == nil {
		t.Fatal("expected buffers map to be initialized")
	}
}

func TestIngestAndFlushAll(t *testing.T) {
	cat, store := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "year": "2025"},
		{"id": int64(2), "name": "bob", "year": "2025"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Verify parquet files were written to the store.
	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	parquetCount := 0
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetCount++
			if obj.Size == 0 {
				t.Errorf("parquet file %s has zero size", obj.Key)
			}
		}
	}
	if parquetCount == 0 {
		t.Fatal("expected at least one parquet file in the store")
	}
}

// TestFlushBufferChunkIDIsFullUUID is a #494 regression: chunk IDs used to
// be an 8-char (32-bit) prefix of a UUID, giving a ~0.6% chance of a
// birthday collision at 100k files in one table — and mergeFileEntries
// REPLACES a colliding manifest entry rather than erroring, so a collision
// silently dropped a whole file's rows. The chunk ID must now be a full,
// unpruned UUID string.
func TestFlushBufferChunkIDIsFullUUID(t *testing.T) {
	cat, store := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "year": "2025"},
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	found := false
	for _, obj := range objects {
		base := obj.Key
		if idx := strings.LastIndex(base, "/"); idx >= 0 {
			base = base[idx+1:]
		}
		if !strings.HasPrefix(base, "chunk_") || !strings.HasSuffix(base, ".parquet") {
			continue
		}
		found = true
		id := strings.TrimSuffix(strings.TrimPrefix(base, "chunk_"), ".parquet")
		if len(id) != 36 {
			t.Fatalf("chunk id %q is %d chars, want a full 36-char UUID (not a truncated prefix)", id, len(id))
		}
		if _, err := uuid.Parse(id); err != nil {
			t.Fatalf("chunk id %q does not parse as a UUID: %v", id, err)
		}
	}
	if !found {
		t.Fatal("expected at least one chunk_*.parquet file")
	}
}

func TestIngestPartitionDistribution(t *testing.T) {
	cat, store := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "year": "2024"},
		{"id": int64(2), "name": "bob", "year": "2025"},
		{"id": int64(3), "name": "carol", "year": "2024"},
		{"id": int64(4), "name": "dave", "year": "2026"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Each distinct year value should produce a separate parquet file under its partition path.
	years := map[string]bool{"2024": false, "2025": false, "2026": false}

	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/" + testTable + "/"})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		for y := range years {
			if strings.Contains(obj.Key, "year="+y) {
				years[y] = true
			}
		}
	}

	for y, found := range years {
		if !found {
			t.Errorf("expected parquet file for partition year=%s", y)
		}
	}
}

func TestFlushAllUpdatesManifest(t *testing.T) {
	cat, _ := setupCatalog(t)
	ing := New(cat, testTable, testSchema, []string{"year"}, DefaultConfig())
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "year": "2025"},
		{"id": int64(2), "name": "bob", "year": "2025"},
		{"id": int64(3), "name": "carol", "year": "2026"},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	manifest, err := cat.GetManifest(ctx, testTable)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	if len(manifest.Partitions) != 2 {
		t.Fatalf("expected 2 partitions in manifest, got %d", len(manifest.Partitions))
	}

	totalFiles := 0
	var totalRows int64
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
		for _, f := range p.Files {
			totalRows += f.NumRows
			if f.SizeBytes == 0 {
				t.Errorf("file %s has zero size in manifest", f.Path)
			}
			if f.Path == "" {
				t.Error("file entry has empty path")
			}
		}
	}

	if totalFiles != 2 {
		t.Fatalf("expected 2 file entries in manifest, got %d", totalFiles)
	}
	if totalRows != 3 {
		t.Fatalf("expected 3 total rows across manifest entries, got %d", totalRows)
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxBufferSize != 128*1024*1024 {
		t.Errorf("expected MaxBufferSize 128 MB, got %d", cfg.MaxBufferSize)
	}
	if cfg.MaxBufferRows != 1_000_000 {
		t.Errorf("expected MaxBufferRows 1M, got %d", cfg.MaxBufferRows)
	}
	if cfg.FlushInterval != 60*time.Second {
		t.Errorf("expected FlushInterval 60s, got %v", cfg.FlushInterval)
	}
	if cfg.RowGroupSize != 128*1024 {
		t.Errorf("expected RowGroupSize 128K, got %d", cfg.RowGroupSize)
	}
}

func TestIngestValidation_MissingRequiredColumn(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "strict", schema, []string{"year"})

	ing := New(cat, "strict", schema, []string{"year"}, DefaultConfig())

	// Missing required column "id"
	err := ing.Ingest(ctx, []map[string]any{
		{"name": "alice", "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for missing NOT NULL column")
	}
	if !strings.Contains(err.Error(), "missing required column") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIngestValidation_NullInNotNullColumn(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "strict2", schema, []string{"year"})

	ing := New(cat, "strict2", schema, []string{"year"}, DefaultConfig())

	// Null value for NOT NULL column
	err := ing.Ingest(ctx, []map[string]any{
		{"id": nil, "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for null in NOT NULL column")
	}
	if !strings.Contains(err.Error(), "cannot be null") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIngestValidation_WrongType(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "typed", schema, []string{"year"})

	ing := New(cat, "typed", schema, []string{"year"}, DefaultConfig())

	// Wrong type: string where int64 expected
	err := ing.Ingest(ctx, []map[string]any{
		{"id": "not-a-number", "name": "alice", "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	if !strings.Contains(err.Error(), "expected integer") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIngestValidation_NullableColumnMissing(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "nullable", schema, []string{"year"})

	ing := New(cat, "nullable", schema, []string{"year"}, DefaultConfig())

	// Missing nullable column "name" should be OK
	err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "year": "2025"},
	})
	if err != nil {
		t.Fatalf("unexpected error for missing nullable column: %v", err)
	}
}

func TestIngestDatePartitionPath(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "day", Type: parquet.TypeDate, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "dated", schema, []string{"day"})

	ing := New(cat, "dated", schema, []string{"day"}, DefaultConfig())

	rows := []map[string]any{
		{"id": int64(1), "day": time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC)},
		{"id": int64(2), "day": time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/dated/"})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	found := map[string]bool{"day=2026-03-18": false, "day=2026-03-19": false}
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		// Must not contain spaces or colons from time.Time.String()
		if strings.Contains(obj.Key, " ") || strings.Contains(obj.Key, ":") {
			t.Errorf("partition path contains invalid characters: %s", obj.Key)
		}
		for k := range found {
			if strings.Contains(obj.Key, k) {
				found[k] = true
			}
		}
	}
	for k, v := range found {
		if !v {
			t.Errorf("expected partition path containing %q", k)
		}
	}
}

func TestIngestTimestampPartitionPath(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "timestamped", schema, []string{"ts"})

	ing := New(cat, "timestamped", schema, []string{"ts"}, DefaultConfig())

	rows := []map[string]any{
		{"id": int64(1), "ts": time.Date(2026, 3, 18, 14, 30, 0, 0, time.UTC)},
	}

	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	objects, err := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/timestamped/"})
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}

	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		if strings.Contains(obj.Key, " ") {
			t.Errorf("timestamp partition path contains spaces: %s", obj.Key)
		}
		if !strings.Contains(obj.Key, "ts=2026-03-18T14") {
			t.Errorf("expected RFC3339 formatted timestamp in path, got: %s", obj.Key)
		}
	}
}

func TestIngestValidation_NetworkTypes(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	_ = cat.Init(ctx)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "src_ip", Type: parquet.TypeIPv4, Nullable: false},
		{Name: "dst_ip", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "year", Type: parquet.TypeString, Nullable: false},
	}}
	_ = cat.CreateTable(ctx, "network", schema, []string{"year"})

	ing := New(cat, "network", schema, []string{"year"}, DefaultConfig())

	// Valid: string values for network types
	err := ing.Ingest(ctx, []map[string]any{
		{"src_ip": "192.168.1.1", "dst_ip": "::1", "mac": "aa:bb:cc:dd:ee:ff", "year": "2025"},
	})
	if err != nil {
		t.Fatalf("unexpected error for valid network types: %v", err)
	}

	// Invalid: integer where string expected for IPV4
	err = ing.Ingest(ctx, []map[string]any{
		{"src_ip": 12345, "year": "2025"},
	})
	if err == nil {
		t.Fatal("expected error for integer in IPV4 column")
	}
	if !strings.Contains(err.Error(), "expected string") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFlushExtractsColumnStats(t *testing.T) {
	cat, _ := setupCatalog(t)
	cfg := DefaultConfig()
	ing := New(cat, testTable, testSchema, nil, cfg)
	ctx := context.Background()

	rows := []map[string]any{
		{"id": int64(10), "name": "alice", "year": "2025"},
		{"id": int64(20), "name": "bob", "year": "2025"},
		{"id": int64(5), "name": "zara", "year": "2026"},
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	manifest, err := cat.GetManifest(ctx, testTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) == 0 || len(manifest.Partitions[0].Files) == 0 {
		t.Fatal("expected at least one file in manifest")
	}

	fe := manifest.Partitions[0].Files[0]
	if fe.ColumnStats == nil {
		t.Fatal("expected ColumnStats to be populated after flush")
	}

	idStats, ok := fe.ColumnStats["id"]
	if !ok {
		t.Fatal("expected 'id' column in ColumnStats")
	}
	// Parquet stats report int64 min/max
	if idStats.MinValue == nil || idStats.MaxValue == nil {
		t.Fatal("expected non-nil min/max for 'id'")
	}
}

func TestMinFlushRows_SkipsTinyBuffers(t *testing.T) {
	cat, store := setupCatalog(t)
	cfg := DefaultConfig()
	cfg.MinFlushRows = 50 // require 50 rows before timer flush
	ing := New(cat, testTable, testSchema, []string{"year"}, cfg)
	ctx := context.Background()

	// Ingest 5 rows — below MinFlushRows threshold
	rows := make([]map[string]any, 5)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "name": "x", "year": "2025"}
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}

	// flushReady (timer path) should NOT flush — buffer too small
	if err := ing.flushReady(ctx); err != nil {
		t.Fatal(err)
	}

	objects, _ := store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/"})
	if len(objects) > 0 {
		t.Errorf("flushReady should not flush %d rows (min %d), but wrote %d files",
			5, cfg.MinFlushRows, len(objects))
	}

	// FlushAll (explicit) should always flush
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	objects, _ = store.List(ctx, testBucket, objstore.ListOptions{Prefix: "tables/"})
	parquetCount := 0
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetCount++
		}
	}
	if parquetCount != 1 {
		t.Errorf("FlushAll should flush regardless of MinFlushRows, got %d files", parquetCount)
	}
}

// dateTestSchema is a DATE-carrying table partitioned by a plain string key,
// used by the #560 ingest regression test below.
var dateTestSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "d", Type: parquet.TypeDate},
	{Name: "part", Type: parquet.TypeString},
}}

func setupDateCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "dated", dateTestSchema, []string{"part"}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return cat
}

// TestIngestRejectsInvalidDate pins #560's ingest half: an unparseable or
// nonexistent calendar date must be REJECTED at the ingest boundary, not
// silently written as day 0 (1970-01-01) — silent data corruption, since the
// original text is gone once written. A valid date must still ingest.
func TestIngestRejectsInvalidDate(t *testing.T) {
	cat := setupDateCatalog(t)
	ing := New(cat, "dated", dateTestSchema, []string{"part"}, DefaultConfig())
	ctx := context.Background()

	invalid := []string{"not-a-date", "2026-02-30", "2026-13-01", "2026-02-29", "5/6/7", "2026-01-32"}
	for _, lit := range invalid {
		t.Run("reject/"+lit, func(t *testing.T) {
			rows := []map[string]any{{"id": int64(1), "d": lit, "part": "p"}}
			if err := ing.Ingest(ctx, rows); err == nil {
				t.Fatalf("Ingest(d=%q) = nil, want an error — an invalid date must not be stored as the epoch", lit)
			}
		})
	}

	// A valid date must still ingest cleanly, including the PostgreSQL-valid
	// non-canonical spellings the widened accept-set now takes (#560).
	for _, lit := range []string{"2026-03-15", "2026-3-5", "2026/03/15", "20260315"} {
		rows := []map[string]any{{"id": int64(2), "d": lit, "part": "p"}}
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("Ingest(d=%q) = %v, want a valid date to ingest", lit, err)
		}
	}
}

// TestIngestRejectsInvalidDateNestedInRow pins the container half of #560: a
// DATE nested in a ROW must be validated at the ingest boundary too, not slip
// past the top-level checks and reach the native writer's leaf as the epoch.
func TestIngestRejectsInvalidDateNestedInRow(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, testBucket)
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "dt", Type: parquet.TypeDate, Nullable: true},
		}},
		{Name: "part", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "nested_dated", schema, []string{"part"}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	ing := New(cat, "nested_dated", schema, []string{"part"}, DefaultConfig())

	bad := []map[string]any{{"id": int64(1), "rw": map[string]any{"dt": "2026-02-30"}, "part": "p"}}
	if err := ing.Ingest(ctx, bad); err == nil {
		t.Fatal("Ingest of a ROW with an invalid nested DATE = nil, want an error (not a silently-stored epoch)")
	}

	good := []map[string]any{{"id": int64(2), "rw": map[string]any{"dt": "2026-03-15"}, "part": "p"}}
	if err := ing.Ingest(ctx, good); err != nil {
		t.Fatalf("Ingest of a ROW with a valid nested DATE = %v, want success", err)
	}
}
