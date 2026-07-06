package catalog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// sketchCountingStore wraps a Store and counts Get calls to sketch blobs.
type sketchCountingStore struct {
	objstore.Store
	mu         sync.Mutex
	sketchGets int
}

func (s *sketchCountingStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	if strings.Contains(key, "sketches") {
		s.mu.Lock()
		s.sketchGets++
		s.mu.Unlock()
	}
	return s.Store.Get(ctx, bucket, key)
}

func (s *sketchCountingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sketchGets
}

// TestAggregateColumnStats_Memoized: computing table stats reads one
// sketches blob per file — on an object-store-backed catalog that was 600
// serial S3 GETs of PLANNING on every query at SF10 (2026-07-05: ~40s per
// query, the dominant cold-S3 standalone cost). The aggregate must be
// memoized per manifest version: repeat calls fetch nothing, and a
// manifest update (re-ANALYZE, ingest) invalidates the entry.
func TestAggregateColumnStats_Memoized(t *testing.T) {
	ctx := context.Background()
	mem := objstore.NewMemStore()
	if err := mem.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	store := &sketchCountingStore{Store: mem}
	cat := New(NewMemKV(), store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
	}}
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	const numFiles = 3
	const rowsPerFile = 1000
	for fi := 0; fi < numFiles; fi++ {
		rows := make([]map[string]any, rowsPerFile)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(fi*rowsPerFile + i)}
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("tables/events/chunk_%04d.parquet", fi)
		data := buf.Bytes()
		if _, err := mem.Put(ctx, "test", key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		entry := FileEntry{Path: key, SizeBytes: int64(len(data)), NumRows: rowsPerFile, CreatedAt: time.Now()}
		if err := cat.AddFiles(ctx, "events", nil, "tables/events/", []FileEntry{entry}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cat.AnalyzeTable(ctx, "events"); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}

	base := store.count()
	stats1, err := cat.AggregateColumnStats(ctx, "events")
	if err != nil {
		t.Fatalf("AggregateColumnStats #1: %v", err)
	}
	if stats1 == nil || stats1["id"].NDV == 0 {
		t.Fatal("first aggregate returned no NDV — fixture broken")
	}
	afterFirst := store.count()
	if afterFirst == base {
		t.Fatal("test setup: first aggregate fetched no sketch blobs — externalized path not exercised")
	}

	// Second call: memoized, zero sketch fetches.
	stats2, err := cat.AggregateColumnStats(ctx, "events")
	if err != nil {
		t.Fatalf("AggregateColumnStats #2: %v", err)
	}
	if got := store.count(); got != afterFirst {
		t.Fatalf("second aggregate fetched %d sketch blobs, want 0 (memoization broken)", got-afterFirst)
	}
	if stats2["id"].NDV != stats1["id"].NDV {
		t.Fatalf("memoized NDV %d != original %d", stats2["id"].NDV, stats1["id"].NDV)
	}

	// Manifest update (re-ANALYZE rewrites sketch keys and bumps
	// UpdatedAt) must invalidate the memoized entry.
	if _, err := cat.AnalyzeTable(ctx, "events"); err != nil {
		t.Fatalf("re-AnalyzeTable: %v", err)
	}
	preThird := store.count()
	if _, err := cat.AggregateColumnStats(ctx, "events"); err != nil {
		t.Fatalf("AggregateColumnStats #3: %v", err)
	}
	if got := store.count(); got == preThird {
		t.Fatal("aggregate after manifest update fetched no sketch blobs — stale cache served")
	}
}
