package physical

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// scanCacheFixture builds a MemStore catalog with one 3-column file so
// two consumers with different RequiredColumns can share a cache.
func scanCacheFixture(t *testing.T, rows int) *catalog.Catalog {
	t.Helper()
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "id2", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}}
	rowMaps := make([]map[string]any, rows)
	for i := range rowMaps {
		rowMaps[i] = map[string]any{"id": int64(i), "id2": int64(i + 10), "val": "v"}
	}
	data := writeTestParquetMultiRG(t, schema, rowMaps)

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "items", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/items/chunk_001.parquet"
	if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	entry := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: int64(rows), CreatedAt: time.Now()}
	if err := cat.AddFiles(ctx, "items", map[string]string{}, "tables/items/", []catalog.FileEntry{entry}); err != nil {
		t.Fatal(err)
	}
	return cat
}

func drainSource(t *testing.T, src *catalogScanSource) []*batch.RecordBatch {
	t.Helper()
	ctx := context.Background()
	if err := src.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	var out []*batch.RecordBatch
	for {
		b, err := src.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		out = append(out, b)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return out
}

// TestScanCachePerConsumerProjection: the cache stores the UNION of all
// consumers' columns, but each consumer must receive only its OWN
// columns — a hash-join build side stores its input batches verbatim,
// so leaking the union into consumers multiplies build memory and spill
// bytes (the Q21 semi/anti churn). Also verifies the cache does NOT
// reserve its (consumer-shared) vectors on the memory tracker — the
// 2026-07-06 double-charge stalled SF10 Q21 6× — and that
// releaseScanCache drops the batches.
func TestScanCachePerConsumerProjection(t *testing.T) {
	cat := scanCacheFixture(t, 100)
	tracker := memory.NewTracker("scan-cache-test", 1<<30)
	cache := &scanCached{unionCols: []string{"id", "id2"}}

	// Consumer A populates: scans the union, receives only "id".
	a := &catalogScanSource{
		catalog:      cat,
		tableName:    "items",
		requiredCols: []string{"id"},
		cache:        cache,
		memTracker:   tracker,
	}
	batchesA := drainSource(t, a)
	if len(batchesA) == 0 {
		t.Fatal("consumer A got no batches")
	}
	var rowsA int
	for _, b := range batchesA {
		if len(b.Schema) != 1 || b.Schema[0].Name != "id" {
			t.Fatalf("consumer A schema = %v, want [id]", b.Schema)
		}
		rowsA += b.Len
	}
	if rowsA != 100 {
		t.Fatalf("consumer A rows = %d, want 100", rowsA)
	}

	// The cache itself holds the union. Its vectors are shared with
	// consumers (builds charge them via hashBuildBytes), so the cache
	// must NOT add a second charge — the ledger would exceed RSS and
	// stall every append in relief waits.
	if len(cache.batches) == 0 {
		t.Fatal("cache not populated")
	}
	for _, cb := range cache.batches {
		if len(cb.Schema) != 2 {
			t.Fatalf("cache schema = %v, want union [id id2]", cb.Schema)
		}
	}
	if used := tracker.Used(); used != 0 {
		t.Fatalf("cache double-charged %d bytes to the tracker; shared vectors must not be re-reserved", used)
	}

	// Consumer B replays: receives only "id2", values intact.
	b := &catalogScanSource{
		catalog:      cat,
		tableName:    "items",
		requiredCols: []string{"id2"},
		cache:        cache,
		memTracker:   tracker,
	}
	batchesB := drainSource(t, b)
	var rowsB int
	for _, rb := range batchesB {
		if len(rb.Schema) != 1 || rb.Schema[0].Name != "id2" {
			t.Fatalf("consumer B schema = %v, want [id2]", rb.Schema)
		}
		for i := 0; i < rb.Len; i++ {
			if got := rb.Columns[0].Int64Data[i]; got < 10 {
				t.Fatalf("consumer B saw id2=%d < 10 — wrong column data", got)
			}
		}
		rowsB += rb.Len
	}
	if rowsB != 100 {
		t.Fatalf("consumer B rows = %d, want 100", rowsB)
	}

	// releaseScanCache drops the cached batches and the map.
	p := &Planner{scanCache: map[string]*scanCached{"items": cache}}
	p.releaseScanCache()
	if cache.batches != nil {
		t.Fatal("cached batches not dropped by releaseScanCache")
	}
	if p.scanCache != nil {
		t.Fatal("scanCache not cleared")
	}
}

// TestMergeDuplicateScansKeepsConsumerColumns: the union goes on the
// cache entry; the scan NODES keep their own RequiredColumns (the old
// behavior rewrote every node to the union).
func TestMergeDuplicateScansKeepsConsumerColumns(t *testing.T) {
	newTestScanNode := func(table string, cols []string) *logical.Node {
		return &logical.Node{Type: logical.NodeScan, TableName: table, RequiredColumns: cols}
	}
	newTestJoinNode := func(l, r *logical.Node) *logical.Node {
		return &logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{l, r}}
	}

	p := &Planner{}
	nodeA := newTestScanNode("items", []string{"id"})
	nodeB := newTestScanNode("items", []string{"id2"})
	root := newTestJoinNode(nodeA, nodeB)

	p.mergeDuplicateScans(root)

	if got := nodeA.RequiredColumns; len(got) != 1 || got[0] != "id" {
		t.Fatalf("nodeA columns rewritten to %v, want [id]", got)
	}
	if got := nodeB.RequiredColumns; len(got) != 1 || got[0] != "id2" {
		t.Fatalf("nodeB columns rewritten to %v, want [id2]", got)
	}
	entry := p.scanCache["items"]
	if entry == nil {
		t.Fatal("no cache entry created")
	}
	if len(entry.unionCols) != 2 {
		t.Fatalf("unionCols = %v, want [id id2]", entry.unionCols)
	}

	// A SELECT-* consumer degrades the union to full schema (nil).
	p2 := &Planner{}
	root2 := newTestJoinNode(
		newTestScanNode("items", []string{"id"}),
		newTestScanNode("items", nil))
	p2.mergeDuplicateScans(root2)
	if entry := p2.scanCache["items"]; entry == nil || entry.unionCols != nil {
		t.Fatalf("unionCols = %v with a SELECT-* consumer, want nil (full schema)", entry.unionCols)
	}
}
