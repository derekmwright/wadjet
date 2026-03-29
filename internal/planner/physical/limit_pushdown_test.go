package physical

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// setupManyFiles creates a table with numFiles parquet files, each containing
// rowsPerFile rows, and returns the catalog.
func setupManyFiles(t *testing.T, tableName string, numFiles, rowsPerFile int) (*catalog.Catalog, objstore.Store) {
	t.Helper()
	ctx := context.Background()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "val", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < numFiles; i++ {
		rows := make([]map[string]any, rowsPerFile)
		for j := 0; j < rowsPerFile; j++ {
			rows[j] = map[string]any{
				"id":  int64(i*rowsPerFile + j),
				"val": fmt.Sprintf("v_%d_%d", i, j),
			}
		}

		var buf bytes.Buffer
		cfg := parquet.DefaultWriterConfig()
		cfg.Compression = parquet.CompressionNone
		w, err := parquet.NewWriter(&buf, schema, cfg)
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
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tableName, i)
		if _, err := store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), ""); err != nil {
			t.Fatal(err)
		}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
			Path:      path,
			SizeBytes: int64(len(data)),
			NumRows:   int64(rowsPerFile),
		}}); err != nil {
			t.Fatal(err)
		}
	}

	return cat, store
}

// TestLimitShortCircuit verifies that LIMIT N enables lazy file scanning
// and returns results without downloading all files. Regression test for #10.
func TestLimitShortCircuit(t *testing.T) {
	const numFiles = 50
	const rowsPerFile = 20
	ctx := context.Background()

	cat, _ := setupManyFiles(t, "sc_test", numFiles, rowsPerFile)
	planner := NewPlanner(cat)

	// Build: SELECT id, val FROM sc_test LIMIT 5
	scan := logical.NewScan("sc_test", "sc_test")
	limit := logical.NewLimit(scan, 5, 0)

	plan, err := planner.Plan(ctx, limit)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	// Verify LIMIT was pushed to the scan source
	cs, ok := plan.Pipeline.Source.(*catalogScanSource)
	if !ok {
		t.Fatalf("expected catalogScanSource, got %T", plan.Pipeline.Source)
	}
	if cs.rowLimit != 5 {
		t.Errorf("expected rowLimit=5 on scan source, got %d", cs.rowLimit)
	}

	// Run the pipeline
	if err := plan.Pipeline.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	plan.Pipeline.Close()

	// Verify result row count
	sink, ok := plan.Pipeline.Sink.(*exec.CollectSink)
	if !ok {
		t.Fatalf("expected CollectSink, got %T", plan.Pipeline.Sink)
	}
	if len(sink.Rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(sink.Rows))
	}

	// Verify lazy scanning: scanned rows should be far less than total.
	// With lazy file-level workers, only a few files are read before the
	// pipeline cancels (LIMIT satisfied → context cancelled → workers stop).
	ses := cs.inner.(*scannerExecSource)
	scanned := atomic.LoadInt64(&ses.scanner.rowsScanned)
	totalRows := int64(numFiles * rowsPerFile)
	t.Logf("scanned %d / %d total rows (%.0f%%)", scanned, totalRows, float64(scanned)/float64(totalRows)*100)

	// Under the race detector, cancellation propagates more slowly so more
	// file workers complete before LIMIT kills them. Use 75% as a generous
	// threshold — the key invariant is that we don't scan ALL files.
	if scanned >= totalRows*3/4 {
		t.Errorf("LIMIT short-circuit failed: scanned %d of %d rows (%.0f%%), expected <75%%", scanned, totalRows, float64(scanned)/float64(totalRows)*100)
	}
}

// TestLimitNotPushedThroughSort verifies that LIMIT over ORDER BY does NOT
// push the limit to the scan source (sort needs all rows).
func TestLimitNotPushedThroughSort(t *testing.T) {
	ctx := context.Background()
	cat, _ := setupManyFiles(t, "sort_test", 10, 10)
	planner := NewPlanner(cat)

	// Build: SELECT * FROM sort_test ORDER BY id LIMIT 5
	scan := logical.NewScan("sort_test", "sort_test")
	sort := logical.NewSort(scan, []logical.OrderExpr{{Column: "id"}})
	limit := logical.NewLimit(sort, 5, 0)

	plan, err := planner.Plan(ctx, limit)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	plan.Pipeline.Close()

	// TopN optimization should NOT push limit to scan source —
	// it still needs to read all data to sort.
	// The source type changes with TopN (sortSourceAdapter), so the
	// catalogScanSource assertion should fail.
	if cs, ok := plan.Pipeline.Source.(*catalogScanSource); ok {
		if cs.rowLimit != 0 {
			t.Errorf("LIMIT should not be pushed through ORDER BY, but rowLimit=%d", cs.rowLimit)
		}
	}
}
