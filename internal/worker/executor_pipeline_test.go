package worker

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"

	"io"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestExecutePipelineWithNATSKV exercises the exact code path used in distributed
// mode: coordinator writes table metadata to NATS KV, worker creates its own
// catalog from the same KV, re-parses SQL, builds a standalone plan, and executes.
//
// This test reproduces the SF10 distributed benchmark bug where all 22 queries
// return 0 rows because the worker's pipeline execution produces empty results.
func TestExecutePipelineWithNATSKV(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// 1. Start embedded NATS server (same as tpch-bench coordinator)
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Host = "127.0.0.1"
	natsCfg.Port = -1 // random port
	natsCfg.StoreDir = t.TempDir()
	embNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	defer embNATS.Shutdown()

	// 2. Create in-process connection (coordinator side) and JetStream
	nc, err := distributed.ConnectInProcess(embNATS.Server())
	if err != nil {
		t.Fatalf("connecting NATS: %v", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}

	// 3. Create MemStore with test data (simulates S3)
	bucket := "test-bench"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}

	// Write test Parquet files for two tables
	ordersRows := []map[string]any{
		{"o_orderkey": int64(1), "o_custkey": int64(100), "o_totalprice": 1500.50},
		{"o_orderkey": int64(2), "o_custkey": int64(200), "o_totalprice": 2300.75},
		{"o_orderkey": int64(3), "o_custkey": int64(100), "o_totalprice": 800.25},
	}
	writeParquetFile(t, store, bucket, "tables/orders/part-0.parquet", ordersRows)

	lineitemRows := []map[string]any{
		{"l_orderkey": int64(1), "l_partkey": int64(10), "l_quantity": 5.0, "l_extendedprice": 100.0},
		{"l_orderkey": int64(1), "l_partkey": int64(20), "l_quantity": 3.0, "l_extendedprice": 200.0},
		{"l_orderkey": int64(2), "l_partkey": int64(10), "l_quantity": 7.0, "l_extendedprice": 350.0},
		{"l_orderkey": int64(3), "l_partkey": int64(30), "l_quantity": 2.0, "l_extendedprice": 50.0},
	}
	writeParquetFile(t, store, bucket, "tables/lineitem/part-0.parquet", lineitemRows)

	// 4. Create NATS KV catalog (coordinator side) and register tables + files
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("NewNATSKV: %v", err)
	}
	coordCat := catalog.New(kv, store, bucket)
	if err := coordCat.Init(ctx); err != nil {
		t.Fatalf("catalog Init: %v", err)
	}

	// Register table schemas
	ordersSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "o_orderkey", Type: parquet.TypeInt64},
			{Name: "o_custkey", Type: parquet.TypeInt64},
			{Name: "o_totalprice", Type: parquet.TypeFloat64},
		},
	}
	lineitemSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "l_orderkey", Type: parquet.TypeInt64},
			{Name: "l_partkey", Type: parquet.TypeInt64},
			{Name: "l_quantity", Type: parquet.TypeFloat64},
			{Name: "l_extendedprice", Type: parquet.TypeFloat64},
		},
	}

	if err := coordCat.CreateTable(ctx, "orders", ordersSchema, nil); err != nil {
		t.Fatalf("create orders: %v", err)
	}
	if err := coordCat.CreateTable(ctx, "lineitem", lineitemSchema, nil); err != nil {
		t.Fatalf("create lineitem: %v", err)
	}

	// Add file entries to manifests (same as discoverData)
	// Use SizeBytes=0 to tell readAllSized to use io.ReadAll (unknown size)
	// In production, discoverData sets correct sizes from S3 listing.
	if err := coordCat.AddFiles(ctx, "orders", nil, "", []catalog.FileEntry{
		{Path: "tables/orders/part-0.parquet", SizeBytes: 0},
	}); err != nil {
		t.Fatalf("add orders files: %v", err)
	}
	if err := coordCat.AddFiles(ctx, "lineitem", nil, "", []catalog.FileEntry{
		{Path: "tables/lineitem/part-0.parquet", SizeBytes: 0},
	}); err != nil {
		t.Fatalf("add lineitem files: %v", err)
	}

	// 5. Verify catalog has files (coordinator side)
	manifest, err := coordCat.GetManifest(ctx, "orders")
	if err != nil {
		t.Fatalf("coord GetManifest orders: %v", err)
	}
	totalFiles := 0
	for _, p := range manifest.Partitions {
		totalFiles += len(p.Files)
	}
	t.Logf("Coordinator catalog has %d orders files", totalFiles)
	if totalFiles == 0 {
		t.Fatal("coordinator catalog has no orders files")
	}

	// 6. Create executor (worker side) with the SAME store but its OWN JetStream
	// In real deployment, the worker has its own NATS connection. Here we reuse
	// the same connection since it's the same embedded server.
	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, js)
	executor.logger = logger

	// 6b. Verify catalog from the worker's perspective
	workerKV, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("worker NewNATSKV: %v", err)
	}
	workerCat := catalog.New(workerKV, store, bucket)
	if err := workerCat.Init(ctx); err != nil {
		t.Fatalf("worker catalog Init: %v", err)
	}
	wManifest, err := workerCat.GetManifest(ctx, "orders")
	if err != nil {
		t.Fatalf("worker GetManifest orders: %v (THIS IS THE BUG — worker can't see catalog metadata)", err)
	}
	wFiles := 0
	for _, p := range wManifest.Partitions {
		wFiles += len(p.Files)
	}
	t.Logf("Worker catalog has %d orders files", wFiles)
	if wFiles == 0 {
		t.Fatal("worker catalog has no orders files — metadata not visible to worker")
	}

	// 6c. Test standalone execution (same catalog, same store)
	// This uses the same code path as standalone mode.
	{
		standaloneCat := catalog.New(workerKV, store, bucket)
		if err := standaloneCat.Init(ctx); err != nil {
			t.Fatalf("standalone catalog Init: %v", err)
		}

		// First, verify the manifest directly
		m, err := standaloneCat.GetManifest(ctx, "orders")
		if err != nil {
			t.Fatalf("standalone GetManifest: %v", err)
		}
		for pi, p := range m.Partitions {
			t.Logf("  partition[%d]: values=%v, files=%d", pi, p.Values, len(p.Files))
			for fi, f := range p.Files {
				t.Logf("    file[%d]: path=%s, size=%d", fi, f.Path, f.SizeBytes)
			}
		}

		// Verify the file is readable from the store
		rc, _, err := store.Get(ctx, bucket, "tables/orders/part-0.parquet")
		if err != nil {
			t.Fatalf("standalone store.Get: %v", err)
		}
		fileData, _ := io.ReadAll(rc)
		rc.Close()
		t.Logf("  file data size: %d bytes", len(fileData))

		// Verify GetTable works
		tableMeta, err := standaloneCat.GetTable(ctx, "orders")
		if err != nil {
			t.Fatalf("standalone GetTable: %v", err)
		}
		t.Logf("  table schema columns: %d", len(tableMeta.Schema.Columns))
		for _, c := range tableMeta.Schema.Columns {
			t.Logf("    col: %s type=%d", c.Name, c.Type)
		}

		plnr := physical.NewPlanner(standaloneCat)
		parsed, err := plansql.Parse("SELECT o_orderkey, o_custkey, o_totalprice FROM orders")
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		sel, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("ExtractSelect: %v", err)
		}
		logPlan, err := logical.BuildFromSelect(sel)
		if err != nil {
			t.Fatalf("BuildFromSelect: %v", err)
		}
		plnr.AnnotateScanColumns(ctx, logPlan)
		logPlan = logical.Optimize(logPlan, func(p *logical.Node) {
			plnr.AnnotateScanColumns(ctx, p)
		})
		t.Logf("Logical plan:\n%s", logPlan.PrettyPrint(0))

		physPlan, err := plnr.Plan(ctx, logPlan)
		if err != nil {
			t.Fatalf("standalone Plan: %v", err)
		}
		t.Logf("Pipeline source type: %T", physPlan.Pipeline.Source)
		t.Logf("Pipeline ops: %d", len(physPlan.Pipeline.Ops))
		t.Logf("Pipeline sink type: %T", physPlan.Pipeline.Sink)

		if err := physPlan.Pipeline.Source.Init(ctx); err != nil {
			t.Fatalf("source Init: %v", err)
		}
		// Try reading batches directly from source
		batchCount := 0
		totalSourceRows := int64(0)
		for {
			b, err := physPlan.Pipeline.Source.Next(ctx)
			if err != nil {
				t.Fatalf("source Next error: %v", err)
			}
			if b == nil {
				break
			}
			batchCount++
			totalSourceRows += int64(b.ActiveLen())
			t.Logf("  source batch %d: len=%d active=%d cols=%d", batchCount, b.Len, b.ActiveLen(), len(b.Columns))
		}
		t.Logf("Source produced %d batches, %d rows", batchCount, totalSourceRows)
		if totalSourceRows == 0 {
			t.Fatal("SOURCE produces 0 rows — bug is in the scanner")
		}
	}

	// 7. Execute a simple pipeline task (SELECT * FROM orders)
	t.Run("simple_select", func(t *testing.T) {
		task := distributed.Task{
			ID:           "pipe-1",
			QueryID:      "q1",
			StageID:      "pipeline-0",
			Type:         distributed.TaskTypePipeline,
			SQLText:      "SELECT o_orderkey, o_custkey, o_totalprice FROM orders",
			DataBucket:   bucket,
			ResultBucket: bucket,
			ResultPrefix: "queries/q1/pipeline-0/",
		}

		result := executor.Execute(ctx, task, "w1")
		if !result.Success {
			t.Fatalf("pipeline failed: %s", result.Error)
		}
		t.Logf("simple_select: %d rows (inline=%d bytes, path=%s)",
			result.NumRows, len(result.InlineData), result.ResultPath)
		if result.NumRows != 3 {
			t.Errorf("expected 3 rows, got %d", result.NumRows)
		}
	})

	// 8. Execute a join pipeline task (same pattern as TPC-H distributed)
	t.Run("join_query", func(t *testing.T) {
		task := distributed.Task{
			ID:           "pipe-2",
			QueryID:      "q2",
			StageID:      "pipeline-0",
			Type:         distributed.TaskTypePipeline,
			SQLText:      "SELECT o_orderkey, SUM(l_extendedprice) AS total FROM orders JOIN lineitem ON o_orderkey = l_orderkey GROUP BY o_orderkey ORDER BY total DESC",
			DataBucket:   bucket,
			ResultBucket: bucket,
			ResultPrefix: "queries/q2/pipeline-0/",
		}

		result := executor.Execute(ctx, task, "w1")
		if !result.Success {
			t.Fatalf("join pipeline failed: %s", result.Error)
		}
		t.Logf("join_query: %d rows (inline=%d bytes, path=%s)",
			result.NumRows, len(result.InlineData), result.ResultPath)
		if result.NumRows != 3 {
			t.Errorf("expected 3 rows, got %d", result.NumRows)
		}
	})

	// 9. Execute with ScanFileFilter (probe-split pattern)
	t.Run("probe_split", func(t *testing.T) {
		task := distributed.Task{
			ID:               "pipe-3",
			QueryID:          "q3",
			StageID:          "pipeline-0",
			Type:             distributed.TaskTypePipeline,
			SQLText:          "SELECT o_orderkey, SUM(l_extendedprice) AS total FROM orders JOIN lineitem ON o_orderkey = l_orderkey GROUP BY o_orderkey ORDER BY total DESC",
			DataBucket:       bucket,
			ResultBucket:     bucket,
			ResultPrefix:     "queries/q3/pipeline-0/",
			ScanFileFilter:   map[string][]string{"lineitem": {"tables/lineitem/part-0.parquet"}},
			PartialAggregate: true,
		}

		result := executor.Execute(ctx, task, "w1")
		if !result.Success {
			t.Fatalf("probe-split pipeline failed: %s", result.Error)
		}
		t.Logf("probe_split: %d rows (inline=%d bytes, path=%s)",
			result.NumRows, len(result.InlineData), result.ResultPath)
		if result.NumRows != 3 {
			t.Errorf("expected 3 rows, got %d", result.NumRows)
		}
	})
}

// TestExecutePipelineStaleSizeBytes verifies that the scanner produces correct
// results even when catalog SizeBytes is stale (smaller than actual file).
// Before the fix, readAllSized would truncate the file and buildRGUnits would
// silently skip the unreadable file, returning 0 rows with no error.
func TestExecutePipelineStaleSizeBytes(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Host = "127.0.0.1"
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	defer embNATS.Shutdown()

	nc, err := distributed.ConnectInProcess(embNATS.Server())
	if err != nil {
		t.Fatalf("connecting NATS: %v", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}

	bucket := "test-stale"
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}
	writeParquetFile(t, store, bucket, "tables/users/part-0.parquet", rows)

	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog.New(kv, store, bucket)
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	// Register with a STALE SizeBytes (100 bytes, actual is ~500+)
	if err := cat.AddFiles(ctx, "users", nil, "", []catalog.FileEntry{
		{Path: "tables/users/part-0.parquet", SizeBytes: 100},
	}); err != nil {
		t.Fatal(err)
	}

	cache := NewLRUCache(64 * 1024 * 1024)
	executor := NewExecutor(store, cache, js)
	executor.logger = logger

	task := distributed.Task{
		ID:           "pipe-stale",
		QueryID:      "q-stale",
		StageID:      "pipeline-0",
		Type:         distributed.TaskTypePipeline,
		SQLText:      "SELECT id, name FROM users",
		DataBucket:   bucket,
		ResultBucket: bucket,
		ResultPrefix: "queries/q-stale/pipeline-0/",
	}

	result := executor.Execute(ctx, task, "w1")
	if !result.Success {
		t.Fatalf("pipeline failed (should succeed with readAllSized fix): %s", result.Error)
	}
	if result.NumRows != 2 {
		t.Errorf("expected 2 rows with stale SizeBytes, got %d", result.NumRows)
	}
	t.Logf("stale SizeBytes: %d rows (correct despite size mismatch)", result.NumRows)
}

func writeParquetFile(t *testing.T, store objstore.Store, bucket, path string, rows []map[string]any) {
	t.Helper()
	schema := schemaFromRows(rows)
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := buf.Bytes()
	if _, err := store.Put(ctx, bucket, path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
}
