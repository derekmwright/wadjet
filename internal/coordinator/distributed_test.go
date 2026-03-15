package coordinator

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
	"github.com/derekmwright/caelum/internal/worker"
)

// setupDistributed creates an embedded NATS, coordinator, and worker for testing.
func setupDistributed(t *testing.T) (context.Context, *Coordinator, objstore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Start embedded NATS with temp storage dir
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1 // random port
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("creating JetStream: %v", err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setting up streams: %v", err)
	}

	// Create in-memory object store and catalog
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}

	cat := catalog.New(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	// Start worker
	w := worker.New(worker.Config{
		NATSUrl:       embeddedNATS.ClientURL(),
		MaxConcurrent: 4,
		CacheBytes:    64 * 1024 * 1024,
	}, store, nc, js, logger)
	if err := w.Start(ctx); err != nil {
		t.Fatalf("starting worker: %v", err)
	}
	t.Cleanup(w.Stop)

	// Create coordinator
	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)

	return ctx, coord, store
}

// ingestTestData creates a table and writes test data.
func ingestTestData(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog, tableName string, schema []parquet.Column, rows []map[string]any) {
	t.Helper()

	// Create table
	if err := cat.CreateTable(ctx, tableName, parquet.Schema{Columns: schema}, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	// Write parquet file
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

	data := buf.Bytes()
	filePath := "tables/" + tableName + "/chunk_0001.parquet"
	if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	// Register in manifest
	if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
		Path:      filePath,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("adding file to manifest: %v", err)
	}
}

func TestDistributedScanQuery(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"user_id": "u1", "amount": 10.0},
		{"user_id": "u2", "amount": 20.0},
		{"user_id": "u1", "amount": 30.0},
	}
	ingestTestData(t, ctx, store, cat, "events", schema, rows)

	// Execute distributed scan via SubmitScanQuery
	result, err := coord.SubmitScanQuery(ctx, "events", nil, nil)
	if err != nil {
		t.Fatalf("SubmitScanQuery: %v", err)
	}
	if result.State != "completed" {
		t.Fatalf("expected completed, got %s (error: %s)", result.State, result.Error)
	}
	if result.TotalRows != 3 {
		t.Fatalf("expected 3 rows, got %d", result.TotalRows)
	}
}

func TestDistributedExecuteSQL(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"user_id": "u1", "amount": 10.0},
		{"user_id": "u2", "amount": 20.0},
		{"user_id": "u1", "amount": 30.0},
		{"user_id": "u2", "amount": 40.0},
	}
	ingestTestData(t, ctx, store, cat, "events", schema, rows)

	// Execute via SQL
	result, err := coord.ExecuteSQL(ctx, "SELECT * FROM events")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}
	if result.Plan == "" {
		t.Error("expected non-empty plan")
	}
	t.Logf("Plan:\n%s", result.Plan)
	t.Logf("Rows: %d, Elapsed: %s", len(result.Rows), result.Elapsed)
}

func TestDistributedAggregateQuery(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"user_id": "u1", "amount": 10.0},
		{"user_id": "u2", "amount": 20.0},
		{"user_id": "u1", "amount": 30.0},
		{"user_id": "u2", "amount": 40.0},
	}
	ingestTestData(t, ctx, store, cat, "events", schema, rows)

	result, err := coord.ExecuteSQL(ctx, "SELECT user_id, SUM(amount) as total FROM events GROUP BY user_id")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	t.Logf("Result files: %v", result.ResultFiles)
	t.Logf("Rows: %d, Columns: %v", len(result.Rows), result.Columns)
	for i, row := range result.Rows {
		t.Logf("  row[%d]: %v", i, row)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result.Rows))
	}

	// Verify aggregates
	totals := make(map[string]float64)
	for _, row := range result.Rows {
		uid := row["user_id"].(string)
		total, ok := row["total"].(float64)
		if !ok {
			// Try int64 (count produces int64)
			if iv, ok := row["total"].(int64); ok {
				total = float64(iv)
			}
		}
		totals[uid] = total
	}
	if totals["u1"] != 40.0 {
		t.Errorf("u1 total: got %f, want 40.0", totals["u1"])
	}
	if totals["u2"] != 60.0 {
		t.Errorf("u2 total: got %f, want 60.0", totals["u2"])
	}
	t.Logf("Plan:\n%s", result.Plan)
}
