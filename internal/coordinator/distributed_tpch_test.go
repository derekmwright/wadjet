package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// setupTPCHDistributed creates an embedded NATS, coordinator, worker, and loads
// TPC-H SF0.01 data. Returns a ready-to-query coordinator and cleanup via t.Cleanup.
func setupTPCHDistributed(t *testing.T) (context.Context, *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Embedded NATS
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
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

	// In-memory object store + NATS KV-backed catalog (so pipeline tasks
	// can reconstruct the catalog from the same NATS KV)
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("creating NATS KV: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	// Load TPC-H SF0.01 data
	data := tpch.Generate(tpch.SF001)
	for tableName, schema := range tpch.AllTables {
		if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("creating table %s: %v", tableName, err)
		}
		rows := data[tableName]
		if len(rows) == 0 {
			continue
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatalf("parquet writer for %s: %v", tableName, err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatalf("writing %s rows: %v", tableName, err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("closing %s writer: %v", tableName, err)
		}
		filePath := fmt.Sprintf("tables/%s/chunk_0001.parquet", tableName)
		pdata := buf.Bytes()
		if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
			t.Fatalf("storing %s parquet: %v", tableName, err)
		}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
			Path:      filePath,
			SizeBytes: int64(len(pdata)),
			NumRows:   int64(len(rows)),
			CreatedAt: time.Now(),
		}}); err != nil {
			t.Fatalf("adding %s to manifest: %v", tableName, err)
		}
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

	// Wait briefly for worker heartbeat to register
	time.Sleep(200 * time.Millisecond)

	// Create coordinator
	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)

	return ctx, coord
}

// TestDistributedTPCHBuildCache runs TPC-H join queries with BuildCacheThreshold=1
// so the build cache pre-scan path fires on every non-probe table, even at SF0.01.
// This validates that the pre-scan → S3 cache → worker read path produces correct
// results identical to the non-cached path, catching SF100 regressions cheaply.
func TestDistributedTPCHBuildCache(t *testing.T) {
	ctx, coord := setupTPCHDistributed(t)

	// Set threshold to 1 byte so every build table triggers pre-scanning.
	coord.BuildCacheThreshold = 1

	// Queries with joins — these will exercise the build cache path.
	// Q02, Q05, Q07 have multi-table joins with build-side tables.
	tests := []struct {
		qNum     int
		expected int
	}{
		{2, 5},   // supplier/partsupp/part joins
		{5, 5},   // customer/orders/lineitem/supplier/nation/region
		{7, 4},   // supplier/lineitem/orders/customer/nation
		{9, 150}, // part/supplier/lineitem/partsupp/orders/nation
		{10, 20}, // customer/orders/lineitem/nation
		{12, 2},  // orders/lineitem
	}

	t.Logf("Worker count: %d (BuildCacheThreshold=1)", coord.workers.Count())

	for _, tt := range tests {
		q := tpch.TPCHQueries[tt.qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", tt.qNum, q.Name), func(t *testing.T) {
			start := time.Now()
			result, err := coord.ExecuteSQL(ctx, q.SQL)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("Q%02d ExecuteSQL failed (build cache path): %v", tt.qNum, err)
			}
			if result.Error != "" {
				t.Fatalf("Q%02d error: %s", tt.qNum, result.Error)
			}
			rows := result.Rows()
			t.Logf("Q%02d: %d rows in %v (build cache active)", tt.qNum, len(rows), elapsed)
			if len(rows) != tt.expected {
				t.Errorf("Q%02d: got %d rows, want %d", tt.qNum, len(rows), tt.expected)
			}
		})
	}
}

// TestDistributedTPCH runs TPC-H queries through the full distributed pipeline
// (coordinator → NATS → worker → S3 → merge) and validates row counts against
// the standalone expected results.
func TestDistributedTPCH(t *testing.T) {
	ctx, coord := setupTPCHDistributed(t)

	tests := []struct {
		qNum     int
		expected int
	}{
		{1, 6},
		{2, 5},
		{3, 10},
		{4, 5},
		{5, 5},
		{6, 1},
		{7, 4},
		{8, 2},
		{9, 150},
		{10, 20},
		{11, 235},
		{12, 2},
		{13, 100},
		{14, 1},
		{15, 1},
		{16, 293},
		{17, 1},
		{18, 0}, // 0 rows at SF0.01
		{19, 1},
		{20, 3},
		{21, 1},
		{22, 7},
	}

	t.Logf("Worker count: %d", coord.workers.Count())

	for _, tt := range tests {
		q := tpch.TPCHQueries[tt.qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", tt.qNum, q.Name), func(t *testing.T) {
			start := time.Now()
			result, err := coord.ExecuteSQL(ctx, q.SQL)
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("Q%02d ExecuteSQL failed: %v", tt.qNum, err)
			}
			if result.Error != "" {
				t.Fatalf("Q%02d error: %s", tt.qNum, result.Error)
			}
			rows := result.Rows()
			t.Logf("Q%02d: %d rows in %v (plan:\n%s)", tt.qNum, len(rows), elapsed, result.Plan)
			if len(rows) > 0 {
				t.Logf("  first row: %v", rows[0])
			}
			if len(rows) != tt.expected {
				t.Errorf("Q%02d: got %d rows, want %d", tt.qNum, len(rows), tt.expected)
			}
		})
	}
}
