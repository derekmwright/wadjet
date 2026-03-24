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

// TestShuffleCorrectness runs TPC-H queries with 3 workers (shuffle stages enabled)
// against SF0.01 data. This reproduces the SF10 bug where shuffle-based joins
// return 0 rows for Q02, Q08 and possibly others.
func TestShuffleCorrectness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

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

	// Start 3 real workers to execute tasks
	for i := 0; i < 3; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: 4,
			CacheBytes:    64 * 1024 * 1024,
		}, store, nc, js, logger)
		if err := w.Start(ctx); err != nil {
			t.Fatalf("starting worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
	}

	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)

	// Inject fake heartbeats so coordinator sees 3 workers immediately
	// (real workers are running but their first heartbeat fires after 10s)
	for i := 0; i < 3; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{
			WorkerID:  fmt.Sprintf("fake-worker-%d", i),
			Timestamp: time.Now(),
		})
	}

	t.Logf("Worker count: %d", coord.workers.Count())
	if coord.workers.Count() < 3 {
		t.Fatalf("expected >= 3 workers, got %d", coord.workers.Count())
	}

	// Expected row counts from standalone TPC-H SF0.01 (from distributed_tpch_test.go)
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
		{18, 0},
		{19, 1},
		{20, 3},
		{21, 1},
		{22, 7},
	}

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
			t.Logf("Q%02d: %d rows in %v", tt.qNum, len(rows), elapsed)
			if len(rows) > 0 {
				t.Logf("  first row: %v", rows[0])
			}
			t.Logf("  plan:\n%s", result.Plan)
			if len(rows) != tt.expected {
				t.Errorf("Q%02d: got %d rows, want %d", tt.qNum, len(rows), tt.expected)
			}
		})
	}
}
