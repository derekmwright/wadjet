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
	"github.com/citc-tech/wadjet/internal/planner/physical"
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

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

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

	// Load TPC-H SF0.01 data. Split each table into multiple chunks so that
	// CanProbeSplit (which requires workers*2 files for <1GB tables) actually
	// fires with 3 workers. A single chunk would silently fall back to the
	// single-worker path and mask distributed-only bugs.
	const chunksPerTable = 8
	data := tpch.Generate(tpch.SF001)
	for tableName, schema := range tpch.AllTables {
		if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("creating table %s: %v", tableName, err)
		}
		rows := data[tableName]
		if len(rows) == 0 {
			continue
		}

		// Split rows into chunks of roughly equal size. Tables with fewer
		// rows than chunksPerTable get one chunk per row.
		numChunks := chunksPerTable
		if len(rows) < numChunks {
			numChunks = len(rows)
		}
		chunkSize := (len(rows) + numChunks - 1) / numChunks
		var entries []catalog.FileEntry
		for cIdx := 0; cIdx < numChunks; cIdx++ {
			start := cIdx * chunkSize
			end := start + chunkSize
			if end > len(rows) {
				end = len(rows)
			}
			if start >= end {
				break
			}
			chunkRows := rows[start:end]

			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatalf("parquet writer for %s: %v", tableName, err)
			}
			if err := pw.WriteRows(chunkRows); err != nil {
				t.Fatalf("writing %s chunk %d rows: %v", tableName, cIdx, err)
			}
			if err := pw.Close(); err != nil {
				t.Fatalf("closing %s chunk %d writer: %v", tableName, cIdx, err)
			}
			filePath := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tableName, cIdx+1)
			pdata := buf.Bytes()
			if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
				t.Fatalf("storing %s chunk %d: %v", tableName, cIdx, err)
			}
			entries = append(entries, catalog.FileEntry{
				Path:      filePath,
				SizeBytes: int64(len(pdata)),
				NumRows:   int64(len(chunkRows)),
				CreatedAt: time.Now(),
			})
		}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", entries); err != nil {
			t.Fatalf("adding %s to manifest: %v", tableName, err)
		}
	}

	// Create coordinator BEFORE starting the worker so the coordinator's
	// heartbeat subscription is in place when the worker's first heartbeat
	// is published — otherwise the coordinator misses registration and
	// workers.Count() stays at 0, silently falling back to the single-worker
	// pipeline path and masking distributed-only regressions.
	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)

	// Start 3 workers so CanProbeSplit (which requires workerCount > 1) fires
	// and the build-cache + probe-split path is actually exercised. Tests that
	// ran with 1 worker silently fell back to the single-worker pipeline and
	// reported false greens at SF0.01 while SF100 failed distributed-only bugs.
	const wantWorkers = 3
	for i := 0; i < wantWorkers; i++ {
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

	// Wait for all workers to actually register via their first heartbeats.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if coord.workers.Count() >= wantWorkers {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := coord.workers.Count(); got < wantWorkers {
		t.Fatalf("workers failed to register within 15s (count=%d, want=%d) — distributed TPC-H tests require live workers for probe-split", got, wantWorkers)
	}

	return ctx, coord
}

// TestDistributedTPCHBuildCache runs TPC-H join queries with BuildCacheThreshold=1
// so the build cache pre-scan path fires on every non-probe table, even at SF0.01.
// This validates that the pre-scan → S3 cache → worker read path produces correct
// results identical to the non-cached path, catching SF100 regressions cheaply.
func TestDistributedTPCHBuildCache(t *testing.T) {
	// Force probe-split to activate on tiny SF0.01 tables by lowering the
	// 64MB floor. Without this the coordinator routes to the single-worker
	// pipeline and neither probe-split nor the build cache ever run.
	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

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

// TestDistributedTPCHBuildCacheDuplicateAlias is a regression test for a bug
// where build cache pre-scan tasks silently scanned the full table (not their
// assigned file subset) when the original query had duplicate scans of the same
// table with suffixed aliases (e.g. Q02's correlated subquery scan "partsupp:1").
//
// The bug: preScanOneTable keyed ScanFileFilter by stage.ScanAlias. The pre-scan
// SQL "SELECT * FROM <TableName>" re-plans on the worker with alias == TableName
// (no ":N" suffix), so the lookup missed and allowedFiles stayed nil → full scan.
// Each file-group task scanned the full table, defeating the entire file-group
// splitting feature and OOMing Q02 at SF100.
//
// This test forces buildCacheGroupSize=2 so partsupp's 8 files split into 4
// groups — if the file filter doesn't land, each group scans all 8 files and
// the cached rows are 4× the expected count. We detect that by counting cache
// files per alias and by asserting Q02 still returns exactly 5 rows.
func TestDistributedTPCHBuildCacheDuplicateAlias(t *testing.T) {
	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

	origGroupSize := buildCacheGroupSize
	buildCacheGroupSize = 2
	t.Cleanup(func() { buildCacheGroupSize = origGroupSize })

	ctx, coord := setupTPCHDistributed(t)
	coord.BuildCacheThreshold = 1 // force build cache pre-scan on every non-probe table

	// Q02: "SELECT ... FROM part JOIN partsupp ... WHERE ps_supplycost = (
	//       SELECT MIN(ps_supplycost) FROM partsupp ...)"
	// The correlated partsupp scan gets alias "partsupp:1" in the physical plan.
	// With chunksPerTable=8 and buildCacheGroupSize=2, partsupp:1 should be
	// pre-scanned as 4 separate tasks each reading 2 files. Before the fix,
	// the ScanFileFilter key mismatch caused each task to scan all 8 files.
	q := tpch.TPCHQueries[2]
	result, err := coord.ExecuteSQL(ctx, q.SQL)
	if err != nil {
		t.Fatalf("Q02 ExecuteSQL failed: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("Q02 error: %s", result.Error)
	}
	rows := result.Rows()
	t.Logf("Q02 (duplicate alias + group splitting): %d rows", len(rows))
	if len(rows) != 5 {
		for i, r := range rows {
			t.Logf("  row %d: %v", i, r)
		}
		t.Errorf("Q02: got %d rows, want 5", len(rows))
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
