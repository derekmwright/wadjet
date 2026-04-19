//go:build !race

// The tests in this file spin up an embedded NATS server with multiple
// workers exercising the JetStream API concurrently (CreateOrUpdateConsumer,
// Fetch, Ack from N workers against the same stream). nats-server v2.12.6
// has a known upstream data race in jsAccount.reservedStorage /
// configUpdateCheck / stream.updateWithAdvisory that fires under exactly
// this access pattern. The race is benign for correctness but trips the
// pre-push race detector and is not something we can fix in this repo
// short of upgrading or patching nats-server.
//
// Skipping this file under -race keeps the race-detector run clean for
// our own code while preserving full distributed-mode coverage in normal
// (-race=off) test runs. When nats-server is upgraded past the fix,
// remove this build tag.

package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
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
	//
	// Workers are started SEQUENTIALLY (each Start completes before the next
	// begins) to avoid concurrent CreateOrUpdateConsumer calls on the embedded
	// JetStream, which trigger an upstream nats-server race in
	// jsAccount.reservedStorage / configUpdateCheck (nats-server v2.12.6).
	// Sequential startup is fine for tests; production workers run in
	// separate processes with their own NATS connections.
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
		// Brief gap to let JetStream stream/consumer state quiesce before the
		// next worker mutates it.
		time.Sleep(50 * time.Millisecond)
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

// polarsSchemas mirrors the actual TPC-H schema used by the Polars-exported
// SF100 parquet dataset: INT64 for every *key column and DATE for every date
// column. The hardcoded benchmarks/tpch schemas use INT32 + STRING because
// the local datagen wrote that way; the Polars exporter doesn't. Using these
// schemas in a regression test reproduces the SF100 environment locally so
// we can hunt the Q05 0-rows bug without paying for an EC2 deploy each time.
var polarsSchemas = map[string]parquet.Schema{
	"region": {Columns: []parquet.Column{
		{Name: "r_regionkey", Type: parquet.TypeInt64},
		{Name: "r_name", Type: parquet.TypeString},
		{Name: "r_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"nation": {Columns: []parquet.Column{
		{Name: "n_nationkey", Type: parquet.TypeInt64},
		{Name: "n_name", Type: parquet.TypeString},
		{Name: "n_regionkey", Type: parquet.TypeInt64},
		{Name: "n_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"supplier": {Columns: []parquet.Column{
		{Name: "s_suppkey", Type: parquet.TypeInt64},
		{Name: "s_name", Type: parquet.TypeString},
		{Name: "s_address", Type: parquet.TypeString},
		{Name: "s_nationkey", Type: parquet.TypeInt64},
		{Name: "s_phone", Type: parquet.TypeString},
		{Name: "s_acctbal", Type: parquet.TypeFloat64},
		{Name: "s_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"part": {Columns: []parquet.Column{
		{Name: "p_partkey", Type: parquet.TypeInt64},
		{Name: "p_name", Type: parquet.TypeString},
		{Name: "p_mfgr", Type: parquet.TypeString},
		{Name: "p_brand", Type: parquet.TypeString},
		{Name: "p_type", Type: parquet.TypeString},
		{Name: "p_size", Type: parquet.TypeInt64},
		{Name: "p_container", Type: parquet.TypeString},
		{Name: "p_retailprice", Type: parquet.TypeFloat64},
		{Name: "p_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"partsupp": {Columns: []parquet.Column{
		{Name: "ps_partkey", Type: parquet.TypeInt64},
		{Name: "ps_suppkey", Type: parquet.TypeInt64},
		{Name: "ps_availqty", Type: parquet.TypeInt64},
		{Name: "ps_supplycost", Type: parquet.TypeFloat64},
		{Name: "ps_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"customer": {Columns: []parquet.Column{
		{Name: "c_custkey", Type: parquet.TypeInt64},
		{Name: "c_name", Type: parquet.TypeString},
		{Name: "c_address", Type: parquet.TypeString},
		{Name: "c_nationkey", Type: parquet.TypeInt64},
		{Name: "c_phone", Type: parquet.TypeString},
		{Name: "c_acctbal", Type: parquet.TypeFloat64},
		{Name: "c_mktsegment", Type: parquet.TypeString},
		{Name: "c_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"orders": {Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_custkey", Type: parquet.TypeInt64},
		{Name: "o_orderstatus", Type: parquet.TypeString},
		{Name: "o_totalprice", Type: parquet.TypeFloat64},
		{Name: "o_orderdate", Type: parquet.TypeDate},
		{Name: "o_orderpriority", Type: parquet.TypeString},
		{Name: "o_clerk", Type: parquet.TypeString},
		{Name: "o_shippriority", Type: parquet.TypeInt64},
		{Name: "o_comment", Type: parquet.TypeString, Nullable: true},
	}},
	"lineitem": {Columns: []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_suppkey", Type: parquet.TypeInt64},
		{Name: "l_linenumber", Type: parquet.TypeInt64},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
		{Name: "l_extendedprice", Type: parquet.TypeFloat64},
		{Name: "l_discount", Type: parquet.TypeFloat64},
		{Name: "l_tax", Type: parquet.TypeFloat64},
		{Name: "l_returnflag", Type: parquet.TypeString},
		{Name: "l_linestatus", Type: parquet.TypeString},
		{Name: "l_shipdate", Type: parquet.TypeDate},
		{Name: "l_commitdate", Type: parquet.TypeDate},
		{Name: "l_receiptdate", Type: parquet.TypeDate},
		{Name: "l_shipinstruct", Type: parquet.TypeString},
		{Name: "l_shipmode", Type: parquet.TypeString},
		{Name: "l_comment", Type: parquet.TypeString, Nullable: true},
	}},
}

// convertRowsToPolars upcasts every int32 value in each row to int64. The
// parquet writer's Date encoder accepts the original "YYYY-MM-DD" strings via
// parseDateForWrite, so date columns just stay as strings — the schema
// declares them TypeDate which triggers the conversion. Floats and other
// types are pass-through.
func convertRowsToPolars(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		nr := make(map[string]any, len(r))
		for k, v := range r {
			if iv, ok := v.(int32); ok {
				nr[k] = int64(iv)
			} else {
				nr[k] = v
			}
		}
		out[i] = nr
	}
	return out
}

// setupTPCHDistributedPolars is a near-clone of setupTPCHDistributed that
// registers the Polars-style schemas (INT64 + DATE) and writes the SF0.01
// data through them. This reproduces the SF100 environment exactly enough
// to hunt the Q05 0-rows regression locally.
func setupTPCHDistributedPolars(t *testing.T) (context.Context, *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

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

	const chunksPerTable = 8
	// SF0.3 generates ~450K orders rows and ~1.8M lineitem rows so each
	// cache chunk gets ~56K orders rows — many WSHF chunks per file,
	// exercising real chunk-boundary transitions while staying small enough
	// to run on a dev box without OOMing the embedded NATS test process.
	data := tpch.Generate(tpch.ScaleFactor(0.3))
	for tableName, schema := range polarsSchemas {
		if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("creating table %s: %v", tableName, err)
		}
		rows := convertRowsToPolars(data[tableName])
		if len(rows) == 0 {
			continue
		}
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

	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)

	// Workers MUST have a spillDir so the build-cache pre-scan path uses
	// shuffleStreamSink (write to spill file) and the read path uses
	// cachedFileStreamSource.openShuffleFile (mmap from spill). Without a
	// spill dir, the workers fall back to in-memory paths that don't exist
	// in production, masking SF100-only bugs in the mmap/streaming paths.
	const wantWorkers = 3
	for i := 0; i < wantWorkers; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: 4,
			CacheBytes:    64 * 1024 * 1024,
			SpillDir:      t.TempDir(),
			MemoryBudget:  256 * 1024 * 1024, // small budget so spill paths fire
		}, store, nc, js, logger)
		if err := w.Start(ctx); err != nil {
			t.Fatalf("starting worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
		time.Sleep(50 * time.Millisecond)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if coord.workers.Count() >= wantWorkers {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := coord.workers.Count(); got < wantWorkers {
		t.Fatalf("workers failed to register within 15s (count=%d, want=%d)", got, wantWorkers)
	}

	return ctx, coord
}

// TestDistributedTPCHBuildCacheSF100Sample runs Q05 against an actual sample
// of the SF100 Polars-exported parquet files, downloaded one file per table
// into /tmp/sf100-sample. This bypasses every difference between our local
// data generator and the SF100 environment — same parquet encoding, same
// schemas, same data, same distinct value distribution. If the SF100 Q05
// 0-rows bug is in any code path we exercise here, this test will reproduce
// it locally without an EC2 deploy.
//
// The test is skipped automatically when /tmp/sf100-sample is missing so it
// doesn't fail in CI; manually populated via:
//
//	mkdir -p /tmp/sf100-sample && cd /tmp/sf100-sample && \
//	  for t in region nation supplier customer part partsupp orders lineitem; do \
//	    aws s3 cp s3://wadjet-bench-sf100-use2/$t/0_0.parquet $t-0_0.parquet \
//	      --region us-east-2; done
func TestDistributedTPCHBuildCacheSF100Sample(t *testing.T) {
	const sampleDir = "/tmp/sf100-sample"
	if _, err := os.Stat(sampleDir); os.IsNotExist(err) {
		t.Skipf("SF100 sample dir %s missing — see test comment for setup", sampleDir)
	}

	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

	// At SF100, orders has ~150M rows so reverse-bloom (>50M build inner)
	// fires for the customer×orders join. With our 10M-row sample we'd miss
	// that path. Force it on for realism.
	origRev := physical.ReverseBloomInnerThreshold
	physical.ReverseBloomInnerThreshold = 1
	t.Cleanup(func() { physical.ReverseBloomInnerThreshold = origRev })

	// SF100 splits the orders source files into 9 cache files (groupSize=2,
	// 17 source files). With our local sample of 1 orders source file, the
	// default groupSize=2 produces 1 cache file. Force groupSize=1 so we
	// also get a multi-WSHF-cache-file pattern even with one source file.
	// Mostly cosmetic with one source file but matches the SF100 path.
	origGroupSize := buildCacheGroupSize
	buildCacheGroupSize = 1
	t.Cleanup(func() { buildCacheGroupSize = origGroupSize })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Standard distributed-test infra: NATS, store, catalog, 3 workers.
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog: %v", err)
	}

	// Per-table file lists. lineitem and orders use multiple files so probe-
	// split fires (needs >= workerCount*2 files to partition).
	tableFiles := map[string][]string{
		"region":   {"region-0_0.parquet"},
		"nation":   {"nation-0_0.parquet"},
		"supplier": {"supplier-0_0.parquet"},
		"customer": {"customer-0_0.parquet"},
		"part":     {"part-0_0.parquet"},
		"partsupp": {"partsupp-0_0.parquet"},
		"orders":   {"orders-0_0.parquet"},
		"lineitem": {"lineitem-0_0.parquet", "lineitem-0_1.parquet", "lineitem-0_2.parquet", "lineitem-0_3.parquet"},
	}

	// Load each table from its real Polars parquet file. Probe the schema
	// from the first file (same as tpch-bench --skip-load) so the catalog
	// knows the actual INT64/DATE types.
	for _, tbl := range []string{"region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"} {
		files := tableFiles[tbl]
		var schema parquet.Schema
		var entries []catalog.FileEntry
		for i, fname := range files {
			localPath := fmt.Sprintf("%s/%s", sampleDir, fname)
			fi, err := os.Stat(localPath)
			if err != nil {
				t.Fatalf("stat %s: %v", localPath, err)
			}
			f, err := os.Open(localPath)
			if err != nil {
				t.Fatalf("open %s: %v", localPath, err)
			}
			pr, err := parquet.NewReader(f, fi.Size())
			if err != nil {
				f.Close()
				t.Fatalf("parquet %s: %v", localPath, err)
			}
			if i == 0 {
				schema = pr.Schema()
			}
			numRows := pr.NumRows()
			f.Close()
			raw, err := os.ReadFile(localPath)
			if err != nil {
				t.Fatalf("read %s: %v", localPath, err)
			}
			storePath := fmt.Sprintf("tables/%s/%s", tbl, fname)
			if _, err := store.Put(ctx, "test", storePath, bytes.NewReader(raw), int64(len(raw)), "application/octet-stream"); err != nil {
				t.Fatalf("put %s: %v", localPath, err)
			}
			entries = append(entries, catalog.FileEntry{
				Path:      storePath,
				SizeBytes: fi.Size(),
				NumRows:   numRows,
				CreatedAt: time.Now(),
			})
		}
		if err := cat.CreateTable(ctx, tbl, schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl, err)
		}
		if err := cat.AddFiles(ctx, tbl, nil, "tables/"+tbl+"/", entries); err != nil {
			t.Fatalf("addfiles %s: %v", tbl, err)
		}
		var totalRows int64
		var totalBytes int64
		for _, e := range entries {
			totalRows += e.NumRows
			totalBytes += e.SizeBytes
		}
		t.Logf("loaded %s: %d rows %d bytes %d files", tbl, totalRows, totalBytes, len(entries))
	}

	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)
	// Customer (~195 MB) is below this; orders (~261 MB single file) is
	// above. Caches ONLY orders, mimicking SF100.
	coord.BuildCacheThreshold = 240 * 1024 * 1024

	// Heap profiling: capture a heap profile every second so we can see
	// where memory goes during Q05 execution. Profiles are written to the
	// test temp dir and printed on test exit.
	if os.Getenv("WADJET_HEAP_PROFILE") == "1" {
		profileDir := t.TempDir()
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			i := 0
			var ms runtime.MemStats
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					i++
					runtime.ReadMemStats(&ms)
					path := fmt.Sprintf("%s/heap-%03d-%dMB.pprof", profileDir, i, ms.HeapAlloc/(1024*1024))
					if f, err := os.Create(path); err == nil {
						_ = pprof.WriteHeapProfile(f)
						f.Close()
						t.Logf("heap profile @ heap=%dMB sys=%dMB → %s", ms.HeapAlloc/(1024*1024), ms.Sys/(1024*1024), path)
					}
				}
			}
		}()
		t.Cleanup(func() { close(stop) })
	}

	// 2 workers (not 3) so 2 probe files per worker fits in dev box memory.
	// Each worker hashes the full 10M-row orders cache; with 2 workers the
	// total build-side memory is bounded.
	const wantWorkers = 2
	for i := 0; i < wantWorkers; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: 2,
			CacheBytes:    256 * 1024 * 1024,
			SpillDir:      t.TempDir(),
			MemoryBudget:  2 * 1024 * 1024 * 1024, // 2 GB per worker
		}, store, nc, js, logger)
		if err := w.Start(ctx); err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
		time.Sleep(50 * time.Millisecond)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if coord.workers.Count() >= wantWorkers {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := coord.workers.Count(); got < wantWorkers {
		t.Fatalf("workers failed to register (got %d want %d)", got, wantWorkers)
	}

	q := tpch.TPCHQueries[5]
	result, err := coord.ExecuteSQL(ctx, q.SQL)
	if err != nil {
		t.Fatalf("Q05 ExecuteSQL: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("Q05 error: %s", result.Error)
	}
	rows := result.Rows()
	t.Logf("Q05 against SF100 sample: %d rows", len(rows))
	for i, r := range rows {
		t.Logf("  row %d: %v", i, r)
	}
	if len(rows) == 0 {
		t.Errorf("Q05 returned 0 rows — REPRODUCED the SF100 bug locally!")
	}
}

// sf100SampleCluster builds a 2-worker distributed cluster loaded with the
// local /tmp/sf100-sample parquet files. Extracted so Q12 and Q17 repros can
// each get a fresh process memory-wise (Q17's decorrelated subquery triggers
// a lineitem-wide build cache that dwarfs Q12's state — running them in the
// same process OOMs a dev box).
func sf100SampleCluster(t *testing.T, memoryBudget int64) (context.Context, *Coordinator) {
	return sf100SampleClusterN(t, memoryBudget, 2, 2)
}

// sf100SampleClusterN lets the caller pick worker count and max-concurrent-tasks
// per worker — used to isolate memory-pressure issues.
func sf100SampleClusterN(t *testing.T, memoryBudget int64, wantWorkers, maxConcurrent int) (context.Context, *Coordinator) {
	t.Helper()
	const sampleDir = "/tmp/sf100-sample"
	if _, err := os.Stat(sampleDir); os.IsNotExist(err) {
		t.Skipf("SF100 sample dir %s missing — see TestDistributedTPCHBuildCacheSF100Sample for setup", sampleDir)
	}

	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

	origRev := physical.ReverseBloomInnerThreshold
	physical.ReverseBloomInnerThreshold = 1
	t.Cleanup(func() { physical.ReverseBloomInnerThreshold = origRev })

	origGroupSize := buildCacheGroupSize
	buildCacheGroupSize = 1
	t.Cleanup(func() { buildCacheGroupSize = origGroupSize })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog: %v", err)
	}

	// Match the Q05 test: probe tables get multiple files so probe-split fires
	// (>= workerCount*2 files). Q12 probes lineitem; Q17 probes lineitem too.
	tableFiles := map[string][]string{
		"region":   {"region-0_0.parquet"},
		"nation":   {"nation-0_0.parquet"},
		"supplier": {"supplier-0_0.parquet"},
		"customer": {"customer-0_0.parquet"},
		"part":     {"part-0_0.parquet"},
		"partsupp": {"partsupp-0_0.parquet"},
		"orders":   {"orders-0_0.parquet"},
		"lineitem": {"lineitem-0_0.parquet", "lineitem-0_1.parquet", "lineitem-0_2.parquet", "lineitem-0_3.parquet"},
	}

	for _, tbl := range []string{"region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"} {
		files := tableFiles[tbl]
		var schema parquet.Schema
		var entries []catalog.FileEntry
		for i, fname := range files {
			localPath := fmt.Sprintf("%s/%s", sampleDir, fname)
			fi, err := os.Stat(localPath)
			if err != nil {
				t.Fatalf("stat %s: %v", localPath, err)
			}
			f, err := os.Open(localPath)
			if err != nil {
				t.Fatalf("open %s: %v", localPath, err)
			}
			pr, err := parquet.NewReader(f, fi.Size())
			if err != nil {
				f.Close()
				t.Fatalf("parquet %s: %v", localPath, err)
			}
			if i == 0 {
				schema = pr.Schema()
			}
			numRows := pr.NumRows()
			f.Close()
			raw, err := os.ReadFile(localPath)
			if err != nil {
				t.Fatalf("read %s: %v", localPath, err)
			}
			storePath := fmt.Sprintf("tables/%s/%s", tbl, fname)
			if _, err := store.Put(ctx, "test", storePath, bytes.NewReader(raw), int64(len(raw)), "application/octet-stream"); err != nil {
				t.Fatalf("put %s: %v", localPath, err)
			}
			entries = append(entries, catalog.FileEntry{
				Path:      storePath,
				SizeBytes: fi.Size(),
				NumRows:   numRows,
				CreatedAt: time.Now(),
			})
		}
		if err := cat.CreateTable(ctx, tbl, schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl, err)
		}
		if err := cat.AddFiles(ctx, tbl, nil, "tables/"+tbl+"/", entries); err != nil {
			t.Fatalf("addfiles %s: %v", tbl, err)
		}
		var totalRows int64
		var totalBytes int64
		for _, e := range entries {
			totalRows += e.NumRows
			totalBytes += e.SizeBytes
		}
		t.Logf("loaded %s: %d rows %d bytes %d files", tbl, totalRows, totalBytes, len(entries))
	}

	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)
	// Leave BuildCacheThreshold at production default (2 GB). Orders at
	// SF1-sample is 273 MB, well below — this matches production SF10, where
	// orders (~1.5 GB) also stays below the default threshold, so the cache
	// path does NOT fire. If we want to test the cache path explicitly, a
	// separate test should force it.

	for i := 0; i < wantWorkers; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: maxConcurrent,
			CacheBytes:    256 * 1024 * 1024,
			SpillDir:      t.TempDir(),
			MemoryBudget:  memoryBudget,
		}, store, nc, js, logger)
		if err := w.Start(ctx); err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
		time.Sleep(50 * time.Millisecond)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if coord.workers.Count() >= wantWorkers {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := coord.workers.Count(); got < wantWorkers {
		t.Fatalf("workers failed to register (got %d want %d)", got, wantWorkers)
	}
	return ctx, coord
}

// runSF100SampleQuery runs one TPC-H query against the sample cluster and
// logs wall time + up to 10 result rows. Used by the Q12/Q17 local repro
// tests to iterate on the distributed perf regression.
//
// Set WADJET_CPU_PROFILE=<path> to capture a CPU profile over just the
// ExecuteSQL call (coordinator + in-process workers all run in the test
// process, so one profile covers everything).
func runSF100SampleQuery(t *testing.T, ctx context.Context, coord *Coordinator, qNum int) {
	t.Helper()
	q := tpch.TPCHQueries[qNum]
	// Periodic memory log so OOM kills leave a breadcrumb trail — tells us
	// which phase blew up even when the test process dies before completion.
	// Also drop a heap profile every tick when WADJET_HEAP_PROFILE=<dir>.
	stopMem := make(chan struct{})
	heapDir := os.Getenv("WADJET_HEAP_PROFILE")
	go func() {
		tk := time.NewTicker(1 * time.Second)
		defer tk.Stop()
		var ms runtime.MemStats
		i := 0
		for {
			select {
			case <-stopMem:
				return
			case <-tk.C:
				i++
				runtime.ReadMemStats(&ms)
				t.Logf("[heap] alloc=%dMB sys=%dMB inUse=%dMB",
					ms.HeapAlloc/(1<<20), ms.Sys/(1<<20), ms.HeapInuse/(1<<20))
				if heapDir != "" {
					path := fmt.Sprintf("%s/q%02d-heap-%03d-%dMB.pprof", heapDir, qNum, i, ms.HeapAlloc/(1<<20))
					if f, err := os.Create(path); err == nil {
						_ = pprof.WriteHeapProfile(f)
						_ = f.Close()
					}
				}
			}
		}
	}()
	defer close(stopMem)
	if path := os.Getenv("WADJET_CPU_PROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			t.Fatalf("create cpu profile: %v", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			t.Fatalf("start cpu profile: %v", err)
		}
		defer pprof.StopCPUProfile()
		t.Logf("CPU profile → %s", path)
	}
	start := time.Now()
	result, err := coord.ExecuteSQL(ctx, q.SQL)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Q%02d ExecuteSQL: %v (elapsed %s)", qNum, err, elapsed)
	}
	if result.Error != "" {
		t.Fatalf("Q%02d error: %s (elapsed %s)", qNum, result.Error, elapsed)
	}
	rows := result.Rows()
	t.Logf("Q%02d (%s): %d rows in %s", qNum, q.Name, len(rows), elapsed)
	for i, r := range rows {
		if i >= 10 {
			t.Logf("  ... (%d more rows)", len(rows)-10)
			break
		}
		t.Logf("  row %d: %v", i, r)
	}
}

// TestDistributedTPCHQ12SF100Sample is the local repro for the Q12 SF10
// distributed perf regression (project_q12_q17_regression_2026-04-18).
// Historical baseline: ~2s. Current: ~60s.
func TestDistributedTPCHQ12SF100Sample(t *testing.T) {
	ctx, coord := sf100SampleCluster(t, 2*1024*1024*1024)
	runSF100SampleQuery(t, ctx, coord, 12)
}

// TestDistributedTPCHQ17SF100Sample is the local repro for the Q17 SF10
// distributed perf regression. Q17's correlated scalar subquery decorrelates
// into a per-worker GROUP BY l_partkey over the entire lineitem partition
// (~2M groups) on top of a 5M-row part hash join — so the worker budget
// has to cover both.
func TestDistributedTPCHQ17SF100Sample(t *testing.T) {
	// Dev-box OOM guard: this test loads 40M lineitem rows into an in-
	// process MemStore, runs an aggregate pre-compute that materializes
	// 5M groups on ONE worker, and a probe-split path over 2 workers.
	// Peak test-process heap is 12-17 GB; WSL / laptop dev environments
	// typically kill before completion. Unlike TestDistributedTPCHQ12SF100Sample,
	// Q17 has no smaller in-memory alternative because the inner aggregate
	// spans full lineitem. Run on a beefy workstation or EC2 gate.
	if os.Getenv("WADJET_HEAVY_TESTS") != "1" {
		t.Skip("skipping Q17 SF100-sample repro — set WADJET_HEAVY_TESTS=1 on a host with ≥24 GB RAM")
	}

	// Force the aggregate-shuffle pre-compute path on at this scale: the
	// SF1-sample inner lineitem scan is ~1.2 GB, roughly equal to the
	// production 1 GB aggregateShuffleThreshold. Lower to 1 byte to
	// guarantee detection fires regardless of file size variance.
	origAggShuffle := aggregateShuffleThreshold
	aggregateShuffleThreshold = 1
	t.Cleanup(func() { aggregateShuffleThreshold = origAggShuffle })

	// 2 workers × 1 concurrent task each: one probe-split task per worker
	// at a time. Without the pre-compute, Q17's inner aggregate scans full
	// lineitem per task and OOMs at ~17 GB total heap.
	ctx, coord := sf100SampleClusterN(t, 2*1024*1024*1024, 2, 1)
	runSF100SampleQuery(t, ctx, coord, 17)
}

// TestDistributedTPCHBuildCachePolarsQ05 is the local repro for the SF100
// Q05 0-rows bug. It uses Polars-style schemas (INT64 keys, DATE dates) so
// the streaming source / filter / hash join paths exercise the same types as
// the SF100 deploy. With orders forced through the build cache (threshold
// between orders and customer at SF0.01) and groupSize=1 to mimic the SF100
// multi-cache-file pattern, this test should reproduce 0 rows locally if the
// bug is in the parts of the system we exercise here.
func TestDistributedTPCHBuildCachePolarsQ05(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SF0.3 Polars repro test in -short mode (heavy: ~55s, ~500MB)")
	}
	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

	origGroupSize := buildCacheGroupSize
	buildCacheGroupSize = 1
	t.Cleanup(func() { buildCacheGroupSize = origGroupSize })

	// Force reverse-bloom on at SF0.3 scale. This optimization only fires
	// for build-side estimates over 50M (inner) / 10M (semi/anti) rows by
	// default; SF0.3 orders is ~450K rows, so without overrides this code
	// path is silently skipped locally and SF100-only bugs in the bloom
	// injection path can hide indefinitely. Setting both thresholds to 1
	// makes every inner-join build pass through reverse-bloom.
	origRev := physical.ReverseBloomInnerThreshold
	physical.ReverseBloomInnerThreshold = 1
	t.Cleanup(func() { physical.ReverseBloomInnerThreshold = origRev })

	ctx, coord := setupTPCHDistributedPolars(t)
	// Dump per-table estimated sizes so we can pick a threshold that caches
	// ONLY orders (mimicking SF100 where only orders > 2 GB).
	for _, tbl := range []string{"orders", "customer", "supplier", "nation", "region", "lineitem"} {
		mf, err := coord.catalog.GetManifest(ctx, tbl)
		if err != nil {
			t.Logf("manifest %s: %v", tbl, err)
			continue
		}
		var bytes int64
		var n int
		for _, p := range mf.Partitions {
			for _, f := range p.Files {
				bytes += f.SizeBytes
				n++
			}
		}
		t.Logf("table %s: %d bytes across %d files", tbl, bytes, n)
	}
	// Threshold lands between customer (~4.7 MB at SF0.3) and orders (~30 MB
	// at SF0.3) so only orders is cached, mimicking the SF100 setup where
	// the 2 GB default catches only the orders table.
	coord.BuildCacheThreshold = 8 * 1024 * 1024

	q := tpch.TPCHQueries[5]
	result, err := coord.ExecuteSQL(ctx, q.SQL)
	if err != nil {
		t.Fatalf("Q05 ExecuteSQL failed: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("Q05 error: %s", result.Error)
	}
	rows := result.Rows()
	t.Logf("Q05 (Polars schema, orders-only cache, groupSize=1): %d rows", len(rows))
	for i, r := range rows {
		t.Logf("  row %d: %v", i, r)
	}
	if len(rows) != 5 {
		t.Errorf("Q05: got %d rows, want 5 — this is the SF100 regression", len(rows))
	}
}

// TestDistributedTPCHQ17AggregateShuffle forces the aggregate-shuffle
// pre-compute path on Q17 at SF0.01 by lowering shuffleBuildThreshold.
// The inner lineitem scan at SF0.01 is ~30 KB — far below the production
// 4 GB gate — so without the override the new path never fires. This test
// proves the full chain works end-to-end:
//
//   1. PickAggregateShuffleCandidate fires on Q17's decorrelated plan.
//   2. Coordinator dispatches preComputeDerivedAggregate, which runs the
//      reconstructed GROUP BY l_partkey SQL on a worker and caches to S3.
//   3. Probe-split tasks carry the signatures; worker substitutes the
//      matching Aggregate subtree with a streaming source of the cache.
//   4. Q17 returns its 1 expected row with the correct avg_yearly value.
//
// If this test passes but the SF1-sample / SF10 run doesn't, the gap is
// scale-specific (e.g. pre-compute task memory) not structural.
func TestDistributedTPCHQ17AggregateShuffle(t *testing.T) {
	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

	// Lower the aggregate-shuffle threshold to 1 byte so SF0.01's tiny
	// inner lineitem scan (~3.7 MB) trips detection. Production uses 1 GB
	// which would never fire at SF0.01.
	origAggShuffle := aggregateShuffleThreshold
	aggregateShuffleThreshold = 1
	t.Cleanup(func() { aggregateShuffleThreshold = origAggShuffle })

	// Also lower the base-table shuffle threshold so the routing precedence
	// check (feat branch: aggregate-shuffle wins over base-table shuffle)
	// still exercises both sides.
	origShuffle := shuffleBuildThreshold
	shuffleBuildThreshold = 1
	t.Cleanup(func() { shuffleBuildThreshold = origShuffle })

	ctx, coord := setupTPCHDistributed(t)

	q := tpch.TPCHQueries[17]
	result, err := coord.ExecuteSQL(ctx, q.SQL)
	if err != nil {
		t.Fatalf("Q17 ExecuteSQL failed: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("Q17 error: %s", result.Error)
	}
	rows := result.Rows()
	t.Logf("Q17 (aggregate-shuffle path forced): %d rows", len(rows))
	for i, r := range rows {
		t.Logf("  row %d: %v", i, r)
	}
	if len(rows) != 1 {
		t.Errorf("Q17: got %d rows, want 1", len(rows))
	}
}

// TestDistributedTPCHBuildCachePartialOrders is a regression test for the
// SF100 Q05 0-rows bug. At SF100 only the orders table exceeds the default
// 2 GB build-cache threshold, so the cache fires for orders but not for the
// other build tables (customer/supplier/nation/region). Q05 came back with
// 0 rows in that configuration even though it returned the correct 5 rows
// when the cache was forced on every build table (BuildCacheThreshold=1) at
// SF0.01.
//
// This test reproduces the SF100 cache distribution at SF0.01 by setting a
// threshold that lands between the smallest "big" table (orders) and the
// largest "small" table (any of customer/supplier/nation/region), so only
// orders is cached. To also mimic SF100's multi-cache-file pattern (orders
// gets split into ~9 files at SF100 with groupSize=2), force buildCacheGroupSize
// to 1 so orders' source files split into one cache file each.
func TestDistributedTPCHBuildCachePartialOrders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-cache-file SF0.01 repro test in -short mode")
	}
	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

	origGroupSize := buildCacheGroupSize
	buildCacheGroupSize = 1
	t.Cleanup(func() { buildCacheGroupSize = origGroupSize })

	ctx, coord := setupTPCHDistributed(t)

	// Threshold lands between orders and customer at SF0.01.
	coord.BuildCacheThreshold = 30 * 1024

	q := tpch.TPCHQueries[5]
	result, err := coord.ExecuteSQL(ctx, q.SQL)
	if err != nil {
		t.Fatalf("Q05 ExecuteSQL failed: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("Q05 error: %s", result.Error)
	}
	rows := result.Rows()
	t.Logf("Q05 (orders-only cache, groupSize=1): %d rows", len(rows))
	for i, r := range rows {
		t.Logf("  row %d: %v", i, r)
	}
	if len(rows) != 5 {
		t.Errorf("Q05: got %d rows, want 5", len(rows))
	}
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

// TestDistributedTPCHForcedShuffle runs join-heavy TPC-H queries at SF0.01
// with shuffleBuildThreshold lowered to 1 byte, forcing the shuffle execution
// path. Validates correctness end-to-end: the planner picks the candidate,
// both shuffle stages dispatch and complete, partitioned shards are produced
// and consumed, and the merged result matches the expected row counts.
//
// This is the local correctness gate before any deploy of the shuffle change.
// It also captures peak heap per query so we can compare shuffle-on vs
// shuffle-off in T13.
func TestDistributedTPCHForcedShuffle(t *testing.T) {
	// Force probe-split to activate (so mergeInfo is non-nil and the shuffle
	// branch is reachable). Same trick as TestDistributedTPCHBuildCache.
	origMinBytes := physical.ProbeSplitMinBytes
	physical.ProbeSplitMinBytes = 1
	t.Cleanup(func() { physical.ProbeSplitMinBytes = origMinBytes })

	// Force shuffle path on every join-heavy query by lowering the threshold
	// to 1 byte.
	origShuffle := shuffleBuildThreshold
	shuffleBuildThreshold = 1
	t.Cleanup(func() { shuffleBuildThreshold = origShuffle })

	ctx, coord := setupTPCHDistributed(t)
	t.Logf("Worker count: %d (shuffleBuildThreshold=1)", coord.workers.Count())

	tests := []struct {
		qNum     int
		expected int
	}{
		{3, 10},  // shipping priority — orders+lineitem+customer (THE Q03 we care about at SF100)
		{5, 5},   // customer/orders/lineitem/supplier/nation/region
		{7, 4},   // supplier/lineitem/orders/customer/nation
		{9, 150}, // part/supplier/lineitem/partsupp/orders/nation
		{10, 20}, // customer/orders/lineitem/nation
		{12, 2},  // orders/lineitem
	}

	for _, tt := range tests {
		q := tpch.TPCHQueries[tt.qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", tt.qNum, q.Name), func(t *testing.T) {
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			start := time.Now()
			result, err := coord.ExecuteSQL(ctx, q.SQL)
			elapsed := time.Since(start)

			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)
			heapDelta := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
			peakHeap := memAfter.HeapSys

			if err != nil {
				t.Fatalf("Q%02d shuffle path: ExecuteSQL failed: %v", tt.qNum, err)
			}
			if result.Error != "" {
				t.Fatalf("Q%02d shuffle path: %s", tt.qNum, result.Error)
			}
			rows := result.Rows()
			t.Logf("Q%02d: %d rows in %v (shuffle ON, heap_delta=%d KB, heap_sys=%d KB)",
				tt.qNum, len(rows), elapsed, heapDelta/1024, peakHeap/1024)
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
