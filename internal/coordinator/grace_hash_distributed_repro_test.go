//go:build !race

package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// TestGraceHashJoinDistributedRepro reproduces the harness's
// micro_grace_hash_join 120s timeout in-process, with no pgx / external
// process / file storage in the loop. Same data shape:
//
//	micro_build:  500_000 rows of (build_key int64, build_val int64,
//	                              build_pad string ~80 bytes)
//	              build_key in [1, 50000], so 10 build rows per probe key
//	micro_probe:   50_000 rows of (probe_key int64, probe_val int64),
//	              probe_key = i+1 for i in [0, 49999]
//	query:        SELECT b.build_key, b.build_val, p.probe_val
//	              FROM micro_build b JOIN micro_probe p
//	                ON b.build_key = p.probe_key
//
// Standalone single-process completes in ~200 ms (see
// internal/harness/grace_hash_repro_test.go::TestGraceHashJoinReproStandalone).
// If the in-process distributed cluster hangs at the same point as the
// `cmd/tpch-harness --mode=local` run does, the bug is in coordinator /
// dispatch / shuffle / gather, not in the engine kernels. From there we
// can attach worker-side log output to triangulate the offending stage.
func TestGraceHashJoinDistributedRepro(t *testing.T) {
	if os.Getenv("WADJET_GRACE_HASH_REPRO") != "1" {
		t.Skip("set WADJET_GRACE_HASH_REPRO=1 to enable (heavy: 500K rows, parquet write)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	logLevel := slog.LevelWarn
	if os.Getenv("WADJET_GRACE_HASH_REPRO_VERBOSE") == "1" {
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

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
		t.Fatalf("nats connect: %v", err)
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
		t.Fatalf("catalog init: %v", err)
	}

	// Build micro_build and micro_probe synthetically (matching
	// internal/harness/micros.go::generateMicroData seed=42 shape).
	rng := rand.New(rand.NewSource(42))
	buildSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "build_key", Type: parquet.TypeInt64},
		{Name: "build_val", Type: parquet.TypeInt64},
		{Name: "build_pad", Type: parquet.TypeString},
	}}
	probeSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "probe_key", Type: parquet.TypeInt64},
		{Name: "probe_val", Type: parquet.TypeInt64},
	}}
	const pad = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	buildRows := make([]map[string]any, 500_000)
	for i := range buildRows {
		buildRows[i] = map[string]any{
			"build_key": int64(rng.Intn(50_000) + 1),
			"build_val": rng.Int63(),
			"build_pad": fmt.Sprintf("%s-%d", pad, i),
		}
	}
	probeRows := make([]map[string]any, 50_000)
	for i := range probeRows {
		probeRows[i] = map[string]any{
			"probe_key": int64(i + 1),
			"probe_val": rng.Int63(),
		}
	}
	writeTable(t, ctx, cat, store, "micro_build", buildSchema, buildRows)
	writeTable(t, ctx, cat, store, "micro_probe", probeSchema, probeRows)

	coord := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	const numWorkers = 2
	for i := 0; i < numWorkers; i++ {
		w := worker.New(worker.Config{
			NATSUrl: embeddedNATS.ClientURL(), MaxConcurrent: 4, CacheBytes: 64 * 1024 * 1024,
			SpillDir: t.TempDir(),
		}, store, nc, js, logger)
		// Workers must outlive the setup/query ctx: tying taskLoop to a
		// short-lived ctx silently killed task consumption mid-test once
		// the deadline passed (issue #143 — the -race "deadlock" was the
		// worker dying at setupDistributed\'s 30s timeout).
		workerCtx, workerCancel := context.WithCancel(context.Background())
		t.Cleanup(workerCancel)
		if err := w.Start(workerCtx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
		time.Sleep(50 * time.Millisecond)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if coord.workers.Count() >= numWorkers {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	const sql = `SELECT b.build_key, b.build_val, p.probe_val
FROM micro_build b JOIN micro_probe p ON b.build_key = p.probe_key`

	t0 := time.Now()
	res, err := coord.ExecuteSQL(ctx, sql)
	elapsed := time.Since(t0)
	if err != nil {
		t.Fatalf("ExecuteSQL after %v: %v", elapsed, err)
	}
	if res.Error != "" {
		t.Fatalf("query error after %v: %s", elapsed, res.Error)
	}
	t.Logf("distributed in-process: %d rows in %v", len(mustRows(t, res)), elapsed)
	if len(mustRows(t, res)) == 0 {
		t.Fatal("expected non-zero rows")
	}
}

// writeTable writes one parquet chunk per table and registers it with the catalog.
func writeTable(t *testing.T, ctx context.Context, cat *catalog.Catalog, store objstore.Store, name string, schema parquet.Schema, rows []map[string]any) {
	t.Helper()
	if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("parquet writer %s: %v", name, err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("write rows %s: %v", name, err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close %s: %v", name, err)
	}
	path := fmt.Sprintf("tables/%s/chunk_0001.parquet", name)
	data := buf.Bytes()
	if _, err := store.Put(ctx, "test", path, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("put %s: %v", name, err)
	}
	entries := []catalog.FileEntry{{
		Path:      path,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}
	if err := cat.AddFiles(ctx, name, map[string]string{}, "tables/"+name+"/", entries); err != nil {
		t.Fatalf("addfiles %s: %v", name, err)
	}
}
