package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// TestGatherBudgetSpill_E2E_RowIdentical is the end-to-end regression test
// for the streaming-SQLResult refactor: a distributed no-LIMIT query whose
// result exceeds Config.GatherResultBudget must (a) come back as a LAZY
// SQLResult (in-memory prefix + scratch replay), not a hard failure (the
// PR #142 interim behavior), and (b) yield exactly the same rows as the
// uncapped run.
func TestGatherBudgetSpill_E2E_RowIdentical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

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

	// One table, 20K rows ≈ several hundred KB decoded — far past the
	// 32 KiB budget used below, far under the uncapped default.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}}
	const numRows = 20000
	rows := make([]map[string]any, numRows)
	for i := range rows {
		rows[i] = map[string]any{"n": int64(i), "s": fmt.Sprintf("val-%06d", i)}
	}
	if err := cat.CreateTable(ctx, "nums", schema, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("parquet writer: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}
	pdata := buf.Bytes()
	const filePath = "tables/nums/chunk_0001.parquet"
	if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
		t.Fatalf("storing parquet: %v", err)
	}
	if err := cat.AddFiles(ctx, "nums", map[string]string{}, "tables/nums/", []catalog.FileEntry{{
		Path:      filePath,
		SizeBytes: int64(len(pdata)),
		NumRows:   numRows,
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("adding to manifest: %v", err)
	}

	w := worker.New(worker.Config{
		NATSUrl:       embeddedNATS.ClientURL(),
		MaxConcurrent: 4,
		CacheBytes:    64 * 1024 * 1024,
	}, store, nc, js, logger)
	// Workers must outlive the setup/query ctx: tying taskLoop to a
	// short-lived ctx silently killed task consumption mid-test once
	// the deadline passed (issue #143 — the -race "deadlock" was the
	// worker dying at setupDistributed\'s 30s timeout).
	workerCtx, workerCancel := context.WithCancel(context.Background())
	t.Cleanup(workerCancel)
	if err := w.Start(workerCtx); err != nil {
		t.Fatalf("starting worker: %v", err)
	}
	t.Cleanup(w.Stop)

	coord := New(Config{
		NATSUrl:            embeddedNATS.ClientURL(),
		ResultBucket:       "test",
		GatherResultBudget: -1, // uncapped baseline first
	}, cat, nc, js, logger)
	coord.workers.record(distributed.WorkerHeartbeat{WorkerID: "fake-worker-0", Timestamp: time.Now()})

	const sql = "SELECT n, s FROM nums"
	canon := func(rs []map[string]any) []string {
		out := make([]string, len(rs))
		for i, r := range rs {
			out[i] = fmt.Sprintf("%v|%v", r["n"], r["s"])
		}
		sort.Strings(out)
		return out
	}

	baseline, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("uncapped ExecuteSQL: %v", err)
	}
	if baseline.stream != nil {
		t.Fatal("uncapped result must be materialized, not lazy")
	}
	baseRows := canon(mustRows(t, baseline))
	if len(baseRows) != numRows {
		t.Fatalf("uncapped rows = %d, want %d", len(baseRows), numRows)
	}

	// Tiny budget: the same query must DEGRADE to the lazy spill-replay
	// form and still return every row.
	coord.config.GatherResultBudget = 32 * 1024
	capped, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("capped ExecuteSQL must degrade gracefully, got error: %v", err)
	}
	if capped.stream == nil {
		t.Fatal("capped result is not lazy — budget did not trigger spill")
	}
	if capped.Batches != nil {
		t.Fatal("lazy result must not also carry materialized Batches")
	}
	if capped.TotalRows != numRows {
		t.Fatalf("capped TotalRows = %d, want %d", capped.TotalRows, numRows)
	}
	cappedRows := canon(mustRows(t, capped))
	if len(cappedRows) != numRows {
		t.Fatalf("capped rows = %d, want %d", len(cappedRows), numRows)
	}
	for i := range baseRows {
		if baseRows[i] != cappedRows[i] {
			t.Fatalf("row %d differs: uncapped %q vs capped %q", i, baseRows[i], cappedRows[i])
		}
	}
}
