package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// TestStreamingExchangeShuffleCorrectness runs shuffle-heavy TPC-H shapes
// through a 3-worker cluster with --streaming-exchange semantics enabled
// (Config.StreamingExchange + per-worker peer servers) and asserts (a) row
// counts identical to the S3-only path (the same expectations
// TestShuffleCorrectness pins) and (b) that the peer tier actually served
// reads — guarding against a silently-dead accelerator that would make (a)
// vacuous.
func TestStreamingExchangeShuffleCorrectness(t *testing.T) {
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

	// 3 workers, each with a spill dir (required for LocalStageCache — the
	// thing peers serve from) and a peer-exchange listener on a test port.
	workers := make([]*worker.Worker, 3)
	for i := range workers {
		w := worker.New(worker.Config{
			WorkerID:       fmt.Sprintf("peer-worker-%d", i),
			NATSUrl:        embeddedNATS.ClientURL(),
			MaxConcurrent:  4,
			CacheBytes:     64 * 1024 * 1024,
			SpillDir:       t.TempDir(),
			PeerListenAddr: "127.0.0.1:0",
		}, store, nc, js, logger)
		// Workers must outlive the query ctx (see TestShuffleCorrectness).
		workerCtx, workerCancel := context.WithCancel(context.Background())
		t.Cleanup(workerCancel)
		if err := w.Start(workerCtx); err != nil {
			t.Fatalf("starting worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
		workers[i] = w
	}

	coord := New(Config{
		NATSUrl:           embeddedNATS.ClientURL(),
		ResultBucket:      "test",
		StreamingExchange: true,
	}, cat, nc, js, logger)

	// Bootstrap heartbeats with the REAL worker IDs and peer addresses so
	// the annotator can resolve worker → address before the first real
	// heartbeat fires (~10s in); real heartbeats then refresh the same
	// entries, PeerAddr included.
	for i, w := range workers {
		if w.PeerExchangeAddr() == "" {
			t.Fatal("worker advertises no peer address despite PeerListenAddr")
		}
		coord.workers.record(distributed.WorkerHeartbeat{
			WorkerID:  fmt.Sprintf("peer-worker-%d", i),
			PeerAddr:  w.PeerExchangeAddr(),
			Timestamp: time.Now(),
		})
	}

	// Shuffle-heavy subset: repartitioned joins (Q2, Q5, Q9), fused
	// scan-aggregate (Q1), sharded DISTINCT-ish grouping (Q16), semi-join
	// (Q18 empty result guards the zero-row path), Q21 broadcast chains.
	tests := []struct {
		qNum     int
		expected int
	}{
		{1, 6},
		{2, 5},
		{5, 5},
		{9, 150},
		{16, 293},
		{18, 0},
		{21, 1},
	}
	for _, tt := range tests {
		q := tpch.TPCHQueries[tt.qNum]
		t.Run(fmt.Sprintf("Q%02d_%s", tt.qNum, q.Name), func(t *testing.T) {
			result, err := coord.ExecuteSQL(ctx, q.SQL)
			if err != nil {
				t.Fatalf("Q%02d ExecuteSQL failed: %v", tt.qNum, err)
			}
			if result.Error != "" {
				t.Fatalf("Q%02d error: %s", tt.qNum, result.Error)
			}
			rows := mustRows(t, result)
			if len(rows) != tt.expected {
				t.Errorf("Q%02d: got %d rows, want %d", tt.qNum, len(rows), tt.expected)
			}
		})
	}

	var hits, falls, uploaded, cancelled, failed int64
	for _, w := range workers {
		h, f := w.PeerFetchStats()
		hits += h
		falls += f
		up, ca, fa := w.UploadStats()
		uploaded += up
		cancelled += ca
		failed += fa
	}
	t.Logf("peer tier: %d hits, %d fallthroughs; async uploads: %d landed, %d cancelled, %d failed",
		hits, falls, uploaded, cancelled, failed)
	if hits == 0 {
		t.Error("peer tier never served a read — the accelerator is dead and row parity above is vacuous")
	}
	// Phase B: stage/shuffle outputs upload in the background. Zero total
	// means AsyncUpload never reached the workers — the deferred-durability
	// path is dead and the row parity above proved only Phase A.
	if uploaded+cancelled == 0 {
		t.Error("no background uploads ran — Phase-B async path is dead")
	}
	if failed != 0 {
		t.Errorf("%d background uploads abandoned", failed)
	}
}
