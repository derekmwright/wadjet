package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// workerGateStore parks reads of table data that come from a WORKER TASK and
// releases them only when the task's context is cancelled (or the test lets
// them through). A worker task context is identified by the progress reporter
// the worker attaches to it (worker.go, exec.WithProgressReporter) — nothing
// else in the process sets one, so a parked read here is proof that a
// distributed stage task is executing, and a parked read that returns while
// the gate is still shut is proof that the task's context was cancelled.
type workerGateStore struct {
	objstore.Store
	held    atomic.Int32
	entered atomic.Int32
	release chan struct{}
}

func (g *workerGateStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	if strings.HasPrefix(key, "tables/") && strings.HasSuffix(key, ".parquet") &&
		exec.ProgressReporterFromContext(ctx) != nil {
		g.held.Add(1)
		g.entered.Add(1)
		defer g.held.Add(-1)
		select {
		case <-ctx.Done():
			return nil, objstore.ObjectInfo{}, ctx.Err()
		case <-g.release:
		}
	}
	return g.Store.Get(ctx, bucket, key)
}

func (g *workerGateStore) waitHeld(t *testing.T, n int32, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if g.held.Load() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no worker task reached the gate within %s (held=%d)", within, g.held.Load())
}

// TestDistributedQueryCancellationStopsStages is the distributed arm of query
// cancellation: cancelling the query context must stop the WORK, not just
// unblock the client.
//
// The coordinator honours ctx throughout executeStageDAG and, on the way out,
// broadcasts wadjet.cancel.<root> (cleanupQuery). Workers only act on that if
// they can match the broadcast's root id against a stage task's stage-scoped
// QueryID — the seam this test guards. Evidence that the stage stopped is the
// parked read returning while the gate is still shut: only the task context
// being cancelled can do that.
func TestDistributedQueryCancellationStopsStages(t *testing.T) {
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(setupCancel)

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
	if err := distributed.SetupStreams(setupCtx, js); err != nil {
		t.Fatalf("setting up streams: %v", err)
	}

	store := &workerGateStore{Store: objstore.NewMemStore(), release: make(chan struct{})}
	if err := store.MakeBucket(setupCtx, "test"); err != nil {
		t.Fatal(err)
	}
	// Let every parked read go at teardown, so a failed assertion cannot
	// leave worker goroutines wedged in Stop().
	t.Cleanup(func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	})

	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("creating NATS KV: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(setupCtx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	const table = "lineitem"
	schema := tpch.AllTables[table]
	rows := tpch.Generate(tpch.SF001)[table]
	if len(rows) == 0 {
		t.Fatal("no lineitem rows generated")
	}
	if err := cat.CreateTable(setupCtx, table, schema, nil); err != nil {
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
	filePath := fmt.Sprintf("tables/%s/chunk_0001.parquet", table)
	data := buf.Bytes()
	if _, err := store.Put(setupCtx, "test", filePath, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatalf("storing parquet: %v", err)
	}
	if err := cat.AddFiles(setupCtx, table, map[string]string{}, "tables/"+table+"/", []catalog.FileEntry{{
		Path:      filePath,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("adding to manifest: %v", err)
	}

	for i := 0; i < 2; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: 2,
			CacheBytes:    16 * 1024 * 1024,
			SpillDir:      t.TempDir(),
		}, store, nc, js, logger)
		// Workers must outlive the query ctx (see TestShuffleCorrectness).
		workerCtx, workerCancel := context.WithCancel(context.Background())
		t.Cleanup(workerCancel)
		if err := w.Start(workerCtx); err != nil {
			t.Fatalf("starting worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
	}

	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
		// LocalFastPathBytes stays 0 (disabled), so the query runs the
		// distributed stage DAG rather than the coordinator-local pipeline.
	}, cat, nc, js, logger)
	for i := 0; i < 2; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{
			WorkerID:  fmt.Sprintf("fake-worker-%d", i),
			Timestamp: time.Now(),
		})
	}

	queryCtx, cancelQuery := context.WithCancel(context.Background())
	defer cancelQuery()

	type sqlOutcome struct {
		err  error
		took time.Duration
	}
	done := make(chan sqlOutcome, 1)
	go func() {
		start := time.Now()
		res, qErr := coord.ExecuteSQL(queryCtx, "SELECT COUNT(*) FROM lineitem")
		res.Close()
		done <- sqlOutcome{err: qErr, took: time.Since(start)}
	}()

	// A stage task is now executing on a worker, parked in the scan.
	store.waitHeld(t, 1, 60*time.Second)

	cancelQuery()

	select {
	case out := <-done:
		if out.err == nil {
			t.Fatalf("ExecuteSQL returned success for a cancelled query (took %s)", out.took)
		}
		t.Logf("ExecuteSQL returned after %s: %v", out.took, out.err)
	case <-time.After(60 * time.Second):
		t.Fatal("ExecuteSQL never returned after cancellation")
	}

	// The load-bearing assertion: the gate is still shut, so the parked
	// worker read can only return by its task context being cancelled.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if store.held.Load() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker stage task still running %s after the query was cancelled "+
				"(%d reads parked in the scan): cancellation freed the client but not the cluster",
				30*time.Second, store.held.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Nothing new picks the query back up (retries of a cancelled query
	// would re-park reads in the gate).
	entered := store.entered.Load()
	time.Sleep(2 * time.Second)
	if store.held.Load() != 0 {
		t.Errorf("a worker re-entered the scan after cancellation (held=%d, total entries %d→%d)",
			store.held.Load(), entered, store.entered.Load())
	}
}
