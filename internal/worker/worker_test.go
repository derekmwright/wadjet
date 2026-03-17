package worker

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/nats-io/nats.go"
)

func setupWorkerNATS(t *testing.T) (context.Context, *distributed.EmbeddedNATS, *objstore.MemStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatal(err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "results"); err != nil {
		t.Fatal(err)
	}

	return ctx, en, store
}

func TestNewWorker(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)
	_ = ctx

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	w := New(Config{
		MaxConcurrent: 2,
		CacheBytes:    1024 * 1024,
	}, store, nc, js, nil)
	if w == nil {
		t.Fatal("expected non-nil worker")
	}
	if w.config.WorkerID == "" {
		t.Error("expected auto-generated WorkerID")
	}
	if w.config.MaxConcurrent != 2 {
		t.Errorf("MaxConcurrent: got %d, want 2", w.config.MaxConcurrent)
	}
}

func TestNewWorkerDefaults(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)
	_ = ctx

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	// Zero values should get defaults
	w := New(Config{}, store, nc, js, nil)
	if w.config.MaxConcurrent != 4 {
		t.Errorf("default MaxConcurrent: got %d, want 4", w.config.MaxConcurrent)
	}
	if w.config.CacheBytes != 256*1024*1024 {
		t.Errorf("default CacheBytes: got %d", w.config.CacheBytes)
	}
}

func TestNewWorkerWithResultStore(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)
	_ = ctx

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	w := New(Config{
		ResultStoreBytes: 10 * 1024 * 1024,
		MemoryBudget:     100 * 1024 * 1024,
		SpillDir:         t.TempDir(),
	}, store, nc, js, nil)
	if w == nil {
		t.Fatal("expected non-nil worker")
	}
}

func TestWorkerStartStop(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(Config{
		WorkerID:      "test-worker",
		MaxConcurrent: 2,
		CacheBytes:    1024 * 1024,
	}, store, nc, js, logger)

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Give it a moment to initialize
	time.Sleep(100 * time.Millisecond)

	w.Stop()
}

func TestWorkerStartWithClusterID(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(Config{
		WorkerID:      "cluster-worker",
		ClusterID:     "afb-east",
		MaxConcurrent: 2,
		CacheBytes:    1024 * 1024,
	}, store, nc, js, logger)

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start with cluster: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	w.Stop()
}

func TestWorkerHandlesTask(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)

	// Write test data
	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}
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
	data := buf.Bytes()
	if _, err := store.Put(ctx, "results", "input/data.parquet",
		bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	// Create worker connection
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(Config{
		WorkerID:      "task-worker",
		MaxConcurrent: 2,
		CacheBytes:    1024 * 1024,
	}, store, nc, js, logger)

	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Subscribe to results to verify completion
	resultCh := make(chan distributed.ResultNotification, 1)
	sub, err := nc.Subscribe(distributed.SubjectResultsAll, func(msg *nats.Msg) {
		var rn distributed.ResultNotification
		if err := distributed.Unmarshal(msg.Data, &rn); err == nil {
			resultCh <- rn
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// Publish a scan task
	task := distributed.Task{
		ID:           "scan-test",
		QueryID:      "q-test",
		StageID:      "s0",
		Type:         distributed.TaskTypeScan,
		Files:        []string{"input/data.parquet"},
		ResultBucket: "results",
		ResultPrefix: "output/",
		CreatedAt:    time.Now(),
	}
	taskData, err := distributed.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}

	subject := distributed.TaskSubject(string(task.Type), task.QueryID, task.StageID)
	if err := nc.Publish(subject, taskData); err != nil {
		t.Fatal(err)
	}

	// Wait for result
	select {
	case result := <-resultCh:
		if !result.Success {
			t.Fatalf("task failed: %s", result.Error)
		}
		if result.NumRows != 2 {
			t.Errorf("expected 2 rows, got %d", result.NumRows)
		}
		if result.TaskID != "scan-test" {
			t.Errorf("TaskID: got %q, want %q", result.TaskID, "scan-test")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for task result")
	}
}

func TestWorkerIsCancelled(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(Config{
		WorkerID:      "cancel-worker",
		MaxConcurrent: 2,
		CacheBytes:    1024 * 1024,
	}, store, nc, js, logger)

	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	// Initially not cancelled
	if w.isCancelled("q-test") {
		t.Error("should not be cancelled initially")
	}

	// Publish cancellation
	cancelSubject := distributed.CancelSubject("q-test")
	if err := nc.Publish(cancelSubject, []byte("q-test")); err != nil {
		t.Fatal(err)
	}
	nc.Flush()

	// Wait for cancellation to be processed
	time.Sleep(200 * time.Millisecond)

	if !w.isCancelled("q-test") {
		t.Error("should be cancelled after publishing cancel message")
	}
}

func TestWorkerSetMetrics(t *testing.T) {
	ctx, en, store := setupWorkerNATS(t)
	_ = ctx

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	w := New(Config{}, store, nc, js, nil)
	w.SetMetrics(nil) // should not panic
}
