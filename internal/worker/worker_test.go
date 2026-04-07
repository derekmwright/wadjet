package worker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
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

// TestWorkerSweepsStaleBuildCacheFiles verifies that Worker.Start() removes
// leftover build-cache spill files from a previous crash, while leaving other
// files in the spill dir alone. Without this sweep, repeated SF100 crashes
// would fill up the c7gd NVMe volume and break subsequent runs.
func TestWorkerSweepsStaleBuildCacheFiles(t *testing.T) {
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

	spillDir := t.TempDir()

	// Files the sweep MUST delete.
	stale := []string{
		"build-cache-1234.wshf",
		"build-cache-load-abc.wshf",
		"build-cache-deadbeef.wshf",
	}
	for _, name := range stale {
		path := filepath.Join(spillDir, name)
		if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Files the sweep MUST keep — different prefix or different suffix.
	keep := []string{
		"grace-spill-99.bin",
		"build-cache-1234.txt", // wrong suffix
		"unrelated.wshf",       // wrong prefix
	}
	for _, name := range keep {
		path := filepath.Join(spillDir, name)
		if err := os.WriteFile(path, []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := New(Config{
		WorkerID:      "sweep-worker",
		MaxConcurrent: 1,
		CacheBytes:    1024 * 1024,
		SpillDir:      spillDir,
	}, store, nc, js, logger)
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer w.Stop()

	for _, name := range stale {
		if _, err := os.Stat(filepath.Join(spillDir, name)); !os.IsNotExist(err) {
			t.Errorf("stale file %q should have been swept, stat err=%v", name, err)
		}
	}
	for _, name := range keep {
		if _, err := os.Stat(filepath.Join(spillDir, name)); err != nil {
			t.Errorf("file %q should have been preserved, stat err=%v", name, err)
		}
	}
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

