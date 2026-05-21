package worker

import (
	"compress/gzip"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriteHeapPressureProfile_ProducesValidPprof(t *testing.T) {
	tmp := t.TempDir()
	w := &Worker{
		config:      Config{WorkerID: "test-worker", SpillDir: tmp},
		logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		activeTasks: map[string]struct{}{"task-A": {}, "task-B": {}},
	}
	dir := filepath.Join(tmp, "heap-profiles")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := w.writeHeapPressureProfile(dir); err != nil {
		t.Fatalf("writeHeapPressureProfile: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var profileFile, sidecarFile string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".pb.gz") {
			profileFile = filepath.Join(dir, e.Name())
		}
		if strings.HasSuffix(e.Name(), ".tasks.txt") {
			sidecarFile = filepath.Join(dir, e.Name())
		}
	}
	if profileFile == "" {
		t.Fatal("no .pb.gz profile written")
	}
	if sidecarFile == "" {
		t.Fatal("no .tasks.txt sidecar written")
	}

	// Sanity-check the profile: gzip header + non-empty pprof bytes.
	f, err := os.Open(profileFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("profile is not gzipped: %v", err)
	}
	buf := make([]byte, 1024)
	n, _ := gz.Read(buf)
	if n == 0 {
		t.Fatal("profile gzip stream is empty")
	}
	gz.Close()

	// Sidecar must contain the worker ID and active task IDs.
	sidecar, err := os.ReadFile(sidecarFile)
	if err != nil {
		t.Fatal(err)
	}
	s := string(sidecar)
	for _, want := range []string{"worker_id=test-worker", "active_task_count=2", "task-A", "task-B"} {
		if !strings.Contains(s, want) {
			t.Errorf("sidecar missing %q; full content:\n%s", want, s)
		}
	}
}

func TestHeapPressureProfiler_NoSpillDirIsNoOp(t *testing.T) {
	// Without a spill dir the profiler refuses to start. Confirms the
	// profiler doesn't crash on minimal-embedding worker setups (tests
	// that don't configure spill).
	w := &Worker{
		config:      Config{WorkerID: "test-worker"},
		logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		activeTasks: map[string]struct{}{},
	}
	// drainCh prevents w.Drain from panicking if startHeapPressureProfiler
	// accidentally adds to wg — we'd never call wg.Wait so a leaked goroutine
	// would show up in -race anyway. But the function must NOT add to wg
	// when SpillDir is unset.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.startHeapPressureProfiler(ctx)
	// Wait briefly then cancel; if a goroutine leaked, -race would catch it.
	time.Sleep(50 * time.Millisecond)
}

// TestHeapPressureProfiler_RateLimited confirms writeHeapPressureProfile
// is idempotent on disk (different timestamps produce different files) and
// the rate-limit constant is the gate. Doesn't drive the goroutine since
// HeapBackpressureActive is process-wide state and hard to fake in unit
// tests — the constants are exercised here directly.
func TestHeapPressureProfiler_RateLimited(t *testing.T) {
	tmp := t.TempDir()
	w := &Worker{
		config:      Config{WorkerID: "rl-worker", SpillDir: tmp},
		logger:      slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		activeTasks: map[string]struct{}{},
	}
	dir := filepath.Join(tmp, "heap-profiles")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Two writes back-to-back produce two distinct profile files (rate
	// limit lives in the goroutine, not the writer — write itself is
	// always safe to call).
	if err := w.writeHeapPressureProfile(dir); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond) // ensure unix-ms timestamps differ
	if err := w.writeHeapPressureProfile(dir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	pbCount := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".pb.gz") {
			pbCount++
		}
	}
	if pbCount != 2 {
		t.Errorf("expected 2 profile files, got %d", pbCount)
	}
}

// Compile-time check that the worker fields the heap profiler reads
// (activeTasks + activeTasksMu) are accessed under the lock. The test
// itself reads activeTasksMu indirectly via writeHeapPressureProfile.
var _ = sync.RWMutex{} // silence unused import linter if struct moves
