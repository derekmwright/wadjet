package worker

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestUploadManagerFlush_WaitsForPendingUploads: Flush must block until
// pending background uploads land (the graceful-drain durability handoff:
// once the worker exits, consumers of its stage outputs have only the S3
// copy). Contrast with Drain, which cancels.
func TestUploadManagerFlush_WaitsForPendingUploads(t *testing.T) {
	gate := &gatedStore{MemStore: objstore.NewMemStore(), gate: make(chan struct{})}
	if err := gate.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	m := newUploadManager(gate, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	src := writeTempFile(t, []byte("stage output bytes"))
	m.StartTask("root-q", "task-1", "w-1", []uploadJob{{bucket: "b", key: "queries/root-q/out.wshf", srcPath: src}}, distributed.UploadEager)

	flushed := make(chan error, 1)
	go func() { flushed <- m.Flush(context.Background()) }()

	select {
	case err := <-flushed:
		t.Fatalf("Flush returned (%v) while the upload was still gated", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(gate.gate)
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("Flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not return after the upload completed")
	}
	if _, _, err := gate.Get(context.Background(), "b", "queries/root-q/out.wshf"); err != nil {
		t.Fatalf("uploaded object missing after Flush: %v", err)
	}
}

// TestUploadManagerFlush_Timeout: an expired context aborts the wait with
// ctx.Err() so Drain can escalate to the cancel path.
func TestUploadManagerFlush_Timeout(t *testing.T) {
	gate := &gatedStore{MemStore: objstore.NewMemStore(), gate: make(chan struct{})} // never opens
	if err := gate.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	m := newUploadManager(gate, nil, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	src := writeTempFile(t, []byte("x"))
	m.StartTask("root-q", "task-1", "w-1", []uploadJob{{bucket: "b", key: "k", srcPath: src}}, distributed.UploadEager)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := m.Flush(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Flush = %v, want DeadlineExceeded", err)
	}
	m.Drain() // release the stuck job so the test goroutine can exit
}

func TestUploadManagerFlush_NilAndEmpty(t *testing.T) {
	var m *uploadManager
	if err := m.Flush(context.Background()); err != nil {
		t.Fatalf("nil Flush: %v", err)
	}
	m2 := newUploadManager(objstore.NewMemStore(), nil, nil)
	if err := m2.Flush(context.Background()); err != nil {
		t.Fatalf("empty Flush: %v", err)
	}
}

// TestWorkerDrain_CompletesWithBackgroundLoops is the regression test for
// the drain deadlock: heartbeat-style background goroutines exit only on
// context cancel, and the old single-WaitGroup Drain waited for them BEFORE
// cancelling — so Drain never returned. With the wg/bgWG split, Drain must
// (1) wait for in-flight task goroutines, (2) flush uploads, (3) cancel and
// join the background group.
func TestWorkerDrain_CompletesWithBackgroundLoops(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		config:   Config{WorkerID: "drain-test"},
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		drainCh:  make(chan struct{}),
		cancel:   cancel,
		executor: NewExecutor(store, NewLRUCache(1024), nil),
	}

	// Background loop: exits only on ctx cancel (the heartbeat shape).
	bgExited := atomic.Bool{}
	w.bgWG.Add(1)
	go func() {
		defer w.bgWG.Done()
		<-ctx.Done()
		bgExited.Store(true)
	}()

	// In-flight task: finishes on its own shortly after drain begins, and
	// must NOT be aborted by drain (ctx must still be alive when it ends).
	taskSawCancel := atomic.Bool{}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		select {
		case <-ctx.Done():
			taskSawCancel.Store(true)
		case <-time.After(150 * time.Millisecond):
		}
	}()

	done := make(chan struct{})
	go func() { w.Drain(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not complete (background-loop deadlock)")
	}
	if taskSawCancel.Load() {
		t.Fatal("in-flight task was cancelled during drain; drain must let tasks finish")
	}
	if !bgExited.Load() {
		t.Fatal("background loop still running after Drain returned")
	}
	if !w.Draining() {
		t.Fatal("Draining() must report true after Drain")
	}
}

// TestWorkerDrain_StreamingShuffleMarkerLoop is the regression test for the
// drain deadlock shipped with --streaming-shuffle-read: the stats marker
// loop registered itself on w.wg (the in-flight task group Drain waits on
// pre-cancel) instead of bgWG, so with the flag on, Drain never returned —
// every graceful shutdown (tpch-harness teardown, SIGTERM, rolling update)
// hung forever. The loop must ride bgWG exactly like the heartbeat.
func TestWorkerDrain_StreamingShuffleMarkerLoop(t *testing.T) {
	store := objstore.NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		config:   Config{WorkerID: "drain-marker-test", StreamingShuffleRead: true},
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		drainCh:  make(chan struct{}),
		cancel:   cancel,
		executor: NewExecutor(store, NewLRUCache(1024), nil),
	}
	w.startShuffleStreamMarkerLoop(ctx)

	done := make(chan struct{})
	go func() { w.Drain(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not complete: streaming-shuffle marker loop blocks drain (wg vs bgWG)")
	}
}

// TestWorkerDrain_ScanDecodeAheadMarkerLoop: the scan-decode-ahead stats
// marker loop must ride bgWG, not w.wg — the same drain-deadlock class as
// the streaming-shuffle marker loop above.
func TestWorkerDrain_ScanDecodeAheadMarkerLoop(t *testing.T) {
	store := objstore.NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		config:   Config{WorkerID: "drain-scan-marker-test", ScanDecodeAhead: true},
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		drainCh:  make(chan struct{}),
		cancel:   cancel,
		executor: NewExecutor(store, NewLRUCache(1024), nil),
	}
	w.startScanDecodeAheadMarkerLoop(ctx)

	done := make(chan struct{})
	go func() { w.Drain(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not complete: scan-decode-ahead marker loop blocks drain (wg vs bgWG)")
	}
}

// TestWorkerDrain_EmitsFinalScanStats: Drain must emit the same
// end-of-life scan/IO marker lines as Stop — including the final
// per-query decode-ahead sweep. The 2026-08-08 SF1 --runs=2 repro lost
// every drained worker's per-query decode spans because the final
// emission lived only on the Stop path, and SIGTERM'd workers
// (harness teardown, rolling updates) always go through Drain.
func TestWorkerDrain_EmitsFinalScanStats(t *testing.T) {
	store := objstore.NewMemStore()
	var logBuf syncBuffer
	_, cancel := context.WithCancel(context.Background())
	w := &Worker{
		config:   Config{WorkerID: "drain-final-stats-test", ScanDecodeAhead: true},
		logger:   slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})),
		drainCh:  make(chan struct{}),
		cancel:   cancel,
		executor: NewExecutor(store, NewLRUCache(1024), nil),
	}
	w.executor.logger = w.logger
	w.executor.foldScanDecodeAheadQueryStats("q-drain", 3, 0, 0, 0, 0, 0, 0, 0, 0, 7e6, 4096, 0)

	done := make(chan struct{})
	go func() { w.Drain(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not complete")
	}
	out := logBuf.String()
	if !strings.Contains(out, "scan decode-ahead stats (final)") {
		t.Error("drain did not emit final decode-ahead stats line")
	}
	if !strings.Contains(out, "q-drain") || !strings.Contains(out, "decode_ms=7") {
		t.Errorf("drain did not sweep per-query decode-ahead stats; log:\n%s", out)
	}
	if !strings.Contains(out, "proc io stats (final)") {
		t.Error("drain did not emit final proc io stats line")
	}
}

// syncBuffer is a mutex-guarded bytes.Buffer — slog handlers may be
// written from multiple goroutines (marker loops) during Drain.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestWorkerDrain_TimeoutEscalatesToStop: with DrainTimeout set and a task
// that never finishes, Drain must give up and hard-stop (cancel) instead of
// hanging past the platform's kill window.
func TestWorkerDrain_TimeoutEscalatesToStop(t *testing.T) {
	store := objstore.NewMemStore()
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		config:   Config{WorkerID: "drain-timeout-test", DrainTimeout: 200 * time.Millisecond},
		logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		drainCh:  make(chan struct{}),
		cancel:   cancel,
		executor: NewExecutor(store, NewLRUCache(1024), nil),
	}
	// Wedged task: only exits when ctx is cancelled (i.e. by the escalation).
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		<-ctx.Done()
	}()

	done := make(chan struct{})
	go func() { w.Drain(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not escalate on timeout")
	}
}

// TestWorkerBeginDrain_Idempotent: BeginDrain from multiple sources (signal,
// NATS drain subject, /drain endpoint) must not panic on double-close and
// must trip DrainRequested exactly once.
func TestWorkerBeginDrain_Idempotent(t *testing.T) {
	w := &Worker{
		config:  Config{WorkerID: "idem"},
		logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		drainCh: make(chan struct{}),
	}
	select {
	case <-w.DrainRequested():
		t.Fatal("DrainRequested fired before BeginDrain")
	default:
	}
	w.BeginDrain()
	w.BeginDrain()
	select {
	case <-w.DrainRequested():
	default:
		t.Fatal("DrainRequested not fired after BeginDrain")
	}
	if !w.Draining() {
		t.Fatal("Draining() false after BeginDrain")
	}
}
