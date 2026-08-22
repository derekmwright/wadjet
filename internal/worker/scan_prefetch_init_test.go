package worker

import (
	"context"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// countingGetStore records how many Get calls have been ISSUED. Only the
// interface methods are promoted, so the tiered path's LocalPathStore
// probes miss and every file resolves through Get — exactly the shape the
// prefetcher exists for.
type countingGetStore struct {
	objstore.Store
	gets atomic.Int64
}

func (c *countingGetStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	c.gets.Add(1)
	return c.Store.Get(ctx, bucket, key)
}

// blockingGetStore parks every Get until release is closed or ctx is
// cancelled, so a test can observe the download workers while they are
// definitively in flight.
type blockingGetStore struct {
	objstore.Store
	gets    atomic.Int64
	release chan struct{}
}

func (b *blockingGetStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	b.gets.Add(1)
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, objstore.ObjectInfo{}, ctx.Err()
	}
	return b.Store.Get(ctx, bucket, key)
}

// pollUntil polls cond until it holds or the deadline passes, reporting
// whether it held. Unlike waitFor (upload_manager_test.go) it does not
// fail the test — these call sites must cancel a context first.
func pollUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// TestPrefetchStartsAtInitBeforeBuildLoad is the straggler-tier fix's
// regression test (docs/benchmarks/straggler-tier-verdict-2026-08-16.md):
// the degraded scan mode is a 5.8-7.2 s prefetch-take wait that starts only
// after the join build side finished loading. Init must have issued reads
// before a simulated build load completes — i.e. before the first Next.
func TestPrefetchStartsAtInitBeforeBuildLoad(t *testing.T) {
	ctx := context.Background()
	mem := objstore.NewMemStore()
	const bucket = "test"
	keys, wantSum := writePrefetchFixture(t, mem, bucket, 6, 30)
	store := &countingGetStore{Store: mem}

	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	if src.prefetch == nil {
		t.Fatal("Init did not start the prefetcher")
	}
	if !src.prefetchStartedAtInit {
		t.Fatal("prefetchAtInit not recorded")
	}

	// The build load: the runner's work between source Init and the first
	// Next. The prefetcher must be downloading THROUGH it.
	const buildLoad = 150 * time.Millisecond
	buildStart := time.Now()
	if !pollUntil(buildLoad, func() bool { return store.gets.Load() > 0 }) {
		t.Fatalf("no Get issued during the %s build load — prefetch did not overlap it", buildLoad)
	}
	time.Sleep(buildLoad - time.Since(buildStart))

	got := store.gets.Load()
	if got == 0 {
		t.Fatal("prefetcher issued no reads before the first Next")
	}

	sum, rows := drainValSum(t, ctx, src)
	if sum != wantSum || rows != 6*30 {
		t.Fatalf("sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, 6*30)
	}
	// The lead stat is what the next SF100 run reads to verify the overlap
	// directly: it must cover the simulated build load.
	if !src.acq.prefetchAtInit {
		t.Error("acq stats do not record the at-Init start")
	}
	if src.acq.prefetchLeadNs < buildLoad.Nanoseconds() {
		t.Errorf("prefetch lead = %dms, want >= %dms (the build load it overlapped)",
			src.acq.prefetchLeadNs/1e6, buildLoad.Milliseconds())
	}
}

// TestPrefetchAtInitKillSwitch verifies WADJET_PREFETCH_AT_INIT=0 restores
// the start-at-first-open behavior: no read is issued until the scan asks
// for a file, and the same rows come back.
func TestPrefetchAtInitKillSwitch(t *testing.T) {
	prev := prefetchAtInit.Set(false)
	defer prefetchAtInit.Set(prev)

	ctx := context.Background()
	mem := objstore.NewMemStore()
	const bucket = "test"
	keys, wantSum := writePrefetchFixture(t, mem, bucket, 6, 30)
	store := &countingGetStore{Store: mem}

	e := &Executor{store: store, spillDir: t.TempDir()}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	if src.prefetch != nil {
		t.Fatal("prefetcher started at Init despite the kill switch")
	}
	time.Sleep(50 * time.Millisecond)
	if n := store.gets.Load(); n != 0 {
		t.Fatalf("%d Gets issued before the first Next with the kill switch on", n)
	}

	sum, rows := drainValSum(t, ctx, src)
	if sum != wantSum || rows != 6*30 {
		t.Fatalf("kill-switch read: sum=%d rows=%d, want sum=%d rows=%d", sum, rows, wantSum, 6*30)
	}
	if src.prefetch == nil {
		t.Fatal("prefetcher never started on the first open")
	}
	if src.acq.prefetchAtInit {
		t.Error("acq stats claim an at-Init start under the kill switch")
	}
}

// TestPrefetchInitCtxCancelStopsWorkers pins the ctx-lifetime guarantee:
// the download goroutines are tied to the task context Init was given, so
// cancelling the task stops all of them without a Close and without
// leaking a goroutine past task end.
func TestPrefetchInitCtxCancelStopsWorkers(t *testing.T) {
	mem := objstore.NewMemStore()
	const bucket = "test"
	keys, _ := writePrefetchFixture(t, mem, bucket, 12, 20)
	store := &blockingGetStore{Store: mem, release: make(chan struct{})}

	// Settle first: the fixture writer and any earlier test's goroutines
	// must be gone before the baseline is taken.
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	spill := t.TempDir()
	e := &Executor{store: store, spillDir: spill}
	src := newCachedFileStreamSource(e, "", bucket, keys)
	if err := src.Init(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	if src.prefetch == nil {
		cancel()
		t.Fatal("Init did not start the prefetcher")
	}
	// Every worker is parked inside Get.
	if !pollUntil(5*time.Second, func() bool { return store.gets.Load() >= int64(scanPrefetchConcurrency) }) {
		cancel()
		t.Fatalf("only %d Gets in flight, want %d", store.gets.Load(), scanPrefetchConcurrency)
	}
	if n := runtime.NumGoroutine(); n <= baseline {
		cancel()
		t.Fatalf("goroutine count %d did not rise above baseline %d — workers never ran", n, baseline)
	}

	// Task end: cancelling the context alone must stop every worker.
	cancel()
	if !pollUntil(5*time.Second, func() bool { return runtime.NumGoroutine() <= baseline }) {
		t.Fatalf("goroutines still running after ctx cancel: %d, baseline %d",
			runtime.NumGoroutine(), baseline)
	}

	// Close after cancel must not hang and must reap any temp files.
	done := make(chan struct{})
	go func() {
		src.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung after ctx cancel")
	}
	close(store.release)
	assertNoPrefetchTemps(t, spill)
}
