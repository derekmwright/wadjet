package objstore

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMinIOStore_UploadSemaphore_Bounds verifies the per-instance upload
// semaphore caps concurrent acquireUpload calls at MaxConcurrentUploads,
// regardless of how many goroutines try to acquire simultaneously.
func TestMinIOStore_UploadSemaphore_Bounds(t *testing.T) {
	const maxConcurrent = 3
	store := &MinIOStore{
		uploadSem: make(chan struct{}, maxConcurrent),
	}

	var inFlight, peak int64
	var wg sync.WaitGroup
	const callers = 20

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := store.acquireUpload(context.Background())
			if err != nil {
				t.Errorf("acquireUpload: %v", err)
				return
			}
			defer release()
			cur := atomic.AddInt64(&inFlight, 1)
			defer atomic.AddInt64(&inFlight, -1)
			for {
				p := atomic.LoadInt64(&peak)
				if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
					break
				}
			}
			// Hold the slot briefly so contention is observable.
			time.Sleep(20 * time.Millisecond)
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt64(&peak); got > int64(maxConcurrent) {
		t.Errorf("peak in-flight uploads = %d, want ≤ %d", got, maxConcurrent)
	}
}

// TestMinIOStore_UploadSemaphore_ReleasesOnContextCancel verifies that
// a goroutine blocked on acquireUpload returns the context error when
// the context is cancelled, instead of waiting forever.
func TestMinIOStore_UploadSemaphore_ReleasesOnContextCancel(t *testing.T) {
	store := &MinIOStore{
		uploadSem: make(chan struct{}, 1),
	}

	// Saturate the semaphore.
	release1, err := store.acquireUpload(context.Background())
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	defer release1()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.acquireUpload(ctx)
		done <- err
	}()

	// Give the goroutine a moment to block on the channel send.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("acquireUpload err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquireUpload did not return after context cancel")
	}
}

// TestMinIOStore_UploadSemaphore_NilWhenUnconfigured: a MinIOStore that
// was constructed without a semaphore (zero-value or test fake) should
// return a no-op release without blocking.
func TestMinIOStore_UploadSemaphore_NilWhenUnconfigured(t *testing.T) {
	store := &MinIOStore{} // no uploadSem
	release, err := store.acquireUpload(context.Background())
	if err != nil {
		t.Fatalf("acquireUpload on unconfigured store: %v", err)
	}
	release() // must not panic
}
