package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The background-upload gate must run uploadSlotsBusy-wide while the
// foreground probe reports activity and open back up to uploadSlotsIdle
// when it clears (SF100 Q06 2026-08-05: 8 background compress+PUT
// streams starved the next query's scans to ~19s on a 2s plan).
func TestUploadSlotGateYieldsToForeground(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	busy := atomic.Bool{}
	busy.Store(true)
	m.SetForegroundProbe(busy.Load)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cur, peak atomic.Int64
	var wg sync.WaitGroup
	release := make(chan struct{})
	for i := 0; i < uploadSlotsIdle; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !m.acquireSlot(ctx) {
				return
			}
			defer m.releaseSlot()
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-release
			cur.Add(-1)
		}()
	}
	// Let the gate settle while busy: only uploadSlotsBusy should hold slots.
	time.Sleep(300 * time.Millisecond)
	if got := peak.Load(); got > uploadSlotsBusy {
		t.Fatalf("busy peak concurrency = %d, want <= %d", got, uploadSlotsBusy)
	}

	// Foreground clears: the remaining jobs should widen to uploadSlotsIdle.
	busy.Store(false)
	time.Sleep(300 * time.Millisecond)
	if got := peak.Load(); got != uploadSlotsIdle {
		t.Fatalf("idle peak concurrency = %d, want %d", got, uploadSlotsIdle)
	}
	close(release)
	wg.Wait()

	if m.UploadYieldNs() == 0 {
		t.Fatal("yieldNs should record time spent waiting on the busy gate")
	}
}

// Cancellation must abort waiters promptly (query teardown while yielding).
func TestUploadSlotGateCancel(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	// Fill the busy width.
	ctx := context.Background()
	for i := 0; i < uploadSlotsBusy; i++ {
		if !m.acquireSlot(ctx) {
			t.Fatal("initial acquire failed")
		}
	}
	cctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- m.acquireSlot(cctx) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("cancelled acquire must return false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled acquire did not return")
	}
}
