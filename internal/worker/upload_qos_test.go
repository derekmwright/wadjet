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
	m.NoteForegroundQuery("test-root") // open the protection window

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cur, peak atomic.Int64
	var wg sync.WaitGroup
	release := make(chan struct{})
	for i := 0; i < uploadSlotsIdle; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !m.acquireSlot(ctx, nil) {
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
	m.NoteForegroundQuery("test-root")
	// Fill the busy width.
	ctx := context.Background()
	for i := 0; i < uploadSlotsBusy; i++ {
		if !m.acquireSlot(ctx, nil) {
			t.Fatal("initial acquire failed")
		}
	}
	cctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- m.acquireSlot(cctx, nil) }()
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

// Urgent roots (demand-released) and long-waiting jobs must escape the
// busy width — the v1 flat gate starved the backlog for whole suites.
func TestUploadSlotGateUrgencyAndBoundedYield(t *testing.T) {
	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })
	m.NoteForegroundQuery("test-root")
	ctx := context.Background()
	// Saturate the busy width.
	for i := 0; i < uploadSlotsBusy; i++ {
		if !m.acquireSlot(ctx, nil) {
			t.Fatal("initial acquire failed")
		}
	}
	// Non-urgent job stays gated...
	blocked := make(chan bool, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		blocked <- m.acquireSlot(c, nil)
	}()
	if ok := <-blocked; ok {
		t.Fatal("non-urgent job passed a saturated busy gate")
	}
	// ...an urgent root's job does not.
	qs := &queryUploadState{}
	qs.urgent.Store(true)
	done := make(chan bool, 1)
	go func() { done <- m.acquireSlot(ctx, qs) }()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("urgent acquire returned false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("urgent job did not bypass the busy gate")
	}
}


// The v3 epoch clock: the busy width applies only inside the protection
// window a NEW root query opens; the window expires (drains resume full
// width even while busy) and repeat tasks of the same root do not
// re-open it.
func TestUploadSlotGateEpochWindow(t *testing.T) {
	old := uploadProtectMs
	uploadProtectMs = 150
	defer func() { uploadProtectMs = old }()

	m := newUploadManager(nil, nil, nil)
	m.SetForegroundProbe(func() bool { return true })

	// No window opened yet: busy alone must NOT throttle.
	if got := m.slotCap(); got != uploadSlotsIdle {
		t.Fatalf("no-window cap = %d, want idle %d", got, uploadSlotsIdle)
	}
	m.NoteForegroundQuery("root-a")
	if got := m.slotCap(); got != uploadSlotsBusy {
		t.Fatalf("in-window cap = %d, want busy %d", got, uploadSlotsBusy)
	}
	time.Sleep(200 * time.Millisecond) // window expires
	if got := m.slotCap(); got != uploadSlotsIdle {
		t.Fatalf("expired-window cap = %d, want idle %d", got, uploadSlotsIdle)
	}
	// Same root again: no new window.
	m.NoteForegroundQuery("root-a")
	if got := m.slotCap(); got != uploadSlotsIdle {
		t.Fatalf("repeat-root cap = %d, want idle %d", got, uploadSlotsIdle)
	}
	// A NEW root re-opens it.
	m.NoteForegroundQuery("root-b")
	if got := m.slotCap(); got != uploadSlotsBusy {
		t.Fatalf("new-root cap = %d, want busy %d", got, uploadSlotsBusy)
	}
}
