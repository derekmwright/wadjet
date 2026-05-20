package dataplane

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestPickWorkerAndReserveForStage_HonorsCap connects two workers and
// drives many concurrent reservations on the same stageKey with cap=1.
// At any instant the in-flight count for that stage on any single
// worker must be ≤ 1. Validates the deadlock-free invariant:
// per-stageID reservations don't block each other across stages and
// don't reorder.
func TestPickWorkerAndReserveForStage_HonorsCap(t *testing.T) {
	srv, stopAll := newTestServerWithWorkers(t, "w-A", "w-B")
	defer stopAll()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const cap = 1
	const concurrent = 16
	stageKey := "q-test:stage-build"

	var maxObserved sync.Map // workerID -> int (peak count)
	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wid, err := srv.PickWorkerAndReserveForStage(ctx, stageKey, cap)
			if err != nil {
				t.Errorf("PickAndReserve: %v", err)
				return
			}
			// Sample the in-flight count under the lock and update peak.
			srv.mu.Lock()
			cur := srv.inFlight[wid][stageKey]
			srv.mu.Unlock()
			if cur > cap {
				t.Errorf("worker %s holds %d > cap %d for stage %s", wid, cur, cap, stageKey)
			}
			// Track peak per worker (best-effort under concurrent samples).
			for {
				prev, _ := maxObserved.LoadOrStore(wid, cur)
				if cur <= prev.(int) {
					break
				}
				if maxObserved.CompareAndSwap(wid, prev, cur) {
					break
				}
			}
			// Hold the slot briefly so other goroutines see contention.
			time.Sleep(5 * time.Millisecond)
			srv.ReleaseStage(wid, stageKey)
		}()
	}
	wg.Wait()

	maxObserved.Range(func(k, v any) bool {
		if n, _ := v.(int); n > cap {
			t.Errorf("peak in-flight on %v = %d > cap %d", k, n, cap)
		}
		return true
	})

	// After all goroutines finish, in-flight for the stageKey should
	// be zero on every worker.
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for wid, counts := range srv.inFlight {
		if c := counts[stageKey]; c != 0 {
			t.Errorf("worker %s leaked %d reservations for %s", wid, c, stageKey)
		}
	}
}

// TestPickWorkerAndReserveForStage_BlocksUntilRelease confirms that
// when all workers are at cap, a further PickAndReserve blocks until
// ReleaseStage frees a slot. Catches the case where cond signaling
// fails to wake waiters.
func TestPickWorkerAndReserveForStage_BlocksUntilRelease(t *testing.T) {
	srv, stopAll := newTestServerWithWorkers(t, "w-A")
	defer stopAll()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stageKey := "q-block:stage-build"
	// Saturate w-A.
	wid, err := srv.PickWorkerAndReserveForStage(ctx, stageKey, 1)
	if err != nil {
		t.Fatalf("initial reserve: %v", err)
	}
	if wid != "w-A" {
		t.Fatalf("unexpected pick %q", wid)
	}

	// Start a second reserve in a goroutine; it should block.
	picked := make(chan string, 1)
	pickErr := make(chan error, 1)
	go func() {
		w, err := srv.PickWorkerAndReserveForStage(ctx, stageKey, 1)
		if err != nil {
			pickErr <- err
			return
		}
		picked <- w
	}()

	// Confirm it actually blocks for at least ~50 ms.
	select {
	case w := <-picked:
		t.Fatalf("PickAndReserve returned %q without waiting; cap not enforced", w)
	case err := <-pickErr:
		t.Fatalf("PickAndReserve errored unexpectedly: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Release frees the slot; the waiter should pick up immediately.
	srv.ReleaseStage(wid, stageKey)
	select {
	case w := <-picked:
		if w != "w-A" {
			t.Fatalf("post-release pick = %q, want w-A", w)
		}
	case err := <-pickErr:
		t.Fatalf("post-release err: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("waiter did not wake after release")
	}
	// Clean up the second reservation.
	srv.ReleaseStage("w-A", stageKey)
}

// TestPickWorkerAndReserveForStage_CtxCancelReleasesWaiter confirms
// that cancelling the caller's context releases the cond.Wait without
// leaking the goroutine or holding the lock.
func TestPickWorkerAndReserveForStage_CtxCancelReleasesWaiter(t *testing.T) {
	srv, stopAll := newTestServerWithWorkers(t, "w-A")
	defer stopAll()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stageKey := "q-cancel:stage-build"

	// Saturate.
	if _, err := srv.PickWorkerAndReserveForStage(ctx, stageKey, 1); err != nil {
		t.Fatalf("initial reserve: %v", err)
	}

	// Second reserver: cancel its context after 50 ms; expect ctx err.
	cctx, ccancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := srv.PickWorkerAndReserveForStage(cctx, stageKey, 1)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	ccancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ctx err on cancelled reserver, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error: %v (want context.Canceled)", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("cancelled reserver did not return")
	}
}

// TestUnregisterClearsInFlight confirms that when a worker is
// unregistered, all reservations held on it drop from the in-flight
// map. Without this, a vanished worker's stale reservation would wedge
// future PickAndReserve calls forever (the per-query
// ResultNotification path never fires for tasks abandoned with the
// dead worker).
//
// Drives unregister() directly rather than via client.Stop() because
// the real production path runs the same unregister call: gRPC stream
// close → Connect handler returns → deferred unregister fires.
func TestUnregisterClearsInFlight(t *testing.T) {
	srv, stopAll := newTestServerWithWorkers(t, "w-A", "w-B")
	defer stopAll()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stageKey := "q-disc:stage-build"

	// Saturate both workers at cap=2 with two reserves on each.
	for i := 0; i < 4; i++ {
		if _, err := srv.PickWorkerAndReserveForStage(ctx, stageKey, 2); err != nil {
			t.Fatalf("reserve #%d: %v", i, err)
		}
	}
	srv.mu.Lock()
	preA := srv.inFlight["w-A"][stageKey]
	preB := srv.inFlight["w-B"][stageKey]
	srv.mu.Unlock()
	if preA == 0 || preB == 0 {
		t.Fatalf("setup: expected reservations on both workers; got A=%d B=%d", preA, preB)
	}

	// Simulate a w-A disconnect — production path is the same call.
	srv.unregister("w-A")

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if _, ok := srv.inFlight["w-A"]; ok {
		t.Errorf("inFlight entry for disconnected worker w-A not cleared")
	}
	if got := srv.inFlight["w-B"][stageKey]; got != preB {
		t.Errorf("w-B inFlight changed from %d to %d after w-A unregister; should be untouched",
			preB, got)
	}
}

// TestReleaseStageWithoutReserveIsNoop guards against a footgun: late-
// arriving ResultNotifications from a task that completed AFTER the
// release (e.g., worker disconnect path already cleared the inFlight
// map) must not underflow the counter or panic.
func TestReleaseStageWithoutReserveIsNoop(t *testing.T) {
	srv, stopAll := newTestServerWithWorkers(t, "w-A")
	defer stopAll()

	// No prior reserve; calling Release should be silent.
	srv.ReleaseStage("w-A", "q:stage-x")
	srv.ReleaseStage("never-existed", "q:stage-y")

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if _, ok := srv.inFlight["never-existed"]; ok {
		t.Error("ReleaseStage created an entry for an unknown worker")
	}
}

// newTestServerWithWorkers spins up a real Server and connects
// `workerIDs` clients to it, returning once every client reports
// connected. Returns a stopAll func that closes the clients and the
// server in the right order.
func newTestServerWithWorkers(t *testing.T, workerIDs ...string) (*Server, func()) {
	t.Helper()
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "tt"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	clients := make([]*Client, 0, len(workerIDs))
	for _, id := range workerIDs {
		c := NewClient(ClientConfig{
			CoordAddr:        srv.Addr(),
			WorkerID:         id,
			ReconnectBackoff: 25 * time.Millisecond,
		}, nil)
		c.Start(ctx)
		clients = append(clients, c)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return len(srv.ConnectedWorkers()) == len(workerIDs)
	}) {
		t.Fatalf("not all workers connected; got %v", srv.ConnectedWorkers())
	}
	return srv, func() {
		cancel()
		for _, c := range clients {
			c.Stop()
		}
		srv.Stop(0)
	}
}
