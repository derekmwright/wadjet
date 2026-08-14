package coordinator

import (
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

func TestWorkerRegistryRecord(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   30 * time.Second,
		logger:  slog.Default(),
	}

	hb := distributed.WorkerHeartbeat{
		WorkerID:    "w-1",
		MemoryUsed:  100 * 1024 * 1024,
		MemoryTotal: 512 * 1024 * 1024,
		Timestamp:   time.Now(),
	}

	wr.record(hb)

	active := wr.ActiveWorkers()
	if len(active) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(active))
	}
	if active[0].WorkerID != "w-1" {
		t.Fatalf("expected w-1, got %s", active[0].WorkerID)
	}
}

func TestWorkerRegistryReapStale(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   1 * time.Millisecond,
		logger:  slog.Default(),
	}

	wr.record(distributed.WorkerHeartbeat{
		WorkerID:  "w-stale",
		Timestamp: time.Now(),
	})
	wr.record(distributed.WorkerHeartbeat{
		WorkerID:  "w-active",
		Timestamp: time.Now(),
	})
	// Simulate stale worker by backdating LastSeen directly
	wr.workers["w-stale"].LastSeen = time.Now().Add(-1 * time.Hour)

	reaped := wr.ReapStale()
	if reaped != 1 {
		t.Fatalf("expected 1 reaped, got %d", reaped)
	}

	active := wr.ActiveWorkers()
	if len(active) != 1 {
		t.Fatalf("expected 1 active, got %d", len(active))
	}
	if active[0].WorkerID != "w-active" {
		t.Fatalf("expected w-active, got %s", active[0].WorkerID)
	}
}

func TestWorkerRegistryCount(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   30 * time.Second,
		logger:  slog.Default(),
	}

	if wr.Count() != 0 {
		t.Fatal("expected 0 workers")
	}

	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-1", Timestamp: time.Now()})
	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-2", Timestamp: time.Now()})

	if wr.Count() != 2 {
		t.Fatalf("expected 2 workers, got %d", wr.Count())
	}

	// Update existing worker
	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-1", MemoryUsed: 200, Timestamp: time.Now()})
	if wr.Count() != 2 {
		t.Fatalf("expected still 2 workers, got %d", wr.Count())
	}
}

func TestTaskLiveness(t *testing.T) {
	tl := NewTaskLiveness()

	now := time.Now()
	tl.Update([]string{"task-1", "task-2", "task-3"}, now)

	// All tasks are fresh — none stuck
	stuck := tl.StuckTasks(30 * time.Second)
	if len(stuck) != 0 {
		t.Errorf("expected 0 stuck tasks, got %d", len(stuck))
	}

	// Simulate task-1 going stale
	tl.mu.Lock()
	tl.tasks["task-1"] = now.Add(-2 * time.Minute)
	tl.mu.Unlock()

	stuck = tl.StuckTasks(60 * time.Second)
	if len(stuck) != 1 || stuck[0] != "task-1" {
		t.Errorf("expected [task-1] stuck, got %v", stuck)
	}

	// Remove completed task
	tl.Remove("task-1")
	stuck = tl.StuckTasks(60 * time.Second)
	if len(stuck) != 0 {
		t.Errorf("expected 0 stuck after remove, got %d", len(stuck))
	}
}

// TestTaskLiveness_AssignExpireWorker: dispatch-time assignment makes a
// dead worker's tasks — reported or not — immediately stuck via
// ExpireWorker, without ever letting bare assignment (a task queued on a
// busy-but-alive worker) count as stuck. Regression companion to the
// 2026-08-11 wedge fix.
func TestTaskLiveness_AssignExpireWorker(t *testing.T) {
	tl := NewTaskLiveness()
	tl.Assign("t1", "w1")
	tl.Assign("t2", "w1")
	tl.Assign("t3", "w2")
	tl.Update([]string{"t2"}, time.Now()) // t2 reported; t1/t3 never

	// Assignment alone is not stuckness.
	if s := tl.StuckTasks(time.Nanosecond); len(s) != 1 || s[0] != "t2" {
		// only t2 has a clock, and only after it goes stale
		if len(s) != 0 {
			t.Fatalf("pre-expiry stuck = %v, want at most [t2]", s)
		}
	}

	// Expire w1: both its tasks are immediately stuck, w2's untouched.
	expired := tl.ExpireWorker("w1")
	sort.Strings(expired)
	if len(expired) != 2 || expired[0] != "t1" || expired[1] != "t2" {
		t.Fatalf("expired = %v, want [t1 t2]", expired)
	}
	stuck := tl.StuckTasks(time.Hour)
	sort.Strings(stuck)
	if len(stuck) != 2 || stuck[0] != "t1" || stuck[1] != "t2" {
		t.Fatalf("post-expiry stuck = %v, want [t1 t2]", stuck)
	}

	// Remove (result arrived / re-dispatched) clears the binding: a second
	// expiry of the same worker resurrects nothing.
	tl.Remove("t1")
	tl.Remove("t2")
	if e := tl.ExpireWorker("w1"); len(e) != 0 {
		t.Fatalf("re-expiry after Remove = %v, want empty", e)
	}
	if s := tl.StuckTasks(time.Hour); len(s) != 0 {
		t.Fatalf("stuck after Remove = %v, want empty", s)
	}
}

func TestTaskLivenessFromHeartbeat(t *testing.T) {
	wr := &WorkerRegistry{
		workers:  make(map[string]*WorkerInfo),
		stale:    30 * time.Second,
		logger:   slog.Default(),
		Liveness: NewTaskLiveness(),
	}

	hb := distributed.WorkerHeartbeat{
		WorkerID:      "w-1",
		ActiveTaskIDs: []string{"task-a", "task-b"},
		Timestamp:     time.Now(),
	}
	wr.record(hb)

	// Both tasks should be tracked
	wr.Liveness.mu.RLock()
	if len(wr.Liveness.tasks) != 2 {
		t.Errorf("expected 2 tracked tasks, got %d", len(wr.Liveness.tasks))
	}
	wr.Liveness.mu.RUnlock()

	// Second heartbeat drops task-b (completed)
	hb2 := distributed.WorkerHeartbeat{
		WorkerID:      "w-1",
		ActiveTaskIDs: []string{"task-a"},
		Timestamp:     time.Now(),
	}
	wr.record(hb2)

	// task-a should be refreshed, task-b still tracked (not auto-removed)
	wr.Liveness.mu.RLock()
	if _, ok := wr.Liveness.tasks["task-a"]; !ok {
		t.Error("task-a should still be tracked")
	}
	wr.Liveness.mu.RUnlock()
}

// TestMarkWorkerSeenKeepsAliveWithoutHeartbeat is the regression test for
// the multi-signal liveness fix. A worker that registers and then stops
// heartbeating but continues publishing results / TaskProgress / gather
// batches must NOT be reaped. Without MarkWorkerSeen the coord would reap
// the worker after `stale` and orphan its in-flight tasks.
func TestMarkWorkerSeenKeepsAliveWithoutHeartbeat(t *testing.T) {
	wr := &WorkerRegistry{
		workers:  make(map[string]*WorkerInfo),
		stale:    50 * time.Millisecond,
		logger:   slog.Default(),
		Liveness: NewTaskLiveness(),
	}

	// Register via heartbeat so worker exists in the registry.
	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-busy", Timestamp: time.Now()})

	// Backdate heartbeat past the stale TTL — without other signals
	// the worker would be reaped on the next ReapStale tick.
	wr.workers["w-busy"].LastSeen = time.Now().Add(-1 * time.Hour)

	// Result publish from the worker arrives before the reaper runs.
	// MarkWorkerSeen updates LastSeen so the worker is no longer stale.
	wr.MarkWorkerSeen("w-busy")

	if reaped := wr.ReapStale(); reaped != 0 {
		t.Fatalf("expected 0 reaped after MarkWorkerSeen; got %d (multi-signal liveness regression)", reaped)
	}
	if wr.Count() != 1 {
		t.Fatalf("expected w-busy to remain active; got %d workers", wr.Count())
	}
}

// TestMarkWorkerSeenIgnoresEmptyAndUnknown verifies that the helper is
// safe to call on every worker→coord message without preconditions:
// empty workerID is a no-op, and a workerID for a worker not yet
// registered is a no-op (registration still requires a real heartbeat
// to populate cluster ID and pool stats).
func TestMarkWorkerSeenIgnoresEmptyAndUnknown(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   30 * time.Second,
		logger:  slog.Default(),
	}

	// Empty workerID: must not panic, must not register a worker.
	wr.MarkWorkerSeen("")
	if len(wr.workers) != 0 {
		t.Errorf("empty workerID should not create a worker; got %d", len(wr.workers))
	}

	// Unknown workerID: same — registration still requires a heartbeat.
	wr.MarkWorkerSeen("w-unknown")
	if len(wr.workers) != 0 {
		t.Errorf("unknown workerID should not create a worker; got %d", len(wr.workers))
	}
}

// TestReapStillFiresWhenAllSignalsStop confirms the reaper still works
// when no signals (heartbeat, TaskProgress, results, gather) arrive at
// all. Multi-signal liveness widens the proof-of-life inputs but does
// NOT extend the TTL itself.
func TestReapStillFiresWhenAllSignalsStop(t *testing.T) {
	wr := &WorkerRegistry{
		workers:  make(map[string]*WorkerInfo),
		stale:    50 * time.Millisecond,
		logger:   slog.Default(),
		Liveness: NewTaskLiveness(),
	}

	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-dead", Timestamp: time.Now()})
	wr.workers["w-dead"].LastSeen = time.Now().Add(-1 * time.Hour)
	// No MarkWorkerSeen call — simulating a truly dead worker.

	if reaped := wr.ReapStale(); reaped != 1 {
		t.Fatalf("expected 1 reaped (truly dead worker); got %d", reaped)
	}
}

// Reap grace (docs/design/reap-grace.md): a worker past the stale TTL but
// inside the grace window, holding outputs with no durable copy, is
// deferred instead of reaped. Regression for the 2026-08-13 Q03-R2
// reap-while-alive incident: ~105s NIC silence vs the 90s TTL invalidated
// the worker's not-yet-uploaded shuffle outputs and cost a 2m56 re-dispatch.
func TestReapStaleGraceDefersWorkerWithPendingOutputs(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   90 * time.Second,
		grace:   90 * time.Second,
		logger:  slog.Default(),
	}
	pending := 1
	wr.PendingNonDurable = func(string) int { return pending }

	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-silent", Timestamp: time.Now()})
	wr.workers["w-silent"].LastSeen = time.Now().Add(-105 * time.Second)

	if reaped := wr.ReapStale(); reaped != 0 {
		t.Fatalf("expected grace deferral, got %d reaped", reaped)
	}
	if !wr.MayRecover("w-silent") {
		t.Fatal("grace-deferred worker must report MayRecover")
	}
	if wr.IsAlive("w-silent") {
		t.Fatal("grace-deferred worker must not report IsAlive")
	}

	// Uploads landed, pending drains to zero: the same silence now reaps.
	pending = 0
	if reaped := wr.ReapStale(); reaped != 1 {
		t.Fatalf("expected reap once outputs drained, got %d", reaped)
	}
}

func TestReapStaleGraceExhaustedReapsDespitePendingOutputs(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   90 * time.Second,
		grace:   90 * time.Second,
		logger:  slog.Default(),
	}
	wr.PendingNonDurable = func(string) int { return 7 }

	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-gone", Timestamp: time.Now()})
	wr.workers["w-gone"].LastSeen = time.Now().Add(-181 * time.Second) // past stale+grace

	if reaped := wr.ReapStale(); reaped != 1 {
		t.Fatalf("grace is bounded: expected reap past stale+grace, got %d", reaped)
	}
	if wr.MayRecover("w-gone") {
		t.Fatal("reaped worker must not report MayRecover")
	}
}

func TestReapStaleNoGraceWithoutCallback(t *testing.T) {
	// Streaming exchange off → no PendingNonDurable callback → grace never
	// engages, reap behavior identical to pre-grace.
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   90 * time.Second,
		grace:   90 * time.Second,
		logger:  slog.Default(),
	}
	wr.record(distributed.WorkerHeartbeat{WorkerID: "w-silent", Timestamp: time.Now()})
	wr.workers["w-silent"].LastSeen = time.Now().Add(-105 * time.Second)

	if reaped := wr.ReapStale(); reaped != 1 {
		t.Fatalf("nil callback must disable grace, got %d reaped", reaped)
	}
}
