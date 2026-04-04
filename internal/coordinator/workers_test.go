package coordinator

import (
	"log/slog"
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
