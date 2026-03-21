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
