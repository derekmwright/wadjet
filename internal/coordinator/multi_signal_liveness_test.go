package coordinator

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// TestWorkerRegistry_TaskProgressKeepsWorkerAlive is a regression test for
// the 2026-04-29 SF10 Q03 hot-potato pattern. Workers GC-thrashed under
// lineitem broadcast-join build pressure and their global heartbeat
// goroutine starved past the 90s stale TTL — even while per-task goroutines
// still emitted TaskProgress. Coord reaped them, JetStream redelivered to
// other equally-stressed workers, and the cluster cycled until query
// timeout.
//
// The fix has the WorkerRegistry treat TaskProgress messages as a
// heartbeat-equivalent liveness signal: any TaskProgress for a registered
// worker bumps its LastSeen. This test simulates the exact failure shape
// (heartbeat silent, TaskProgress flowing) and asserts the worker is NOT
// reaped.
func TestWorkerRegistry_TaskProgressKeepsWorkerAlive(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("NATS: %v", err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	// Aggressive 200ms stale TTL so the test runs in <2s.
	wr := NewWorkerRegistry(nc, logger, 200*time.Millisecond)
	defer wr.Close()

	// Step 1: register a worker via heartbeat, the only path that creates
	// a WorkerInfo entry. After this the worker should be active.
	hb := distributed.WorkerHeartbeat{
		WorkerID:      "w-progress",
		ClusterID:     "test",
		ActiveTaskIDs: []string{"task-A"},
		Timestamp:     time.Now(),
	}
	data, err := distributed.Marshal(hb)
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(distributed.SubjectHeartbeat, data); err != nil {
		t.Fatal(err)
	}
	nc.Flush()
	time.Sleep(50 * time.Millisecond)

	// Step 2: stop heartbeats. Emit only TaskProgress for the same worker,
	// at 100ms intervals (faster than the 200ms stale TTL). With the fix,
	// each TaskProgress arrival bumps LastSeen and prevents reap. Without
	// the fix, the worker would be reaped after 200ms of heartbeat silence.
	publishProgress := func() {
		tp := distributed.TaskProgress{
			QueryID:       "q",
			StageID:       "s",
			TaskID:        "task-A",
			WorkerID:      "w-progress",
			RowsProcessed: 1,
			Timestamp:     time.Now(),
		}
		data, err := distributed.Marshal(tp)
		if err != nil {
			t.Fatal(err)
		}
		if err := nc.Publish(distributed.TaskProgressSubject(tp.QueryID, tp.TaskID), data); err != nil {
			t.Fatal(err)
		}
	}

	// Run the no-heartbeat phase for 600ms = 3× the stale TTL.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		publishProgress()
		nc.Flush()
		time.Sleep(100 * time.Millisecond)
	}

	// Step 3: ReapStale should leave the worker alive because TaskProgress
	// arrivals kept LastSeen fresh. Without the fix, the worker would have
	// been reaped 400ms ago.
	if reaped := wr.ReapStale(); reaped != 0 {
		t.Fatalf("worker was reaped despite active TaskProgress: ReapStale returned %d", reaped)
	}
	active := wr.ActiveWorkers()
	if len(active) != 1 || active[0].WorkerID != "w-progress" {
		t.Fatalf("worker not active after TaskProgress flow: active=%+v", active)
	}
}

// TestWorkerRegistry_ReapStillFiresWhenTaskProgressStops verifies the gate
// works in the other direction: when TaskProgress stops too, the worker is
// reaped at the configured stale TTL. We don't want the multi-signal
// behavior to swallow genuine deaths.
func TestWorkerRegistry_ReapStillFiresWhenTaskProgressStops(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("NATS: %v", err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	wr := NewWorkerRegistry(nc, logger, 200*time.Millisecond)
	defer wr.Close()

	hb := distributed.WorkerHeartbeat{
		WorkerID:  "w-dead",
		ClusterID: "test",
		Timestamp: time.Now(),
	}
	data, _ := distributed.Marshal(hb)
	nc.Publish(distributed.SubjectHeartbeat, data)
	nc.Flush()
	time.Sleep(50 * time.Millisecond)

	// No further heartbeat, no TaskProgress. After the stale TTL the worker
	// is genuinely silent.
	time.Sleep(400 * time.Millisecond)
	if reaped := wr.ReapStale(); reaped != 1 {
		t.Fatalf("genuinely silent worker not reaped: ReapStale returned %d", reaped)
	}
}
