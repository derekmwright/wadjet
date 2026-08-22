package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
)

// TestPickWorkerForAffinityBeforeLocality regression-tests the ordering fix
// in pickWorkerFor (docs/adr/0008-task-placement-policy.md): a task that
// carries BOTH an AffinityWorkerID and InputLocations hints that all point
// at a DIFFERENT worker must place on the affinity worker, not the
// locality one.
//
// This is exactly the probe-split broadcast-join shape: the task's
// InputLocations hint at its (small, replicated) broadcast build's single
// producer, while AffinityWorkerID names the rendezvous owner of its
// base-table probe files — the worker whose cache actually holds the
// task's bytes. Before this fix, pickLocalityWorkerFrom ran first and saw
// "all hints on one worker," placing the whole task on the build producer
// (docs/design/scan-affinity.md §probe-split affinity — the straggler tier
// this change exists to remove).
func TestPickWorkerForAffinityBeforeLocality(t *testing.T) {
	srv := dataplane.NewServer(dataplane.ServerConfig{
		Addr:      "127.0.0.1:0",
		ClusterID: "test-cluster",
	}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientA := dataplane.NewClient(dataplane.ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "w-a",
		ReconnectBackoff: 25 * time.Millisecond,
	}, nil)
	clientA.Start(ctx)
	defer clientA.Stop()

	clientB := dataplane.NewClient(dataplane.ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "w-b",
		ReconnectBackoff: 25 * time.Millisecond,
	}, nil)
	clientB.Start(ctx)
	defer clientB.Stop()

	if !waitForCondition(t, 5*time.Second, func() bool {
		return len(srv.ConnectedWorkers()) == 2
	}) {
		t.Fatalf("workers never connected, got %v", srv.ConnectedWorkers())
	}

	s := NewScheduler(nil, nil)
	s.SetDataPlaneServer(srv)
	s.localityPlacement = true
	s.registry = &WorkerRegistry{
		workers: map[string]*WorkerInfo{
			"w-a": {WorkerID: "w-a", PeerAddr: "10.0.0.1:9095", LastSeen: time.Now()},
			"w-b": {WorkerID: "w-b", PeerAddr: "10.0.0.2:9095", LastSeen: time.Now()},
		},
		stale: 90 * time.Second,
	}

	task := distributed.Task{
		// Every InputLocations hint points at w-a's PeerAddr — the shape
		// pickLocalityWorkerFrom treats as "all hints on one worker."
		InputLocations: map[string]string{
			"queries/q/build-1/cache.wshf": "10.0.0.1:9095",
		},
		// But the task's actual bytes (its base-table probe slice)
		// rendezvous-hash to w-b.
		AffinityWorkerID: "w-b",
	}

	id, method, ok := s.pickWorkerFor(task, nil, 1)
	if !ok {
		t.Fatal("pickWorkerFor returned ok=false")
	}
	if method != "affine" {
		t.Errorf("method = %q, want %q (affinity must outrank locality)", method, "affine")
	}
	if id != "w-b" {
		t.Errorf("workerID = %q, want %q", id, "w-b")
	}

	// affinityBeforeLocality OFF restores the pre-2026-08-22 order: the
	// same task now places by locality (on w-a), not affinity.
	prev := affinityBeforeLocality.Set(false)
	t.Cleanup(func() { affinityBeforeLocality.Set(prev) })

	id, method, ok = s.pickWorkerFor(task, nil, 1)
	if !ok {
		t.Fatal("pickWorkerFor returned ok=false")
	}
	if method != "local" {
		t.Errorf("method = %q, want %q (switch off must restore the old order)", method, "local")
	}
	if id != "w-a" {
		t.Errorf("workerID = %q, want %q", id, "w-a")
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return cond()
}
