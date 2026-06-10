package coordinator

import (
	"log/slog"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

func TestPickLeastLoadedWorker(t *testing.T) {
	active := []*WorkerInfo{
		{WorkerID: "w1", PoolBudget: 1000, PoolUsed: 800}, // free 200
		{WorkerID: "w2", PoolBudget: 1000, PoolUsed: 100}, // free 900
		{WorkerID: "w3", PoolBudget: 2000, PoolUsed: 500}, // free 1500
		{WorkerID: "legacy"},                              // no pool stats
	}
	tests := []struct {
		name      string
		connected []string
		inflight  map[string]int64
		wantID    string
		wantOK    bool
	}{
		{"most free wins", []string{"w1", "w2", "w3"}, nil, "w3", true},
		{"inflight charges count", []string{"w1", "w2", "w3"},
			map[string]int64{"w3": 1400}, "w2", true}, // w3 free drops to 100
		{"disconnected workers excluded", []string{"w1", "w2"}, nil, "w2", true},
		{"unregistered and legacy skipped", []string{"legacy", "ghost"}, nil, "", false},
		{"no connected workers", nil, nil, "", false},
		{"all overcommitted picks least bad", []string{"w1", "w2"},
			map[string]int64{"w1": 500, "w2": 2000}, "w1", true}, // -300 vs -1100
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := pickLeastLoadedWorker(tc.connected, active, tc.inflight)
			if id != tc.wantID || ok != tc.wantOK {
				t.Fatalf("pickLeastLoadedWorker = (%q,%v), want (%q,%v)", id, ok, tc.wantID, tc.wantOK)
			}
		})
	}
}

func TestSchedulerInflightLifecycle(t *testing.T) {
	s := NewScheduler(nil, nil)
	s.noteInflight(distributed.Task{ID: "t1", EstimatedBytes: 100}, "w1")
	s.noteInflight(distributed.Task{ID: "t2", EstimatedBytes: 50}, "w1")
	s.noteInflight(distributed.Task{ID: "t3", EstimatedBytes: 0}, "w1") // no estimate → not tracked
	s.noteInflight(distributed.Task{ID: "t4", EstimatedBytes: 70}, "w2")

	per := s.inflightByWorker()
	if per["w1"] != 150 || per["w2"] != 70 {
		t.Fatalf("inflight = %v, want w1:150 w2:70", per)
	}

	// Result arrival releases the charge.
	s.TaskDone("t1")
	if per := s.inflightByWorker(); per["w1"] != 50 {
		t.Fatalf("after TaskDone, w1 = %d, want 50", per["w1"])
	}

	// TTL expiry hands accounting off to heartbeat PoolUsed.
	s.inflightMu.Lock()
	e := s.inflight["t2"]
	e.expires = time.Now().Add(-time.Second)
	s.inflight["t2"] = e
	s.inflightMu.Unlock()
	if per := s.inflightByWorker(); per["w1"] != 0 {
		t.Fatalf("after TTL expiry, w1 = %d, want 0", per["w1"])
	}
}

// TestTaskRetrier_GrowEstimateOnRetry: every re-dispatch (failure or stuck)
// doubles the admission estimate, bounded by the attempt cap; tasks with no
// estimate stay at 0 (unknown).
func TestTaskRetrier_GrowEstimateOnRetry(t *testing.T) {
	rep := &collectingRepublisher{}
	tasks := retryTestTasks(2)
	tasks[0].EstimatedBytes = 100
	// tasks[1] stays 0.
	tr := newTaskRetrier(tasks, true, rep.republish, slog.Default(), "s")

	if tr.Observe(failResult("a", "boom")) {
		t.Fatal("terminal too early")
	}
	got := waitRepublished(t, rep, 1)
	if got[0].EstimatedBytes != 200 {
		t.Fatalf("retry 1 estimate = %d, want 200", got[0].EstimatedBytes)
	}
	if re, _ := tr.RetryStuck([]string{"a"}); len(re) != 1 {
		t.Fatalf("stuck retry = %v, want [a]", re)
	}
	got = waitRepublished(t, rep, 2)
	if got[1].EstimatedBytes != 400 {
		t.Fatalf("retry 2 estimate = %d, want 400", got[1].EstimatedBytes)
	}

	if tr.Observe(failResult("b", "boom")) {
		t.Fatal("terminal too early for b")
	}
	got = waitRepublished(t, rep, 3)
	if got[2].ID != "b" || got[2].EstimatedBytes != 0 {
		t.Fatalf("no-estimate retry = %+v, want b with 0", got[2])
	}
}

// TestTaskRetrier_TotalBytes: worker-reported SizeBytes sums across
// successful tasks; duplicates don't double-count; failures contribute 0.
func TestTaskRetrier_TotalBytes(t *testing.T) {
	tr := newTaskRetrier(retryTestTasks(3), false, nil, slog.Default(), "s")
	rA := okResult("a", "f-a")
	rA.SizeBytes = 100
	rB := okResult("b", "f-b")
	rB.SizeBytes = 250
	tr.Observe(rA)
	tr.Observe(rB)
	tr.Observe(rA) // duplicate
	tr.Observe(failResult("c", "boom"))
	if got := tr.TotalBytes(); got != 350 {
		t.Fatalf("TotalBytes = %d, want 350", got)
	}
}

func TestEstimateComputeTaskBytes(t *testing.T) {
	join := physical.Stage{
		Type:         physical.StageBroadcastJoin,
		Dependencies: []string{"probe-dep", "build-dep"},
		LeftDepStage: "probe-dep",
	}
	tests := []struct {
		name       string
		stage      physical.Stage
		inputs     map[string]StageOutput
		numTasks   int
		probeSplit bool
		want       int64
	}{
		{"partitioned splits", physical.Stage{}, map[string]StageOutput{
			"d": {Kind: OutputPartitioned, Bytes: 900},
		}, 3, false, 300},
		{"replicated charges full", physical.Stage{}, map[string]StageOutput{
			"d": {Kind: OutputReplicated, Bytes: 900},
		}, 3, false, 900},
		{"singlepart charges full", physical.Stage{}, map[string]StageOutput{
			"d": {Kind: OutputSinglePart, Bytes: 900},
		}, 3, false, 900},
		{"probe-split slices the probe", join, map[string]StageOutput{
			"probe-dep": {Kind: OutputSinglePart, Bytes: 600},
			"build-dep": {Kind: OutputReplicated, Bytes: 90},
		}, 3, true, 290}, // 600/3 + 90
		{"unknown sizes contribute nothing", physical.Stage{}, map[string]StageOutput{
			"d1": {Kind: OutputPartitioned, Bytes: 0},
			"d2": {Kind: OutputReplicated, Bytes: 100},
		}, 2, false, 100},
		{"zero tasks clamps to 1", physical.Stage{}, map[string]StageOutput{
			"d": {Kind: OutputPartitioned, Bytes: 500},
		}, 0, false, 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := estimateComputeTaskBytes(tc.stage, tc.inputs, tc.numTasks, tc.probeSplit)
			if got != tc.want {
				t.Fatalf("estimateComputeTaskBytes = %d, want %d", got, tc.want)
			}
		})
	}
}
