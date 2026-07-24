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

func TestPickLocalityWorkerFrom(t *testing.T) {
	active := []*WorkerInfo{
		{WorkerID: "w1", PeerAddr: "10.0.0.1:9095"},
		{WorkerID: "w2", PeerAddr: "10.0.0.2:9095"},
		{WorkerID: "w3"}, // no peer addr advertised
	}
	oneWorker := map[string]string{
		"queries/q/s1/a.wshf": "10.0.0.1:9095",
		"queries/q/s1/b.wshf": "10.0.0.1:9095",
	}
	tests := []struct {
		name          string
		locs          map[string]string
		connected     []string
		batchAssigned map[string]int
		batchLen      int
		wantID        string
		wantOK        bool
	}{
		{"all hints on one worker", oneWorker,
			[]string{"w1", "w2"}, nil, 3, "w1", true},
		{"hints span workers fall through", map[string]string{
			"queries/q/s1/a.wshf": "10.0.0.1:9095",
			"queries/q/s1/b.wshf": "10.0.0.2:9095",
		}, []string{"w1", "w2"}, nil, 3, "", false},
		{"no hints fall through", nil, []string{"w1", "w2"}, nil, 3, "", false},
		{"hinted worker not connected", oneWorker,
			[]string{"w2"}, nil, 3, "", false},
		{"unknown peer addr", map[string]string{
			"queries/q/s1/a.wshf": "10.9.9.9:9095",
		}, []string{"w1", "w2"}, nil, 3, "", false},
		{"batch cap bites", oneWorker,
			[]string{"w1", "w2"}, map[string]int{"w1": 2}, 3, "", false},
		{"batch cap admits under ceil", oneWorker,
			[]string{"w1", "w2"}, map[string]int{"w1": 1}, 3, "w1", true},
		{"no connected workers", oneWorker, nil, nil, 3, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := distributed.Task{InputLocations: tc.locs}
			id, ok := pickLocalityWorkerFrom(task, tc.connected, active, tc.batchAssigned, tc.batchLen)
			if id != tc.wantID || ok != tc.wantOK {
				t.Fatalf("pickLocalityWorkerFrom = (%q,%v), want (%q,%v)", id, ok, tc.wantID, tc.wantOK)
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
	tr := newTaskRetrier(tasks, true, rep.republish, slog.Default(), "s", nil)

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
	tr := newTaskRetrier(retryTestTasks(3), false, nil, slog.Default(), "s", nil)
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

func TestMinBatchWorkers(t *testing.T) {
	tests := []struct {
		name      string
		connected []string
		assigned  map[string]int
		want      []string
	}{
		{"empty batch keeps all", []string{"w1", "w2", "w3"},
			map[string]int{}, []string{"w1", "w2", "w3"}},
		{"loaded worker drops out", []string{"w1", "w2", "w3"},
			map[string]int{"w1": 1}, []string{"w2", "w3"}},
		{"min-count subset survives", []string{"w1", "w2", "w3"},
			map[string]int{"w1": 2, "w2": 1, "w3": 1}, []string{"w2", "w3"}},
		{"no connected workers", nil, map[string]int{"w1": 1}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := minBatchWorkers(tc.connected, tc.assigned)
			if len(got) != len(tc.want) {
				t.Fatalf("minBatchWorkers = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("minBatchWorkers = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestBinpackSpreadFanout replays the 2026-07-20 Q20 pathology: heartbeat
// pool stats are stale for the whole fan-out (w3 reports most free the
// entire time) and per-task estimates are too small to flip the ranking.
// Without the same-batch restriction all 12 tasks land on w3; with it the
// fan-out spreads 4/4/4 while w3 still wins each tie-break round.
func TestBinpackSpreadFanout(t *testing.T) {
	active := []*WorkerInfo{
		{WorkerID: "w1", PoolBudget: 1000, PoolUsed: 500},
		{WorkerID: "w2", PoolBudget: 1000, PoolUsed: 400},
		{WorkerID: "w3", PoolBudget: 1000, PoolUsed: 100},
	}
	connected := []string{"w1", "w2", "w3"}
	inflight := map[string]int64{} // estimates tiny: 1 byte per task
	batchAssigned := map[string]int{}
	counts := map[string]int{}
	for i := 0; i < 12; i++ {
		id, ok := pickLeastLoadedWorker(minBatchWorkers(connected, batchAssigned), active, inflight)
		if !ok {
			t.Fatalf("pick %d failed", i)
		}
		batchAssigned[id]++
		counts[id]++
		inflight[id]++ // per-task charge too small to matter
	}
	if counts["w1"] != 4 || counts["w2"] != 4 || counts["w3"] != 4 {
		t.Fatalf("fan-out spread = %v, want 4/4/4", counts)
	}
	// Sanity: without the restriction, the stale ranking clumps.
	clump := map[string]int{}
	inflight = map[string]int64{}
	for i := 0; i < 12; i++ {
		id, _ := pickLeastLoadedWorker(connected, active, inflight)
		clump[id]++
		inflight[id]++
	}
	if clump["w3"] != 12 {
		t.Fatalf("unrestricted control = %v, want all 12 on w3", clump)
	}
}
