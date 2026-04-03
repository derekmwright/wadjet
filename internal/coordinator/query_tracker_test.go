package coordinator

import (
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// --- QueryState.String() ---

func TestQueryStateString(t *testing.T) {
	tests := []struct {
		state QueryState
		want  string
	}{
		{QueryStatePending, "pending"},
		{QueryStateRunning, "running"},
		{QueryStateCompleted, "completed"},
		{QueryStateFailed, "failed"},
		{QueryStateCancelled, "cancelled"},
		{QueryState(99), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.state.String()
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// --- QueryTracker lifecycle ---

func TestQueryTrackerRegisterAndGet(t *testing.T) {
	qt := NewQueryTracker()

	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", Type: distributed.TaskTypeScan, TotalTasks: 3},
		"s1": {StageID: "s1", Type: distributed.TaskTypeAggregate, TotalTasks: 1, Dependencies: []string{"s0"}},
	}
	qt.Register("q1", "SELECT * FROM t", stages, []string{"s0", "s1"})

	info := qt.Get("q1")
	if info == nil {
		t.Fatal("expected to find query q1")
	}
	if info.State != QueryStatePending {
		t.Errorf("expected pending, got %s", info.State)
	}
	if info.SQL != "SELECT * FROM t" {
		t.Errorf("SQL: got %q", info.SQL)
	}
	if len(info.Stages) != 2 {
		t.Errorf("expected 2 stages, got %d", len(info.Stages))
	}
	if len(info.StageOrder) != 2 {
		t.Errorf("expected 2 stage order, got %d", len(info.StageOrder))
	}
}

func TestQueryTrackerGetMissing(t *testing.T) {
	qt := NewQueryTracker()
	info := qt.Get("nonexistent")
	if info != nil {
		t.Fatal("expected nil for missing query")
	}
}

func TestQueryTrackerStart(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SELECT 1", map[string]*StageInfo{}, nil)
	qt.Start("q1")

	info := qt.Get("q1")
	if info.State != QueryStateRunning {
		t.Errorf("expected running, got %s", info.State)
	}
}

func TestQueryTrackerStartMissing(t *testing.T) {
	qt := NewQueryTracker()
	qt.Start("nonexistent") // should not panic
}

func TestQueryTrackerComplete(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SELECT 1", map[string]*StageInfo{}, nil)
	qt.Start("q1")
	qt.Complete("q1")

	info := qt.Get("q1")
	if info.State != QueryStateCompleted {
		t.Errorf("expected completed, got %s", info.State)
	}
	if info.EndTime.IsZero() {
		t.Error("expected non-zero EndTime")
	}
}

func TestQueryTrackerCompleteMissing(t *testing.T) {
	qt := NewQueryTracker()
	qt.Complete("nonexistent") // should not panic
}

func TestQueryTrackerFail(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SELECT 1", map[string]*StageInfo{}, nil)
	qt.Start("q1")
	qt.Fail("q1", "something went wrong")

	info := qt.Get("q1")
	if info.State != QueryStateFailed {
		t.Errorf("expected failed, got %s", info.State)
	}
	if info.Error != "something went wrong" {
		t.Errorf("Error: got %q", info.Error)
	}
	if info.EndTime.IsZero() {
		t.Error("expected non-zero EndTime")
	}
}

func TestQueryTrackerFailMissing(t *testing.T) {
	qt := NewQueryTracker()
	qt.Fail("nonexistent", "error") // should not panic
}

func TestQueryTrackerCancel(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SELECT 1", map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 1},
	}, []string{"s0"})
	qt.Start("q1")
	qt.Cancel("q1")

	info := qt.Get("q1")
	if info.State != QueryStateCancelled {
		t.Errorf("expected cancelled, got %s", info.State)
	}
	if info.EndTime.IsZero() {
		t.Error("expected non-zero EndTime")
	}
}

func TestQueryTrackerCancelMissing(t *testing.T) {
	qt := NewQueryTracker()
	qt.Cancel("nonexistent") // should not panic
}

// --- RecordResult ---

func TestRecordResult(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 2},
	}
	qt.Register("q1", "SELECT 1", stages, []string{"s0"})
	qt.Start("q1")

	// First result
	complete := qt.RecordResult(distributed.ResultNotification{
		QueryID:    "q1",
		StageID:    "s0",
		TaskID:     "t1",
		Success:    true,
		ResultPath: "path/to/result1.parquet",
		NumRows:    100,
	})
	if complete {
		t.Error("expected incomplete after 1 of 2 results")
	}

	// Second result
	complete = qt.RecordResult(distributed.ResultNotification{
		QueryID:    "q1",
		StageID:    "s0",
		TaskID:     "t2",
		Success:    true,
		ResultPath: "path/to/result2.parquet",
		NumRows:    200,
	})
	if !complete {
		t.Error("expected complete after 2 of 2 results")
	}

	info := qt.Get("q1")
	if info.TotalRows != 300 {
		t.Errorf("TotalRows: got %d, want 300", info.TotalRows)
	}
	if len(info.ResultFiles) != 2 {
		t.Errorf("ResultFiles: got %d, want 2", len(info.ResultFiles))
	}
}

func TestRecordResultFailed(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 1},
	}
	qt.Register("q1", "SELECT 1", stages, []string{"s0"})
	qt.Start("q1")

	complete := qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1",
		StageID: "s0",
		TaskID:  "t1",
		Success: false,
		Error:   "something failed",
	})
	if !complete {
		t.Error("expected stage complete (1 failed of 1 total)")
	}

	info := qt.Get("q1")
	if info.TotalRows != 0 {
		t.Errorf("TotalRows: got %d, want 0", info.TotalRows)
	}
}

func TestRecordResultMissingQuery(t *testing.T) {
	qt := NewQueryTracker()
	complete := qt.RecordResult(distributed.ResultNotification{
		QueryID: "nonexistent",
		StageID: "s0",
		TaskID:  "t1",
		Success: true,
	})
	if complete {
		t.Error("expected false for missing query")
	}
}

func TestRecordResultMissingStage(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SELECT 1", map[string]*StageInfo{}, nil)
	qt.Start("q1")

	complete := qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1",
		StageID: "nonexistent",
		TaskID:  "t1",
		Success: true,
	})
	if complete {
		t.Error("expected false for missing stage")
	}
}

func TestRecordResultDedup(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 2},
	}
	qt.Register("q1", "SELECT 1", stages, []string{"s0"})
	qt.Start("q1")

	result := distributed.ResultNotification{
		QueryID:    "q1",
		StageID:    "s0",
		TaskID:     "t1",
		Success:    true,
		ResultPath: "result1.parquet",
		NumRows:    100,
	}

	// First recording
	complete := qt.RecordResult(result)
	if complete {
		t.Error("expected incomplete after 1 of 2")
	}

	// Duplicate (worker retry) — should be ignored
	complete = qt.RecordResult(result)
	if complete {
		t.Error("duplicate should not trigger completion")
	}

	info := qt.Get("q1")
	if info.TotalRows != 100 {
		t.Errorf("TotalRows after dedup: got %d, want 100", info.TotalRows)
	}
	if info.Stages["s0"].DoneTasks != 1 {
		t.Errorf("DoneTasks after dedup: got %d, want 1", info.Stages["s0"].DoneTasks)
	}

	// Second unique task completes the stage
	complete = qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t2",
		Success: true, NumRows: 200,
	})
	if !complete {
		t.Error("expected complete after 2 unique tasks")
	}
}

// --- IsComplete ---

func TestIsComplete(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 1},
		"s1": {StageID: "s1", TotalTasks: 1, Dependencies: []string{"s0"}},
	}
	qt.Register("q1", "SQL", stages, []string{"s0", "s1"})
	qt.Start("q1")

	if qt.IsComplete("q1") {
		t.Error("should not be complete with no results")
	}

	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t1", Success: true,
	})
	if qt.IsComplete("q1") {
		t.Error("should not be complete with only s0 done")
	}

	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s1", TaskID: "t2", Success: true,
	})
	if !qt.IsComplete("q1") {
		t.Error("should be complete with all stages done")
	}
}

func TestIsCompleteMissingQuery(t *testing.T) {
	qt := NewQueryTracker()
	if qt.IsComplete("nonexistent") {
		t.Error("should return false for missing query")
	}
}

// --- GetReadyStages ---

func TestGetReadyStages(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 1},
		"s1": {StageID: "s1", TotalTasks: 1, Dependencies: []string{"s0"}},
		"s2": {StageID: "s2", TotalTasks: 1, Dependencies: []string{"s0"}},
		"s3": {StageID: "s3", TotalTasks: 1, Dependencies: []string{"s1", "s2"}},
	}
	qt.Register("q1", "SQL", stages, []string{"s0", "s1", "s2", "s3"})
	qt.Start("q1")

	// Initially only s0 should be ready (no deps)
	// But s0 has TotalTasks=1 and DoneTasks=0, so it's "not started yet"
	// GetReadyStages only returns stages where all deps are complete
	ready := qt.GetReadyStages("q1")
	// s0 has no deps, so it's ready
	found := false
	for _, id := range ready {
		if id == "s0" {
			found = true
		}
	}
	if !found {
		t.Errorf("s0 should be ready (no dependencies): got %v", ready)
	}

	// Complete s0
	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t0", Success: true,
	})

	ready = qt.GetReadyStages("q1")
	// s1 and s2 should now be ready
	readyMap := make(map[string]bool)
	for _, id := range ready {
		readyMap[id] = true
	}
	if !readyMap["s1"] {
		t.Error("s1 should be ready after s0 completes")
	}
	if !readyMap["s2"] {
		t.Error("s2 should be ready after s0 completes")
	}
	if readyMap["s3"] {
		t.Error("s3 should NOT be ready (depends on s1 and s2)")
	}
}

func TestGetReadyStagesMissingQuery(t *testing.T) {
	qt := NewQueryTracker()
	ready := qt.GetReadyStages("nonexistent")
	if ready != nil {
		t.Errorf("expected nil for missing query, got %v", ready)
	}
}

// --- SetStageTasks ---

func TestSetStageTasks(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 0}, // initially unknown
	}
	qt.Register("q1", "SQL", stages, []string{"s0"})

	qt.SetStageTasks("q1", "s0", 5)

	info := qt.Get("q1")
	if info.Stages["s0"].TotalTasks != 5 {
		t.Errorf("expected 5 total tasks, got %d", info.Stages["s0"].TotalTasks)
	}
}

func TestSetStageTasksMissingQuery(t *testing.T) {
	qt := NewQueryTracker()
	qt.SetStageTasks("nonexistent", "s0", 5) // should not panic
}

func TestSetStageTasksMissingStage(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SQL", map[string]*StageInfo{}, nil)
	qt.SetStageTasks("q1", "nonexistent", 5) // should not panic
}

// --- UpdateResultPath ---

func TestUpdateResultPath(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 1},
	}
	qt.Register("q1", "SQL", stages, []string{"s0"})
	qt.Start("q1")

	qt.RecordResult(distributed.ResultNotification{
		QueryID:    "q1",
		StageID:    "s0",
		TaskID:     "t1",
		Success:    true,
		InlineData: []byte("inline"),
		NumRows:    10,
	})

	qt.UpdateResultPath("q1", "s0", "t1", "materialized/path.parquet")

	results := qt.StageResults("q1", "s0")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ResultPath != "materialized/path.parquet" {
		t.Errorf("ResultPath: got %q, want %q", results[0].ResultPath, "materialized/path.parquet")
	}
}

func TestUpdateResultPathMissingQuery(t *testing.T) {
	qt := NewQueryTracker()
	qt.UpdateResultPath("nonexistent", "s0", "t1", "path") // should not panic
}

func TestUpdateResultPathMissingStage(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SQL", map[string]*StageInfo{}, nil)
	qt.UpdateResultPath("q1", "nonexistent", "t1", "path") // should not panic
}

func TestUpdateResultPathMissingTask(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 1},
	}
	qt.Register("q1", "SQL", stages, []string{"s0"})
	qt.UpdateResultPath("q1", "s0", "nonexistent", "path") // should not panic
}

// --- List ---

func TestList(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SELECT 1", map[string]*StageInfo{}, nil)
	qt.Register("q2", "SELECT 2", map[string]*StageInfo{}, nil)
	qt.Register("q3", "SELECT 3", map[string]*StageInfo{}, nil)
	qt.Start("q1")
	qt.Complete("q1")
	qt.Start("q2")
	qt.Fail("q2", "error")

	queries := qt.List()
	if len(queries) != 3 {
		t.Fatalf("expected 3 queries, got %d", len(queries))
	}

	// Verify we got shallow copies
	stateMap := make(map[string]QueryState)
	for _, q := range queries {
		stateMap[q.QueryID] = q.State
	}
	if stateMap["q1"] != QueryStateCompleted {
		t.Errorf("q1: expected completed, got %s", stateMap["q1"])
	}
	if stateMap["q2"] != QueryStateFailed {
		t.Errorf("q2: expected failed, got %s", stateMap["q2"])
	}
	if stateMap["q3"] != QueryStatePending {
		t.Errorf("q3: expected pending, got %s", stateMap["q3"])
	}
}

func TestListEmpty(t *testing.T) {
	qt := NewQueryTracker()
	queries := qt.List()
	if len(queries) != 0 {
		t.Errorf("expected 0 queries, got %d", len(queries))
	}
}

// --- StageResults ---

func TestStageResults(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 2},
	}
	qt.Register("q1", "SQL", stages, []string{"s0"})
	qt.Start("q1")

	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t1", Success: true, NumRows: 10,
	})
	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t2", Success: true, NumRows: 20,
	})

	results := qt.StageResults("q1", "s0")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestStageResultsMissingQuery(t *testing.T) {
	qt := NewQueryTracker()
	results := qt.StageResults("nonexistent", "s0")
	if results != nil {
		t.Errorf("expected nil, got %v", results)
	}
}

func TestStageResultsMissingStage(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q1", "SQL", map[string]*StageInfo{}, nil)
	results := qt.StageResults("q1", "nonexistent")
	if results != nil {
		t.Errorf("expected nil, got %v", results)
	}
}

// --- Concurrent access ---

func TestQueryTrackerConcurrentAccess(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 100},
	}
	qt.Register("q-concurrent", "SQL", stages, []string{"s0"})
	qt.Start("q-concurrent")

	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func(i int) {
			qt.RecordResult(distributed.ResultNotification{
				QueryID:    "q-concurrent",
				StageID:    "s0",
				TaskID:     "t-" + time.Now().String(),
				Success:    true,
				NumRows:    1,
				ResultPath: "path/result.parquet",
			})
			qt.Get("q-concurrent")
			qt.IsComplete("q-concurrent")
			qt.GetReadyStages("q-concurrent")
			qt.List()
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}

	info := qt.Get("q-concurrent")
	if info.TotalRows != 100 {
		t.Errorf("TotalRows: got %d, want 100", info.TotalRows)
	}
}

// --- ActiveQueryIDs ---

func TestActiveQueryIDs(t *testing.T) {
	qt := NewQueryTracker()
	qt.Register("q-pending", "SQL", map[string]*StageInfo{}, nil)
	qt.Register("q-running", "SQL", map[string]*StageInfo{}, nil)
	qt.Start("q-running")
	qt.Register("q-done", "SQL", map[string]*StageInfo{}, nil)
	qt.Start("q-done")
	qt.Complete("q-done")
	qt.Register("q-failed", "SQL", map[string]*StageInfo{}, nil)
	qt.Start("q-failed")
	qt.Fail("q-failed", "err")

	active := qt.ActiveQueryIDs()
	if _, ok := active["q-pending"]; !ok {
		t.Error("pending query should be active")
	}
	if _, ok := active["q-running"]; !ok {
		t.Error("running query should be active")
	}
	if _, ok := active["q-done"]; ok {
		t.Error("completed query should not be active")
	}
	if _, ok := active["q-failed"]; ok {
		t.Error("failed query should not be active")
	}
	if len(active) != 2 {
		t.Errorf("expected 2 active queries, got %d", len(active))
	}
}

func TestActiveQueryIDsEmpty(t *testing.T) {
	qt := NewQueryTracker()
	active := qt.ActiveQueryIDs()
	if len(active) != 0 {
		t.Errorf("expected 0 active queries, got %d", len(active))
	}
}

// --- CollectResultPaths ---

func TestCollectResultPaths(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 2},
		"s1": {StageID: "s1", TotalTasks: 1, Dependencies: []string{"s0"}},
	}
	qt.Register("q1", "SQL", stages, []string{"s0", "s1"})
	qt.Start("q1")

	// Record s0 results — one with path, one inline (no path yet)
	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t1",
		Success: true, ResultPath: "path/t1.parquet", NumRows: 10,
	})
	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t2",
		Success: true, InlineData: []byte("data"), NumRows: 5,
	})

	// Before materialization: only t1 has a path
	paths := qt.CollectResultPaths("q1")
	if len(paths["s0"]) != 1 {
		t.Errorf("expected 1 path for s0 before materialization, got %d", len(paths["s0"]))
	}

	// Simulate materialization of t2
	qt.UpdateResultPath("q1", "s0", "t2", "path/t2.parquet")

	// After materialization: both have paths
	paths = qt.CollectResultPaths("q1")
	if len(paths["s0"]) != 2 {
		t.Errorf("expected 2 paths for s0 after materialization, got %d", len(paths["s0"]))
	}
}

func TestCollectResultPathsMissing(t *testing.T) {
	qt := NewQueryTracker()
	if qt.CollectResultPaths("nonexistent") != nil {
		t.Error("expected nil for missing query")
	}
}

// --- RecordResult with no ResultPath should not add to ResultFiles ---

func TestStalledStages(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 2},
		"s1": {StageID: "s1", TotalTasks: 1, Dependencies: []string{"s0"}},
	}
	qt.Register("q1", "SQL", stages, []string{"s0", "s1"})
	qt.Start("q1")
	qt.SetStageTasks("q1", "s0", 2)
	qt.MarkScheduled("q1", "s0")

	// No stall yet — just scheduled
	if stalled := qt.StalledStages("q1", 10*time.Minute); len(stalled) != 0 {
		t.Errorf("expected no stalled stages, got %v", stalled)
	}

	// Backdate ScheduledAt to simulate stall
	qt.mu.Lock()
	qt.queries["q1"].Stages["s0"].ScheduledAt = time.Now().Add(-15 * time.Minute)
	qt.mu.Unlock()

	// Only 0 of 2 tasks reported — should be stalled
	stalled := qt.StalledStages("q1", 10*time.Minute)
	if len(stalled) != 1 || stalled[0] != "s0" {
		t.Errorf("expected [s0] stalled, got %v", stalled)
	}

	// Record one result — still 1 pending, still stalled
	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t1", Success: true,
	})
	stalled = qt.StalledStages("q1", 10*time.Minute)
	if len(stalled) != 1 {
		t.Errorf("expected 1 stalled stage with 1 pending task, got %v", stalled)
	}

	// Record second result — stage complete, no longer stalled
	qt.RecordResult(distributed.ResultNotification{
		QueryID: "q1", StageID: "s0", TaskID: "t2", Success: true,
	})
	stalled = qt.StalledStages("q1", 10*time.Minute)
	if len(stalled) != 0 {
		t.Errorf("expected no stalled stages after completion, got %v", stalled)
	}

	// Unscheduled stage should not be detected
	stalled = qt.StalledStages("q1", 10*time.Minute)
	if len(stalled) != 0 {
		t.Errorf("unscheduled s1 should not be stalled, got %v", stalled)
	}
}

func TestRecordResultNoResultPath(t *testing.T) {
	qt := NewQueryTracker()
	stages := map[string]*StageInfo{
		"s0": {StageID: "s0", TotalTasks: 1},
	}
	qt.Register("q1", "SQL", stages, []string{"s0"})
	qt.Start("q1")

	qt.RecordResult(distributed.ResultNotification{
		QueryID:    "q1",
		StageID:    "s0",
		TaskID:     "t1",
		Success:    true,
		InlineData: []byte("data"),
		NumRows:    5,
	})

	info := qt.Get("q1")
	if len(info.ResultFiles) != 0 {
		t.Errorf("expected 0 result files (inline only), got %d", len(info.ResultFiles))
	}
}
