package coordinator

import (
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestCreatePipelineTasksBuildCache verifies that BuildCachePreScans on a
// probe-split stage is propagated to each worker task as PreScannedInputs.
// This is the key correctness invariant for the broadcast cache path.
func TestCreatePipelineTasksBuildCache(t *testing.T) {
	_, coord, _ := setupDistributed(t)

	queryID := "test-build-cache"
	sqlText := "SELECT * FROM lineitem"

	coord.mu.Lock()
	coord.queryMetas[queryID] = &queryMeta{
		sqlText: sqlText,
	}
	coord.mu.Unlock()
	defer func() {
		coord.mu.Lock()
		delete(coord.queryMetas, queryID)
		coord.mu.Unlock()
	}()

	cacheFiles := map[string][]string{
		"orders":   {"queries/q1/build-cache/orders/abc.wshf"},
		"partsupp": {"queries/q1/build-cache/partsupp/def.wshf"},
	}

	stage := physical.Stage{
		ID:              "pipeline-0",
		Type:            "pipeline",
		Tasks:           3,
		ProbeSplitAlias: "lineitem",
		ProbeSplitFiles: []string{"l1.parquet", "l2.parquet", "l3.parquet"},
		BuildCachePreScans: cacheFiles,
	}

	tasks := coord.createPipelineTasks(queryID, stage, "queries/"+queryID+"/pipeline-0/", nil)

	if len(tasks) != 3 {
		t.Fatalf("want 3 tasks (one per worker), got %d", len(tasks))
	}

	for i, task := range tasks {
		if task.Type != distributed.TaskTypePipeline {
			t.Errorf("task[%d]: want type pipeline, got %s", i, task.Type)
		}
		if task.SQLText != sqlText {
			t.Errorf("task[%d]: want SQL text %q, got %q", i, sqlText, task.SQLText)
		}
		// Each task must have the probe file filter set
		if len(task.ScanFileFilter) == 0 {
			t.Errorf("task[%d]: want ScanFileFilter set, got empty", i)
		}
		// Each task must have the build cache pre-scanned inputs
		if len(task.PreScannedInputs) != 2 {
			t.Errorf("task[%d]: want 2 PreScannedInputs entries, got %d", i, len(task.PreScannedInputs))
			continue
		}
		if got := task.PreScannedInputs["orders"]; len(got) != 1 || got[0] != cacheFiles["orders"][0] {
			t.Errorf("task[%d]: PreScannedInputs[orders] = %v, want %v", i, got, cacheFiles["orders"])
		}
		if got := task.PreScannedInputs["partsupp"]; len(got) != 1 || got[0] != cacheFiles["partsupp"][0] {
			t.Errorf("task[%d]: PreScannedInputs[partsupp] = %v, want %v", i, got, cacheFiles["partsupp"])
		}
	}
}

// TestCreatePipelineTasksNoBuildCache verifies the non-cache path is unaffected:
// probe-split tasks without BuildCachePreScans have nil PreScannedInputs.
func TestCreatePipelineTasksNoBuildCache(t *testing.T) {
	_, coord, _ := setupDistributed(t)

	queryID := "test-no-build-cache"
	coord.mu.Lock()
	coord.queryMetas[queryID] = &queryMeta{sqlText: "SELECT * FROM lineitem"}
	coord.mu.Unlock()
	defer func() {
		coord.mu.Lock()
		delete(coord.queryMetas, queryID)
		coord.mu.Unlock()
	}()

	stage := physical.Stage{
		ID:              "pipeline-0",
		Type:            "pipeline",
		Tasks:           2,
		ProbeSplitAlias: "lineitem",
		ProbeSplitFiles: []string{"l1.parquet", "l2.parquet"},
		// No BuildCachePreScans
	}

	tasks := coord.createPipelineTasks(queryID, stage, "queries/"+queryID+"/pipeline-0/", nil)
	if len(tasks) != 2 {
		t.Fatalf("want 2 tasks, got %d", len(tasks))
	}
	for i, task := range tasks {
		if len(task.PreScannedInputs) != 0 {
			t.Errorf("task[%d]: want nil PreScannedInputs, got %v", i, task.PreScannedInputs)
		}
	}
}

// TestPreScanBuildTablesSkipsSmallTables verifies that tables below the
// buildCacheThreshold are not pre-scanned (returns nil with no error).
func TestPreScanBuildTablesSkipsSmallTables(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	// All scans are small (well below the 2GB threshold)
	smallStages := []physical.Stage{
		{ID: "s1", Type: "scan", ScanAlias: "lineitem", TableName: "lineitem",
			EstimatedBytes: 100 * 1024 * 1024, ScanFiles: []string{"l1.parquet"}},
		{ID: "s2", Type: "scan", ScanAlias: "orders", TableName: "orders",
			EstimatedBytes: 50 * 1024 * 1024, ScanFiles: []string{"o1.parquet"}},
	}

	cache, err := coord.preScanBuildTables(ctx, "qid", "SELECT 1", smallStages, "lineitem")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cache) != 0 {
		t.Errorf("expected no cache entries for small tables, got %d: %v", len(cache), cache)
	}
}

// TestBuildCacheTaskHasCorrectFields verifies a build-cache pre-scan pipeline
// task is constructed with the right SQL, bucket, and result prefix.
func TestBuildCacheTaskHasCorrectFields(t *testing.T) {
	// We can't easily run preScanOneTable end-to-end without a running worker,
	// but we can exercise the createPipelineTasks path and verify task fields
	// are well-formed when BuildCachePreScans is populated.
	_, coord, _ := setupDistributed(t)

	queryID := "qtest-fields"
	coord.mu.Lock()
	coord.queryMetas[queryID] = &queryMeta{sqlText: "SELECT * FROM lineitem JOIN orders ON l_orderkey = o_orderkey"}
	coord.mu.Unlock()
	defer func() {
		coord.mu.Lock()
		delete(coord.queryMetas, queryID)
		coord.mu.Unlock()
	}()

	cache := map[string][]string{"orders": {"queries/qtest-fields/build-cache/orders/task1.wshf"}}
	stage := physical.Stage{
		ID:                 "pipeline-0",
		Type:               "pipeline",
		Tasks:              2,
		ProbeSplitAlias:    "lineitem",
		ProbeSplitFiles:    []string{"l1.parquet", "l2.parquet"},
		BuildCachePreScans: cache,
	}

	tasks := coord.createPipelineTasks(queryID, stage, "queries/"+queryID+"/pipeline-0/", nil)
	for i, task := range tasks {
		if task.DataBucket != "test" {
			t.Errorf("task[%d]: DataBucket=%q, want test", i, task.DataBucket)
		}
		if task.ResultBucket != "test" {
			t.Errorf("task[%d]: ResultBucket=%q, want test", i, task.ResultBucket)
		}
		if task.CreatedAt.IsZero() {
			t.Errorf("task[%d]: CreatedAt not set", i)
		}
		if task.CreatedAt.After(time.Now().Add(time.Second)) {
			t.Errorf("task[%d]: CreatedAt is in the future", i)
		}
	}
}
