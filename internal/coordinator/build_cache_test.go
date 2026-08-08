package coordinator

import (
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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
		ID:                 "pipeline-0",
		Type:               "pipeline",
		Tasks:              3,
		ProbeSplitAlias:    "lineitem",
		ProbeSplitFiles:    []string{"l1.parquet", "l2.parquet", "l3.parquet"},
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

// TestPreScanBuildTablesInjectableThreshold verifies that BuildCacheThreshold
// on the Coordinator overrides the default 2GB constant, allowing the pre-scan
// path to trigger on small datasets (e.g., SF1 with a 1MB threshold).
func TestPreScanBuildTablesInjectableThreshold(t *testing.T) {
	_, coord, _ := setupDistributed(t)

	// Set a 1MB threshold so even small tables trigger the pre-scan path.
	coord.BuildCacheThreshold = 1 * 1024 * 1024 // 1 MB

	stages := []physical.Stage{
		{ID: "s1", Type: "scan", ScanAlias: "lineitem", TableName: "lineitem",
			EstimatedBytes: 50 * 1024 * 1024, ScanFiles: []string{"l1.parquet"}},
		{ID: "s2", Type: "scan", ScanAlias: "orders", TableName: "orders",
			EstimatedBytes: 5 * 1024 * 1024, ScanFiles: []string{"o1.parquet"}},
		{ID: "s3", Type: "scan", ScanAlias: "part", TableName: "part",
			EstimatedBytes: 500 * 1024, ScanFiles: []string{"p1.parquet"}},
	}

	// With 1MB threshold, orders (5MB) should be a candidate but part (500KB) should not.
	large := physical.LargeBuildScans(stages, "lineitem", coord.BuildCacheThreshold)
	if len(large) != 1 {
		t.Fatalf("expected 1 large build scan with 1MB threshold, got %d", len(large))
	}
	if large[0].ScanAlias != "orders" {
		t.Errorf("expected large scan alias 'orders', got %q", large[0].ScanAlias)
	}

	// Verify lineitem (probe table) is never included regardless of size.
	largeAll := physical.LargeBuildScans(stages, "lineitem", 1) // threshold=1 byte
	for _, s := range largeAll {
		if s.ScanAlias == "lineitem" {
			t.Error("probe table 'lineitem' must never be included in build scans")
		}
	}
}

// TestPreScanBuildTablesDefaultThreshold verifies that a zero BuildCacheThreshold
// falls back to the default 2GB constant and skips small tables.
func TestPreScanBuildTablesDefaultThreshold(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	// Don't set BuildCacheThreshold — leave at zero (use default).
	stages := []physical.Stage{
		{ID: "s1", Type: "scan", ScanAlias: "lineitem", TableName: "lineitem",
			EstimatedBytes: 50 * 1024 * 1024, ScanFiles: []string{"l1.parquet"}},
		{ID: "s2", Type: "scan", ScanAlias: "orders", TableName: "orders",
			EstimatedBytes: 5 * 1024 * 1024, ScanFiles: []string{"o1.parquet"}},
	}

	cache, err := coord.preScanBuildTables(ctx, "qid-default", "SELECT 1", stages, "lineitem")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cache) != 0 {
		t.Errorf("expected no cache entries with default 2GB threshold for 5MB table, got %d", len(cache))
	}
}

// TestPrunedScanColumnsFiltersAlienColumns verifies that prunedScanColumns
// strips columns that don't exist on the table's schema. The optimizer's
// column-pruning pass over-approximates by listing columns from neighbouring
// scans on every Stage.Columns slice (e.g. partsupp:1's stage in Q02 lists
// r_name, p_mfgr, etc.). The build-cache pre-scan SQL goes through the parser,
// which fails on invalid columns — so the intersect step here is what keeps
// the cache path from blowing up Q02 at SF100.
func TestPrunedScanColumnsFiltersAlienColumns(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	// Create a small table with a known schema.
	schema := []parquet.Column{
		{Name: "ps_partkey", Type: parquet.TypeInt64},
		{Name: "ps_suppkey", Type: parquet.TypeInt64},
		{Name: "ps_supplycost", Type: parquet.TypeFloat64},
	}
	if err := coord.catalog.CreateTable(ctx, "ps_test", parquet.Schema{Columns: schema}, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	// Stage.Columns over-approximates: contains real columns from partsupp
	// PLUS columns belonging to other tables in the join chain. The pruner
	// must drop the alien ones AND dedupe.
	stage := physical.Stage{
		ScanAlias: "partsupp:1",
		TableName: "ps_test",
		Columns: []string{
			"ps_partkey",    // real
			"r_name",        // alien (from region)
			"ps_suppkey",    // real
			"p_mfgr",        // alien (from part)
			"ps_partkey",    // duplicate — should be deduped
			"ps_supplycost", // real
			"s_phone",       // alien (from supplier)
		},
	}

	got := coord.prunedScanColumns(ctx, stage)
	want := []string{"ps_partkey", "ps_suppkey", "ps_supplycost"}
	if len(got) != len(want) {
		t.Fatalf("got %d columns %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("col %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

// TestPrunedScanColumnsEmptyAndUnknown returns nil for empty input or unknown
// table, which forces the caller to fall back to SELECT *.
func TestPrunedScanColumnsEmptyAndUnknown(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	// Empty Columns → nil.
	if got := coord.prunedScanColumns(ctx, physical.Stage{TableName: "anything"}); got != nil {
		t.Errorf("empty columns: got %v, want nil", got)
	}

	// Unknown table → nil (catalog lookup error).
	stage := physical.Stage{TableName: "no_such_table", Columns: []string{"a", "b"}}
	if got := coord.prunedScanColumns(ctx, stage); got != nil {
		t.Errorf("unknown table: got %v, want nil", got)
	}
}

// TestPreScanBuildTablesThresholdBoundary verifies exact boundary behavior:
// tables at exactly the threshold are included, below are excluded.
func TestPreScanBuildTablesThresholdBoundary(t *testing.T) {
	_, coord, _ := setupDistributed(t)

	const boundary = int64(10 * 1024 * 1024) // 10 MB
	coord.BuildCacheThreshold = boundary

	stages := []physical.Stage{
		{ID: "probe", Type: "scan", ScanAlias: "lineitem", TableName: "lineitem",
			EstimatedBytes: 100 * 1024 * 1024},
		{ID: "at", Type: "scan", ScanAlias: "at_boundary", TableName: "at_boundary",
			EstimatedBytes: boundary}, // exactly at threshold
		{ID: "below", Type: "scan", ScanAlias: "below_boundary", TableName: "below_boundary",
			EstimatedBytes: boundary - 1}, // 1 byte below
		{ID: "above", Type: "scan", ScanAlias: "above_boundary", TableName: "above_boundary",
			EstimatedBytes: boundary + 1}, // 1 byte above
	}

	large := physical.LargeBuildScans(stages, "lineitem", coord.BuildCacheThreshold)

	found := make(map[string]bool)
	for _, s := range large {
		found[s.ScanAlias] = true
	}

	if !found["at_boundary"] {
		t.Error("table at exactly the threshold should be included")
	}
	if found["below_boundary"] {
		t.Error("table below the threshold should NOT be included")
	}
	if !found["above_boundary"] {
		t.Error("table above the threshold should be included")
	}
	if found["lineitem"] {
		t.Error("probe table should never be included")
	}
}
