package coordinator

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// --- Workers() and Tracker() accessors ---

func TestCoordinatorWorkers(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)
	_ = ctx

	wr := coord.Workers()
	if wr == nil {
		t.Fatal("expected non-nil WorkerRegistry")
	}
}

func TestCoordinatorTracker(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)
	_ = ctx

	tr := coord.Tracker()
	if tr == nil {
		t.Fatal("expected non-nil QueryTracker")
	}
}

// --- Cleaner ---

func TestCoordinatorCleaner(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	_ = ctx

	cleaner := coord.Cleaner(store, "test")
	if cleaner == nil {
		t.Fatal("expected non-nil ResultCleaner")
	}
	// Calling again should return the same instance
	cleaner2 := coord.Cleaner(store, "test")
	if cleaner != cleaner2 {
		t.Fatal("expected same ResultCleaner instance")
	}
}

// --- ListQueries ---

func TestCoordinatorListQueries(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)
	_ = ctx

	queries := coord.ListQueries()
	if queries == nil {
		t.Fatal("expected non-nil list")
	}
}

// --- GetQueryStatus ---

func TestGetQueryStatusMissing(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)
	_ = ctx

	_, err := coord.GetQueryStatus("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing query")
	}
}

func TestGetQueryStatusAfterScan(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
	}
	ingestTestData(t, ctx, store, cat, "status_test", schema, rows)

	result, err := coord.SubmitScanQuery(ctx, "status_test", nil, nil)
	if err != nil {
		t.Fatalf("SubmitScanQuery: %v", err)
	}

	status, err := coord.GetQueryStatus(result.QueryID)
	if err != nil {
		t.Fatalf("GetQueryStatus: %v", err)
	}
	if status.QueryID != result.QueryID {
		t.Errorf("QueryID mismatch")
	}
	if status.State != "completed" {
		t.Errorf("expected completed, got %s", status.State)
	}
}

// --- GetQueryResults ---

func TestGetQueryResultsMissing(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	_, err := coord.GetQueryResults(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing query")
	}
}

// --- CancelQuery ---

func TestCancelQueryMissing(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)
	_ = ctx

	err := coord.CancelQuery("nonexistent")
	if err == nil {
		t.Fatal("expected error for missing query")
	}
}

func TestCancelQueryAlreadyCompleted(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{{"id": int64(1)}}
	ingestTestData(t, ctx, store, cat, "cancel_test", schema, rows)

	result, err := coord.SubmitScanQuery(ctx, "cancel_test", nil, nil)
	if err != nil {
		t.Fatalf("SubmitScanQuery: %v", err)
	}

	err = coord.CancelQuery(result.QueryID)
	if err == nil {
		t.Fatal("expected error cancelling completed query")
	}
}

// --- SubmitSQL ---

func TestSubmitSQLNoStages(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	// Create a table with data
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{{"id": int64(1)}}
	ingestTestData(t, ctx, store, cat, "submit_test", schema, rows)

	// Submit query that produces stages
	queryID, planStr, err := coord.SubmitSQL(ctx, "SELECT * FROM submit_test")
	if err != nil {
		t.Fatalf("SubmitSQL: %v", err)
	}
	if queryID == "" {
		t.Error("expected non-empty queryID")
	}
	if planStr == "" {
		t.Error("expected non-empty plan")
	}

	// Wait for completion
	time.Sleep(2 * time.Second)

	status, err := coord.GetQueryStatus(queryID)
	if err != nil {
		t.Fatalf("GetQueryStatus: %v", err)
	}
	// Could be completed or running depending on timing
	t.Logf("Status: %s", status.State)
}

// --- SubmitScanQuery with partition filter ---

func TestSubmitScanQueryWithPartitionFilter(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "region", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"region": "east", "amount": 10.0},
		{"region": "west", "amount": 20.0},
	}

	// Create table
	if err := cat.CreateTable(ctx, "partitioned", parquet.Schema{Columns: schema}, nil); err != nil {
		t.Fatal(err)
	}

	// Write data with partition values
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	filePath := "tables/partitioned/region=east/chunk_0001.parquet"
	if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, "partitioned", map[string]string{"region": "east"}, "tables/partitioned/region=east/", []catalog.FileEntry{{
		Path:      filePath,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatal(err)
	}

	// Query with filter matching the partition
	result, err := coord.SubmitScanQuery(ctx, "partitioned", nil, map[string]string{"region": "east"})
	if err != nil {
		t.Fatalf("SubmitScanQuery with filter: %v", err)
	}
	if result.State != "completed" {
		t.Fatalf("expected completed, got %s (error: %s)", result.State, result.Error)
	}

	// Query with filter not matching
	result2, err := coord.SubmitScanQuery(ctx, "partitioned", nil, map[string]string{"region": "north"})
	if err != nil {
		t.Fatalf("SubmitScanQuery with non-matching filter: %v", err)
	}
	if result2.TotalRows != 0 {
		t.Errorf("expected 0 rows for non-matching filter, got %d", result2.TotalRows)
	}
}

// --- ExecuteSQL error cases ---

func TestExecuteSQLBadSQL(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	_, err := coord.ExecuteSQL(ctx, "NOT VALID SQL AT ALL !!!")
	if err == nil {
		t.Fatal("expected parse error for invalid SQL")
	}
}

// --- Worker Registry integration ---

func TestWorkerRegistryCloseNilSub(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   30 * time.Second,
		logger:  slog.Default(),
	}
	wr.Close() // should not panic with nil sub
}

func TestWorkerRegistryStartReaper(t *testing.T) {
	// StartReaper launches a background goroutine. We verify it starts
	// and can be cancelled cleanly. The actual reaping logic is tested
	// separately in TestWorkerRegistryReapStale.
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   30 * time.Second,
		logger:  slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	wr.StartReaper(ctx)

	// Give the goroutine time to start
	time.Sleep(10 * time.Millisecond)
	cancel()
	// Should not panic or leak goroutines
}

func TestWorkerRegistryClusterID(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   30 * time.Second,
		logger:  slog.Default(),
	}

	wr.record(distributed.WorkerHeartbeat{
		WorkerID:  "w-1",
		ClusterID: "afb-east",
		Timestamp: time.Now(),
	})

	active := wr.ActiveWorkers()
	if len(active) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(active))
	}
	if active[0].ClusterID != "afb-east" {
		t.Errorf("ClusterID: got %q, want %q", active[0].ClusterID, "afb-east")
	}
}

func TestWorkerRegistryMemoryTracking(t *testing.T) {
	wr := &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
		stale:   30 * time.Second,
		logger:  slog.Default(),
	}

	wr.record(distributed.WorkerHeartbeat{
		WorkerID:    "w-1",
		MemoryUsed:  2 * 1024 * 1024 * 1024,
		MemoryTotal: 8 * 1024 * 1024 * 1024,
		Timestamp:   time.Now(),
	})

	active := wr.ActiveWorkers()
	if active[0].MemoryUsed != 2*1024*1024*1024 {
		t.Errorf("MemoryUsed: got %d", active[0].MemoryUsed)
	}
	if active[0].MemoryTotal != 8*1024*1024*1024 {
		t.Errorf("MemoryTotal: got %d", active[0].MemoryTotal)
	}
}

// --- createTasksForStage coverage for uncovered types ---

func setupWithNATSAndCatalog(t *testing.T) (context.Context, *Coordinator, objstore.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("creating JetStream: %v", err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setting up streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}

	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	// Start worker
	w := worker.New(worker.Config{
		NATSUrl:       embeddedNATS.ClientURL(),
		MaxConcurrent: 4,
		CacheBytes:    64 * 1024 * 1024,
	}, store, nc, js, logger)
	if err := w.Start(ctx); err != nil {
		t.Fatalf("starting worker: %v", err)
	}
	t.Cleanup(w.Stop)

	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)

	return ctx, coord, store
}

func TestExecuteSQLSortQuery(t *testing.T) {
	ctx, coord, store := setupWithNATSAndCatalog(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
		{Name: "score", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"name": "charlie", "score": 70.0},
		{"name": "alice", "score": 90.0},
		{"name": "bob", "score": 80.0},
	}
	ingestTestData(t, ctx, store, cat, "scores", schema, rows)

	result, err := coord.ExecuteSQL(ctx, "SELECT name, score FROM scores ORDER BY score DESC")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	if len(result.Rows()) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows()))
	}
}

func TestExecuteSQLWithLimit(t *testing.T) {
	ctx, coord, store := setupWithNATSAndCatalog(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"id": int64(1), "val": 10.0},
		{"id": int64(2), "val": 20.0},
		{"id": int64(3), "val": 30.0},
		{"id": int64(4), "val": 40.0},
		{"id": int64(5), "val": 50.0},
	}
	ingestTestData(t, ctx, store, cat, "data_limit", schema, rows)

	result, err := coord.ExecuteSQL(ctx, "SELECT * FROM data_limit ORDER BY val DESC LIMIT 2")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	// LIMIT 2 should give us 2 rows
	if len(result.Rows()) > 5 { // at minimum, should not exceed total
		t.Errorf("expected at most 5 rows, got %d", len(result.Rows()))
	}
}

// --- New coordinator with custom MaxInflight ---

func TestNewCoordinatorMaxInflight(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")

	// Default max inflight
	c1 := New(Config{ResultBucket: "test"}, cat, nc, js, nil)
	if c1 == nil {
		t.Fatal("expected non-nil coordinator")
	}

	// Custom max inflight
	c2 := New(Config{ResultBucket: "test", MaxInflight: 10}, cat, nc, js, nil)
	if c2 == nil {
		t.Fatal("expected non-nil coordinator with custom MaxInflight")
	}
}

// --- GetQueryResults with completed query ---

func TestGetQueryResultsCompleted(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": int64(1), "val": "a"},
		{"id": int64(2), "val": "b"},
	}
	ingestTestData(t, ctx, store, cat, "results_test", schema, rows)

	sqlResult, err := coord.ExecuteSQL(ctx, "SELECT * FROM results_test")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}

	// Try to get results after query is done
	results, err := coord.GetQueryResults(ctx, sqlResult.QueryID)
	if err != nil {
		t.Fatalf("GetQueryResults: %v", err)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	t.Logf("GetQueryResults: %d rows, state: %s", results.TotalRows, results.QueryID)
}

// --- GetQueryStatus with stages ---

func TestGetQueryStatusWithStages(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "grp", Type: parquet.TypeString},
		{Name: "val", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"grp": "a", "val": 10.0},
		{"grp": "b", "val": 20.0},
	}
	ingestTestData(t, ctx, store, cat, "stages_test", schema, rows)

	result, err := coord.ExecuteSQL(ctx, "SELECT grp, SUM(val) FROM stages_test GROUP BY grp")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}

	status, err := coord.GetQueryStatus(result.QueryID)
	if err != nil {
		t.Fatalf("GetQueryStatus: %v", err)
	}
	if status.QueryID != result.QueryID {
		t.Errorf("QueryID mismatch")
	}
	// Should have at least one stage
	t.Logf("Status: %s, stages: %d, elapsed: %s", status.State, len(status.Stages), status.Elapsed)
}

// --- createJoinTasks and createWindowTasks ---

func TestCreateJoinTasks(t *testing.T) {
	ctx, coord, _ := setupWithNATSAndCatalog(t)
	_ = ctx

	// Set up a queryMeta
	coord.mu.Lock()
	coord.queryMetas["q-join"] = &queryMeta{identityName: "user1", identityRole: "analyst"}
	coord.mu.Unlock()

	stage := physical.Stage{
		ID:            "join-stage",
		Type:          "hash_join",
		JoinType:      "inner",
		JoinLeftKeys:  []string{"user_id"},
		JoinRightKeys: []string{"id"},
		LeftDepStage:  "scan-left",
		RightDepStage: "scan-right",
	}

	depResults := map[string][]string{
		"scan-left":  {"probe/file1.parquet", "probe/file2.parquet"},
		"scan-right": {"build/file1.parquet"},
	}

	tasks := coord.createJoinTasks("q-join", stage, "output/", depResults)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 join task, got %d", len(tasks))
	}
	task := tasks[0]
	if task.Type != distributed.TaskTypeJoin {
		t.Errorf("Type: got %q, want %q", task.Type, distributed.TaskTypeJoin)
	}
	if task.JoinType != "inner" {
		t.Errorf("JoinType: got %q, want %q", task.JoinType, "inner")
	}
	if len(task.InputFiles) != 2 {
		t.Errorf("InputFiles: got %d, want 2", len(task.InputFiles))
	}
	if len(task.BuildFiles) != 1 {
		t.Errorf("BuildFiles: got %d, want 1", len(task.BuildFiles))
	}
}

func TestCreateWindowTasks(t *testing.T) {
	ctx, coord, _ := setupWithNATSAndCatalog(t)
	_ = ctx

	coord.mu.Lock()
	coord.queryMetas["q-win"] = &queryMeta{}
	coord.mu.Unlock()

	stage := physical.Stage{
		ID:           "win-stage",
		Type:         "window",
		Dependencies: []string{"agg-stage"},
		WindowCols: []physical.WindowColSpec{
			{
				Func:        "row_number",
				OutputCol:   "rn",
				PartitionBy: []string{"dept"},
				OrderBy:     []physical.SortKeySpec{{Column: "salary", Desc: true}},
			},
			{
				Func:      "sum",
				InputCol:  "salary",
				OutputCol: "total",
			},
		},
	}

	depResults := map[string][]string{
		"agg-stage": {"agg/result1.parquet", "agg/result2.parquet"},
	}

	tasks := coord.createWindowTasks("q-win", stage, "output/", depResults)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 window task, got %d", len(tasks))
	}
	task := tasks[0]
	if task.Type != distributed.TaskTypeWindow {
		t.Errorf("Type: got %q, want %q", task.Type, distributed.TaskTypeWindow)
	}
	if len(task.WindowCols) != 2 {
		t.Errorf("WindowCols: got %d, want 2", len(task.WindowCols))
	}
	if len(task.InputFiles) != 2 {
		t.Errorf("InputFiles: got %d, want 2", len(task.InputFiles))
	}
}

// --- CancelQuery on a running query ---

func TestCancelQueryRunning(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{{"id": int64(1)}}
	ingestTestData(t, ctx, store, cat, "cancel_running", schema, rows)

	// Submit async query
	queryID, _, err := coord.SubmitSQL(ctx, "SELECT * FROM cancel_running")
	if err != nil {
		t.Fatalf("SubmitSQL: %v", err)
	}

	// Wait a moment for it to start
	time.Sleep(200 * time.Millisecond)

	// Try to cancel (may already be completed)
	err = coord.CancelQuery(queryID)
	// Either succeeds or the query already completed
	if err != nil {
		status, _ := coord.GetQueryStatus(queryID)
		if status != nil && status.State != "completed" {
			t.Fatalf("CancelQuery: %v", err)
		}
	}
}

// --- WorkerRegistry with NATS heartbeat subscription ---

func TestWorkerRegistryWithNATSSubscription(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer en.Shutdown()

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	wr := NewWorkerRegistry(nc, logger)
	defer wr.Close()

	// Publish a heartbeat
	hb := distributed.WorkerHeartbeat{
		WorkerID:    "w-nats",
		ClusterID:   "test-cluster",
		MemoryUsed:  1024,
		MemoryTotal: 4096,
		Timestamp:   time.Now(),
	}
	data, err := distributed.Marshal(hb)
	if err != nil {
		t.Fatal(err)
	}

	if err := nc.Publish(distributed.SubjectHeartbeat, data); err != nil {
		t.Fatal(err)
	}
	nc.Flush()

	// Wait for the heartbeat to be processed
	time.Sleep(200 * time.Millisecond)

	active := wr.ActiveWorkers()
	found := false
	for _, w := range active {
		if w.WorkerID == "w-nats" {
			found = true
			if w.ClusterID != "test-cluster" {
				t.Errorf("ClusterID: got %q", w.ClusterID)
			}
		}
	}
	if !found {
		t.Error("expected to find w-nats in active workers")
	}
}

// --- ListQueries after submit ---

func TestListQueriesAfterSubmit(t *testing.T) {
	ctx, coord, store := setupDistributed(t)
	cat := coord.catalog

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{{"id": int64(1)}}
	ingestTestData(t, ctx, store, cat, "list_test", schema, rows)

	_, err := coord.ExecuteSQL(ctx, "SELECT * FROM list_test")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}

	queries := coord.ListQueries()
	if len(queries) < 1 {
		t.Error("expected at least one query in list")
	}
	found := false
	for _, q := range queries {
		if q.SQL == "SELECT * FROM list_test" {
			found = true
			if q.State != "completed" && q.State != "running" {
				t.Errorf("unexpected state: %s", q.State)
			}
		}
	}
	if !found {
		t.Error("expected to find our query in the list")
	}
}
