package physical

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// setupCatalog creates an in-memory catalog with an "events" table containing
// one partition and one file. It returns the catalog and a cancel-safe context.
func setupCatalog(t *testing.T) (*catalog.Catalog, context.Context) {
	t.Helper()
	ctx := context.Background()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "event_id", Type: parquet.TypeString},
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "ts", Type: parquet.TypeTimestamp},
			{Name: "year", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, "events", schema, []string{"year"}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if err := cat.AddFiles(ctx, "events",
		map[string]string{"year": "2026"},
		"tables/events/year=2026/",
		[]catalog.FileEntry{
			{Path: "tables/events/year=2026/chunk_001.parquet", SizeBytes: 1024, NumRows: 100},
		},
	); err != nil {
		t.Fatalf("add files: %v", err)
	}

	return cat, ctx
}

// setupCatalogWithUsers creates the "events" table plus a small "users" table
// for join tests.
func setupCatalogWithUsers(t *testing.T) (*catalog.Catalog, context.Context) {
	t.Helper()
	cat, ctx := setupCatalog(t)

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "user_id", Type: parquet.TypeString},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatalf("create users table: %v", err)
	}
	if err := cat.AddFiles(ctx, "users",
		map[string]string{},
		"tables/users/",
		[]catalog.FileEntry{
			{Path: "tables/users/chunk_001.parquet", SizeBytes: 512, NumRows: 50},
		},
	); err != nil {
		t.Fatalf("add users files: %v", err)
	}

	return cat, ctx
}

func TestPlanDistributed_SimpleScan(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	stages, err := planner.PlanDistributed(ctx, scan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	if len(stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(stages))
	}

	s := stages[0]
	if s.Type != "scan" {
		t.Errorf("expected stage type 'scan', got %q", s.Type)
	}
	if s.TableName != "events" {
		t.Errorf("expected table 'events', got %q", s.TableName)
	}
	if len(s.Dependencies) != 0 {
		t.Errorf("scan stage should have no dependencies, got %v", s.Dependencies)
	}
}

func TestPlanDistributed_AggregateScan(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	agg := logical.NewAggregate(scan,
		[]string{"user_id"},
		[]logical.AggExpr{
			{Func: "count", InputCol: "event_id", OutputCol: "cnt"},
		},
	)

	stages, err := planner.PlanDistributed(ctx, agg)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	if len(stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(stages))
	}

	scanStage := stages[0]
	aggStage := stages[1]

	if scanStage.Type != "scan" {
		t.Errorf("first stage should be 'scan', got %q", scanStage.Type)
	}
	if aggStage.Type != "aggregate" {
		t.Errorf("second stage should be 'aggregate', got %q", aggStage.Type)
	}
	if len(aggStage.Dependencies) == 0 {
		t.Fatal("aggregate stage should depend on scan stage")
	}
	if aggStage.Dependencies[0] != scanStage.ID {
		t.Errorf("aggregate depends on %q, want %q", aggStage.Dependencies[0], scanStage.ID)
	}
	if len(aggStage.GroupByCols) != 1 || aggStage.GroupByCols[0] != "user_id" {
		t.Errorf("unexpected group-by cols: %v", aggStage.GroupByCols)
	}
	if len(aggStage.AggSpecs) != 1 {
		t.Fatalf("expected 1 agg spec, got %d", len(aggStage.AggSpecs))
	}
	if aggStage.AggSpecs[0].Func != "count" || aggStage.AggSpecs[0].OutputCol != "cnt" {
		t.Errorf("unexpected agg spec: %+v", aggStage.AggSpecs[0])
	}
}

func TestPlanDistributed_SortAggregateScan(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	agg := logical.NewAggregate(scan,
		[]string{"user_id"},
		[]logical.AggExpr{
			{Func: "count", InputCol: "event_id", OutputCol: "cnt"},
		},
	)
	sort := logical.NewSort(agg, []logical.OrderExpr{
		{Column: "cnt", Desc: true},
	})

	stages, err := planner.PlanDistributed(ctx, sort)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	if len(stages) != 4 {
		t.Fatalf("expected 4 stages (scan, aggregate, sort, merge_sort), got %d", len(stages))
	}

	if stages[0].Type != "scan" {
		t.Errorf("stage 0 should be 'scan', got %q", stages[0].Type)
	}
	if stages[1].Type != "aggregate" {
		t.Errorf("stage 1 should be 'aggregate', got %q", stages[1].Type)
	}
	if stages[2].Type != "sort" {
		t.Errorf("stage 2 should be 'sort', got %q", stages[2].Type)
	}
	if stages[3].Type != "merge_sort" {
		t.Errorf("stage 3 should be 'merge_sort', got %q", stages[3].Type)
	}

	// Sort depends only on aggregate (its immediate predecessor), not scan
	if len(stages[2].Dependencies) != 1 || stages[2].Dependencies[0] != stages[1].ID {
		t.Errorf("sort stage should depend only on aggregate stage %q, got %v", stages[1].ID, stages[2].Dependencies)
	}
	if len(stages[2].SortKeys) != 1 {
		t.Fatalf("expected 1 sort key, got %d", len(stages[2].SortKeys))
	}
	if stages[2].SortKeys[0].Column != "cnt" || !stages[2].SortKeys[0].Desc {
		t.Errorf("unexpected sort key: %+v", stages[2].SortKeys[0])
	}

	// Merge sort depends only on the sort stage
	if len(stages[3].Dependencies) != 1 || stages[3].Dependencies[0] != stages[2].ID {
		t.Errorf("merge_sort should depend only on sort stage, got %v", stages[3].Dependencies)
	}
	if len(stages[3].SortKeys) != 1 || stages[3].SortKeys[0].Column != "cnt" {
		t.Errorf("merge_sort should have same sort keys as sort stage")
	}
}

func TestPlanDistributed_JoinTwoScans(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewPlanner(cat)

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	join := logical.NewJoin(left, right, "inner", "e.user_id = u.user_id")

	stages, err := planner.PlanDistributed(ctx, join)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	// Expect at least 3 stages: left scan, right scan, join
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	var joinStage *Stage
	scanCount := 0
	for i := range stages {
		switch stages[i].Type {
		case "scan":
			scanCount++
		case "hash_join", "broadcast_join":
			joinStage = &stages[i]
		}
	}

	if scanCount != 2 {
		t.Errorf("expected 2 scan stages, got %d", scanCount)
	}
	if joinStage == nil {
		t.Fatal("expected a join stage")
	}
	if joinStage.JoinType != "inner" {
		t.Errorf("expected join type 'inner', got %q", joinStage.JoinType)
	}
	if len(joinStage.JoinLeftKeys) == 0 || len(joinStage.JoinRightKeys) == 0 {
		t.Errorf("expected join keys, got left=%v right=%v", joinStage.JoinLeftKeys, joinStage.JoinRightKeys)
	}
}

func TestPlanDistributed_WindowScan(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	win := logical.NewWindow(scan, []logical.WindowExpr{
		{
			Func:        "row_number",
			OutputCol:   "rn",
			PartitionBy: []string{"user_id"},
			OrderBy:     []logical.OrderExpr{{Column: "ts", Desc: false}},
		},
	})

	stages, err := planner.PlanDistributed(ctx, win)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	if len(stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(stages))
	}

	if stages[0].Type != "scan" {
		t.Errorf("stage 0 should be 'scan', got %q", stages[0].Type)
	}
	if stages[1].Type != "window" {
		t.Errorf("stage 1 should be 'window', got %q", stages[1].Type)
	}
	if len(stages[1].Dependencies) == 0 {
		t.Fatal("window stage should depend on scan stage")
	}
	if len(stages[1].WindowCols) != 1 {
		t.Fatalf("expected 1 window col, got %d", len(stages[1].WindowCols))
	}
	wc := stages[1].WindowCols[0]
	if wc.Func != "row_number" {
		t.Errorf("expected func 'row_number', got %q", wc.Func)
	}
	if wc.OutputCol != "rn" {
		t.Errorf("expected output col 'rn', got %q", wc.OutputCol)
	}
	if len(wc.PartitionBy) != 1 || wc.PartitionBy[0] != "user_id" {
		t.Errorf("unexpected partition by: %v", wc.PartitionBy)
	}
}

func TestPlan_SimpleScan(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	plan, err := planner.Plan(ctx, scan)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}

	if plan.Pipeline == nil {
		t.Fatal("expected Pipeline to be non-nil")
	}
	if plan.Pipeline.Source == nil {
		t.Error("expected Pipeline.Source to be non-nil")
	}
	if plan.Pipeline.Sink == nil {
		t.Error("expected Pipeline.Sink to be non-nil")
	}
	// Plan also populates Stages
	if len(plan.Stages) == 0 {
		t.Error("expected Plan to also populate Stages")
	}
}

func TestExpandFederatedScans(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()

	// Shared KV simulates NATS KV replicated via leaf nodes
	sharedKV := catalog.NewMemKV()

	// Central cluster: has "events" table
	central := catalog.NewWithCluster(sharedKV, store, "test", "central")
	if err := central.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "event_id", Type: parquet.TypeString},
			{Name: "ts", Type: parquet.TypeTimestamp},
		},
	}
	if err := central.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := central.AddFiles(ctx, "events", nil, "tables/events/",
		[]catalog.FileEntry{
			{Path: "tables/events/central-001.parquet", SizeBytes: 1024, NumRows: 100},
			{Path: "tables/events/central-002.parquet", SizeBytes: 1024, NumRows: 100},
		}); err != nil {
		t.Fatal(err)
	}

	// Edge cluster "afb-east": also has "events" table
	edge := catalog.NewWithCluster(sharedKV, store, "test", "afb-east")
	if err := edge.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := edge.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := edge.AddFiles(ctx, "events", nil, "tables/events/",
		[]catalog.FileEntry{
			{Path: "tables/events/east-001.parquet", SizeBytes: 2048, NumRows: 200},
		}); err != nil {
		t.Fatal(err)
	}

	// Plan from central's perspective
	planner := NewPlanner(central)

	scan := logical.NewScan("events", "e")
	agg := logical.NewAggregate(scan, nil, []logical.AggExpr{
		{Func: "count", InputCol: "event_id", OutputCol: "cnt"},
	})

	stages, err := planner.PlanDistributed(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}

	// Before expansion: 1 scan + 1 aggregate
	if len(stages) != 2 {
		t.Fatalf("expected 2 stages before expansion, got %d", len(stages))
	}

	// Expand
	expanded := planner.ExpandFederatedScans(stages)

	// After expansion: 2 scan stages (one per cluster) + 1 aggregate
	scanStages := 0
	aggStages := 0
	for _, s := range expanded {
		switch s.Type {
		case "scan":
			scanStages++
		case "aggregate":
			aggStages++
		}
	}

	if scanStages != 2 {
		t.Fatalf("expected 2 scan stages after expansion, got %d", scanStages)
	}
	if aggStages != 1 {
		t.Fatalf("expected 1 aggregate stage, got %d", aggStages)
	}

	// Verify cluster IDs on scan stages
	clusterIDs := map[string]bool{}
	for _, s := range expanded {
		if s.Type == "scan" {
			clusterIDs[s.ClusterID] = true
			if s.ClusterID == "central" && len(s.ScanFiles) != 2 {
				t.Errorf("central scan should have 2 files, got %d", len(s.ScanFiles))
			}
			if s.ClusterID == "afb-east" && len(s.ScanFiles) != 1 {
				t.Errorf("afb-east scan should have 1 file, got %d", len(s.ScanFiles))
			}
		}
	}
	if !clusterIDs["central"] || !clusterIDs["afb-east"] {
		t.Errorf("expected scan stages for central and afb-east, got %v", clusterIDs)
	}

	// Verify aggregate depends on both scan stages
	for _, s := range expanded {
		if s.Type == "aggregate" {
			if len(s.Dependencies) != 2 {
				t.Errorf("aggregate should depend on 2 scan stages, got %d: %v", len(s.Dependencies), s.Dependencies)
			}
		}
	}
}

func TestExpandFederatedScans_SingleCluster(t *testing.T) {
	// With only one cluster, ExpandFederatedScans should be a no-op
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	stages, err := planner.PlanDistributed(ctx, scan)
	if err != nil {
		t.Fatal(err)
	}

	expanded := planner.ExpandFederatedScans(stages)

	// Should be unchanged (single cluster)
	if len(expanded) != len(stages) {
		t.Fatalf("expected %d stages (unchanged), got %d", len(stages), len(expanded))
	}
}

func TestScanStage_TableNameAndFiles(t *testing.T) {
	cat, ctx := setupCatalog(t)
	planner := NewPlanner(cat)

	scan := logical.NewScan("events", "e")
	stages, err := planner.PlanDistributed(ctx, scan)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	if len(stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(stages))
	}

	s := stages[0]
	if s.TableName != "events" {
		t.Errorf("expected table 'events', got %q", s.TableName)
	}
	if len(s.ScanFiles) != 1 {
		t.Fatalf("expected 1 scan file, got %d", len(s.ScanFiles))
	}
	if s.ScanFiles[0] != "tables/events/year=2026/chunk_001.parquet" {
		t.Errorf("unexpected scan file: %q", s.ScanFiles[0])
	}
	if s.Tasks != 1 {
		t.Errorf("expected 1 task (one file), got %d", s.Tasks)
	}
}

func TestPlanDistributed_ShuffleJoin(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)
	planner := NewPlanner(cat)
	planner.WorkerCount = 4 // enables shuffle stages

	left := logical.NewScan("events", "e")
	right := logical.NewScan("users", "u")
	join := logical.NewJoin(left, right, "inner", "e.user_id = u.user_id")

	stages, err := planner.PlanDistributed(ctx, join)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	// users has only 1 file → isBroadcastCandidate returns true
	// → no shuffles needed, join is broadcast_join with 1 task
	var hasBroadcast bool
	for _, s := range stages {
		if s.Type == "broadcast_join" {
			hasBroadcast = true
		}
	}
	if !hasBroadcast {
		t.Fatal("expected broadcast_join for small right-side table")
	}

	// Verify no shuffle stages were created for broadcast
	for _, s := range stages {
		if s.Type == "shuffle" {
			t.Errorf("broadcast join should not have shuffle stages, found %s", s.ID)
		}
	}
}

// setupCatalogWithLargeTables creates two tables with many files so both
// sides exceed the broadcast threshold and trigger shuffle stages.
func setupCatalogWithLargeTables(t *testing.T) (*catalog.Catalog, context.Context) {
	t.Helper()
	ctx := context.Background()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	ordersSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "o_orderkey", Type: parquet.TypeInt64},
			{Name: "o_custkey", Type: parquet.TypeInt64},
		},
	}
	if err := cat.CreateTable(ctx, "orders", ordersSchema, nil); err != nil {
		t.Fatal(err)
	}
	orderFiles := make([]catalog.FileEntry, 20)
	for i := range orderFiles {
		orderFiles[i] = catalog.FileEntry{
			Path: fmt.Sprintf("tables/orders/chunk_%03d.parquet", i),
			SizeBytes: 10 * 1024 * 1024,
			NumRows:   100000,
		}
	}
	if err := cat.AddFiles(ctx, "orders", map[string]string{}, "tables/orders/", orderFiles); err != nil {
		t.Fatal(err)
	}

	lineitemSchema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "l_orderkey", Type: parquet.TypeInt64},
			{Name: "l_quantity", Type: parquet.TypeFloat64},
		},
	}
	if err := cat.CreateTable(ctx, "lineitem", lineitemSchema, nil); err != nil {
		t.Fatal(err)
	}
	liFiles := make([]catalog.FileEntry, 100)
	for i := range liFiles {
		liFiles[i] = catalog.FileEntry{
			Path: fmt.Sprintf("tables/lineitem/chunk_%03d.parquet", i),
			SizeBytes: 10 * 1024 * 1024,
			NumRows:   500000,
		}
	}
	if err := cat.AddFiles(ctx, "lineitem", map[string]string{}, "tables/lineitem/", liFiles); err != nil {
		t.Fatal(err)
	}

	return cat, ctx
}

func TestPlanDistributed_ShuffleJoinLargeTables(t *testing.T) {
	cat, ctx := setupCatalogWithLargeTables(t)
	planner := NewPlanner(cat)
	planner.WorkerCount = 4

	left := logical.NewScan("lineitem", "l")
	right := logical.NewScan("orders", "o")
	join := logical.NewJoin(left, right, "inner", "l.l_orderkey = o.o_orderkey")

	stages, err := planner.PlanDistributed(ctx, join)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	var shuffles, joins []Stage
	for _, s := range stages {
		switch s.Type {
		case "shuffle":
			shuffles = append(shuffles, s)
		case "hash_join":
			joins = append(joins, s)
		}
	}

	// Both tables are large (>10 files each), so expect 2 shuffle stages
	if len(shuffles) != 2 {
		t.Fatalf("expected 2 shuffle stages, got %d", len(shuffles))
	}
	if len(joins) != 1 {
		t.Fatalf("expected 1 join stage, got %d", len(joins))
	}

	// Verify shuffle properties
	for _, s := range shuffles {
		if s.NumPartitions != 4 {
			t.Errorf("shuffle %s: expected 4 partitions, got %d", s.ID, s.NumPartitions)
		}
		if len(s.ShuffleKeys) == 0 {
			t.Errorf("shuffle %s: no shuffle keys", s.ID)
		}
	}

	// Verify join stage is partitioned
	js := joins[0]
	if js.NumPartitions != 4 {
		t.Errorf("join: expected 4 partitions, got %d", js.NumPartitions)
	}
	if js.Tasks != 4 {
		t.Errorf("join: expected 4 tasks, got %d", js.Tasks)
	}
	// Join should depend on shuffle stages, not scan stages
	for _, dep := range js.Dependencies {
		found := false
		for _, s := range shuffles {
			if s.ID == dep {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("join dependency %q is not a shuffle stage", dep)
		}
	}
}

// TestPlanDistributed_MultiWayJoinShuffleKeys verifies that shuffle stages in
// a multi-way join (A ⋈ B ⋈ C) have correctly assigned key columns.
// Regression: parseJoinKeys assigned left/right based on position in "="
// rather than which child subtree owns the column, causing "shuffle key X
// not found in schema" errors when the second join's shuffle read from the
// wrong upstream stage.
func TestPlanDistributed_MultiWayJoinShuffleKeys(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Create 3 large tables: customer, orders, lineitem
	for _, tbl := range []struct {
		name string
		cols []parquet.Column
		n    int
	}{
		{"customer", []parquet.Column{
			{Name: "c_custkey", Type: parquet.TypeInt64},
			{Name: "c_name", Type: parquet.TypeString},
		}, 15},
		{"orders", []parquet.Column{
			{Name: "o_orderkey", Type: parquet.TypeInt64},
			{Name: "o_custkey", Type: parquet.TypeInt64},
		}, 20},
		{"lineitem", []parquet.Column{
			{Name: "l_orderkey", Type: parquet.TypeInt64},
			{Name: "l_quantity", Type: parquet.TypeFloat64},
		}, 100},
	} {
		if err := cat.CreateTable(ctx, tbl.name, parquet.Schema{Columns: tbl.cols}, nil); err != nil {
			t.Fatal(err)
		}
		files := make([]catalog.FileEntry, tbl.n)
		for i := range files {
			files[i] = catalog.FileEntry{
				Path:      fmt.Sprintf("tables/%s/chunk_%03d.parquet", tbl.name, i),
				SizeBytes: 10 * 1024 * 1024,
				NumRows:   100000,
			}
		}
		if err := cat.AddFiles(ctx, tbl.name, map[string]string{}, "tables/"+tbl.name+"/", files); err != nil {
			t.Fatal(err)
		}
	}

	planner := NewPlanner(cat)
	planner.WorkerCount = 4

	// Build left-deep join: (customer ⋈ orders) ⋈ lineitem
	// Join condition deliberately puts the key from the RIGHT child table
	// on the LEFT side of "=" to exercise the fixJoinKeyOrder logic.
	custScan := logical.NewScan("customer", "c")
	ordScan := logical.NewScan("orders", "o")
	join1 := logical.NewJoin(custScan, ordScan, "inner", "c.c_custkey = o.o_custkey")

	liScan := logical.NewScan("lineitem", "l")
	// Note: l_orderkey is from lineitem (right child) but appears on LEFT of "=".
	// Without fixJoinKeyOrder, the left shuffle would get key "l_orderkey" but
	// depend on join1 output (which has customer+orders columns, not lineitem).
	join2 := logical.NewJoin(join1, liScan, "inner", "l.l_orderkey = o.o_orderkey")

	stages, err := planner.PlanDistributed(ctx, join2)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	// Collect shuffle stages and their dependencies
	stageByID := make(map[string]Stage)
	for _, s := range stages {
		stageByID[s.ID] = s
	}

	var shuffles []Stage
	for _, s := range stages {
		if s.Type == "shuffle" {
			shuffles = append(shuffles, s)
		}
	}

	// Should have 4 shuffle stages: 2 for join1, 2 for join2
	if len(shuffles) != 4 {
		t.Fatalf("expected 4 shuffle stages, got %d", len(shuffles))
	}

	// Find the second pair of shuffles (for join2) — they depend on
	// join1's output (or lineitem scan).
	custCols := map[string]bool{"c_custkey": true, "c_name": true}
	ordCols := map[string]bool{"o_orderkey": true, "o_custkey": true}
	liCols := map[string]bool{"l_orderkey": true, "l_quantity": true}
	join1Cols := make(map[string]bool)
	for k := range custCols {
		join1Cols[k] = true
	}
	for k := range ordCols {
		join1Cols[k] = true
	}

	for _, s := range shuffles {
		if len(s.Dependencies) != 1 {
			continue
		}
		dep := stageByID[s.Dependencies[0]]

		// Determine which column set the dependency produces
		var depCols map[string]bool
		switch {
		case dep.Type == "scan" && dep.TableName == "customer":
			depCols = custCols
		case dep.Type == "scan" && dep.TableName == "orders":
			depCols = ordCols
		case dep.Type == "scan" && dep.TableName == "lineitem":
			depCols = liCols
		default:
			// Depends on join or another shuffle — check if it's the
			// join1 output (customer+orders columns).
			depCols = join1Cols
		}

		// Every shuffle key must exist in the dependency's column set
		for _, key := range s.ShuffleKeys {
			if !depCols[key] {
				t.Errorf("shuffle %s (depends on %s %s) has key %q not in dependency columns %v",
					s.ID, dep.Type, dep.ID, key, depCols)
			}
		}
	}
}

// TestPlanDistributed_ColumnPruning verifies that distributed scan, shuffle,
// and join stages carry only the columns needed by downstream operators.
// Regression: before this fix, all stages read ALL columns from Parquet,
// causing 50-70% unnecessary I/O in multi-join queries on wide tables.
func TestPlanDistributed_ColumnPruning(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Wide lineitem table (16 columns, like TPC-H)
	liSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_partkey", Type: parquet.TypeInt64},
		{Name: "l_suppkey", Type: parquet.TypeInt64},
		{Name: "l_linenumber", Type: parquet.TypeInt64},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
		{Name: "l_extendedprice", Type: parquet.TypeFloat64},
		{Name: "l_discount", Type: parquet.TypeFloat64},
		{Name: "l_tax", Type: parquet.TypeFloat64},
		{Name: "l_returnflag", Type: parquet.TypeString},
		{Name: "l_linestatus", Type: parquet.TypeString},
		{Name: "l_shipdate", Type: parquet.TypeString},
		{Name: "l_commitdate", Type: parquet.TypeString},
		{Name: "l_receiptdate", Type: parquet.TypeString},
		{Name: "l_shipinstruct", Type: parquet.TypeString},
		{Name: "l_shipmode", Type: parquet.TypeString},
		{Name: "l_comment", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "lineitem", liSchema, nil); err != nil {
		t.Fatal(err)
	}
	liFiles := make([]catalog.FileEntry, 50)
	for i := range liFiles {
		liFiles[i] = catalog.FileEntry{
			Path:      fmt.Sprintf("tables/lineitem/chunk_%03d.parquet", i),
			SizeBytes: 10 * 1024 * 1024,
			NumRows:   500000,
		}
	}
	if err := cat.AddFiles(ctx, "lineitem", map[string]string{}, "tables/lineitem/", liFiles); err != nil {
		t.Fatal(err)
	}

	// Orders table (narrower)
	ordSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_custkey", Type: parquet.TypeInt64},
		{Name: "o_orderstatus", Type: parquet.TypeString},
		{Name: "o_totalprice", Type: parquet.TypeFloat64},
		{Name: "o_orderdate", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "orders", ordSchema, nil); err != nil {
		t.Fatal(err)
	}
	ordFiles := make([]catalog.FileEntry, 20)
	for i := range ordFiles {
		ordFiles[i] = catalog.FileEntry{
			Path:      fmt.Sprintf("tables/orders/chunk_%03d.parquet", i),
			SizeBytes: 10 * 1024 * 1024,
			NumRows:   100000,
		}
	}
	if err := cat.AddFiles(ctx, "orders", map[string]string{}, "tables/orders/", ordFiles); err != nil {
		t.Fatal(err)
	}

	// Build plan: SELECT SUM(l_extendedprice) FROM lineitem JOIN orders ON l_orderkey = o_orderkey
	// Only needs: l_orderkey (join key), l_extendedprice (aggregate input),
	// o_orderkey (join key) — NOT the other 13 lineitem or 4 orders columns
	liScan := logical.NewScan("lineitem", "l")
	ordScan := logical.NewScan("orders", "o")
	join := logical.NewJoin(liScan, ordScan, "inner", "l.l_orderkey = o.o_orderkey")
	agg := &logical.Node{
		Type:     logical.NodeAggregate,
		Children: []*logical.Node{join},
		AggExprs: []logical.AggExpr{
			{Func: "sum", InputCol: "l_extendedprice", OutputCol: "total"},
		},
	}

	// Run optimizer to compute RequiredColumns/NeededColumns
	planner := NewPlanner(cat)
	planner.WorkerCount = 4
	planner.AnnotateScanColumns(ctx, agg)
	optimized := logical.Optimize(agg, func(plan *logical.Node) {
		planner.AnnotateScanColumns(ctx, plan)
	})

	stages, err := planner.PlanDistributed(ctx, optimized)
	if err != nil {
		t.Fatalf("PlanDistributed: %v", err)
	}

	// Verify scan stages have column pruning
	for _, s := range stages {
		if s.Type != "scan" {
			continue
		}
		if len(s.Columns) == 0 {
			t.Errorf("scan stage %s (%s): expected column pruning (Columns set), got nil", s.ID, s.TableName)
			continue
		}
		if s.TableName == "lineitem" && len(s.Columns) >= 16 {
			t.Errorf("lineitem scan: expected column pruning (<16 columns), got %d: %v", len(s.Columns), s.Columns)
		}
	}

	// Verify shuffle stages have column pruning (if present)
	for _, s := range stages {
		if s.Type != "shuffle" {
			continue
		}
		if len(s.Columns) == 0 {
			t.Errorf("shuffle stage %s: expected column pruning (Columns set), got nil", s.ID)
			continue
		}
		// Shuffle columns should include join keys + aggregate inputs
		colSet := make(map[string]bool)
		for _, c := range s.Columns {
			colSet[c] = true
		}
		// Must have join keys
		for _, key := range s.ShuffleKeys {
			if !colSet[key] {
				t.Errorf("shuffle %s: shuffle key %q not in Columns %v", s.ID, key, s.Columns)
			}
		}
		// Should NOT have all 16 lineitem columns
		if len(s.Columns) >= 16 {
			t.Errorf("shuffle %s: expected pruned columns, got %d: %v", s.ID, len(s.Columns), s.Columns)
		}
	}

	// Print stage summary for debugging
	for _, s := range stages {
		cols := make([]string, len(s.Columns))
		copy(cols, s.Columns)
		sort.Strings(cols)
		t.Logf("stage %s [%s] table=%s columns=%v", s.ID, s.Type, s.TableName, cols)
	}
}
