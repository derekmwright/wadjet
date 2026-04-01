package physical

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/exec"
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

	// Fused scan-aggregate: scan (with fused agg) + final_aggregate
	if len(stages) != 2 {
		t.Fatalf("expected 2 stages (fused scan-agg + final_aggregate), got %d", len(stages))
	}

	scanStage := stages[0]
	finalAggStage := stages[1]

	if scanStage.Type != "scan" {
		t.Errorf("first stage should be 'scan', got %q", scanStage.Type)
	}
	if finalAggStage.Type != "final_aggregate" {
		t.Errorf("second stage should be 'final_aggregate', got %q", finalAggStage.Type)
	}

	// Verify fused aggregation on scan
	if len(scanStage.FusedAggGroupBy) != 1 || scanStage.FusedAggGroupBy[0] != "user_id" {
		t.Errorf("expected fused group-by [user_id], got %v", scanStage.FusedAggGroupBy)
	}
	if len(scanStage.FusedAggSpecs) != 1 {
		t.Fatalf("expected 1 fused agg spec, got %d", len(scanStage.FusedAggSpecs))
	}
	if scanStage.FusedAggSpecs[0].Func != "count" || scanStage.FusedAggSpecs[0].OutputCol != "cnt" {
		t.Errorf("unexpected fused agg spec: %+v", scanStage.FusedAggSpecs[0])
	}

	// final_aggregate depends on scan
	if len(finalAggStage.Dependencies) != 1 || finalAggStage.Dependencies[0] != scanStage.ID {
		t.Errorf("final_aggregate should depend on scan %q, got %v", scanStage.ID, finalAggStage.Dependencies)
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

	// Fused: scan (with fused agg) + final_aggregate + sort + merge_sort
	if len(stages) != 4 {
		t.Fatalf("expected 4 stages (fused scan-agg, final_aggregate, sort, merge_sort), got %d", len(stages))
	}

	if stages[0].Type != "scan" {
		t.Errorf("stage 0 should be 'scan', got %q", stages[0].Type)
	}
	if stages[1].Type != "final_aggregate" {
		t.Errorf("stage 1 should be 'final_aggregate', got %q", stages[1].Type)
	}
	if stages[2].Type != "sort" {
		t.Errorf("stage 2 should be 'sort', got %q", stages[2].Type)
	}
	if stages[3].Type != "merge_sort" {
		t.Errorf("stage 3 should be 'merge_sort', got %q", stages[3].Type)
	}

	// Verify fused aggregation on scan
	if len(stages[0].FusedAggSpecs) == 0 {
		t.Fatal("scan stage should have fused aggregate specs")
	}

	// final_aggregate depends on scan (fused agg)
	if len(stages[1].Dependencies) != 1 || stages[1].Dependencies[0] != stages[0].ID {
		t.Errorf("final_aggregate should depend on scan %q, got %v", stages[0].ID, stages[1].Dependencies)
	}
	// Sort depends only on final_aggregate
	if len(stages[2].Dependencies) != 1 || stages[2].Dependencies[0] != stages[1].ID {
		t.Errorf("sort stage should depend only on final_aggregate stage %q, got %v", stages[1].ID, stages[2].Dependencies)
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

	// Before expansion: 1 scan (with fused agg) + 1 final_aggregate
	if len(stages) != 2 {
		t.Fatalf("expected 2 stages before expansion (fused scan-agg + final), got %d", len(stages))
	}

	// Verify fused aggregation on scan stage
	if len(stages[0].FusedAggSpecs) == 0 {
		t.Fatal("expected fused aggregate specs on scan stage")
	}

	// Expand
	expanded := planner.ExpandFederatedScans(stages)

	// After expansion: 2 scan stages (one per cluster, both with fused agg) + 1 final_aggregate
	scanStages := 0
	aggStages := 0
	for _, s := range expanded {
		switch s.Type {
		case "scan":
			scanStages++
		case "final_aggregate":
			aggStages++
		}
	}

	if scanStages != 2 {
		t.Fatalf("expected 2 scan stages after expansion, got %d", scanStages)
	}
	if aggStages != 1 {
		t.Fatalf("expected 1 final_aggregate stage, got %d", aggStages)
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

	// Verify final_aggregate depends on both scan stages
	for _, s := range expanded {
		if s.Type == "final_aggregate" {
			if len(s.Dependencies) != 2 {
				t.Errorf("final_aggregate should depend on 2 scan stages, got %d: %v", len(s.Dependencies), s.Dependencies)
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

	// Verify shuffle properties (numPartitions = workerCount * 8 = 32)
	for _, s := range shuffles {
		if s.NumPartitions != 32 {
			t.Errorf("shuffle %s: expected 32 partitions, got %d", s.ID, s.NumPartitions)
		}
		if len(s.ShuffleKeys) == 0 {
			t.Errorf("shuffle %s: no shuffle keys", s.ID)
		}
	}

	// Verify join stage is partitioned
	js := joins[0]
	if js.NumPartitions != 32 {
		t.Errorf("join: expected 32 partitions, got %d", js.NumPartitions)
	}
	if js.Tasks != 32 {
		t.Errorf("join: expected 32 tasks, got %d", js.Tasks)
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

func TestShouldRoutePipeline(t *testing.T) {
	tests := []struct {
		name     string
		stages   []Stage
		workers  int
		wantPipe bool
	}{
		{
			name:     "single worker always pipeline",
			stages:   []Stage{{Type: "scan", EstimatedBytes: 1 << 30}},
			workers:  1,
			wantPipe: true,
		},
		{
			name: "small scan with shuffles → pipeline",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 100 << 20}, // 100 MB
				{Type: "scan", EstimatedBytes: 50 << 20},  // 50 MB
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "hash_join", JoinType: "inner"},
			},
			workers:  3,
			wantPipe: true, // shuffle overhead > negative parallelism benefit (scan too small)
		},
		{
			name: "large scan no shuffles → distributed",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 6 << 30}, // 6 GB
				{Type: "aggregate"},
			},
			workers:  3,
			wantPipe: false, // 0.5s overhead < 4.2s parallelism benefit
		},
		{
			name: "large scan with many shuffles → pipeline",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 2 << 30}, // 2 GB
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "hash_join", JoinType: "inner"},
				{Type: "sort"},
				{Type: "merge_sort"},
			},
			workers:  3,
			wantPipe: true, // 5.3s overhead > 1.1s parallelism benefit
		},
		{
			name: "huge scan outweighs shuffle overhead → distributed",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 20 << 30}, // 20 GB
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "hash_join", JoinType: "inner"},
			},
			workers:  3,
			wantPipe: false, // 5.5s overhead < 15s parallelism benefit
		},
		{
			name: "large scan 2 shuffles inner join → distributed (Q12-like)",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 7500 << 20}, // 7.3 GB (lineitem)
				{Type: "scan", EstimatedBytes: 1700 << 20}, // 1.7 GB (orders)
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "hash_join", JoinType: "inner"},
				{Type: "aggregate"},
				{Type: "final_aggregate"},
				{Type: "sort"},
				{Type: "merge_sort"},
			},
			workers:  3,
			wantPipe: false, // 5.3s overhead < 6.5s benefit (inner join, compute parallelism)
		},
		{
			name: "large scan 2 shuffles semi join → pipeline (Q04-like)",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 7500 << 20}, // 7.3 GB (lineitem)
				{Type: "scan", EstimatedBytes: 1700 << 20}, // 1.7 GB (orders)
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "hash_join", JoinType: "semi"},
				{Type: "aggregate"},
				{Type: "final_aggregate"},
				{Type: "sort"},
				{Type: "merge_sort"},
			},
			workers:  3,
			wantPipe: true, // 5.3s overhead > 2.0s benefit (semi join discount)
		},
		{
			name: "large scan 2 shuffles no sort → distributed (Q14-like)",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 7500 << 20}, // 7.3 GB (lineitem)
				{Type: "scan", EstimatedBytes: 250 << 20},  // 240 MB (part)
				{Type: "shuffle"},
				{Type: "shuffle"},
				{Type: "hash_join", JoinType: "inner"},
				{Type: "aggregate"},
				{Type: "final_aggregate"},
			},
			workers:  3,
			wantPipe: false, // 3.0s overhead < 5.0s benefit
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldRoutePipeline(tt.stages, tt.workers)
			if got != tt.wantPipe {
				t.Errorf("ShouldRoutePipeline() = %v, want %v", got, tt.wantPipe)
			}
		})
	}
}

func TestShouldSplitScan(t *testing.T) {
	makeFiles := func(n int) []string {
		files := make([]string, n)
		for i := range files {
			files[i] = fmt.Sprintf("file-%d.parquet", i)
		}
		return files
	}

	tests := []struct {
		name      string
		stages    []Stage
		workers   int
		wantSplit bool
	}{
		{
			name:      "single worker never splits",
			stages:    []Stage{{Type: "scan", EstimatedBytes: 1 << 30, ScanFiles: makeFiles(10)}},
			workers:   1,
			wantSplit: false,
		},
		{
			name:      "small scan not worth splitting",
			stages:    []Stage{{Type: "scan", EstimatedBytes: 100 << 20, ScanFiles: makeFiles(6)}},
			workers:   3,
			wantSplit: false, // 100 MB < 192 MB threshold (3 workers × 64 MB)
		},
		{
			name:      "too few files not worth splitting",
			stages:    []Stage{{Type: "scan", EstimatedBytes: 512 << 20, ScanFiles: makeFiles(4)}},
			workers:   3,
			wantSplit: false, // 4 files < 6 min (3 workers × 2 files)
		},
		{
			name: "large scan with enough files → split",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 7500 << 20, ScanFiles: makeFiles(96)},
				{Type: "shuffle"},
				{Type: "hash_join", JoinType: "inner"},
			},
			workers:   3,
			wantSplit: true,
		},
		{
			name: "multiple scan stages totaled under threshold",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 100 << 20, ScanFiles: makeFiles(3)},
				{Type: "scan", EstimatedBytes: 100 << 20, ScanFiles: makeFiles(2)},
			},
			workers:   3,
			wantSplit: false, // 200 MB < 256 MB threshold
		},
		{
			name: "multiple large scan stages → split",
			stages: []Stage{
				{Type: "scan", EstimatedBytes: 7500 << 20, ScanFiles: makeFiles(96)},
				{Type: "scan", EstimatedBytes: 1700 << 20, ScanFiles: makeFiles(24)},
			},
			workers:   3,
			wantSplit: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldSplitScan(tt.stages, tt.workers)
			if got != tt.wantSplit {
				t.Errorf("ShouldSplitScan() = %v, want %v", got, tt.wantSplit)
			}
		})
	}
}

func TestExtractScanStages(t *testing.T) {
	stages := []Stage{
		{ID: "scan-0", Type: "scan", TableName: "lineitem"},
		{ID: "scan-1", Type: "scan", TableName: "orders"},
		{ID: "shuffle-0", Type: "shuffle"},
		{ID: "hash_join-0", Type: "hash_join"},
		{ID: "aggregate-0", Type: "aggregate"},
	}
	scans := ExtractScanStages(stages)
	if len(scans) != 2 {
		t.Fatalf("expected 2 scan stages, got %d", len(scans))
	}
	if scans[0].TableName != "lineitem" {
		t.Errorf("scan[0].TableName = %q, want %q", scans[0].TableName, "lineitem")
	}
	if scans[1].TableName != "orders" {
		t.Errorf("scan[1].TableName = %q, want %q", scans[1].TableName, "orders")
	}
}

func TestHasSelfJoins(t *testing.T) {
	tests := []struct {
		name   string
		stages []Stage
		want   bool
	}{
		{
			"no self-join",
			[]Stage{
				{Type: "scan", TableName: "orders"},
				{Type: "scan", TableName: "lineitem"},
				{Type: "join"},
			},
			false,
		},
		{
			"self-join small table (nation)",
			[]Stage{
				{Type: "scan", TableName: "part"},
				{Type: "scan", TableName: "nation", EstimatedBytes: 2048},
				{Type: "scan", TableName: "region"},
				{Type: "scan", TableName: "nation", EstimatedBytes: 2048},
			},
			false, // small tables (< 1 MB) are exempt
		},
		{
			"self-join large table (lineitem)",
			[]Stage{
				{Type: "scan", TableName: "orders"},
				{Type: "scan", TableName: "lineitem", EstimatedBytes: 500 * 1024 * 1024},
				{Type: "scan", TableName: "lineitem", EstimatedBytes: 500 * 1024 * 1024},
			},
			true,
		},
		{
			"non-scan duplicate ignored",
			[]Stage{
				{Type: "scan", TableName: "orders"},
				{Type: "join", TableName: "orders"},
				{Type: "scan", TableName: "lineitem"},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasSelfJoins(tt.stages); got != tt.want {
				t.Errorf("HasSelfJoins() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiLevelMergeAggregateTree(t *testing.T) {
	// Create leaf stages that exceed mergeFanout (16) to trigger multi-level merge.
	var stages []Stage
	leafIDs := make([]string, 20)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("scan-%d", i)
		leafIDs[i] = id
		stages = append(stages, Stage{ID: id, Type: "scan", Tasks: 1})
	}
	groupBy := []string{"region"}
	aggSpecs := []AggSpec{{Func: "count_star", OutputCol: "cnt"}}

	emitMergeAggregateTree(&stages, leafIDs, groupBy, aggSpecs, stages[:20])

	// Should have 20 scans + intermediate merge stages + 1 final merge
	var intermediates, finals []Stage
	for _, s := range stages[20:] {
		if s.MergeGroupCount > 0 {
			intermediates = append(intermediates, s)
		} else if s.Type == "final_aggregate" {
			finals = append(finals, s)
		}
	}

	if len(intermediates) == 0 {
		t.Fatal("expected intermediate merge stages, got none")
	}
	if len(finals) != 1 {
		t.Fatalf("expected 1 final stage, got %d", len(finals))
	}

	// Final stage depends on intermediate stages
	finalDeps := make(map[string]bool)
	for _, d := range finals[0].Dependencies {
		finalDeps[d] = true
	}
	for _, interm := range intermediates {
		if !finalDeps[interm.ID] {
			t.Errorf("final stage missing dependency on %s", interm.ID)
		}
	}

	// Verify intermediate stages have correct MergeGroup/MergeGroupCount
	for i, interm := range intermediates {
		if interm.MergeGroup != i {
			t.Errorf("intermediate %d: MergeGroup=%d, want %d", i, interm.MergeGroup, i)
		}
		if interm.MergeGroupCount != len(intermediates) {
			t.Errorf("intermediate %d: MergeGroupCount=%d, want %d", i, interm.MergeGroupCount, len(intermediates))
		}
	}

	t.Logf("Multi-level merge: %d scans → %d intermediates → 1 final", 20, len(intermediates))
}

func TestMultiLevelMergeNotTriggered(t *testing.T) {
	// With <= 16 upstream tasks, should emit single-level merge.
	var stages []Stage
	leafIDs := make([]string, 8)
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("scan-%d", i)
		leafIDs[i] = id
		stages = append(stages, Stage{ID: id, Type: "scan", Tasks: 1})
	}

	emitMergeAggregateTree(&stages, leafIDs, []string{"g"}, []AggSpec{{Func: "sum", InputCol: "x", OutputCol: "sx"}}, stages[:8])

	// Should have 8 scans + 1 final_aggregate (no intermediates)
	mergeStages := stages[8:]
	if len(mergeStages) != 1 {
		t.Fatalf("expected 1 merge stage for %d upstream tasks, got %d", 8, len(mergeStages))
	}
	if mergeStages[0].MergeGroupCount != 0 {
		t.Errorf("single-level merge should have MergeGroupCount=0, got %d", mergeStages[0].MergeGroupCount)
	}
}

func TestMultiLevelMergeSortTree(t *testing.T) {
	// Create enough child stages to trigger multi-level merge sort.
	var stages []Stage
	for i := 0; i < 20; i++ {
		stages = append(stages, Stage{ID: fmt.Sprintf("scan-%d", i), Type: "scan", Tasks: 1})
	}
	sortStageID := "sort-20"
	stages = append(stages, Stage{ID: sortStageID, Type: "sort", Tasks: 1})

	sortKeys := []SortKeySpec{{Column: "total", Desc: true}}
	emitMergeSortTree(&stages, sortStageID, sortKeys, stages[:20])

	var intermediates, finals []Stage
	for _, s := range stages[21:] { // skip scans + sort stage
		if s.MergeGroupCount > 0 {
			intermediates = append(intermediates, s)
		} else if s.Type == "merge_sort" {
			finals = append(finals, s)
		}
	}

	if len(intermediates) == 0 {
		t.Fatal("expected intermediate merge_sort stages")
	}
	if len(finals) != 1 {
		t.Fatalf("expected 1 final merge_sort, got %d", len(finals))
	}
	t.Logf("Multi-level merge sort: %d partials → %d intermediates → 1 final", 20, len(intermediates))
}

func TestDeferredJoinBridge(t *testing.T) {
	// Verify that deferredJoinBridge collects child pipeline batches
	// and waits for the build barrier before returning.
	ctx := context.Background()

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	rows := []map[string]any{
		{"id": int64(1), "val": "a"},
		{"id": int64(2), "val": "b"},
		{"id": int64(3), "val": "c"},
	}
	source := exec.NewSliceSource(schema, rows)
	barrier := make(chan struct{})
	var buildErr error

	bridge := &deferredJoinBridge{
		childSource: source,
		childOps:    nil,
		barrier:     barrier,
		buildErr:    &buildErr,
		workers:     1,
	}

	// Simulate build completing
	close(barrier)

	if err := bridge.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Should be able to read all batches
	count := 0
	for {
		b, err := bridge.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		count += b.Len
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestDeferredJoinBridgeBuildError(t *testing.T) {
	ctx := context.Background()
	source := exec.NewSliceSource(nil, nil)
	barrier := make(chan struct{})
	buildErr := fmt.Errorf("build failed: out of memory")

	bridge := &deferredJoinBridge{
		childSource: source,
		childOps:    nil,
		barrier:     barrier,
		buildErr:    &buildErr,
		workers:     1,
	}

	close(barrier)

	err := bridge.Init(ctx)
	if err == nil {
		t.Fatal("expected error from build failure")
	}
	if err.Error() != "build failed: out of memory" {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestExtractScanStages_ClearsFusedAgg verifies that ExtractScanStages returns
// scan stages with their FusedAggSpecs still set (the caller is responsible
// for clearing them in scan-split mode). This test documents the interaction:
// PlanDistributed sets FusedAggSpecs on scan stages for fused scan-aggregate,
// but scan-split pipeline mode must clear them so scan tasks produce raw rows.
func TestExtractScanStages_ClearsFusedAgg(t *testing.T) {
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

	// Verify fused agg is set on the scan stage
	scanStages := ExtractScanStages(stages)
	if len(scanStages) != 1 {
		t.Fatalf("expected 1 scan stage, got %d", len(scanStages))
	}
	if len(scanStages[0].FusedAggSpecs) == 0 {
		t.Fatal("scan stage should have FusedAggSpecs from fused scan-aggregate")
	}

	// Simulate what the coordinator does in scan-split mode: clear fused agg
	for i := range scanStages {
		scanStages[i].FusedAggSpecs = nil
		scanStages[i].FusedAggGroupBy = nil
	}

	if scanStages[0].FusedAggSpecs != nil {
		t.Error("FusedAggSpecs should be nil after clearing for scan-split")
	}
	if scanStages[0].FusedAggGroupBy != nil {
		t.Error("FusedAggGroupBy should be nil after clearing for scan-split")
	}
}
