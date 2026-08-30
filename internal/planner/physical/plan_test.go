package physical

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

	// Native-DAG always emits a terminal exchange-gather stage.
	if len(stages) != 2 {
		t.Fatalf("expected 2 stages (scan + exchange-gather), got %d", len(stages))
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
	if stages[1].Type != StageExchangeGather {
		t.Errorf("expected final stage exchange-gather, got %q", stages[1].Type)
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

	// Fused scan-aggregate + final_aggregate + terminal exchange-gather.
	if len(stages) != 3 {
		t.Fatalf("expected 3 stages (fused scan-agg + final_aggregate + exchange-gather), got %d", len(stages))
	}

	scanStage := stages[0]
	finalAggStage := stages[1]
	if stages[2].Type != StageExchangeGather {
		t.Errorf("third stage should be exchange-gather, got %q", stages[2].Type)
	}

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

	// Fused: scan (with fused agg) + final_aggregate (with sort fused in
	// by fuseSortIntoPredecessor, merge_sort collapsed) + exchange-gather.
	if len(stages) != 3 {
		t.Fatalf("expected 3 stages (fused scan-agg, final_aggregate, exchange-gather), got %d", len(stages))
	}

	if stages[0].Type != "scan" {
		t.Errorf("stage 0 should be 'scan', got %q", stages[0].Type)
	}
	if stages[1].Type != "final_aggregate" {
		t.Errorf("stage 1 should be 'final_aggregate', got %q", stages[1].Type)
	}
	if stages[2].Type != StageExchangeGather {
		t.Errorf("stage 2 should be 'exchange-gather', got %q", stages[2].Type)
	}

	// Verify fused aggregation on scan
	if len(stages[0].FusedAggSpecs) == 0 {
		t.Fatal("scan stage should have fused aggregate specs")
	}

	// final_aggregate depends on scan (fused agg). The sort is fused into
	// the final_aggregate via fuseSortIntoPredecessor — verify SortKeys
	// landed on it.
	if len(stages[1].Dependencies) != 1 || stages[1].Dependencies[0] != stages[0].ID {
		t.Errorf("final_aggregate should depend on scan %q, got %v", stages[0].ID, stages[1].Dependencies)
	}
	if len(stages[1].SortKeys) != 1 {
		t.Fatalf("expected 1 fused sort key on final_aggregate, got %d", len(stages[1].SortKeys))
	}
	if stages[1].SortKeys[0].Column != "cnt" || !stages[1].SortKeys[0].Desc {
		t.Errorf("unexpected fused sort key: %+v", stages[1].SortKeys[0])
	}

	// exchange-gather depends on final_aggregate.
	if len(stages[2].Dependencies) != 1 || stages[2].Dependencies[0] != stages[1].ID {
		t.Errorf("exchange-gather should depend only on final_aggregate %q, got %v", stages[1].ID, stages[2].Dependencies)
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

	if len(stages) != 3 {
		t.Fatalf("expected 3 stages (scan, window, exchange-gather), got %d", len(stages))
	}

	if stages[0].Type != "scan" {
		t.Errorf("stage 0 should be 'scan', got %q", stages[0].Type)
	}
	if stages[1].Type != "window" {
		t.Errorf("stage 1 should be 'window', got %q", stages[1].Type)
	}
	if stages[2].Type != StageExchangeGather {
		t.Errorf("stage 2 should be 'exchange-gather', got %q", stages[2].Type)
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

	// Before expansion: 1 scan (with fused agg) + 1 final_aggregate + 1 exchange-gather
	if len(stages) != 3 {
		t.Fatalf("expected 3 stages before expansion (fused scan-agg + final + gather), got %d", len(stages))
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

	// Native-DAG always emits a trailing exchange-gather stage.
	if len(stages) != 2 {
		t.Fatalf("expected 2 stages (scan + exchange-gather), got %d", len(stages))
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
		if s.Type == StageExchangeRepartition {
			t.Errorf("broadcast join should not have shuffle stages, found %s", s.ID)
		}
	}
}

// TestBroadcastBytesThreshold_TightBudgetDemotesToShuffle covers the
// memory-conditioned broadcast→shuffle promotion (Class C). With the
// default threshold (100 MB), a small users table is broadcast as before.
// With BroadcastBytesThreshold lowered to 256 bytes (smaller than the
// users file's 512 bytes), the planner promotes the same join to
// hash_join + shuffle stages — proving the decision is conditioned on
// the threshold, not just absolute size.
func TestBroadcastBytesThreshold_TightBudgetDemotesToShuffle(t *testing.T) {
	cat, ctx := setupCatalogWithUsers(t)

	// Default threshold: users (512 bytes) is broadcast.
	{
		planner := NewPlanner(cat)
		planner.WorkerCount = 4
		left := logical.NewScan("events", "e")
		right := logical.NewScan("users", "u")
		join := logical.NewJoin(left, right, "inner", "e.user_id = u.user_id")
		stages, err := planner.PlanDistributed(ctx, join)
		if err != nil {
			t.Fatalf("PlanDistributed default: %v", err)
		}
		var got string
		for _, s := range stages {
			if s.Type == "broadcast_join" || s.Type == "hash_join" {
				got = s.Type
			}
		}
		if got != "broadcast_join" {
			t.Errorf("default threshold: expected broadcast_join, got %q", got)
		}
	}

	// Tight threshold: 256 < users size (512 bytes) → must demote to hash_join.
	{
		planner := NewPlanner(cat)
		planner.WorkerCount = 4
		planner.BroadcastBytesThreshold = 256
		left := logical.NewScan("events", "e")
		right := logical.NewScan("users", "u")
		join := logical.NewJoin(left, right, "inner", "e.user_id = u.user_id")
		stages, err := planner.PlanDistributed(ctx, join)
		if err != nil {
			t.Fatalf("PlanDistributed tight: %v", err)
		}
		var got string
		for _, s := range stages {
			if s.Type == "broadcast_join" || s.Type == "hash_join" {
				got = s.Type
			}
		}
		if got != "hash_join" {
			t.Errorf("tight threshold: expected hash_join (broadcast demoted), got %q", got)
		}
	}

	// Negative threshold: broadcast disabled entirely.
	{
		planner := NewPlanner(cat)
		planner.WorkerCount = 4
		planner.BroadcastBytesThreshold = -1
		left := logical.NewScan("events", "e")
		right := logical.NewScan("users", "u")
		join := logical.NewJoin(left, right, "inner", "e.user_id = u.user_id")
		stages, err := planner.PlanDistributed(ctx, join)
		if err != nil {
			t.Fatalf("PlanDistributed disabled: %v", err)
		}
		for _, s := range stages {
			if s.Type == "broadcast_join" {
				t.Errorf("negative threshold: expected NO broadcast_join, found %s", s.ID)
			}
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
		case StageExchangeRepartition:
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
		if s.Exchange == nil {
			t.Errorf("shuffle %s: Exchange is nil", s.ID)
			continue
		}
		if s.Exchange.Count != 32 {
			t.Errorf("shuffle %s: expected 32 partitions, got %d", s.ID, s.Exchange.Count)
		}
		if len(s.Exchange.Keys) == 0 {
			t.Errorf("shuffle %s: no shuffle keys", s.ID)
		}
	}

	// Verify join stage is partitioned
	js := joins[0]
	if js.JoinPartitionCount != 32 {
		t.Errorf("join: expected 32 partitions, got %d", js.JoinPartitionCount)
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

	// Shuffle-carrying units: standalone repartitions plus Exchange-
	// carrying joins (fuseJoinShuffle absorbs join1's output exchange;
	// the three plain scans keep standalone exchanges — pass-through
	// scans are not fused by design).
	var shuffles []Stage
	for _, s := range stages {
		if s.Type == StageExchangeRepartition {
			shuffles = append(shuffles, s)
		}
		if s.Type == StageHashJoin && s.Exchange != nil {
			u := s
			u.Dependencies = []string{s.ID}
			shuffles = append(shuffles, u)
		}
	}

	// 4 shuffle units total: 3 standalone (scan-fed) + 1 fused join.
	if len(shuffles) != 4 {
		t.Fatalf("expected 4 shuffle-carrying stages, got %d", len(shuffles))
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

		// Every shuffle key must exist in the dependency's column set.
		// Strip table qualifiers ("c.c_custkey" -> "c_custkey") because
		// parseJoinKeys now preserves qualifiers (needed for self-join
		// chain resolution); the worker's shuffle sink uses
		// exec.ColumnIndexFallback which strips them on miss.
		if s.Exchange != nil {
			for _, key := range s.Exchange.Keys {
				bare := key
				if dot := strings.Index(key, "."); dot >= 0 {
					bare = key[dot+1:]
				}
				if !depCols[bare] {
					t.Errorf("shuffle %s (depends on %s %s) has key %q (bare %q) not in dependency columns %v",
						s.ID, dep.Type, dep.ID, key, bare, depCols)
				}
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
		if s.Type != StageExchangeRepartition {
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
		if s.Exchange != nil {
			for _, key := range s.Exchange.Keys {
				if !colSet[key] {
					t.Errorf("shuffle %s: shuffle key %q not in Columns %v", s.ID, key, s.Columns)
				}
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


func TestCanProbeSplit(t *testing.T) {
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
		wantAlias string
		wantOK    bool
	}{
		{
			name:    "single worker never probe-splits",
			stages:  []Stage{{Type: "scan", ScanAlias: "t", ScanFiles: makeFiles(10), EstimatedBytes: 1 << 30}},
			workers: 1,
			wantOK:  false,
		},
		{
			name: "picks largest scan as probe",
			stages: []Stage{
				{Type: "scan", ScanAlias: "lineitem", ScanFiles: makeFiles(12), EstimatedBytes: 7500 << 20},
				{Type: "scan", ScanAlias: "orders", ScanFiles: makeFiles(6), EstimatedBytes: 1700 << 20},
				{Type: "hash_join", JoinType: "inner"},
			},
			workers:   3,
			wantAlias: "lineitem",
			wantOK:    true,
		},
		{
			name: "semi join — largest scan is probe (RightSemiJoin handles local swap)",
			stages: []Stage{
				{Type: "scan", ScanAlias: "lineitem", ScanFiles: makeFiles(12), EstimatedBytes: 7500 << 20},
				{Type: "scan", ScanAlias: "orders", ScanFiles: makeFiles(6), EstimatedBytes: 1700 << 20},
				{Type: "hash_join", JoinType: "semi", BuildTableAlias: "lineitem"},
			},
			workers:   3,
			wantAlias: "lineitem",
			wantOK:    true,
		},
		{
			name: "anti join — largest scan is probe (RightAntiJoin handles local swap)",
			stages: []Stage{
				{Type: "scan", ScanAlias: "lineitem", ScanFiles: makeFiles(12), EstimatedBytes: 7500 << 20},
				{Type: "scan", ScanAlias: "orders", ScanFiles: makeFiles(6), EstimatedBytes: 1700 << 20},
				{Type: "hash_join", JoinType: "anti", BuildTableAlias: "lineitem"},
			},
			workers:   3,
			wantAlias: "lineitem",
			wantOK:    true,
		},
		{
			name: "inner join — largest scan is probe regardless of build alias",
			stages: []Stage{
				{Type: "scan", ScanAlias: "lineitem", ScanFiles: makeFiles(12), EstimatedBytes: 7500 << 20},
				{Type: "scan", ScanAlias: "orders", ScanFiles: makeFiles(6), EstimatedBytes: 1700 << 20},
				{Type: "hash_join", JoinType: "inner", BuildTableAlias: "lineitem"},
			},
			workers:   3,
			wantAlias: "lineitem",
			wantOK:    true,
		},
		{
			name: "small scan with many files still picked as probe",
			stages: []Stage{
				{Type: "scan", ScanAlias: "lineitem", ScanFiles: makeFiles(12), EstimatedBytes: 7500 << 20},
				{Type: "scan", ScanAlias: "orders", ScanFiles: makeFiles(2), EstimatedBytes: 100 << 20},
				{Type: "hash_join", JoinType: "semi", BuildTableAlias: "lineitem"},
			},
			workers:   3,
			wantAlias: "lineitem",
			wantOK:    true,
		},
		{
			name: "large table relaxed min files",
			stages: []Stage{
				{Type: "scan", ScanAlias: "lineitem", ScanFiles: makeFiles(6), EstimatedBytes: 25 << 30},
				{Type: "scan", ScanAlias: "orders", ScanFiles: makeFiles(3), EstimatedBytes: 3 << 30},
				{Type: "hash_join", JoinType: "semi", BuildTableAlias: "lineitem"},
			},
			workers:   3,
			wantAlias: "lineitem",
			wantOK:    true, // lineitem is largest, 6 files ≥ 3 (workerCount, relaxed for >1GB)
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alias, _, ok := CanProbeSplit(tt.stages, tt.workers)
			if ok != tt.wantOK {
				t.Errorf("CanProbeSplit() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && alias != tt.wantAlias {
				t.Errorf("CanProbeSplit() alias = %q, want %q", alias, tt.wantAlias)
			}
		})
	}
}


func TestLargeBuildScans(t *testing.T) {
	const gb = 1024 * 1024 * 1024
	stages := []Stage{
		{ID: "s1", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 60 * gb, ScanFiles: []string{"l1.parquet"}},
		{ID: "s2", Type: "scan", ScanAlias: "orders", EstimatedBytes: 15 * gb, ScanFiles: []string{"o1.parquet"}},
		{ID: "s3", Type: "scan", ScanAlias: "partsupp", EstimatedBytes: 8 * gb, ScanFiles: []string{"ps1.parquet"}},
		{ID: "s4", Type: "scan", ScanAlias: "nation", EstimatedBytes: 1024, ScanFiles: []string{"n1.parquet"}},
		{ID: "s5", Type: "hash_join", ScanAlias: ""},
	}
	threshold := int64(2 * gb)

	t.Run("excludes probe alias and small tables", func(t *testing.T) {
		large := LargeBuildScans(stages, "lineitem", threshold)
		// should include orders and partsupp, but not lineitem (probe) or nation (small) or hash_join
		if len(large) != 2 {
			t.Fatalf("want 2 large build scans, got %d: %v", len(large), large)
		}
		aliases := map[string]bool{}
		for _, s := range large {
			aliases[s.ScanAlias] = true
		}
		if !aliases["orders"] || !aliases["partsupp"] {
			t.Errorf("expected orders and partsupp, got %v", aliases)
		}
		if aliases["lineitem"] {
			t.Errorf("probe alias lineitem should be excluded")
		}
	})

	t.Run("no large scans below threshold", func(t *testing.T) {
		large := LargeBuildScans(stages, "lineitem", 100*gb)
		if len(large) != 0 {
			t.Fatalf("want 0 large build scans at 100GB threshold, got %d", len(large))
		}
	})

	t.Run("empty stages", func(t *testing.T) {
		large := LargeBuildScans(nil, "lineitem", threshold)
		if len(large) != 0 {
			t.Fatalf("want 0 for nil stages, got %d", len(large))
		}
	})

	t.Run("probe alias is only large scan", func(t *testing.T) {
		onlyProbe := []Stage{
			{ID: "s1", Type: "scan", ScanAlias: "lineitem", EstimatedBytes: 60 * gb},
			{ID: "s2", Type: "scan", ScanAlias: "nation", EstimatedBytes: 1024},
		}
		large := LargeBuildScans(onlyProbe, "lineitem", threshold)
		if len(large) != 0 {
			t.Fatalf("want 0 when only large scan is probe, got %d", len(large))
		}
	})
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

	emitMergeAggregateTree(&stages, leafIDs, groupBy, nil, nil, aggSpecs, stages[:20])

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

	emitMergeAggregateTree(&stages, leafIDs, []string{"g"}, nil, nil, []AggSpec{{Func: "sum", InputCol: "x", OutputCol: "sx"}}, stages[:8])

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


// TestBuildTopN_WiresSpillManager: ORDER BY ... LIMIT plans must attach the
// spill manager like plain ORDER BY does — without it the pre-sort input
// buffered fully untracked (the top-K heap only runs at finalize).
func TestBuildTopN_WiresSpillManager(t *testing.T) {
	cat, ctx := setupCatalog(t)
	p := NewPlanner(cat)
	p.MemoryBudget = 1 << 30

	node := logical.NewScan("events", "e")
	sortNode := logical.NewSort(node, []logical.OrderExpr{{Column: "event_id"}})

	src, _, _, err := p.buildTopN(ctx, sortNode, 10)
	if err != nil {
		t.Fatalf("buildTopN: %v", err)
	}
	adapter, ok := src.(*sortSourceAdapter)
	if !ok {
		t.Fatalf("expected sortSourceAdapter, got %T", src)
	}
	if adapter.sort.Spill == nil {
		t.Fatal("buildTopN left Sort.Spill nil — pre-sort input is untracked and unspillable")
	}
	if adapter.sort.Limit != 10 {
		t.Fatalf("Limit = %d, want 10", adapter.sort.Limit)
	}
}
