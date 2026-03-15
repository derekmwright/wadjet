package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/caelum/internal/planner/logical"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// setupCatalog creates an in-memory catalog with an "events" table containing
// one partition and one file. It returns the catalog and a cancel-safe context.
func setupCatalog(t *testing.T) (*catalog.Catalog, context.Context) {
	t.Helper()
	ctx := context.Background()

	store := objstore.NewMemStore()
	cat := catalog.New(store, "test")
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

	if len(stages) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(stages))
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

	// Sort depends on both previous stages
	if len(stages[2].Dependencies) < 2 {
		t.Errorf("sort stage should depend on scan and aggregate stages, got %v", stages[2].Dependencies)
	}
	if len(stages[2].SortKeys) != 1 {
		t.Fatalf("expected 1 sort key, got %d", len(stages[2].SortKeys))
	}
	if stages[2].SortKeys[0].Column != "cnt" || !stages[2].SortKeys[0].Desc {
		t.Errorf("unexpected sort key: %+v", stages[2].SortKeys[0])
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
