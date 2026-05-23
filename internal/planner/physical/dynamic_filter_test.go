package physical

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
)

func sqlToStagesWithDynamicFilters(t *testing.T, cat *catalog.Catalog, ctx context.Context, sql string, workerCount int) []Stage {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	scanAnnotator := func(plan *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	logicalPlan = logical.Optimize(logicalPlan, scanAnnotator)

	planner := NewPlanner(cat)
	planner.WorkerCount = workerCount
	planner.DynamicFiltersEnabled = true

	stages, err := planner.PlanDistributed(ctx, logicalPlan)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}

// TestDynamicFilterPassQ17 verifies Q17 gets Emit/Consume annotations +
// the stat-dep edge when DynamicFiltersEnabled is on.
//
// Q17 shape (SF10-sized catalog):
//
//	scan-0 (lineitem, 60M rows)   ── exchange-repartition ──┐
//	                                                         ├── hash_join (l_partkey = p_partkey)
//	scan-1 (part filtered ~13K)   ── exchange-repartition ──┘
//
// Expected: scan-1 emits a bloom on p_partkey; scan-0 consumes it on
// l_partkey; scan-0.Dependencies includes scan-1.ID (stat-dep edge).
func TestDynamicFilterPassQ17(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT
		SUM(l_extendedprice) / 7.0 as avg_yearly
	FROM lineitem
	JOIN part ON p_partkey = l_partkey
	WHERE p_brand = 'Brand#23'
		AND p_container = 'MED BOX'
		AND l_quantity < (
			SELECT 0.2 * AVG(l_quantity)
			FROM lineitem
			WHERE l_partkey = p_partkey
		)`

	stages := sqlToStagesWithDynamicFilters(t, cat, ctx, sql, 3)

	var partScan, lineitemScan *Stage
	for i := range stages {
		s := &stages[i]
		if s.Type != StageScan {
			continue
		}
		if s.TableName == "part" {
			partScan = s
		}
		if s.TableName == "lineitem" && lineitemScan == nil {
			lineitemScan = s
		}
	}
	if partScan == nil {
		t.Fatal("missing part scan stage")
	}
	if lineitemScan == nil {
		t.Fatal("missing lineitem scan stage")
	}

	if len(partScan.EmitDynamicFilters) == 0 {
		t.Errorf("part scan should emit; got 0 entries")
	} else {
		found := false
		for _, e := range partScan.EmitDynamicFilters {
			if e.KeyColumn == "p_partkey" {
				found = true
				if e.KeyType != "int32" && e.KeyType != "int64" {
					t.Errorf("KeyType = %q, want int32 or int64", e.KeyType)
				}
				if e.BloomBits < 1024 {
					t.Errorf("BloomBits = %d, want >= 1024", e.BloomBits)
				}
			}
		}
		if !found {
			t.Errorf("part scan EmitDynamicFilters missing p_partkey: %+v", partScan.EmitDynamicFilters)
		}
	}

	if len(lineitemScan.ConsumeDynamicFilters) == 0 {
		t.Errorf("lineitem scan should consume; got 0 entries")
	} else {
		found := false
		for _, c := range lineitemScan.ConsumeDynamicFilters {
			if c.TargetColumn == "l_partkey" && c.SourceStageID == partScan.ID {
				found = true
			}
		}
		if !found {
			t.Errorf("lineitem scan ConsumeDynamicFilters missing l_partkey from %s: %+v",
				partScan.ID, lineitemScan.ConsumeDynamicFilters)
		}
	}

	depFound := false
	for _, d := range lineitemScan.Dependencies {
		if d == partScan.ID {
			depFound = true
			break
		}
	}
	if !depFound {
		t.Errorf("lineitem scan Dependencies missing stat-dep edge to %s: %v",
			partScan.ID, lineitemScan.Dependencies)
	}
}

// TestDynamicFilterPassDisabled confirms the gate — flag off → no annotations.
func TestDynamicFilterPassDisabled(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const sql = `SELECT SUM(l_extendedprice)
		FROM lineitem JOIN part ON p_partkey = l_partkey
		WHERE p_brand = 'Brand#23'`
	stages := sqlToStages(t, cat, ctx, sql, 3) // default planner — flag off.
	for _, s := range stages {
		if len(s.EmitDynamicFilters) > 0 {
			t.Errorf("stage %s has EmitDynamicFilters when flag is off", s.ID)
		}
		if len(s.ConsumeDynamicFilters) > 0 {
			t.Errorf("stage %s has ConsumeDynamicFilters when flag is off", s.ID)
		}
	}
}
