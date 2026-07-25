package coordinator

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
)

// chainFusionBroadcastThreshold forces the SF10-like join mix at SF0.01
// test scale: mid tables (orders, customer, lineitem partitions) hash-join
// while small dims broadcast — the shape whose 1:1 chains fuseStageChains
// targets. Probed 2026-07-25: at 16 KB all eight chain-bearing TPC-H
// queries plan with ChainedJoins; the cluster-derived default broadcasts
// everything at this scale and fuses nothing.
const chainFusionBroadcastThreshold = 16 << 10

// TestStageChainFusionDifferential runs the TPC-H queries whose plans carry
// 1:1 join chains (the fuseStageChains targets) through a real 3-worker
// cluster twice — fusion on and off — and diffs the full sorted row sets.
// This is the correctness gate for stage-chain fusion: both arms must be
// row-identical (docs/design/stage-chain-fusion.md). A plan-level
// engagement guard keeps the diff from passing vacuously if plan shapes
// drift away from fusing.
func TestStageChainFusionDifferential(t *testing.T) {
	ctx, coord := setupTPCHDistributedAtScale(t, tpch.SF001)
	coord.config.BroadcastBytesOverride = chainFusionBroadcastThreshold

	prev := physical.StageFusion.Load()
	t.Cleanup(func() { physical.StageFusion.Store(prev) })

	runArm := func(t *testing.T, qNum int, fusion bool) []map[string]any {
		t.Helper()
		physical.StageFusion.Store(fusion)
		result, err := coord.ExecuteSQL(ctx, tpch.TPCHQueries[qNum].SQL)
		if err != nil {
			t.Fatalf("Q%02d fusion=%v: %v", qNum, fusion, err)
		}
		if result.Error != "" {
			t.Fatalf("Q%02d fusion=%v: %s", qNum, fusion, result.Error)
		}
		rows := mustRows(t, result)
		sort.Slice(rows, func(i, j int) bool {
			return fmt.Sprint(rows[i]) < fmt.Sprint(rows[j])
		})
		return rows
	}

	// planEngages mirrors the coordinator's planning inputs and reports
	// whether fuseStageChains fires on the query.
	planEngages := func(t *testing.T, qNum int) bool {
		t.Helper()
		physical.StageFusion.Store(true)
		stages := planStagesForTest(t, ctx, coord.catalog, tpch.TPCHQueries[qNum].SQL, 3, chainFusionBroadcastThreshold)
		for _, s := range stages {
			if len(s.ChainedJoins) > 0 {
				return true
			}
		}
		return false
	}

	// Queries whose plans carry fusable 1:1 join chains (scout 2026-07-25):
	// Q02 Q05 Q07 Q08 Q09 Q10 Q18 Q21. Q03 rides along as a no-chain control.
	engaged := 0
	for _, qNum := range []int{2, 3, 5, 7, 8, 9, 10, 18, 21} {
		qNum := qNum
		t.Run(fmt.Sprintf("Q%02d", qNum), func(t *testing.T) {
			if planEngages(t, qNum) {
				engaged++
				t.Logf("Q%02d: fusion engages at this scale", qNum)
			}
			off := runArm(t, qNum, false)
			on := runArm(t, qNum, true)
			if len(on) != len(off) {
				t.Fatalf("row count: fused=%d unfused=%d", len(on), len(off))
			}
			for i := range off {
				if !reflect.DeepEqual(on[i], off[i]) {
					t.Fatalf("row %d differs:\n  fused:   %v\n  unfused: %v", i, on[i], off[i])
				}
			}
			t.Logf("Q%02d: %d rows identical across arms", qNum, len(off))
		})
	}
	if engaged < 6 {
		t.Errorf("fusion engaged on only %d queries; differential is near-vacuous (want >= 6)", engaged)
	}
}

// planStagesForTest plans SQL through PlanDistributed with the given worker
// count and broadcast threshold, mirroring the coordinator's inputs.
func planStagesForTest(t *testing.T, ctx context.Context, cat *catalog.Catalog, sql string, workers int, broadcastThreshold int64) []physical.Stage {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("logical plan: %v", err)
	}
	planner := physical.NewPlanner(cat)
	planner.WorkerCount = workers
	planner.BroadcastBytesThreshold = broadcastThreshold
	planner.AnnotateScanColumns(ctx, plan)
	optimized := logical.Optimize(plan, func(p *logical.Node) {
		planner.AnnotateScanColumns(ctx, p)
	})
	stages, err := planner.PlanDistributed(ctx, optimized)
	if err != nil {
		t.Fatalf("plan distributed: %v", err)
	}
	return stages
}
