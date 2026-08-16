package coordinator

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
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
	prevAgg := physical.StageFusionAgg.Load()
	t.Cleanup(func() {
		physical.StageFusion.Store(prev)
		physical.StageFusionAgg.Store(prevAgg)
	})

	runArm := func(t *testing.T, qNum int, fusion, aggFusion bool) []map[string]any {
		t.Helper()
		physical.StageFusion.Store(fusion)
		physical.StageFusionAgg.Store(aggFusion)
		result, err := coord.ExecuteSQL(ctx, tpch.TPCHQueries[qNum].SQL)
		if err != nil {
			t.Fatalf("Q%02d fusion=%v/agg=%v: %v", qNum, fusion, aggFusion, err)
		}
		if result.Error != "" {
			t.Fatalf("Q%02d fusion=%v/agg=%v: %s", qNum, fusion, aggFusion, result.Error)
		}
		rows := mustRows(t, result)
		sort.Slice(rows, func(i, j int) bool {
			return fmt.Sprint(rows[i]) < fmt.Sprint(rows[j])
		})
		return rows
	}

	// planEngages mirrors the coordinator's planning inputs and reports
	// which fusion mechanisms fire on the query.
	planEngages := func(t *testing.T, qNum int) (joins, aggs bool) {
		t.Helper()
		physical.StageFusion.Store(true)
		physical.StageFusionAgg.Store(true)
		stages := planStagesForTest(t, ctx, coord.catalog, tpch.TPCHQueries[qNum].SQL, 3, chainFusionBroadcastThreshold)
		for _, s := range stages {
			if len(s.ChainedJoins) > 0 {
				joins = true
			}
			if len(s.ChainedAggSpecs) > 0 || len(s.ChainedAggGroupBy) > 0 {
				aggs = true
			}
		}
		return joins, aggs
	}

	// Queries whose plans carry fusable 1:1 join chains (scout 2026-07-25):
	// Q02 Q05 Q07 Q08 Q09 Q10 Q18 Q21. Q03 rides along as a join→agg-only
	// shape; the three arms are kill-switch / join-fusion-only / full.
	joinEngaged, aggEngaged := 0, 0
	for _, qNum := range []int{2, 3, 5, 7, 8, 9, 10, 18, 21} {
		qNum := qNum
		t.Run(fmt.Sprintf("Q%02d", qNum), func(t *testing.T) {
			j, a := planEngages(t, qNum)
			if j {
				joinEngaged++
			}
			if a {
				aggEngaged++
			}
			t.Logf("Q%02d engagement: joins=%v aggs=%v", qNum, j, a)
			off := runArm(t, qNum, false, false)
			for _, arm := range []struct {
				name          string
				fusion, aggOn bool
			}{{"join-only", true, false}, {"full", true, true}} {
				got := runArm(t, qNum, arm.fusion, arm.aggOn)
				if len(got) != len(off) {
					t.Fatalf("%s arm row count: %d vs unfused %d", arm.name, len(got), len(off))
				}
				for i := range off {
					if err := rowsEquivalent(got[i], off[i]); err != nil {
						t.Fatalf("%s arm row %d differs (%v):\n  fused:   %v\n  unfused: %v", arm.name, i, err, got[i], off[i])
					}
				}
			}
			t.Logf("Q%02d: %d rows identical across all three arms", qNum, len(off))
		})
	}
	if joinEngaged < 6 {
		t.Errorf("join fusion engaged on only %d queries; differential is near-vacuous (want >= 6)", joinEngaged)
	}
	if aggEngaged < 4 {
		t.Errorf("agg absorb engaged on only %d queries; differential is near-vacuous (want >= 4)", aggEngaged)
	}
}

// rowsEquivalent compares two result rows: non-float columns must match
// exactly; float columns within 1e-9 relative tolerance. The agg absorb
// changes how many partials the final merges (per-join-task instead of the
// dropped stage's fan-out), so float SUMs legitimately drift in the last
// bits — the same accumulation-order class the engine accepts across plan
// shapes (TPC-H validation itself is tolerance-based).
func rowsEquivalent(a, b map[string]any) error {
	if len(a) != len(b) {
		return fmt.Errorf("column count %d vs %d", len(a), len(b))
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return fmt.Errorf("column %s missing", k)
		}
		fa, aok := va.(float64)
		fb, bok := vb.(float64)
		if aok && bok {
			if fa != fb {
				den := math.Max(math.Abs(fa), math.Abs(fb))
				if den > 0 && math.Abs(fa-fb)/den > 1e-9 {
					return fmt.Errorf("column %s: %v vs %v", k, fa, fb)
				}
			}
			continue
		}
		if !reflect.DeepEqual(va, vb) {
			return fmt.Errorf("column %s: %v vs %v", k, va, vb)
		}
	}
	return nil
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
