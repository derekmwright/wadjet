//go:build !race

// See distributed_tpch_test.go for why this package is skipped under -race
// (nats-server v2.12.6 upstream race under concurrent JetStream consumer
// mutations).

package coordinator

import (
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestPreComputeDerivedAggregate_Q17SF001 exercises the Phase 1 pre-compute
// primitive end-to-end against the SF0.01 distributed harness: the
// coordinator detects Q17's derived-aggregate build, constructs the
// aggregate SQL from stage metadata, dispatches it to an in-process worker,
// and receives the result file path. Confirms the wiring works — the
// worker-side subplan substitution that actually USES the output lands in
// the follow-up commit.
//
// The detection threshold is bypassed by calling the primitive directly
// with the candidate derived from Q17's plan; at SF0.01 the scan is tiny
// and below the production 4 GB threshold, but the primitive is agnostic
// to the threshold check (that's the caller's job).
func TestPreComputeDerivedAggregate_Q17SF001(t *testing.T) {
	ctx, coord := setupTPCHDistributed(t)

	// Plan Q17 through the same path ExecuteSQL uses so we get a realistic
	// stage graph and a real AggregateShuffleCandidate.
	q17 := tpch.TPCHQueries[17].SQL
	parsed, err := plansql.Parse(q17)
	if err != nil {
		t.Fatalf("parse Q17: %v", err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract Q17: %v", err)
	}
	plan, err := logical.BuildFromSelect(info)
	if err != nil {
		t.Fatalf("build Q17 logical plan: %v", err)
	}
	planner := physical.NewPlanner(coord.catalog)
	planner.WorkerCount = coord.workers.Count()
	planner.AnnotateScanColumns(ctx, plan)
	optimized := logical.Optimize(plan, func(p *logical.Node) {
		planner.AnnotateScanColumns(ctx, p)
	})
	physPlan, err := planner.Plan(ctx, optimized)
	if err != nil {
		t.Fatalf("Q17 physical plan: %v", err)
	}
	stages := physPlan.Stages

	// Bypass the production threshold — at SF0.01 the input scan is well
	// below 4 GB, so PickAggregateShuffleCandidate would return !ok. Use a
	// tiny threshold so the detection fires for this test. Production code
	// uses the real threshold; this just exercises the dispatch primitive.
	cand, ok := physical.PickAggregateShuffleCandidate(stages, 1)
	if !ok {
		t.Fatal("Q17 at SF0.01: expected aggregate-shuffle candidate with threshold=1")
	}
	t.Logf("candidate: agg=%s input=%s bytes=%d groupBy=%v",
		cand.AggregateStageID, cand.InputScanAlias, cand.InputScanBytes, cand.GroupByKeys)

	paths, err := coord.preComputeDerivedAggregate(ctx, "test-q17", cand, stages)
	if err != nil {
		t.Fatalf("preComputeDerivedAggregate: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("expected non-empty result paths from aggregate pre-compute")
	}
	t.Logf("aggregate pre-compute produced %d cache file(s): %v", len(paths), paths)

	// Sanity-check: each path should exist in the result bucket and be
	// non-empty. The bucket is an in-memory MemStore populated by the worker.
	for _, p := range paths {
		obj, _, err := coord.catalog.Store().Get(ctx, coord.config.ResultBucket, p)
		if err != nil {
			t.Fatalf("get %s: %v", p, err)
		}
		// Just confirm the object exists — structural validation of the .wshf
		// format is out of scope for this test.
		_ = obj.Close()
	}
}
