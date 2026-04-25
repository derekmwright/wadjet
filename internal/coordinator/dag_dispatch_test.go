package coordinator

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestDispatchStageDAG_RoutesPerType verifies each stage type lands in
// the right backend via the dispatchHooks test seam.
func TestDispatchStageDAG_RoutesPerType(t *testing.T) {
	coord := &Coordinator{}
	calls := map[string]int{}
	coord.dispatchHooks = &dispatchHooks{
		onRepartition: func(s physical.Stage) { calls["rep"]++ },
		onReplicate:   func(s physical.Stage) { calls["repl"]++ },
		onGather:      func(s physical.Stage) { calls["gather"]++ },
		onPipeline:    func(s physical.Stage) { calls["pipe"]++ },
		onScan:        func(s physical.Stage) { calls["scan"]++ },
	}
	plan := []physical.Stage{
		{ID: "scan-0", Type: physical.StageScan},
		{ID: "rep-0", Type: physical.StageExchangeRepartition, Dependencies: []string{"scan-0"}},
		{ID: "repl-0", Type: physical.StageExchangeReplicate, Dependencies: []string{"rep-0"}},
		{ID: "pipe-0", Type: physical.StagePipeline, Dependencies: []string{"repl-0"}},
		{ID: "gather-0", Type: physical.StageExchangeGather, Dependencies: []string{"pipe-0"}},
	}
	if err := coord.dispatchStageDAG(context.Background(), plan); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if calls["rep"] != 1 || calls["repl"] != 1 || calls["gather"] != 1 ||
		calls["pipe"] != 1 || calls["scan"] != 1 {
		t.Errorf("dispatch counts wrong: %v", calls)
	}
}

// TestDispatchStageDAG_UnknownType returns an error rather than panicking.
func TestDispatchStageDAG_UnknownType(t *testing.T) {
	coord := &Coordinator{}
	plan := []physical.Stage{{ID: "bogus-0", Type: "__unknown__"}}
	if err := coord.dispatchStageDAG(context.Background(), plan); err == nil {
		t.Fatal("expected error for unknown stage type")
	}
}

// TestHasExchangeStages covers the detection helper.
func TestHasExchangeStages(t *testing.T) {
	if hasExchangeStages(nil) {
		t.Error("nil: expected false")
	}
	if hasExchangeStages([]physical.Stage{{Type: physical.StageScan}}) {
		t.Error("no exchanges: expected false")
	}
	if !hasExchangeStages([]physical.Stage{{Type: physical.StageExchangeReplicate}}) {
		t.Error("with replicate: expected true")
	}
	if !hasExchangeStages([]physical.Stage{{Type: physical.StageExchangeGather}}) {
		t.Error("with gather: expected true")
	}
	if !hasExchangeStages([]physical.Stage{{Type: physical.StageExchangeRepartition}}) {
		t.Error("with repartition: expected true")
	}
}
