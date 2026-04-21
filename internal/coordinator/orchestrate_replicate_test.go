package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// TestOrchestrateReplicate_WrongType ensures the shim rejects stages that
// aren't StageExchangeReplicate.
func TestOrchestrateReplicate_WrongType(t *testing.T) {
	coord := &Coordinator{} // shim should reject before touching state
	_, err := coord.orchestrateReplicate(context.Background(),
		physical.Stage{Type: physical.StageExchangeRepartition},
		"qid", "SELECT 1", nil, "lineitem")
	if err == nil {
		t.Fatal("expected error for wrong stage type")
	}
	if !strings.Contains(err.Error(), "exchange-replicate") {
		t.Errorf("error should mention expected type: %v", err)
	}
}

// TestOrchestrateReplicate_DelegatesToPreScan verifies that orchestrateReplicate
// with a valid StageExchangeReplicate stage produces the same result as calling
// preScanBuildTables directly (i.e., nil cache for all-small tables).
func TestOrchestrateReplicate_DelegatesToPreScan(t *testing.T) {
	ctx, coord, _ := setupDistributed(t)

	stages := []physical.Stage{
		{ID: "s1", Type: physical.StageScan, ScanAlias: "lineitem", TableName: "lineitem",
			EstimatedBytes: 50 * 1024 * 1024, ScanFiles: []string{"l1.parquet"}},
		{ID: "s2", Type: physical.StageScan, ScanAlias: "orders", TableName: "orders",
			EstimatedBytes: 30 * 1024 * 1024, ScanFiles: []string{"o1.parquet"}},
	}

	replicateStage := physical.Stage{ID: "ex-0", Type: physical.StageExchangeReplicate}

	// Call via the shim.
	shimCache, shimErr := coord.orchestrateReplicate(ctx, replicateStage, "qid-replicate", "SELECT 1", stages, "lineitem")

	// Call directly for comparison.
	directCache, directErr := coord.preScanBuildTables(ctx, "qid-replicate", "SELECT 1", stages, "lineitem")

	if shimErr != nil {
		t.Fatalf("orchestrateReplicate returned unexpected error: %v", shimErr)
	}
	if directErr != nil {
		t.Fatalf("preScanBuildTables returned unexpected error: %v", directErr)
	}
	if len(shimCache) != len(directCache) {
		t.Errorf("cache length mismatch: shim=%d direct=%d", len(shimCache), len(directCache))
	}
}
