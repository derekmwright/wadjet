package coordinator

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// TestOrchestrateGather_WrongType ensures the shim rejects stages that
// aren't StageExchangeGather.
func TestOrchestrateGather_WrongType(t *testing.T) {
	coord := &Coordinator{}
	_, _, err := coord.orchestrateGather(
		physical.Stage{Type: physical.StageExchangeReplicate},
		newSliceStream(nil), nil, nil,
	)
	if err == nil {
		t.Fatal("expected error for wrong stage type")
	}
	if !strings.Contains(err.Error(), "exchange-gather") {
		t.Errorf("error should mention expected type: %v", err)
	}
}
