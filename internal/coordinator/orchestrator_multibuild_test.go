package coordinator

import "testing"

// TestMultiBuildShuffle_Dormant ensures orchestrateMultiBuildShuffle
// compiles and is reachable. It is not wired into dispatch until Phase 3.
func TestMultiBuildShuffle_Dormant(t *testing.T) {
	// Verify the method exists and accepts the expected shape.
	// A real end-to-end test lives in Phase 3 when the orchestrator
	// is called from coordinator dispatch.
	var _ = (*Coordinator).orchestrateMultiBuildShuffle
}
