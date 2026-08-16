package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestPartitionOnArrival_SpillEligibleAllocatesSpillState confirms that any
// spill-eligible build (MemTracker + Spill both set) routes through the
// partition-on-arrival path and allocates spillState upfront. There is no
// at-entry pressure heuristic any more — partitioning is unconditional for
// spill-eligible builds, so the flat path is reached only by callers without a
// tracker/spill. (Replaces the retired TestSharedPoolUnderPressure and
// TestPartitionOnArrival_LowPressureFlatPath_NoAllocBlowup, whose
// low-pressure-stays-flat guarantee is now structural rather than heuristic.)
func TestPartitionOnArrival_SpillEligibleAllocatesSpillState(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := make([]map[string]any, 1000)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i)}
	}

	tmpDir := t.TempDir()
	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker

	if err := hj.Build(context.Background(), NewSliceSource(schema, rows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState == nil {
		t.Fatal("spill-eligible build must partition on arrival and allocate spillState upfront")
	}
}
