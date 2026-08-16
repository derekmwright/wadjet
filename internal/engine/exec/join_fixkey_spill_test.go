package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestFixKeyAssignment_PartitionedBuildSafe: FixKeyAssignment's rebuild
// path must not touch buildBatches after a partition-on-arrival build —
// evicted partitions nil their entries (join_partition_arrival.go), the
// same hazard PruneBuildColumns hit on SF10 standalone Q21. The trigger
// needs BOTH a spilled build and a misassigned key pair: the SQL put the
// build column on the left of "=", so RightKeys carries a probe-qualified
// name that only resolves in the build schema via columnIndexFallback.
// Pre-guard, the swap set needsRebuild and the rebuild walked the nil'd
// entries. The guard keeps the swap (probe resolution needs the corrected
// names) and skips the rebuild — the arrival-time index is already keyed
// on the correct physical column, so it must survive untouched.
func TestFixKeyAssignment_PartitionedBuildSafe(t *testing.T) {
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	const buildN = 10000
	rightRows := make([]map[string]any, buildN)
	for i := range rightRows {
		rightRows[i] = map[string]any{
			"id":  int64(i),
			"val": "build-row-padding-for-size",
		}
	}

	tmpDir := t.TempDir()
	// Budget tight enough that partitions must evict during build —
	// eviction is what nils buildBatches entries (matches
	// TestPruneBuildColumns_PartitionedBuildSafe's working point).
	tracker := memory.NewTracker("test", 400_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Misassigned pair: "id" (build column) landed in LeftKeys, and
	// RightKeys got the probe-qualified "src.id" — which
	// columnIndexFallback resolves to the build's "id" at arrival time,
	// so the build succeeds and indexes the correct column. Semi join
	// with a join filter keeps SemiAntiKeyOnly false, so batches are
	// stored and the build dispatches to buildPartitioned.
	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"src.id"})
	hj.Spill = sm
	hj.MemTracker = tracker

	if err := hj.Build(context.Background(), NewSliceSource(rightSchema, rightRows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState == nil {
		t.Fatal("test setup: build did not take the partition-on-arrival path")
	}
	nilEntries := 0
	for _, b := range hj.buildBatches {
		if b == nil {
			nilEntries++
		}
	}
	if nilEntries == 0 {
		t.Fatal("test setup: no partition was evicted — budget too generous to exercise the crash")
	}
	rowsBefore := hj.buildRows
	keyIdxBefore := append([]int(nil), hj.buildKeyIdx...)

	// The planner's post-build hook. Pre-guard this swapped the keys,
	// set needsRebuild, and dereferenced the nil'd entries.
	hj.FixKeyAssignment()

	// The swap itself must stand — probe-side resolution depends on it.
	if hj.LeftKeys[0] != "src.id" || hj.RightKeys[0] != "id" {
		t.Fatalf("keys not swapped: left=%v right=%v", hj.LeftKeys, hj.RightKeys)
	}
	// The partitioned index must be untouched: same rows, same build key
	// column — a rebuild from the nil-holed buildBatches would have
	// dropped every evicted partition's rows even if it didn't crash.
	if hj.buildRows != rowsBefore {
		t.Fatalf("buildRows changed %d → %d; partitioned index was rebuilt", rowsBefore, hj.buildRows)
	}
	for i, idx := range hj.buildKeyIdx {
		if idx != keyIdxBefore[i] {
			t.Fatalf("buildKeyIdx[%d] changed %d → %d", i, keyIdxBefore[i], idx)
		}
	}
}
