package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestPruneBuildColumns_PartitionedBuildSafe: PruneBuildColumns after a
// partition-on-arrival build must not touch buildBatches — evicted
// partitions nil their entries (join_partition_arrival.go), and restored
// partitions carry the unpruned schema. Before the guard, every SF10+
// standalone Q21 run (semi/anti join with a join filter, build side large
// enough to evict) crashed with a nil dereference in PruneBuildColumns
// called from the planner's post-build hook — the 2026-07-05 finding that
// killed the cold-S3 suite after Q20.
func TestPruneBuildColumns_PartitionedBuildSafe(t *testing.T) {
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
	// TestPartitionOnArrival_BasicSpill's working point).
	tracker := memory.NewTracker("test", 400_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Semi join with a join filter keeps SemiAntiKeyOnly false, so the
	// build stores batches and dispatches to buildPartitioned.
	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
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

	// The planner's post-build hook for semi/anti joins with a filter.
	// Pre-guard this dereferenced the nil'd entries and crashed.
	hj.PruneBuildColumns([]string{"val"})

	// The partitioned representation must be untouched: schema unpruned,
	// batch slots still consistent with the partition bookkeeping.
	if len(hj.buildSchema) != len(rightSchema) {
		t.Fatalf("buildSchema pruned to %d cols on a partitioned build, want untouched %d",
			len(hj.buildSchema), len(rightSchema))
	}
}
