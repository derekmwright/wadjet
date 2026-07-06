package exec

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// storeProjBuildSchema is the wide build schema for the projection tests:
// only id (join key) and threshold (filter column) are needed at probe time;
// pad1/pad2 exist to be dropped by BuildStoreCols.
var storeProjBuildSchema = []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "pad1", Type: parquet.TypeString},
	{Name: "threshold", Type: parquet.TypeFloat64},
	{Name: "pad2", Type: parquet.TypeString},
}

func storeProjBuildRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"id":        int64(i),
			"pad1":      "wide-column-that-projection-should-drop-aaaaaaaa",
			"threshold": float64(i % 100),
			"pad2":      "wide-column-that-projection-should-drop-bbbbbbbb",
		}
	}
	return rows
}

var storeProjProbeSchema = []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "val", Type: parquet.TypeFloat64},
}

func storeProjProbeRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "val": 50.0}
	}
	return rows
}

// storeProjFilter mirrors physical.BuildSemiAntiFilter's contract: columns
// are resolved by NAME against whatever storage the build retained — this is
// what makes name-preserving projection transparent to the probe.
func storeProjFilter(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	pi := probe.ColumnIndex("val")
	bi := build.ColumnIndex("threshold")
	if pi < 0 || bi < 0 {
		return false
	}
	return probe.Columns[pi].Float64Data[probeRow] > build.Columns[bi].Float64Data[buildRow]
}

// runStoreProjProbe probes hj with probeN rows and returns the emitted ids.
func runStoreProjProbe(t *testing.T, hj *HashJoin, probeN int) map[int64]bool {
	t.Helper()
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(storeProjProbeSchema, storeProjProbeRows(probeN)),
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("probe pipeline: %v", err)
	}
	ids := make(map[int64]bool, len(sink.Rows))
	for _, row := range sink.Rows {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("expected int64 id, got %T", row["id"])
		}
		if ids[id] {
			t.Fatalf("duplicate id %d in semi/anti output", id)
		}
		ids[id] = true
	}
	return ids
}

// TestBuildStoreCols_FilteredSemiPartitionedSpill: the arrival-time
// projection must keep a partition-on-arrival build narrow (keys + filter
// columns only) through eviction, spill files, and the spilled-partition
// probe replay — while producing exactly the rows an unprojected filtered
// semi join produces. This is the production Q21 shape: semi join with a
// JoinFilter, build too large for the budget.
func TestBuildStoreCols_FilteredSemiPartitionedSpill(t *testing.T) {
	const buildN, probeN = 10000, 1000

	tmpDir := t.TempDir()
	tracker := memory.NewTracker("test", 400_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker
	hj.SemiAntiFilter = storeProjFilter
	hj.BuildStoreCols = []string{"id", "threshold"}

	if err := hj.Build(context.Background(), NewSliceSource(storeProjBuildSchema, storeProjBuildRows(buildN))); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState == nil {
		t.Fatal("test setup: build did not take the partition-on-arrival path")
	}
	if len(hj.spillState.spilledParts) == 0 {
		t.Fatal("test setup: no partition spilled — budget too generous to exercise the spilled replay")
	}
	if got, want := len(hj.buildSchema), 2; got != want {
		t.Fatalf("buildSchema has %d columns, want %d (projection did not narrow the stored build)", got, want)
	}

	// Probe row i (val=50.0) matches iff build threshold (id%100) < 50.
	ids := runStoreProjProbe(t, hj, probeN)
	if len(ids) != probeN/2 {
		t.Fatalf("semi join emitted %d rows, want %d", len(ids), probeN/2)
	}
	for id := range ids {
		if id%100 >= 50 {
			t.Fatalf("semi join emitted id %d whose filter (threshold=%d < 50) is false", id, id%100)
		}
	}
}

// TestBuildStoreCols_FilteredAntiPartitionedSpill: the anti variant of the
// spill test — unmatched-or-filter-false probe rows must be emitted, and the
// spilled-partition replay must evaluate the filter against the narrow
// spilled build data (which is why the join keys ride along in
// BuildStoreCols: the replay re-indexes spilled batches by key).
func TestBuildStoreCols_FilteredAntiPartitionedSpill(t *testing.T) {
	const buildN, probeN = 10000, 1000

	tmpDir := t.TempDir()
	tracker := memory.NewTracker("test", 400_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(AntiJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker
	hj.SemiAntiFilter = storeProjFilter
	hj.BuildStoreCols = []string{"id", "threshold"}

	if err := hj.Build(context.Background(), NewSliceSource(storeProjBuildSchema, storeProjBuildRows(buildN))); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
		t.Fatal("test setup: build did not spill")
	}
	if got, want := len(hj.buildSchema), 2; got != want {
		t.Fatalf("buildSchema has %d columns, want %d", got, want)
	}

	ids := runStoreProjProbe(t, hj, probeN)
	if len(ids) != probeN/2 {
		t.Fatalf("anti join emitted %d rows, want %d", len(ids), probeN/2)
	}
	for id := range ids {
		if id%100 < 50 {
			t.Fatalf("anti join emitted id %d whose filter (threshold=%d < 50) is true — should have been suppressed", id, id%100)
		}
	}
}

// TestBuildStoreCols_FlatPathNarrowsStorage: the no-spill flat build path
// (embedded queries, tests) stores projectForStore views over the arrival
// batches; results and storage width must match the partitioned path.
func TestBuildStoreCols_FlatPathNarrowsStorage(t *testing.T) {
	const buildN, probeN = 5000, 1000

	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.SemiAntiFilter = storeProjFilter
	hj.BuildStoreCols = []string{"id", "threshold"}

	if err := hj.Build(context.Background(), NewSliceSource(storeProjBuildSchema, storeProjBuildRows(buildN))); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState != nil {
		t.Fatal("test setup: expected the flat (no-spill) build path")
	}
	if got, want := len(hj.buildSchema), 2; got != want {
		t.Fatalf("buildSchema has %d columns, want %d", got, want)
	}
	for i, bb := range hj.buildBatches {
		if bb != nil && len(bb.Columns) != 2 {
			t.Fatalf("buildBatches[%d] has %d columns, want 2", i, len(bb.Columns))
		}
	}

	ids := runStoreProjProbe(t, hj, probeN)
	if len(ids) != probeN/2 {
		t.Fatalf("semi join emitted %d rows, want %d", len(ids), probeN/2)
	}
}

// TestBuildStoreCols_UnresolvableNameDisablesProjection: a store column that
// doesn't resolve against the first arrival batch must disable projection
// entirely (full-width storage, pre-projection behavior) rather than break
// the build or the filter.
func TestBuildStoreCols_UnresolvableNameDisablesProjection(t *testing.T) {
	const buildN, probeN = 5000, 1000

	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.SemiAntiFilter = storeProjFilter
	hj.BuildStoreCols = []string{"id", "no_such_column"}

	if err := hj.Build(context.Background(), NewSliceSource(storeProjBuildSchema, storeProjBuildRows(buildN))); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if got, want := len(hj.buildSchema), len(storeProjBuildSchema); got != want {
		t.Fatalf("buildSchema has %d columns, want full width %d (projection should be disabled)", got, want)
	}

	ids := runStoreProjProbe(t, hj, probeN)
	if len(ids) != probeN/2 {
		t.Fatalf("semi join emitted %d rows, want %d", len(ids), probeN/2)
	}
}
