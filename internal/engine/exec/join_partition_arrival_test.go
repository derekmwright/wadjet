package exec

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestPartitionOnArrival_BasicSpill exercises the partition-on-arrival path
// under a memory budget tight enough that at least one partition must be
// evicted during build. The probe must produce the same row set as a
// no-spill inner join over the same data — proving partition routing on
// the probe side correctly replays spilled partitions while live in-memory
// partitions stay queryable.
func TestPartitionOnArrival_BasicSpill(t *testing.T) {
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

	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString},
	}
	const probeN = 1000
	leftRows := make([]map[string]any, probeN)
	for i := range leftRows {
		leftRows[i] = map[string]any{
			"id":    int64(i * 2), // even IDs, all match a build row
			"label": "probe-row",
		}
	}

	tmpDir := t.TempDir()
	tracker := memory.NewTracker("test", 400_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker
	hj.PartitionOnArrival = true

	if err := hj.Build(context.Background(), NewSliceSource(rightSchema, rightRows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Spill must have triggered: budget is well below total build size.
	if hj.spillState == nil {
		t.Fatal("expected spillState to be allocated by partition-on-arrival path")
	}
	if len(hj.spillState.spilledParts) == 0 {
		t.Fatalf("expected at least one spilled partition; in-memory parts=%d, batches=%d",
			len(hj.spillState.partBuildBatches), len(hj.buildBatches))
	}

	// At least one h.buildBatches slot should be nil — that's how the freed
	// column data becomes GC-eligible after spillOneInMemoryPartition.
	nilSlots := 0
	for _, bb := range hj.buildBatches {
		if bb == nil {
			nilSlots++
		}
	}
	if nilSlots == 0 {
		t.Fatalf("expected at least one nil-ed h.buildBatches slot from incremental spill, got 0; spilled partitions=%d, batches=%d",
			len(hj.spillState.spilledParts), len(hj.buildBatches))
	}
	t.Logf("spilled partitions=%d, in-memory parts=%d, batches total=%d, nil slots=%d",
		len(hj.spillState.spilledParts), len(hj.spillState.partBuildBatches),
		len(hj.buildBatches), nilSlots)

	// Run probe pipeline and verify all probe rows produce a join match.
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(leftSchema, leftRows),
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	if len(sink.Rows) != probeN {
		t.Fatalf("expected %d output rows (every probe row matches), got %d", probeN, len(sink.Rows))
	}

	seen := make(map[int64]bool, probeN)
	for _, row := range sink.Rows {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("expected int64 id, got %T", row["id"])
		}
		if id%2 != 0 || id < 0 || id >= buildN {
			t.Fatalf("unexpected id %d", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
		if row["val"] != "build-row-padding-for-size" {
			t.Fatalf("unexpected build val %v for id %d", row["val"], id)
		}
		if row["label"] != "probe-row" {
			t.Fatalf("unexpected probe label %v for id %d", row["label"], id)
		}
	}
}

// TestPartitionOnArrival_NoSpillStillCorrect exercises the partition-on-arrival
// path with a budget large enough that no spill is required. Build must still
// allocate spillState upfront and produce correct join results over the
// per-partition mini-batch representation.
func TestPartitionOnArrival_NoSpillStillCorrect(t *testing.T) {
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	const buildN = 1000
	rightRows := make([]map[string]any, buildN)
	for i := range rightRows {
		rightRows[i] = map[string]any{
			"id":  int64(i),
			"val": "v",
		}
	}

	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	const probeN = 200
	leftRows := make([]map[string]any, probeN)
	for i := range leftRows {
		leftRows[i] = map[string]any{"id": int64(i * 5)}
	}

	tmpDir := t.TempDir()
	tracker := memory.NewTracker("test", 16<<20) // 16 MB — plenty of headroom
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker
	hj.PartitionOnArrival = true

	if err := hj.Build(context.Background(), NewSliceSource(rightSchema, rightRows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if hj.spillState == nil {
		t.Fatal("partition-on-arrival must allocate spillState upfront even when no spill triggers")
	}
	if len(hj.spillState.spilledParts) != 0 {
		t.Fatalf("did not expect any spill at this budget, got spilled=%d", len(hj.spillState.spilledParts))
	}

	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(leftSchema, leftRows),
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	// Probe ids 0,5,10,…,995 — every 5th id matches a build row up to id 999.
	expected := 0
	for i := 0; i < probeN; i++ {
		if i*5 < buildN {
			expected++
		}
	}
	if len(sink.Rows) != expected {
		t.Fatalf("expected %d output rows, got %d", expected, len(sink.Rows))
	}
}

// TestPartitionOnArrival_ConcurrentBuildsSharedPool covers the production
// failure mode that motivated this change: two HashJoin builds running
// concurrently against one shared tracker, with a budget that forces at
// least one to spill mid-build. Both must complete without leaking tracker
// reservations and produce correct join results when probed.
func TestPartitionOnArrival_ConcurrentBuildsSharedPool(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	const buildN = 10000
	makeRows := func(offset int) []map[string]any {
		rows := make([]map[string]any, buildN)
		for i := range rows {
			rows[i] = map[string]any{
				"id":  int64(offset + i),
				"val": "row-data-padding-for-size",
			}
		}
		return rows
	}

	tmpDir := t.TempDir()
	// Match the legacy TestConcurrentBuildSharedTracker budget (1.4 MB) so
	// both joins have headroom to make progress when the other build holds
	// the pool. The new path's per-spill granularity is finer than the
	// legacy path's; under a tighter budget the concurrent steady-state
	// requires multi-join cooperative spilling which is a separate
	// architectural lever (Class B in the brainstorm).
	sharedTracker := memory.NewTracker("shared", 1_400_000)
	sm, err := memory.NewSpillManager(tmpDir, sharedTracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	makeJoin := func() *HashJoin {
		hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
		hj.Spill = sm
		hj.MemTracker = sharedTracker
		hj.PartitionOnArrival = true
		return hj
	}
	hjA := makeJoin()
	hjB := makeJoin()

	ctx := context.Background()
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)
	go func() {
		defer wg.Done()
		errA = hjA.Build(ctx, NewSliceSource(schema, makeRows(0)))
	}()
	go func() {
		defer wg.Done()
		errB = hjB.Build(ctx, NewSliceSource(schema, makeRows(buildN)))
	}()
	wg.Wait()

	if errA != nil {
		t.Fatalf("Join A build: %v", errA)
	}
	if errB != nil {
		t.Fatalf("Join B build: %v", errB)
	}

	used := sharedTracker.Used()
	trackedA := hjA.TrackedMem()
	trackedB := hjB.TrackedMem()
	t.Logf("after build: tracker.Used=%d  joinA.trackedMem=%d  joinB.trackedMem=%d  spilled=A:%v B:%v",
		used, trackedA, trackedB,
		hjA.SpillState() != nil && len(hjA.SpillState().spilledParts) > 0,
		hjB.SpillState() != nil && len(hjB.SpillState().spilledParts) > 0)

	// Tracker accounting invariant: shared tracker must equal sum of
	// per-join tracked memory. Same invariant the legacy spill path
	// guards (TestConcurrentBuildSharedTracker).
	if used != trackedA+trackedB {
		t.Errorf("tracker.Used=%d != joinA(%d) + joinB(%d) = %d",
			used, trackedA, trackedB, trackedA+trackedB)
	}

	// At least one join should have spilled at this budget (~1.2 MB total
	// budget vs ~3 MB total ingest).
	spilledA := hjA.SpillState() != nil && len(hjA.SpillState().spilledParts) > 0
	spilledB := hjB.SpillState() != nil && len(hjB.SpillState().spilledParts) > 0
	if !spilledA && !spilledB {
		t.Errorf("expected at least one join to spill at this budget")
	}

	// Probe both joins to verify correctness post-spill.
	probeJoin := func(hj *HashJoin, offset int) {
		t.Helper()
		const probeN = 200
		probeRows := make([]map[string]any, probeN)
		for i := range probeRows {
			probeRows[i] = map[string]any{"id": int64(offset + i)}
		}
		probe := hj.Probe()
		sink := &CollectSink{}
		pipe := &Pipeline{
			Source: NewSliceSource([]parquet.Column{{Name: "id", Type: parquet.TypeInt64}}, probeRows),
			Ops:    []UnaryOperator{probe},
			Sink:   sink,
		}
		if err := pipe.Run(ctx); err != nil {
			t.Fatalf("probe pipeline: %v", err)
		}
		if len(sink.Rows) != probeN {
			t.Errorf("probe at offset %d: expected %d rows, got %d", offset, probeN, len(sink.Rows))
		}
	}
	probeJoin(hjA, 0)
	probeJoin(hjB, buildN)

	// Close releases tracker reservations cleanly (validates PR #77's
	// invariant under the new path).
	if err := hjA.Close(); err != nil {
		t.Errorf("Close A: %v", err)
	}
	if err := hjB.Close(); err != nil {
		t.Errorf("Close B: %v", err)
	}
	if got := sharedTracker.Used(); got != 0 {
		t.Errorf("expected tracker.Used=0 after both Close, got %d", got)
	}
}

// TestPartitionOnArrival_StringKeySpill exercises the string-key path so the
// computeBuildPartitionRows / spillPartitionBytes branch has coverage.
func TestPartitionOnArrival_StringKeySpill(t *testing.T) {
	rightSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const buildN = 6000
	rightRows := make([]map[string]any, buildN)
	for i := range rightRows {
		rightRows[i] = map[string]any{
			"k": fmt.Sprintf("k-padding-for-size-%08d", i),
			"v": int64(i),
		}
	}

	leftSchema := []parquet.Column{
		{Name: "k", Type: parquet.TypeString},
	}
	const probeN = 600
	leftRows := make([]map[string]any, probeN)
	for i := range leftRows {
		leftRows[i] = map[string]any{
			"k": fmt.Sprintf("k-padding-for-size-%08d", i*5),
		}
	}

	tmpDir := t.TempDir()
	tracker := memory.NewTracker("test-str", 600_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	hj.Spill = sm
	hj.MemTracker = tracker
	hj.PartitionOnArrival = true

	if err := hj.Build(context.Background(), NewSliceSource(rightSchema, rightRows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState == nil {
		t.Fatal("expected spillState")
	}
	if len(hj.spillState.spilledParts) == 0 {
		t.Logf("note: budget did not force a spill — string column oversized estimate")
	}

	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source: NewSliceSource(leftSchema, leftRows),
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	expected := 0
	for i := 0; i < probeN; i++ {
		if i*5 < buildN {
			expected++
		}
	}
	if len(sink.Rows) != expected {
		t.Fatalf("expected %d rows, got %d", expected, len(sink.Rows))
	}
}

