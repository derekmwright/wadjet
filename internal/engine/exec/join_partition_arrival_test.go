package exec

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
	// Budget that exercises cross-operator cooperative spill: each concurrent
	// build ends at ~1.23 MB tracked (≈2.46 MB combined), so a shared budget
	// below the combined footprint forces at least one join to give up
	// partition data mid-build so the other can make Reserve progress. Without
	// the cooperative-spill advisor (B), this deadlocks because each join only
	// spills its own partitions and one mid-build join holding the pool starves
	// the other.
	//
	// The budget MUST clear a specific transient: once one join finishes its
	// build, its full tracked memory (~1.23 MB) stays pinned until Close (not
	// Build completion) and is NOT reclaimable by cooperative spill (a finished
	// peer won't spill). The still-building join must then fit its next batch
	// reservation (~0.15 MB) on top of that pinned residual via the strict
	// path. So the floor is ~1.23 MB + 0.15 MB ≈ 1.4 MB. The old 1.6 MB left
	// only ~0.15 MB of slack there, which CI's slower scheduling intermittently
	// tripped ("memory budget exceeded", #155). 2.0 MB keeps the budget below
	// the ~2.46 MB combined footprint (spill still forced) while giving ~0.6 MB
	// of headroom over that transient.
	sharedTracker := memory.NewTracker("shared", 2_000_000)
	sm, err := memory.NewSpillManager(tmpDir, sharedTracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	makeJoin := func() *HashJoin {
		hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
		hj.Spill = sm
		hj.MemTracker = sharedTracker
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

// TestPartitionOnArrival_MultiBatchAccumulatorFill is the buildRef-integrity
// regression for the reusable per-partition accumulator. It feeds the build
// side as MANY tiny arrival batches, so every partition's accumulator spans
// multiple arrival batches (appends at dstStart > 0) and fills/freezes more
// than once (buildN > accumFlushRows). Each build row carries a UNIQUE payload,
// so any error in the accumulator's row offset — a wrong appendRows dstStart, a
// wrong frozen Len, or a Reset/overwrite of a still-live accumulator — would
// return the wrong payload for a probed key and fail the per-key assertion
// below. Duplicate keys spread across distinct arrival batches exercise match
// chains that cross accumulator-freeze boundaries. The spill subcase forces
// partially-filled accumulators to be frozen and evicted mid-build.
func TestPartitionOnArrival_MultiBatchAccumulatorFill(t *testing.T) {
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	const (
		buildN       = 6000 // > accumFlushRows (2048): accumulators freeze and refill
		dupKeys      = 200   // ids 0..dupKeys-1 appear twice with distinct payloads
		arrivalBatch = 37    // tiny: forces many partial per-partition appends
	)

	rightRows := make([]map[string]any, 0, buildN+dupKeys)
	for i := 0; i < buildN; i++ {
		rightRows = append(rightRows, map[string]any{
			"id":  int64(i),
			"val": fmt.Sprintf("val-%d", i),
		})
	}
	// Duplicate keys 0..dupKeys-1 with a distinct payload, appended last so they
	// land in much later arrival batches than the originals — the resulting
	// 2-entry match chains straddle accumulator freezes.
	for i := 0; i < dupKeys; i++ {
		rightRows = append(rightRows, map[string]any{
			"id":  int64(i),
			"val": fmt.Sprintf("dup-%d", i),
		})
	}

	makeBatches := func() []*batch.RecordBatch {
		var out []*batch.RecordBatch
		for i := 0; i < len(rightRows); i += arrivalBatch {
			end := i + arrivalBatch
			if end > len(rightRows) {
				end = len(rightRows)
			}
			out = append(out, batch.FromRows(rightSchema, rightRows[i:end]))
		}
		return out
	}

	leftSchema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	leftRows := make([]map[string]any, buildN)
	for i := 0; i < buildN; i++ {
		leftRows[i] = map[string]any{"id": int64(i)}
	}

	cases := []struct {
		name      string
		budget    int64
		wantSpill bool
	}{
		{"no-spill", 64 << 20, false},
		{"spill", 150_000, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tracker := memory.NewTracker("test", tc.budget)
			sm, err := memory.NewSpillManager(tmpDir, tracker)
			if err != nil {
				t.Fatal(err)
			}
			defer sm.Cleanup()

			hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
			hj.Spill = sm
			hj.MemTracker = tracker

			if err := hj.Build(context.Background(), NewBatchSource(makeBatches())); err != nil {
				t.Fatalf("Build failed: %v", err)
			}
			if hj.spillState == nil {
				t.Fatal("partition-on-arrival must allocate spillState")
			}
			spilled := len(hj.spillState.spilledParts)
			if tc.wantSpill && spilled == 0 {
				t.Fatalf("expected at least one spilled partition at budget %d", tc.budget)
			}
			if !tc.wantSpill && spilled != 0 {
				t.Fatalf("did not expect spill at budget %d, got %d spilled", tc.budget, spilled)
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

			// Gather build payloads per probed key. id i<dupKeys matches two
			// build rows (val-i and dup-i); id i>=dupKeys matches exactly one.
			got := make(map[int64][]string, buildN)
			for _, row := range sink.Rows {
				id, ok := row["id"].(int64)
				if !ok {
					t.Fatalf("expected int64 id, got %T", row["id"])
				}
				val, ok := row["val"].(string)
				if !ok {
					t.Fatalf("expected string val for id %d, got %T", id, row["val"])
				}
				got[id] = append(got[id], val)
			}

			totalExpected := 0
			for i := 0; i < buildN; i++ {
				want := map[string]bool{fmt.Sprintf("val-%d", i): true}
				if i < dupKeys {
					want[fmt.Sprintf("dup-%d", i)] = true
				}
				totalExpected += len(want)

				gotVals := got[int64(i)]
				if len(gotVals) != len(want) {
					t.Fatalf("id %d: got %d matches %v, want %d", i, len(gotVals), gotVals, len(want))
				}
				for _, g := range gotVals {
					if !want[g] {
						t.Fatalf("id %d: wrong payload %q (a corrupt accumulator offset would surface here); got=%v",
							i, g, gotVals)
					}
				}
			}
			if len(sink.Rows) != totalExpected {
				t.Fatalf("expected %d output rows, got %d", totalExpected, len(sink.Rows))
			}
		})
	}
}

