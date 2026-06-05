package exec

import (
	"context"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestGraceHashJoinSpill verifies that the Grace Hash Join spill-to-disk
// produces correct results. Sets a tiny memory budget so spill is triggered
// during the build phase.
func TestGraceHashJoinSpill(t *testing.T) {
	// Build side: 5000 rows (produces 3 batches at DefaultBatchSize=2048)
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	const buildN = 5000
	var rightRows []map[string]any
	for i := 0; i < buildN; i++ {
		rightRows = append(rightRows, map[string]any{
			"id":  int64(i),
			"val": "build-row",
		})
	}

	// Probe side: 500 rows that match some build rows
	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString},
	}
	const probeN = 500
	var leftRows []map[string]any
	for i := 0; i < probeN; i++ {
		leftRows = append(leftRows, map[string]any{
			"id":    int64(i * 2), // even IDs only, up to 998
			"label": "probe-row",
		})
	}

	tmpDir, err := os.MkdirTemp("", "grace-hash-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Budget allows ~1 batch before ShouldSpill triggers (80% threshold).
	// Each batch of 2048 rows ≈ 200KB. Budget of 250KB → triggers after 1 batch.
	tracker := memory.NewTracker("test", 250_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker

	buildSource := NewSliceSource(rightSchema, rightRows)
	if err := hj.Build(context.Background(), buildSource); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Verify spill was triggered
	if hj.spillState == nil {
		t.Fatal("expected spillState to be non-nil (spill should have triggered)")
	}
	if len(hj.spillState.spilledParts) == 0 {
		t.Fatal("expected at least one spilled partition")
	}
	t.Logf("spilled %d partitions, in-memory batches: %d",
		len(hj.spillState.spilledParts), len(hj.buildBatches))

	// Run probe pipeline
	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	// Inner join on even IDs: 500 probe rows × 1 match each = 500 rows
	if len(sink.Rows) != probeN {
		t.Fatalf("expected %d rows, got %d", probeN, len(sink.Rows))
	}

	// Verify all results have correct keys
	seen := make(map[int64]bool)
	for _, row := range sink.Rows {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("expected int64 id, got %T", row["id"])
		}
		if id%2 != 0 || id < 0 || id >= buildN {
			t.Fatalf("unexpected id: %d", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id: %d", id)
		}
		seen[id] = true
		if row["val"] != "build-row" {
			t.Fatalf("expected val='build-row', got %v", row["val"])
		}
		if row["label"] != "probe-row" {
			t.Fatalf("expected label='probe-row', got %v", row["label"])
		}
	}
}

// TestGraceHashJoinSpill_SemiJoin verifies spill works for semi joins.
func TestGraceHashJoinSpill_SemiJoin(t *testing.T) {
	const buildN = 5000
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	var rightRows []map[string]any
	for i := 0; i < buildN; i++ {
		rightRows = append(rightRows, map[string]any{"id": int64(i)})
	}

	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString},
	}
	var leftRows []map[string]any
	for i := 0; i < 300; i++ {
		leftRows = append(leftRows, map[string]any{
			"id":    int64(i * 3), // every 3rd ID
			"label": "probe",
		})
	}

	tmpDir, err := os.MkdirTemp("", "grace-semi-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tracker := memory.NewTracker("test", 200_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(SemiJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker

	buildSource := NewSliceSource(rightSchema, rightRows)
	if err := hj.Build(context.Background(), buildSource); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{Source: source, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Pipeline failed: %v", err)
	}

	// Semi join: probe rows where id exists in build (0,3,6,...,4999)
	expected := 0
	for i := 0; i < 300; i++ {
		if i*3 < buildN {
			expected++
		}
	}

	if len(sink.Rows) != expected {
		t.Fatalf("expected %d rows, got %d", expected, len(sink.Rows))
	}
}

// TestColumnarSpillRoundtrip tests that the columnar batch spill format
// correctly serializes and deserializes all supported types.
func TestColumnarSpillRoundtrip(t *testing.T) {
	schema := []parquet.Column{
		{Name: "i64", Type: parquet.TypeInt64},
		{Name: "i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f64", Type: parquet.TypeFloat64},
		{Name: "str", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeBool},
	}

	rows := []map[string]any{
		{"i64": int64(1), "i32": int32(10), "f64": 1.5, "str": "hello", "b": true},
		{"i64": int64(2), "i32": nil, "f64": 2.5, "str": nil, "b": false},
		{"i64": int64(3), "i32": int32(30), "f64": 3.5, "str": "world", "b": true},
	}

	tmpDir, err := os.MkdirTemp("", "spill-roundtrip-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create batch from rows
	original := fromRowsForTest(schema, rows)

	// Write to disk
	path, err := writeSpillBatches(tmpDir, []*batch.RecordBatch{original})
	if err != nil {
		t.Fatalf("writeSpillBatches: %v", err)
	}

	// Read back
	batches, err := readSpillBatches(path)
	if err != nil {
		t.Fatalf("readSpillBatches: %v", err)
	}

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	got := batches[0]
	if got.Len != original.Len {
		t.Fatalf("expected %d rows, got %d", original.Len, got.Len)
	}

	// Verify data matches
	for i := 0; i < got.Len; i++ {
		// int64
		if got.Columns[0].Int64Data[i] != original.Columns[0].Int64Data[i] {
			t.Errorf("row %d i64: got %d, want %d",
				i, got.Columns[0].Int64Data[i], original.Columns[0].Int64Data[i])
		}
		// float64
		if got.Columns[2].Float64Data[i] != original.Columns[2].Float64Data[i] {
			t.Errorf("row %d f64: got %f, want %f",
				i, got.Columns[2].Float64Data[i], original.Columns[2].Float64Data[i])
		}
		// bool
		if got.Columns[4].BoolData[i] != original.Columns[4].BoolData[i] {
			t.Errorf("row %d bool: got %v, want %v",
				i, got.Columns[4].BoolData[i], original.Columns[4].BoolData[i])
		}
	}

	// Check nulls round-tripped correctly
	if !got.Columns[1].Nulls.IsNullFast(1) {
		t.Error("expected i32 row 1 to be null")
	}
	if !got.Columns[3].Nulls.IsNullFast(1) {
		t.Error("expected str row 1 to be null")
	}
	if got.Columns[1].Nulls.IsNullFast(0) {
		t.Error("expected i32 row 0 to be non-null")
	}
}

// TestCompactBatchForRows_NullStrings verifies that compactBatchForRows
// correctly maintains BytesColumn offset continuity when null string values
// are present. Previously, skipping Set for null rows left zero-valued offsets
// that corrupted subsequent string values.
func TestCompactBatchForRows_NullStrings(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}

	rows := []map[string]any{
		{"id": int64(0), "name": "alice"},
		{"id": int64(1), "name": nil},
		{"id": int64(2), "name": "charlie"},
		{"id": int64(3), "name": nil},
		{"id": int64(4), "name": "eve"},
	}
	original := batch.FromRows(schema, rows)

	// Select rows 0,1,2,3,4 (all rows — exercises null gaps at indices 1,3)
	compact := compactBatchForRows(original, []int{0, 1, 2, 3, 4})
	if compact.Len != 5 {
		t.Fatalf("expected 5 rows, got %d", compact.Len)
	}

	// Verify non-null strings via offsets
	nameCol := compact.Columns[1]
	for i, want := range []string{"alice", "", "charlie", "", "eve"} {
		if i == 1 || i == 3 {
			if !nameCol.Nulls.IsNullFast(i) {
				t.Errorf("row %d: expected null", i)
			}
			continue
		}
		got := nameCol.BytesData.StringValue(i)
		if got != want {
			t.Errorf("row %d: got %q, want %q", i, got, want)
		}
	}

	// Also verify spill roundtrip with null strings
	tmpDir, err := os.MkdirTemp("", "compact-null-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	path, err := writeSpillBatches(tmpDir, []*batch.RecordBatch{compact})
	if err != nil {
		t.Fatalf("writeSpillBatches: %v", err)
	}
	batches, err := readSpillBatches(path)
	if err != nil {
		t.Fatalf("readSpillBatches: %v", err)
	}
	if len(batches) != 1 || batches[0].Len != 5 {
		t.Fatalf("unexpected batch count/size")
	}

	reloaded := batches[0]
	for i, want := range []string{"alice", "", "charlie", "", "eve"} {
		if i == 1 || i == 3 {
			if !reloaded.Columns[1].Nulls.IsNullFast(i) {
				t.Errorf("reloaded row %d: expected null", i)
			}
			continue
		}
		got := reloaded.Columns[1].BytesData.StringValue(i)
		if got != want {
			t.Errorf("reloaded row %d: got %q, want %q", i, got, want)
		}
	}
}

// TestGraceHashJoinSpill_Parallel verifies Grace Hash Join spill correctness
// under parallel pipeline execution. Previously, multiple workers shared the
// same spillBatchWriter without synchronization, corrupting the binary stream.
func TestGraceHashJoinSpill_Parallel(t *testing.T) {
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}
	const buildN = 5000
	var rightRows []map[string]any
	for i := 0; i < buildN; i++ {
		rightRows = append(rightRows, map[string]any{
			"id":  int64(i),
			"val": "build-row",
		})
	}

	leftSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "label", Type: parquet.TypeString},
	}
	const probeN = 500
	var leftRows []map[string]any
	for i := 0; i < probeN; i++ {
		leftRows = append(leftRows, map[string]any{
			"id":    int64(i * 2),
			"label": "probe-row",
		})
	}

	tmpDir, err := os.MkdirTemp("", "grace-parallel-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	tracker := memory.NewTracker("test", 250_000)
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker

	buildSource := NewSliceSource(rightSchema, rightRows)
	if err := hj.Build(context.Background(), buildSource); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
		t.Fatal("expected spill to trigger")
	}

	source := NewSliceSource(leftSchema, leftRows)
	probe := hj.Probe()
	sink := &CollectSink{}
	pipe := &Pipeline{
		Source:  source,
		Ops:    []UnaryOperator{probe},
		Sink:   sink,
		Workers: 4, // Force parallel — exercises concurrent probe spill writes
	}
	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Parallel pipeline failed: %v", err)
	}

	if len(sink.Rows) != probeN {
		t.Fatalf("expected %d rows, got %d", probeN, len(sink.Rows))
	}

	seen := make(map[int64]bool)
	for _, row := range sink.Rows {
		id := row["id"].(int64)
		if seen[id] {
			t.Fatalf("duplicate id: %d", id)
		}
		seen[id] = true
	}
}

// TestConcurrentBuildSharedTracker verifies that two hash joins sharing a single
// MemTracker keep their accounting balanced (tracker.Used == sum of per-join
// trackedMem) when one spills under concurrent build. The original regression
// was the legacy flat path's spillBuildBatches() calling tracker.Reset(), which
// zeroed ALL concurrent builds' tracked memory → unchecked allocation and OOM in
// multi-way joins (Q21). That path is retired; spill-eligible builds now use
// partition-on-arrival (buildPartitioned), whose spillOneInMemoryPartition only
// Releases its own partition bytes and never touches the shared tracker's total,
// so the invariant holds structurally. This test now guards that invariant under
// buildPartitioned's cooperative cross-operator spill.
func TestConcurrentBuildSharedTracker(t *testing.T) {
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

	tmpDir, err := os.MkdirTemp("", "concurrent-build-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Shared budget that forces at least one build to spill while leaving
	// headroom for buildPartitioned's residual hash-table overhead, which (for
	// partition-on-arrival) stays in the tracker for spilled partitions until
	// Close rather than being dropped by a flat-path arena rebuild.
	//
	// 2.0MB sits deliberately between the concurrent-startup transient and the
	// steady-state footprint of two 10K-row builds (~2.4MB+ combined, observed
	// final tracker.Used). It is high enough that the first reservation that
	// crosses budget happens only once BOTH joins already hold abundant
	// spillable partitions (~0.9MB each) — so self- or cooperative-spill always
	// frees room — yet low enough that the combined footprint still forces a
	// spill. The earlier 1.6MB minimum tripped a rare startup race: a join could
	// need to reserve while it had nothing of its own to evict and its peer
	// wasn't yet a reachable cooperative-spill target, hard-failing on a ~KB gap
	// (flaked TestConcurrentBuildSharedTracker in CI; see spillUntilCanReserve).
	budget := int64(2_000_000)
	sharedTracker := memory.NewTracker("shared", budget)
	sm, err := memory.NewSpillManager(tmpDir, sharedTracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hjA := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hjA.Spill = sm
	hjA.MemTracker = sharedTracker

	hjB := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hjB.Spill = sm
	hjB.MemTracker = sharedTracker

	// Build both concurrently — this is the real-world scenario in multi-way
	// joins where buildJoin() launches goroutines for each join level.
	ctx := context.Background()
	var errA, errB error
	done := make(chan struct{})
	go func() {
		defer close(done)
		errA = hjA.Build(ctx, NewSliceSource(schema, makeRows(0)))
	}()
	errB = hjB.Build(ctx, NewSliceSource(schema, makeRows(buildN)))
	<-done

	if errA != nil {
		t.Fatalf("Join A Build failed: %v", errA)
	}
	if errB != nil {
		t.Fatalf("Join B Build failed: %v", errB)
	}

	usedFinal := sharedTracker.Used()
	trackedA := hjA.TrackedMem()
	trackedB := hjB.TrackedMem()
	t.Logf("final: tracker.Used=%d, joinA.trackedMem=%d, joinB.trackedMem=%d",
		usedFinal, trackedA, trackedB)

	// KEY INVARIANT: tracker.Used() must equal the sum of both joins' tracked memory.
	// Before the fix, spillBuildBatches() called Reset() which zeroed the tracker,
	// then ForceReserve'd only the spilling join's in-memory amount — erasing the
	// other join's accounting. This caused unchecked allocation and OOM.
	sumTracked := trackedA + trackedB
	if usedFinal != sumTracked {
		t.Errorf("tracker.Used()=%d != joinA.trackedMem(%d) + joinB.trackedMem(%d) = %d",
			usedFinal, trackedA, trackedB, sumTracked)
	}

	// Both joins should have positive tracked memory (both ingested data)
	if trackedA <= 0 {
		t.Error("expected join A to have positive trackedMem")
	}
	if trackedB <= 0 {
		t.Error("expected join B to have positive trackedMem")
	}

	// At least one should have spilled under this budget
	spilledA := hjA.SpillState() != nil
	spilledB := hjB.SpillState() != nil
	if !spilledA && !spilledB {
		t.Error("expected at least one join to spill under budget")
	}
	t.Logf("spilled: A=%v B=%v", spilledA, spilledB)
}

// TestConcurrentSpillFileNaming verifies that concurrent calls to
// writeSpillBatches produce unique file paths. Before the fix, a TOCTOU
// race in the os.Stat-based naming allowed two goroutines to claim the
// same filename, causing "file not found" when one join's cleanup deleted
// the other's data.
func TestConcurrentSpillFileNaming(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "spill-naming-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	b := fromRowsForTest(schema, []map[string]any{{"id": int64(1)}})

	const goroutines = 20
	paths := make([]string, goroutines)
	errs := make([]error, goroutines)
	done := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer func() { done <- struct{}{} }()
			paths[idx], errs[idx] = writeSpillBatches(tmpDir, []*batch.RecordBatch{b})
		}(i)
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	// All paths must be unique
	seen := make(map[string]int)
	for i, p := range paths {
		if prev, ok := seen[p]; ok {
			t.Errorf("duplicate path: goroutine %d and %d both got %s", prev, i, p)
		}
		seen[p] = i
	}

	// All files must be readable
	for i, p := range paths {
		batches, err := readSpillBatches(p)
		if err != nil {
			t.Errorf("goroutine %d file unreadable: %v", i, err)
		}
		if len(batches) != 1 || batches[0].Len != 1 {
			t.Errorf("goroutine %d: expected 1 batch with 1 row", i)
		}
	}
}

func fromRowsForTest(schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	return batch.FromRows(schema, rows)
}
