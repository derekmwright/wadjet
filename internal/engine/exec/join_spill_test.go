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

func fromRowsForTest(schema []parquet.Column, rows []map[string]any) *batch.RecordBatch {
	return batch.FromRows(schema, rows)
}
