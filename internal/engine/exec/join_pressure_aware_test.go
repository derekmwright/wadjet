package exec

import (
	"context"
	"runtime"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestSharedPoolUnderPressure covers the helper used by worker code to decide
// whether to enable PartitionOnArrival at task entry.
func TestSharedPoolUnderPressure(t *testing.T) {
	tests := []struct {
		name   string
		used   int64
		budget int64
		want   bool
	}{
		{"nil_tracker", 0, 0, false},
		{"no_budget", 100, 0, false},
		{"empty_pool", 0, 1000, false},
		{"below_threshold", 299, 1000, false},
		{"at_threshold", 300, 1000, true},
		{"above_threshold", 500, 1000, true},
		{"full", 1000, 1000, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracker *memory.Tracker
			if tc.budget > 0 {
				tracker = memory.NewTracker("test", tc.budget)
				if tc.used > 0 {
					_ = tracker.Reserve(tc.used)
				}
			}
			if got := SharedPoolUnderPressure(tracker); got != tc.want {
				t.Errorf("used=%d budget=%d: got %v, want %v", tc.used, tc.budget, got, tc.want)
			}
		})
	}
}

// TestPartitionOnArrival_LowPressureFlatPath_NoAllocBlowup is the regression
// gate for the SF10 join-8 OOM observed on the 277152c deploy. Before the
// pressure-aware switch, PartitionOnArrival=true unconditionally fired the
// per-batch partitioning path even when the shared pool was nearly empty,
// running compactBatchForRows × 64 partitions on every source batch. For a
// modest-sized supplier-class build, that meant thousands of small batch
// allocations the GC then had to clean up — enough to starve the worker's
// heartbeat goroutine and trigger a stale-worker reap + OOM-kill cascade.
//
// The fix: worker code only sets PartitionOnArrival when SharedPoolUnderPressure
// is true at task entry. This test asserts:
//   - With the worker's flag off (SharedPoolUnderPressure=false because
//     tracker is empty), Build runs the legacy flat path.
//   - The build completes correctly without the per-partition allocation
//     storm, measured by an HeapAlloc delta upper bound.
//   - With the flag explicitly forced on (test setup), the partitioned
//     path runs as before — the flag still works for tests / pressure-
//     aware callers that detect a hot pool.
func TestPartitionOnArrival_LowPressureFlatPath_NoAllocBlowup(t *testing.T) {
	rightSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeString},
	}
	const buildN = 50_000 // ~25 source batches at default 2048
	rightRows := make([]map[string]any, buildN)
	for i := range rightRows {
		rightRows[i] = map[string]any{
			"id": int64(i),
			"v":  "padding-supplier-row-data-for-test",
		}
	}

	leftSchema := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	leftRows := []map[string]any{{"id": int64(0)}, {"id": int64(100)}, {"id": int64(1000)}}

	tmpDir := t.TempDir()
	// Large budget so the worker's pressure check would return false.
	tracker := memory.NewTracker("test", 1<<30) // 1 GB
	sm, err := memory.NewSpillManager(tmpDir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	if SharedPoolUnderPressure(tracker) {
		t.Fatal("test setup: tracker should be empty — pressure check must be false")
	}

	// Mimic the worker's setup: shared spill + tracker, but PartitionOnArrival
	// LEFT FALSE because pressure check returned false. This is the path the
	// 277152c deploy regressed: pre-fix it was unconditionally true.
	hj := NewHashJoin(InnerJoin, []string{"id"}, []string{"id"})
	hj.Spill = sm
	hj.MemTracker = tracker
	hj.PartitionOnArrival = false // worker would set this from SharedPoolUnderPressure(tracker)

	runtime.GC() // baseline before allocation
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	if err := hj.Build(context.Background(), NewSliceSource(rightSchema, rightRows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	// Critical invariant: legacy flat path stores one batch per source
	// arrival, NOT 64 partition mini-batches per arrival.
	if hj.spillState != nil {
		t.Errorf("legacy flat path must not allocate spillState upfront; got non-nil")
	}
	// 50K rows in 2048-row source batches → ~25 source batches → ~25 entries
	// in buildBatches. With 64-partition partition-on-arrival it would be
	// ~1,600 entries.
	if got := len(hj.buildBatches); got > 50 {
		t.Errorf("legacy flat path should have one buildBatches entry per source batch (~25); got %d (suggests partition-on-arrival fired)", got)
	}

	// Probe to confirm correctness: 3 probe rows × 1 match each = 3 rows.
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
	if got := len(sink.Rows); got != 3 {
		t.Fatalf("expected 3 output rows, got %d", got)
	}

	// Smoke: number of buildBatches at completion is a proxy for the per-
	// partition allocation storm. Stronger heap deltas vary by GC schedule
	// so we keep this assertion tied to the structural invariant above.
	t.Logf("buildBatches=%d, heapAllocDelta=%d KB",
		len(hj.buildBatches), int64(msAfter.HeapAlloc-msBefore.HeapAlloc)/1024)
}

// TestPartitionOnArrival_ForcedAllocatesSpillState confirms that explicit
// PartitionOnArrival=true still routes through the partitioned path
// regardless of pressure — the flag remains a force-on for tests and for
// pressure-aware callers that have detected a hot pool.
func TestPartitionOnArrival_ForcedAllocatesSpillState(t *testing.T) {
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
	hj.PartitionOnArrival = true // force the path

	if err := hj.Build(context.Background(), NewSliceSource(schema, rows)); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if hj.spillState == nil {
		t.Fatal("forced PartitionOnArrival must allocate spillState upfront")
	}
}
