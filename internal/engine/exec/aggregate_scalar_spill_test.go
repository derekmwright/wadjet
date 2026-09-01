package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// scalarSpillBatch builds a batch whose STRING column is SHAPE-ONLY: it carries
// per-row lengths and no bytes, which is what the scan ships when the planner
// proves every use of the column is a shape use (COUNT(col), LENGTH(col),
// IS NULL). Every third row is NULL, so COUNT(col) and COUNT(*) differ.
func scalarSpillBatch(t *testing.T, n int) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, n)
	cur := uint32(0)
	for i := 0; i < n; i++ {
		cur += uint32(i%17 + 1)
		b.Columns[0].BytesData.Offsets[i+1] = cur
		if i%3 == 0 {
			b.Columns[0].Nulls.SetNull(i)
		} else {
			b.Columns[0].Nulls.SetValid(i)
		}
		b.Columns[1].Int64Data[i] = int64(i)
		b.Columns[1].Nulls.SetValid(i)
	}
	b.Columns[0].BytesData.ShapeOnly = true
	b.Len = n
	return b
}

// An ungrouped aggregate must answer under any memory budget (#779).
//
// Mechanism. A scalar aggregate has no GROUP BY, so canUseExternalMerge is
// false and the pressure branch took the legacy raw-row spill: b.ToRows(),
// which reads every column's VALUES. When one of those columns is shape-only
// — the scan decoded lengths and no bytes, which is exactly what it does for
// COUNT(col) — the read hits the shape-only guard and the query fails with
// "some consumer of this column is not a shape consumer". A correct query that
// answers only while it has memory to spare.
//
// The fix is not to teach the row buffer about shape-only columns: an
// ungrouped aggregate's state is one row of accumulators, so buffering its
// INPUT saves no memory at all — Finalize rebuilds the same state out of rows
// that were written to disk and read back first. It consumes directly instead.
//
// Reverting it fails this with the shape-only panic, converted to an error by
// the pipeline's boundary.
func TestScalarAggregateAnswersUnderPressureWithAShapeOnlyColumn(t *testing.T) {
	const n, batches = 500, 6
	mk := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate(nil, []AggColumn{
			{Func: AggCount, InputCol: "s", OutputCol: "n_s", OutputType: parquet.TypeInt64},
			{Func: AggCount, OutputCol: "n_all", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "v", OutputCol: "s_v", OutputType: parquet.TypeInt64},
		})
		h.Spill = sm
		return h
	}
	mkBatches := func() []*batch.RecordBatch {
		out := make([]*batch.RecordBatch, 0, batches)
		for i := 0; i < batches; i++ {
			out = append(out, scalarSpillBatch(t, n))
		}
		return out
	}

	// A tracker already over its threshold, so ShouldSpillFor answers true on
	// the FIRST batch — the pressure branch is taken every time, which is the
	// branch under test.
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)

	ref := runHashAggToMap(t, mk(nil), mkBatches())
	got := runHashAggToMap(t, mk(sm), mkBatches())

	if len(ref) != 1 || len(got) != 1 {
		t.Fatalf("rows: reference=%d pressured=%d, want 1 each", len(ref), len(got))
	}
	// Recomputed from the generator, so a run where both arms lost the same
	// rows still fails: 500 rows per batch, every third NULL.
	nulls := 0
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			nulls++
		}
	}
	want := map[string]int64{
		"n_s":   int64(batches * (n - nulls)),
		"n_all": int64(batches * n),
		"s_v":   int64(batches) * int64(n*(n-1)/2),
	}
	for col, exp := range want {
		if v := ref[0][col].(int64); v != exp {
			t.Errorf("reference %s = %d, want %d", col, v, exp)
		}
		if v := got[0][col].(int64); v != exp {
			t.Errorf("pressured %s = %d, want %d", col, v, exp)
		}
	}
}
