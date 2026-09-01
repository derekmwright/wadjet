package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The forcing knob must actually put a drain on the batch it names, and the
// answer must not change because it did.
//
// Both halves matter. A knob that quietly fails to engage (no spill directory,
// a productivity gate that swallows the drain, a tracker with no budget) turns
// every gate armed with it into a test of the in-memory path — which passes,
// and proves nothing. And a knob that engages but changes the answer would be
// manufacturing its own failures.
func TestForceAggDrainEveryDrainsAndDoesNotChangeTheAnswer(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	mkBatches := func() []*batch.RecordBatch {
		var out []*batch.RecordBatch
		for bi := 0; bi < 5; bi++ {
			b := batch.NewRecordBatch(schema, 40)
			for ri := 0; ri < 40; ri++ {
				idx := bi*40 + ri
				b.Columns[0].Int64Data[ri] = int64(idx % 7)
				b.Columns[0].Nulls.SetValid(ri)
				b.Columns[1].Int64Data[ri] = int64(idx)
				b.Columns[1].Nulls.SetValid(ri)
			}
			b.Len = 40
			out = append(out, b)
		}
		return out
	}
	mk := func(sm *memory.SpillManager) *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
			{Func: AggCount, InputCol: "v", OutputCol: "n", OutputType: parquet.TypeInt64},
		})
		h.Spill = sm
		return h
	}

	ref := runHashAggToMap(t, mk(nil), mkBatches())

	// A budget far larger than the state: nothing here spills on its own, so a
	// drain that happens is one the knob forced.
	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	before := ForcedAggDrains.Load()
	restore := ForceAggDrainEvery(2)
	defer ForceAggDrainEvery(restore)
	h := mk(sm)
	got := runHashAggToMap(t, h, mkBatches())
	forced := ForcedAggDrains.Load() - before
	if forced == 0 {
		t.Fatal("ForceAggDrainEvery(2) forced no drain: the knob did not engage")
	}

	index := func(rows []map[string]any) map[int64][2]int64 {
		m := make(map[int64][2]int64, len(rows))
		for _, r := range rows {
			m[r["k"].(int64)] = [2]int64{r["s"].(int64), r["n"].(int64)}
		}
		return m
	}
	refIdx, gotIdx := index(ref), index(got)
	if len(refIdx) != len(gotIdx) {
		t.Fatalf("groups: reference=%d forced-drain=%d", len(refIdx), len(gotIdx))
	}
	for k, want := range refIdx {
		if have := gotIdx[k]; have != want {
			t.Errorf("key %d: forced-drain (sum,count)=%v, want %v", k, have, want)
		}
	}
}

// ForceAggDrainEvery(0) must leave the aggregate exactly as it found it, so a
// gate that disarms the knob is really testing the unforced path.
func TestForceAggDrainEveryDisarms(t *testing.T) {
	restore := ForceAggDrainEvery(0)
	defer ForceAggDrainEvery(restore)
	h := NewHashAggregate([]string{"k"}, []AggColumn{{Func: AggCount, OutputCol: "n", OutputType: parquet.TypeInt64}})
	if h.forcedDrainDue() {
		t.Error("a disarmed knob still reported a drain due")
	}
}
