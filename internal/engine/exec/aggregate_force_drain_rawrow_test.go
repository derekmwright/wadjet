// SPDX-License-Identifier: MIT

package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A forced drain that lands on the legacy raw-row branch (an extra-state
// aggregate such as COUNT(DISTINCT), or any grouping-set aggregate) is counted
// on ForcedRawRowDrains and NOT on ForcedAggDrains. ForcedAggDrains counts the
// external-merge drains only, as it did before #1700: gates in the coordinator
// read "it did not move" as "this shape is outside the external-merge class",
// so counting raw-row drains there would move a class boundary.
func TestForcedRawRowDrainIsNotAnExternalMergeDrain(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	var batches []*batch.RecordBatch
	for bi := 0; bi < 3; bi++ {
		b := batch.NewRecordBatch(schema, 20)
		for ri := 0; ri < 20; ri++ {
			b.Columns[0].Int64Data[ri] = int64(ri % 4)
			b.Columns[0].Nulls.SetValid(ri)
			b.Columns[1].Int64Data[ri] = int64(bi*20 + ri)
			b.Columns[1].Nulls.SetValid(ri)
		}
		b.Len = 20
		batches = append(batches, b)
	}
	tracker := memory.NewTracker("test", 1<<30)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHashAggregate([]string{"k"}, []AggColumn{
		{Func: AggCountDistinct, InputCol: "v", OutputCol: "n", OutputType: parquet.TypeInt64},
	})
	h.Spill = sm

	aggBefore, rawBefore := ForcedAggDrains.Load(), ForcedRawRowDrains.Load()
	defer ForceAggDrainEvery(ForceAggDrainEvery(1))
	rows := runHashAggToMap(t, h, batches)

	if d := ForcedAggDrains.Load() - aggBefore; d != 0 {
		t.Errorf("ForcedAggDrains moved by %d on a raw-row aggregate; want 0 (it counts external-merge drains only)", d)
	}
	if d := ForcedRawRowDrains.Load() - rawBefore; d != 3 {
		t.Errorf("ForcedRawRowDrains moved by %d over 3 forced batches; want 3", d)
	}
	if len(rows) != 4 {
		t.Fatalf("groups: got %d, want 4", len(rows))
	}
	for _, r := range rows {
		if n := r["n"].(int64); n != 15 {
			t.Errorf("k=%v: count(distinct v) = %d, want 15", r["k"], n)
		}
	}
}
