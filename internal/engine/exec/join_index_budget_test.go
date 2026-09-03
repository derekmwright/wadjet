package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #823: the index the build pre-allocates from BuildRowHint is charged to the
// tracker through an unceilinged ForceReserve before the rows it is sized for
// have arrived. It must never be the charge that crosses the budget.

func arrivalBuildRows(n int, pad string) ([]parquet.Column, []map[string]any) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "k": int64(i), "pad": pad}
	}
	return schema, rows
}

// arrivalSource emits the given rows as batches of chunk rows each (chunk <= 0
// means one batch holding everything), which is the only difference between
// this gate's two arms.
func arrivalSource(schema []parquet.Column, rows []map[string]any, chunk int) Source {
	if chunk <= 0 || chunk >= len(rows) {
		return NewBatchSource([]*batch.RecordBatch{batch.FromRows(schema, rows)})
	}
	var batches []*batch.RecordBatch
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		batches = append(batches, batch.FromRows(schema, rows[start:end]))
	}
	return NewBatchSource(batches)
}

// #823: the pre-allocated index is charged before the rows exist, so it must be
// sized by the room that exists. BuildRowHint here describes a build 100× what
// the budget could hold; charging for it put 191,072 bytes on a 512 KiB ledger
// on a 20-row batch, and every later Reserve in the query was then measured
// against a floor describing a build that had not happened.
func TestHashIndexIsNotPreSizedPastTheBudgetsRoom(t *testing.T) {
	schema, rows := arrivalBuildRows(20, "p")
	const budget = 512 << 10
	tracker := memory.NewTracker("test", budget)
	// Everything except the join already holds most of the budget, exactly as a
	// scan's file load does on the shape this was measured on.
	tracker.ForceReserve(412074)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	hj := NewHashJoin(InnerJoin, []string{"k"}, []string{"k"})
	hj.Spill = sm
	hj.MemTracker = tracker
	hj.BuildRowHint = 200000 // 200k × 40 B = 8 MB of index for a 512 KiB budget

	if err := hj.Build(context.Background(), arrivalSource(schema, rows, 0)); err != nil {
		t.Fatalf("build: %v", err)
	}
	overhead := hj.hashTableOverhead()
	// The rule, asserted as the rule: unspillable index state may claim at most
	// the room that was FREE when the build started. Sized by the hint instead,
	// this is 8 MB of index against a 512 KiB budget.
	if room := int64(budget) - 412074; overhead > room {
		t.Fatalf("the build pre-sized %d bytes of hash index for 20 rows because BuildRowHint said "+
			"%d, with only %d bytes of the budget free — the index is unspillable and this charge "+
			"is what crosses the budget (#823); it must be bounded by the room that exists, not by "+
			"the estimate", overhead, hj.BuildRowHint, room)
	}
	if u := tracker.Used(); u > budget {
		t.Fatalf("tracker used=%d past a %d budget after building 20 rows: the pre-size is the "+
			"charge that crossed it", u, int64(budget))
	}
	t.Logf("index overhead for 20 rows with a %d-row hint: %d bytes; tracker used=%d of %d",
		hj.BuildRowHint, overhead, tracker.Used(), int64(budget))
}
