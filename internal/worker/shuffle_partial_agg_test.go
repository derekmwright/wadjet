package worker

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Regression tests for cappedPartialAgg (exchange partial aggregation):
// the shuffle sender pre-combines rows on the planner-proven keys with
// name-preserving SUM/MIN/MAX before partitioning. Consumers must be
// unable to tell partials from raw rows except by row count, so the
// tests assert merged values and name preservation, plus the epoch-flush
// behavior that bounds memory.

func partialAggSchema() []parquet.Column {
	return []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "q", Type: parquet.TypeFloat64},
	}
}

func drainAll(t *testing.T, p *cappedPartialAgg, batches ...*batch.RecordBatch) []map[string]any {
	t.Helper()
	ctx := context.Background()
	var out []map[string]any
	collect := func(bs []*batch.RecordBatch) {
		for _, b := range bs {
			out = append(out, b.ToRows()...)
		}
	}
	for _, b := range batches {
		flushed, err := p.consume(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		collect(flushed)
	}
	flushed, err := p.drain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	collect(flushed)
	return out
}

func sumByIntKey(rows []map[string]any, keyCol, valCol string) map[string]float64 {
	out := map[string]float64{}
	for _, r := range rows {
		k := fmt.Sprintf("%v", r[keyCol])
		if v, ok := r[valCol].(float64); ok {
			out[k] += v
		}
	}
	return out
}

func TestCappedPartialAgg_SumMergesAndPreservesNames(t *testing.T) {
	p := newCappedPartialAgg(
		[]string{"k"},
		[]distributed.AggSpec{{Func: "sum", InputCol: "q", OutputCol: "q"}},
		0,
	)
	schema := partialAggSchema()
	rows := drainAll(t, p,
		batch.FromRows(schema, []map[string]any{
			{"k": int64(1), "q": 2.0},
			{"k": int64(1), "q": 3.0},
			{"k": int64(2), "q": 10.0},
		}),
		batch.FromRows(schema, []map[string]any{
			{"k": int64(1), "q": 5.0},
			{"k": int64(3), "q": 7.0},
		}),
	)
	if len(rows) != 3 {
		t.Fatalf("got %d partial rows, want 3 groups: %v", len(rows), rows)
	}
	// Output columns must keep the raw payload names — consumers resolve
	// by name and must not see a renamed aggregate.
	for _, col := range []string{"k", "q"} {
		if _, ok := rows[0][col]; !ok {
			t.Fatalf("partial row missing column %q: %v", col, rows[0])
		}
	}
	sums := sumByIntKey(rows, "k", "q")
	want := map[string]float64{"1": 10, "2": 10, "3": 7}
	for k, w := range want {
		if sums[k] != w {
			t.Errorf("group %s sum = %v, want %v", k, sums[k], w)
		}
	}
	if p.inRows != 5 || p.outRows != 3 {
		t.Errorf("counters in=%d out=%d, want 5/3", p.inRows, p.outRows)
	}
}

func TestCappedPartialAgg_CapFlushesEpochsAndStaysMergeable(t *testing.T) {
	// A 1-byte cap forces a flush after every consume — the degenerate
	// worst case. Partials must still merge to the right totals downstream
	// (here: merged by the test harness, standing in for the consumer).
	p := newCappedPartialAgg(
		[]string{"k"},
		[]distributed.AggSpec{{Func: "sum", InputCol: "q", OutputCol: "q"}},
		1,
	)
	schema := partialAggSchema()
	var batches []*batch.RecordBatch
	for i := 0; i < 4; i++ {
		batches = append(batches, batch.FromRows(schema, []map[string]any{
			{"k": int64(1), "q": 1.0},
			{"k": int64(2), "q": 2.0},
		}))
	}
	rows := drainAll(t, p, batches...)
	if p.flushes < 4 {
		t.Errorf("flushes = %d, want >= 4 (cap must force per-batch epochs)", p.flushes)
	}
	// Multiple partial rows per group are expected — that's the degrade
	// path. They must still sum to the true totals.
	sums := sumByIntKey(rows, "k", "q")
	if sums["1"] != 4 || sums["2"] != 8 {
		t.Errorf("merged sums = %v, want map[1:4 2:8]", sums)
	}
}

func TestCappedPartialAgg_MinMax(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "lo", Type: parquet.TypeFloat64},
		{Name: "hi", Type: parquet.TypeFloat64},
	}
	p := newCappedPartialAgg(
		[]string{"k"},
		[]distributed.AggSpec{
			{Func: "min", InputCol: "lo", OutputCol: "lo"},
			{Func: "max", InputCol: "hi", OutputCol: "hi"},
		},
		0,
	)
	rows := drainAll(t, p,
		batch.FromRows(schema, []map[string]any{
			{"k": int64(1), "lo": 5.0, "hi": 5.0},
			{"k": int64(1), "lo": 2.0, "hi": 9.0},
			{"k": int64(1), "lo": 7.0, "hi": 1.0},
		}),
	)
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1: %v", len(rows), rows)
	}
	if rows[0]["lo"] != 2.0 || rows[0]["hi"] != 9.0 {
		t.Errorf("min/max = %v/%v, want 2/9", rows[0]["lo"], rows[0]["hi"])
	}
}

func TestCappedPartialAgg_SelectionVectorRespected(t *testing.T) {
	p := newCappedPartialAgg(
		[]string{"k"},
		[]distributed.AggSpec{{Func: "sum", InputCol: "q", OutputCol: "q"}},
		0,
	)
	b := batch.FromRows(partialAggSchema(), []map[string]any{
		{"k": int64(1), "q": 100.0}, // deselected below
		{"k": int64(1), "q": 3.0},
		{"k": int64(2), "q": 4.0},
	})
	b.Sel = []uint32{1, 2}
	rows := drainAll(t, p, b)
	sums := sumByIntKey(rows, "k", "q")
	if sums["1"] != 3 || sums["2"] != 4 {
		t.Errorf("sums = %v, want map[1:3 2:4] (deselected row must not count)", sums)
	}
}
