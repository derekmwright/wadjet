package exec

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// DISTINCT is planned as a keys-only HashAggregate with GroupByAll (sweep
// finding #11): the dedicated Distinct operator's seen-set grew without
// bound or tracking. These tests cover the GroupByAll mode: dedup semantics
// (the old operator's contract), schema pass-through, and spill.

func drainAgg(tb testing.TB, h *HashAggregate) []map[string]any {
	tb.Helper()
	if err := h.Finalize(context.Background()); err != nil {
		tb.Fatal(err)
	}
	var rows []map[string]any
	for {
		b, err := h.Next(context.Background())
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			return rows
		}
		rows = append(rows, b.ToRows()...)
	}
}

func TestGroupByAll_Dedup(t *testing.T) {
	schema := []parquet.Column{
		{Name: "city", Type: parquet.TypeString},
		{Name: "state", Type: parquet.TypeString},
	}
	h := NewHashAggregate(nil, nil)
	h.GroupByAll = true
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(context.Background(), batch.FromRows(schema, []map[string]any{
		{"city": "Portland", "state": "OR"},
		{"city": "Seattle", "state": "WA"},
		{"city": "Portland", "state": "OR"},
		{"city": "Portland", "state": "ME"}, // same city, different state
	})); err != nil {
		t.Fatal(err)
	}
	// Duplicates arriving in a later batch must still dedup.
	if err := h.Consume(context.Background(), batch.FromRows(schema, []map[string]any{
		{"city": "Seattle", "state": "WA"},
	})); err != nil {
		t.Fatal(err)
	}

	rows := drainAgg(t, h)
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3 distinct", len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[fmt.Sprintf("%v|%v", r["city"], r["state"])] = true
	}
	for _, want := range []string{"Portland|OR", "Seattle|WA", "Portland|ME"} {
		if !seen[want] {
			t.Errorf("missing distinct tuple %s", want)
		}
	}
}

func TestGroupByAll_NullsAndSchema(t *testing.T) {
	schema := []parquet.Column{
		{Name: "t1.val", Type: parquet.TypeInt64}, // qualified name must pass through verbatim
	}
	h := NewHashAggregate(nil, nil)
	h.GroupByAll = true
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h.Consume(context.Background(), batch.FromRows(schema, []map[string]any{
		{"t1.val": nil},
		{"t1.val": int64(1)},
		{"t1.val": nil}, // duplicate null collapses
	})); err != nil {
		t.Fatal(err)
	}

	out := h.outputSchema()
	if len(out) != 1 || out[0].Name != "t1.val" {
		t.Fatalf("output schema = %v, want pass-through [t1.val]", out)
	}
	if out[0].Type != parquet.TypeInt64 {
		t.Fatalf("output type = %v, want Int64 preserved", out[0].Type)
	}

	rows := drainAgg(t, h)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (null + 1)", len(rows))
	}
}

// A clone (parallel pipeline path) must inherit GroupByAll — without it the
// clone would resolve as a scalar aggregate and silently drop every group.
func TestGroupByAll_CloneSink(t *testing.T) {
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}
	h := NewHashAggregate(nil, nil)
	h.GroupByAll = true
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	clone := h.CloneSink().(*HashAggregate)
	if !clone.GroupByAll {
		t.Fatal("CloneSink dropped GroupByAll")
	}
	if err := clone.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := clone.Consume(context.Background(), batch.FromRows(schema, []map[string]any{
		{"n": int64(1)}, {"n": int64(1)}, {"n": int64(2)},
	})); err != nil {
		t.Fatal(err)
	}
	h.MergeSink(clone)
	rows := drainAgg(t, h)
	if len(rows) != 2 {
		t.Fatalf("got %d rows after merge, want 2", len(rows))
	}
}

// The point of the rewrite: a DISTINCT that exceeds the memory budget must
// spill and still produce the exact distinct set. The old operator had no
// spill path at all.
func TestGroupByAll_Spill(t *testing.T) {
	tracker := memory.NewTracker("distinct-spill-test", 1<<20) // 1 MiB budget
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "tag", Type: parquet.TypeString},
	}
	h := NewHashAggregate(nil, nil)
	h.GroupByAll = true
	h.Spill = sm
	if err := h.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	const distinct = 50_000
	ctx := context.Background()
	for start := 0; start < distinct; start += 1000 {
		rows := make([]map[string]any, 0, 2000)
		for i := start; i < start+1000; i++ {
			row := map[string]any{"id": int64(i), "tag": fmt.Sprintf("tag-%d", i%7)}
			rows = append(rows, row, row) // every row duplicated
		}
		if err := h.Consume(ctx, batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
	}

	rows := drainAgg(t, h)
	if len(rows) != distinct {
		t.Fatalf("got %d rows, want %d distinct", len(rows), distinct)
	}
	ids := make([]int, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, int(r["id"].(int64)))
	}
	sort.Ints(ids)
	for i, id := range ids {
		if id != i {
			t.Fatalf("ids[%d] = %d; distinct set corrupted", i, id)
		}
	}
}
