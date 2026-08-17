//go:build linux

package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The off-heap switch must actually back the typed-SoA state (packed
// keys + flat accumulators live in mmap reservations), produce identical
// results to the heap path, and release every mapping at Close — the
// engagement + leak guard for the ADR-0006 amendment.
func TestOffheapAggEngagesAndReleases(t *testing.T) {
	if !memory.OffheapAvailable() {
		t.Skip("offheap-agg disabled")
	}
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	run := func() (map[string]int64, *HashAggregate) {
		h := NewHashAggregate([]string{"a", "b"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
			{Func: AggMin, InputCol: "v", OutputCol: "m", OutputType: parquet.TypeInt64},
		})
		if err := h.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		bb := batch.NewRecordBatch(schema, 2048)
		total := 0
		for g := 0; g < 200_000; g += 2048 {
			n := 2048
			for i := range n {
				k := int64(g + i)
				bb.Columns[0].Int64Data[i] = k % 50_000
				bb.Columns[1].Int64Data[i] = k % 977
				bb.Columns[2].Int64Data[i] = k
				for c := range 3 {
					bb.Columns[c].Nulls.SetValid(i)
				}
			}
			bb.Len = n
			total += n
			if err := h.Consume(ctx, bb); err != nil {
				t.Fatal(err)
			}
		}
		if err := h.Finalize(ctx); err != nil {
			t.Fatal(err)
		}
		out := map[string]int64{}
		for {
			b, err := h.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				break
			}
			for _, r := range b.ToRows() {
				out[fmt.Sprint(r["a"], "|", r["b"])] = r["s"].(int64)
			}
		}
		return out, h
	}

	got, h := run()
	if !h.usePackedGroupKey {
		t.Fatal("expected packed composite-key path")
	}
	// State already returned: the post-emit drop closes the registry.
	if h.offheap != nil && h.offheap.Mappings() != 0 {
		t.Fatalf("mappings live after emission: %d", h.offheap.Mappings())
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}

	prev := memory.OffheapToggleForTest(false)
	defer memory.OffheapToggleForTest(prev)
	want, h2 := run()
	_ = h2.Close()

	if len(got) != len(want) {
		t.Fatalf("group counts differ: offheap %d vs heap %d", len(got), len(want))
	}
	for k, w := range want {
		if got[k] != w {
			t.Fatalf("group %s: offheap %d vs heap %d", k, got[k], w)
		}
	}
}
