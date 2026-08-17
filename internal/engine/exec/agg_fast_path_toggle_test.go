package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The agg-fast-paths kill switch must actually gate the typed fast-path
// family: with it off, an aggregate that would take the single-int path
// runs generic — and produces the same answer. Guards against the switch
// going dormant (gating nothing) after refactors of the Init fast-path
// selection block.
func TestAggFastPathsKillSwitch(t *testing.T) {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{
		{"k": int64(1), "v": int64(2)},
		{"k": int64(2), "v": int64(5)},
		{"k": int64(1), "v": int64(10)},
	}
	build := func() *HashAggregate {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "v", OutputCol: "s", OutputType: parquet.TypeInt64},
		})
		if err := h.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := h.Consume(context.Background(), batch.FromRows(schema, rows)); err != nil {
			t.Fatal(err)
		}
		return h
	}

	on := build()
	if !on.useIntGroupKey {
		t.Fatal("toggle on: expected single-int fast path to engage")
	}

	prev := aggFastPaths.Set(false)
	defer aggFastPaths.Set(prev)
	off := build()
	if off.useIntGroupKey || off.simpleAggs {
		t.Fatal("toggle off: typed fast path engaged despite kill switch")
	}

	wantSums := sumByKey(aggRows(t, on), "k")
	gotSums := sumByKey(aggRows(t, off), "k")
	if len(gotSums) != len(wantSums) {
		t.Fatalf("group counts differ: fast %v vs generic %v", wantSums, gotSums)
	}
	for k, want := range wantSums {
		if got := gotSums[k]; got != want {
			t.Errorf("key %v: fast path sum %v, generic %v", k, want, got)
		}
	}
}
