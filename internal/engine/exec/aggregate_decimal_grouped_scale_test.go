package exec

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The GROUPED half of the cross-scale refusal (#685 review, item A).
//
// An ungrouped DECIMAL aggregate handed values at two scales raises 22003
// through kernel.Accumulator.DecScaleConflict. The grouped paths keep their
// state in the flat SoA arrays instead — one scale per aggregate (fa.decScale),
// set from the first batch's column, with the scatter adding raw unscaled
// integers into it — so there is no per-group accumulator to carry that flag
// and the same input came back 25.50 for a pair that has no reading under one
// scale: 12.75 is 1275 at scale 2, 0.1275 is 1275 at scale 4, and their sum is
// 2550 under whichever scale won.
//
// The latch is the operator's (HashAggregate.decScaleConflict), set where
// Consume already compares the batch's vector against the aggregate's
// established scale, and read at the same decAggErr the ungrouped form reads.
func TestGroupedDecimalAggregateRefusesTwoContributingScales(t *testing.T) {
	ctx := context.Background()
	at := func(scale int, key int64, unscaled int64) *batch.RecordBatch {
		s := []parquet.Column{
			{Name: "k", Type: parquet.TypeInt64},
			{Name: "a", Type: parquet.TypeDecimal, Precision: 18, Scale: scale, Nullable: true},
		}
		b := batch.NewRecordBatch(s, 1)
		b.Columns[0].Int64Data[0] = key
		b.Columns[0].Nulls.SetValid(0)
		b.Columns[1].DecimalData.Data[0] = batch.Int128From(unscaled)
		b.Columns[1].Nulls.SetValid(0)
		return b
	}

	for _, tc := range []struct {
		name    string
		sameKey bool
	}{
		// Same group: the two values are added into ONE accumulator slot, so
		// the wrong answer is arithmetic.
		{"one group", true},
		// Different groups: no slot mixes them, but the OUTPUT column still
		// renders both under one declared scale, so one of them is wrong.
		{"two groups", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHashAggregate([]string{"k"}, []AggColumn{
				{Func: AggSum, InputCol: "a", OutputCol: "s",
					OutputType: parquet.TypeDecimal, OutputPrecision: batch.MaxDecimalPrecision, OutputScale: 2},
			})
			if err := h.Init(ctx); err != nil {
				t.Fatalf("init: %v", err)
			}
			secondKey := int64(1)
			if !tc.sameKey {
				secondKey = 2
			}
			if err := h.Consume(ctx, at(2, 1, 1275)); err != nil { // 12.75
				t.Fatalf("consume: %v", err)
			}
			if err := h.Consume(ctx, at(4, secondKey, 1275)); err != nil { // 0.1275
				t.Fatalf("consume: %v", err)
			}
			if err := h.Finalize(ctx); err != nil {
				t.Fatalf("finalize: %v", err)
			}
			out, err := h.Next(ctx)
			if err == nil {
				var got any
				if out != nil {
					if rows := out.ToRows(); len(rows) > 0 {
						got = rows[0]["s"]
					}
				}
				t.Fatalf("the grouped aggregate ANSWERED %#v for two contributing scales; "+
					"the Int128s are counted in one scale and these are not, so this must be "+
					"a 22003 refusal exactly as the ungrouped form is", got)
			}
			if !strings.Contains(err.Error(), "two different scales") {
				t.Errorf("refusal does not name the disagreement: %v", err)
			}
		})
	}

	// The control: one scale throughout costs nothing, grouped or not. Without
	// it the latch could be a blanket refusal and this file would not notice.
	t.Run("one scale is not a conflict", func(t *testing.T) {
		h := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "a", OutputCol: "s",
				OutputType: parquet.TypeDecimal, OutputPrecision: batch.MaxDecimalPrecision, OutputScale: 2},
		})
		if err := h.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		for _, b := range []*batch.RecordBatch{at(2, 1, 1275), at(2, 1, 100), at(2, 2, 25)} {
			if err := h.Consume(ctx, b); err != nil {
				t.Fatalf("consume: %v", err)
			}
		}
		if err := h.Finalize(ctx); err != nil {
			t.Fatalf("finalize: %v", err)
		}
		out, err := h.Next(ctx)
		if err != nil {
			t.Fatalf("a single-scale grouped aggregate was refused: %v", err)
		}
		got := map[int64]string{}
		for _, r := range out.ToRows() {
			k, _ := r["k"].(int64)
			s, _ := r["s"].(string)
			got[k] = s
		}
		if got[1] != "13.75" || got[2] != "0.25" {
			t.Errorf("grouped sums = %v, want map[1:13.75 2:0.25]", got)
		}
	})
}
