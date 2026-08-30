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

	// The MORSEL-CLONE seams. Consume's latch compares a batch against the
	// scale its OWN operator established, so two clones fed one scale each
	// never see a disagreement — the merge is the only place it exists, and
	// before item A's second pass the "prefer first nonzero" upgrade took the
	// primary's scale and discarded it. Both routes through mergeSinkState are
	// here: the upgrade loop, and the empty-primary adopt.
	t.Run("two clones at two scales", func(t *testing.T) {
		parent := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "a", OutputCol: "s",
				OutputType: parquet.TypeDecimal, OutputPrecision: batch.MaxDecimalPrecision, OutputScale: 2},
		})
		if err := parent.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		// The primary consumes at scale 2 itself, so it holds groups and the
		// merge below takes the upgrade-loop route rather than the adopt one.
		if err := parent.Consume(ctx, at(2, 1, 1275)); err != nil {
			t.Fatalf("primary consume: %v", err)
		}
		clone := parent.CloneSink().(*HashAggregate)
		if err := clone.Init(ctx); err != nil {
			t.Fatalf("clone init: %v", err)
		}
		if err := clone.Consume(ctx, at(4, 1, 1275)); err != nil {
			t.Fatalf("clone consume: %v", err)
		}
		parent.MergeSink(clone)
		assertGroupedScaleRefusal(t, ctx, parent)
	})

	t.Run("empty primary adopts a clone at another scale", func(t *testing.T) {
		parent := NewHashAggregate([]string{"k"}, []AggColumn{
			{Func: AggSum, InputCol: "a", OutputCol: "s",
				OutputType: parquet.TypeDecimal, OutputPrecision: batch.MaxDecimalPrecision, OutputScale: 2},
		})
		if err := parent.Init(ctx); err != nil {
			t.Fatalf("init: %v", err)
		}
		// A batch that RESOLVES the primary's schema at scale 2 but leaves it
		// with no groups: an empty selection. That is what routes the merge
		// through adoptStateFrom instead, which overwrites the scale wholesale
		// and so has to compare BEFORE it does.
		resolveOnly := at(2, 1, 1275)
		resolveOnly.Sel = []uint32{}
		if err := parent.Consume(ctx, resolveOnly); err != nil {
			t.Fatalf("primary consume: %v", err)
		}
		if parent.groupCount() != 0 {
			t.Fatalf("the primary holds %d groups, so this merge will not take the adopt "+
				"path and the sub-test covers the seam it already covers", parent.groupCount())
		}
		clone := parent.CloneSink().(*HashAggregate)
		if err := clone.Init(ctx); err != nil {
			t.Fatalf("clone init: %v", err)
		}
		if err := clone.Consume(ctx, at(4, 1, 1275)); err != nil {
			t.Fatalf("clone consume: %v", err)
		}
		parent.MergeSink(clone)
		assertGroupedScaleRefusal(t, ctx, parent)
	})

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

// assertGroupedScaleRefusal drains h and requires the 22003 a cross-scale pair
// owes, naming the answer it gave instead.
func assertGroupedScaleRefusal(t *testing.T, ctx context.Context, h *HashAggregate) {
	t.Helper()
	if err := h.Finalize(ctx); err != nil {
		if strings.Contains(err.Error(), "two different scales") {
			return
		}
		t.Fatalf("finalize failed, but not over the scale disagreement: %v", err)
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
}
