package exec

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Construction-time group-index layout for BOUNDED sinks — the rule in
// two_level_hash.go's twoLevelBoundedMinGroups.
//
// A sink whose owner finalizes and rebuilds it every C bytes
// (worker.cappedPartialAgg) cannot amortize a flat→bucketed conversion: the
// rehash lands near the end of an epoch on a table that is about to be torn
// down. SF100 Q18 measured that at 3-4× on the stage (mean task 2.25 s flat
// vs 6.96/10.13 s bucketed, ~675 ns per live entry converted). The decision
// is a property of the operator's configuration, so it is taken once, before
// the first row.

// boundedTestBatches builds nBatches × 2048 rows of a NEAR-UNIQUE int key —
// the shape that mints a group on every row and therefore always reaches the
// conversion gate.
func boundedTestBatches(nBatches int) []*batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	const perBatch = 2048
	out := make([]*batch.RecordBatch, 0, nBatches)
	for bi := 0; bi < nBatches; bi++ {
		rows := make([]map[string]any, 0, perBatch)
		for i := 0; i < perBatch; i++ {
			g := int64(bi*perBatch + i)
			rows = append(rows, map[string]any{"k": g * 2654435761, "v": g})
		}
		out = append(out, batch.FromRows(schema, rows))
	}
	return out
}

func boundedTestAggs() []AggColumn {
	return []AggColumn{
		{Func: AggCount, OutputCol: "cnt", OutputType: parquet.TypeInt64},
		{Func: AggSum, InputCol: "v", OutputCol: "sv", OutputType: parquet.TypeInt64},
	}
}

// runBoundedFill consumes the batches through one HashAggregate carrying the
// given epoch cap and reports how many conversions it paid and whether its
// index ended bucketed.
func runBoundedFill(t *testing.T, cap int64, batches []*batch.RecordBatch) (conversions int64, bucketed, bornFlat bool, ceiling int64) {
	t.Helper()
	ctx := context.Background()
	h := NewHashAggregate([]string{"k"}, boundedTestAggs())
	h.SetEpochByteCap(cap)
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	before := TwoLevelConversions.Load()
	for _, b := range batches {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	return TwoLevelConversions.Load() - before, h.intTwoLevel != nil, h.IndexBornFlat(), h.GroupCeiling()
}

// TestBoundedSinkIsBornFlatAndNeverConverts is the regression for the SF100
// Q18 stage: the same fill that converts once as an unbounded sink must not
// convert at all once the sink declares the byte cap its owner enforces.
func TestBoundedSinkIsBornFlatAndNeverConverts(t *testing.T) {
	batches := boundedTestBatches(24)

	t.Run("unbounded control converts", func(t *testing.T) {
		withTwoLevelStrict(t, true, 4096, func() {
			conv, bucketed, bornFlat, _ := runBoundedFill(t, 0, batches)
			if conv != 1 || !bucketed {
				t.Fatalf("unbounded sink: conversions=%d bucketed=%v, want 1/true "+
					"(the control must still take the adaptive path)", conv, bucketed)
			}
			if bornFlat {
				t.Error("unbounded sink reported bornFlat")
			}
		})
	})

	t.Run("bounded sink stays flat", func(t *testing.T) {
		withTwoLevelStrict(t, true, 4096, func() {
			conv, bucketed, bornFlat, ceiling := runBoundedFill(t, defaultBenchPartialAggCap, batches)
			if conv != 0 {
				t.Errorf("bounded sink paid %d conversions, want 0", conv)
			}
			if bucketed {
				t.Error("bounded sink ended with a bucketed index")
			}
			if !bornFlat {
				t.Error("bounded sink did not report bornFlat")
			}
			if ceiling <= 0 || ceiling >= twoLevelBoundedMinGroups {
				t.Errorf("group ceiling = %d, want 0 < ceiling < %d for a 128 MB cap",
					ceiling, twoLevelBoundedMinGroups)
			}
		})
	})

	// Eager mode (the WADJET_TWO_LEVEL_AT override the oracles run under)
	// must not smuggle a conversion past the layout decision either.
	t.Run("bounded sink stays flat under the eager override", func(t *testing.T) {
		withTwoLevel(t, true, 4096, func() {
			conv, bucketed, _, _ := runBoundedFill(t, defaultBenchPartialAggCap, batches)
			if conv != 0 || bucketed {
				t.Fatalf("eager override: conversions=%d bucketed=%v, want 0/false", conv, bucketed)
			}
		})
	})

	// The rule is Gmax, not "bounded ⇒ flat": a cap large enough to hold G*
	// groups leaves the adaptive path alone.
	t.Run("a cap above G* still converts", func(t *testing.T) {
		withTwoLevelStrict(t, true, 4096, func() {
			// perGroupStateBytes for this shape is ~46 B, so a cap of
			// G* × 64 B is comfortably past the ceiling either way.
			conv, bucketed, bornFlat, ceiling := runBoundedFill(t, twoLevelBoundedMinGroups*64, batches)
			if ceiling < twoLevelBoundedMinGroups {
				t.Fatalf("ceiling = %d, want >= %d — the cap under test is too small",
					ceiling, twoLevelBoundedMinGroups)
			}
			if bornFlat || conv != 1 || !bucketed {
				t.Fatalf("large-cap sink: bornFlat=%v conversions=%d bucketed=%v, want false/1/true",
					bornFlat, conv, bucketed)
			}
		})
	})

	t.Run("kill switch restores the runtime-convert behavior", func(t *testing.T) {
		prev := bornFlatToggle.Set(false)
		defer bornFlatToggle.Set(prev)
		withTwoLevelStrict(t, true, 4096, func() {
			conv, bucketed, bornFlat, _ := runBoundedFill(t, defaultBenchPartialAggCap, batches)
			if bornFlat {
				t.Error("bornFlat set with WADJET_TWO_LEVEL_BORN_FLAT=0")
			}
			if conv != 1 || !bucketed {
				t.Fatalf("switch off: conversions=%d bucketed=%v, want 1/true", conv, bucketed)
			}
		})
	})
}

// TestPerGroupStateBytesBoundsTheQ18Shape pins the derivation behind
// twoLevelBoundedMinGroups: the exchange partial aggregate's own numbers
// (128 MB cap, one int key, one COUNT and one SUM) must produce a ceiling in
// the neighbourhood of the 2.50 M groups per flush measured at SF100, and
// therefore below G*.
func TestPerGroupStateBytesBoundsTheQ18Shape(t *testing.T) {
	schema := []parquet.Column{
		{Name: "l_orderkey", Type: parquet.TypeInt64},
		{Name: "l_quantity", Type: parquet.TypeFloat64},
	}
	b := batch.FromRows(schema, []map[string]any{
		{"l_orderkey": int64(1), "l_quantity": 17.0},
	})
	cases := []struct {
		name string
		aggs []AggColumn
	}{
		{"sum only", []AggColumn{
			{Func: AggSum, InputCol: "l_quantity", OutputCol: "sq", OutputType: parquet.TypeFloat64},
		}},
		{"count + sum", []AggColumn{
			{Func: AggCount, OutputCol: "c", OutputType: parquet.TypeInt64},
			{Func: AggSum, InputCol: "l_quantity", OutputCol: "sq", OutputType: parquet.TypeFloat64},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := NewHashAggregate([]string{"l_orderkey"}, c.aggs)
			h.SetEpochByteCap(128 << 20)
			if err := h.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			if err := h.Consume(context.Background(), b); err != nil {
				t.Fatal(err)
			}
			s := h.perGroupStateBytes(b)
			if s < 24 || s > 64 {
				t.Fatalf("per-group state = %d B, want 24..64 for a single int key "+
					"with %d aggregate(s) — the derivation in twoLevelBoundedMinGroups "+
					"assumes ~46 B", s, len(c.aggs))
			}
			if got := h.GroupCeiling(); got >= twoLevelBoundedMinGroups {
				t.Fatalf("ceiling = %d groups at a 128 MB cap, want < G* = %d: the "+
					"Q18 stage measured 2.50 M groups per flush", got, twoLevelBoundedMinGroups)
			}
			if !h.IndexBornFlat() {
				t.Fatal("Q18-shaped bounded sink was not born flat")
			}
		})
	}
}
