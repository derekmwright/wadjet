package exec

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The partial-state run format must carry every accumulator field its
// (Func, IsFloat, IsDecimal) spec claims, exactly (#782).
//
// This is a FORMAT gate, not an aggregate gate: it drives partialSpillWriter
// and partialSpillReader directly, so it holds whether or not any HashAggregate
// path happens to reach a given combination today. The accumulator it writes
// has EVERY value field set to a distinct non-zero value, so a spec that sends
// a value down the wrong arm — a DECIMAL sum encoded as SumI64, a FLOAT sum
// encoded as SumI64 — reads back as a zero rather than as a coincidentally
// equal number, and the diff names the field.
func TestPartialSpillRunCarriesEveryAccumulatorFieldItsSpecClaims(t *testing.T) {
	// One accumulator populated in every field. A correct round trip returns
	// the subset the spec selects; nothing else is asserted, because emitAcc
	// deliberately writes only what the spec's arm needs.
	full := func() kernel.Accumulator {
		return kernel.Accumulator{
			Count:       77,
			SumI64:      -9_007_199_254_740_993,
			SumF64:      -1.7976931348623157e+300,
			SumDec:      batch.Int128{Hi: 3, Lo: 0xfedcba9876543210},
			MinI64:      -1 << 62,
			MaxI64:      1<<62 - 1,
			MinF64:      -2.5e-300,
			MaxF64:      3.5e300,
			MinDec:      batch.Int128{Hi: -2, Lo: 17},
			MaxDec:      batch.Int128{Hi: 5, Lo: 0x0123456789abcdef},
			HasMin:      true,
			HasMax:      true,
			DecOverflow: true,
			IsFloat:     true,
			IsDecimal:   true,
			DecScale:    6,
		}
	}
	type field struct {
		name string
		get  func(a *kernel.Accumulator) any
	}
	cases := []struct {
		name   string
		spec   partialAggSpec
		fields []field
	}{
		{"sum-int", partialAggSpec{Func: AggSum},
			[]field{{"Count", func(a *kernel.Accumulator) any { return a.Count }},
				{"SumI64", func(a *kernel.Accumulator) any { return a.SumI64 }}}},
		{"sum-float", partialAggSpec{Func: AggSum, IsFloat: true},
			[]field{{"Count", func(a *kernel.Accumulator) any { return a.Count }},
				{"SumF64", func(a *kernel.Accumulator) any { return a.SumF64 }}}},
		{"sum-decimal", partialAggSpec{Func: AggSum, IsDecimal: true, DecScale: 6},
			[]field{{"Count", func(a *kernel.Accumulator) any { return a.Count }},
				{"SumDec", func(a *kernel.Accumulator) any { return a.SumDec }},
				{"DecOverflow", func(a *kernel.Accumulator) any { return a.DecOverflow }},
				{"DecScale", func(a *kernel.Accumulator) any { return a.DecScale }}}},
		{"avg-float", partialAggSpec{Func: AggAvg, IsFloat: true},
			[]field{{"Count", func(a *kernel.Accumulator) any { return a.Count }},
				{"SumF64", func(a *kernel.Accumulator) any { return a.SumF64 }}}},
		{"avg-decimal", partialAggSpec{Func: AggAvg, IsDecimal: true, DecScale: 6},
			[]field{{"Count", func(a *kernel.Accumulator) any { return a.Count }},
				{"SumDec", func(a *kernel.Accumulator) any { return a.SumDec }},
				{"DecScale", func(a *kernel.Accumulator) any { return a.DecScale }}}},
		{"count", partialAggSpec{Func: AggCount},
			[]field{{"Count", func(a *kernel.Accumulator) any { return a.Count }}}},
		{"min-int", partialAggSpec{Func: AggMin},
			[]field{{"HasMin", func(a *kernel.Accumulator) any { return a.HasMin }},
				{"MinI64", func(a *kernel.Accumulator) any { return a.MinI64 }}}},
		{"min-float", partialAggSpec{Func: AggMin, IsFloat: true},
			[]field{{"HasMin", func(a *kernel.Accumulator) any { return a.HasMin }},
				{"MinF64", func(a *kernel.Accumulator) any { return a.MinF64 }}}},
		{"min-decimal", partialAggSpec{Func: AggMin, IsDecimal: true, DecScale: 6},
			[]field{{"HasMin", func(a *kernel.Accumulator) any { return a.HasMin }},
				{"MinDec", func(a *kernel.Accumulator) any { return a.MinDec }},
				{"DecScale", func(a *kernel.Accumulator) any { return a.DecScale }}}},
		{"max-int", partialAggSpec{Func: AggMax},
			[]field{{"HasMax", func(a *kernel.Accumulator) any { return a.HasMax }},
				{"MaxI64", func(a *kernel.Accumulator) any { return a.MaxI64 }}}},
		{"max-float", partialAggSpec{Func: AggMax, IsFloat: true},
			[]field{{"HasMax", func(a *kernel.Accumulator) any { return a.HasMax }},
				{"MaxF64", func(a *kernel.Accumulator) any { return a.MaxF64 }}}},
		{"max-decimal", partialAggSpec{Func: AggMax, IsDecimal: true, DecScale: 6},
			[]field{{"HasMax", func(a *kernel.Accumulator) any { return a.HasMax }},
				{"MaxDec", func(a *kernel.Accumulator) any { return a.MaxDec }},
				{"DecScale", func(a *kernel.Accumulator) any { return a.DecScale }}}},
	}

	dir := t.TempDir()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			header := partialSpillHeader{
				GroupCols: []partialGroupCol{{Name: "k", Type: parquet.TypeInt64}},
				Aggs:      []partialAggSpec{tc.spec},
			}
			w, path, err := newPartialSpillWriter(dir, header)
			if err != nil {
				t.Fatalf("newPartialSpillWriter: %v", err)
			}
			want := full()
			g := partialGroup{
				SortKey: []byte("k1"),
				KeyVals: []partialKeyValue{{Tag: partialTagInt64, I64: 41}},
				Accs:    []kernel.Accumulator{want},
			}
			if err := w.Write(g); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if _, err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			r, err := openPartialSpillReader(path)
			if err != nil {
				t.Fatalf("openPartialSpillReader: %v", err)
			}
			defer r.Close()
			if got, exp := r.header.Aggs[0], tc.spec; got != exp {
				t.Fatalf("header spec round trip: got %+v, want %+v", got, exp)
			}
			back, err := r.Next()
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if back == nil {
				t.Fatal("Next returned no group")
			}
			if kv := back.KeyVals[0]; kv.Tag != partialTagInt64 || kv.I64 != 41 {
				t.Errorf("group key round trip: tag=%d i64=%d, want tag=%d i64=41", kv.Tag, kv.I64, partialTagInt64)
			}
			for _, f := range tc.fields {
				if got, exp := f.get(&back.Accs[0]), f.get(&want); got != exp {
					t.Errorf("%s: got %v, want %v", f.name, got, exp)
				}
			}
		})
	}
}
