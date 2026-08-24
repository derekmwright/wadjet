package kernel

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Regression coverage for #457: MIN/MAX aggregate accumulators over a float
// column ignored the NaN total order. The row/slice updaters compared with
// raw IEEE `<`/`>`, which are always false against a NaN, so whichever value
// arrived FIRST kept the accumulator slot regardless of what arrived later —
// an arrival-order-dependent answer. PostgreSQL's float8_cmp_internal (and
// wadjet's own CompareFloat64/CompareFloat32, which the sort/rank family
// already followed per #446) place NaN ABOVE every other value: MAX over a
// set containing a NaN is always NaN, and MIN is the smallest non-NaN value
// unless every value is NaN. These tests pin that answer under every arrival
// order the old code could disagree about.

// --- Row-level updaters (ResolveRowMin/ResolveRowMax) -----------------------

func TestMinMaxRowFloat64_NaNArrivalOrder(t *testing.T) {
	cases := []struct {
		name    string
		vals    []float64
		wantMin float64
		// MAX is always NaN in every case here (each input set contains a
		// NaN), so there is no separate wantMax field.
	}{
		{"nan_first", []float64{math.NaN(), 5.0, 3.0, -100.0}, -100.0},
		{"nan_last", []float64{5.0, 3.0, -100.0, math.NaN()}, -100.0},
		{"nan_middle", []float64{5.0, math.NaN(), 3.0, -100.0}, -100.0},
	}
	minK := ResolveRowMin(batch.TypeFloat64)
	maxK := ResolveRowMax(batch.TypeFloat64)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vec := makeFloatVec(tc.vals, nil)
			var accMin, accMax Accumulator
			for i := range tc.vals {
				minK(&accMin, vec, i)
				maxK(&accMax, vec, i)
			}
			if !accMin.HasMin || !accMax.HasMax {
				t.Fatal("expected HasMin/HasMax true")
			}
			if accMin.MinF64 != tc.wantMin {
				t.Errorf("MIN = %v, want %v (NaN sorts greatest, so MIN must be the smallest FINITE value "+
					"regardless of where the NaN arrived — a raw IEEE `<` comparison lets a NaN that arrives "+
					"FIRST get stuck as the accumulator's min forever, since every later `v < NaN` is false)",
					accMin.MinF64, tc.wantMin)
			}
			if !math.IsNaN(accMax.MaxF64) {
				t.Errorf("MAX = %v, want NaN (PostgreSQL: NaN sorts greatest, so MAX over any set "+
					"containing a NaN is NaN — arrival order must not matter)", accMax.MaxF64)
			}
		})
	}
}

func TestMinMaxRowFloat32_NaNArrivalOrder(t *testing.T) {
	nan32 := float32(math.NaN())
	cases := []struct {
		name    string
		vals    []float32
		wantMin float64
	}{
		{"nan_first", []float32{nan32, 5.0, 3.0, -100.0}, -100.0},
		{"nan_last", []float32{5.0, 3.0, -100.0, nan32}, -100.0},
		{"nan_middle", []float32{5.0, nan32, 3.0, -100.0}, -100.0},
	}
	minK := ResolveRowMin(batch.TypeFloat32)
	maxK := ResolveRowMax(batch.TypeFloat32)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vec := makeFloat32Vec(tc.vals, nil)
			var accMin, accMax Accumulator
			for i := range tc.vals {
				minK(&accMin, vec, i)
				maxK(&accMax, vec, i)
			}
			if accMin.MinF64 != tc.wantMin {
				t.Errorf("MIN = %v, want %v", accMin.MinF64, tc.wantMin)
			}
			if !math.IsNaN(accMax.MaxF64) {
				t.Errorf("MAX = %v, want NaN", accMax.MaxF64)
			}
		})
	}
}

// TestMinMaxRowFloat64_AllNaN: a group whose every value is NaN answers NaN
// for both MIN and MAX (PostgreSQL, verified live: NaN is its own minimum
// and maximum when nothing else is present).
func TestMinMaxRowFloat64_AllNaN(t *testing.T) {
	vec := makeFloatVec([]float64{math.NaN(), math.NaN(), math.NaN()}, nil)
	minK := ResolveRowMin(batch.TypeFloat64)
	maxK := ResolveRowMax(batch.TypeFloat64)
	var accMin, accMax Accumulator
	for i := 0; i < 3; i++ {
		minK(&accMin, vec, i)
		maxK(&accMax, vec, i)
	}
	if !accMin.HasMin || !math.IsNaN(accMin.MinF64) {
		t.Errorf("MIN over all-NaN = %v (HasMin=%v), want NaN", accMin.MinF64, accMin.HasMin)
	}
	if !accMax.HasMax || !math.IsNaN(accMax.MaxF64) {
		t.Errorf("MAX over all-NaN = %v (HasMax=%v), want NaN", accMax.MaxF64, accMax.HasMax)
	}
}

// TestMinMaxRowFloat64_NaNAndNull: NULLs are skipped as usual; a NaN mixed
// with NULLs (and nothing else) still answers NaN for both MIN and MAX, and
// a NaN mixed with NULLs and one finite value answers the finite value for
// MIN and NaN for MAX (PostgreSQL, verified live).
func TestMinMaxRowFloat64_NaNAndNull(t *testing.T) {
	t.Run("nan_and_nulls_only", func(t *testing.T) {
		vec := makeFloatVec([]float64{math.NaN(), 0, 0}, []int{1, 2})
		minK := ResolveRowMin(batch.TypeFloat64)
		maxK := ResolveRowMax(batch.TypeFloat64)
		var accMin, accMax Accumulator
		for i := 0; i < 3; i++ {
			minK(&accMin, vec, i)
			maxK(&accMax, vec, i)
		}
		if !accMin.HasMin || !math.IsNaN(accMin.MinF64) {
			t.Errorf("MIN = %v (HasMin=%v), want NaN", accMin.MinF64, accMin.HasMin)
		}
		if !accMax.HasMax || !math.IsNaN(accMax.MaxF64) {
			t.Errorf("MAX = %v (HasMax=%v), want NaN", accMax.MaxF64, accMax.HasMax)
		}
	})

	t.Run("null_nan_finite", func(t *testing.T) {
		vec := makeFloatVec([]float64{0, math.NaN(), 2.0}, []int{0})
		minK := ResolveRowMin(batch.TypeFloat64)
		maxK := ResolveRowMax(batch.TypeFloat64)
		var accMin, accMax Accumulator
		for i := 0; i < 3; i++ {
			minK(&accMin, vec, i)
			maxK(&accMax, vec, i)
		}
		if accMin.MinF64 != 2.0 {
			t.Errorf("MIN = %v, want 2 (the NULL is skipped, the only finite value is the min)", accMin.MinF64)
		}
		if !math.IsNaN(accMax.MaxF64) {
			t.Errorf("MAX = %v, want NaN", accMax.MaxF64)
		}
	})
}

// --- Batch/slice updaters (ResolveBatchMin/ResolveBatchMax — the scalar,
// no-GROUP-BY aggregate fast path) ------------------------------------------

func TestBatchMinMaxFloat64_NaNArrivalOrder(t *testing.T) {
	cases := []struct {
		name string
		vals []float64
	}{
		{"nan_first", []float64{math.NaN(), 5.0, 3.0, -100.0}},
		{"nan_last", []float64{5.0, 3.0, -100.0, math.NaN()}},
		{"nan_middle", []float64{5.0, math.NaN(), 3.0, -100.0}},
	}
	minK := ResolveBatchMin(batch.TypeFloat64)
	maxK := ResolveBatchMax(batch.TypeFloat64)
	for _, tc := range cases {
		for _, useSel := range []bool{false, true} {
			name := tc.name
			if useSel {
				name += "_with_sel"
			}
			t.Run(name, func(t *testing.T) {
				vec := makeFloatVec(tc.vals, nil)
				var sel []uint32
				if useSel {
					sel = []uint32{0, 1, 2, 3}
				}
				var accMin, accMax Accumulator
				minK(&accMin, vec, sel, len(tc.vals))
				maxK(&accMax, vec, sel, len(tc.vals))
				if accMin.MinF64 != -100.0 {
					t.Errorf("MIN = %v, want -100", accMin.MinF64)
				}
				if !math.IsNaN(accMax.MaxF64) {
					t.Errorf("MAX = %v, want NaN", accMax.MaxF64)
				}
			})
		}
	}
}

func TestBatchMinMaxFloat32_NaNArrivalOrder(t *testing.T) {
	nan32 := float32(math.NaN())
	vec := makeFloat32Vec([]float32{5.0, nan32, 3.0, -100.0}, nil)
	minK := ResolveBatchMin(batch.TypeFloat32)
	maxK := ResolveBatchMax(batch.TypeFloat32)
	var accMin, accMax Accumulator
	minK(&accMin, vec, nil, 4)
	maxK(&accMax, vec, nil, 4)
	if accMin.MinF64 != -100.0 {
		t.Errorf("MIN = %v, want -100", accMin.MinF64)
	}
	if !math.IsNaN(accMax.MaxF64) {
		t.Errorf("MAX = %v, want NaN", accMax.MaxF64)
	}
}

// --- Accumulator.Merge (parallel/spill-drain combine of two partials) ------

// TestAccumulatorMergeFloat_NaN pins the merge direction that #457 also
// broke: MAX must adopt a NaN merged in from either side (NaN sorts
// greatest, so it wins a MAX comparison against anything), while MIN must
// NOT adopt a NaN merged in from the other side as long as a finite value is
// available anywhere (NaN sorts greatest, so it never wins a MIN comparison
// against a finite value) — a partial's MinF64/MaxF64 is only NaN when
// EVERY value that partial saw was itself NaN. Checked in both merge
// orders, which is what the spill drain's k-way merge (Accumulator.Merge)
// and the morsel-parallel scalar merge both rely on.
func TestAccumulatorMergeFloat_NaN(t *testing.T) {
	mkFloatAcc := func(hasMin, hasMax bool, min, max float64) Accumulator {
		return Accumulator{IsFloat: true, HasMin: hasMin, HasMax: hasMax, MinF64: min, MaxF64: max}
	}

	t.Run("all_nan_side_first", func(t *testing.T) {
		// a saw only NaN values; b saw a finite range. The union's minimum
		// is b's finite minimum (NaN never wins MIN), its maximum is NaN
		// (NaN always wins MAX).
		a := mkFloatAcc(true, true, math.NaN(), math.NaN())
		b := mkFloatAcc(true, true, -5.0, 5.0)
		a.Merge(&b)
		if a.MinF64 != -5.0 {
			t.Errorf("MIN = %v, want -5 (a's minimum was NaN only because every value IT saw was NaN; "+
				"the union's minimum is the other side's finite value)", a.MinF64)
		}
		if !math.IsNaN(a.MaxF64) {
			t.Errorf("MAX = %v, want NaN", a.MaxF64)
		}
	})

	t.Run("all_nan_side_second", func(t *testing.T) {
		a := mkFloatAcc(true, true, -5.0, 5.0)
		b := mkFloatAcc(true, true, math.NaN(), math.NaN())
		a.Merge(&b)
		if a.MinF64 != -5.0 {
			t.Errorf("MIN = %v, want -5 (merging in an all-NaN partial must not replace a's finite minimum)", a.MinF64)
		}
		if !math.IsNaN(a.MaxF64) {
			t.Errorf("MAX = %v, want NaN (merging in a NaN maximum must adopt it)", a.MaxF64)
		}
	})

	t.Run("finite_min_nan_max_both_directions", func(t *testing.T) {
		// One side saw {1.0, NaN}: MinF64=1.0 (NaN never became the min
		// because a finite value was already there), MaxF64=NaN.
		a := mkFloatAcc(true, true, 1.0, math.NaN())
		b := mkFloatAcc(true, true, -3.0, 2.0)
		a.Merge(&b)
		if a.MinF64 != -3.0 {
			t.Errorf("MIN = %v, want -3 (the finite minimum from the other side)", a.MinF64)
		}
		if !math.IsNaN(a.MaxF64) {
			t.Errorf("MAX = %v, want NaN (this side's NaN maximum must survive the merge)", a.MaxF64)
		}
	})
}
