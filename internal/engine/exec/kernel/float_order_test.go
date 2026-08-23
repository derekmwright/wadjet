package kernel

import (
	"math"
	"sort"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func nan() float64 { z := 0.0; return z / z }

// TestFloatOrderIsPostgres pins the ORDER, not merely its totality: ADR-0012
// makes PostgreSQL the authority, and float8_cmp_internal puts NaN above
// every other value and equal to itself. A comparator that only made NaN
// *consistent* — say, smallest, or a tie folded to 0 — would satisfy the
// property test and still answer `ORDER BY f` differently from PostgreSQL.
func TestFloatOrderIsPostgres(t *testing.T) {
	cases := []struct {
		name string
		a, b float64
		want int
	}{
		{"nan_after_finite", nan(), 1e308, 1},
		{"finite_before_nan", -1e308, nan(), -1},
		{"nan_after_inf", nan(), math.Inf(1), 1},
		{"neg_inf_before_nan", math.Inf(-1), nan(), -1},
		{"nan_equals_nan", nan(), nan(), 0},
		{"zero_equals_negative_zero", 0.0, math.Copysign(0, -1), 0},
		{"ordinary", 1.0, 2.0, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CompareFloat64(tc.a, tc.b); got != tc.want {
				t.Errorf("CompareFloat64(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
			if got := CompareFloat32(float32(tc.a), float32(tc.b)); got != tc.want {
				t.Errorf("CompareFloat32(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestVectorOrderIsTransitiveWithNaN is #446's counterexample verbatim. The
// old per-position tie made these three vectors a CYCLE — a <= b, b <= c, and
// a > c — so ResolveSortCompare did not return a total order for VECTOR, and
// SortMergeJoin uses it for key EQUALITY.
func TestVectorOrderIsTransitiveWithNaN(t *testing.T) {
	col := parquet.Column{Name: "v", Type: parquet.TypeVector, Nullable: true, Dimension: 3}
	rows := []map[string]any{
		{"v": []float32{float32(nan()), 0, 2}}, // a
		{"v": []float32{0, 1, 2}},              // b
		{"v": []float32{1, 0, 1}},              // c
	}
	vec := containerBatch(t, col, rows)
	cmp := ResolveSortCompare(col.Type)

	// NaN is greatest, so a is above both of the others and the cycle is gone.
	if c := cmp(vec, 0, vec, 1); c <= 0 {
		t.Errorf("[NaN,0,2] vs [0,1,2] = %d, want > 0 (NaN sorts after 0)", c)
	}
	if c := cmp(vec, 1, vec, 2); c >= 0 {
		t.Errorf("[0,1,2] vs [1,0,1] = %d, want < 0", c)
	}
	if c := cmp(vec, 0, vec, 2); c <= 0 {
		t.Errorf("[NaN,0,2] vs [1,0,1] = %d, want > 0", c)
	}

	idx := []int{0, 1, 2}
	sort.SliceStable(idx, func(i, j int) bool { return cmp(vec, idx[i], vec, idx[j]) < 0 })
	if idx[2] != 0 {
		t.Errorf("sorted order %v: the NaN-leading vector must sort last", idx)
	}
}

// TestScalarFloatColumnSortsNaNLast is the same rule one level down: the
// scalar comparators are what a plain `ORDER BY float_col` runs, and they
// used to report every NaN pair equal, so a NaN landed wherever the sort's
// input order happened to leave it.
func TestScalarFloatColumnSortsNaNLast(t *testing.T) {
	for _, typ := range []parquet.TypeID{parquet.TypeFloat64, parquet.TypeFloat32} {
		vec := batch.NewVector(typ, 4)
		vals := []float64{2, nan(), math.Inf(1), -1}
		for i, v := range vals {
			vec.SetValue(i, castFloat(typ, v))
		}
		for _, arm := range []struct {
			name string
			cmp  SortCompareKernel
		}{
			{"nulls_first", ResolveSortCompare(typ)},
			{"nulls_last", ResolveSortCompareNullsLast(typ)},
			{"no_nulls", ResolveSortCompareNoNulls(typ)},
		} {
			idx := []int{0, 1, 2, 3}
			sort.SliceStable(idx, func(i, j int) bool { return arm.cmp(vec, idx[i], vec, idx[j]) < 0 })
			want := []int{3, 0, 2, 1} // -1, 2, +Inf, NaN
			for k := range want {
				if idx[k] != want[k] {
					t.Fatalf("%v/%s: sorted %v, want %v (NaN last, PostgreSQL's float order)",
						typ, arm.name, idx, want)
				}
			}
		}
	}
}

func castFloat(typ parquet.TypeID, v float64) any {
	if typ == parquet.TypeFloat32 {
		return float32(v)
	}
	return v
}
