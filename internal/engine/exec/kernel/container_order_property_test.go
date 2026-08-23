package kernel

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Property test for the #415 container total orders, over RANDOM data —
// container_sort_test.go pins specific examples; this generates many rows
// per type and checks the properties any total order must have, plus the
// one SortMergeJoin actually depends on:
//
//	P1 reflexive:      cmp(i,i) == 0
//	P2 antisymmetric:  cmp(i,j) == -sign(cmp(j,i))
//	P3 transitive:     cmp(i,j)<=0 && cmp(j,k)<=0  =>  cmp(i,k)<=0
//	P4 key-consistent: cmp(i,j)==0  <=>  the two values are the same value
//
// P4's "the same value" is fmt's own recursive rendering of the boxed value
// (vec.GetValue, via "%#v"), not a byte-level key encoder: the real encoder
// (appendColumnValue et al.) lives in package exec, which already imports
// kernel — reaching for it here would cycle. fmt renders a float by testing
// IsNaN rather than by "==", so two NaN cells compare equal here exactly
// when the comparator says they do (this generator's propNaN always
// produces the one canonical NaN bit pattern, so the two notions of "same
// NaN" never actually diverge in this test).

func arrayIntCol() parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}}
}

func arrayOfRowCol() parquet.Column {
	return parquet.Column{Name: "v", Type: parquet.TypeArray, Nullable: true,
		ElementType: &parquet.Column{Name: "element", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}}}
}

func propGen(rng *rand.Rand, col parquet.Column, withNaN bool) any {
	if rng.Intn(8) == 0 {
		return nil
	}
	switch col.Type {
	case parquet.TypeArray:
		n := rng.Intn(4)
		out := make([]any, n)
		for i := range out {
			out[i] = propGenElem(rng, *col.ElementType)
		}
		return out
	case parquet.TypeRow:
		m := map[string]any{}
		for _, f := range col.Fields {
			m[f.Name] = propGenElem(rng, f)
		}
		return m
	case parquet.TypeMap:
		n := rng.Intn(3)
		out := map[string]any{}
		for i := 0; i < n; i++ {
			out[fmt.Sprintf("k%d", rng.Intn(3))] = propMaybeNull(rng, int64(rng.Intn(3)))
		}
		return out
	case parquet.TypeVector:
		out := make([]float32, col.Dimension)
		for i := range out {
			if withNaN && rng.Intn(6) == 0 {
				out[i] = float32(propNaN())
				continue
			}
			out[i] = float32(rng.Intn(3))
		}
		return out
	}
	return nil
}

// propNaN returns a NaN computed at runtime (0.0/0.0), matching zzGen's
// approach in the original scratch test: the point is a value the compiler
// cannot constant-fold away, not a particular payload — every call produces
// the same canonical quiet-NaN bit pattern regardless.
func propNaN() float64 { z := 0.0; return z / z }

func propMaybeNull(rng *rand.Rand, v any) any {
	if rng.Intn(5) == 0 {
		return nil
	}
	return v
}

func propGenElem(rng *rand.Rand, col parquet.Column) any {
	switch col.Type {
	case parquet.TypeString:
		if rng.Intn(5) == 0 {
			return nil
		}
		return []string{"", "a", "b", "aa"}[rng.Intn(4)]
	case parquet.TypeInt64:
		if rng.Intn(5) == 0 {
			return nil
		}
		return int64(rng.Intn(3))
	case parquet.TypeRow:
		if rng.Intn(6) == 0 {
			return nil
		}
		m := map[string]any{}
		for _, f := range col.Fields {
			m[f.Name] = propGenElem(rng, f)
		}
		return m
	}
	return nil
}

// hasNaN reports whether v (a VECTOR cell's boxed []float32, this
// generator's only source of NaN) contains one. compareVectorAt ties a NaN
// element against WHATEVER is at the same position in the other operand
// (container_sort.go: "NaN compares equal to everything, as it does in the
// FLOAT32 and FLOAT64 column comparators") — a per-POSITION tie, not a
// whole-value one, exactly like a bare FLOAT64 column's own NaN handling
// (which is only ever asked about ONE position, so it never has occasion to
// break down the same way). Chaining that per-position tie across DIFFERENT
// pairs of vectors — the only way a 3+-element vector CAN be compared — does
// not compose into a value-level equivalence relation, so P2/P3/P4 do not
// hold once a NaN is anywhere in the comparison: excluded here, matching the
// container-order doc's own "no more ordered than a float column of NaNs"
// admission. This is NOT the same as the pre-existing scalar-float NaN tie a
// separate reviewer investigation found unrelated to #415 — over 3+ elements
// the per-position tie genuinely breaks transitivity (filed as #446, not
// fixed in this round: the natural fix is a semantics decision that also
// touches ARRAY(FLOAT)'s identical exposure).
func hasNaN(v any) bool {
	fs, ok := v.([]float32)
	if !ok {
		return false
	}
	for _, f := range fs {
		if math.IsNaN(float64(f)) {
			return true
		}
	}
	return false
}

func propSign(x int) int {
	if x < 0 {
		return -1
	}
	if x > 0 {
		return 1
	}
	return 0
}

func TestContainerOrderProperties(t *testing.T) {
	cases := []struct {
		name    string
		col     parquet.Column
		withNaN bool
	}{
		{"array_string", arrayCol(), false},
		{"array_int64", arrayIntCol(), false},
		{"row", rowCol(), false},
		{"map", mapCol(), false},
		{"vector", vectorCol(), false},
		{"vector_nan", vectorCol(), true},
		{"array_of_row", arrayOfRowCol(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(7))
			const n = 60
			rows := make([]map[string]any, n)
			for i := range rows {
				rows[i] = map[string]any{"v": propGen(rng, tc.col, tc.withNaN)}
			}
			vec := containerBatch(t, tc.col, rows)
			cmp := ResolveSortCompare(tc.col.Type)
			if cmp == nil {
				t.Fatalf("nil comparator")
			}
			keyOf := func(i int) string { return fmt.Sprintf("%#v", vec.GetValue(i)) }
			nan := make([]bool, n)
			for i := range rows {
				nan[i] = hasNaN(rows[i]["v"])
			}

			p1, p2, p3, p4 := 0, 0, 0, 0
			for i := 0; i < n; i++ {
				if c := cmp(vec, i, vec, i); c != 0 && p1 < 3 {
					p1++
					t.Errorf("P1 reflexive: row %d vs itself = %d (%v)", i, c, rows[i]["v"])
				}
				for j := 0; j < n; j++ {
					if nan[i] || nan[j] {
						continue // see hasNaN
					}
					cij, cji := cmp(vec, i, vec, j), cmp(vec, j, vec, i)
					if propSign(cij) != -propSign(cji) && p2 < 3 {
						p2++
						t.Errorf("P2 antisymmetry: (%d,%d)=%d (%d,%d)=%d\n  a=%v\n  b=%v",
							i, j, cij, j, i, cji, rows[i]["v"], rows[j]["v"])
					}
					if vec.Nulls.IsNullFast(i) || vec.Nulls.IsNullFast(j) {
						continue
					}
					eqCmp := cij == 0
					if eqK := keyOf(i) == keyOf(j); eqK != eqCmp && p4 < 4 {
						p4++
						t.Errorf("P4 value-equality vs compare disagree: rows %d/%d cmp=%d keyEq=%v\n  a=%v\n  b=%v",
							i, j, cij, eqK, rows[i]["v"], rows[j]["v"])
					}
				}
			}
			for i := 0; i < n && p3 < 3; i++ {
				for j := 0; j < n && p3 < 3; j++ {
					if nan[i] || nan[j] || cmp(vec, i, vec, j) > 0 {
						continue
					}
					for k := 0; k < n && p3 < 3; k++ {
						if nan[k] || cmp(vec, j, vec, k) > 0 {
							continue
						}
						if cmp(vec, i, vec, k) > 0 {
							p3++
							t.Errorf("P3 transitivity: %d<=%d<=%d but (%d,%d)=%d\n  a=%v\n  b=%v\n  c=%v",
								i, j, k, i, k, cmp(vec, i, vec, k), rows[i]["v"], rows[j]["v"], rows[k]["v"])
						}
					}
				}
			}
		})
	}
}
