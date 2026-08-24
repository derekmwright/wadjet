package kernel

import (
	"fmt"
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// #459: the FLOAT predicate kernels compared with Go's operators, which are
// IEEE754, while ORDER BY / GROUP BY / rank() had already moved to
// PostgreSQL's float total order (NaN greatest, NaN = NaN, -0.0 = +0.0). So
// `WHERE f = f` dropped every NaN row and `WHERE f > 1e300` dropped them too,
// and the same query answered differently depending on which comparison path
// the planner chose.
//
// The reference below is deliberately NOT the implementation: it is
// CompareFloat64's three-way answer put through the operator, which is the
// definition ADR-0012 item 8 states. The kernels use cheap branch forms
// instead, and this is what keeps the two the same relation.
//
// Live PostgreSQL 17 (the authority, per ADR-0012):
//
//	'NaN'::float8 = 'NaN'::float8    -> t
//	'NaN'::float8 > 'Infinity'       -> t
//	'NaN'::float8 <> 1.0             -> t
//	1.0 < 'NaN'::float8              -> t
//	(-0.0)::float8 = 0.0::float8     -> t

var floatOps = []CompareOp{OpEq, OpNe, OpLt, OpLe, OpGt, OpGe}

// pgFloatRef is the ORDER, applied. The one definition; everything else in
// this file is checked against it.
func pgFloatRef(a, b float64, op CompareOp) bool {
	return applyCompareOp(CompareFloat64(a, b), op)
}

// floatCorpus spans the classes the order distinguishes: NaN (two payloads),
// both zeros, both infinities, and ordinary finite values.
func floatCorpus() []float64 {
	otherNaN := math.Float64frombits(math.Float64bits(math.NaN()) ^ 0xF)
	if !math.IsNaN(otherNaN) || math.Float64bits(otherNaN) == math.Float64bits(math.NaN()) {
		panic("fixture: expected a second, differently-payloaded NaN")
	}
	return []float64{
		math.NaN(), otherNaN, math.Inf(1), math.Inf(-1),
		0, math.Copysign(0, -1), 1, -1, 1e300, -1e300, 1e-300,
	}
}

func TestFloatPredicatesFollowPostgresOrder(t *testing.T) {
	corpus := floatCorpus()
	for _, op := range floatOps {
		for _, a := range corpus {
			for _, b := range corpus {
				want := pgFloatRef(a, b, op)
				if got := FloatCompareOp(a, b, op); got != want {
					t.Errorf("FloatCompareOp(%v, %v, %s) = %v, want %v", a, b, opLabel(op), got, want)
				}
				if got := FloatCompareOp(float32(a), float32(b), op); got != want {
					// float32 conversion is exact for every corpus member's
					// CLASS (NaN stays NaN, ±0 stays ±0, ±Inf stays ±Inf,
					// 1e300 saturates to +Inf but does so on both sides).
					if !float32ClassAgrees(a, b) {
						continue
					}
					t.Errorf("FloatCompareOp[float32](%v, %v, %s) = %v, want %v", a, b, opLabel(op), got, want)
				}
			}
		}
	}
}

// float32ClassAgrees reports whether narrowing both operands to float32 keeps
// their ORDER — it does not for 1e300 (which saturates to +Inf) against
// +Inf, and for 1e-300 (which flushes to 0) against 0.
func float32ClassAgrees(a, b float64) bool {
	na, nb := float64(float32(a)), float64(float32(b))
	return CompareFloat64(na, nb) == CompareFloat64(a, b)
}

// TestFloatFilterKernelAgainstConstant sweeps the vectorized col-const kernel
// (the WHERE path and the scan's predicate pushdown) over the same corpus, for
// FLOAT64 and FLOAT32, with and without a selection vector and a null mask.
func TestFloatFilterKernelAgainstConstant(t *testing.T) {
	corpus := floatCorpus()
	for _, op := range floatOps {
		for _, konst := range corpus {
			t.Run(fmt.Sprintf("f64_%s_%v", opLabel(op), konst), func(t *testing.T) {
				vec := batch.NewVector(batch.TypeFloat64, len(corpus))
				copy(vec.Float64Data, corpus)
				k := ResolveFilterKernel(batch.TypeFloat64, op, konst)
				if k == nil {
					t.Fatal("no kernel")
				}
				got := k(vec, nil, len(corpus), make([]uint32, 0, len(corpus)))
				var want []uint32
				for i, v := range corpus {
					if pgFloatRef(v, konst, op) {
						want = append(want, uint32(i))
					}
				}
				assertSel(t, got, want, corpus, konst, op)

				// Selection vector: the same answer over odd rows only.
				sel := []uint32{}
				for i := range corpus {
					if i%2 == 1 {
						sel = append(sel, uint32(i))
					}
				}
				got = k(vec, sel, len(corpus), make([]uint32, 0, len(corpus)))
				want = nil
				for _, i := range sel {
					if pgFloatRef(corpus[i], konst, op) {
						want = append(want, i)
					}
				}
				assertSel(t, got, want, corpus, konst, op)

				// A NULL never satisfies a comparison, whatever the order.
				nvec := batch.NewVector(batch.TypeFloat64, len(corpus))
				copy(nvec.Float64Data, corpus)
				nvec.Nulls.SetNull(0)
				got = k(nvec, nil, len(corpus), make([]uint32, 0, len(corpus)))
				for _, idx := range got {
					if idx == 0 {
						t.Errorf("%s: a NULL row was selected", opLabel(op))
					}
				}
			})
		}
	}

	// FLOAT32, one pass: the same rule on the narrower storage.
	f32corpus := []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)),
		0, float32(math.Copysign(0, -1)), 1, -1}
	for _, op := range floatOps {
		for _, konst := range f32corpus {
			vec := batch.NewVector(batch.TypeFloat32, len(f32corpus))
			copy(vec.Float32Data, f32corpus)
			k := ResolveFilterKernel(batch.TypeFloat32, op, float64(konst))
			got := k(vec, nil, len(f32corpus), make([]uint32, 0, len(f32corpus)))
			var want []uint32
			for i, v := range f32corpus {
				if pgFloatRef(float64(v), float64(konst), op) {
					want = append(want, uint32(i))
				}
			}
			if !selEqual(got, want) {
				t.Errorf("float32 %s %v: got %v, want %v", opLabel(op), konst, got, want)
			}
		}
	}
}

// TestFloatColColFilterKernel is the `WHERE a <op> b` half — and with a = b
// the `WHERE f = f` row of #459's matrix, which PostgreSQL answers TRUE for a
// NaN and wadjet answered FALSE.
func TestFloatColColFilterKernel(t *testing.T) {
	corpus := floatCorpus()
	// Every ordered pair, laid out as two parallel columns.
	var left, right []float64
	for _, a := range corpus {
		for _, b := range corpus {
			left = append(left, a)
			right = append(right, b)
		}
	}
	n := len(left)
	lv := batch.NewVector(batch.TypeFloat64, n)
	rv := batch.NewVector(batch.TypeFloat64, n)
	copy(lv.Float64Data, left)
	copy(rv.Float64Data, right)

	for _, op := range floatOps {
		k := ResolveColColFilterKernel(batch.TypeFloat64, op)
		if k == nil {
			t.Fatal("no col-col kernel for FLOAT64")
		}
		got := k(lv, rv, nil, n, make([]uint32, 0, n))
		var want []uint32
		for i := range left {
			if pgFloatRef(left[i], right[i], op) {
				want = append(want, uint32(i))
			}
		}
		if !selEqual(got, want) {
			t.Errorf("col-col %s: %d rows selected, want %d", opLabel(op), len(got), len(want))
			for i := range left {
				w := pgFloatRef(left[i], right[i], op)
				if g := containsIdx(got, uint32(i)); g != w {
					t.Errorf("  %v %s %v = %v, want %v", left[i], opLabel(op), right[i], g, w)
				}
			}
		}
	}

	// The named row: `WHERE f = f` selects the NaN rows too.
	k := ResolveColColFilterKernel(batch.TypeFloat64, OpEq)
	self := k(lv, lv, nil, n, make([]uint32, 0, n))
	if len(self) != n {
		t.Errorf("`f = f` selected %d of %d rows; every non-null row equals itself in PostgreSQL, NaN included", len(self), n)
	}
}

// TestFloatInFilterMatchesNaN: a Go map cannot hold a reachable NaN key, so an
// IN list containing NaN could never match a NaN column value. PostgreSQL's
// `f IN ('NaN')` selects them.
func TestFloatInFilterMatchesNaN(t *testing.T) {
	corpus := floatCorpus()
	for _, negate := range []bool{false, true} {
		for _, list := range [][]any{
			{math.NaN()},
			{1.0, math.NaN()},
			{1.0, -1.0},
			{math.Copysign(0, -1)},
			{0.0},
		} {
			vec := batch.NewVector(batch.TypeFloat64, len(corpus))
			copy(vec.Float64Data, corpus)
			k := ResolveInFilterKernel(batch.TypeFloat64, list, negate)
			got := k(vec, nil, len(corpus), make([]uint32, 0, len(corpus)))
			var want []uint32
			for i, v := range corpus {
				member := false
				for _, e := range list {
					if CompareFloat64(v, e.(float64)) == 0 {
						member = true
						break
					}
				}
				if member != negate {
					want = append(want, uint32(i))
				}
			}
			if !selEqual(got, want) {
				t.Errorf("IN %v negate=%v: got %v, want %v (corpus %v)", list, negate, got, want, corpus)
			}

			// FLOAT32 storage probes the same set.
			f32 := batch.NewVector(batch.TypeFloat32, len(corpus))
			for i, v := range corpus {
				f32.Float32Data[i] = float32(v)
			}
			k32 := ResolveInFilterKernel(batch.TypeFloat32, list, negate)
			got32 := k32(f32, nil, len(corpus), make([]uint32, 0, len(corpus)))
			want = nil
			for i, v := range corpus {
				member := false
				for _, e := range list {
					if CompareFloat64(float64(float32(v)), e.(float64)) == 0 {
						member = true
						break
					}
				}
				if member != negate {
					want = append(want, uint32(i))
				}
			}
			if !selEqual(got32, want) {
				t.Errorf("float32 IN %v negate=%v: got %v, want %v", list, negate, got32, want)
			}
		}
	}
}

// TestCanonicalFloatBitsAgreeWithTheComparator is the key half of the same
// rule: two values the comparator calls equal must serialize alike, or a
// GROUP BY splits a group that ORDER BY puts in one peer set.
func TestCanonicalFloatBitsAgreeWithTheComparator(t *testing.T) {
	corpus := floatCorpus()
	for _, a := range corpus {
		for _, b := range corpus {
			equal := CompareFloat64(a, b) == 0
			sameBits := KeyFloat64Bits(a) == KeyFloat64Bits(b)
			if equal != sameBits {
				t.Errorf("CompareFloat64(%v,%v)==0 is %v but KeyFloat64Bits agreement is %v", a, b, equal, sameBits)
			}
			e32 := CompareFloat32(float32(a), float32(b)) == 0
			s32 := KeyFloat32Bits(float32(a)) == KeyFloat32Bits(float32(b))
			if e32 != s32 {
				t.Errorf("CompareFloat32(%v,%v)==0 is %v but KeyFloat32Bits agreement is %v", a, b, e32, s32)
			}
		}
	}
}

func assertSel(t *testing.T, got, want []uint32, corpus []float64, konst float64, op CompareOp) {
	t.Helper()
	if selEqual(got, want) {
		return
	}
	t.Errorf("%s %v: got %v, want %v", opLabel(op), konst, got, want)
	for i, v := range corpus {
		w := pgFloatRef(v, konst, op)
		if g := containsIdx(got, uint32(i)); g != w {
			t.Errorf("  row %d (%v): got %v, want %v", i, v, g, w)
		}
	}
}

func selEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsIdx(s []uint32, i uint32) bool {
	for _, v := range s {
		if v == i {
			return true
		}
	}
	return false
}
