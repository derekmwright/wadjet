package kernel

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

func float32Vec(t *testing.T, vals ...float32) *batch.Vector {
	t.Helper()
	v := batch.NewVector(batch.TypeFloat32, len(vals))
	for i, f := range vals {
		v.Float32Data[i] = f
	}
	v.Len = len(vals)
	return v
}

func asSet(idxs []uint32) map[uint32]struct{} {
	s := make(map[uint32]struct{}, len(idxs))
	for _, i := range idxs {
		s[i] = struct{}{}
	}
	return s
}

func setEqual(a, b map[uint32]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// TestInFilterFloat32MatchesPostgresRealArray is the regression for #549. A
// multi-element `real IN (...)` compared at float64 width against float64
// literals, so a literal not exactly representable in float32 (0.1) matched no
// row — `f IN (0.1, …)` returned nothing where the row was present.
//
// The expected values are PostgreSQL's, taken from postgres:17 over a `real`
// column holding real(i)+0.1: PostgreSQL builds `real = ANY(real[])` for a
// multi-element list — the literals cast to real, NOT the column widened to
// double (EXPLAIN VERBOSE) — so 0.1 in the list narrows to the same real the
// column holds and the row matches.
//
// The test deliberately does NOT assert `IN == OR-of-equals`. PostgreSQL's
// scalar `real = 0.1` WIDENS to double and returns 0 rows, so IN and `=`
// legitimately DISAGREE for real in PostgreSQL; wadjet's `=` narrows and is a
// separate divergence (#631). Pinning the two kernels as equal here would
// defend the wrong behavior the day #631 is fixed.
func TestInFilterFloat32MatchesPostgresRealArray(t *testing.T) {
	// Rows 0..7 hold i+0.1 (non-representable); 8..10 hold 0.5, 1.5, 2.5.
	vec := float32Vec(t,
		float32(0)+0.1, float32(1)+0.1, float32(2)+0.1, float32(3)+0.1,
		float32(4)+0.1, float32(5)+0.1, float32(6)+0.1, float32(7)+0.1,
		0.5, 1.5, 2.5,
	)

	cases := []struct {
		name string
		lits []any
		want []uint32 // PostgreSQL 17's answer for real IN (lits)
	}{
		{"non-representable present values match",
			[]any{0.1, 1.1, 2.1, 3.1, 4.1, 5.1, 6.1, 7.1},
			[]uint32{0, 1, 2, 3, 4, 5, 6, 7}},
		{"exactly-representable values match",
			[]any{0.5, 1.5, 2.5}, []uint32{8, 9, 10}},
		{"mixed representable and non-representable",
			[]any{0.1, 3.1, 1.5}, []uint32{0, 3, 9}},
		{"absent values match nothing",
			[]any{100.1, 200.5, 0.15}, nil},
		{"present and absent together keep only the present",
			[]any{2.1, 999.9, 5.1}, []uint32{2, 5}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kern := ResolveInFilterKernel(batch.TypeFloat32, tc.lits, false)
			if kern == nil {
				t.Fatal("no FLOAT32 IN kernel")
			}
			got := kern(vec, nil, vec.Len, nil)
			if !setEqual(asSet(got), asSet(tc.want)) {
				t.Fatalf("IN kept %v, want %v (PostgreSQL 17)", got, tc.want)
			}
		})
	}
}

// TestInFilterFloat32EdgeValues pins the special float32 values against
// PostgreSQL 17's `real IN (...)`: NaN matches NaN (PostgreSQL's NaN = NaN is
// TRUE), ±Inf are legal real values that match their rows, +0.0 and -0.0 are
// one value, and two literals that narrow to the same float32 collapse.
func TestInFilterFloat32EdgeValues(t *testing.T) {
	negZero := float32(math.Copysign(0, -1))
	// id: 0 NaN, 1 +Inf, 2 -Inf, 3 +0.0, 4 -0.0, 5 1.5, 6 0.1
	vec := float32Vec(t,
		float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)),
		0.0, negZero, 1.5, float32(0)+0.1,
	)

	cases := []struct {
		name string
		lits []any
		want []uint32
	}{
		{"NaN literal matches the NaN row", []any{math.NaN(), 2.5}, []uint32{0}},
		{"+Inf literal matches the +Inf row", []any{math.Inf(1), 2.5}, []uint32{1}},
		{"-Inf literal matches the -Inf row", []any{math.Inf(-1), 2.5}, []uint32{2}},
		{"both infinities match both rows", []any{math.Inf(1), math.Inf(-1)}, []uint32{1, 2}},
		{"+0.0 literal matches both signed zeros", []any{0.0, 2.5}, []uint32{3, 4}},
		{"-0.0 literal matches both signed zeros", []any{math.Copysign(0, -1), 2.5}, []uint32{3, 4}},
		// 0.1 and 0.10000001 both narrow to float32(0.1); the set dedups them.
		{"two literals collapsing to one float32", []any{0.1, 0.10000001, 1.5}, []uint32{5, 6}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kern := ResolveInFilterKernel(batch.TypeFloat32, tc.lits, false)
			if kern == nil {
				t.Fatal("no FLOAT32 IN kernel")
			}
			got := kern(vec, nil, vec.Len, nil)
			if !setEqual(asSet(got), asSet(tc.want)) {
				t.Fatalf("IN kept %v, want %v (PostgreSQL 17)", got, tc.want)
			}
		})
	}
}

// TestInFilterFloat32NegateComplement checks NOT IN keeps the complement of IN
// over the non-null rows, still at float32 width.
func TestInFilterFloat32NegateComplement(t *testing.T) {
	vec := float32Vec(t, float32(0)+0.1, float32(1)+0.1, float32(2)+0.1, 0.5, 1.5)
	lits := []any{0.1, 1.1} // present, non-representable

	in := asSet(ResolveInFilterKernel(batch.TypeFloat32, lits, false)(vec, nil, vec.Len, nil))
	notIn := asSet(ResolveInFilterKernel(batch.TypeFloat32, lits, true)(vec, nil, vec.Len, nil))
	if !setEqual(in, map[uint32]struct{}{0: {}, 1: {}}) {
		t.Fatalf("IN kept %v, want {0,1}", in)
	}
	if !setEqual(notIn, map[uint32]struct{}{2: {}, 3: {}, 4: {}}) {
		t.Fatalf("NOT IN kept %v, want {2,3,4}", notIn)
	}
}

// TestInFilterFloat32OverflowDeclines guards the false positive the #549 fix
// could otherwise introduce in a MULTI-element list: a finite literal past
// real's range narrows to +Inf, and inserting that would make `real IN (1e40,
// 1.5)` MATCH a genuine +Inf row. PostgreSQL casts the array to real[] and
// raises 22003 for the whole predicate (verified on postgres:17), so the
// kernel DECLINES (returns nil) and the caller raises the error — see
// exec.floatConstError. (A SINGLE-element `IN (1e40)` instead WIDENS to double
// and misses with no error; that arity is covered by
// TestInFilterFloat32SingleElementWidens.)
func TestInFilterFloat32OverflowDeclines(t *testing.T) {
	if kern := ResolveInFilterKernel(batch.TypeFloat32, []any{1e40, 1.5}, false); kern != nil {
		t.Fatal("overflow literal in a multi-element list built a kernel; it must decline so the caller raises 22003")
	}
	// A literal that is ITSELF ±Inf is a legal real value, not an overflow, and
	// must still build a kernel.
	if kern := ResolveInFilterKernel(batch.TypeFloat32, []any{math.Inf(1), 1.5}, false); kern == nil {
		t.Fatal("a genuine +Inf literal is a legal real value and must build a kernel")
	}
}

// TestFloat32FitOf is the unit check for the one primitive every real-width
// refusal reads: the literal declines here and in exec.floatConstError, and
// expr.Cast's REAL arm. It has to separate four answers, because PostgreSQL
// does (utils/adt/float.c, and float4in for the literal form):
//
//   - a finite value past real's range OVERFLOWS (narrowing gives +/-Inf,
//     which would MATCH a genuine infinite row);
//   - a non-zero value below real's smallest denormal UNDERFLOWS (narrowing
//     gives 0.0, which would match every row holding zero);
//   - a value that is ITSELF +/-Inf, NaN or zero is a legal real and FITS;
//   - the denormal boundary itself FITS: 1e-45 is representable, 1e-46 is not,
//     which is where PostgreSQL draws the line (`SELECT CAST(1e-45 AS real)`
//     answers 1e-45, `CAST(1e-46 AS real)` raises 22003).
func TestFloat32FitOf(t *testing.T) {
	cases := []struct {
		v    float64
		want Float32Fit
	}{
		{1e40, Float32Overflows},   // finite, past FLT_MAX -> +Inf
		{-1e40, Float32Overflows},  // finite, past -FLT_MAX -> -Inf
		{3.5e38, Float32Overflows}, // just past FLT_MAX (~3.4028e38)
		{1e-46, Float32Underflows}, // non-zero, rounds to 0.0 in real
		{-1e-46, Float32Underflows},
		{1e-50, Float32Underflows},
		// Denormal: real's smallest is about 1.4e-45, so 1e-45 survives as a
		// denormal and must NOT be refused.
		{1e-45, Float32Fits},
		{-1e-45, Float32Fits},
		{math.SmallestNonzeroFloat32, Float32Fits},
		{3.4e38, Float32Fits},          // inside real's range
		{math.MaxFloat32, Float32Fits}, // exactly the boundary is representable
		{-math.MaxFloat32, Float32Fits},
		{0.1, Float32Fits}, // ordinary
		{0, Float32Fits},   // zero is a value, not an underflow
		{math.Copysign(0, -1), Float32Fits},
		{math.Inf(1), Float32Fits},  // already Inf: a legal real value
		{math.Inf(-1), Float32Fits}, // already -Inf
		{math.NaN(), Float32Fits},   // NaN is neither direction of failure
	}
	for _, tc := range cases {
		if got := Float32FitOf(tc.v); got != tc.want {
			t.Errorf("Float32FitOf(%v) = %v, want %v", tc.v, got, tc.want)
		}
		// The boxed predicate is the same question with a box around it, and
		// the two must never disagree — exec.floatConstError reads one and
		// float32InSet the other for the SAME list.
		if got, want := Float32LitUnrepresentable(tc.v), tc.want != Float32Fits; got != want {
			t.Errorf("Float32LitUnrepresentable(%v) = %v, want %v", tc.v, got, want)
		}
	}
}

// TestInFilterFloat32UnderflowDeclines is the other direction of
// TestInFilterFloat32OverflowDeclines, and the one that was a WRONG ANSWER
// rather than a miss: float32(1e-46) is 0.0, so the list matched every row
// holding zero. The kernel must decline so the caller raises 22003, exactly as
// PostgreSQL refuses `real IN (1e-46, 3.1)`.
func TestInFilterFloat32UnderflowDeclines(t *testing.T) {
	if kern := ResolveInFilterKernel(batch.TypeFloat32, []any{1e-46, 1.5}, false); kern != nil {
		t.Fatal("underflowing literal in a multi-element list built a kernel; it must decline so the caller raises 22003")
	}
	// The representable neighbour, and a genuine zero, still build one.
	if kern := ResolveInFilterKernel(batch.TypeFloat32, []any{1e-45, 1.5}, false); kern == nil {
		t.Fatal("1e-45 is a real denormal and must build a kernel")
	}
	if kern := ResolveInFilterKernel(batch.TypeFloat32, []any{0.0, 1.5}, false); kern == nil {
		t.Fatal("zero is a legal real value and must build a kernel")
	}
}

// TestInFilterDecimalMatchesEqualsWidth answers #549's open question: DECIMAL
// does NOT have the same asymmetry. inFilterDecimal scales each literal to the
// column's scale and drops any that do not fit exactly, mirroring
// compareFilterDecimal, so IN and `=` agree at every scale (PostgreSQL's
// numeric compares exactly, no widening).
func TestInFilterDecimalMatchesEqualsWidth(t *testing.T) {
	vec := decimalVec(t, 2, "0.10", "1.10", "2.10", "1.50")
	lits := []any{"0.10", "1.10", "2.10", "1.50"}

	kern := ResolveInFilterKernel(batch.TypeDecimal, lits, false)
	if kern == nil {
		t.Fatal("no DECIMAL IN kernel")
	}
	gotIn := asSet(kern(vec, nil, vec.Len, nil))
	if !setEqual(gotIn, map[uint32]struct{}{0: {}, 1: {}, 2: {}, 3: {}}) {
		t.Fatalf("DECIMAL IN kept %v, want all four rows", gotIn)
	}
	orSet := map[uint32]struct{}{}
	for _, lit := range lits {
		k := ResolveFilterKernel(batch.TypeDecimal, OpEq, lit)
		if k == nil {
			t.Fatalf("no scalar = kernel for %v", lit)
		}
		for _, idx := range k(vec, nil, vec.Len, nil) {
			orSet[idx] = struct{}{}
		}
	}
	if !setEqual(gotIn, orSet) {
		t.Fatalf("DECIMAL IN %v disagrees with OR-of-equals %v (unlike real, DECIMAL agrees)", gotIn, orSet)
	}
}

// TestInFilterFloat32SingleElementWidens pins PostgreSQL's ARITY split (#549
// re-review): a single-element `real IN (x)` folds to `= 'x'::double
// precision` and WIDENS to double, unlike the multi-element list which narrows
// to real[]. All values verified on postgres:17.
//
//	IN (0.1)  → 0 rows  (0.1 is not representable in float32; the widened
//	                     column value differs from the double literal)
//	IN (1.5)  → matches (1.5 is exact in float32, so widening agrees)
//	IN (1e40) → 0 rows, NO error (a finite double that widens and misses; it
//	                     is never narrowed to the +Inf a real cast would raise
//	                     22003 on — that is the MULTI-element case)
func TestInFilterFloat32SingleElementWidens(t *testing.T) {
	// Rows 0..2 hold i+0.1 (non-representable); 3 holds 1.5 (representable).
	vec := float32Vec(t, float32(0)+0.1, float32(1)+0.1, float32(2)+0.1, 1.5)

	cases := []struct {
		name string
		lits []any
		want []uint32
	}{
		{"non-representable single misses (widens)", []any{0.1}, nil},
		{"representable single matches", []any{1.5}, []uint32{3}},
		{"finite over-range single misses with no error", []any{1e40}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kern := ResolveInFilterKernel(batch.TypeFloat32, tc.lits, false)
			if kern == nil {
				t.Fatal("single-element real IN must build a (widening) kernel, not decline")
			}
			got := kern(vec, nil, vec.Len, nil)
			if !setEqual(asSet(got), asSet(tc.want)) {
				t.Fatalf("IN kept %v, want %v (PostgreSQL 17, single-element widens)", got, tc.want)
			}
		})
	}
}

// TestInFilterFloat32ArityFromSyntacticCount pins the v4 fix (#549 re-review):
// the narrow/widen width is chosen from the SYNTACTIC element count, not from
// the number of literals that reach the kernel after a NULL is stripped. With a
// non-representable literal present, a syntactic arity of 2 must NARROW (match),
// while an arity of 1 must WIDEN (miss) — even though both are handed the same
// single-element []any{0.1}. Before v4 the resolver keyed off len(values) and
// both answered miss.
func TestInFilterFloat32ArityFromSyntacticCount(t *testing.T) {
	vec := float32Vec(t, float32(0)+0.1, 1.5) // row 0 = float32(0.1), row 1 = 1.5
	lits := []any{0.1}                        // the non-NULL survivor of IN (0.1, NULL)

	// Syntactic arity 2 (the source list was (0.1, NULL)): NARROW -> matches.
	narrow := ResolveInFilterKernelArity(batch.TypeFloat32, lits, false, 2)
	if got := narrow(vec, nil, vec.Len, nil); !setEqual(asSet(got), map[uint32]struct{}{0: {}}) {
		t.Fatalf("syntactic arity 2 kept %v, want {0} (must narrow like PG real = ANY(real[]))", got)
	}
	// Syntactic arity 1 (the source list was (0.1)): WIDEN -> misses.
	widen := ResolveInFilterKernelArity(batch.TypeFloat32, lits, false, 1)
	if got := widen(vec, nil, vec.Len, nil); len(got) != 0 {
		t.Fatalf("syntactic arity 1 kept %v, want none (must widen like PG = double)", got)
	}
	// An over-range literal at syntactic arity 2 declines (-> 22003 upstream);
	// at arity 1 it builds a widening kernel that simply misses (no error).
	if k := ResolveInFilterKernelArity(batch.TypeFloat32, []any{1e40}, false, 2); k != nil {
		t.Error("over-range literal at syntactic arity 2 must decline the kernel (22003)")
	}
	if k := ResolveInFilterKernelArity(batch.TypeFloat32, []any{1e40}, false, 1); k == nil {
		t.Error("over-range literal at syntactic arity 1 must widen (build a kernel), not decline")
	}
}
