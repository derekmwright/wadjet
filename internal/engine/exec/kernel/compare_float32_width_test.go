package kernel

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestCompareFilterFloat32WidensLikePostgres is the regression for #631.
//
// `real <op> <numeric literal>` NARROWED the literal to float32 and compared
// at real width. PostgreSQL has no `real <op> numeric` operator to resolve to,
// so it resolves the comparison through float8 and the COLUMN is the side that
// moves (EXPLAIN VERBOSE on postgres:17):
//
//	real = 3.1  ->  Filter: (r_val = '3.1'::double precision)
//
// Over a column holding real(i)+0.1 the two readings are different predicates.
// Every want below is PostgreSQL 17's answer over exactly these values:
//
//	CREATE TABLE rp (r_key bigint, r_val real);
//	INSERT INTO rp SELECT i, i+0.1 FROM generate_series(0,7) i;
//	INSERT INTO rp VALUES (8,0.5),(9,1.5),(10,0),(11,16777216);
//
// The pre-fix narrowing answered `= 3.1` with row 3, `<= 3.1` and `>= 3.1`
// with row 3 on BOTH sides, and `< 3.1` without it — four of the six operators
// moved a row, which is why this is not only an equality question.
func TestCompareFilterFloat32WidensLikePostgres(t *testing.T) {
	// Rows 0..7 hold i+0.1 (not representable in float32); 8..9 hold 0.5 and
	// 1.5 (exact); 10 holds 0.0; 11 holds 2^24, the first integer real cannot
	// follow.
	vec := float32Vec(t,
		float32(0)+0.1, float32(1)+0.1, float32(2)+0.1, float32(3)+0.1,
		float32(4)+0.1, float32(5)+0.1, float32(6)+0.1, float32(7)+0.1,
		0.5, 1.5, 0, 16777216,
	)

	cases := []struct {
		name string
		op   CompareOp
		lit  any
		want []uint32
	}{
		// The non-representable literal, all six operators. float32(3)+0.1
		// widens to 3.0999999046325684, which is BELOW 3.1 — so row 3 leaves
		// `=`, joins `<` and `<=`, and leaves `>=`.
		{"eq non-representable", OpEq, 3.1, nil},
		{"ne non-representable", OpNe, 3.1, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		{"lt non-representable", OpLt, 3.1, []uint32{0, 1, 2, 3, 8, 9, 10}},
		{"le non-representable", OpLe, 3.1, []uint32{0, 1, 2, 3, 8, 9, 10}},
		{"gt non-representable", OpGt, 3.1, []uint32{4, 5, 6, 7, 11}},
		{"ge non-representable", OpGe, 3.1, []uint32{4, 5, 6, 7, 11}},
		// An exactly-representable literal answers the same at either width —
		// the case that masked the defect for years.
		{"eq representable", OpEq, 1.5, []uint32{9}},
		{"eq zero", OpEq, 0.0, []uint32{10}},
		// An INTEGER literal is widened too: PostgreSQL plans `r_val = 3` as
		// '3'::double precision. 16777217 is exact in double and not in real,
		// so it matches nothing; narrowing would round it onto row 11.
		{"eq integer past mantissa", OpEq, int64(16777217), nil},
		{"eq integer exact", OpEq, int64(16777216), []uint32{11}},
		{"gt integer past mantissa", OpGt, int64(16777217), nil},
		{"lt integer past mantissa", OpLt, int64(16777217), []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		// A finite literal past real's range is an ordinary double here: it
		// equals nothing, everything is below it, and PostgreSQL raises no
		// error (the 22003 belongs to the multi-element IN, which casts the
		// array to real[] — #549).
		{"eq over range", OpEq, 1e40, nil},
		{"lt over range", OpLt, 1e40, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		{"gt over range", OpGt, 1e40, nil},
		{"gt negative over range", OpGt, -1e40, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kern := ResolveFilterKernel(batch.TypeFloat32, c.op, c.lit)
			if kern == nil {
				t.Fatalf("no kernel for %v %v", c.op, c.lit)
			}
			got := kern(vec, nil, vec.Len, make([]uint32, 0, vec.Len))
			if !setEqual(asSet(got), asSet(c.want)) {
				t.Errorf("got %v, want %v (PostgreSQL 17)", got, c.want)
			}
		})
	}
}

// TestCompareFilterFloat32WidenKeepsPostgresFloatOrder checks that widening did
// not cost the FLOAT total order #459 established: NaN is the greatest value
// and equal to itself, ±0.0 are one value, and ±Inf order normally. The
// widening conversion preserves every one of those, but the kernel is a
// separate loop from compareFilterFloat and so needs its own proof.
//
// Wants are PostgreSQL 17's over a real column holding
// {NaN, 0.0, -0.0, Infinity, -Infinity, 1.0, 2.0}.
func TestCompareFilterFloat32WidenKeepsPostgresFloatOrder(t *testing.T) {
	nan := float32(math.NaN())
	negZero := float32(0)
	negZero = -negZero
	vec := float32Vec(t, nan, 0, negZero, float32(math.Inf(1)), float32(math.Inf(-1)), 1, 2)

	cases := []struct {
		name string
		op   CompareOp
		lit  any
		want []uint32
	}{
		{"gt 1e300 admits NaN and +Inf", OpGt, 1e300, []uint32{0, 3}},
		{"ge 1e300 admits NaN and +Inf", OpGe, 1e300, []uint32{0, 3}},
		{"lt 1e300 excludes NaN", OpLt, 1e300, []uint32{1, 2, 4, 5, 6}},
		{"ne 1e300 admits NaN", OpNe, 1e300, []uint32{0, 1, 2, 3, 4, 5, 6}},
		{"eq NaN selects the NaN row", OpEq, math.NaN(), []uint32{0}},
		{"le NaN selects everything", OpLe, math.NaN(), []uint32{0, 1, 2, 3, 4, 5, 6}},
		{"gt NaN selects nothing", OpGt, math.NaN(), nil},
		{"ge NaN selects the NaN row", OpGe, math.NaN(), []uint32{0}},
		{"eq zero folds -0.0", OpEq, 0.0, []uint32{1, 2}},
		{"eq +Inf", OpEq, math.Inf(1), []uint32{3}},
		{"eq -Inf", OpEq, math.Inf(-1), []uint32{4}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kern := ResolveFilterKernel(batch.TypeFloat32, c.op, c.lit)
			if kern == nil {
				t.Fatalf("no kernel for %v %v", c.op, c.lit)
			}
			got := kern(vec, nil, vec.Len, make([]uint32, 0, vec.Len))
			if !setEqual(asSet(got), asSet(c.want)) {
				t.Errorf("got %v, want %v (PostgreSQL 17)", got, c.want)
			}
		})
	}
}

// TestCompareFilterFloat32WidenHonorsNullsAndSelection exercises the three
// loop shapes the kernel carries — with and without a selection vector, with
// and without nulls — since a widening kernel that got one of them wrong would
// otherwise pass every test above (they all take the no-sel, no-null loop).
func TestCompareFilterFloat32WidenHonorsNullsAndSelection(t *testing.T) {
	vec := float32Vec(t, float32(0)+0.1, 1.5, float32(2)+0.1, 1.5, 9.5)
	vec.Nulls.SetNull(2)

	// `= 1.5` over rows {0.1, 1.5, NULL, 1.5, 9.5}: PostgreSQL answers rows 1
	// and 3, and a NULL row satisfies no predicate.
	kern := ResolveFilterKernel(batch.TypeFloat32, OpEq, 1.5)
	if got := kern(vec, nil, vec.Len, make([]uint32, 0, vec.Len)); !setEqual(asSet(got), asSet([]uint32{1, 3})) {
		t.Errorf("no selection, with nulls: got %v, want [1 3]", got)
	}
	// The same predicate behind a prior filter's selection vector, which must
	// be intersected rather than ignored.
	sel := []uint32{2, 3, 4}
	if got := kern(vec, sel, vec.Len, make([]uint32, 0, vec.Len)); !setEqual(asSet(got), asSet([]uint32{3})) {
		t.Errorf("with selection, with nulls: got %v, want [3]", got)
	}

	// A NaN constant takes the kernel's other branch (resolveFloatConstPred's
	// one-argument form), so drive both loop shapes through it too. `<> NaN`
	// is TRUE for every non-NaN row and there is no NaN here.
	ne := ResolveFilterKernel(batch.TypeFloat32, OpNe, math.NaN())
	if got := ne(vec, nil, vec.Len, make([]uint32, 0, vec.Len)); !setEqual(asSet(got), asSet([]uint32{0, 1, 3, 4})) {
		t.Errorf("NaN constant, no selection: got %v, want [0 1 3 4]", got)
	}
	if got := ne(vec, sel, vec.Len, make([]uint32, 0, vec.Len)); !setEqual(asSet(got), asSet([]uint32{3, 4})) {
		t.Errorf("NaN constant, with selection: got %v, want [3 4]", got)
	}
}

// TestFloat32ScalarAndMultiInDisagreeOnPurpose pins the one property that
// forbids lowering `real IN (...)` to a chain of `=`, and vice versa: on the
// SAME literal, PostgreSQL's scalar comparison widens and its multi-element IN
// narrows, so they answer differently.
//
//	SELECT count(*) FROM rp WHERE r_val =  16777217        -> 0
//	SELECT count(*) FROM rp WHERE r_val IN (16777217, 99)  -> 1   (row 2^24)
//
// Both verified on postgres:17. A future simplification that folds either into
// the other fails here.
func TestFloat32ScalarAndMultiInDisagreeOnPurpose(t *testing.T) {
	vec := float32Vec(t, 16777216, 1.5)

	eq := ResolveFilterKernel(batch.TypeFloat32, OpEq, int64(16777217))
	if got := eq(vec, nil, vec.Len, make([]uint32, 0, vec.Len)); len(got) != 0 {
		t.Errorf("scalar `= 16777217` selected %v, want none — PostgreSQL widens", got)
	}

	in := ResolveInFilterKernel(batch.TypeFloat32, []any{int64(16777217), int64(99)}, false)
	if in == nil {
		t.Fatal("no IN kernel")
	}
	if got := in(vec, nil, vec.Len, make([]uint32, 0, vec.Len)); !setEqual(asSet(got), asSet([]uint32{0})) {
		t.Errorf("multi-element `IN (16777217, 99)` selected %v, want [0] — PostgreSQL narrows to real[]", got)
	}
}
