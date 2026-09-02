package kernel

import (
	"math"
	"testing"
)

// An INTEGER column against a numeric constant that no integer equals (#704).
//
// `int64(3.5)` is 3 and `int64(-0.5)` is 0 — Go truncates toward zero — so the
// filter kernels compared against a value a row could hold: `c = 3.5` matched
// the row holding 3 and `c = -0.5` the row holding 0, silently, on the
// vectorized path and the ColumnCompareLit row path both. PostgreSQL compares
// `bigint = numeric` exactly and answers no rows.
//
// The table is the whole contract, per operator and both signs: an equality
// has no rows, an inequality has all of them, and an ordering compares against
// FLOOR with the operator tightened (`c >= 3.5` is `c > 3`, not `c >= 3`).
// The integral cases must be untouched — that is the "nothing but the intended
// class moved" half (correctness-fix protocol rule 4).
func TestIntFilterBoundAnswersAFractionalConstant(t *testing.T) {
	for _, tc := range []struct {
		name    string
		v       any
		op      CompareOp
		wantN   int64
		wantOp  CompareOp
		wantVer IntBoundVerdict
	}{
		// A fraction: no integer equals it.
		{"eq_positive_fraction", 3.5, OpEq, 0, OpEq, IntBoundNone},
		{"ne_positive_fraction", 3.5, OpNe, 0, OpNe, IntBoundAll},
		{"gt_positive_fraction", 3.5, OpGt, 3, OpGt, IntBoundCompare},
		{"ge_positive_fraction", 3.5, OpGe, 3, OpGt, IntBoundCompare},
		{"lt_positive_fraction", 3.5, OpLt, 3, OpLe, IntBoundCompare},
		{"le_positive_fraction", 3.5, OpLe, 3, OpLe, IntBoundCompare},
		// The NEGATIVE half, which truncation got wrong in the other
		// direction: floor(-0.5) is -1, int64(-0.5) is 0.
		{"eq_negative_fraction", -0.5, OpEq, 0, OpEq, IntBoundNone},
		{"gt_negative_fraction", -0.5, OpGt, -1, OpGt, IntBoundCompare},
		{"ge_negative_fraction", -0.5, OpGe, -1, OpGt, IntBoundCompare},
		{"lt_negative_fraction", -0.5, OpLt, -1, OpLe, IntBoundCompare},
		{"le_negative_fraction", -0.5, OpLe, -1, OpLe, IntBoundCompare},
		// An INTEGRAL float is unchanged, whatever its spelling.
		{"eq_integral_float", 3.0, OpEq, 3, OpEq, IntBoundCompare},
		{"ge_integral_float", 3.0, OpGe, 3, OpGe, IntBoundCompare},
		{"eq_exponent_integral", 1e2, OpEq, 100, OpEq, IntBoundCompare},
		{"eq_negative_integral", -3.0, OpEq, -3, OpEq, IntBoundCompare},
		// Every non-float box is Int64FilterConst's own answer, operator
		// included: the ordinary path is exactly what it was.
		{"int64_box", int64(7), OpGe, 7, OpGe, IntBoundCompare},
		{"int_box", 7, OpEq, 7, OpEq, IntBoundCompare},
		{"text_box", "7", OpLt, 7, OpLt, IntBoundCompare},
		// Past the carrier the verdict is the whole column's, which is also
		// how the infinities are answered — no arm of their own.
		{"eq_above_int64", 1e30, OpEq, 0, OpEq, IntBoundNone},
		{"lt_above_int64", 1e30, OpLt, 0, OpLt, IntBoundAll},
		{"gt_above_int64", 1e30, OpGt, 0, OpGt, IntBoundNone},
		{"lt_positive_infinity", math.Inf(1), OpLt, 0, OpLt, IntBoundAll},
		{"gt_negative_infinity", math.Inf(-1), OpGt, 0, OpGt, IntBoundAll},
		{"lt_negative_infinity", math.Inf(-1), OpLt, 0, OpLt, IntBoundNone},
		{"eq_below_int64", -1e30, OpEq, 0, OpEq, IntBoundNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, op, verdict, st := IntFilterBound(tc.v, tc.op)
			if st != IntConstOK {
				t.Fatalf("status %v, want IntConstOK", st)
			}
			if verdict != tc.wantVer {
				t.Errorf("verdict %v, want %v", verdict, tc.wantVer)
			}
			if verdict != IntBoundCompare {
				return // n and op are unread for a whole-column verdict
			}
			if n != tc.wantN || op != tc.wantOp {
				t.Errorf("(%v, %v), want (%v, %v)", n, op, tc.wantN, tc.wantOp)
			}
		})
	}

	// A NaN constant declines rather than comparing against Go's
	// implementation-defined float->int conversion. No SQL spelling reaches
	// this: a quoted 'NaN' is read by the integer grammar and refused there.
	if _, _, _, st := IntFilterBound(math.NaN(), OpEq); st != IntConstSyntax {
		t.Errorf("NaN: status %v, want IntConstSyntax", st)
	}
}

// The int32-backed narrowing keeps #536's refusal for every box that is not a
// number, and answers a NUMBER outside int32 with a verdict — `c_i32 > 3.5e9`
// names no int32 and no int32 satisfies it, which is a fact about the column
// rather than an error about the literal.
func TestInt32FilterBoundNarrowsWithoutLosingTheRefusal(t *testing.T) {
	if _, _, _, st := Int32FilterBound("3000000000", OpEq); st != IntConstRange {
		t.Errorf("quoted out-of-range literal: status %v, want IntConstRange (#536)", st)
	}
	if _, _, _, st := Int32FilterBound(int64(3000000000), OpEq); st != IntConstRange {
		t.Errorf("integer box out of range: status %v, want IntConstRange (#536)", st)
	}
	for _, tc := range []struct {
		name string
		op   CompareOp
		want IntBoundVerdict
	}{
		{"gt", OpGt, IntBoundNone},
		{"lt", OpLt, IntBoundAll},
		{"eq", OpEq, IntBoundNone},
		{"ne", OpNe, IntBoundAll},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, verdict, st := Int32FilterBound(3.5e9, tc.op)
			if st != IntConstOK {
				t.Fatalf("status %v, want IntConstOK", st)
			}
			if verdict != tc.want {
				t.Errorf("verdict %v, want %v", verdict, tc.want)
			}
		})
	}
	// And a fraction inside int32's range takes the ordinary rewrite.
	n, op, verdict, st := Int32FilterBound(3.5, OpGe)
	if st != IntConstOK || verdict != IntBoundCompare || n != 3 || op != OpGt {
		t.Errorf("int32 >= 3.5: (%v, %v, %v, %v), want (3, OpGt, IntBoundCompare, IntConstOK)", n, op, verdict, st)
	}
}
