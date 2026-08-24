package kernel

import (
	"math/big"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// #477: ResolveColColFilterKernel had no DECIMAL arm, so it returned nil.
// ColColFilter attaches its row-at-a-time fallback only when the two column
// TYPES differ, and two DECIMALs share a TypeID — so there was nothing to fall
// back to and `WHERE d_2 = d_4` failed the query for every operator.

func TestColColFilterDecimalSameScale(t *testing.T) {
	left := decimalVec(t, 2, "1.50", "2.00", "-3.25", "0.00")
	right := decimalVec(t, 2, "1.50", "1.99", "-3.26", "0.01")
	for _, tc := range []struct {
		op   CompareOp
		want []uint32
	}{
		{OpEq, []uint32{0}},
		{OpNe, []uint32{1, 2, 3}},
		{OpLt, []uint32{3}},
		{OpLe, []uint32{0, 3}},
		{OpGt, []uint32{1, 2}},
		{OpGe, []uint32{0, 1, 2}},
	} {
		kern := ResolveColColFilterKernel(batch.TypeDecimal, tc.op)
		if kern == nil {
			t.Fatalf("op %d: no DECIMAL col-col kernel", tc.op)
		}
		got := kern(left, right, nil, 4, make([]uint32, 0, 4))
		if !sameSel(got, tc.want) {
			t.Errorf("op %d: selected %v, want %v", tc.op, got, tc.want)
		}
	}
}

// The two columns need not share a scale, and "1.50" and "1.5000" are the same
// number: the comparison rescales rather than reading the unscaled integers
// as if they were commensurable.
func TestColColFilterDecimalCrossScale(t *testing.T) {
	left := decimalVec(t, 2, "1.50", "2.00", "-3.25")
	right := decimalVec(t, 4, "1.5000", "1.9999", "-3.2501")
	kern := ResolveColColFilterKernel(batch.TypeDecimal, OpEq)
	if got := kern(left, right, nil, 3, make([]uint32, 0, 3)); !sameSel(got, []uint32{0}) {
		t.Errorf("cross-scale equality selected %v, want [0]", got)
	}
	kern = ResolveColColFilterKernel(batch.TypeDecimal, OpGt)
	if got := kern(left, right, nil, 3, make([]uint32, 0, 3)); !sameSel(got, []uint32{1, 2}) {
		t.Errorf("cross-scale > selected %v, want [1 2]", got)
	}
	// Reading the unscaled integers directly would say 150 > 15000 is false
	// AND 150 = 15000 is false, so equality alone cannot catch the defect.
	kern = ResolveColColFilterKernel(batch.TypeDecimal, OpLt)
	if got := kern(left, right, nil, 3, make([]uint32, 0, 3)); !sameSel(got, nil) {
		t.Errorf("cross-scale < selected %v, want none", got)
	}
}

// The rescale that does not fit Int128 still answers exactly, through big.Int.
func TestColColFilterDecimalCrossScaleOverflow(t *testing.T) {
	// A near-full-width unscaled value at scale 0 against scale 30: rescaling
	// by 10^30 has no Int128.
	wide := new(big.Int)
	wide.SetString("170141183460469231731687303715884105000", 10)
	left := batch.NewVectorWithScale(batch.TypeDecimal, 1, 0)
	left.DecimalData.Data[0] = batch.ParseDecimalString(wide.String(), 0)
	left.Len = 1
	right := decimalVec(t, 30, "1.0")
	if got := ResolveColColFilterKernel(batch.TypeDecimal, OpGt)(
		left, right, nil, 1, make([]uint32, 0, 1)); !sameSel(got, []uint32{0}) {
		t.Errorf("overflowing rescale selected %v, want [0]", got)
	}
}

// A NULL on either side makes the comparison UNKNOWN, and a WHERE admits only
// TRUE — the same rule every other col-col kernel follows.
func TestColColFilterDecimalNullsAndSelection(t *testing.T) {
	left := decimalVec(t, 2, "1.50", "2.00", "3.00", "4.00")
	right := decimalVec(t, 2, "1.50", "2.00", "3.00", "4.00")
	left.Nulls.SetNull(1)
	right.Nulls.SetNull(2)
	kern := ResolveColColFilterKernel(batch.TypeDecimal, OpEq)
	if got := kern(left, right, nil, 4, make([]uint32, 0, 4)); !sameSel(got, []uint32{0, 3}) {
		t.Errorf("with nulls selected %v, want [0 3]", got)
	}
	// A prior selection vector is honoured and compacted, not ignored.
	if got := kern(left, right, []uint32{1, 3}, 4, make([]uint32, 0, 4)); !sameSel(got, []uint32{3}) {
		t.Errorf("with a selection vector selected %v, want [3]", got)
	}
}

func sameSel(got, want []uint32) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
