package kernel

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

func decimalVec(t *testing.T, scale int, vals ...string) *batch.Vector {
	t.Helper()
	v := batch.NewVectorWithScale(batch.TypeDecimal, len(vals), scale)
	for i, s := range vals {
		v.DecimalData.Data[i] = batch.ParseDecimalString(s, scale)
	}
	v.Len = len(vals)
	return v
}

// TestInFilterDecimalMemoIsKeyedByScale guards the per-batch literal set that
// inFilterDecimal now builds once instead of per call.
//
// The set is a function of the COLUMN's scale — a literal that does not fit
// the scale exactly equals nothing and drops out — so caching it
// unconditionally would answer a 4-scale column from a 2-scale column's set.
// One kernel over two scales is the case that catches it.
func TestInFilterDecimalMemoIsKeyedByScale(t *testing.T) {
	kern := ResolveInFilterKernel(batch.TypeDecimal, []any{"1.50", "2.25"}, false)
	if kern == nil {
		t.Fatal("no DECIMAL IN kernel")
	}

	// Scale 2: both literals are representable; 1.50 and 2.25 match.
	v2 := decimalVec(t, 2, "1.50", "2.25", "3.00")
	if got := kern(v2, nil, v2.Len, nil); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("scale 2: got %v, want [0 1]", got)
	}
	// Same kernel, again: the memoized set must give the same answer.
	if got := kern(v2, nil, v2.Len, nil); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("scale 2 second call: got %v, want [0 1] — the memo changed the answer", got)
	}
	// A DIFFERENT scale through the SAME kernel: the literals rescale, and
	// the values that match are the same ones.
	v4 := decimalVec(t, 4, "1.5000", "2.2500", "3.0000")
	if got := kern(v4, nil, v4.Len, nil); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("scale 4: got %v, want [0 1] — the memo is not keyed by scale", got)
	}
	// Back to scale 2, to catch a memo that only re-keys forward.
	if got := kern(v2, nil, v2.Len, nil); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("scale 2 after scale 4: got %v, want [0 1]", got)
	}

	// A literal the column's scale cannot hold exactly matches nothing, and
	// that verdict must also be re-derived per scale: 1.005 is representable
	// at scale 4 and truncates at scale 2.
	kern = ResolveInFilterKernel(batch.TypeDecimal, []any{"1.0050"}, false)
	if got := kern(v2, nil, v2.Len, nil); len(got) != 0 {
		t.Fatalf("scale 2 with an unrepresentable literal: got %v, want none", got)
	}
	v4b := decimalVec(t, 4, "1.0050", "9.0000")
	if got := kern(v4b, nil, v4b.Len, nil); len(got) != 1 || got[0] != 0 {
		t.Fatalf("scale 4 with the same literal: got %v, want [0] — the scale-2 verdict was cached", got)
	}
}
