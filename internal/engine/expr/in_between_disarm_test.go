package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// stringColBatch is one STRING column s with the given values, used to
// exercise the boxedPair rules keyed off a TEXT column declaration.
func stringColBatch(values ...string) *batch.RecordBatch {
	schema := []parquet.Column{{Name: "s", Type: parquet.TypeString}}
	b := batch.NewRecordBatch(schema, len(values))
	for i, v := range values {
		b.Columns[0].SetValue(i, v)
	}
	b.Len = len(values)
	return b
}

// TestInMixedArmedDisarmedPairsAppliesEachPairsOwnRule pins the mixed case
// pairSetState's node-level disarm cache (added alongside the In/Between
// hoist) must not collapse into: an IN list where SOME members carry a
// declaration-driven rule and others do not.
//
// "s IN ('bogus', 1.50)" over a STRING column pairs two different kinds
// against s: 'bogus' is a QUOTED string literal (boxQuoted), which does not
// combine with boxText in pairApplies and so PERMANENTLY disarms itself and
// falls through to compare()'s plain string equality; 1.50 is an UNQUOTED
// numeric literal (boxNumber), which DOES apply against a TEXT column
// (ADR-0012 item 5) — the column's bytes compare against the literal's exact
// SOURCE TEXT, not its parsed float64. A bug that let one settled pair's
// disarmed answer leak into a per-NODE "every pair is disarmed" cache would
// compare the column's "1.50" against fmt.Sprint(1.5) == "1.5" instead of
// against "1.50" and silently miss the row.
func TestInMixedArmedDisarmedPairsAppliesEachPairsOwnRule(t *testing.T) {
	b := stringColBatch("1.50", "no-match", "1.50")
	e := compileExprSQL(t, "s IN ('bogus', 1.50)")
	in, ok := e.(*In)
	if !ok {
		t.Fatalf("compiled to %T, want *In", e)
	}
	// Run it over every row more than once so the per-pair AND per-node
	// disarm caches both have a chance to settle before the assertions that
	// matter, the way the row loop actually exercises them across a batch.
	for pass := 0; pass < 3; pass++ {
		for row := 0; row < b.Len; row++ {
			got := in.EvalBool(b, row)
			want := row == 0 || row == 2
			if got != want {
				t.Fatalf("pass %d row %d: s IN ('bogus', 1.50) = %v, want %v", pass, row, got, want)
			}
		}
	}
	// The armed pair (against 1.50) must have stayed armed — the set can
	// never legitimately reach the fully-disarmed fast path here.
	if in.disarm.disarmed(in.pairs) {
		t.Error("mixed IN list (one armed pair) reported fully disarmed")
	}
}

// TestBetweenMixedArmedDisarmedBoundsAppliesEachBoundsOwnRule is Between's
// counterpart: the lower bound is a QUOTED literal (disarmed against a TEXT
// column) and the upper bound is a numeric literal (armed, exact-text rule).
func TestBetweenMixedArmedDisarmedBoundsAppliesEachBoundsOwnRule(t *testing.T) {
	b := stringColBatch("1.20", "1.50", "9")
	e := compileExprSQL(t, "s BETWEEN '1.00' AND 1.50")
	bt, ok := e.(*Between)
	if !ok {
		t.Fatalf("compiled to %T, want *Between", e)
	}
	// Bytewise, not numeric: "1.00" <= "1.20" <= "1.50" true; "1.00" <=
	// "1.50" <= "1.50" true; "1.00" <= "9" <= "1.50" false — "9" (byte '9')
	// sorts above both bounds ('9' > '1'), so it passes the low bound but
	// fails the high one.
	want := []bool{true, true, false}
	for pass := 0; pass < 3; pass++ {
		for row := 0; row < b.Len; row++ {
			got := bt.EvalBool(b, row)
			if got != want[row] {
				t.Fatalf("pass %d row %d: s BETWEEN '1.00' AND 1.50 = %v, want %v", pass, row, got, want[row])
			}
		}
	}
	if bt.disarm.disarmed(bt.pairs) {
		t.Error("mixed BETWEEN bounds (one armed pair) reported fully disarmed")
	}
}

// TestInAllDisarmedSettlesToFastPath is the companion positive case: an IN
// list over an ordinary INT column, where no member carries a
// declaration-driven rule, must settle its node-level cache to "disarmed"
// and keep answering correctly through the plain compare() path once it
// does — this is BenchmarkFilterIn's exact shape.
func TestInAllDisarmedSettlesToFastPath(t *testing.T) {
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}
	b := batch.NewRecordBatch(schema, 3)
	b.Columns[0].SetValue(0, int64(7))
	b.Columns[0].SetValue(1, int64(8))
	b.Columns[0].SetValue(2, int64(11))
	b.Len = 3
	e := compileExprSQL(t, "n IN (7, 11, 23)")
	in, ok := e.(*In)
	if !ok {
		t.Fatalf("compiled to %T, want *In", e)
	}
	want := []bool{true, false, true}
	for row := 0; row < b.Len; row++ {
		if got := in.EvalBool(b, row); got != want[row] {
			t.Fatalf("row %d: n IN (7, 11, 23) = %v, want %v", row, got, want[row])
		}
	}
	if !in.disarm.disarmed(in.pairs) {
		t.Error("all-int-literal IN list never settled to the disarmed fast path")
	}
	// The fast path must still answer correctly once settled.
	for row := 0; row < b.Len; row++ {
		if got := in.EvalBool(b, row); got != want[row] {
			t.Fatalf("post-settle row %d: n IN (7, 11, 23) = %v, want %v", row, got, want[row])
		}
	}
}
