package oracle

import "testing"

func canon(cols []string, rows []map[string]any) *Canon {
	return Canonicalize(&Result{Columns: cols, Rows: rows})
}

// TestCanonIntegerFloatTypingAcrossKillSwitch pins #301: an optimization
// kill switch can move a same-engine value between int64 and float64
// representations without changing the VALUE — WADJET_INT_ARITH=0 falls
// `ClientIP - 1`-shaped arithmetic back to the float64 path on purpose (see
// BinOpNumeric's doc comment in internal/engine/expr/binop_numeric.go) — so
// the intra-engine (same-engine, different switch settings) comparison
// Canonicalize/Canon.Diff drives must not read that representation change
// as a divergence. Before this fix, canonCell rendered a float64 at a fixed
// 6 significant digits regardless of whether it was an exact integer, so
// -1000213612 (int64, full precision) and -1000213612.0 (float64, quantized
// to "-1.00021e+09") were reported as different answers — the exact
// ClickBench Q36 divergence.
func TestCanonIntegerFloatTypingAcrossKillSwitch(t *testing.T) {
	cols := []string{"n"}
	want := canon(cols, []map[string]any{{"n": int64(-1000213612)}})
	for _, v := range []any{float64(-1000213612), int32(-1000213612), int64(-1000213612)} {
		got := canon(cols, []map[string]any{{"n": v}})
		if d := want.Diff(got, Query{}); d != "" {
			t.Errorf("%T(%v) diverged from int64 of the same value: %s", v, v, d)
		}
	}
	// A real value error must still be caught — quantization must not
	// swallow two integers that differ past the 4-significant-digit floor.
	a := canon(cols, []map[string]any{{"n": int64(1000213612)}})
	b := canon(cols, []map[string]any{{"n": float64(1000213712)}}) // differs in the 7th digit
	if d := a.Diff(b, Query{}); d == "" {
		t.Error("a genuinely different value was accepted as int/float typing noise")
	}
}

// TestCanonFloatNoiseStillAbsorbed guards that fixing the integer-typing
// false-positive (#301) did not regress the ORIGINAL reason canonCell
// quantizes floats at all: accumulation-order noise between two correct
// runs of a genuinely fractional value (SUM/AVG) must still compare equal.
func TestCanonFloatNoiseStillAbsorbed(t *testing.T) {
	cols := []string{"revenue"}
	exact := canon(cols, []map[string]any{{"revenue": 494176258.39}})
	noisy := canon(cols, []map[string]any{{"revenue": 494176258.40}}) // ~2e-11 relative
	if d := exact.Diff(noisy, Query{}); d != "" {
		t.Errorf("one-ULP-class noise registered as divergence: %s", d)
	}
	inflated := canon(cols, []map[string]any{{"revenue": 494176258.39 * 25}})
	if d := exact.Diff(inflated, Query{}); d == "" {
		t.Error("a 25x inflated value passed as noise")
	}
}
