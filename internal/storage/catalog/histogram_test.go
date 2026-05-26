package catalog

import (
	"math"
	"testing"
)

func TestHistogramRangeSelectivity(t *testing.T) {
	// Uniform distribution 1..10000.
	sample := make([]any, 10000)
	for i := range sample {
		sample[i] = int64(i + 1)
	}
	h := BuildHistogramFromSamples(sample, 64)
	if h == nil {
		t.Fatal("BuildHistogramFromSamples returned nil")
	}

	cases := []struct {
		name        string
		lo, hi      int64
		expectSel   float64
		tolerancePc float64
	}{
		{"first quarter", 1, 2500, 0.25, 5},
		{"middle half", 2500, 7500, 0.5, 5},
		{"second half", 5000, 10000, 0.5, 5},
		{"full range", 1, 10000, 1.0, 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sel := h.SelectivityRange(c.lo, c.hi)
			errPct := math.Abs(sel-c.expectSel) / c.expectSel * 100
			t.Logf("[%d, %d] sel=%.4f expected=%.4f err=%.1f%%", c.lo, c.hi, sel, c.expectSel, errPct)
			if errPct > c.tolerancePc {
				t.Errorf("selectivity %.4f differs from expected %.4f by %.1f%% (tolerance %.1f%%)",
					sel, c.expectSel, errPct, c.tolerancePc)
			}
		})
	}
}

func TestHistogramLEEdges(t *testing.T) {
	sample := make([]any, 1000)
	for i := range sample {
		sample[i] = int64(i + 1)
	}
	h := BuildHistogramFromSamples(sample, 32)

	// Below min → 0
	if sel := h.SelectivityLE(int64(0)); sel != 0 {
		t.Errorf("LE 0 should be 0, got %f", sel)
	}
	// Above max → 1
	if sel := h.SelectivityLE(int64(2000)); sel != 1 {
		t.Errorf("LE 2000 should be 1, got %f", sel)
	}
	// At median → ~0.5
	sel := h.SelectivityLE(int64(500))
	if sel < 0.4 || sel > 0.6 {
		t.Errorf("LE 500 should be ~0.5, got %f", sel)
	}
}

func TestHistogramRoundTrip(t *testing.T) {
	sample := make([]any, 5000)
	for i := range sample {
		sample[i] = int64(i*7 + 3)
	}
	h := BuildHistogramFromSamples(sample, 64)
	if h == nil {
		t.Fatal("nil histogram")
	}
	b := h.Bytes()
	got := HistogramFromBytes(b)
	if got == nil {
		t.Fatal("decode returned nil")
	}
	if got.TotalValues != h.TotalValues {
		t.Errorf("TotalValues: got %d want %d", got.TotalValues, h.TotalValues)
	}
	// Selectivity should be identical post-roundtrip.
	origSel := h.SelectivityRange(int64(1000), int64(8000))
	decSel := got.SelectivityRange(int64(1000), int64(8000))
	if origSel != decSel {
		t.Errorf("selectivity diverged: orig=%f decoded=%f", origSel, decSel)
	}
}

func TestHistogramTPCHDateRange(t *testing.T) {
	// Simulate l_shipdate: 7 years of dates as int32 days since 1992-01-01.
	// Q04's filter: 1993-07-01 (year 1.5) to 1993-09-30 (year 1.75) — ~3.6% selectivity.
	sample := make([]any, 60000)
	for i := range sample {
		// Uniform across 7*365 = 2555 days
		sample[i] = int64(i * 2555 / 60000)
	}
	h := BuildHistogramFromSamples(sample, 64)

	q4lo := int64(547)  // ~year 1.5
	q4hi := int64(639)  // ~year 1.75
	sel := h.SelectivityRange(q4lo, q4hi)
	t.Logf("Q04-style date range %d-%d: sel=%.4f (expect ~0.036)", q4lo, q4hi, sel)
	if sel < 0.02 || sel > 0.05 {
		t.Errorf("Q04 date selectivity off: got %.4f", sel)
	}

	// Q21-style 2-year span (1995-1996, year 3-4)
	q21lo := int64(3 * 365)
	q21hi := int64(5 * 365)
	sel = h.SelectivityRange(q21lo, q21hi)
	t.Logf("Q21-style 2-year range %d-%d: sel=%.4f (expect ~0.286)", q21lo, q21hi, sel)
	if sel < 0.25 || sel > 0.32 {
		t.Errorf("Q21 date selectivity off: got %.4f", sel)
	}
}
