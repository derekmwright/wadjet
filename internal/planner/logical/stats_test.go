package logical

import (
	"testing"
)

func TestEstimateNDVFromStats_IntegerRange(t *testing.T) {
	// Column with int64 range [1, 100] → NDV = 100
	cs := ScanColumnStats{MinValue: int64(1), MaxValue: int64(100)}
	ndv := estimateNDVFromStats(cs, 10000)
	if ndv != 100 {
		t.Errorf("expected NDV=100 for range [1,100], got %.0f", ndv)
	}
}

func TestEstimateNDVFromStats_CappedAtRows(t *testing.T) {
	// Range [1, 1000000] but only 500 rows → NDV capped at 500
	cs := ScanColumnStats{MinValue: int64(1), MaxValue: int64(1000000)}
	ndv := estimateNDVFromStats(cs, 500)
	if ndv != 500 {
		t.Errorf("expected NDV=500 (capped at rows), got %.0f", ndv)
	}
}

func TestEstimateNDVFromStats_FloatRange(t *testing.T) {
	// Float range [0.0, 99.0] → NDV = 100
	cs := ScanColumnStats{MinValue: float64(0), MaxValue: float64(99)}
	ndv := estimateNDVFromStats(cs, 10000)
	if ndv != 100 {
		t.Errorf("expected NDV=100 for float range [0,99], got %.0f", ndv)
	}
}

func TestEstimateNDVFromStats_StringFallback(t *testing.T) {
	// String columns fall back to heuristic
	cs := ScanColumnStats{MinValue: "aaa", MaxValue: "zzz"}
	ndv := estimateNDVFromStats(cs, 10000)
	// Should use sqrt(rows)*10 heuristic, not crash
	if ndv <= 0 {
		t.Errorf("expected positive NDV for string column, got %.0f", ndv)
	}
}

func TestEstimateScanStats_WithCatalogStats(t *testing.T) {
	// Scan node with catalog column stats should use real stats instead of naming heuristics
	n := &Node{
		Type:            NodeScan,
		ScanRowEstimate: 100000,
		ScanColumns:     []string{"id", "status"},
		ScanColStats: map[string]ScanColumnStats{
			"id":     {MinValue: int64(1), MaxValue: int64(100000)},
			"status": {MinValue: int64(0), MaxValue: int64(4)},
		},
	}

	stats := estimateScanStats(n)

	// id: range [1, 100000] → NDV = 100000 (matches row count)
	idNDV := stats.ColNDV["id"]
	if idNDV != 100000 {
		t.Errorf("id NDV: got %.0f, want 100000", idNDV)
	}

	// status: range [0, 4] → NDV = 5
	statusNDV := stats.ColNDV["status"]
	if statusNDV != 5 {
		t.Errorf("status NDV: got %.0f, want 5", statusNDV)
	}
}

func TestEstimateScanStats_FallsBackToHeuristics(t *testing.T) {
	// Without catalog stats, should still work with naming heuristics
	n := &Node{
		Type:            NodeScan,
		ScanRowEstimate: 10000,
		ScanColumns:     []string{"id", "status"},
	}

	stats := estimateScanStats(n)
	if stats.ColNDV["id"] <= 0 {
		t.Error("expected positive NDV for 'id' column")
	}
	if stats.ColNDV["status"] <= 0 {
		t.Error("expected positive NDV for 'status' column")
	}
}
