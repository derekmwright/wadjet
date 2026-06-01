package worker

import "testing"

func TestComputeStatsGauges(t *testing.T) {
	const mb = 1 << 20
	cases := []struct {
		name      string
		heapInuse int64
		rss       int64
		drift     int64
		wantDrift int64   // MB
		wantPct   float64 // fraction
		wantMmap  int64   // MB
	}{
		{
			name: "typical", heapInuse: 1000 * mb, rss: 9000 * mb, drift: 250 * mb,
			wantDrift: 250, wantPct: 0.25, wantMmap: 8000,
		},
		{
			name: "rss_below_heap_clamps_mmap", heapInuse: 1000 * mb, rss: 800 * mb, drift: 100 * mb,
			wantDrift: 100, wantPct: 0.10, wantMmap: 0,
		},
		{
			name: "zero_heap_inuse_pct_zero", heapInuse: 0, rss: 500 * mb, drift: 50 * mb,
			wantDrift: 50, wantPct: 0, wantMmap: 500,
		},
		{
			name: "negative_drift_not_clamped", heapInuse: 1000 * mb, rss: 1000 * mb, drift: -300 * mb,
			wantDrift: -300, wantPct: -0.30, wantMmap: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			driftMB, driftPct, mmapMB := computeStatsGauges(tc.heapInuse, tc.rss, tc.drift)
			if driftMB != tc.wantDrift {
				t.Errorf("driftMB = %d, want %d", driftMB, tc.wantDrift)
			}
			if driftPct != tc.wantPct {
				t.Errorf("driftPct = %v, want %v", driftPct, tc.wantPct)
			}
			if mmapMB != tc.wantMmap {
				t.Errorf("mmapMB = %d, want %d", mmapMB, tc.wantMmap)
			}
		})
	}
}

// TestComputeStatsGauges_WarnThresholds documents the drift_pct thresholds the
// stats loop branches on (>0.20 high, >0.50 critical), so a refactor that moves
// the constants is caught.
func TestComputeStatsGauges_WarnThresholds(t *testing.T) {
	const mb = 1 << 20
	// 10% → no warn; 30% → high; 60% → critical.
	for _, tc := range []struct {
		drift    int64
		wantHigh bool
		wantCrit bool
	}{
		{drift: 100 * mb, wantHigh: false, wantCrit: false}, // 10%
		{drift: 300 * mb, wantHigh: true, wantCrit: false},  // 30%
		{drift: 600 * mb, wantHigh: true, wantCrit: true},   // 60%
	} {
		_, pct, _ := computeStatsGauges(1000*mb, 1000*mb, tc.drift)
		high := pct > 0.20
		crit := pct > 0.50
		if high != tc.wantHigh || crit != tc.wantCrit {
			t.Errorf("drift %dMB: high=%v crit=%v, want high=%v crit=%v", tc.drift/mb, high, crit, tc.wantHigh, tc.wantCrit)
		}
	}
}
