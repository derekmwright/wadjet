package clickbench

import (
	"context"
	"os"
	"runtime"
	"testing"
)

// TestHeapRetentionAcrossQueries probes for aggregation-state retention
// across repeated queries on one DB handle — the shape of the c6a
// validation anomaly where Q05/Q33 hot tries ran SLOWER than cold (state
// from try 1 staying live puts tries 2-3 under GOMEMLIMIT pressure).
// Runs a high-cardinality GROUP BY repeatedly and checks that forced-GC
// heap after each run returns to (near) the post-first-run level instead
// of climbing.
func TestHeapRetentionAcrossQueries(t *testing.T) {
	if os.Getenv("WADJET_HITS_PART") == "" {
		t.Skip("WADJET_HITS_PART not set")
	}
	ctx := context.Background()
	db, _ := openHitsDB(t, ctx)

	q := "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10"

	heapNow := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse
	}

	base := heapNow()
	var after [4]uint64
	for i := 0; i < 4; i++ {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatal(err)
		}
		after[i] = heapNow()
		t.Logf("run %d: heap-in-use after GC = %.1f MB (baseline %.1f MB)",
			i+1, float64(after[i])/1e6, float64(base)/1e6)
	}
	// Allow pools/caches to hold one run's worth of reusable state, but
	// monotone growth across runs 2..4 is a leak.
	if after[3] > after[1]+(after[1]-base)/2 && after[3] > after[1]+64<<20 {
		t.Fatalf("heap climbs across runs: run2=%.1fMB run4=%.1fMB (baseline %.1fMB)",
			float64(after[1])/1e6, float64(after[3])/1e6, float64(base)/1e6)
	}
}
