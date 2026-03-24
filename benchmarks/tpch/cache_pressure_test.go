package tpch

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/worker"
)

// TestCachePressureQ07 measures Q07 performance with different amounts of
// live data in the Go heap. Compares GOGC=100 vs GOGC=off across cache sizes.
func TestCachePressureQ07(t *testing.T) {
	db := setupTPCH(t, ScaleFactor(0.01))
	ctx := context.Background()
	sf := ScaleFactor(0.01)
	q := GetQuery(7, sf)

	db.Query(ctx, q.SQL) // warm-up

	cacheSizes := []struct {
		name  string
		bytes int64
	}{
		{"no_cache", 0},
		{"256MB", 256 * 1024 * 1024},
		{"1GB", 1024 * 1024 * 1024},
		{"4GB", 4 * 1024 * 1024 * 1024},
		{"6GB", 6 * 1024 * 1024 * 1024},
	}

	// Key insight: on c7g.4xlarge workers, GOMEMLIMIT=28.8GB.
	// A 7.2GB cache is 25% of that limit — lots of headroom.
	// Our local test with 10GB limit and 6GB cache (60%) is unrealistic.
	// Test with 28GB limit to match production conditions.
	gogcModes := []struct {
		name     string
		gogc     int
		memlimit int64
	}{
		{"GOGC100_10GB", 100, 10 * 1024 * 1024 * 1024},
		{"GOGCoff_10GB", -1, 10 * 1024 * 1024 * 1024},
		{"GOGC100_28GB", 100, 28 * 1024 * 1024 * 1024},
		{"GOGCoff_28GB", -1, 28 * 1024 * 1024 * 1024},
	}

	for _, gm := range gogcModes {
		t.Run(gm.name, func(t *testing.T) {
			debug.SetMemoryLimit(gm.memlimit)
			if gm.gogc == -1 {
				debug.SetGCPercent(-1)
			} else {
				debug.SetGCPercent(gm.gogc)
			}

			for _, cs := range cacheSizes {
				t.Run(cs.name, func(t *testing.T) {
					var cache *worker.LRUCache
					if cs.bytes > 0 {
						cache = worker.NewLRUCache(cs.bytes)
						chunkSize := 5 * 1024 * 1024
						numChunks := int(cs.bytes) / chunkSize
						for i := 0; i < numChunks; i++ {
							key := fmt.Sprintf("bucket/table/chunk-%04d.parquet", i)
							cache.Put(key, make([]byte, chunkSize))
						}
					}

					runtime.GC()
					var memBefore runtime.MemStats
					runtime.ReadMemStats(&memBefore)

					const runs = 5
					var total time.Duration
					for i := 0; i < runs; i++ {
						start := time.Now()
						db.Query(ctx, q.SQL)
						total += time.Since(start)
					}
					avg := total / runs

					var memAfter runtime.MemStats
					runtime.ReadMemStats(&memAfter)
					numGC := memAfter.NumGC - memBefore.NumGC

					t.Logf("%6s: avg=%v  GC=%d  heap=%dMB",
						cs.name, avg, numGC, memAfter.HeapAlloc/(1024*1024))

					if cache != nil {
						runtime.KeepAlive(cache)
					}
				})
			}
		})
	}

	debug.SetGCPercent(100)
	debug.SetMemoryLimit(-1) // reset
}
