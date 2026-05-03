package worker

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHeartbeatStarvationUnderLoad probes whether the heartbeat goroutine
// can be scheduling-starved under the kind of concurrent allocation
// pressure that SF10 broadcast-join probe phases produce on workers
// (multiple tasks each pushing 3+ GB build-side hash tables through GC).
//
// 2026-05-02 finding (commit landing this test): negative. Under
// GOGC=20 + GOMAXPROCS=4 + 16 task goroutines doing heavy mixed
// allocation (8 MB columnar batches + many small hash-table allocations)
// the heartbeat goroutine still scheduled within ~25 µs P99 of its
// nominal 50 ms cadence. LockOSThread offered no benefit (slightly
// worse, due to syscall overhead). All ticks fired.
//
// Conclusion: the EC2 SF10 heartbeat blackout (Q11 stall, all 6 workers
// reaped simultaneously) is NOT caused by Go runtime scheduling
// starvation on the workers. Likely remaining causes — needing different
// diagnostics:
//   1. NATS server-side slow-consumer drops on the coord's heartbeat sub
//      (heartbeat traffic from many workers fanning into one sub)
//   2. Coord-side heartbeat goroutine starvation (this test measures
//      worker side only)
//   3. cgroup/kernel pause on workers under MemoryHigh pressure
//   4. Network-level pause between worker hosts and coord
//
// Multi-signal liveness at coord (commit landing alongside this test)
// covers cases (1) and the heartbeat-only-stops-but-data-plane-keeps-
// working sliver of (2). It does NOT cover (3) or (4); those would need
// host-side ops fixes (workers_per_node=1, lower max_concurrent).
//
// This test is kept as diagnostic documentation of "tested this
// hypothesis, found nothing." Re-run if a future symptom suggests
// scheduling starvation; if numbers move, the conclusion changes.
//
// Run with: go test -count=1 -run TestHeartbeatStarvation -v ./internal/worker/
func TestHeartbeatStarvationUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("starvation reproducer is slow; skip under -short")
	}

	// Cap GOMAXPROCS so contention is achievable on any test box. SF10
	// production runs on 16 vCPU per host with 2 workers (8 vCPU effective
	// per worker) — running here with 4 lets us oversubscribe at modest
	// goroutine counts.
	prevProcs := runtime.GOMAXPROCS(4)
	defer runtime.GOMAXPROCS(prevProcs)

	const (
		// 4 task goroutines × per-host concurrency hot path. Production
		// SF10 nodes run 2 workers × max_concurrent=4 = 8 task goroutines
		// per host; we oversubscribe to 16 to make GC/scheduler pressure
		// unambiguous in the test result.
		taskGoroutines = 16
		runFor         = 4 * time.Second
		hbInterval     = 50 * time.Millisecond // accelerated from prod 10s for test speed
	)

	type result struct {
		samples []time.Duration
		ticks   int
	}
	measure := func(lockOSThread bool) result {
		var (
			samples = make([]time.Duration, 0, 128)
			samplesMu sync.Mutex
			ticks   int32
		)
		stop := make(chan struct{})

		// Heartbeat goroutine — measures the time between consecutive
		// ticks. Under healthy scheduling this stays at ~hbInterval; under
		// starvation it spikes well past it. The deviation (gap - interval)
		// is the schedule-latency proxy. Latency on the FIRST tick is
		// undefined (no prior tick), so we drop sample 0.
		hbDone := make(chan struct{})
		go func() {
			defer close(hbDone)
			if lockOSThread {
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
			}
			ticker := time.NewTicker(hbInterval)
			defer ticker.Stop()
			var prev time.Time
			for {
				select {
				case <-stop:
					return
				case t := <-ticker.C:
					if !prev.IsZero() {
						gap := t.Sub(prev)
						lat := gap - hbInterval
						if lat < 0 {
							lat = 0
						}
						samplesMu.Lock()
						samples = append(samples, lat)
						samplesMu.Unlock()
					}
					prev = t
					atomic.AddInt32(&ticks, 1)
				}
			}
		}()

		// Task goroutines — busy allocation work to drive GC pressure.
		// Each holds onto a chunk of memory for a bit then releases, so
		// the live heap walks up and down and the GC cycles frequently.
		// Mix of allocation sizes mimics SF10 broadcast-join build (large
		// columnar batches) + scan (smaller batches) + hash table inserts
		// (many small allocations).
		var taskWg sync.WaitGroup
		for i := 0; i < taskGoroutines; i++ {
			taskWg.Add(1)
			go func() {
				defer taskWg.Done()
				keep := make([][]byte, 0, 64)
				smallKeep := make(map[int][]byte, 1<<14)
				ctr := 0
				for {
					select {
					case <-stop:
						return
					default:
					}
					ctr++
					// Big batch: 8 MB columnar-style. Sized to fit a few
					// of these per goroutine before GC trips, mimicking
					// the columnar batch lifecycle.
					b := make([]byte, 8*1024*1024)
					for j := 0; j < len(b); j += 4096 {
						b[j] = byte(j)
					}
					keep = append(keep, b)
					if len(keep) > 8 {
						keep = keep[4:] // 32 MB live per goroutine
					}
					// Many small allocations: hash-table style.
					for k := 0; k < 1024; k++ {
						smallKeep[ctr*1024+k] = make([]byte, 256)
					}
					if len(smallKeep) > 1<<16 {
						// Drop a chunk so the map doesn't grow unbounded.
						i := 0
						for k := range smallKeep {
							delete(smallKeep, k)
							i++
							if i > 1<<15 {
								break
							}
						}
					}
				}
			}()
		}

		time.Sleep(runFor)
		close(stop)
		taskWg.Wait()
		<-hbDone

		samplesMu.Lock()
		out := result{samples: append([]time.Duration(nil), samples...), ticks: int(ticks)}
		samplesMu.Unlock()
		return out
	}

	stats := func(s []time.Duration) (mean, p50, p99, max time.Duration) {
		if len(s) == 0 {
			return
		}
		// In-place sort; caller's slice may be modified.
		sortDurations(s)
		var sum time.Duration
		for _, d := range s {
			sum += d
		}
		mean = sum / time.Duration(len(s))
		p50 = s[len(s)/2]
		p99idx := (len(s) * 99) / 100
		if p99idx >= len(s) {
			p99idx = len(s) - 1
		}
		p99 = s[p99idx]
		max = s[len(s)-1]
		return
	}

	t.Logf("GOMAXPROCS=%d task_goroutines=%d hb_interval=%s run=%s",
		runtime.GOMAXPROCS(0), taskGoroutines, hbInterval, runFor)

	baseline := measure(false)
	bMean, bP50, bP99, bMax := stats(baseline.samples)
	t.Logf("baseline (no LockOSThread): ticks=%d mean=%s p50=%s p99=%s max=%s",
		baseline.ticks, bMean, bP50, bP99, bMax)

	locked := measure(true)
	lMean, lP50, lP99, lMax := stats(locked.samples)
	t.Logf("locked   (LockOSThread):    ticks=%d mean=%s p50=%s p99=%s max=%s",
		locked.ticks, lMean, lP50, lP99, lMax)

	// Heartbeat must keep firing under load. Coord reaps after 90s with
	// 10s prod interval = 9 missed heartbeats. In this accelerated test
	// (50ms interval, 4s run) we expect ~80 ticks. If we get fewer than
	// half that, scheduling is genuinely starved.
	expectedTicks := int(runFor / hbInterval)
	if baseline.ticks < expectedTicks/2 {
		t.Errorf("baseline starved: only %d/%d ticks fired", baseline.ticks, expectedTicks)
	}
	if locked.ticks < expectedTicks/2 {
		t.Errorf("locked starved: only %d/%d ticks fired", locked.ticks, expectedTicks)
	}
}

// sortDurations is a tiny insertion sort to avoid pulling in sort package
// (keeps the test self-contained).
func sortDurations(s []time.Duration) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
