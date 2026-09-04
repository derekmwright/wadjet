package worker

import (
	"context"
	"runtime/metrics"
	"sync/atomic"
	"time"
)

// taskPeakHeapTracker samples live heap bytes at a fast cadence and
// records the maximum observed during one task's execution. The tracker
// runs in a goroutine started at task start and stopped at task end.
//
// Sampling MUST be STW-free. The original implementation called
// runtime.ReadMemStats here, which stops the world: at 50ms cadence ×
// max_concurrent tasks that is up to 80 STWs/second, and under heavy
// page-cache/writeback pressure (Ms stuck in D-state can't park
// promptly) each STW stretches — the 2026-08 "frozen-spin" stall family
// (dispatch-stall arc specimens 1-8 + six trap captures 2026-08-13,
// results/20260813-232615: every SIGABRT dump caught this goroutine in
// [stopping the world] inside ReadMemStats). runtime/metrics reads the
// same counter without stopping the world.
type taskPeakHeapTracker struct {
	peakBytes atomic.Uint64
	// ticks counts completed ticker-driven samples. Tests use it to wait
	// deterministically for the next sample instead of sleeping a fixed
	// duration and hoping it outlasts the 50ms ticker under CPU load.
	ticks  atomic.Uint64
	stop   chan struct{}
	done   chan struct{}
	sample []metrics.Sample
}

// heapAllocMetric is the runtime/metrics equivalent of
// runtime.MemStats.HeapAlloc (bytes of live heap objects).
const heapAllocMetric = "/memory/classes/heap/objects:bytes"

func newTaskPeakHeapTracker(ctx context.Context) *taskPeakHeapTracker {
	t := &taskPeakHeapTracker{
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		sample: []metrics.Sample{{Name: heapAllocMetric}},
	}
	// Sample immediately to capture the initial heap state.
	t.peakBytes.Store(t.heapAlloc())

	go t.run(ctx)
	return t
}

// heapAlloc returns current live heap bytes without stopping the world.
func (t *taskPeakHeapTracker) heapAlloc() uint64 {
	metrics.Read(t.sample)
	return t.sample[0].Value.Uint64()
}

func (t *taskPeakHeapTracker) run(ctx context.Context) {
	defer close(t.done)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.observe()
		case <-t.stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// observe takes one heap reading and raises the peak if it grew. It is
// factored out of run()'s ticker case so tests can count completed
// samples (via ticks) and wait for one deterministically, rather than
// depending on the ticker's wall-clock cadence under load.
func (t *taskPeakHeapTracker) observe() {
	alloc := t.heapAlloc()
	for {
		cur := t.peakBytes.Load()
		if alloc <= cur {
			break
		}
		if t.peakBytes.CompareAndSwap(cur, alloc) {
			break
		}
	}
	t.ticks.Add(1)
}

// Stop halts sampling and waits for the goroutine to exit.
func (t *taskPeakHeapTracker) Stop() {
	close(t.stop)
	<-t.done
}

// PeakMB returns the observed peak live heap bytes in megabytes.
func (t *taskPeakHeapTracker) PeakMB() int64 {
	return int64(t.peakBytes.Load() / (1024 * 1024))
}
