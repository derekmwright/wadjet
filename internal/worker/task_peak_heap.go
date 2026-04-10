package worker

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

// taskPeakHeapTracker samples runtime HeapAlloc at a fast cadence and
// records the maximum observed during one task's execution. The tracker
// runs in a goroutine started at task start and stopped at task end.
type taskPeakHeapTracker struct {
	peakBytes atomic.Uint64
	stop      chan struct{}
	done      chan struct{}
}

func newTaskPeakHeapTracker(ctx context.Context) *taskPeakHeapTracker {
	t := &taskPeakHeapTracker{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	// Sample immediately to capture the initial heap state.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.peakBytes.Store(ms.HeapAlloc)

	go t.run(ctx)
	return t
}

func (t *taskPeakHeapTracker) run(ctx context.Context) {
	defer close(t.done)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			for {
				cur := t.peakBytes.Load()
				if ms.HeapAlloc <= cur {
					break
				}
				if t.peakBytes.CompareAndSwap(cur, ms.HeapAlloc) {
					break
				}
			}
		case <-t.stop:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Stop halts sampling and waits for the goroutine to exit.
func (t *taskPeakHeapTracker) Stop() {
	close(t.stop)
	<-t.done
}

// PeakMB returns the observed peak HeapAlloc in megabytes.
func (t *taskPeakHeapTracker) PeakMB() int64 {
	return int64(t.peakBytes.Load() / (1024 * 1024))
}
