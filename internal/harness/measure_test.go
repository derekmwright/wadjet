package harness

import (
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

func TestCollectorAggregatesPeak(t *testing.T) {
	c := NewCollector()
	c.StartWindow("q05")

	heartbeats := []uint64{
		100 * 1024 * 1024,
		300 * 1024 * 1024,
		800 * 1024 * 1024,
		200 * 1024 * 1024,
	}
	for i, h := range heartbeats {
		c.Observe(distributed.WorkerHeartbeat{
			WorkerID:      "w0",
			MemoryUsed:    int64(h),
			Mallocs:       uint64(1000 * (i + 1)),
			NumGoroutines: 50,
			Timestamp:     time.Now().Add(time.Duration(i) * 100 * time.Millisecond),
		})
	}

	m := c.EndWindow("q05")
	if m.PeakHeapMB < 800 {
		t.Errorf("expected peak >= 800 MB, got %d", m.PeakHeapMB)
	}
	// Allocs delta = 4000 - 1000 = 3000
	if m.AllocCount != 3000 {
		t.Errorf("expected allocs delta 3000, got %d", m.AllocCount)
	}
}

func TestCollectorNoWindow(t *testing.T) {
	c := NewCollector()
	// Observe without starting a window should not panic.
	c.Observe(distributed.WorkerHeartbeat{WorkerID: "w0", MemoryUsed: 100})
	m := c.EndWindow("q05")
	if m.Query != "q05" {
		t.Errorf("expected query q05, got %s", m.Query)
	}
}

func TestHangDetectorTriggersOnMonotonicGrowth(t *testing.T) {
	hd := NewHangDetector(30 * time.Second)
	now := time.Now()

	for i := 0; i < 36; i++ {
		hung := hd.Observe(now.Add(time.Duration(i)*time.Second), 100+i)
		if i < 30 && hung {
			t.Errorf("triggered too early at i=%d", i)
		}
	}
	if !hd.Observe(now.Add(36*time.Second), 137) {
		t.Error("expected hang trigger after 36s of monotonic growth")
	}
}

func TestHangDetectorDoesNotTriggerOnNoise(t *testing.T) {
	hd := NewHangDetector(30 * time.Second)
	now := time.Now()
	counts := []int{100, 105, 102, 108, 100, 110, 95, 105, 100, 102}
	for i := 0; i < 60; i++ {
		hung := hd.Observe(now.Add(time.Duration(i)*time.Second), counts[i%len(counts)])
		if hung {
			t.Errorf("false trigger at i=%d", i)
		}
	}
}
