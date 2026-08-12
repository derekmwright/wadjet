package memory

import (
	"context"
	"math"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

func TestSpillManager(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
		{"id": int64(3), "name": "carol"},
	}

	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("expected non-empty path")
	}

	files := sm.SpilledFiles()
	if len(files) != 1 {
		t.Fatalf("expected 1 spill file, got %d", len(files))
	}

	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(back))
	}
	if back[0]["name"] != "alice" {
		t.Fatalf("expected alice, got %v", back[0]["name"])
	}
	if back[0]["id"] != int64(1) {
		t.Fatalf("expected id=1 (int64), got %v (%T)", back[0]["id"], back[0]["id"])
	}
}

func TestSpillManagerShouldSpill(t *testing.T) {
	tracker := NewTracker("test", 100)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	if sm.ShouldSpill() {
		t.Fatal("should not spill at 0% usage")
	}

	tracker.Reserve(55)
	if sm.ShouldSpill() {
		t.Fatal("should not spill at 55% usage")
	}

	tracker.Reserve(10) // now at 65%
	if !sm.ShouldSpill() {
		t.Fatal("should spill at 65% usage")
	}
}

func TestSpillManagerCleanup(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{{"x": int64(1)}}
	sm.SpillRows(rows)
	sm.SpillRows(rows)

	if len(sm.SpilledFiles()) != 2 {
		t.Fatal("expected 2 files")
	}

	sm.Cleanup()
	if len(sm.SpilledFiles()) != 0 {
		t.Fatal("expected 0 files after cleanup")
	}
}

func TestSpillAllTypes(t *testing.T) {
	tracker := NewTracker("test", 1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	rows := []map[string]any{
		{
			"bool_t":   true,
			"bool_f":   false,
			"int32":    int32(42),
			"int64":    int64(1234567890),
			"float32":  float32(3.14),
			"float64":  float64(2.718281828),
			"str":      "hello world",
			"null_val": nil,
		},
	}

	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatal(err)
	}

	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 {
		t.Fatalf("expected 1 row, got %d", len(back))
	}

	row := back[0]
	if row["bool_t"] != true {
		t.Errorf("bool_t: expected true, got %v", row["bool_t"])
	}
	if row["bool_f"] != false {
		t.Errorf("bool_f: expected false, got %v", row["bool_f"])
	}
	if row["int32"] != int32(42) {
		t.Errorf("int32: expected 42, got %v (%T)", row["int32"], row["int32"])
	}
	if row["int64"] != int64(1234567890) {
		t.Errorf("int64: expected 1234567890, got %v", row["int64"])
	}
	if v, ok := row["float32"].(float32); !ok || math.Abs(float64(v)-3.14) > 0.001 {
		t.Errorf("float32: expected ~3.14, got %v (%T)", row["float32"], row["float32"])
	}
	if v, ok := row["float64"].(float64); !ok || math.Abs(v-2.718281828) > 0.0001 {
		t.Errorf("float64: expected ~2.718, got %v", row["float64"])
	}
	if row["str"] != "hello world" {
		t.Errorf("str: expected 'hello world', got %v", row["str"])
	}
	if row["null_val"] != nil {
		t.Errorf("null_val: expected nil, got %v", row["null_val"])
	}
}

func TestSpillEmptyRows(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	path, err := sm.SpillRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if path != "" {
		t.Fatalf("expected empty path for empty rows, got %q", path)
	}
}

// TestSpillConcurrentManagers verifies that multiple SpillManagers sharing the
// same base directory produce unique file names. Before the fix, per-instance
// atomic counters caused collisions (spill-1.bin vs spill-1.bin) when concurrent
// tasks ran on the same worker — one task's Cleanup would delete another's files.
func TestSpillConcurrentManagers(t *testing.T) {
	baseDir := t.TempDir()
	tracker := NewTracker("test", 100*1024*1024)

	const numManagers = 4
	const filesPerManager = 5
	managers := make([]*SpillManager, numManagers)
	allPaths := make([][]string, numManagers)

	// Create multiple SpillManagers sharing the same base directory,
	// simulating concurrent tasks on the same worker.
	for i := range managers {
		sm, err := NewSpillManager(baseDir, tracker)
		if err != nil {
			t.Fatal(err)
		}
		managers[i] = sm
	}

	// Write spill files from all managers concurrently.
	rows := []map[string]any{{"id": int64(1), "val": "test"}}
	for i, sm := range managers {
		for j := 0; j < filesPerManager; j++ {
			path, err := sm.SpillRows(rows)
			if err != nil {
				t.Fatalf("manager %d file %d: %v", i, j, err)
			}
			allPaths[i] = append(allPaths[i], path)
		}
	}

	// Verify no path collisions across managers.
	seen := make(map[string]int)
	for i, paths := range allPaths {
		for _, p := range paths {
			if prev, ok := seen[p]; ok {
				t.Fatalf("path collision: manager %d and %d both produced %s", prev, i, p)
			}
			seen[p] = i
		}
	}

	// Verify that cleaning up one manager doesn't break another's files.
	managers[0].Cleanup()
	for _, path := range allPaths[1] {
		if _, err := ReadSpilledRows(path); err != nil {
			t.Fatalf("manager 1 file unreadable after manager 0 cleanup: %v", err)
		}
	}

	// Clean up remaining.
	for _, sm := range managers[1:] {
		sm.Cleanup()
	}
}

func TestSpillLargeDataset(t *testing.T) {
	tracker := NewTracker("test", 100*1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	n := 10000
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"id":    int64(i),
			"value": float64(i) * 1.5,
			"label": "row",
		}
	}

	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatal(err)
	}

	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != n {
		t.Fatalf("expected %d rows, got %d", n, len(back))
	}
	if back[999]["id"] != int64(999) {
		t.Fatalf("expected id=999, got %v", back[999]["id"])
	}
}

func TestShouldSpillFor_CheapTriggersAt40Percent(t *testing.T) {
	tr := NewTracker("t", 1000)
	dir := t.TempDir()
	sm, err := NewSpillManager(dir, tr)
	if err != nil {
		t.Fatal(err)
	}
	tr.ForceReserve(399)
	if sm.ShouldSpillFor(SpillCheap) {
		t.Errorf("at 39.9%% of budget, SpillCheap should not fire")
	}
	tr.ForceReserve(2) // now 401, above 40%
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Errorf("at 40.1%% of budget, SpillCheap should fire")
	}
}

func TestShouldSpillFor_ExpensiveTriggersAt90Percent(t *testing.T) {
	tr := NewTracker("t", 1000)
	dir := t.TempDir()
	sm, err := NewSpillManager(dir, tr)
	if err != nil {
		t.Fatal(err)
	}
	tr.ForceReserve(899)
	if sm.ShouldSpillFor(SpillExpensive) {
		t.Errorf("at 89.9%% of budget, SpillExpensive should not fire")
	}
	tr.ForceReserve(2) // now 901, above 90%
	if !sm.ShouldSpillFor(SpillExpensive) {
		t.Errorf("at 90.1%% of budget, SpillExpensive should fire")
	}
	// Sanity: at 90.1% SpillCheap also fires.
	if !sm.ShouldSpillFor(SpillCheap) {
		t.Errorf("at 90.1%% of budget, SpillCheap should also fire")
	}
}

// TestPauseOnHeapBackpressure_NoLimit confirms the helper is a no-op when
// GOMEMLIMIT is unset. Most embedded callers (single-process wadjet.DB,
// tests not running under the worker) hit this path, and PauseOnHeapBackpressure
// must stay cheap there.
func TestPauseOnHeapBackpressure_NoLimit(t *testing.T) {
	// Reset cached state so the test sees current GOMEMLIMIT.
	resetHeapBackpressureCache(t)

	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(math.MaxInt64) // disable limit
	t.Cleanup(func() { debug.SetMemoryLimit(prev); resetHeapBackpressureCache(t) })

	if HeapBackpressureActive() {
		t.Errorf("HeapBackpressureActive should be false when GOMEMLIMIT is unset")
	}
	start := time.Now()
	if err := PauseOnHeapBackpressure(context.Background()); err != nil {
		t.Errorf("PauseOnHeapBackpressure returned err: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("PauseOnHeapBackpressure should be near-instant, took %v", elapsed)
	}
}

// TestPauseOnHeapBackpressure_RespectsContextCancel confirms that when
// pressure is active and a pause is in flight, ctx cancellation is honored
// promptly so worker shutdown isn't blocked on a 50ms sleep per task.
func TestPauseOnHeapBackpressure_RespectsContextCancel(t *testing.T) {
	resetHeapBackpressureCache(t)

	// Force the cached value to "active" by writing it directly.
	heapBackpressureLastValue.Store(true)
	heapBackpressureLastCheckNS.Store(time.Now().UnixNano())
	t.Cleanup(func() { resetHeapBackpressureCache(t) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediate

	start := time.Now()
	err := PauseOnHeapBackpressure(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Errorf("PauseOnHeapBackpressure should return ctx.Err() when cancelled")
	}
	if elapsed > 20*time.Millisecond {
		t.Errorf("PauseOnHeapBackpressure should return promptly on cancel, took %v", elapsed)
	}
}

// TestPauseOnHeapBackpressure_PausesUnderPressure confirms that when
// HeapBackpressureActive is true and ctx is alive, PauseOnHeapBackpressure
// blocks for ~HeapBackpressurePauseDuration. Validates the actual sleep
// path runs end-to-end.
func TestPauseOnHeapBackpressure_PausesUnderPressure(t *testing.T) {
	resetHeapBackpressureCache(t)

	heapBackpressureLastValue.Store(true)
	heapBackpressureLastCheckNS.Store(time.Now().UnixNano())
	t.Cleanup(func() { resetHeapBackpressureCache(t) })

	start := time.Now()
	if err := PauseOnHeapBackpressure(context.Background()); err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < HeapBackpressurePauseDuration {
		t.Errorf("expected pause >= %v, got %v", HeapBackpressurePauseDuration, elapsed)
	}
	// Allow generous slack for scheduler jitter (CI under load).
	if elapsed > HeapBackpressurePauseDuration*4 {
		t.Errorf("pause took longer than expected: %v vs %v expected",
			elapsed, HeapBackpressurePauseDuration)
	}
}

// TestHeapGauges_ConcurrentCallers hammers both lock-free gauges from many
// goroutines. Under -race this validates the CAS-elected-refresher design:
// concurrent readers of the cached value must not race the single refresher
// doing ReadMemStats, and every caller must get a coherent bool.
func TestHeapGauges_ConcurrentCallers(t *testing.T) {
	resetHeapBackpressureCache(t)
	t.Cleanup(func() { resetHeapBackpressureCache(t) })

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				HeapBackpressureActive()
				heapPressureExceeded()
			}
		}()
	}
	wg.Wait()
}

// BenchmarkHeapGaugesParallel measures the per-call cost of the pressure
// gauges under parallel load — the shape of the SF100 hot path, where every
// operator's per-batch spill check and every fragment runner's backpressure
// check hit these concurrently.
func BenchmarkHeapGaugesParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			HeapBackpressureActive()
			heapPressureExceeded()
		}
	})
}

// resetHeapBackpressureCache wipes the package-level cache so the next call
// re-reads runtime.MemStats. Tests that toggle GOMEMLIMIT must call this
// after Setenv/SetMemoryLimit.
func resetHeapBackpressureCache(_ *testing.T) {
	heapBackpressureLastCheckNS.Store(0)
	heapBackpressureLastValue.Store(false)
	heapPressureMemLimit.Store(0) // force re-read on next call
}
