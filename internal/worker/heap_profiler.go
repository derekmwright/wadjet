package worker

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/memory"
)

// heapPressureProfilePoll is how often the heap-pressure profiler checks
// HeapBackpressureActive. The check itself is cached at 100 ms inside
// memory.HeapBackpressureActive, so polling more often than that just
// burns CPU. 1 s is fine — the SF100 heap-pin failure mode (Q17 reaped
// at T+5 m) has wide-window timing.
const heapPressureProfilePoll = 1 * time.Second

// heapPressureProfileRateLimit caps how often a profile is written when
// pressure is sustained. Without a rate limit a SF100 worker spending
// 10 minutes pinned would write ~600 profiles totaling several GB of
// disk. 30 s gives 20 profiles across a 10-minute pin — enough to see
// the heap evolve and pick a representative one for analysis.
const heapPressureProfileRateLimit = 30 * time.Second

// startHeapPressureProfiler spawns a background goroutine that writes a
// pprof heap profile to local disk whenever the process heap crosses the
// HeapBackpressureActive threshold (70 % of GOMEMLIMIT). Rate-limited so
// sustained pressure produces ~20 profiles per 10 min, not 600.
//
// Each profile is paired with a sidecar `.tasks.txt` listing the active
// task IDs and a HeapAlloc snapshot at write time. That sidecar is what
// lets a post-deploy analyst attribute the profile back to (queryID,
// stageID): the orchestration log contains `task_id` → stage mapping;
// the profile contains allocation tree; the sidecar bridges the two.
//
// Output paths:
//
//	<spillDir>/heap-profiles/heap-<workerID>-<unix_ms>.pb.gz
//	<spillDir>/heap-profiles/heap-<workerID>-<unix_ms>.tasks.txt
//
// Analyse with `go tool pprof /path/to/heap-*.pb.gz`.
//
// Disabled when no SpillDir is configured (test paths, minimal embeds).
func (w *Worker) startHeapPressureProfiler(ctx context.Context) {
	if w.config.SpillDir == "" {
		w.logger.Debug("heap pressure profiler disabled: no SpillDir configured")
		return
	}
	dir := filepath.Join(w.config.SpillDir, "heap-profiles")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		w.logger.Warn("heap pressure profiler: mkdir failed", "dir", dir, "err", err)
		return
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		var lastWrite time.Time
		ticker := time.NewTicker(heapPressureProfilePoll)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if !memory.HeapBackpressureActive() {
				continue
			}
			if !lastWrite.IsZero() && time.Since(lastWrite) < heapPressureProfileRateLimit {
				continue
			}
			lastWrite = time.Now()
			if err := w.writeHeapPressureProfile(dir); err != nil {
				w.logger.Warn("heap pressure profile write failed", "err", err)
			}
		}
	}()
	w.logger.Info("heap pressure profiler started",
		"dir", dir,
		"poll_interval", heapPressureProfilePoll,
		"rate_limit", heapPressureProfileRateLimit)
}

// writeHeapPressureProfile snapshots the live heap to a gzipped pprof
// file in dir and writes a sidecar with active-task IDs + HeapAlloc.
// runtime.GC is called first so the profile reflects only live objects;
// otherwise garbage retained until the next GC inflates the tree with
// stuff that's about to be collected.
func (w *Worker) writeHeapPressureProfile(dir string) error {
	ts := time.Now().UnixMilli()
	base := fmt.Sprintf("heap-%s-%d", w.config.WorkerID, ts)
	profilePath := filepath.Join(dir, base+".pb.gz")
	sidecarPath := filepath.Join(dir, base+".tasks.txt")

	// Snapshot active task IDs + heap stats BEFORE the GC so the sidecar
	// reflects the actual pressure point. The profile itself is taken
	// post-GC because pprof's live-heap mode is what we want.
	w.activeTasksMu.RLock()
	activeIDs := make([]string, 0, len(w.activeTasks))
	for id := range w.activeTasks {
		activeIDs = append(activeIDs, id)
	}
	w.activeTasksMu.RUnlock()

	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)

	runtime.GC()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)

	f, err := os.Create(profilePath)
	if err != nil {
		return fmt.Errorf("create profile %s: %w", profilePath, err)
	}
	gz := gzip.NewWriter(f)
	if pErr := pprof.WriteHeapProfile(gz); pErr != nil {
		_ = gz.Close()
		_ = f.Close()
		_ = os.Remove(profilePath)
		return fmt.Errorf("write heap profile: %w", pErr)
	}
	if cErr := gz.Close(); cErr != nil {
		_ = f.Close()
		return fmt.Errorf("close gzip: %w", cErr)
	}
	if cErr := f.Close(); cErr != nil {
		return fmt.Errorf("close profile file: %w", cErr)
	}

	sidecar := fmt.Sprintf(
		"worker_id=%s\n"+
			"timestamp_unix_ms=%d\n"+
			"timestamp_rfc3339=%s\n"+
			"heap_alloc_before_gc_mb=%d\n"+
			"heap_alloc_after_gc_mb=%d\n"+
			"heap_sys_mb=%d\n"+
			"heap_idle_mb=%d\n"+
			"heap_inuse_mb=%d\n"+
			"gc_num=%d\n"+
			"active_task_count=%d\n"+
			"active_task_ids=%v\n",
		w.config.WorkerID,
		ts,
		time.UnixMilli(ts).UTC().Format(time.RFC3339Nano),
		msBefore.HeapAlloc/1024/1024,
		msAfter.HeapAlloc/1024/1024,
		msAfter.HeapSys/1024/1024,
		msAfter.HeapIdle/1024/1024,
		msAfter.HeapInuse/1024/1024,
		msAfter.NumGC,
		len(activeIDs),
		activeIDs,
	)
	if sErr := os.WriteFile(sidecarPath, []byte(sidecar), 0o644); sErr != nil {
		// Sidecar failure isn't fatal — the profile carries the data,
		// just without the active-task bridge.
		w.logger.Warn("heap pressure sidecar write failed",
			"path", sidecarPath, "err", sErr)
	}

	w.logger.Info("heap pressure profile written",
		"path", profilePath,
		"heap_alloc_before_gc_mb", msBefore.HeapAlloc/1024/1024,
		"heap_alloc_after_gc_mb", msAfter.HeapAlloc/1024/1024,
		"active_tasks", len(activeIDs))
	return nil
}
