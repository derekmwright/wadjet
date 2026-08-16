package worker

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"runtime/pprof"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// heapStats is an STW-free snapshot of the MemStats-equivalent heap
// gauges, read via runtime/metrics: alloc==HeapAlloc, inuse==HeapInuse,
// idle==HeapIdle, sys==HeapSys, gcCycles==NumGC.
type heapStats struct {
	alloc, inuse, idle, sys uint64
	gcCycles                uint64
}

func heapProfilerStats() heapStats {
	s := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	runtimemetrics.Read(s)
	objects, unused := s[0].Value.Uint64(), s[1].Value.Uint64()
	free, released := s[2].Value.Uint64(), s[3].Value.Uint64()
	return heapStats{
		alloc:    objects,
		inuse:    objects + unused,
		idle:     free + released,
		sys:      objects + unused + free + released,
		gcCycles: s[4].Value.Uint64(),
	}
}

// longTaskWatchAt is how long a task may run before the watcher fires a
// one-shot snapshot (goroutine stacks + heap + sidecar). 5 min is well
// above all SF10/SF100 short queries (Q16 ~1 min, Q06 ~1.5 min) and
// catches Q21/Q18-class wall regressions without firing on healthy
// workloads.
const longTaskWatchAt = 5 * time.Minute

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
	w.bgWG.Add(1)
	go func() {
		defer w.bgWG.Done()
		var lastWrite time.Time
		ticker := time.NewTicker(heapPressureProfilePoll)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			// Raw gauge: the profiler documents the high-heap state itself,
			// including evictable cache residency the adjusted gauge ignores.
			if !memory.RawHeapBackpressureActive() {
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

	// STW-free heap stats (see taskPeakHeapTracker doc comment): the
	// sidecar numbers ride runtime/metrics; the runtime.GC stays because
	// pprof's live-heap profile is only accurate post-collection, and the
	// 30s rate limit bounds its cost.
	allocBefore := heapProfilerStats().alloc

	runtime.GC()

	after := heapProfilerStats()

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
		allocBefore/1024/1024,
		after.alloc/1024/1024,
		after.sys/1024/1024,
		after.idle/1024/1024,
		after.inuse/1024/1024,
		after.gcCycles,
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
		"heap_alloc_before_gc_mb", allocBefore/1024/1024,
		"heap_alloc_after_gc_mb", after.alloc/1024/1024,
		"active_tasks", len(activeIDs))
	return nil
}

// startLongTaskWatcher spawns a one-shot goroutine that fires a
// snapshot if the task is still active after longTaskWatchAt elapses.
// The snapshot includes the live goroutine profile (stacks), live heap
// profile, and a sidecar with task/query/stage IDs + heap stats.
//
// Returns a cancel func the caller must invoke (typically via defer)
// when the task completes — that prevents the snapshot from firing on
// short tasks. The cancel is idempotent.
//
// Motivation: 2026-05-22 SF100 Q21 ran 13m50s with zero
// HeapBackpressureActive events. The existing heap_profiler.go fires
// only above 70% GOMEMLIMIT; sub-threshold long tasks (CPU-bound,
// blocked goroutines, plan inefficiencies) produce no diagnostic data.
// This watcher closes that gap with a once-per-task snapshot at the
// 5-min mark.
//
// Disabled when SpillDir is unset.
func (w *Worker) startLongTaskWatcher(ctx context.Context, task distributed.Task) func() {
	if w.config.SpillDir == "" {
		return func() {}
	}
	dir := filepath.Join(w.config.SpillDir, "heap-profiles")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		w.logger.Warn("long-task watcher: mkdir failed", "dir", dir, "err", err)
		return func() {}
	}
	cancelCh := make(chan struct{})
	var cancelOnce func()
	{
		ch := cancelCh
		cancelOnce = func() {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	}

	w.bgWG.Add(1)
	go func() {
		defer w.bgWG.Done()
		select {
		case <-ctx.Done():
			return
		case <-cancelCh:
			return
		case <-time.After(longTaskWatchAt):
		}
		if err := w.writeLongTaskSnapshot(dir, task); err != nil {
			w.logger.Warn("long-task snapshot failed",
				"task_id", task.ID, "err", err)
		}
	}()
	return cancelOnce
}

// writeLongTaskSnapshot dumps a goroutine profile, a heap profile, and
// a sidecar describing the task. Run when a task crosses
// longTaskWatchAt. Files share the long-task-<workerID>-<task_id>-...
// naming so they're easy to grep alongside the heap-pressure-fired
// profiles in the same directory.
func (w *Worker) writeLongTaskSnapshot(dir string, task distributed.Task) error {
	ts := time.Now().UnixMilli()
	base := fmt.Sprintf("long-task-%s-%s-%d", w.config.WorkerID, task.ID, ts)
	goroutinePath := filepath.Join(dir, base+".goroutines.txt")
	heapPath := filepath.Join(dir, base+".heap.pb.gz")
	sidecarPath := filepath.Join(dir, base+".meta.txt")

	// Goroutine dump (text format, debug=2 for full stacks). This is
	// the load-bearing artifact for diagnosing wall-time-without-heap
	// failures: blocked I/O, channel waits, GC-mark-assist parks, lock
	// contention all surface in the stacks.
	gf, err := os.Create(goroutinePath)
	if err != nil {
		return fmt.Errorf("create goroutine profile: %w", err)
	}
	if gErr := pprof.Lookup("goroutine").WriteTo(gf, 2); gErr != nil {
		_ = gf.Close()
		_ = os.Remove(goroutinePath)
		return fmt.Errorf("write goroutine profile: %w", gErr)
	}
	if cErr := gf.Close(); cErr != nil {
		return fmt.Errorf("close goroutine profile: %w", cErr)
	}

	// Heap profile alongside for memory state at the snapshot point.
	runtime.GC()
	hf, err := os.Create(heapPath)
	if err != nil {
		return fmt.Errorf("create heap profile: %w", err)
	}
	if pErr := pprof.WriteHeapProfile(hf); pErr != nil {
		_ = hf.Close()
		_ = os.Remove(heapPath)
		return fmt.Errorf("write heap profile: %w", pErr)
	}
	if cErr := hf.Close(); cErr != nil {
		return fmt.Errorf("close heap profile: %w", cErr)
	}

	// STW-free (see heapProfilerStats): a 5-minute task usually means
	// pressure, exactly when a ReadMemStats STW stretches worst.
	hs := heapProfilerStats()
	sidecar := fmt.Sprintf(
		"worker_id=%s\n"+
			"task_id=%s\n"+
			"query_id=%s\n"+
			"stage_id=%s\n"+
			"stage_type=%s\n"+
			"task_type=%s\n"+
			"timestamp_unix_ms=%d\n"+
			"timestamp_rfc3339=%s\n"+
			"watch_threshold=%s\n"+
			"heap_alloc_mb=%d\n"+
			"heap_sys_mb=%d\n"+
			"heap_inuse_mb=%d\n"+
			"gc_num=%d\n"+
			"num_goroutines=%d\n",
		w.config.WorkerID,
		task.ID,
		task.QueryID,
		task.StageID,
		task.StageType,
		task.Type,
		ts,
		time.UnixMilli(ts).UTC().Format(time.RFC3339Nano),
		longTaskWatchAt,
		hs.alloc/1024/1024,
		hs.sys/1024/1024,
		hs.inuse/1024/1024,
		hs.gcCycles,
		runtime.NumGoroutine(),
	)
	if sErr := os.WriteFile(sidecarPath, []byte(sidecar), 0o644); sErr != nil {
		w.logger.Warn("long-task sidecar write failed",
			"path", sidecarPath, "err", sErr)
	}

	w.logger.Info("long-task snapshot written",
		"task_id", task.ID,
		"query_id", task.QueryID,
		"stage_id", task.StageID,
		"goroutines", runtime.NumGoroutine(),
		"heap_alloc_mb", hs.alloc/1024/1024,
		"goroutine_path", goroutinePath,
		"heap_path", heapPath)
	return nil
}
