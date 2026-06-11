package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"
)

// startHeapDumper runs a goroutine that writes a Go heap profile to disk
// every interval. Each profile is written to a fresh file, then fsync'd
// before move-into-place so the latest snapshot survives an OOM-kill.
//
// Enabled by env var WADJET_HEAP_DUMP_INTERVAL (Go duration string,
// e.g. "5s"). Profiles are written under WADJET_HEAP_DUMP_DIR (default
// /tmp/wadjet-heap). Each snapshot also logs MemStats so the trace is
// readable without pprof.
//
// Why disk-snapshot rather than `task completed` slog: the Q18 SF10 OOM
// (project_q18_sf10_native_dag_oom_2026-04-24) showed that slog writes
// during query execution don't reach disk before SIGKILL. fsync on each
// snapshot guarantees the latest profile survives.
func startHeapDumper(ctx context.Context, logger *slog.Logger) {
	intervalStr := os.Getenv("WADJET_HEAP_DUMP_INTERVAL")
	if intervalStr == "" {
		return
	}
	interval, err := time.ParseDuration(intervalStr)
	// 100ms floor: sub-second cadences exist to catch allocation bursts
	// that live and die between 1s frames (the 512 MiB edge OOM allocated
	// ~280 MB in <500ms). The dump itself costs ~10-30ms; an operator
	// asking for 100ms accepts that overhead knowingly.
	if err != nil || interval < 100*time.Millisecond {
		logger.Warn("invalid WADJET_HEAP_DUMP_INTERVAL", "value", intervalStr, "err", err)
		return
	}
	dir := os.Getenv("WADJET_HEAP_DUMP_DIR")
	if dir == "" {
		dir = "/tmp/wadjet-heap"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		logger.Warn("heap dump dir create failed", "dir", dir, "err", err)
		return
	}

	pid := os.Getpid()
	logger.Info("heap dumper started",
		"interval", interval, "dir", dir, "pid", pid)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		seq := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++
				dumpHeap(logger, dir, pid, seq)
			}
		}
	}()
}

// dumpHeap writes a heap profile to <dir>/heap-<pid>-<seq>.pprof and
// logs the current MemStats line. fsync'd so it survives crash.
func dumpHeap(logger *slog.Logger, dir string, pid, seq int) {
	// Force a fresh MemStats sample first (cheap) so the profile and the
	// logged stats refer to the same memory snapshot.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	heapMB := ms.HeapAlloc / (1024 * 1024)
	sysMB := ms.Sys / (1024 * 1024)

	path := filepath.Join(dir, "heap-"+strconv.Itoa(pid)+"-"+strconv.Itoa(seq)+".pprof")
	f, err := os.Create(path)
	if err != nil {
		logger.Warn("heap dump create failed", "path", path, "err", err)
		return
	}
	if err := pprof.Lookup("heap").WriteTo(f, 0); err != nil {
		logger.Warn("heap profile write failed", "path", path, "err", err)
		f.Close()
		return
	}
	if err := f.Sync(); err != nil {
		logger.Warn("heap profile sync failed", "path", path, "err", err)
	}
	f.Close()

	// One-line summary makes the profile sequence readable in coord.log
	// without opening pprof; pair the seq with file path for cross-ref.
	logger.Info("heap snapshot",
		"seq", seq, "path", path,
		"heap_mb", heapMB, "sys_mb", sysMB,
		"num_gc", ms.NumGC, "goroutines", runtime.NumGoroutine())
}

