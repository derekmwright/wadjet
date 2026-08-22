package harness

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

// trackerPeakMBPattern matches the "tracker_peak_mb=<N>" slog attribute that
// internal/worker/worker.go emits on every "task completed" line whose
// TaskStats.TrackerPeak (the per-task memory.Tracker high-water mark) is
// nonzero. See maxTrackerPeakMB for why this is the reliable signal for
// "did this run put a task's tracked memory under real pressure."
var trackerPeakMBPattern = regexp.MustCompile(`tracker_peak_mb=(\d+)`)

// maxTrackerPeakMB scans every *.log file directly under logsDir (coord.log,
// worker-N.log — written by Cluster.spawn via cmd.Stdout/Stderr) and returns
// the largest tracker_peak_mb value logged by any task.
//
// Why this instead of the worker heartbeat's SpillDiskUsed: workers
// heartbeat on a fixed 10s cadence (internal/worker/worker.go), while a
// single local-mode query — even one whose build side is forced through the
// spill path — routinely completes in well under a second. A per-query
// heartbeat window can open and close between two ticks and see nothing, not
// because nothing spilled but because the sample missed it; worse, the
// spilling task's own spill directory is removed via `defer
// os.RemoveAll(...)` (executor_fragment.go) the instant the task returns, so
// even a "wait and re-check" strategy loses the race against a task that
// finishes in milliseconds.
//
// tracker_peak_mb has neither problem: it's written to the log file
// synchronously when the task completes (collectTaskStats reads
// tracker.Peak() before the deferred cleanup runs), so there's no sampling
// window to miss. And because memory.Tracker.Reserve() rejects any request
// that would push `used` over `budget` (internal/engine/memory/tracker.go),
// a task's tracker_peak_mb can never exceed its configured budget — a task
// that only ever needed a few MB peaks at a few MB, while a task whose build
// side doesn't fit saturates at the ceiling. buildPartitioned
// (internal/engine/exec/join_partition_arrival.go) only reaches that
// ceiling by way of a failed Reserve() that falls through to
// spillUntilCanReserve, i.e. an actual partition eviction to disk — so
// observing the configured budget in tracker_peak_mb is direct evidence the
// spill path ran, not just that a budget was configured.
func maxTrackerPeakMB(logsDir string) (int64, error) {
	matches, err := filepath.Glob(filepath.Join(logsDir, "*.log"))
	if err != nil {
		return 0, err
	}
	var max int64
	for _, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			m := trackerPeakMBPattern.FindStringSubmatch(scanner.Text())
			if m == nil {
				continue
			}
			mb, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				continue
			}
			if mb > max {
				max = mb
			}
		}
		f.Close()
	}
	return max, nil
}
