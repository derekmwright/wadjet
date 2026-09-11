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

// maxTrackerPeakMB reads *.log directly under logsDir and returns the greatest
// task-completion tracker_peak_mb. Synchronous completion logging precedes
// cleanup, avoiding the 10s heartbeat's missed-short-query window.
// This measures tracked peak, not spill bytes: ForceReserve can exceed budget,
// and reaching a threshold alone does not establish an actual disk eviction.
// Keep the distinction when using the value as the large-slice spill proxy.
// See docs/internals/harness-task-peak-log-signal.md for the design.
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
