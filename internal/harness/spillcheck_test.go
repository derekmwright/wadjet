package harness

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMaxTrackerPeakMB(t *testing.T) {
	dir := t.TempDir()
	worker0 := `time=2026-08-21T21:19:11.018-04:00 level=INFO msg="loaded table" table=lineitem
time=2026-08-21T21:19:21.289-04:00 level=INFO msg="task completed" task_id=a1e64df6 success=true rows=500000 duration=109ms stage_id=join-2 peak_heap_mb=166 tracker_peak_mb=16 operator_peaks="HashJoin/build=p" drift_mb=176
time=2026-08-21T21:19:22.000-04:00 level=INFO msg="task completed" task_id=b2 success=true rows=6 tracker_peak_mb=3
`
	coord := `time=2026-08-21T21:19:20.000-04:00 level=INFO msg="task completed" task_id=c3 success=true rows=1 tracker_peak_mb=48
`
	other := `this line has no tracker_peak_mb at all`
	if err := os.WriteFile(filepath.Join(dir, "worker-0.log"), []byte(worker0), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "coord.log"), []byte(coord), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "other.txt"), []byte(other), 0644); err != nil {
		t.Fatal(err)
	}

	got, err := maxTrackerPeakMB(dir)
	if err != nil {
		t.Fatalf("maxTrackerPeakMB: %v", err)
	}
	if got != 48 {
		t.Errorf("want max tracker_peak_mb=48 across both log files, got %d", got)
	}
}

func TestMaxTrackerPeakMBNoLogs(t *testing.T) {
	got, err := maxTrackerPeakMB(t.TempDir())
	if err != nil {
		t.Fatalf("maxTrackerPeakMB: %v", err)
	}
	if got != 0 {
		t.Errorf("want 0 for a dir with no log files, got %d", got)
	}
}

func TestMaxTrackerPeakMBMissingDir(t *testing.T) {
	got, err := maxTrackerPeakMB(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("maxTrackerPeakMB: %v", err)
	}
	if got != 0 {
		t.Errorf("want 0 for a missing dir (Glob returns no matches, not an error), got %d", got)
	}
}
