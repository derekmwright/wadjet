package worker

import (
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestSweepAbandonedScratchRoots is the regression for the disk-filling half
// of #318: a worker killed hard (OOM kill, kill -9) never runs the deferred
// RemoveAll on its stage directories, so the only thing that can reclaim them
// is a later process. Scratch used to live directly in the shared temp dir,
// where an orphan was indistinguishable from a live worker's working set and
// therefore could not be swept at all — a 98 GB orphan filled a dev box.
//
// The sweep decides ownership by asking whether the pid still exists, so the
// cases below are exactly the decision boundary.
func TestSweepAbandonedScratchRoots(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp) // os.TempDir() reads TMPDIR on unix

	deadPID := findDeadPID(t)
	dirs := map[string]bool{ // path suffix → should survive the sweep
		scratchRootPrefix + strconv.Itoa(deadPID):     false,
		scratchRootPrefix + strconv.Itoa(os.Getpid()): true, // this process is alive
		scratchRootPrefix + "notanumber":              true, // unparseable: leave it
		"unrelated-dir":                               true, // not ours at all
	}
	for name := range dirs {
		full := filepath.Join(tmp, name)
		if err := os.MkdirAll(filepath.Join(full, "stage-abc"), 0o755); err != nil {
			t.Fatalf("creating %s: %v", full, err)
		}
		if err := os.WriteFile(filepath.Join(full, "stage-abc", "spill.wshf"), []byte("x"), 0o644); err != nil {
			t.Fatalf("writing spill file: %v", err)
		}
	}

	w := &Worker{logger: slog.Default()}
	w.sweepAbandonedScratchRoots()

	for name, wantSurvive := range dirs {
		_, err := os.Stat(filepath.Join(tmp, name))
		switch {
		case wantSurvive && os.IsNotExist(err):
			t.Errorf("%s was swept; a directory whose owner may be alive must be left alone", name)
		case !wantSurvive && err == nil:
			t.Errorf("%s survived; an abandoned scratch root must be reclaimed", name)
		}
	}
}

// TestNewWorkerOwnsItsScratchRoot pins the other half: with no SpillDir
// configured, a worker gets a directory of its own instead of writing into
// the shared temp dir, which is what makes an abandoned one attributable.
func TestNewWorkerOwnsItsScratchRoot(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	got := resolveScratchDir("", slog.Default())
	want := filepath.Join(tmp, scratchRootPrefix+strconv.Itoa(os.Getpid()))
	if got != want {
		t.Fatalf("SpillDir = %q, want %q", got, want)
	}
	if _, err := os.Stat(got); err != nil {
		t.Errorf("scratch root not created: %v", err)
	}

	// An explicit SpillDir is left exactly as configured — the operator
	// usually picked a dedicated volume.
	explicit := filepath.Join(tmp, "operator-chosen")
	if got := resolveScratchDir(explicit, slog.Default()); got != explicit {
		t.Errorf("configured SpillDir = %q, want %q", got, explicit)
	}
}

// TestStageDirsAreSwept covers the prefix that was missing from the boot
// sweep: stage-<taskID> is what a killed worker leaves behind, and
// "stage-cache" is the LocalStageCache root, which manages its own lifetime.
func TestStageDirsAreSwept(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"stage-task1", "stage-task2", "stage-cache", "shuffle-t1", "unrelated"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}
	w := &Worker{config: Config{SpillDir: dir}, logger: slog.Default()}
	w.sweepStaleBuildCacheFiles()

	for name, wantSurvive := range map[string]bool{
		"stage-task1": false,
		"stage-task2": false,
		"shuffle-t1":  false,
		"stage-cache": true,
		"unrelated":   true,
	} {
		_, err := os.Stat(filepath.Join(dir, name))
		switch {
		case wantSurvive && os.IsNotExist(err):
			t.Errorf("%s was swept but must survive", name)
		case !wantSurvive && err == nil:
			t.Errorf("%s survived the boot sweep; it is a prior process's orphan", name)
		}
	}
}

// findDeadPID returns a pid with no running process. It walks down from a
// high pid rather than assuming any particular number is free.
func findDeadPID(tb testing.TB) int {
	tb.Helper()
	for pid := 4_000_000; pid > 100_000; pid -= 7919 {
		if !processAlive(pid) {
			return pid
		}
	}
	tb.Skip("no free pid found to stand in for a dead worker")
	return 0
}
