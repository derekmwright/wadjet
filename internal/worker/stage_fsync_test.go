package worker

import (
	"os"
	"path/filepath"
	"testing"
)

// syncStageFile must be a no-op by default (the 2026-08-11 shuffle
// finalize writeback-storm fix) and must still sync when the restore
// switch is set — both on a live file, where a real Sync succeeds, and
// on a closed file, where a real Sync errors (proving the call actually
// reached fsync).
func TestSyncStageFile(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "stage.wshf"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("data"); err != nil {
		t.Fatal(err)
	}
	f.Close() // Sync on a closed fd errors — makes the two modes observable

	orig := stageFsyncEnabled
	defer func() { stageFsyncEnabled = orig }()

	stageFsyncEnabled = false
	if err := syncStageFile(f); err != nil {
		t.Fatalf("default mode must not touch the file, got %v", err)
	}

	stageFsyncEnabled = true
	if err := syncStageFile(f); err == nil {
		t.Fatal("restore mode must call Sync (closed fd should error)")
	}
}
