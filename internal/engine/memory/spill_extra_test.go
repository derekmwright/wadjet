package memory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSpillDir(t *testing.T) {
	tracker := NewTracker("test", 1024)
	dir := t.TempDir()
	sm, err := NewSpillManager(dir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	expected := filepath.Join(dir, "wadjet-spill")
	if sm.SpillDir() != expected {
		t.Fatalf("expected dir %q, got %q", expected, sm.SpillDir())
	}
}

func TestSpillManagerNilTracker(t *testing.T) {
	sm, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// With nil tracker, ShouldSpill should return false
	if sm.ShouldSpill() {
		t.Fatal("nil tracker should not trigger spill")
	}
}

func TestSpillManagerZeroBudget(t *testing.T) {
	tracker := NewTracker("test", 0)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Zero budget should not trigger spill
	if sm.ShouldSpill() {
		t.Fatal("zero budget should not trigger spill")
	}
}

func TestSpillManagerMultipleSpills(t *testing.T) {
	tracker := NewTracker("test", 1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	for i := 0; i < 5; i++ {
		rows := []map[string]any{
			{"id": int64(i), "name": "test"},
		}
		path, err := sm.SpillRows(rows)
		if err != nil {
			t.Fatalf("spill %d: %v", i, err)
		}
		if path == "" {
			t.Fatalf("spill %d: empty path", i)
		}
	}

	files := sm.SpilledFiles()
	if len(files) != 5 {
		t.Fatalf("expected 5 spill files, got %d", len(files))
	}
}

func TestSpillRoundtripIntType(t *testing.T) {
	tracker := NewTracker("test", 1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Go int (not int64) should be handled
	rows := []map[string]any{
		{"count": 42}, // plain int
	}
	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatal(err)
	}

	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	// int becomes int64 on read-back
	if back[0]["count"] != int64(42) {
		t.Fatalf("expected int64(42), got %v (%T)", back[0]["count"], back[0]["count"])
	}
}

func TestSpillRoundtripFallbackType(t *testing.T) {
	tracker := NewTracker("test", 1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	// Unsupported type should fall back to string representation
	rows := []map[string]any{
		{"data": []int{1, 2, 3}}, // slice type -> fmt.Sprintf
	}
	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatal(err)
	}

	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if back[0]["data"] != "[1 2 3]" {
		t.Fatalf("expected stringified slice, got %v", back[0]["data"])
	}
}

func TestReadSpilledRowsNonexistent(t *testing.T) {
	_, err := ReadSpilledRows("/nonexistent/path.bin")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestSpillManagerCleanupAlreadyCleaned(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{{"x": int64(1)}}
	sm.SpillRows(rows)

	// First cleanup
	if err := sm.Cleanup(); err != nil {
		t.Fatalf("first cleanup failed: %v", err)
	}

	// Second cleanup should succeed (no files)
	if err := sm.Cleanup(); err != nil {
		t.Fatalf("second cleanup failed: %v", err)
	}
}

func TestSpillManagerCleanupWithMissingFile(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{{"x": int64(1)}}
	path, _ := sm.SpillRows(rows)

	// Manually remove the file before cleanup
	os.Remove(path)

	// Cleanup should report error for missing file
	err = sm.Cleanup()
	if err == nil {
		t.Fatal("expected error for missing file during cleanup")
	}
}

func TestNewSpillManagerInvalidDir(t *testing.T) {
	// Try creating in a read-only location (if possible on this platform)
	_, err := NewSpillManager("/dev/null/impossible", nil)
	if err == nil {
		t.Error("expected error for impossible directory")
	}
}

func TestSpilledFilesIsCopy(t *testing.T) {
	tracker := NewTracker("test", 1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	rows := []map[string]any{{"x": int64(1)}}
	sm.SpillRows(rows)

	files1 := sm.SpilledFiles()
	files2 := sm.SpilledFiles()

	// Modifying the returned slice should not affect the internal state
	files1[0] = "modified"
	files3 := sm.SpilledFiles()
	if files3[0] == "modified" {
		t.Error("SpilledFiles should return a copy, not reference internal slice")
	}
	_ = files2
}

func TestShouldSpillThreshold(t *testing.T) {
	tracker := NewTracker("test", 100)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	// 0% - should not spill
	if sm.ShouldSpill() {
		t.Error("0% should not spill")
	}

	// 79% - should not spill (threshold is 80%)
	tracker.Reserve(79)
	if sm.ShouldSpill() {
		t.Error("79% should not spill")
	}

	// Release back to 0 and reserve 80%
	tracker.Release(79)
	tracker.Reserve(80)
	if sm.ShouldSpill() {
		t.Error("exactly 80% should not spill (> not >=)")
	}

	// 81% - should spill
	tracker.Release(80)
	tracker.Reserve(81)
	if !sm.ShouldSpill() {
		t.Error("81% should trigger spill")
	}
}
