package memory

import (
	"testing"
)

func TestSpillManager(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	rows := []map[string]any{
		{"id": float64(1), "name": "alice"},
		{"id": float64(2), "name": "bob"},
		{"id": float64(3), "name": "carol"},
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

	// Read back
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

	// Use 85% of budget
	tracker.Reserve(85)
	if !sm.ShouldSpill() {
		t.Fatal("should spill at 85% usage")
	}
}

func TestSpillManagerCleanup(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{{"x": float64(1)}}
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
