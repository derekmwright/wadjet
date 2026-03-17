package memory

import (
	"math"
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
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
		{"id": int64(3), "name": "carol"},
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
	if back[0]["id"] != int64(1) {
		t.Fatalf("expected id=1 (int64), got %v (%T)", back[0]["id"], back[0]["id"])
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

	rows := []map[string]any{{"x": int64(1)}}
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

func TestSpillAllTypes(t *testing.T) {
	tracker := NewTracker("test", 1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	rows := []map[string]any{
		{
			"bool_t":   true,
			"bool_f":   false,
			"int32":    int32(42),
			"int64":    int64(1234567890),
			"float32":  float32(3.14),
			"float64":  float64(2.718281828),
			"str":      "hello world",
			"null_val": nil,
		},
	}

	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatal(err)
	}

	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 {
		t.Fatalf("expected 1 row, got %d", len(back))
	}

	row := back[0]
	if row["bool_t"] != true {
		t.Errorf("bool_t: expected true, got %v", row["bool_t"])
	}
	if row["bool_f"] != false {
		t.Errorf("bool_f: expected false, got %v", row["bool_f"])
	}
	if row["int32"] != int32(42) {
		t.Errorf("int32: expected 42, got %v (%T)", row["int32"], row["int32"])
	}
	if row["int64"] != int64(1234567890) {
		t.Errorf("int64: expected 1234567890, got %v", row["int64"])
	}
	if v, ok := row["float32"].(float32); !ok || math.Abs(float64(v)-3.14) > 0.001 {
		t.Errorf("float32: expected ~3.14, got %v (%T)", row["float32"], row["float32"])
	}
	if v, ok := row["float64"].(float64); !ok || math.Abs(v-2.718281828) > 0.0001 {
		t.Errorf("float64: expected ~2.718, got %v", row["float64"])
	}
	if row["str"] != "hello world" {
		t.Errorf("str: expected 'hello world', got %v", row["str"])
	}
	if row["null_val"] != nil {
		t.Errorf("null_val: expected nil, got %v", row["null_val"])
	}
}

func TestSpillEmptyRows(t *testing.T) {
	tracker := NewTracker("test", 1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	path, err := sm.SpillRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if path != "" {
		t.Fatalf("expected empty path for empty rows, got %q", path)
	}
}

func TestSpillLargeDataset(t *testing.T) {
	tracker := NewTracker("test", 100*1024*1024)
	sm, err := NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	n := 10000
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"id":    int64(i),
			"value": float64(i) * 1.5,
			"label": "row",
		}
	}

	path, err := sm.SpillRows(rows)
	if err != nil {
		t.Fatal(err)
	}

	back, err := ReadSpilledRows(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != n {
		t.Fatalf("expected %d rows, got %d", n, len(back))
	}
	if back[999]["id"] != int64(999) {
		t.Fatalf("expected id=999, got %v", back[999]["id"])
	}
}
