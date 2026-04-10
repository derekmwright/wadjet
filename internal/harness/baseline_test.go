package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestBaselineRoundTrip(t *testing.T) {
	bf := BaselineFile{
		Version:    1,
		CapturedAt: "2026-04-08T12:00:00Z",
		CapturedOn: "test fixture",
		Queries: map[string]QueryBaseline{
			"q05": {
				WallMsP50:              118000,
				WallMsTolerancePct:     25,
				PeakHeapMB:             14336,
				PeakHeapTolerancePct:   15,
				AllocCount:             47000000000,
				AllocCountTolerancePct: 20,
				SpillBytesWritten:      8500000000,
				SpillTolerancePct:      30,
				RowCount:               5,
				RowChecksum:            "abc123",
			},
		},
		ProjectionFactors: map[string]Projection{
			"large_slice": {
				WallMsMultiplier:     0.20,
				HeapMultiplier:       0.55,
				AllocCountMultiplier: 0.50,
				SpillMultiplier:      0.45,
			},
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.json")
	if err := bf.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadBaseline(path)
	if err != nil {
		t.Fatalf("LoadBaseline: %v", err)
	}

	got, _ := json.Marshal(loaded)
	want, _ := json.Marshal(&bf)
	if string(got) != string(want) {
		t.Errorf("round trip mismatch:\nwant=%s\ngot =%s", want, got)
	}
}

func TestLoadBaselineMissingFile(t *testing.T) {
	_, err := LoadBaseline("/nonexistent/path.json")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !os.IsNotExist(err) {
		t.Errorf("expected IsNotExist, got %v", err)
	}
}

func TestProjectLocalToGolden(t *testing.T) {
	bf := &BaselineFile{
		Version: 1,
		ProjectionFactors: map[string]Projection{
			"large_slice": {
				WallMsMultiplier:     0.20,
				HeapMultiplier:       0.55,
				AllocCountMultiplier: 0.50,
				SpillMultiplier:      0.45,
			},
		},
	}
	local := QueryMeasurement{
		WallMs:     24000,
		PeakHeapMB: 7884,
		AllocCount: 23500000000,
		SpillBytes: 3825000000,
	}
	projected, err := bf.Project("large_slice", local)
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if projected.WallMs != 120000 {
		t.Errorf("WallMs: want 120000, got %d", projected.WallMs)
	}
	if projected.PeakHeapMB != 14334 && projected.PeakHeapMB != 14335 && projected.PeakHeapMB != 14336 {
		t.Errorf("PeakHeapMB: want ~14336, got %d", projected.PeakHeapMB)
	}
}

func TestProjectMissingSlice(t *testing.T) {
	bf := &BaselineFile{Version: 1, ProjectionFactors: map[string]Projection{}}
	_, err := bf.Project("nonexistent", QueryMeasurement{})
	if err == nil {
		t.Fatal("expected error for missing slice")
	}
}

func TestCompareDetectsRegression(t *testing.T) {
	bf := &BaselineFile{
		Version: 1,
		Queries: map[string]QueryBaseline{
			"q05": {
				WallMsP50:              120000,
				WallMsTolerancePct:     25,
				PeakHeapMB:             14336,
				PeakHeapTolerancePct:   15,
				AllocCount:             47000000000,
				AllocCountTolerancePct: 20,
				SpillBytesWritten:      8500000000,
				SpillTolerancePct:      30,
				RowCount:               5,
				RowChecksum:            "abc123",
			},
		},
	}

	// Within tolerance — pass
	good := QueryMeasurement{
		Query: "q05", WallMs: 130000, PeakHeapMB: 14000, AllocCount: 48000000000,
		SpillBytes: 8000000000, RowCount: 5, RowChecksum: "abc123",
	}
	deltas := bf.Compare(good)
	for _, d := range deltas {
		if d.Status != "PASS" {
			t.Errorf("expected PASS, got %v for %s", d.Status, d.Metric)
		}
	}

	// 2x slower — should regress
	bad := QueryMeasurement{
		Query: "q05", WallMs: 240000, PeakHeapMB: 14000, AllocCount: 48000000000,
		SpillBytes: 8000000000, RowCount: 5, RowChecksum: "abc123",
	}
	deltas = bf.Compare(bad)
	var found bool
	for _, d := range deltas {
		if d.Metric == "wall_ms" && d.Status == "REGRESS" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected wall_ms REGRESS, got %+v", deltas)
	}
}

func TestCompareDetectsCorrectnessFailure(t *testing.T) {
	bf := &BaselineFile{
		Version: 1,
		Queries: map[string]QueryBaseline{
			"q05": {RowCount: 5, RowChecksum: "abc123"},
		},
	}
	wrongRows := QueryMeasurement{Query: "q05", RowCount: 7, RowChecksum: "abc123"}
	deltas := bf.Compare(wrongRows)
	var found bool
	for _, d := range deltas {
		if d.Metric == "row_count" && d.Status == "REGRESS" {
			found = true
		}
	}
	if !found {
		t.Error("expected row_count REGRESS")
	}
}
