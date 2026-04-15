package harness

import (
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// TestRunsCleanThroughFakeCluster exercises the harness pipeline
// (collector -> comparison -> exit code) end-to-end against synthetic
// heartbeats, without needing a real cluster.
func TestRunsCleanThroughFakeCluster(t *testing.T) {
	collector := NewCollector()

	collector.StartWindow("q01")
	for i := 0; i < 5; i++ {
		collector.Observe(distributed.WorkerHeartbeat{
			WorkerID:      "fake-w0",
			MemoryUsed:    int64((i + 1) * 100 * 1024 * 1024),
			Mallocs:       uint64(1000 * (i + 1)),
			NumGoroutines: 50,
			Timestamp:     time.Now().Add(time.Duration(i) * 100 * time.Millisecond),
		})
	}
	m := collector.EndWindow("q01")
	if m.PeakHeapMB < 500 {
		t.Errorf("expected peak >= 500 MB, got %d", m.PeakHeapMB)
	}
	if m.AllocCount != 4000 {
		t.Errorf("expected allocs delta 4000, got %d", m.AllocCount)
	}

	// Build a baseline that matches and check Compare returns no regressions.
	bf := &BaselineFile{
		Version: 1,
		Queries: map[string]QueryBaseline{
			"q01": {
				WallMsP50:            5000,
				WallMsTolerancePct:   100,
				PeakHeapMB:           500,
				PeakHeapTolerancePct: 100,
				RowCount:             0,
				RowChecksum:          "",
			},
		},
	}
	deltas := bf.Compare(m)
	for _, d := range deltas {
		if d.Status == "REGRESS" {
			t.Errorf("unexpected regression: %+v", d)
		}
	}

	// Smoke test computeExitCode.
	r := RunResult{Queries: []QueryMeasurement{m}}
	if code := computeExitCode(r); code != ExitOK {
		t.Errorf("expected ExitOK, got %d", code)
	}
}

func TestComputeExitCodeCorrectness(t *testing.T) {
	r := RunResult{
		Regressions: []QueryDelta{
			{Query: "q05", Metric: "row_count", Status: "REGRESS"},
		},
	}
	if code := computeExitCode(r); code != ExitCorrectness {
		t.Errorf("expected ExitCorrectness (%d), got %d", ExitCorrectness, code)
	}
}

func TestComputeExitCodeRegression(t *testing.T) {
	r := RunResult{
		Regressions: []QueryDelta{
			{Query: "q05", Metric: "wall_ms", Status: "REGRESS"},
		},
	}
	if code := computeExitCode(r); code != ExitRegression {
		t.Errorf("expected ExitRegression (%d), got %d", ExitRegression, code)
	}
}

func TestComputeExitCodeHang(t *testing.T) {
	r := RunResult{
		Hangs: []string{"q05"},
	}
	if code := computeExitCode(r); code != ExitRegression {
		t.Errorf("expected ExitRegression (%d), got %d", ExitRegression, code)
	}
}

func TestDebugPortsMapping(t *testing.T) {
	c := &Cluster{
		cfg:      ClusterConfig{PgAddr: ":15433"},
		httpPort: 8080,
	}
	c.workers = []*managedProcess{
		{role: "worker-0", debugPort: 9200},
		{role: "worker-1", debugPort: 9201},
	}
	ports := c.DebugPorts()
	if ports["coord"] != 8080 {
		t.Errorf("coord port: want 8080, got %d", ports["coord"])
	}
	if ports["worker-0"] != 9200 {
		t.Errorf("worker-0 port: want 9200, got %d", ports["worker-0"])
	}
	if ports["worker-1"] != 9201 {
		t.Errorf("worker-1 port: want 9201, got %d", ports["worker-1"])
	}
	if len(ports) != 3 {
		t.Errorf("want 3 entries, got %d", len(ports))
	}
}
