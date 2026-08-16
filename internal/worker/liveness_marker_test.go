package worker

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/derekmwright/wadjet/internal/metrics"
)

func gaugeValue(tb testing.TB, g prometheus.Gauge) float64 {
	tb.Helper()
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		tb.Fatalf("reading gauge: %v", err)
	}
	return m.GetGauge().GetValue()
}

// TestTrackTaskGauge verifies the active-task set and the
// wadjet_worker_active_tasks gauge move together: the deploy-side
// silent-stall watchdog reads the gauge off /metrics to distinguish a
// wedged-while-busy worker from an idle one.
func TestTrackTaskGauge(t *testing.T) {
	m := metrics.New()
	w := &Worker{
		activeTasks: make(map[string]struct{}),
		metrics:     m,
	}

	w.trackTaskStart("t1")
	w.trackTaskStart("t2")
	if got := gaugeValue(t, m.WorkerActive); got != 2 {
		t.Fatalf("gauge after two starts = %v, want 2", got)
	}
	w.trackTaskEnd("t1")
	if got := gaugeValue(t, m.WorkerActive); got != 1 {
		t.Fatalf("gauge after one end = %v, want 1", got)
	}
	w.trackTaskEnd("t2")
	if got := gaugeValue(t, m.WorkerActive); got != 0 {
		t.Fatalf("gauge after all ends = %v, want 0", got)
	}
	if len(w.activeTasks) != 0 {
		t.Fatalf("activeTasks not empty: %v", w.activeTasks)
	}
}

// TestTrackTaskGauge_NilMetrics: tracking must not panic when metrics are
// unattached (embedded/standalone paths that never call SetMetrics).
func TestTrackTaskGauge_NilMetrics(t *testing.T) {
	w := &Worker{activeTasks: make(map[string]struct{})}
	w.trackTaskStart("t1")
	w.trackTaskEnd("t1")
}

// TestEmitLivenessMarker verifies the marker line lands in the log with
// the fields the silent-stall watchdog greps for. The line must be
// emitted unconditionally — no change detection — so one call must
// produce exactly one line.
func TestEmitLivenessMarker(t *testing.T) {
	var buf bytes.Buffer
	w := &Worker{
		activeTasks: map[string]struct{}{"t1": {}},
		logger:      slog.New(slog.NewTextHandler(&buf, nil)),
	}

	w.emitLivenessMarker(7)

	out := buf.String()
	if strings.Count(out, "liveness marker") != 1 {
		t.Fatalf("want exactly one marker line, got: %q", out)
	}
	for _, field := range []string{"seq=7", "active_tasks=1", "goroutines=", "utime_ms=", "stime_ms="} {
		if !strings.Contains(out, field) {
			t.Errorf("marker line missing %s: %q", field, out)
		}
	}
}
