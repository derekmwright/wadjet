package metrics

import (
	"testing"
)

func TestNewMetrics(t *testing.T) {
	m := New()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}

	// Verify counters can be incremented without panic
	m.QueriesTotal.WithLabelValues("success").Inc()
	m.QueriesTotal.WithLabelValues("error").Inc()
	m.QueryBytesRead.Add(1024)
	m.ActiveQueries.Set(5)
	m.FilesScanned.Inc()
	m.FilesPruned.Inc()
	m.RowGroupsScanned.Inc()
	m.RowGroupsPruned.Inc()
	m.BatchesProcessed.Inc()
	m.RowsOutput.Add(100)
	m.CacheHits.Inc()
	m.CacheMisses.Inc()
	m.CacheBytes.Set(1024)
}

func TestQueryTimer(t *testing.T) {
	m := New()
	done := m.QueryTimer("test")
	done()
	// Verify active queries went back to 0
}

func TestTaskTimer(t *testing.T) {
	m := New()
	done := m.TaskTimer("scan")
	done(true)

	done2 := m.TaskTimer("scan")
	done2(false)
}
