// Package metrics provides Prometheus instrumentation for Wadjet.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metrics for a Wadjet node.
// Uses a custom registry to avoid conflicts with the global default.
type Metrics struct {
	Registry *prometheus.Registry
	// Query metrics
	QueriesTotal     *prometheus.CounterVec
	QueryDuration    *prometheus.HistogramVec
	QueryRowsScanned *prometheus.CounterVec
	QueryBytesRead   prometheus.Counter
	ActiveQueries    prometheus.Gauge

	// Scanner metrics
	FilesScanned      prometheus.Counter
	FilesPruned       prometheus.Counter
	RowGroupsScanned  prometheus.Counter
	RowGroupsPruned   prometheus.Counter
	PartitionsScanned prometheus.Counter
	PartitionsPruned  prometheus.Counter

	// Worker metrics
	WorkerTasks     *prometheus.CounterVec
	WorkerTaskDuration *prometheus.HistogramVec
	WorkerActive    prometheus.Gauge
	WorkerMemory    prometheus.Gauge

	// Coordinator metrics
	RegisteredWorkers prometheus.Gauge

	// Pipeline metrics
	BatchesProcessed prometheus.Counter
	RowsOutput       prometheus.Counter

	// Memory/Spill metrics
	SpillEvents       prometheus.Counter
	SpillBytesWritten prometheus.Counter
	MemoryBudgetBytes prometheus.Gauge
	MemoryUsedBytes   prometheus.Gauge

	// Cache metrics
	CacheHits   prometheus.Counter
	CacheMisses prometheus.Counter
	CacheBytes  prometheus.Gauge
}

// New creates and registers all Prometheus metrics with a custom registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		QueriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wadjet",
			Name:      "queries_total",
			Help:      "Total number of queries executed.",
		}, []string{"status"}),

		QueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "wadjet",
			Name:      "query_duration_seconds",
			Help:      "Query execution duration in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120},
		}, []string{"type"}),

		QueryRowsScanned: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wadjet",
			Name:      "query_rows_scanned_total",
			Help:      "Total rows scanned across all queries.",
		}, []string{"table"}),

		QueryBytesRead: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Name:      "query_bytes_read_total",
			Help:      "Total bytes read from object storage.",
		}),

		ActiveQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wadjet",
			Name:      "active_queries",
			Help:      "Number of currently executing queries.",
		}),

		FilesScanned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "scan",
			Name:      "files_scanned_total",
			Help:      "Total Parquet files scanned.",
		}),

		FilesPruned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "scan",
			Name:      "files_pruned_total",
			Help:      "Total Parquet files pruned (skipped by pushdown).",
		}),

		RowGroupsScanned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "scan",
			Name:      "row_groups_scanned_total",
			Help:      "Total row groups scanned.",
		}),

		RowGroupsPruned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "scan",
			Name:      "row_groups_pruned_total",
			Help:      "Total row groups pruned by stats.",
		}),

		PartitionsScanned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "scan",
			Name:      "partitions_scanned_total",
			Help:      "Total partitions scanned.",
		}),

		PartitionsPruned: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "scan",
			Name:      "partitions_pruned_total",
			Help:      "Total partitions pruned.",
		}),

		WorkerTasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "tasks_total",
			Help:      "Total tasks executed by this worker.",
		}, []string{"type", "status"}),

		WorkerTaskDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "task_duration_seconds",
			Help:      "Worker task execution duration.",
			Buckets:   []float64{0.01, 0.1, 0.5, 1, 5, 10, 30, 60},
		}, []string{"type"}),

		WorkerActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "active_tasks",
			Help:      "Number of tasks currently being executed.",
		}),

		WorkerMemory: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "memory_bytes",
			Help:      "Current memory usage of the worker.",
		}),

		RegisteredWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wadjet",
			Subsystem: "coordinator",
			Name:      "registered_workers",
			Help:      "Number of workers with recent heartbeats.",
		}),

		BatchesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "pipeline",
			Name:      "batches_processed_total",
			Help:      "Total record batches processed through pipelines.",
		}),

		RowsOutput: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "pipeline",
			Name:      "rows_output_total",
			Help:      "Total result rows output to clients.",
		}),

		SpillEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "spill_events_total",
			Help:      "Total number of spill-to-disk events.",
		}),

		SpillBytesWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "spill_bytes_written_total",
			Help:      "Total bytes written to spill files.",
		}),

		MemoryBudgetBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "memory_budget_bytes",
			Help:      "Configured per-task memory budget in bytes.",
		}),

		MemoryUsedBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wadjet",
			Subsystem: "worker",
			Name:      "memory_used_bytes",
			Help:      "Current memory usage tracked by the memory tracker.",
		}),

		CacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "cache",
			Name:      "hits_total",
			Help:      "Cache hits.",
		}),

		CacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "wadjet",
			Subsystem: "cache",
			Name:      "misses_total",
			Help:      "Cache misses.",
		}),

		CacheBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "wadjet",
			Subsystem: "cache",
			Name:      "bytes",
			Help:      "Current cache size in bytes.",
		}),
	}

	// Register all metrics with custom registry
	reg.MustRegister(
		m.QueriesTotal,
		m.QueryDuration,
		m.QueryRowsScanned,
		m.QueryBytesRead,
		m.ActiveQueries,
		m.FilesScanned,
		m.FilesPruned,
		m.RowGroupsScanned,
		m.RowGroupsPruned,
		m.PartitionsScanned,
		m.PartitionsPruned,
		m.WorkerTasks,
		m.WorkerTaskDuration,
		m.WorkerActive,
		m.WorkerMemory,
		m.RegisteredWorkers,
		m.BatchesProcessed,
		m.RowsOutput,
		m.SpillEvents,
		m.SpillBytesWritten,
		m.MemoryBudgetBytes,
		m.MemoryUsedBytes,
		m.CacheHits,
		m.CacheMisses,
		m.CacheBytes,
	)

	return m
}

// Handler returns an HTTP handler for the /metrics endpoint using the custom registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// QueryTimer returns a function that records query duration when called.
func (m *Metrics) QueryTimer(queryType string) func() {
	start := time.Now()
	m.ActiveQueries.Inc()
	return func() {
		m.ActiveQueries.Dec()
		m.QueryDuration.WithLabelValues(queryType).Observe(time.Since(start).Seconds())
	}
}

// TaskTimer returns a function that records task duration when called.
func (m *Metrics) TaskTimer(taskType string) func(success bool) {
	start := time.Now()
	m.WorkerActive.Inc()
	return func(success bool) {
		m.WorkerActive.Dec()
		m.WorkerTaskDuration.WithLabelValues(taskType).Observe(time.Since(start).Seconds())
		status := "success"
		if !success {
			status = "failure"
		}
		m.WorkerTasks.WithLabelValues(taskType, status).Inc()
	}
}
