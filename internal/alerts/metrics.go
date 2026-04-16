package alerts

import "github.com/prometheus/client_golang/prometheus"

var (
	metricEvaluations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wadjet_alert_evaluations_total",
		Help: "Count of alert evaluations by alert and status.",
	}, []string{"alert", "status"})

	metricEvalDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wadjet_alert_evaluation_duration_seconds",
		Help:    "Time to evaluate an alert (query + sink delivery).",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
	}, []string{"alert"})

	metricRowsMatched = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wadjet_alert_rows_matched",
		Help: "Row count returned by the most recent evaluation of each alert.",
	}, []string{"alert"})

	metricListErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "wadjet_alert_scheduler_list_errors_total",
		Help: "Scheduler catalog.ListAlerts errors.",
	})

	metricWebhookRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wadjet_alert_webhook_retries_total",
		Help: "Count of webhook retry attempts per alert.",
	}, []string{"alert"})
)

// Collectors returns all Prometheus collectors for the alerts package so the
// caller can register them with their custom registry (mirrors the project's
// metrics.New() pattern — no promauto / global registry usage).
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		metricEvaluations,
		metricEvalDuration,
		metricRowsMatched,
		metricListErrors,
		metricWebhookRetries,
	}
}
