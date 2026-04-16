// Package alerts implements CREATE ALERT DDL runtime: scheduler, sinks
// (webhook, alert_history table), and Prometheus metrics. Alerts run
// exclusively on the leader coordinator; see internal/coordinator for
// lifecycle wiring. The DDL grammar is parsed in internal/planner/sql.
package alerts
