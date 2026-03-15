// Package distributed provides NATS-based distributed coordination.
package distributed

// NATS subject hierarchy for Caelum.
const (
	// Task subjects — JetStream WorkQueue retention
	SubjectTasksScan      = "caelum.tasks.scan"
	SubjectTasksAggregate = "caelum.tasks.aggregate"
	SubjectTasksJoin      = "caelum.tasks.join"
	SubjectTasksSort      = "caelum.tasks.sort"
	SubjectTasksWindow    = "caelum.tasks.window"
	SubjectTasksAll       = "caelum.tasks.>"

	// Result notifications — JetStream Interest retention
	SubjectResults    = "caelum.results"
	SubjectResultsAll = "caelum.results.>"

	// Worker heartbeats — Core NATS
	SubjectHeartbeat = "caelum.workers.heartbeat"

	// Query cancellation — Core NATS
	SubjectCancel = "caelum.cancel"

	// Catalog locks — NATS KV
	SubjectCatalogLock = "caelum.catalog.lock"

	// Stream names
	StreamTasks   = "CAELUM_TASKS"
	StreamResults = "CAELUM_RESULTS"

	// KV bucket for catalog locks
	KVCatalogLocks = "caelum_catalog_locks"
)

// TaskSubject returns the NATS subject for a task of the given type.
func TaskSubject(taskType string, queryID string, stageID string) string {
	return SubjectTasksScan[:len("caelum.tasks.")] + taskType + "." + queryID + "." + stageID
}

// ResultSubject returns the NATS subject for a result notification.
func ResultSubject(queryID, stageID, taskID string) string {
	return SubjectResults + "." + queryID + "." + stageID + "." + taskID
}

// QueryResultSubject returns the wildcard subject for all results of a query.
func QueryResultSubject(queryID string) string {
	return SubjectResults + "." + queryID + ".>"
}

// CancelSubject returns the NATS subject for cancelling a specific query.
func CancelSubject(queryID string) string {
	return SubjectCancel + "." + queryID
}

// CancelSubjectAll returns the wildcard subject for all cancellation messages.
func CancelSubjectAll() string {
	return SubjectCancel + ".>"
}
