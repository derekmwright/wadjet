// Package distributed provides NATS-based distributed coordination.
package distributed

// NATS subject hierarchy for Wadjet.
const (
	// Task subjects — JetStream WorkQueue retention
	SubjectTasksScan      = "wadjet.tasks.scan"
	SubjectTasksAggregate = "wadjet.tasks.aggregate"
	SubjectTasksJoin      = "wadjet.tasks.join"
	SubjectTasksSort      = "wadjet.tasks.sort"
	SubjectTasksWindow    = "wadjet.tasks.window"
	SubjectTasksShuffle   = "wadjet.tasks.shuffle"
	SubjectTasksAll       = "wadjet.tasks.>"

	// Result notifications — JetStream Interest retention
	SubjectResults    = "wadjet.results"
	SubjectResultsAll = "wadjet.results.>"

	// Worker heartbeats — Core NATS
	SubjectHeartbeat = "wadjet.workers.heartbeat"

	// Query cancellation — Core NATS
	SubjectCancel = "wadjet.cancel"

	// Catalog locks — NATS KV
	SubjectCatalogLock = "wadjet.catalog.lock"

	// Stream names
	StreamTasks   = "WADJET_TASKS"
	StreamResults = "WADJET_RESULTS"

	// KV bucket for catalog locks
	KVCatalogLocks = "wadjet_catalog_locks"
)

// TaskSubject returns the NATS subject for a task of the given type.
func TaskSubject(taskType string, queryID string, stageID string) string {
	return SubjectTasksScan[:len("wadjet.tasks.")] + taskType + "." + queryID + "." + stageID
}

// ClusterTaskSubject returns the NATS subject for a task targeted at a specific cluster.
// Format: wadjet.tasks.<clusterID>.<type>.<queryID>.<stageID>
func ClusterTaskSubject(clusterID, taskType, queryID, stageID string) string {
	return "wadjet.tasks." + clusterID + "." + taskType + "." + queryID + "." + stageID
}

// ClusterTasksFilter returns the filter subject for a worker to receive only its cluster's tasks.
func ClusterTasksFilter(clusterID string) string {
	return "wadjet.tasks." + clusterID + ".>"
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
