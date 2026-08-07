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

	// Per-task progress — Core NATS. Published by workers from inside
	// task hot loops (every ~2s when progress is being made). Coord
	// subscribes via SubjectTaskProgressAll and feeds into per-stage
	// progress detection so a long-running task that's still pushing
	// rows is distinguishable from a wedged task that's stopped making
	// forward progress.
	SubjectTaskProgress    = "wadjet.task.progress"
	SubjectTaskProgressAll = "wadjet.task.progress.>"

	// Async-upload completion (streaming exchange Phase B) — Core NATS.
	// Published by workers when a task's background stage-output uploads
	// land; coordinator flips per-key durability bits. Best-effort: a
	// lost message leaves keys non-durable, which only means the
	// coordinator stays conservative about them.
	SubjectUploadComplete = "wadjet.uploads.complete"

	// Deferred-upload release (shuffle durability "lazy",
	// docs/design/shuffle-durability.md) — Core NATS. Published by the
	// coordinator when a root query's scratch needs its durable S3 copies
	// after all (a consumer reported a missing input whose producer is
	// still alive, or the coordinator itself needs to read a stage
	// output). Workers holding queued lazy uploads for that root start
	// them on receipt. Best-effort: a lost message costs one task-retry
	// round (the retry re-triggers the release), never correctness.
	SubjectUploadRelease = "wadjet.uploads.release"

	// Eager consumer dispatch (docs/design/eager-consumer-dispatch.md) —
	// Core NATS. Coordinator republishes a compact per-producer-task file
	// manifest as each task of an eager edge completes; consumer tasks'
	// manifest sources subscribe per (root query, producer stage).
	// Metadata-only; payload bytes never flow through these subjects.
	SubjectEagerManifest = "wadjet.eager"

	// Query cancellation — Core NATS
	SubjectCancel = "wadjet.cancel"

	// Query completion — Core NATS. Published by the coordinator after a
	// query finishes (success, failure, or cancellation) so workers can
	// release per-query resources (LocalStageCache spill files, etc).
	// Distinct from CancelSubject: CancelSubject still means "stop running
	// new tasks for this query"; CompleteSubject means "the query is done,
	// you may free its caches."
	SubjectComplete = "wadjet.complete"

	// Catalog locks — NATS KV
	SubjectCatalogLock = "wadjet.catalog.lock"

	// Query active check — Core NATS request/reply
	// Workers ask coordinator if a query is still active before executing
	// stale tasks pulled from JetStream.
	SubjectQueryActive = "wadjet.query.active"

	// Worker profile collection — Core NATS request/reply
	SubjectProfileStart   = "wadjet.workers.profile.start"
	SubjectProfileCollect = "wadjet.workers.profile.collect"

	// Dead-letter queue — failed tasks for inspection/retry
	SubjectDLQ    = "wadjet.dlq"
	SubjectDLQAll = "wadjet.dlq.>"

	// Priority task lane — latency-critical, dimension-class tasks
	// (dyn-filter emitter scans) that must never queue behind bulk scan
	// fan-out. Separate subject space + stream because WADJET_TASKS is a
	// WorkQueue stream whose main consumer filters wadjet.tasks.> — a
	// second consumer under the same prefix would overlap, which WorkQueue
	// retention forbids. Workers drain this lane with dedicated slots
	// outside MaxConcurrent (docs/design/attach-on-arrival-dynamic-filters.md).
	SubjectPriTasksAll = "wadjet.pritasks.>"

	// Stream names
	StreamTasks    = "WADJET_TASKS"
	StreamPriTasks = "WADJET_TASKS_PRI"
	StreamResults  = "WADJET_RESULTS"
	StreamDLQ      = "WADJET_DLQ"

	// KV bucket for catalog locks
	KVCatalogLocks = "wadjet_catalog_locks"
)

// TaskSubject returns the NATS subject for a task of the given type.
func TaskSubject(taskType string, queryID string, stageID string) string {
	return SubjectTasksScan[:len("wadjet.tasks.")] + taskType + "." + queryID + "." + stageID
}

// PriTaskSubject is TaskSubject's counterpart on the priority lane.
func PriTaskSubject(taskType string, queryID string, stageID string) string {
	return "wadjet.pritasks." + taskType + "." + queryID + "." + stageID
}

// ClusterPriTaskSubject is ClusterTaskSubject's counterpart on the priority lane.
func ClusterPriTaskSubject(clusterID, taskType, queryID, stageID string) string {
	return "wadjet.pritasks." + clusterID + "." + taskType + "." + queryID + "." + stageID
}

// ClusterPriTasksFilter returns the priority-lane filter subject for a
// worker to receive only its cluster's priority tasks.
func ClusterPriTasksFilter(clusterID string) string {
	return "wadjet.pritasks." + clusterID + ".>"
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

// DLQSubject returns the NATS subject for a DLQ entry.
func DLQSubject(queryID, taskID string) string {
	return SubjectDLQ + "." + queryID + "." + taskID
}

// CancelSubject returns the NATS subject for cancelling a specific query.
func CancelSubject(queryID string) string {
	return SubjectCancel + "." + queryID
}

// CancelSubjectAll returns the wildcard subject for all cancellation messages.
func CancelSubjectAll() string {
	return SubjectCancel + ".>"
}

// CompleteSubject returns the NATS subject signalling that a query has
// finished and per-query worker state may be released.
func CompleteSubject(queryID string) string {
	return SubjectComplete + "." + queryID
}

// CompleteSubjectAll returns the wildcard subject for all completion messages.
func CompleteSubjectAll() string {
	return SubjectComplete + ".>"
}

// TaskProgressSubject returns the NATS subject for a specific task's
// per-task progress messages. Published by the worker; subscribed
// via wildcard by the coordinator.
func TaskProgressSubject(queryID, taskID string) string {
	return SubjectTaskProgress + "." + queryID + "." + taskID
}

// EagerManifestSubject returns the NATS subject on which the coordinator
// publishes ProducerTaskManifest messages for one producer stage of one
// root query. Consumer tasks subscribe with the exact subject (no
// wildcard); root and stage IDs are sanitized to NATS token characters
// by construction (UUIDs / stage-N names).
func EagerManifestSubject(rootQueryID, stageID string) string {
	return SubjectEagerManifest + "." + rootQueryID + "." + stageID
}
