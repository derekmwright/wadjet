// Package coordinator manages query planning, scheduling, and lifecycle tracking.
package coordinator

import (
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
)

// QueryState represents the lifecycle state of a query.
type QueryState int

const (
	QueryStatePending    QueryState = iota
	QueryStateRunning
	QueryStateCompleted
	QueryStateFailed
	QueryStateCancelled
)

func (s QueryState) String() string {
	switch s {
	case QueryStatePending:
		return "pending"
	case QueryStateRunning:
		return "running"
	case QueryStateCompleted:
		return "completed"
	case QueryStateFailed:
		return "failed"
	case QueryStateCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// StageInfo tracks the state of a single stage within a query.
type StageInfo struct {
	StageID      string
	Type         distributed.TaskType
	TotalTasks   int
	DoneTasks    int
	FailedTasks  int
	Scheduled    bool      // true once tasks have been dispatched (prevents re-scheduling)
	ScheduledAt  time.Time // when tasks were dispatched (zero if not yet scheduled)
	Dependencies []string  // stage IDs that must complete before this stage
	Results      []distributed.ResultNotification
	SeenTasks    map[string]bool // dedup: task IDs already recorded (handles retries)
}

// QueryInfo tracks the full state of a query.
type QueryInfo struct {
	QueryID     string
	SQL         string
	State       QueryState
	Stages      map[string]*StageInfo
	StageOrder  []string // topological order
	StartTime   time.Time
	EndTime     time.Time
	Error       string
	ResultFiles []string
	TotalRows   int64
}

// QueryTracker manages per-query lifecycle state.
type QueryTracker struct {
	mu      sync.RWMutex
	queries map[string]*QueryInfo
}

// NewQueryTracker creates a new query tracker.
func NewQueryTracker() *QueryTracker {
	return &QueryTracker{
		queries: make(map[string]*QueryInfo),
	}
}

// Register registers a new query.
func (qt *QueryTracker) Register(queryID, sql string, stages map[string]*StageInfo, stageOrder []string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	qt.queries[queryID] = &QueryInfo{
		QueryID:    queryID,
		SQL:        sql,
		State:      QueryStatePending,
		Stages:     stages,
		StageOrder: stageOrder,
		StartTime:  time.Now(),
	}
}

// Start marks a query as running.
func (qt *QueryTracker) Start(queryID string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	if q, ok := qt.queries[queryID]; ok {
		q.State = QueryStateRunning
	}
}

// RecordResult records a task result for a query stage. Idempotent — duplicate
// results from worker retries are silently ignored.
// Returns true if the stage is now complete.
func (qt *QueryTracker) RecordResult(result distributed.ResultNotification) (stageComplete bool) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	q, ok := qt.queries[result.QueryID]
	if !ok {
		return false
	}

	stage, ok := q.Stages[result.StageID]
	if !ok {
		return false
	}

	// Dedup: skip if this task result was already recorded (worker retry).
	if stage.SeenTasks == nil {
		stage.SeenTasks = make(map[string]bool)
	}
	if stage.SeenTasks[result.TaskID] {
		return false
	}
	stage.SeenTasks[result.TaskID] = true

	stage.Results = append(stage.Results, result)
	if result.Success {
		stage.DoneTasks++
		if result.ResultPath != "" {
			q.ResultFiles = append(q.ResultFiles, result.ResultPath)
		}
		q.TotalRows += result.NumRows
	} else {
		stage.FailedTasks++
	}

	return stage.DoneTasks+stage.FailedTasks >= stage.TotalTasks
}

// GetReadyStages returns stages whose dependencies are all complete.
func (qt *QueryTracker) GetReadyStages(queryID string) []string {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return nil
	}

	var ready []string
	for _, stageID := range q.StageOrder {
		stage := q.Stages[stageID]
		if stage.Scheduled {
			continue // already dispatched
		}
		if stage.DoneTasks+stage.FailedTasks >= stage.TotalTasks {
			continue // already done
		}

		allDepsComplete := true
		for _, dep := range stage.Dependencies {
			depStage := q.Stages[dep]
			if depStage == nil || depStage.DoneTasks < depStage.TotalTasks {
				allDepsComplete = false
				break
			}
		}
		if allDepsComplete {
			stage.Scheduled = true
			stage.ScheduledAt = time.Now()
			ready = append(ready, stageID)
		}
	}
	return ready
}

// Complete marks a query as completed.
func (qt *QueryTracker) Complete(queryID string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	if q, ok := qt.queries[queryID]; ok {
		q.State = QueryStateCompleted
		q.EndTime = time.Now()
	}
}

// Fail marks a query as failed.
func (qt *QueryTracker) Fail(queryID string, err string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	if q, ok := qt.queries[queryID]; ok {
		q.State = QueryStateFailed
		q.EndTime = time.Now()
		q.Error = err
	}
}

// Get returns the current state of a query.
func (qt *QueryTracker) Get(queryID string) *QueryInfo {
	qt.mu.RLock()
	defer qt.mu.RUnlock()
	q, ok := qt.queries[queryID]
	if !ok {
		return nil
	}
	// Return a shallow copy
	copy := *q
	return &copy
}

// StageFailed returns the error message if a completed stage has all tasks
// failed (no successful tasks). Returns "" if the stage succeeded or is
// still in progress.
func (qt *QueryTracker) StageFailed(queryID, stageID string) string {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return ""
	}
	stage, ok := q.Stages[stageID]
	if !ok || stage.DoneTasks+stage.FailedTasks < stage.TotalTasks {
		return "" // not complete yet
	}
	if stage.DoneTasks > 0 {
		return "" // at least some tasks succeeded
	}
	// All tasks failed — collect first error
	for _, r := range stage.Results {
		if r.Error != "" {
			return r.Error
		}
	}
	return "all tasks failed"
}

// IsComplete returns true if all stages of the query are done.
func (qt *QueryTracker) IsComplete(queryID string) bool {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return false
	}

	for _, stage := range q.Stages {
		if stage.DoneTasks+stage.FailedTasks < stage.TotalTasks {
			return false
		}
	}
	return true
}

// SetStageTasks updates the total task count for a stage (used when intermediate
// stage tasks are created dynamically after dependencies complete).
func (qt *QueryTracker) SetStageTasks(queryID, stageID string, count int) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return
	}
	if stage, ok := q.Stages[stageID]; ok {
		stage.TotalTasks = count
	}
}

// MarkScheduled marks a stage as already dispatched, preventing
// GetReadyStages from re-scheduling it.
func (qt *QueryTracker) MarkScheduled(queryID, stageID string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return
	}
	if stage, ok := q.Stages[stageID]; ok {
		stage.Scheduled = true
		stage.ScheduledAt = time.Now()
	}
}

// StalledStages returns stages that have been scheduled for longer than the
// given threshold but still have unreported tasks. Used by the coordinator
// watchdog to detect and fail queries where result notifications were lost.
func (qt *QueryTracker) StalledStages(queryID string, threshold time.Duration) []string {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	q, ok := qt.queries[queryID]
	if !ok || q.State != QueryStateRunning {
		return nil
	}

	now := time.Now()
	var stalled []string
	for _, stageID := range q.StageOrder {
		stage := q.Stages[stageID]
		if !stage.Scheduled || stage.ScheduledAt.IsZero() {
			continue
		}
		pending := stage.TotalTasks - stage.DoneTasks - stage.FailedTasks
		if pending > 0 && now.Sub(stage.ScheduledAt) > threshold {
			stalled = append(stalled, stageID)
		}
	}
	return stalled
}

// UpdateResultPath updates the result path for a specific task result.
// Used when inline results are materialized to S3 for downstream stages.
func (qt *QueryTracker) UpdateResultPath(queryID, stageID, taskID, path string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return
	}
	stage, ok := q.Stages[stageID]
	if !ok {
		return
	}
	for i := range stage.Results {
		if stage.Results[i].TaskID == taskID {
			stage.Results[i].ResultPath = path
			q.ResultFiles = append(q.ResultFiles, path)
			return
		}
	}
}

// CollectResultPaths gathers all result file paths from completed stages
// under a single lock acquisition. This avoids the race where a shallow
// copy's stage pointers are read without synchronization.
func (qt *QueryTracker) CollectResultPaths(queryID string) map[string][]string {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return nil
	}
	depResults := make(map[string][]string)
	for stageID, stage := range q.Stages {
		for _, r := range stage.Results {
			if r.Success && r.ResultPath != "" {
				depResults[stageID] = append(depResults[stageID], r.ResultPath)
			}
		}
	}
	return depResults
}

// Cancel marks a query as cancelled.
func (qt *QueryTracker) Cancel(queryID string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	if q, ok := qt.queries[queryID]; ok {
		q.State = QueryStateCancelled
		q.EndTime = time.Now()
	}
}

// Delete removes a query from the tracker entirely.
func (qt *QueryTracker) Delete(queryID string) {
	qt.mu.Lock()
	defer qt.mu.Unlock()
	delete(qt.queries, queryID)
}

// ReapCompleted returns and removes query IDs in a terminal state
// (completed, failed, cancelled) whose EndTime is older than maxAge.
func (qt *QueryTracker) ReapCompleted(maxAge time.Duration) []string {
	qt.mu.Lock()
	defer qt.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	var reaped []string
	for id, q := range qt.queries {
		switch q.State {
		case QueryStateCompleted, QueryStateFailed, QueryStateCancelled:
			if !q.EndTime.IsZero() && q.EndTime.Before(cutoff) {
				delete(qt.queries, id)
				reaped = append(reaped, id)
			}
		}
	}
	return reaped
}

// ActiveQueryIDs returns the set of query IDs in a non-terminal state
// (pending or running). Used by ResultCleaner to avoid deleting
// intermediate files for in-flight queries.
func (qt *QueryTracker) ActiveQueryIDs() map[string]struct{} {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	active := make(map[string]struct{})
	for id, q := range qt.queries {
		if q.State == QueryStatePending || q.State == QueryStateRunning {
			active[id] = struct{}{}
		}
	}
	return active
}

// List returns all tracked queries (shallow copies).
func (qt *QueryTracker) List() []*QueryInfo {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	result := make([]*QueryInfo, 0, len(qt.queries))
	for _, q := range qt.queries {
		copy := *q
		result = append(result, &copy)
	}
	return result
}

// StageResults returns the result notifications for a given stage.
func (qt *QueryTracker) StageResults(queryID, stageID string) []distributed.ResultNotification {
	qt.mu.RLock()
	defer qt.mu.RUnlock()

	q, ok := qt.queries[queryID]
	if !ok {
		return nil
	}
	stage, ok := q.Stages[stageID]
	if !ok {
		return nil
	}
	results := make([]distributed.ResultNotification, len(stage.Results))
	copy(results, stage.Results)
	return results
}
