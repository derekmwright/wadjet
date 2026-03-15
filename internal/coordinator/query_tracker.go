// Package coordinator manages query planning, scheduling, and lifecycle tracking.
package coordinator

import (
	"sync"
	"time"

	"github.com/derekmwright/caelum/internal/distributed"
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
	Dependencies []string // stage IDs that must complete before this stage
	Results      []distributed.ResultNotification
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

// RecordResult records a task result for a query stage.
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
		if stage.DoneTasks+stage.FailedTasks >= stage.TotalTasks {
			continue // already done
		}
		if stage.DoneTasks > 0 || stage.FailedTasks > 0 {
			continue // in progress
		}

		allDepsComplete := true
		for _, dep := range stage.Dependencies {
			depStage := q.Stages[dep]
			if depStage.DoneTasks < depStage.TotalTasks {
				allDepsComplete = false
				break
			}
		}
		if allDepsComplete {
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
