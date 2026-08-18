package worker

import (
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// TestTaskCancelledMatchesRootQueryID pins the identity mismatch that made
// query cancellation stop at the coordinator.
//
// The coordinator broadcasts cancellation under the ROOT query id
// (cleanupQuery → wadjet.cancel.<root>, internal/coordinator/coordinator.go),
// while every native-DAG stage task is published with a stage-scoped QueryID —
// "st-<stage>-<root>" (internal/coordinator/execute_stage_dag.go). Testing a
// stage task against the cancelled set by its own QueryID therefore never
// matched, so a cancelled or timed-out distributed query freed the client
// while its in-flight stage tasks ran to completion.
func TestTaskCancelledMatchesRootQueryID(t *testing.T) {
	const root = "q-a1b2c3"
	stageTask := distributed.Task{
		ID:           "task-0",
		QueryID:      fmt.Sprintf("st-%s-%s", "scan-0", root),
		ResultPrefix: fmt.Sprintf("queries/%s/%s/", root, "scan-0"),
	}
	if got := distributed.TaskRootQueryID(&stageTask); got != root {
		t.Fatalf("TaskRootQueryID = %q, want %q", got, root)
	}

	w := &Worker{cancelled: make(map[string]time.Time)}
	rootID := distributed.TaskRootQueryID(&stageTask)
	if w.taskCancelled(stageTask.QueryID, rootID) {
		t.Fatal("task reported cancelled before any cancellation")
	}

	// What the wadjet.cancel.> subscription records: the root id.
	w.cancelled[root] = time.Now()

	if !w.taskCancelled(stageTask.QueryID, rootID) {
		t.Fatal("stage task not recognized as cancelled: the cancel broadcast " +
			"carries the root query id, the task carries a stage-scoped one")
	}

	// Another query's task keeps running: cancellation is per query.
	const otherRoot = "q-zzz999"
	otherTask := distributed.Task{
		QueryID:      fmt.Sprintf("st-%s-%s", "scan-0", otherRoot),
		ResultPrefix: fmt.Sprintf("queries/%s/%s/", otherRoot, "scan-0"),
	}
	if w.taskCancelled(otherTask.QueryID, distributed.TaskRootQueryID(&otherTask)) {
		t.Error("cancelling one query cancelled another query's task")
	}

	// Legacy full-SQL pipeline tasks carry the root id directly and have no
	// query-scratch anchor to recover one from; they must still cancel.
	legacy := distributed.Task{QueryID: root}
	if got := distributed.TaskRootQueryID(&legacy); got != "" {
		t.Fatalf("TaskRootQueryID on a legacy task = %q, want \"\"", got)
	}
	if !w.taskCancelled(legacy.QueryID, "") {
		t.Error("legacy task carrying the root query id was not cancelled")
	}
}

// TestTaskCancelledRecoversRootFromInputs covers stage tasks whose only
// query-scratch anchor is their input list (consumer stages read the
// producer's queries/<root>/... output).
func TestTaskCancelledRecoversRootFromInputs(t *testing.T) {
	const root = "q-deadbeef"
	task := distributed.Task{
		QueryID:    fmt.Sprintf("st-%s-%s", "join-2", root),
		InputFiles: []string{fmt.Sprintf("queries/%s/scan-0/part-0.parquet", root)},
	}
	w := &Worker{cancelled: map[string]time.Time{root: time.Now()}}
	if !w.taskCancelled(task.QueryID, distributed.TaskRootQueryID(&task)) {
		t.Fatal("consumer stage task not recognized as cancelled")
	}
}
