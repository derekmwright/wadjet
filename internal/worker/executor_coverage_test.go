package worker

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// --- Execute dispatch and error handling ---

func TestExecuteUnsupportedTaskType(t *testing.T) {
	store := newTestStore(t, "results")
	cache := NewLRUCache(1024 * 1024)
	executor := NewExecutor(store, cache, nil)
	ctx := context.Background()

	task := distributed.Task{
		ID:      "bad-1",
		QueryID: "q1",
		StageID: "s1",
		Type:    "unknown_type",
	}

	result := executor.Execute(ctx, task, "w1")
	if result.Success {
		t.Fatal("expected failure for unsupported task type")
	}
	if result.Error == "" {
		t.Fatal("expected error message for unsupported task type")
	}
	if result.TaskID != "bad-1" {
		t.Errorf("TaskID: got %q, want %q", result.TaskID, "bad-1")
	}
	if result.WorkerID != "w1" {
		t.Errorf("WorkerID: got %q, want %q", result.WorkerID, "w1")
	}
	if result.Duration <= 0 {
		t.Error("expected non-zero duration")
	}
}
