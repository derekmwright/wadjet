package worker

import (
	"log/slog"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestATombstonedQueryStagesUploadIsRefusedAndCounted is #625's M3.
//
// The coordinator's per-query cleanup is a one-shot LIST+DELETE. Round-0
// measured a cancelled DAG query whose prefix was empty at cancel AND at
// ExecuteSQL's return, with one .wshf landing afterwards — a straggler
// task finishing after the sweep and re-creating the prefix that was just
// reclaimed. Nothing revisits it, so the bytes live to the 1-hour TTL.
//
// A one-shot cleanup cannot win that race, so the loser refuses instead:
// the worker declines to upload a stage output for a query it has already
// tombstoned. The ASYNC upload path has done this since the q22-R2 stall
// (uploadManager.queryState returns nil for a tombstoned root); the three
// SYNCHRONOUS stage uploads did not. Re-arming the sweep was rejected — it
// is the same one-shot with a second chance, and it does not bound the
// number of stragglers.
func TestATombstonedQueryStagesUploadIsRefusedAndCounted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	e := &Executor{
		logger:  logger,
		store:   objstore.NewMemStore(),
		uploads: newUploadManager(nil, nil, logger),
	}

	task := distributed.Task{ID: "task-1", QueryID: "q-terminal",
		ResultPrefix: "queries/q-terminal/scan-0/"}
	root := distributed.TaskRootQueryID(&task)
	if root == "" {
		t.Fatalf("fixture task has no root query id")
	}

	// Live query: the upload proceeds.
	if e.stageOutputRefusedForTerminalQuery(&task) {
		t.Fatal("a live query's stage output was refused")
	}

	before := StageUploadsRefused.Load()
	e.uploads.CancelQuery(root)

	// Terminal query: the straggler is refused, and counted.
	if !e.stageOutputRefusedForTerminalQuery(&task) {
		t.Fatalf("a tombstoned query's straggler stage upload was allowed; it re-creates the "+
			"queries/%s/* prefix the coordinator just reclaimed (#625 M3)", root)
	}
	if got := StageUploadsRefused.Load() - before; got != 1 {
		t.Fatalf("StageUploadsRefused moved by %d, want 1 — an operator has no signal that "+
			"stage outputs are being dropped", got)
	}

	// A different query is unaffected: the tombstone is per root.
	other := distributed.Task{ID: "task-2", QueryID: "q-live",
		ResultPrefix: "queries/q-live/scan-0/"}
	if e.stageOutputRefusedForTerminalQuery(&other) {
		t.Fatal("one query's tombstone refused another query's upload")
	}
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }
