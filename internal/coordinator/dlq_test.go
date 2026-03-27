package coordinator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/nats-io/nats.go/jetstream"
)

func TestDLQ_ListEmpty(t *testing.T) {
	ctx := context.Background()
	js := setupTestJetStream(t)

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}

	dlq := NewDLQ(js)
	entries, err := dlq.List(ctx, 100)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestDLQ_PublishAndList(t *testing.T) {
	ctx := context.Background()
	js := setupTestJetStream(t)

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}

	// Publish some DLQ entries
	for i, entry := range []distributed.DLQEntry{
		{
			EntryID:   "entry-1",
			TaskID:    "task-1",
			QueryID:   "query-1",
			StageID:   "stage-0",
			WorkerID:  "worker-1",
			TaskType:  "scan",
			Error:     "S3 timeout",
			Reason:    "execution_error",
			Timestamp: time.Now().Add(-2 * time.Minute),
		},
		{
			EntryID:   "entry-2",
			TaskID:    "task-2",
			QueryID:   "query-1",
			StageID:   "stage-1",
			WorkerID:  "worker-2",
			TaskType:  "join",
			Error:     "task panicked: index out of range",
			Reason:    "panic",
			Timestamp: time.Now().Add(-1 * time.Minute),
		},
	} {
		data, _ := json.Marshal(entry)
		subj := distributed.DLQSubject(entry.QueryID, entry.TaskID)
		if _, err := js.Publish(ctx, subj, data); err != nil {
			t.Fatalf("publish entry %d: %v", i, err)
		}
	}

	dlq := NewDLQ(js)

	// List all
	entries, err := dlq.List(ctx, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// Most recent first
	if entries[0].EntryID != "entry-2" {
		t.Errorf("expected entry-2 first (most recent), got %s", entries[0].EntryID)
	}

	// List with limit
	limited, err := dlq.List(ctx, 1)
	if err != nil {
		t.Fatalf("List with limit: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("expected 1 entry with limit=1, got %d", len(limited))
	}
}

func TestDLQ_Get(t *testing.T) {
	ctx := context.Background()
	js := setupTestJetStream(t)

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}

	entry := distributed.DLQEntry{
		EntryID:   "entry-abc",
		TaskID:    "task-abc",
		QueryID:   "query-abc",
		StageID:   "stage-0",
		WorkerID:  "worker-1",
		TaskType:  "aggregate",
		Error:     "OOM killed",
		Reason:    "execution_error",
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(entry)
	if _, err := js.Publish(ctx, distributed.DLQSubject(entry.QueryID, entry.TaskID), data); err != nil {
		t.Fatalf("publish: %v", err)
	}

	dlq := NewDLQ(js)

	got, err := dlq.Get(ctx, "entry-abc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.TaskID != "task-abc" {
		t.Errorf("got task_id=%s, want task-abc", got.TaskID)
	}

	// Not found
	_, err = dlq.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent entry")
	}
}

func TestDLQ_PurgeAndCount(t *testing.T) {
	ctx := context.Background()
	js := setupTestJetStream(t)

	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}

	dlq := NewDLQ(js)

	// Publish an entry
	entry := distributed.DLQEntry{
		EntryID:   "entry-purge",
		TaskID:    "task-purge",
		QueryID:   "query-purge",
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(entry)
	if _, err := js.Publish(ctx, distributed.DLQSubject("query-purge", "task-purge"), data); err != nil {
		t.Fatalf("publish: %v", err)
	}

	count, err := dlq.Count(ctx)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}

	// Purge
	if err := dlq.Purge(ctx); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	count, err = dlq.Count(ctx)
	if err != nil {
		t.Fatalf("Count after purge: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count=0 after purge, got %d", count)
	}
}

// setupTestJetStream creates an embedded NATS with JetStream for testing.
func setupTestJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()

	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := distributed.NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("ConnectInProcess: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	return js
}
