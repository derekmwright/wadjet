package worker

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestExecuteFragment_GatherSink_EmptySourcePublishesTerminal regresses the
// race where a fragment task with empty InputFiles AND an OpGatherSink
// terminal would short-circuit before opening the sink, leaving the
// coordinator's gather receiver hanging on a missing terminal until the
// 10-minute gather timeout. The fix opens + finalizes the gather sink even
// on the empty-source path so a terminal marker is published immediately.
func TestExecuteFragment_GatherSink_EmptySourcePublishesTerminal(t *testing.T) {
	ctx := context.Background()

	// Embedded NATS so the executor can publish to a real reply subject and
	// we can subscribe + assert on the terminal arrival.
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	en, err := distributed.NewEmbeddedNATS(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	const subject = "test.fragment.empty-gather"
	var (
		mu       sync.Mutex
		received []distributed.GatherBatchMsg
		done     = make(chan struct{})
	)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var msg distributed.GatherBatchMsg
		if err := distributed.Unmarshal(m.Data, &msg); err != nil {
			return
		}
		mu.Lock()
		received = append(received, msg)
		terminal := msg.Terminal
		mu.Unlock()
		if terminal {
			close(done)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { sub.Unsubscribe() })

	store := objstore.NewMemStore()
	executor := NewExecutor(store, NewLRUCache(1024*1024), nil)
	executor.SetNATSConn(nc)

	// Empty InputFiles forces the empty-source short-circuit.
	task := distributed.Task{
		ID:        "frag-empty-gather",
		QueryID:   "q-empty",
		StageID:   "agg",
		Type:      distributed.TaskTypeStage,
		StageType: "final_aggregate",
		Operators: []distributed.OpSpec{
			{
				Type:        distributed.OpShuffleSource,
				InputAlias:  "upstream",
				InputFiles:  nil, // empty: upstream produced nothing
				InputBucket: "any",
			},
			{
				Type:        distributed.OpHashAggregate,
				GroupByCols: []string{"g"},
				Aggregates:  []distributed.AggSpec{{Func: "sum", InputCol: "v", OutputCol: "total"}},
				MergeMode:   true,
			},
			{
				Type:         distributed.OpGatherSink,
				ReplySubject: subject,
			},
		},
	}
	result := &distributed.ResultNotification{TaskID: task.ID}
	if err := executor.executeStage(ctx, task, result); err != nil {
		t.Fatalf("executeStage: %v", err)
	}
	if result.NumRows != 0 {
		t.Errorf("NumRows: got %d, want 0 (empty source)", result.NumRows)
	}

	// Terminal must arrive promptly. Without the fix this blocks until the
	// test's own timeout fires.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("gather terminal not received from empty-source fragment")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("messages: got %d, want exactly 1 (terminal only, no batches)", len(received))
	}
	if !received[0].Terminal {
		t.Errorf("expected terminal-flag set on the lone message, got %+v", received[0])
	}
	if received[0].Err != "" {
		t.Errorf("terminal carried error: %q (empty source is not an error)", received[0].Err)
	}
}
