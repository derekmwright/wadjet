package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// TestTaskIntakeDeliversAllWithoutLoss is the regression test for the Q08
// AckWait stall (2026-07-21): the old 500ms-polled Fetch intake could
// discard server-delivered messages client-side at the request-expiry
// boundary — unacked, invisible in logs, redelivered only after the
// 10-minute AckWait. The persistent Messages() iterator must drain an
// intake burst larger than the concurrency limit with every message
// delivered exactly once: nothing pending, nothing ack-pending, nothing
// redelivered.
//
// The burst uses undecodable payloads on purpose: handleTask Terms them
// before touching the executor, so the test isolates the intake loop
// (fetch → slot gate → handleTask → terminal ack) from task execution.
func TestTaskIntakeDeliversAllWithoutLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setup streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}

	w := New(Config{
		NATSUrl:       en.ClientURL(),
		MaxConcurrent: 2,
		CacheBytes:    16 * 1024 * 1024,
	}, store, nc, js, logger)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	t.Cleanup(workerCancel)
	if err := w.Start(workerCtx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)

	const burst = 50
	for i := 0; i < burst; i++ {
		if _, err := js.Publish(ctx, distributed.SubjectTasksScan,
			[]byte(fmt.Sprintf("not-a-task-%03d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	consumer, err := js.Consumer(ctx, distributed.StreamTasks, "tasks")
	if err != nil {
		t.Fatalf("lookup consumer: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		if info.NumPending == 0 && info.NumAckPending == 0 {
			if info.NumRedelivered != 0 {
				t.Fatalf("intake redelivered %d messages — a first delivery was dropped unacked", info.NumRedelivered)
			}
			if got := info.Delivered.Consumer; got < burst {
				t.Fatalf("consumer delivered %d < published %d", got, burst)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("intake did not drain: pending=%d ack_pending=%d redelivered=%d",
				info.NumPending, info.NumAckPending, info.NumRedelivered)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestPriorityLaneDrains proves the latency-critical lane is live end to
// end: a worker consumes wadjet.pritasks.> through its dedicated consumer
// and slot pool (docs/design/attach-on-arrival-dynamic-filters.md). Same
// undecodable-payload trick as above — this tests intake wiring, not
// execution.
func TestPriorityLaneDrains(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setup streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}

	w := New(Config{
		NATSUrl:       en.ClientURL(),
		MaxConcurrent: 1,
		CacheBytes:    16 * 1024 * 1024,
	}, store, nc, js, logger)
	workerCtx, workerCancel := context.WithCancel(context.Background())
	t.Cleanup(workerCancel)
	if err := w.Start(workerCtx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)

	const burst = 10
	for i := 0; i < burst; i++ {
		if _, err := js.Publish(ctx, distributed.PriTaskSubject("leaf", "stage", "q", "s"),
			[]byte(fmt.Sprintf("not-a-task-%03d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	consumer, err := js.Consumer(ctx, distributed.StreamPriTasks, "pritasks-leaf")
	if err != nil {
		t.Fatalf("lookup pri consumer: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		if info.NumPending == 0 && info.NumAckPending == 0 {
			if got := info.Delivered.Consumer; got < burst {
				t.Fatalf("pri consumer delivered %d < published %d", got, burst)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("priority lane did not drain: pending=%d ack_pending=%d",
				info.NumPending, info.NumAckPending)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
