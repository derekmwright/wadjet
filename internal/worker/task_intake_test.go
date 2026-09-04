package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// waitForIntakeToDrain waits until `consumer` has DELIVERED at least burst
// messages and then holds nothing pending or ack-pending, and fails when it
// does not reach that state before the deadline.
//
// The order of those two conditions is the whole of #857. The loop this
// replaces exited on `NumPending == 0 && NumAckPending == 0` and only THEN
// read Delivered, and that predicate is equally true of a consumer that has
// not seen anything yet — a fact TestAnIdleConsumerSatisfiesTheOldDrainTest
// asserts directly, and exactly the state CI reported on run 33872290777
// ("consumer delivered 0 < published 50", pending and ack-pending both zero,
// 0.06 s in). On a 2-core runner the first poll can land in the window before
// the consumer has accounted for the burst, so the test declared the drain
// finished and then failed on its own premise.
//
// Engagement first inverts that: a consumer that has seen nothing keeps
// waiting, and a burst that is genuinely never delivered still fails — at the
// deadline, with the same numbers. No retry, no widened timeout; the
// PREDICATE was wrong, not the budget.
//
// What makes this a test defect rather than a production one is that the
// intake CANNOT lose a task published before its iterator binds:
// Worker.Start creates the durable consumer synchronously (worker.go's
// CreateOrUpdateConsumer, whose error aborts Start) before it returns, the
// consumer's DeliverPolicy is the zero value DeliverAllPolicy, and
// WADJET_TASKS is a WorkQueuePolicy stream with MaxAge 1h — so such a task is
// PENDING, not dropped. TestTaskIntakeDeliversTasksPublishedBeforeItBinds is
// the fixture for that claim.
func waitForIntakeToDrain(t *testing.T, ctx context.Context, consumer jetstream.Consumer, burst uint64, lane string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatalf("%s consumer info: %v", lane, err)
		}
		if info.NumRedelivered != 0 {
			t.Fatalf("%s intake redelivered %d messages — a first delivery was dropped unacked",
				lane, info.NumRedelivered)
		}
		if info.Delivered.Consumer >= burst && info.NumPending == 0 && info.NumAckPending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s intake did not drain: delivered=%d of %d, pending=%d ack_pending=%d redelivered=%d",
				lane, info.Delivered.Consumer, burst, info.NumPending, info.NumAckPending, info.NumRedelivered)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

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

	waitForIntakeToDrain(t, ctx, consumer, burst, "main")
}

// TestTaskIntakeDeliversTasksPublishedBeforeItBinds is the fixture for the
// claim #857 turns on: a task published BEFORE the intake's message iterator
// exists is delivered, not lost.
//
// That claim is what makes the flake a test defect and not a production one,
// and rule 10 says the impossibility gets a fixture that attempts it. Here the
// whole burst is published before Worker.Start is ever called — the worst case
// the CI failure would represent if it were real — and the intake must still
// deliver all of it. It holds because Start creates the durable consumer
// synchronously, because DeliverPolicy is the zero value DeliverAllPolicy, and
// because WADJET_TASKS is a WorkQueuePolicy stream that retains an undelivered
// message for MaxAge. Change any one of those — bind the consumer inside the
// taskLoop goroutine, set DeliverNewPolicy, make the stream interest-based —
// and this fails while TestTaskIntakeDeliversAllWithoutLoss above still
// passes, because that one publishes after Start has returned.
func TestTaskIntakeDeliversTasksPublishedBeforeItBinds(t *testing.T) {
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

	// Everything is published while NO worker exists: no consumer, no
	// iterator, nothing bound to the subject at all.
	const burst = 50
	for i := 0; i < burst; i++ {
		if _, err := js.Publish(ctx, distributed.SubjectTasksScan,
			[]byte(fmt.Sprintf("not-a-task-%03d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
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

	consumer, err := js.Consumer(ctx, distributed.StreamTasks, "tasks")
	if err != nil {
		t.Fatalf("lookup consumer: %v", err)
	}
	waitForIntakeToDrain(t, ctx, consumer, burst, "main")
}

// TestAnIdleConsumerSatisfiesTheOldDrainTest is #857's evidence, and it needs
// no worker, no burst and no race to produce it: `NumPending == 0 &&
// NumAckPending == 0` — the condition the drain loop used to exit on — is TRUE
// of a consumer that has never been handed anything.
//
// So that predicate never meant "the intake drained"; it meant "the intake
// holds nothing right now", which is also the state before it starts. CI hit
// the second reading and the test reported the first one's failure
// ("delivered 0 < published 50"). Locally it never fires — 100 runs with
// -race and 100 pinned to a single contended core all passed — because the
// window is a scheduling one, which is why the fix is the predicate rather
// than a reproduction.
func TestAnIdleConsumerSatisfiesTheOldDrainTest(t *testing.T) {
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

	consumer, err := js.CreateOrUpdateConsumer(ctx, distributed.StreamTasks, jetstream.ConsumerConfig{
		Durable:       "tasks",
		FilterSubject: distributed.SubjectTasksAll,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       10 * time.Minute,
		MaxDeliver:    3,
	})
	if err != nil {
		t.Fatalf("create consumer: %v", err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if !(info.NumPending == 0 && info.NumAckPending == 0) {
		t.Fatalf("a consumer that has been handed nothing reports pending=%d ack_pending=%d; "+
			"this test exists to show that state is indistinguishable from a finished drain",
			info.NumPending, info.NumAckPending)
	}
	if info.Delivered.Consumer != 0 {
		t.Fatalf("an idle consumer reports Delivered.Consumer = %d", info.Delivered.Consumer)
	}
	// Which is to say: the old loop would have exited HERE, on a consumer
	// that had been handed nothing, and reported "consumer delivered 0 <
	// published 50" — the CI failure, verbatim. waitForIntakeToDrain waits
	// for delivery FIRST, so this state keeps it waiting.
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

	// Same wait, same reason: this loop carried the identical predicate and
	// so the identical false exit (#857).
	waitForIntakeToDrain(t, ctx, consumer, burst, "pri-leaf")
}
