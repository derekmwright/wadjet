// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// intakeLogRecorder captures the worker's log records with their attributes.
type intakeLogRecorder struct {
	mu      sync.Mutex
	records []map[string]string
}

func (h *intakeLogRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (h *intakeLogRecorder) Handle(_ context.Context, r slog.Record) error {
	m := map[string]string{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, m)
	h.mu.Unlock()
	return nil
}
func (h *intakeLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *intakeLogRecorder) WithGroup(string) slog.Handler      { return h }

func (h *intakeLogRecorder) matching(msg string) []map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []map[string]string
	for _, r := range h.records {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

// startIntakeLogWorker starts a worker on JetStream intake with a recording
// logger. before runs after the streams exist and the worker is built, and
// before Worker.Start.
func startIntakeLogWorker(t *testing.T, ctx context.Context, before func(js jetstream.JetStream, w *Worker)) (jetstream.JetStream, *intakeLogRecorder) {
	t.Helper()
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	en, err := distributed.NewEmbeddedNATS(cfg, slog.New(slog.DiscardHandler))
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
	rec := &intakeLogRecorder{}
	w := New(Config{NATSUrl: en.ClientURL(), MaxConcurrent: 2, CacheBytes: 16 * 1024 * 1024}, store, nc, js, slog.New(rec))
	if before != nil {
		before(js, w)
	}
	workerCtx, workerCancel := context.WithCancel(context.Background())
	t.Cleanup(workerCancel)
	if err := w.Start(workerCtx); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)
	return js, rec
}

var intakeLanes = []struct{ lane, stream, durable string }{
	{"main", distributed.StreamTasks, "tasks"},
	{"pri-leaf", distributed.StreamPriTasks, "pritasks-leaf"},
	{"pri-deep", distributed.StreamPriTasks, "pritasks-deep"},
}

// TestIntakeIteratorWarningNamesLaneAndConsumer: the WARN a lane's intake
// loop logs when its message iterator returns an error names the lane and the
// lane's durable consumer (#857e). Deleting each durable at the server makes
// every lane's iterator return an error once (the server answers the waiting
// pull with 409 Consumer Deleted); that reaches the same log call as the
// missing-heartbeat error, without a 30 s silence.
func TestIntakeIteratorWarningNamesLaneAndConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	js, rec := startIntakeLogWorker(t, ctx, nil)

	for _, l := range intakeLanes {
		deadline := time.Now().Add(10 * time.Second)
		for {
			c, err := js.Consumer(ctx, l.stream, l.durable)
			if err == nil {
				if ci, err := c.Info(ctx); err == nil && ci.NumWaiting == 1 {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("lane %s: no pull outstanding on %s", l.lane, l.durable)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for _, l := range intakeLanes {
		if err := js.DeleteConsumer(ctx, l.stream, l.durable); err != nil {
			t.Fatalf("delete %s: %v", l.durable, err)
		}
	}

	const warn = "task intake: iterator error"
	deadline := time.Now().Add(10 * time.Second)
	for len(rec.matching(warn)) < len(intakeLanes) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	got := map[string]string{}
	for _, r := range rec.matching(warn) {
		if r["lane"] == "" {
			t.Errorf("iterator WARN without a lane: %v", r)
			continue
		}
		got[r["lane"]] = r["consumer"]
	}
	for _, l := range intakeLanes {
		c, ok := got[l.lane]
		switch {
		case !ok:
			t.Errorf("no iterator WARN for lane %s (got %v)", l.lane, got)
		case c != l.durable:
			t.Errorf("lane %s: consumer = %q, want %q", l.lane, c, l.durable)
		}
	}
	for _, r := range rec.matching("task intake loop starting") {
		if r["lane"] == "" || r["consumer"] == "" {
			t.Errorf("intake start line without lane or consumer: %v", r)
		}
	}
}

// TestIntakeTaskLinesNameTheirLane: the two per-delivery lines the intake
// path logs can be attributed to a lane: a payload that does not decode is
// logged with its subject (each lane's subjects are disjoint), and a
// redelivered task is logged with its stream and durable consumer (#857e).
func TestIntakeTaskLinesNameTheirLane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const cancelledQuery = "q-intake-log-cancelled"
	js, rec := startIntakeLogWorker(t, ctx, func(js jetstream.JetStream, w *Worker) {
		// The redelivered task's query counts as cancelled, so the worker
		// logs the redelivery and then Terms it without executing anything.
		w.cancelledMu.Lock()
		w.cancelled[cancelledQuery] = time.Now()
		w.cancelledMu.Unlock()

		// A redelivery before the worker starts: take the task once through
		// the worker's own durable (same configuration as Worker.Start) and
		// Nak it, so the worker's first delivery of it is delivery 2.
		c, err := js.CreateOrUpdateConsumer(ctx, distributed.StreamTasks, jetstream.ConsumerConfig{
			Durable: "tasks", FilterSubject: distributed.SubjectTasksAll,
			AckPolicy: jetstream.AckExplicitPolicy, AckWait: 10 * time.Minute, MaxDeliver: 3,
		})
		if err != nil {
			t.Fatalf("pre-create consumer: %v", err)
		}
		data, err := distributed.Marshal(distributed.Task{ID: "t-redelivered", QueryID: cancelledQuery})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, distributed.SubjectTasksScan, data); err != nil {
			t.Fatal(err)
		}
		batch, err := c.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for m := range batch.Messages() {
			n++
			if err := m.Nak(); err != nil {
				t.Fatal(err)
			}
		}
		if n != 1 {
			t.Fatalf("fetched %d messages, want 1", n)
		}
	})
	deepSubject := distributed.PriTaskSubject("deep", "stage", "q", "s")
	if _, err := js.Publish(ctx, deepSubject, []byte("not-a-task")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) &&
		(len(rec.matching("task redelivered — prior delivery lost or unacked")) == 0 ||
			len(rec.matching("failed to unmarshal task")) == 0) {
		time.Sleep(20 * time.Millisecond)
	}
	re := rec.matching("task redelivered — prior delivery lost or unacked")
	if len(re) != 1 {
		t.Fatalf("%d redelivery lines, want 1", len(re))
	}
	if re[0]["stream"] != distributed.StreamTasks || re[0]["consumer"] != "tasks" {
		t.Errorf("redelivery line stream/consumer = %q/%q, want %q/tasks (%v)", re[0]["stream"], re[0]["consumer"], distributed.StreamTasks, re[0])
	}
	un := rec.matching("failed to unmarshal task")
	if len(un) != 1 {
		t.Fatalf("%d unmarshal-failure lines, want 1", len(un))
	}
	if un[0]["subject"] != deepSubject {
		t.Errorf("unmarshal-failure subject = %q, want %q (%v)", un[0]["subject"], deepSubject, un[0])
	}
}
