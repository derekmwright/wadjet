package worker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/distributed"
)

// #318 poison-task defense. The rules under test, simulated without ever
// OOM-killing anything: a "prior attempt that killed its worker" is exactly
// an attempt record that was written before execution and never cleared —
// which is what a real crash leaves behind, because clearPoisonRecord runs
// only on graceful completion.

func TestPoisonVerdict(t *testing.T) {
	rec := func(degraded bool) *attemptRecord {
		return &attemptRecord{WorkerID: "w1", PID: 1234, Delivery: 1, Degraded: degraded}
	}
	cases := []struct {
		name     string
		prior    *attemptRecord
		delivery uint64
		want     poisonRung
	}{
		{"first delivery", nil, 1, rungNormal},
		{"first delivery ignores stale record", rec(false), 1, rungNormal},
		{"redelivery without record: prior delivery lost pre-execution", nil, 2, rungNormal},
		{"redelivery after mid-execution death: degraded retry", rec(false), 2, rungDegraded},
		{"redelivery after DEGRADED attempt died: quarantine", rec(true), 2, rungQuarantine},
		{"third delivery, prior normal death: degraded retry", rec(false), 3, rungDegraded},
		{"third delivery, prior degraded death: quarantine", rec(true), 3, rungQuarantine},
	}
	for _, tc := range cases {
		if got := poisonVerdict(tc.prior, tc.delivery); got != tc.want {
			t.Errorf("%s: got rung %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestKVAttemptStoreRoundTrip(t *testing.T) {
	ctx, en, _ := setupWorkerNATS(t)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}

	store, err := newKVAttemptStore(ctx, js)
	if err != nil {
		t.Fatalf("newKVAttemptStore: %v", err)
	}

	if _, ok, err := store.Lookup(ctx, "task-1"); err != nil || ok {
		t.Fatalf("empty lookup: ok=%v err=%v", ok, err)
	}
	rec := attemptRecord{WorkerID: "w1", PID: 42, Delivery: 2, Degraded: true, StartedAt: time.Now().UTC()}
	if err := store.Record(ctx, "task-1", rec); err != nil {
		t.Fatalf("Record: %v", err)
	}
	got, ok, err := store.Lookup(ctx, "task-1")
	if err != nil || !ok {
		t.Fatalf("lookup after record: ok=%v err=%v", ok, err)
	}
	if got.WorkerID != rec.WorkerID || got.PID != rec.PID || got.Delivery != rec.Delivery || !got.Degraded {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, rec)
	}
	if err := store.Clear(ctx, "task-1"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, ok, _ := store.Lookup(ctx, "task-1"); ok {
		t.Fatal("record survived Clear")
	}
	// Clearing an absent key is not an error (the common case: graceful
	// completion after the record was TTL'd or never written).
	if err := store.Clear(ctx, "task-1"); err != nil {
		t.Fatalf("Clear absent: %v", err)
	}
	// Task IDs with characters invalid in KV keys must not error.
	if err := store.Record(ctx, "st-join-2/6d9930e3:0", rec); err != nil {
		t.Fatalf("Record with sanitized key: %v", err)
	}
}

// spyAttemptStore records every Record call so a test can observe the rung
// taken during executeIncomingTaskDelivery even though the breadcrumb is
// cleared on graceful completion.
type spyAttemptStore struct {
	*memAttemptStore
	mu      sync.Mutex
	history []attemptRecord
}

func newSpyAttemptStore() *spyAttemptStore {
	return &spyAttemptStore{memAttemptStore: newMemAttemptStore()}
}

func (s *spyAttemptStore) Record(ctx context.Context, taskID string, rec attemptRecord) error {
	s.mu.Lock()
	s.history = append(s.history, rec)
	s.mu.Unlock()
	return s.memAttemptStore.Record(ctx, taskID, rec)
}

func (s *spyAttemptStore) recorded() []attemptRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]attemptRecord, len(s.history))
	copy(out, s.history)
	return out
}

// recordingAcker counts the taskAcker verbs.
type recordingAcker struct {
	acks, naks, terms atomic.Int32
}

func (a *recordingAcker) Ack()        { a.acks.Add(1) }
func (a *recordingAcker) Nak()        { a.naks.Add(1) }
func (a *recordingAcker) Term()       { a.terms.Add(1) }
func (a *recordingAcker) InProgress() {}

func newPoisonTestWorker(t *testing.T) (*Worker, *spyAttemptStore, *nats.Conn) {
	t.Helper()
	_, en, store := setupWorkerNATS(t)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	w := New(Config{
		WorkerID:     "w-test",
		MemoryBudget: 1 << 30,
		SpillDir:     t.TempDir(),
	}, store, nc, js, nil)
	spy := newSpyAttemptStore()
	w.attempts = spy
	return w, spy, nc
}

// failFastTask is a task the executor completes GRACEFULLY (with an error
// result) in milliseconds — the execution itself is irrelevant; the test is
// about the rung decision and the breadcrumb lifecycle around it.
func failFastTask() distributed.Task {
	return distributed.Task{
		ID:      "poison-task-1",
		QueryID: "q-poison",
		StageID: "s1",
		Type:    distributed.TaskTypePipeline,
		SQLText: "THIS IS NOT SQL",
	}
}

// respondToResult ACKs the worker's result publish (request/reply) and
// captures the payload.
func respondToResult(t *testing.T, nc *nats.Conn, task distributed.Task) *atomic.Pointer[distributed.ResultNotification] {
	t.Helper()
	captured := &atomic.Pointer[distributed.ResultNotification]{}
	sub, err := nc.Subscribe(distributed.ResultSubject(task.QueryID, task.StageID, task.ID), func(m *nats.Msg) {
		var rn distributed.ResultNotification
		if err := distributed.Unmarshal(m.Data, &rn); err == nil {
			captured.Store(&rn)
		}
		m.Respond([]byte("ok"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub.Unsubscribe() })
	return captured
}

// TestRedeliveryAfterWorkerDeathTakesDegradedRung is the #318 simulation:
// the prior attempt "fatals mid-execution" — its breadcrumb was written and
// never cleared, exactly the state a real OOM kill leaves in the KV — and
// the redelivery (NumDelivered=2) must execute under the degraded rung, then
// clear the breadcrumb on graceful completion.
func TestRedeliveryAfterWorkerDeathTakesDegradedRung(t *testing.T) {
	w, spy, nc := newPoisonTestWorker(t)
	ctx := context.Background()
	task := failFastTask()

	// The dead prior attempt: record present, never cleared.
	if err := w.attempts.Record(ctx, task.ID, attemptRecord{
		WorkerID: "w-dead", PID: 999999, Delivery: 1, StartedAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	spy.mu.Lock()
	spy.history = nil // observe only the redelivery's writes
	spy.mu.Unlock()

	captured := respondToResult(t, nc, task)
	acker := &recordingAcker{}
	w.executeIncomingTaskDelivery(ctx, task, acker, 2)

	recs := spy.recorded()
	if len(recs) != 1 {
		t.Fatalf("expected exactly one attempt record for the redelivery, got %d", len(recs))
	}
	if !recs[0].Degraded {
		t.Fatalf("redelivery after a mid-execution death must take the DEGRADED rung; record: %+v", recs[0])
	}
	if recs[0].Delivery != 2 {
		t.Errorf("record delivery: got %d want 2", recs[0].Delivery)
	}
	if _, ok, _ := w.attempts.Lookup(ctx, task.ID); ok {
		t.Error("breadcrumb must be cleared after graceful completion (the worker survived)")
	}
	if acker.acks.Load() != 1 || acker.terms.Load() != 0 {
		t.Errorf("acker: acks=%d terms=%d, want 1/0 (graceful completion)", acker.acks.Load(), acker.terms.Load())
	}
	if rn := captured.Load(); rn == nil {
		t.Error("no result published for the degraded attempt")
	} else if rn.Success {
		t.Error("the bad-SQL task should have failed gracefully")
	}
}

// TestDegradedAttemptDeathQuarantines is the final rung: the DEGRADED
// attempt's breadcrumb is still there (that attempt died too), so the next
// delivery must not execute at all — it publishes a terminal failure naming
// the task and Terms the message, ending the crash loop.
func TestDegradedAttemptDeathQuarantines(t *testing.T) {
	w, spy, nc := newPoisonTestWorker(t)
	ctx := context.Background()
	task := failFastTask()

	if err := w.attempts.Record(ctx, task.ID, attemptRecord{
		WorkerID: "w-dead", PID: 999999, Delivery: 2, Degraded: true,
		StartedAt: time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	spy.mu.Lock()
	spy.history = nil
	spy.mu.Unlock()

	captured := respondToResult(t, nc, task)
	acker := &recordingAcker{}
	w.executeIncomingTaskDelivery(ctx, task, acker, 3)

	if acker.terms.Load() != 1 {
		t.Fatalf("quarantine must Term the message (got terms=%d, acks=%d, naks=%d)",
			acker.terms.Load(), acker.acks.Load(), acker.naks.Load())
	}
	if acker.acks.Load() != 0 {
		t.Errorf("quarantined task must not be acked as executed (acks=%d)", acker.acks.Load())
	}
	if got := spy.recorded(); len(got) != 0 {
		t.Errorf("quarantine must not write a fresh attempt record (it does not execute); got %d", len(got))
	}
	rn := captured.Load()
	if rn == nil {
		t.Fatal("quarantine must publish a terminal failure result")
	}
	if rn.Success || rn.Error == "" {
		t.Fatalf("quarantine result must carry the error, got %+v", rn)
	}
	for _, needle := range []string{task.ID, "quarantined", "memory"} {
		if !strings.Contains(rn.Error, needle) {
			t.Errorf("quarantine error must name %q; got: %s", needle, rn.Error)
		}
	}
}

// TestRedeliveryWithoutBreadcrumbStaysNormal: a lost delivery (never began
// executing) must NOT be treated as poison.
func TestRedeliveryWithoutBreadcrumbStaysNormal(t *testing.T) {
	w, spy, nc := newPoisonTestWorker(t)
	ctx := context.Background()
	task := failFastTask()

	respondToResult(t, nc, task)
	acker := &recordingAcker{}
	w.executeIncomingTaskDelivery(ctx, task, acker, 2)

	recs := spy.recorded()
	if len(recs) != 1 {
		t.Fatalf("expected one attempt record, got %d", len(recs))
	}
	if recs[0].Degraded {
		t.Error("redelivery with no prior breadcrumb must execute normally, not degraded")
	}
	if acker.acks.Load() != 1 {
		t.Errorf("expected graceful ack, got acks=%d", acker.acks.Load())
	}
}

// TestDegradedTaskGetsReducedSpillView verifies the degraded flag actually
// changes execution: the task's operators receive a spill view whose budget
// is the reduced cap, and whose spill decisions fire far below the shared
// pool's thresholds.
func TestDegradedTaskGetsReducedSpillView(t *testing.T) {
	w, _, _ := newPoisonTestWorker(t)
	e := w.executor

	fullPool := e.sharedTracker.Budget()
	wantBudget := e.DegradedTaskBudget()
	if wantBudget <= 0 || wantBudget >= fullPool {
		t.Fatalf("degraded budget %d not a reduction of the %d-byte pool", wantBudget, fullPool)
	}

	normalCtx := e.withTaskSpill(context.Background(), distributed.Task{ID: "t"})
	if got := e.spillFor(normalCtx); got != e.sharedSpill {
		t.Fatal("normal task must use the shared spill manager")
	}

	degCtx := e.withTaskSpill(context.Background(), distributed.Task{ID: "t", DegradedMemory: true})
	view := e.spillFor(degCtx)
	if view == e.sharedSpill {
		t.Fatal("degraded task must not use the shared manager directly")
	}
	if got := view.DegradedBudget(); got != wantBudget {
		t.Fatalf("degraded view budget: got %d want %d", got, wantBudget)
	}
	if view.SpillDir() != e.sharedSpill.SpillDir() {
		t.Error("degraded view must share the spill directory")
	}
	if view.Tracker() != e.sharedSpill.Tracker() {
		t.Error("degraded view must charge the shared tracker")
	}

	// Spill decisions: charge the SHARED tracker past 60% of the reduced
	// cap but far under 60% of the full pool — the view must demand spill
	// while the shared manager stays quiet.
	tr := e.sharedSpill.Tracker()
	tr.ForceReserve(wantBudget * 7 / 10)
	defer tr.Release(wantBudget * 7 / 10)
	if e.sharedSpill.ShouldSpill() {
		t.Fatal("shared manager should not demand spill at this usage — test setup wrong")
	}
	if !view.ShouldSpill() {
		t.Fatal("degraded view must demand spill at 70% of its reduced cap (ADR-0006 " +
			"machinery engaging early is the whole point of the degraded rung)")
	}
}
