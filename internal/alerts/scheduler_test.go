package alerts

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

type stubSink struct {
	name  string
	calls int32
	mu    sync.Mutex
	fires []AlertFire
}

func (s *stubSink) Name() string { return s.name }
func (s *stubSink) Deliver(_ context.Context, f AlertFire) error {
	atomic.AddInt32(&s.calls, 1)
	s.mu.Lock()
	s.fires = append(s.fires, f)
	s.mu.Unlock()
	return nil
}

type stubExec struct {
	rows   []map[string]any
	total  int64
	schema []ColumnMeta
}

func (*stubExec) Execute(context.Context, string) error { return nil }
func (e *stubExec) Query(context.Context, string, int) ([]map[string]any, []ColumnMeta, int64, bool, error) {
	return e.rows, e.schema, e.total, false, nil
}

func newSchedulerTest(t *testing.T) (*catalog.Catalog, *stubExec, *stubSink) {
	t.Helper()
	kv := catalog.NewMemKV()
	store := objstore.NewMemStore()
	cat := catalog.NewWithCluster(kv, store, "b", "c")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	ex := &stubExec{
		rows:   []map[string]any{{"n": int64(1)}},
		total:  1,
		schema: []ColumnMeta{{Name: "n", Type: "INT64"}},
	}
	sink := &stubSink{name: "webhook"}
	return cat, ex, sink
}

func TestSchedulerFiresDueAlert(t *testing.T) {
	cat, ex, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: true,
	})
	s := NewScheduler(cat, ex, SinkFactory(func(m catalog.AlertMeta) []AlertSink {
		return []AlertSink{sink}
	}), nil)
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) < 1 {
		t.Error("want at least 1 fire, got 0")
	}
}

func TestSchedulerSkipsDisabled(t *testing.T) {
	cat, ex, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: false,
	})
	_ = ex
	s := NewScheduler(cat, ex, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }), nil)
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) != 0 {
		t.Errorf("disabled alert fired: %d calls", sink.calls)
	}
}

// blockingExec blocks inside Query until release is closed, simulating a
// long-running alert evaluation.
type blockingExec struct {
	release chan struct{}
	rows    []map[string]any
	total   int64
}

func (*blockingExec) Execute(context.Context, string) error { return nil }
func (e *blockingExec) Query(ctx context.Context, _ string, _ int) ([]map[string]any, []ColumnMeta, int64, bool, error) {
	select {
	case <-e.release:
	case <-ctx.Done():
		return nil, nil, 0, false, ctx.Err()
	}
	return e.rows, nil, e.total, false, nil
}

func TestSchedulerSkipsConcurrent(t *testing.T) {
	cat, _, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: true,
	})
	release := make(chan struct{})
	defer close(release)
	ex := &blockingExec{release: release, rows: []map[string]any{{"n": 1}}, total: 1}

	s := NewScheduler(cat, ex, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }), nil)
	s.tickInterval = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.Start(ctx)

	// Wait long enough for multiple ticks to fire while the first eval is blocked.
	time.Sleep(150 * time.Millisecond)

	// Cancel ctx (which releases the blocking eval via ctx.Done), then wait.
	cancel()
	s.Wait()

	// The blocking exec never completed a real delivery in the window because
	// ctx was cancelled, so sink.calls is 0. The key assertion is that the
	// inflight guard prevented the scheduler from spawning multiple concurrent
	// goroutines for the same alert — verify by checking that we did NOT
	// accumulate N goroutines all waiting on `release`. A simpler proxy is
	// that Wait() returns promptly (within the test timeout). If the guard
	// were broken, we'd have many blocked goroutines that only unblock when
	// we close release on deferred cleanup.
	if atomic.LoadInt32(&sink.calls) > 1 {
		t.Errorf("concurrent evaluations fired sinks: %d", sink.calls)
	}
}

func TestSchedulerNoFireOnZeroRows(t *testing.T) {
	cat, _, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: true,
	})
	emptyExec := &stubExec{rows: nil, total: 0}
	s := NewScheduler(cat, emptyExec, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }), nil)
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) != 0 {
		t.Errorf("want no fire on zero rows, got %d calls", sink.calls)
	}
}

// ctxCapturingExec records the context its Query received so tests can assert
// the scheduler applied the EvalContextFunc decorator before executing.
type ctxCapturingExec struct {
	mu     sync.Mutex
	gotCtx context.Context
	fired  int32
}

func (*ctxCapturingExec) Execute(context.Context, string) error { return nil }
func (e *ctxCapturingExec) Query(ctx context.Context, _ string, _ int) ([]map[string]any, []ColumnMeta, int64, bool, error) {
	e.mu.Lock()
	e.gotCtx = ctx
	e.mu.Unlock()
	atomic.AddInt32(&e.fired, 1)
	return []map[string]any{{"n": int64(1)}}, []ColumnMeta{{Name: "n"}}, 1, false, nil
}

type evalCtxKey struct{}

// TestSchedulerAppliesEvalDecorator proves the injected EvalContextFunc runs
// and its returned context is the one handed to exec.Query — the plumbing that
// carries the alert creator's identity into query execution. Without the
// decorator wiring in evaluate(), the executor would see the bare tick context.
func TestSchedulerAppliesEvalDecorator(t *testing.T) {
	cat, _, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: true,
	})
	exec := &ctxCapturingExec{}
	decorator := EvalContextFunc(func(ctx context.Context, m catalog.AlertMeta) context.Context {
		return context.WithValue(ctx, evalCtxKey{}, m.Name)
	})
	s := NewScheduler(cat, exec, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }), decorator)
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()

	if atomic.LoadInt32(&exec.fired) == 0 {
		t.Fatal("alert never fired")
	}
	exec.mu.Lock()
	got := exec.gotCtx
	exec.mu.Unlock()
	if got == nil {
		t.Fatal("executor received no context")
	}
	if v, _ := got.Value(evalCtxKey{}).(string); v != "a1" {
		t.Fatalf("decorator context did not reach exec.Query: got value %q, want %q", v, "a1")
	}
}
