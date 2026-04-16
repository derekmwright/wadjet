package alerts

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
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
	}))
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
	s := NewScheduler(cat, ex, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }))
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) != 0 {
		t.Errorf("disabled alert fired: %d calls", sink.calls)
	}
}

func TestSchedulerNoFireOnZeroRows(t *testing.T) {
	cat, _, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: true,
	})
	emptyExec := &stubExec{rows: nil, total: 0}
	s := NewScheduler(cat, emptyExec, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }))
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) != 0 {
		t.Errorf("want no fire on zero rows, got %d calls", sink.calls)
	}
}
