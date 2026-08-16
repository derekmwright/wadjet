package alerts

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// SinkFactory returns the set of sinks for an alert. Injected so the scheduler
// doesn't know about WebhookSink/TableSink concretely and tests can stub.
type SinkFactory func(m catalog.AlertMeta) []AlertSink

// EvalContextFunc decorates the per-evaluation context for an alert before its
// query runs. Injected so the scheduler stays decoupled from the auth package:
// the owner (coordinator / embedded DB) supplies a func that stamps the alert
// creator's identity onto the context (definer's rights), so the scheduled
// query enforces the creator's ABAC row/column policies instead of running
// unfiltered. A nil func (or a nil return) leaves the context unchanged.
type EvalContextFunc func(ctx context.Context, m catalog.AlertMeta) context.Context

// Scheduler runs alerts on their configured cadence. It owns one goroutine
// that ticks and dispatches per-alert evaluations as short-lived goroutines.
type Scheduler struct {
	cat          *catalog.Catalog
	exec         SQLExecutor
	sinks        SinkFactory
	evalCtxFunc  EvalContextFunc
	tickInterval time.Duration
	logger       *slog.Logger

	// Concurrency guard: alert name → in-flight.
	inflightMu sync.Mutex
	inflight   map[string]bool

	wg sync.WaitGroup
}

// NewScheduler constructs a scheduler with a default 1s tick cadence. evalCtx
// may be nil (no per-alert context decoration); production callers pass a func
// that stamps the alert creator's identity for definer's-rights enforcement.
func NewScheduler(cat *catalog.Catalog, exec SQLExecutor, sinks SinkFactory, evalCtx EvalContextFunc) *Scheduler {
	return &Scheduler{
		cat:          cat,
		exec:         exec,
		sinks:        sinks,
		evalCtxFunc:  evalCtx,
		tickInterval: 1 * time.Second,
		inflight:     make(map[string]bool),
		logger:       slog.Default(),
	}
}

// Start begins the scheduler loop. Returns immediately. Call Wait to block
// until ctx.Done() and all in-flight evaluations complete.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(s.tickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				s.tick(ctx, now)
			}
		}
	}()
}

// Wait blocks until the scheduler goroutine exits and all in-flight
// evaluations have finished.
func (s *Scheduler) Wait() {
	s.wg.Wait()
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	alerts, err := s.cat.ListAlerts(ctx)
	if err != nil {
		s.logger.Warn("list alerts", "err", err)
		metricListErrors.Inc()
		return
	}
	for _, a := range alerts {
		if !a.Enabled {
			continue
		}
		if now.Sub(a.LastEvaluatedAt) < time.Duration(a.IntervalSeconds)*time.Second {
			continue
		}
		if !s.tryClaim(a.Name) {
			metricEvaluations.WithLabelValues(a.Name, "skipped_concurrent").Inc()
			continue
		}
		s.wg.Add(1)
		go func(a catalog.AlertMeta) {
			defer s.wg.Done()
			defer s.release(a.Name)
			s.evaluate(ctx, a, now)
		}(a)
	}
}

func (s *Scheduler) tryClaim(name string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if s.inflight[name] {
		return false
	}
	s.inflight[name] = true
	return true
}

func (s *Scheduler) release(name string) {
	s.inflightMu.Lock()
	delete(s.inflight, name)
	s.inflightMu.Unlock()
}

func (s *Scheduler) evaluate(ctx context.Context, a catalog.AlertMeta, now time.Time) {
	interval := time.Duration(a.IntervalSeconds) * time.Second
	timeout := interval
	if timeout > 60*time.Second {
		timeout = 60 * time.Second
	}
	evalCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Stamp the creator's identity (definer's rights) so the alert query
	// enforces that identity's ABAC policies. A nil decorator leaves the
	// context untouched (auth disabled / embedded). When auth is enabled the
	// decorator fails an unattributed (legacy) alert closed — see
	// alertEvalDecorator.
	if s.evalCtxFunc != nil {
		if decorated := s.evalCtxFunc(evalCtx, a); decorated != nil {
			evalCtx = decorated
		}
	}

	start := time.Now()
	rows, schema, total, truncated, err := s.exec.Query(evalCtx, a.QueryText, MaxRowsPerFire)
	metricEvalDuration.WithLabelValues(a.Name).Observe(time.Since(start).Seconds())
	if err != nil {
		s.logger.Warn("alert query failed", "alert", a.Name, "err", err)
		metricEvaluations.WithLabelValues(a.Name, "error").Inc()
		_ = s.cat.TouchAlertEvaluated(ctx, a.Name, now)
		return
	}
	metricRowsMatched.WithLabelValues(a.Name).Set(float64(total))

	if total == 0 {
		_ = s.cat.TouchAlertEvaluated(ctx, a.Name, now)
		return
	}

	fire := AlertFire{
		AlertName:   a.Name,
		EvaluatedAt: now.UTC(),
		RowCount:    total,
		Rows:        rows,
		Truncated:   truncated,
		Schema:      schema,
	}

	sinks := s.sinks(a)
	var results []SinkResult
	okCount := 0
	for _, sink := range sinks {
		if ts, ok := sink.(*TableSink); ok {
			ts.Results = results
		}
		derr := sink.Deliver(evalCtx, fire)
		if derr == nil {
			results = append(results, SinkResult{Sink: sink.Name(), OK: true})
			okCount++
		} else {
			results = append(results, SinkResult{Sink: sink.Name(), OK: false, Error: derr.Error()})
		}
	}
	switch {
	case okCount == 0:
		metricEvaluations.WithLabelValues(a.Name, "failed").Inc()
	case okCount < len(results):
		metricEvaluations.WithLabelValues(a.Name, "partial").Inc()
	default:
		metricEvaluations.WithLabelValues(a.Name, "delivered").Inc()
	}

	_ = s.cat.TouchAlertEvaluated(ctx, a.Name, now)
}
