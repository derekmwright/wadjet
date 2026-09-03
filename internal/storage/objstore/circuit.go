package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// CircuitState represents the current state of the circuit breaker.
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // normal operation
	CircuitOpen                         // fast-fail, no requests sent
	CircuitHalfOpen                     // testing with limited requests
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// OpClass is the operation class a breaker counts failures for.
//
// The breaker is scoped BY CLASS because the failures it must react to and
// the requests it must protect are not the same traffic. A query READS to
// answer; it WRITES stage output and DELETES scratch off the critical path.
// Three times now (compaction's post-grace deletes, SF100 upload cancels,
// streaming-fallback 404s) a burst of failures on an off-critical-path
// operation opened one process-wide breaker and fast-failed healthy
// base-table reads; each fix excluded one more error class and the defect
// came back in the next one. The invariant, stated once instead of
// enumerated per error class: a failure on a non-read, off-critical-path
// operation never fast-fails the read path (ADR-0028).
type OpClass int

const (
	OpRead   OpClass = iota // Get, GetReaderAt, Head, List, BucketExists
	OpWrite                 // Put, PutIfMatch, MakeBucket
	OpDelete                // Delete
	numOpClasses
)

func (c OpClass) String() string {
	switch c {
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker open: S3 unavailable")

// CircuitConfig configures the circuit breaker behavior.
type CircuitConfig struct {
	FailureThreshold int           // consecutive failures before opening (default: 5)
	ResetTimeout     time.Duration // time in open state before trying half-open (default: 30s)
	HalfOpenMax      int           // max requests in half-open state (default: 1)
	RequestTimeout   time.Duration // per-request timeout (default: 10s)
}

// DefaultCircuitConfig returns sensible defaults.
func DefaultCircuitConfig() CircuitConfig {
	return CircuitConfig{
		FailureThreshold: 5,
		ResetTimeout:     30 * time.Second,
		HalfOpenMax:      1,
		RequestTimeout:   10 * time.Second,
	}
}

// classState is one operation class's breaker. Each class carries its own
// consecutive-failure counter and its own state machine; classes share only
// the config, the logger and the mutex.
type classState struct {
	state        CircuitState
	failures     int
	lastFailure  time.Time
	halfOpenUsed int
	opened       uint64 // transitions into open, for the metric
}

// CircuitStore wraps a Store with circuit breaker protection.
// When consecutive failures on an operation class exceed the threshold that
// class's circuit opens and its requests immediately return ErrCircuitOpen.
// After a reset timeout the class moves to half-open and allows a probe
// request through. The other classes are unaffected.
type CircuitStore struct {
	inner  Store
	config CircuitConfig
	logger *slog.Logger

	mu     sync.Mutex
	cls    [numOpClasses]classState
	onOpen func(OpClass)
}

// NewCircuitStore wraps an existing store with circuit breaker protection.
func NewCircuitStore(inner Store, cfg CircuitConfig, logger *slog.Logger) *CircuitStore {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.ResetTimeout <= 0 {
		cfg.ResetTimeout = 30 * time.Second
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = 1
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	cs := &CircuitStore{
		inner:  inner,
		config: cfg,
		logger: logger,
	}
	for i := range cs.cls {
		cs.cls[i].state = CircuitClosed
	}
	return cs
}

// Config returns the breaker's effective configuration (after defaulting).
func (cs *CircuitStore) Config() CircuitConfig { return cs.config }

// SetOnOpen registers a callback invoked (with the breaker's lock released)
// each time an operation class transitions into the open state. It drives
// the wadjet_circuit_breaker_opened_total{class} counter.
func (cs *CircuitStore) SetOnOpen(fn func(OpClass)) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.onOpen = fn
}

// State returns the worst state across the operation classes: open if any
// class is open, half-open if any is half-open, otherwise closed. Callers
// that care about one class must use StateFor — "is the read path being
// fast-failed" is a question only StateFor(OpRead) answers.
func (cs *CircuitStore) State() CircuitState {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	worst := CircuitClosed
	for i := range cs.cls {
		switch cs.cls[i].state {
		case CircuitOpen:
			return CircuitOpen
		case CircuitHalfOpen:
			worst = CircuitHalfOpen
		}
	}
	return worst
}

// StateFor returns the current state of one operation class.
func (cs *CircuitStore) StateFor(class OpClass) CircuitState {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.cls[class].state
}

// OpenedTotal returns how many times a class's breaker has opened.
func (cs *CircuitStore) OpenedTotal(class OpClass) uint64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.cls[class].opened
}

func (cs *CircuitStore) beforeRequest(class OpClass) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	c := &cs.cls[class]
	switch c.state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		// Check if reset timeout has elapsed
		if time.Since(c.lastFailure) >= cs.config.ResetTimeout {
			c.state = CircuitHalfOpen
			c.halfOpenUsed = 0
			cs.logger.Info("circuit breaker half-open",
				"class", class.String(), "after", cs.config.ResetTimeout)
			return nil
		}
		return ErrCircuitOpen
	case CircuitHalfOpen:
		if c.halfOpenUsed >= cs.config.HalfOpenMax {
			return ErrCircuitOpen
		}
		c.halfOpenUsed++
		return nil
	}
	return nil
}

func (cs *CircuitStore) onSuccess(class OpClass) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	c := &cs.cls[class]
	if c.state == CircuitHalfOpen {
		cs.logger.Info("circuit breaker closed (probe succeeded)", "class", class.String())
	}
	c.failures = 0
	c.state = CircuitClosed
}

func (cs *CircuitStore) onFailure(class OpClass, err error) {
	// Client-side aborts are not S3 health signals. The worker cancels a
	// terminal query's pending async uploads BY DESIGN (streaming
	// exchange: uploadManager.CancelQuery fires on every query
	// complete/cancel broadcast) — under mid-day S3 PUT latency a query
	// boundary can cancel 5+ queued uploads back-to-back, which counted
	// here as consecutive "failures", opened the breaker on every worker
	// simultaneously, and made the NEXT query's healthy GETs fast-fail
	// for the whole 30 s reset window (SF100 2026-07-12: Q21/Q22 —
	// occasionally Q03 — failed terminally after their retries burned
	// into the still-open breaker). context.Canceled can only come from
	// our own callers, so it says nothing about S3: neither a failure nor
	// evidence of health, and it leaves the counter exactly as it was.
	if errors.Is(err, context.Canceled) {
		return
	}
	// Not-found is a DEFINITIVE, healthy S3 answer — the service responded;
	// the key just isn't there (yet). Streaming exchange makes this an
	// expected race: consumers' S3-tier fallbacks probe keys whose
	// background upload hasn't landed (widened by the upload QoS gate,
	// SF100 2026-08-05: repeated fallback 404s opened the breaker and
	// killed the NEXT queries' healthy reads — Q06/Q08 steady FAIL).
	// A round trip that ANSWERED is a success for the counter, not merely
	// neutral: while it was neutral, five failing deletes interleaved with
	// by-design NotFound probes still opened the breaker, where forty
	// interleaved with successful reads did not — the healthy round trips
	// were invisible (#821).
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrBucketNotFound) {
		cs.onSuccess(class)
		return
	}

	var notify func(OpClass)
	cs.mu.Lock()
	c := &cs.cls[class]
	c.failures++
	c.lastFailure = time.Now()

	switch {
	case c.state == CircuitHalfOpen:
		c.state = CircuitOpen
		c.opened++
		notify = cs.onOpen
		cs.logger.Warn("circuit breaker re-opened (half-open probe failed)",
			"class", class.String(), "error", err)
	case c.failures >= cs.config.FailureThreshold && c.state != CircuitOpen:
		c.state = CircuitOpen
		c.opened++
		notify = cs.onOpen
		cs.logger.Warn("circuit breaker opened",
			"class", class.String(),
			"failures", c.failures,
			"threshold", cs.config.FailureThreshold,
			"reset_timeout", cs.config.ResetTimeout,
			"error", err,
		)
	}
	cs.mu.Unlock()

	if notify != nil {
		notify(class)
	}
}

func (cs *CircuitStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, cs.config.RequestTimeout)
}

func (cs *CircuitStore) do(ctx context.Context, class OpClass, fn func(context.Context) error) error {
	if err := cs.beforeRequest(class); err != nil {
		return err
	}
	tctx, cancel := cs.withTimeout(ctx)
	defer cancel()

	err := fn(tctx)
	if err != nil {
		cs.onFailure(class, err)
	} else {
		cs.onSuccess(class)
	}
	return err
}

// Put implements Store. PUT operations are not wrapped in a per-call
// timeout — the inner store's transport-level ResponseHeaderTimeout
// (30 min) plus the caller's ctx already bound upload time. A short
// per-call timeout caused multi-process clusters to fail healthy uploads
// when contended bandwidth dropped per-connection throughput below the
// previous 20 MB/s sizing assumption.
//
// The circuit breaker's failure counting still applies — repeated PUT
// errors trip the WRITE breaker — but a slow-yet-progressing upload no
// longer triggers a context deadline mid-stream, and a tripped write
// breaker never fast-fails a read.
func (cs *CircuitStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	if err := cs.beforeRequest(OpWrite); err != nil {
		return "", err
	}
	etag, err := cs.inner.Put(ctx, bucket, key, r, size, contentType)
	if err != nil {
		cs.onFailure(OpWrite, err)
	} else {
		cs.onSuccess(OpWrite)
	}
	return etag, err
}

// PutIfMatch implements Store. See Put for why we don't wrap in a
// per-call timeout.
func (cs *CircuitStore) PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (string, error) {
	if err := cs.beforeRequest(OpWrite); err != nil {
		return "", err
	}
	etag, err := cs.inner.PutIfMatch(ctx, bucket, key, r, size, contentType, expectedETag)
	if err != nil {
		cs.onFailure(OpWrite, err)
	} else {
		cs.onSuccess(OpWrite)
	}
	return etag, err
}

// putTimeout is no longer used — kept only because it has tests. PUT
// operations now rely on transport-level and caller-level deadlines.
func (cs *CircuitStore) putTimeout(size int64) time.Duration {
	timeout := cs.config.RequestTimeout
	if size > 0 {
		// 20 MB/s ⇒ size_bytes / (20 * 1<<20 bytes/s) seconds
		sized := time.Duration(size/(20*1024*1024)) * time.Second
		if sized > timeout {
			timeout = sized
		}
	}
	return timeout
}

// Get implements Store.
// Get cannot use the standard do() wrapper because do() defers cancel() on
// the timeout context. Since Get returns a streaming ReadCloser, canceling
// the context before the caller reads the body kills the HTTP connection,
// causing every io.ReadAll(rc) to fail with "context canceled".
func (cs *CircuitStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	if err := cs.beforeRequest(OpRead); err != nil {
		return nil, ObjectInfo{}, err
	}
	rc, info, err := cs.inner.Get(ctx, bucket, key)
	if err != nil {
		cs.onFailure(OpRead, err)
		return nil, info, err
	}
	cs.onSuccess(OpRead)
	return rc, info, err
}

// Head implements Store.
func (cs *CircuitStore) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	var info ObjectInfo
	err := cs.do(ctx, OpRead, func(c context.Context) error {
		var e error
		info, e = cs.inner.Head(c, bucket, key)
		return e
	})
	return info, err
}

// List implements Store.
func (cs *CircuitStore) List(ctx context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	err := cs.do(ctx, OpRead, func(c context.Context) error {
		var e error
		objects, e = cs.inner.List(c, bucket, opts)
		return e
	})
	return objects, err
}

// Delete implements Store. Deletes are off the critical path (scratch
// reclamation, compaction's post-grace sweep, DROP TABLE reclaim) and count
// into their own breaker: a slow delete burst must never fast-fail a read.
func (cs *CircuitStore) Delete(ctx context.Context, bucket, key string) error {
	return cs.do(ctx, OpDelete, func(c context.Context) error {
		return cs.inner.Delete(c, bucket, key)
	})
}

// BucketExists implements Store.
func (cs *CircuitStore) BucketExists(ctx context.Context, bucket string) (bool, error) {
	var exists bool
	err := cs.do(ctx, OpRead, func(c context.Context) error {
		var e error
		exists, e = cs.inner.BucketExists(c, bucket)
		return e
	})
	return exists, err
}

// MakeBucket implements Store.
func (cs *CircuitStore) MakeBucket(ctx context.Context, bucket string) error {
	return cs.do(ctx, OpWrite, func(c context.Context) error {
		return cs.inner.MakeBucket(c, bucket)
	})
}

// StoreID implements IdentifiedStore by delegating: the breaker changes
// availability, never object identity.
func (cs *CircuitStore) StoreID() string { return StoreID(cs.inner) }

// GetReaderAt implements ReaderAtStore if the underlying store supports it.
// Like Get, this cannot use do() because it returns a streaming handle.
func (cs *CircuitStore) GetReaderAt(ctx context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	ras, ok := cs.inner.(ReaderAtStore)
	if !ok {
		return nil, 0, fmt.Errorf("underlying store does not support ReaderAt")
	}
	if err := cs.beforeRequest(OpRead); err != nil {
		return nil, 0, err
	}
	ra, size, err := ras.GetReaderAt(ctx, bucket, key)
	if err != nil {
		cs.onFailure(OpRead, err)
		return nil, 0, err
	}
	cs.onSuccess(OpRead)
	return ra, size, nil
}

// Unwrap returns the underlying store.
func (cs *CircuitStore) Unwrap() Store {
	return cs.inner
}

// FindCircuitStore walks a Store's Unwrap() chain and returns the first
// CircuitStore in it, so a caller holding the outermost wrapper (the
// base-table NVMe cache sits ABOVE the breaker) can still reach the breaker
// to attach metrics. Returns nil when the chain holds none.
func FindCircuitStore(s Store) *CircuitStore {
	for s != nil {
		if cs, ok := s.(*CircuitStore); ok {
			return cs
		}
		u, ok := s.(interface{ Unwrap() Store })
		if !ok {
			return nil
		}
		s = u.Unwrap()
	}
	return nil
}
