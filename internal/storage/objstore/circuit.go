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

// CircuitStore wraps a Store with circuit breaker protection.
// When consecutive failures exceed the threshold, the circuit opens and
// all requests immediately return ErrCircuitOpen. After a reset timeout,
// the circuit moves to half-open and allows a probe request through.
type CircuitStore struct {
	inner  Store
	config CircuitConfig
	logger *slog.Logger

	mu           sync.Mutex
	state        CircuitState
	failures     int
	lastFailure  time.Time
	halfOpenUsed int
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
	return &CircuitStore{
		inner:  inner,
		config: cfg,
		logger: logger,
		state:  CircuitClosed,
	}
}

// State returns the current circuit breaker state.
func (cs *CircuitStore) State() CircuitState {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.state
}

func (cs *CircuitStore) beforeRequest() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	switch cs.state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		// Check if reset timeout has elapsed
		if time.Since(cs.lastFailure) >= cs.config.ResetTimeout {
			cs.state = CircuitHalfOpen
			cs.halfOpenUsed = 0
			cs.logger.Info("circuit breaker half-open", "after", cs.config.ResetTimeout)
			return nil
		}
		return ErrCircuitOpen
	case CircuitHalfOpen:
		if cs.halfOpenUsed >= cs.config.HalfOpenMax {
			return ErrCircuitOpen
		}
		cs.halfOpenUsed++
		return nil
	}
	return nil
}

func (cs *CircuitStore) onSuccess() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.state == CircuitHalfOpen {
		cs.logger.Info("circuit breaker closed (probe succeeded)")
	}
	cs.failures = 0
	cs.state = CircuitClosed
}

func (cs *CircuitStore) onFailure(err error) {
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
	// our own callers; S3 slowness surfaces as DeadlineExceeded or
	// transport errors, which still count.
	if errors.Is(err, context.Canceled) {
		return
	}
	// Not-found is a DEFINITIVE, healthy S3 answer — the service responded;
	// the key just isn't there (yet). Streaming exchange makes this an
	// expected race: consumers' S3-tier fallbacks probe keys whose
	// background upload hasn't landed (widened by the upload QoS gate,
	// SF100 2026-08-05: repeated fallback 404s opened the breaker and
	// killed the NEXT queries' healthy reads — Q06/Q08 steady FAIL).
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrBucketNotFound) {
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.failures++
	cs.lastFailure = time.Now()

	if cs.state == CircuitHalfOpen {
		cs.state = CircuitOpen
		cs.logger.Warn("circuit breaker re-opened (half-open probe failed)",
			"error", err)
		return
	}

	if cs.failures >= cs.config.FailureThreshold {
		cs.state = CircuitOpen
		cs.logger.Warn("circuit breaker opened",
			"failures", cs.failures,
			"threshold", cs.config.FailureThreshold,
			"reset_timeout", cs.config.ResetTimeout,
			"error", err,
		)
	}
}

func (cs *CircuitStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, cs.config.RequestTimeout)
}

func (cs *CircuitStore) do(ctx context.Context, fn func(context.Context) error) error {
	if err := cs.beforeRequest(); err != nil {
		return err
	}
	tctx, cancel := cs.withTimeout(ctx)
	defer cancel()

	err := fn(tctx)
	if err != nil {
		cs.onFailure(err)
	} else {
		cs.onSuccess()
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
// errors trip the breaker — but a slow-yet-progressing upload no longer
// triggers a context deadline mid-stream.
func (cs *CircuitStore) Put(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string) (string, error) {
	if err := cs.beforeRequest(); err != nil {
		return "", err
	}
	etag, err := cs.inner.Put(ctx, bucket, key, r, size, contentType)
	if err != nil {
		cs.onFailure(err)
	} else {
		cs.onSuccess()
	}
	return etag, err
}

// PutIfMatch implements Store. See Put for why we don't wrap in a
// per-call timeout.
func (cs *CircuitStore) PutIfMatch(ctx context.Context, bucket, key string, r io.Reader, size int64, contentType string, expectedETag string) (string, error) {
	if err := cs.beforeRequest(); err != nil {
		return "", err
	}
	etag, err := cs.inner.PutIfMatch(ctx, bucket, key, r, size, contentType, expectedETag)
	if err != nil {
		cs.onFailure(err)
	} else {
		cs.onSuccess()
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
	if err := cs.beforeRequest(); err != nil {
		return nil, ObjectInfo{}, err
	}
	rc, info, err := cs.inner.Get(ctx, bucket, key)
	if err != nil {
		cs.onFailure(err)
		return nil, info, err
	}
	cs.onSuccess()
	return rc, info, err
}

// Head implements Store.
func (cs *CircuitStore) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	var info ObjectInfo
	err := cs.do(ctx, func(c context.Context) error {
		var e error
		info, e = cs.inner.Head(c, bucket, key)
		return e
	})
	return info, err
}

// List implements Store.
func (cs *CircuitStore) List(ctx context.Context, bucket string, opts ListOptions) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	err := cs.do(ctx, func(c context.Context) error {
		var e error
		objects, e = cs.inner.List(c, bucket, opts)
		return e
	})
	return objects, err
}

// Delete implements Store.
func (cs *CircuitStore) Delete(ctx context.Context, bucket, key string) error {
	return cs.do(ctx, func(c context.Context) error {
		return cs.inner.Delete(c, bucket, key)
	})
}

// BucketExists implements Store.
func (cs *CircuitStore) BucketExists(ctx context.Context, bucket string) (bool, error) {
	var exists bool
	err := cs.do(ctx, func(c context.Context) error {
		var e error
		exists, e = cs.inner.BucketExists(c, bucket)
		return e
	})
	return exists, err
}

// MakeBucket implements Store.
func (cs *CircuitStore) MakeBucket(ctx context.Context, bucket string) error {
	return cs.do(ctx, func(c context.Context) error {
		return cs.inner.MakeBucket(c, bucket)
	})
}

// GetReaderAt implements ReaderAtStore if the underlying store supports it.
// Like Get, this cannot use do() because it returns a streaming handle.
func (cs *CircuitStore) GetReaderAt(ctx context.Context, bucket, key string) (ReaderAtCloser, int64, error) {
	ras, ok := cs.inner.(ReaderAtStore)
	if !ok {
		return nil, 0, fmt.Errorf("underlying store does not support ReaderAt")
	}
	if err := cs.beforeRequest(); err != nil {
		return nil, 0, err
	}
	ra, size, err := ras.GetReaderAt(ctx, bucket, key)
	if err != nil {
		cs.onFailure(err)
		return nil, 0, err
	}
	cs.onSuccess()
	return ra, size, nil
}

// Unwrap returns the underlying store.
func (cs *CircuitStore) Unwrap() Store {
	return cs.inner
}
