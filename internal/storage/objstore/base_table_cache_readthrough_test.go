package objstore

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gatedStore blocks Get until the gate closes, so tests can hold a
// read-through fetch in flight while asserting single-flight behavior.
type gatedStore struct {
	Store
	gate chan struct{}
	gets atomic.Int64
}

func (s *gatedStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, ObjectInfo, error) {
	s.gets.Add(1)
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil, ObjectInfo{}, ctx.Err()
	}
	return s.Store.Get(ctx, bucket, key)
}

func newGatedCache(t *testing.T, bucket, key string, data []byte) (*BaseTableCache, *gatedStore) {
	t.Helper()
	mem := NewMemStore()
	if data != nil {
		putObject(t, mem, bucket, key, data)
	} else if err := mem.MakeBucket(context.Background(), bucket); err != nil {
		t.Fatal(err)
	}
	inner := &gatedStore{Store: mem, gate: make(chan struct{})}
	c, err := NewBaseTableCache(inner, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, inner
}

// waitFor polls cond until true or the deadline lapses.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBaseTableCacheReadThroughSingleFlight(t *testing.T) {
	const bucket, key = "data", "tables/lineitem/chunk_rt.parquet"
	body := par1([]byte("first-touch-once"))
	c, inner := newGatedCache(t, bucket, key, body)

	const callers = 4
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- c.ReadThrough(context.Background(), bucket, key)
		}()
	}
	// One fetch reaches the inner store; the rest coalesce onto it.
	waitFor(t, "the fetch to start", func() bool { return inner.gets.Load() == 1 })
	close(inner.gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("ReadThrough: %v", err)
		}
	}
	if got := inner.gets.Load(); got != 1 {
		t.Fatalf("inner gets = %d, want 1 (concurrent read-throughs must single-flight)", got)
	}
	s := c.Stats()
	if s.ReadThroughs != 1 || s.ReadThroughBytes != int64(len(body)) || s.ReadThroughFails != 0 {
		t.Fatalf("stats = %+v, want exactly 1 read-through of %d bytes", s, len(body))
	}
	// Read-through populates the cache but is peer-redirected demand, not
	// local demand: the S3-miss ledger must stay untouched.
	if s.Misses != 0 || s.MissBytes != 0 {
		t.Fatalf("stats = %+v, want no S3-miss accounting from read-through", s)
	}
	if _, ok := c.PeerLocalPath(bucket, key); !ok {
		t.Fatal("object must be resident and peer-servable after read-through")
	}
}

func TestBaseTableCacheReadThroughWaiterCancelKeepsPopulate(t *testing.T) {
	const bucket, key = "data", "tables/orders/chunk_rtc.parquet"
	body := par1([]byte("populate-outlives-waiter"))
	c, inner := newGatedCache(t, bucket, key, body)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.ReadThrough(ctx, bucket, key) }()
	waitFor(t, "the fetch to start", func() bool { return inner.gets.Load() == 1 })
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter err = %v, want context.Canceled", err)
	}
	// The detached fetch finishes anyway — the owner will want the bytes.
	close(inner.gate)
	waitFor(t, "the detached populate to land", func() bool { return c.HasCachedPath(bucket, key) })
	if s := c.Stats(); s.ReadThroughs != 1 || s.ReadThroughFails != 0 {
		t.Fatalf("stats = %+v, want the populate to complete despite the canceled waiter", s)
	}
}

func TestBaseTableCacheReadThroughWaitsOutTee(t *testing.T) {
	const bucket, key = "data", "tables/part/chunk_rtt.parquet"
	body := par1([]byte("tee-wins-the-race"))
	c, inner, _ := newTestCache(t, 1<<20)
	putObject(t, inner.Store, "data", key, body)

	// A local miss holds a tee population open (inflight until Close).
	rc, _, err := c.Get(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.ReadThrough(context.Background(), bucket, key) }()
	// Give the read-through time to enter its wait loop, then let the tee
	// finish and admit.
	time.Sleep(3 * readThroughPoll)
	if _, err := io.Copy(io.Discard, rc); err != nil {
		t.Fatalf("draining tee read: %v", err)
	}
	rc.Close()
	if err := <-errCh; err != nil {
		t.Fatalf("ReadThrough: %v", err)
	}
	if got := inner.gets.Load(); got != 1 {
		t.Fatalf("inner gets = %d, want 1 (read-through must ride the tee, not re-fetch)", got)
	}
	if s := c.Stats(); s.ReadThroughs != 0 {
		t.Fatalf("stats = %+v, want 0 read-throughs when the tee populated", s)
	}
}

func TestBaseTableCacheReadThroughFailurePaths(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	if err := c.ReadThrough(context.Background(), "data", "tables/none/missing.parquet"); err == nil {
		t.Fatal("read-through of an absent object must fail")
	}
	if s := c.Stats(); s.ReadThroughFails != 1 || s.ReadThroughs != 0 {
		t.Fatalf("stats = %+v, want 1 failed read-through", s)
	}

	// Advertised size disagreeing with the streamed body must be rejected
	// before admission into the parquet read path.
	key := "tables/supplier/chunk_short.parquet"
	putObject(t, inner.Store, "data", key, par1([]byte("liar")))
	inner.sizeInflate = 4
	if err := c.ReadThrough(context.Background(), "data", key); err == nil {
		t.Fatal("size-mismatched read-through must fail")
	}
	if c.HasCachedPath("data", key) {
		t.Fatal("size-mismatched body must not be admitted")
	}

	if err := c.ReadThrough(context.Background(), "data", "queries/q1/scratch.parquet"); err == nil {
		t.Fatal("ineligible keys must be rejected")
	}
}
