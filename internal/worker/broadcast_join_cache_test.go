package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

func TestBroadcastJoinCache_SingleAcquireRelease(t *testing.T) {
	c := newBroadcastJoinCache()
	var built int32
	hj, release, err := c.Acquire(context.Background(), "k1", "q1", func() (*exec.HashJoin, error) {
		atomic.AddInt32(&built, 1)
		return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if hj == nil {
		t.Fatal("hj is nil")
	}
	if got := atomic.LoadInt32(&built); got != 1 {
		t.Errorf("built=%d, want 1", got)
	}
	if c.Count() != 1 {
		t.Errorf("Count=%d, want 1", c.Count())
	}
	release()
	if c.Count() != 0 {
		t.Errorf("Count after release=%d, want 0 (evicted)", c.Count())
	}
}

func TestBroadcastJoinCache_ConcurrentAcquireSharesBuild(t *testing.T) {
	c := newBroadcastJoinCache()
	var built int32
	const concurrency = 8
	var wg sync.WaitGroup
	releases := make(chan func(), concurrency)
	hjs := make(chan *exec.HashJoin, concurrency)
	start := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			hj, release, err := c.Acquire(context.Background(), "shared-key", "q1", func() (*exec.HashJoin, error) {
				atomic.AddInt32(&built, 1)
				return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
			})
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			hjs <- hj
			releases <- release
		}()
	}
	close(start)
	wg.Wait()
	close(hjs)
	close(releases)

	if got := atomic.LoadInt32(&built); got != 1 {
		t.Errorf("built=%d, want 1 (build called once across %d acquires)", got, concurrency)
	}

	var first *exec.HashJoin
	for hj := range hjs {
		if first == nil {
			first = hj
		} else if hj != first {
			t.Error("acquires returned different *HashJoin instances; want all same")
		}
	}
	if c.Count() != 1 {
		t.Errorf("Count=%d, want 1 (one entry shared across acquires)", c.Count())
	}

	released := 0
	for r := range releases {
		r()
		released++
		if released < concurrency && c.Count() != 1 {
			t.Errorf("Count=%d after %d releases, want 1 (not yet evicted)", c.Count(), released)
		}
	}
	if c.Count() != 0 {
		t.Errorf("Count after %d releases=%d, want 0 (evicted)", concurrency, c.Count())
	}
}

func TestBroadcastJoinCache_BuilderErrorDoesNotLeak(t *testing.T) {
	c := newBroadcastJoinCache()
	wantErr := errors.New("build failed")
	hj, release, err := c.Acquire(context.Background(), "k1", "q1", func() (*exec.HashJoin, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("err=%v, want %v", err, wantErr)
	}
	if hj != nil {
		t.Error("hj should be nil on builder error")
	}
	if release != nil {
		t.Error("release should be nil on builder error")
	}
	if c.Count() != 0 {
		t.Errorf("Count=%d, want 0 (evicted on error)", c.Count())
	}
}

// TestBroadcastJoinCache_BuilderPanicDoesNotStrandWaiters is the #101
// regression: a panic inside build() must not leave e.ready unclosed. Before
// the fix, the panic unwound past close(e.ready), so every concurrent waiter
// blocked on <-e.ready forever — each holding a worker concurrency slot, which
// eventually wedged the worker's task intake (the Q07 final_aggregate stall).
func TestBroadcastJoinCache_BuilderPanicDoesNotStrandWaiters(t *testing.T) {
	c := newBroadcastJoinCache()
	const waiters = 4
	var wg sync.WaitGroup
	builderEntered := make(chan struct{})
	panicNow := make(chan struct{})

	// Builder enters build(), then blocks until we trigger the panic — this
	// holds the entry un-ready so the waiters below deterministically attach
	// and block on <-e.ready (the exact scenario the bug stranded forever).
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, err := c.Acquire(context.Background(), "boom", "q1", func() (*exec.HashJoin, error) {
			close(builderEntered)
			<-panicNow
			panic("simulated build panic")
		})
		if err == nil {
			t.Error("builder Acquire should return an error after a build panic")
		}
	}()
	<-builderEntered

	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Must not hang. Whichever way it returns — inheriting the builder's
			// error, or (if it lands after eviction) succeeding as a fresh
			// builder — it MUST return rather than strand on e.ready.
			_, release, _ := c.Acquire(context.Background(), "boom", "q1", func() (*exec.HashJoin, error) {
				return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
			})
			if release != nil {
				release()
			}
		}()
	}
	// Give the waiters a moment to attach to the in-flight entry, then panic.
	time.Sleep(25 * time.Millisecond)
	close(panicNow)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Acquire waiters hung after a build panic (stranded on e.ready) — #101 regression")
	}
	if c.Count() != 0 {
		t.Errorf("Count=%d, want 0 (poisoned entry must be evicted)", c.Count())
	}
}

// TestBroadcastJoinCache_WaiterContextCancel verifies a waiter abandons its
// block (and releases its refcount/slot) when its context is cancelled, so a
// build that hangs can never strand a deadlined probe task.
func TestBroadcastJoinCache_WaiterContextCancel(t *testing.T) {
	c := newBroadcastJoinCache()
	builderBlock := make(chan struct{})
	builderEntered := make(chan struct{})

	go func() {
		// Builder holds the entry un-ready until we let it finish, after the
		// waiter has already given up.
		_, _, _ = c.Acquire(context.Background(), "slow", "q1", func() (*exec.HashJoin, error) {
			close(builderEntered)
			<-builderBlock
			return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
		})
	}()
	<-builderEntered

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := c.Acquire(ctx, "slow", "q1", func() (*exec.HashJoin, error) {
		return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err=%v, want context.Canceled (waiter must abandon the wait on ctx cancel)", err)
	}
	close(builderBlock)
}

func TestBroadcastJoinCache_CleanupQuery(t *testing.T) {
	c := newBroadcastJoinCache()
	// Two distinct broadcasts in q1 (different keys), one in q2.
	cases := []struct{ q, k string }{
		{"q1", "q1:a"},
		{"q1", "q1:b"},
		{"q2", "q2:a"},
	}
	for _, tc := range cases {
		_, _, err := c.Acquire(context.Background(), tc.k, tc.q, func() (*exec.HashJoin, error) {
			return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
		})
		if err != nil {
			t.Fatalf("acquire %s/%s: %v", tc.q, tc.k, err)
		}
	}
	if c.Count() != 3 {
		t.Errorf("Count=%d, want 3", c.Count())
	}
	if dropped := c.CleanupQuery("q1"); dropped != 2 {
		t.Errorf("CleanupQuery q1 dropped=%d, want 2", dropped)
	}
	if c.Count() != 1 {
		t.Errorf("Count after q1 cleanup=%d, want 1", c.Count())
	}
	if dropped := c.CleanupQuery("q2"); dropped != 1 {
		t.Errorf("CleanupQuery q2 dropped=%d, want 1", dropped)
	}
	if c.Count() != 0 {
		t.Errorf("Count after q2 cleanup=%d, want 0", c.Count())
	}
}

func TestComputeBroadcastJoinKey_IdentityAcrossTasks(t *testing.T) {
	// Two probe tasks for the same broadcast: identical spec → identical key.
	specA := distributed.OpSpec{
		BuildAlias:      "supplier",
		BuildFiles:      []string{"s3://b/q/sh/build/p=0.wshf", "s3://b/q/sh/build/p=1.wshf"},
		JoinType:        "inner",
		LeftKeys:        []string{"l_suppkey"},
		RightKeys:       []string{"s_suppkey"},
		SemiAntiKeyOnly: false,
	}
	specB := specA
	if computeBroadcastJoinKey("q1", specA) != computeBroadcastJoinKey("q1", specB) {
		t.Error("identical specs produced different keys")
	}
	// File order invariance: shuffled file order should not change the key.
	specC := specA
	specC.BuildFiles = []string{specA.BuildFiles[1], specA.BuildFiles[0]}
	if computeBroadcastJoinKey("q1", specA) != computeBroadcastJoinKey("q1", specC) {
		t.Error("file-order-permuted spec produced different key; sort-then-hash expected")
	}
	// Different queries → different keys.
	if computeBroadcastJoinKey("q1", specA) == computeBroadcastJoinKey("q2", specA) {
		t.Error("different queryIDs collided to same key")
	}
	// Different aliases → different keys.
	specD := specA
	specD.BuildAlias = "part"
	if computeBroadcastJoinKey("q1", specA) == computeBroadcastJoinKey("q1", specD) {
		t.Error("different aliases collided to same key")
	}
}
