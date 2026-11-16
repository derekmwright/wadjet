// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// awaitAttached spins (no clock) until a second reference holds key's
// in-flight entry: the waiter is attached to the build the test controls.
func awaitAttached(c *broadcastJoinCache, key string) {
	for {
		c.mu.Lock()
		e := c.entries[key]
		attached := e != nil && e.refcount == 2
		c.mu.Unlock()
		if attached {
			return
		}
		runtime.Gosched()
	}
}

// A WAITER DOES NOT INHERIT THE BUILDER'S CANCELLATION (#1687). The builder
// task's own context is cancelled mid-build — in the gate run, by a sibling
// unary op's refused filter compile inside the same fragment
// (buildUnaryChain builds a fragment's ops concurrently under one errgroup
// context) — so build() fails with `join build cancelled: context canceled`.
// A probe task waiting on that build, whose own context is live, must not
// report that as its failure: it builds the table itself. Before the fix it
// inherited the builder's error, and that cancellation could become the
// query's error in place of the refusal that caused it.
func TestBroadcastJoinCache_WaiterDoesNotInheritBuilderCancellation(t *testing.T) {
	c := newBroadcastJoinCache()
	builderCtx, cancelBuilder := context.WithCancel(context.Background())
	defer cancelBuilder()
	builderEntered := make(chan struct{})
	builderErr := make(chan error, 1)
	go func() {
		_, _, err := c.Acquire(builderCtx, "k", "q1", func() (*exec.HashJoin, error) {
			close(builderEntered)
			<-builderCtx.Done()
			return nil, fmt.Errorf("join build cancelled: %w", builderCtx.Err())
		})
		builderErr <- err
	}()
	<-builderEntered

	type got struct {
		hj      *exec.HashJoin
		release func()
		err     error
	}
	waiter := make(chan got, 1)
	var rebuilt atomic.Int32
	go func() {
		hj, release, err := c.Acquire(context.Background(), "k", "q1", func() (*exec.HashJoin, error) {
			rebuilt.Add(1)
			return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
		})
		waiter <- got{hj, release, err}
	}()
	awaitAttached(c, "k")
	cancelBuilder()

	if err := <-builderErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("builder err = %v, want its own cancellation", err)
	}
	w := <-waiter
	if w.err != nil {
		t.Fatalf("waiter with a live context failed with %v — it inherited the builder's cancellation", w.err)
	}
	if w.hj == nil || rebuilt.Load() != 1 {
		t.Fatalf("waiter hj=%v rebuilt=%d, want its own build, once", w.hj, rebuilt.Load())
	}
	w.release()
	if c.Count() != 0 {
		t.Errorf("Count=%d after release, want 0", c.Count())
	}
}

// A builder failure that is NOT the builder's cancellation is still the
// waiters' answer: the same build fails the same way for them.
func TestBroadcastJoinCache_WaiterInheritsARealBuildFailure(t *testing.T) {
	c := newBroadcastJoinCache()
	wantErr := errors.New("build source init: bad file")
	builderEntered, failNow := make(chan struct{}), make(chan struct{})
	go func() {
		_, _, _ = c.Acquire(context.Background(), "k", "q1", func() (*exec.HashJoin, error) {
			close(builderEntered)
			<-failNow
			return nil, wantErr
		})
	}()
	<-builderEntered
	waiter := make(chan error, 1)
	var rebuilt atomic.Int32
	go func() {
		_, _, err := c.Acquire(context.Background(), "k", "q1", func() (*exec.HashJoin, error) {
			rebuilt.Add(1)
			return exec.NewHashJoin(exec.InnerJoin, []string{"a"}, []string{"b"}), nil
		})
		waiter <- err
	}()
	awaitAttached(c, "k")
	close(failNow)
	if err := <-waiter; !errors.Is(err, wantErr) || rebuilt.Load() != 0 {
		t.Fatalf("waiter err=%v rebuilt=%d, want the builder's real failure and no rebuild", err, rebuilt.Load())
	}
	if c.Count() != 0 {
		t.Errorf("Count=%d, want 0", c.Count())
	}
}
