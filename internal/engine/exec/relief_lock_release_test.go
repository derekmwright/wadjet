// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1706, line 1706n round 2: every way out of a region where a HashJoin build
// holds h.mu by hand — a return, an error return, a panic — releases it
// exactly once.
//
// A partitioned (grace) build is registered for relief, and Build's deferred
// relief close locks h.mu. So a path that leaves buildPartitioned still
// holding h.mu does not just leak the lock: the build's own goroutine blocks
// forever re-locking it, the refusal or the recovered panic never reaches the
// caller, and the join stays registered with h.mu held, so the next registry
// pass (rankCandidates reads Inspect under the registry lock) wedges every
// operator on the manager. A flat build is not registered; a lock it leaks
// hangs its Close instead.
//
// Every wait here is bounded; a goroutine that does not come back is reported
// with the stack that names where it is blocked.

const lockReleaseDeadline = 5 * time.Second

// allGoroutines is the full goroutine dump.
func allGoroutines() string {
	buf := make([]byte, 4<<20)
	return string(buf[:runtime.Stack(buf, true)])
}

// blockedLockingIn returns the stack of a goroutine blocked in
// sync.(*Mutex).Lock with marker on its stack, or "".
func blockedLockingIn(dump, marker string) string {
	for _, g := range strings.Split(dump, "\n\n") {
		if strings.Contains(g, marker) && strings.Contains(g, "sync.(*Mutex).Lock") {
			return g
		}
	}
	return ""
}

// callBounded runs f on its own goroutine behind a query panic boundary (the
// recover Pipeline.Run and the worker's task boundary apply), and returns its
// error, or ok=false when it is still running at the deadline.
func callBounded(f func() error, d time.Duration) (err error, ok bool) {
	done := make(chan error, 1)
	go func() {
		done <- func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = RecoverQueryPanic(context.Background(), "test boundary", r)
				}
			}()
			return f()
		}()
	}()
	select {
	case err := <-done:
		return err, true
	case <-time.After(d):
		return nil, false
	}
}

// mustReturn fails the test, with the blocked goroutine's stack, when f is
// still running at the deadline. marker names the frame a self-deadlock would
// be blocked in.
func mustReturn(t *testing.T, what, marker string, f func() error) error {
	t.Helper()
	err, ok := callBounded(f, lockReleaseDeadline)
	if ok {
		return err
	}
	dump := allGoroutines()
	if g := blockedLockingIn(dump, marker); g != "" {
		t.Fatalf("%s never returned (%v): a leaked lock, the goroutine is blocked locking a mutex that was "+
			"never released (a self-deadlock when the build's own deferred close is the locker), in %s:\n%s",
			what, lockReleaseDeadline, marker, g)
	}
	t.Fatalf("%s never returned (%v), and no goroutine is blocked locking a mutex in %s:\n%s",
		what, lockReleaseDeadline, marker, dump)
	return nil
}

// buildDeferMarker is the frame of Build's deferred relief close, which locks h.mu.
const buildDeferMarker = "exec.(*HashJoin).Build.func"

// checkReliefAfter asserts the operator and its manager still answer after a
// build that ended in a refusal or a panic: the join's own Inspect, a registry
// pass (RequestRelief ranks every registered operator through Inspect under
// the registry lock), and the join's Close.
func checkReliefAfter(t *testing.T, hj *HashJoin, sm *memory.SpillManager) {
	t.Helper()
	mustReturn(t, "HashJoin.Inspect after the build ended", "exec.(*HashJoin).Inspect", func() error {
		hj.Inspect()
		return nil
	})
	if sm != nil {
		mustReturn(t, "SpillManager.RequestRelief after the build ended", "exec.(*HashJoin).Inspect", func() error {
			_, err := sm.RequestRelief(1)
			return err
		})
	}
	mustReturn(t, "HashJoin.Close after the build ended", "exec.(*HashJoin).Close", hj.Close)
}

// keyRefusalText is the #615 refusal a build whose key vector disagrees with
// the resolved key type returns (keyEncodableErr). It carries no SQLSTATE.
const keyRefusalText = `join key "bk" is INT64 on the build side but the pair resolved to TIMESTAMP: ` +
	`the declared type the plan resolved from and the vector that arrived disagree, ` +
	`and keying it anyway would collapse every row onto one key (exec.HashJoin.KeyTypes, #615)`

// TestHashJoinBuildKeyRefusalReleasesLock: a build whose plan resolved the key
// to TIMESTAMP while the build vector is INT64 refuses loudly, on the grace
// path as on the flat path, and leaves nothing locked behind it.
func TestHashJoinBuildKeyRefusalReleasesLock(t *testing.T) {
	for _, grace := range []bool{true, false} {
		name := "flat"
		if grace {
			name = "grace"
		}
		t.Run(name, func(t *testing.T) {
			var tr *memory.Tracker
			var sm *memory.SpillManager
			if grace {
				var cleanup func()
				tr, sm, cleanup = newPhaseSpill(t, 1<<30)
				defer cleanup()
			}
			hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"bk"})
			if grace {
				hj.Spill, hj.MemTracker = sm, tr
			}
			hj.KeyTypes = []batch.TypeID{batch.TypeTimestamp}
			var buildErr error
			returned, ok := callBounded(func() error {
				return hj.Build(context.Background(), NewSliceSource(staleBuildSchema, staleBuildRows()))
			}, lockReleaseDeadline)
			if !ok {
				dump := allGoroutines()
				// What the wedge does to everyone else on the manager.
				wedged := ""
				if sm != nil {
					if _, rok := callBounded(func() error { _, err := sm.RequestRelief(1); return err },
						time.Second); !rok {
						wedged = " A registry pass issued after it (RequestRelief) is wedged too."
					}
				}
				if g := blockedLockingIn(dump, buildDeferMarker); g != "" {
					t.Fatalf("Build never returned its key-type refusal (%v): self-deadlock, the build returned "+
						"holding h.mu and its deferred relief close re-locks it.%s\n%s",
						lockReleaseDeadline, wedged, g)
				}
				t.Fatalf("Build never returned its key-type refusal (%v).%s\n%s", lockReleaseDeadline, wedged, dump)
			}
			buildErr = returned
			if buildErr == nil || buildErr.Error() != keyRefusalText {
				t.Fatalf("Build error = %v\nwant the #615 refusal: %s", buildErr, keyRefusalText)
			}
			var qp *QueryPanic
			if errors.As(buildErr, &qp) {
				t.Fatalf("the refusal arrived as a recovered panic: %v", buildErr)
			}
			checkReliefAfter(t, hj, sm)
			if tr != nil && tr.Used() != 0 {
				t.Errorf("tracker holds %d bytes after the refused build closed", tr.Used())
			}
		})
	}
}

// nilColumnSource hands Build one batch whose schema names a key column it
// carries no vector for: a stand-in for any panic raised inside the arrival's
// locked region (#615 names the encoder panic that region could raise before
// its type check existed). The vectors it does carry are real, so the batch
// passes the source boundary (FlattenForConsumer) and the panic is raised
// under h.mu, where the key column is first read.
type nilColumnSource struct{ sent bool }

var nilColumnSchema = []parquet.Column{
	{Name: "bid", Type: parquet.TypeInt64},
	{Name: "bk", Type: parquet.TypeInt64},
}

func (s *nilColumnSource) Init(context.Context) error { return nil }
func (s *nilColumnSource) Close() error               { return nil }
func (s *nilColumnSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.sent {
		return nil, nil
	}
	s.sent = true
	ok := batch.FromRows(nilColumnSchema[:1], []map[string]any{{"bid": int64(1)}})
	return &batch.RecordBatch{Schema: nilColumnSchema, Columns: []*batch.Vector{ok.Columns[0]}, Len: 1}, nil
}

// wantPanicErr asserts err is the query boundary's conversion of a panic.
func wantPanicErr(t *testing.T, err error) {
	t.Helper()
	var qp *QueryPanic
	if !errors.As(err, &qp) {
		t.Fatalf("Build error = %v, want the recovered panic (*QueryPanic)", err)
	}
}

// TestHashJoinBuildPanicReleasesLock: a panic raised while the build holds
// h.mu by hand reaches the query boundary, and the join, its manager and its
// Close still answer afterwards.
//
//   - grace/first-arrival: inside the arrival region (the key resolution
//     indexes the missing vector);
//   - grace/build-tail: inside the build tail, which round 1 put under h.mu
//     (the phase hook panics there);
//   - grace/relief-window: inside RequestRelief, which the build calls with
//     h.mu RELEASED (the relief-call hook panics there): the release must not
//     unlock a mutex that is not held;
//   - flat/first-arrival: the flat build is not registered, so the leaked lock
//     shows in its Close.
func TestHashJoinBuildPanicReleasesLock(t *testing.T) {
	t.Run("grace/first-arrival", func(t *testing.T) {
		tr, sm, cleanup := newPhaseSpill(t, 1<<30)
		defer cleanup()
		hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"bk"})
		hj.Spill, hj.MemTracker = sm, tr
		err := mustReturn(t, "grace Build after a panic in its first arrival", buildDeferMarker, func() error {
			return hj.Build(context.Background(), &nilColumnSource{})
		})
		wantPanicErr(t, err)
		checkReliefAfter(t, hj, sm)
	})

	t.Run("grace/build-tail", func(t *testing.T) {
		tr, sm, cleanup := newPhaseSpill(t, 1<<30)
		defer cleanup()
		hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"bk"})
		hj.Spill, hj.MemTracker = sm, tr
		restore := SetPhaseHook(func(s PhaseSite, op any) {
			if s == PhaseJoinBuildTail && op == hj {
				panic("injected panic in the build tail")
			}
		})
		defer restore()
		err := mustReturn(t, "grace Build after a panic in its build tail", buildDeferMarker, func() error {
			return hj.Build(context.Background(), NewSliceSource(staleBuildSchema, staleBuildRows()))
		})
		restore()
		wantPanicErr(t, err)
		checkReliefAfter(t, hj, sm)
	})

	t.Run("grace/relief-window", func(t *testing.T) {
		// A Sort holding its input is the registered victim; the requester's
		// single arrival does not fit beside it and asks the registry for
		// relief, and the relief-call hook panics on the requester's goroutine
		// with the requester's h.mu released.
		input, inputBytes := sortPhaseInput()
		reqPad := reqPadFor(inputBytes)
		reqCost := hashBuildBytes(batch.FromRows(staleReqSchema, staleReqRows(reqPad)))
		tr, sm, cleanup := newPhaseSpill(t, inputBytes+reqCost/2)
		defer cleanup()
		s := NewSort([]SortKey{{Column: "pad", Order: Descending}})
		s.Spill = sm
		ctx := context.Background()
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		for _, b := range input {
			if err := s.Consume(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
		defer s.Close()
		var hit atomic.Bool
		restore := memory.SetReliefCallHook(func(site memory.ReliefSite, op memory.AccountedOperator) {
			if site == memory.ReliefSiteRequest && op == memory.AccountedOperator(s) && hit.CompareAndSwap(false, true) {
				panic("injected panic in a relief call")
			}
		})
		defer restore()
		hj := NewHashJoin(InnerJoin, []string{"x"}, []string{"rk"})
		hj.Spill, hj.MemTracker = sm, tr
		err := mustReturn(t, "grace Build after a panic inside its relief request", buildDeferMarker, func() error {
			return hj.Build(ctx, NewSliceSource(staleReqSchema, staleReqRows(reqPad)))
		})
		restore()
		if !hit.Load() {
			t.Fatalf("schedule not reached: the registry never selected the sort (Build error: %v)", err)
		}
		wantPanicErr(t, err)
		checkReliefAfter(t, hj, sm)
	})

	t.Run("flat/first-arrival", func(t *testing.T) {
		hj := NewHashJoin(InnerJoin, []string{"pk"}, []string{"bk"})
		err := mustReturn(t, "flat Build after a panic in its first arrival", "exec.(*HashJoin).Build", func() error {
			return hj.Build(context.Background(), &nilColumnSource{})
		})
		wantPanicErr(t, err)
		checkReliefAfter(t, hj, nil)
	})
}

// TestWindowFinalizePanicReleasesLock: Window.finalizeExternal flushes the
// in-memory remainder as one more sorted run with w.mu held by hand. A panic
// raised by that flush (here: a buffered batch whose selection vector names a
// row it does not have, which Consume buffers without reading and the run
// sort then indexes) reaches the query boundary, and the Window's Close and
// Inspect still answer afterwards instead of blocking on the leaked w.mu.
func TestWindowFinalizePanicReleasesLock(t *testing.T) {
	tr, sm, cleanup := newPhaseSpill(t, 1<<30)
	defer cleanup()
	schema := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "id", Type: parquet.TypeInt64},
	}
	mk := func(base, rows int) *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, rows)
		for r := 0; r < rows; r++ {
			b.Columns[0].Int64Data[r] = 1
			b.Columns[1].Int64Data[r] = int64(base + r)
		}
		b.Len = rows
		return b
	}
	w := NewWindow([]WindowColumn{{
		Func: WinRowNumber, OutputCol: "rn", OutputType: parquet.TypeInt64,
		PartitionBy: []string{"k"},
		OrderBy:     []SortKey{{Column: "id", Order: Ascending}},
	}})
	w.Spill = sm
	ctx := context.Background()
	if err := w.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Every second Consume spills: the first two batches become a run, so
	// Finalize takes finalizeExternal; the third (malformed) stays buffered.
	restore := ForceWindowSpillEvery(2)
	defer ForceWindowSpillEvery(restore)
	for i := 0; i < 2; i++ {
		if err := w.Consume(ctx, mk(i*100, 100)); err != nil {
			t.Fatal(err)
		}
	}
	bad := mk(1000, 4)
	bad.Sel = []uint32{0, 1, 2, 4096}
	if err := w.Consume(ctx, bad); err != nil {
		t.Fatal(err)
	}
	ForceWindowSpillEvery(restore)
	if len(w.runFiles) == 0 {
		t.Fatal("schedule not reached: no run was spilled before Finalize")
	}
	err := mustReturn(t, "Window.Finalize after a panic in its last flush", "exec.(*Window).Finalize", func() error {
		return w.Finalize(ctx)
	})
	wantPanicErr(t, err)
	mustReturn(t, "Window.Inspect after Finalize panicked", "exec.(*Window).Inspect", func() error {
		w.Inspect()
		return nil
	})
	mustReturn(t, "Window.Close after Finalize panicked", "exec.(*Window).Close", w.Close)
	if u := tr.Used(); u != 0 {
		t.Errorf("tracker holds %d bytes after the window closed", u)
	}
}

// TestAggregateInspectAfterFinalizeStarts: a stale Inspect that takes h.mu
// after Finalize's first step (reliefClosed set under h.mu) and before its
// OpClosed store reports the aggregate closed and reads none of the group
// state Finalize goes on to rewrite without h.mu. The schedule is pinned by
// running Finalize's first step here and then, side by side with no ordering
// between them, the Inspect and a write Finalize's partial-merge path makes
// unlocked (resetGroupStateAfterSpill: trackedGroupMem and the tables). Under
// -race the unfixed Inspect is a reported race on trackedGroupMem; without
// -race it reports an active footprint.
func TestAggregateInspectAfterFinalizeStarts(t *testing.T) {
	_, sm, cleanup := newPhaseSpill(t, 1<<30)
	defer cleanup()
	h := newAggPhase(sm)
	ctx := context.Background()
	if err := h.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, b := range aggPhaseInput() {
		if err := h.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	defer h.Close()
	if f := h.Inspect(); f.State == memory.OpClosed || f.OwnedBytes <= 0 {
		t.Fatalf("precondition: a consuming aggregate reports %+v, want an active footprint with owned bytes", f)
	}

	// Finalize's first step, verbatim (agg_spill.go): relief closes under h.mu.
	h.mu.Lock()
	h.reliefClosed = true
	h.mu.Unlock()

	var foot memory.OperatorFootprint
	done := make(chan struct{}, 2)
	go func() { foot = h.Inspect(); done <- struct{}{} }()
	go func() { h.resetGroupStateAfterSpill(); done <- struct{}{} }()
	<-done
	<-done
	if foot.State != memory.OpClosed || foot.OwnedBytes != 0 {
		t.Errorf("Inspect after Finalize closed relief = %+v, want the closed footprint (it read group state "+
			"Finalize rewrites without h.mu)", foot)
	}
}
