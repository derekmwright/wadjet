package exec

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// panicNthSource panics on the Nth Next call (1-based).
type panicNthSource struct {
	inner Source
	n     int64
	calls atomic.Int64
}

func (p *panicNthSource) Init(ctx context.Context) error { return p.inner.Init(ctx) }
func (p *panicNthSource) Close() error                   { return p.inner.Close() }

func (p *panicNthSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if p.calls.Add(1) == p.n {
		var s []int
		_ = s[7] // runtime panic: index out of range
	}
	return p.inner.Next(ctx)
}

// TestKeyOnlyBuildPanicUnderSourceMutexDoesNotDeadlock is the regression for
// the worst thing a panic boundary can do: turn a crash into a hang.
//
// buildParallelKeyOnly calls source.Next while HOLDING sourceMu, so a panic
// raised inside it unwinds past the Unlock. A boundary that recovers there
// without releasing the mutex leaves every sibling morsel worker blocked on
// sourceMu.Lock forever and wg.Wait never returns — the query stops answering
// while still holding its memory budget and its client connection, and no
// timeout in the engine ends it. A crashed server at least restarts.
//
// The panic surface is real: source is wrapped in a flattenSource, whose Next
// runs FlattenViews, which is the #361/#392 raise-a-typed-panic class.
func TestKeyOnlyBuildPanicUnderSourceMutexDoesNotDeadlock(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip("needs >1 CPU for the parallel key-only path")
	}
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeString}}
	rows := make([]map[string]any, 200000)
	for i := range rows {
		rows[i] = map[string]any{"k": "key"}
	}
	src := &panicNthSource{inner: NewSliceSource(schema, rows), n: 3}

	hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
	hj.SemiAntiKeyOnly = true

	done := make(chan error, 1)
	go func() { done <- hj.Build(context.Background(), src) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Build returned nil after a panic in the build source")
		}
		if !strings.Contains(err.Error(), "index out of range") {
			t.Errorf("Build error %q lost the panic value", err)
		}
	case <-time.After(30 * time.Second):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("DEADLOCK: Build did not return 30s after a recovered panic — the "+
			"boundary released the goroutine but not the source mutex.\n%s", buf[:n])
	}
}

// TestKeyOnlyBuildPanicStopsSiblings: the failing worker must also cancel, so
// the siblings stop pulling instead of draining the whole source after the
// build has already failed.
func TestKeyOnlyBuildPanicStopsSiblings(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip("needs >1 CPU for the parallel key-only path")
	}
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeString}}
	rows := make([]map[string]any, 400000)
	for i := range rows {
		rows[i] = map[string]any{"k": "key"}
	}
	src := &panicNthSource{inner: NewSliceSource(schema, rows), n: 2}

	hj := NewHashJoin(AntiJoin, []string{"k"}, []string{"k"})
	hj.SemiAntiKeyOnly = true

	start := time.Now()
	err := hj.Build(context.Background(), src)
	if err == nil {
		t.Fatal("Build returned nil after a panic in the build source")
	}
	// 400k rows is ~196 batches. Cancelling means the workers stop within a
	// few pulls of the panic rather than consuming all of them.
	if got := src.calls.Load(); got > 64 {
		t.Errorf("source pulled %d times after the panic on pull 2 — the siblings "+
			"were not cancelled", got)
	}
	t.Logf("failed in %v after %d source pulls: %v", time.Since(start), src.calls.Load(), err)
}
