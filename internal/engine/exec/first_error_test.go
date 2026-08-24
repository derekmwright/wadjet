package exec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// sentinelErr is a second concrete error type, deliberately not *fmt.wrapError.
type sentinelErr struct{ what string }

func (s sentinelErr) Error() string { return s.what }

// TestFirstErrorTakesDifferentConcreteTypes is the direct regression for #512:
// a bare sync/atomic.Value panics when two stores carry different concrete
// types, which is what two parallel workers reporting differently-shaped
// errors did to the whole server process.
func TestFirstErrorTakesDifferentConcreteTypes(t *testing.T) {
	var f FirstError
	f.Set(fmt.Errorf("wrapped: %w", errors.New("inner"))) // *fmt.wrapError
	f.Set(sentinelErr{what: "second shape"})              // exec.sentinelErr
	f.Set(errors.New("third shape"))                      // *errors.errorString

	if got := f.Err(); got == nil || got.Error() != "wrapped: inner" {
		t.Fatalf("Err() = %v, want the FIRST error", got)
	}
}

// TestFirstErrorIsRaceFreeAcrossShapes drives the same slot from many
// goroutines with alternating concrete types — the actual soak shape.
func TestFirstErrorIsRaceFreeAcrossShapes(t *testing.T) {
	var f FirstError
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				f.Set(fmt.Errorf("worker %d: %w", i, errors.New("boom")))
				return
			}
			f.Set(sentinelErr{what: fmt.Sprintf("worker %d", i)})
		}(i)
	}
	wg.Wait()
	if f.Err() == nil {
		t.Fatal("Err() = nil, want one of the reported errors")
	}
}

func TestFirstErrorIgnoresNil(t *testing.T) {
	var f FirstError
	f.Set(nil)
	if got := f.Err(); got != nil {
		t.Fatalf("Err() = %v, want nil", got)
	}
	f.Set(errors.New("real"))
	if got := f.Err(); got == nil || got.Error() != "real" {
		t.Fatalf("Err() = %v, want the real error", got)
	}
}

// waitFor spins until flag is set, bounded so a scheduling accident fails the
// test rather than hanging it.
func waitFor(flag *atomic.Bool) {
	deadline := time.Now().Add(5 * time.Second)
	for !flag.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}

// The two halves below stage #512's exact sequence, which is a race in the
// wild (17 hits in one soak, never on demand) and must be deterministic here.
//
// Both shapes have to reach runParallel's shared first-error slot, and only
// two of that function's stores can carry different concrete types:
//
//   - a worker's driver.Push error, a *fmt.wrapError, stored under a
//     "unless the pipeline is already cancelling" guard;
//   - whatever the worker's own defer recovers, stored UNCONDITIONALLY.
//
// A panic raised inside an operator never reaches the second one — the
// ChainDriver's own recover converts it first — so the panicking half has to
// be the SOURCE, which the worker calls outside the driver. Hence: one worker
// blocks inside Source.Next, another worker's operator then fails, and only
// once that error is in the slot does the blocked source panic.

type shapeRaceSource struct {
	inner   *SliceSource
	calls   atomic.Int64
	blocked *atomic.Bool // this source is parked inside Next
	armed   *atomic.Bool // an operator error has reached the slot
}

func (s *shapeRaceSource) Init(ctx context.Context) error { return s.inner.Init(ctx) }

func (s *shapeRaceSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	// Call 1 is the warmup pull on the caller's goroutine; call 2 gets a
	// worker running so an operator can fail. Call 3 is the one that parks.
	if s.calls.Add(1) == 3 {
		s.blocked.Store(true)
		waitFor(s.armed)
		time.Sleep(20 * time.Millisecond) // let the operator error land first
		panic(fatalEvalError{err: sentinelErr{what: "source raised a query error"}})
	}
	return s.inner.Next(ctx)
}

func (s *shapeRaceSource) Close() error { return s.inner.Close() }

type shapeRaceOp struct {
	calls   *atomic.Int64
	blocked *atomic.Bool
	armed   *atomic.Bool
}

func (o *shapeRaceOp) Init(context.Context) error { return nil }

func (o *shapeRaceOp) Execute(_ context.Context, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	if o.calls.Add(1) == 1 {
		return b, nil // warmup batch, on the caller's goroutine
	}
	waitFor(o.blocked)
	o.armed.Store(true)
	return nil, fmt.Errorf("operator failed: %w", errors.New("driver error"))
}

func (o *shapeRaceOp) Close() error { return nil }

func (o *shapeRaceOp) Clone() UnaryOperator {
	return &shapeRaceOp{calls: o.calls, blocked: o.blocked, armed: o.armed}
}

// TestRunParallelSurvivesRacingErrorShapes is #512's end-to-end regression:
// the pipeline must come back with an error, not take the process down.
func TestRunParallelSurvivesRacingErrorShapes(t *testing.T) {
	schema := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}}
	rows := make([]map[string]any, 0, 32*batch.DefaultBatchSize)
	for i := 0; i < 32*batch.DefaultBatchSize; i++ {
		rows = append(rows, map[string]any{"a": int64(i)})
	}
	// Repeat so a fixed build cannot pass on scheduling luck alone.
	for attempt := 0; attempt < 5; attempt++ {
		blocked, armed := new(atomic.Bool), new(atomic.Bool)
		pipe := &Pipeline{
			Source: &shapeRaceSource{
				inner:   NewSliceSource(schema, rows),
				blocked: blocked,
				armed:   armed,
			},
			Ops: []UnaryOperator{&shapeRaceOp{
				calls:   new(atomic.Int64),
				blocked: blocked,
				armed:   armed,
			}},
			Sink:    &CollectSink{},
			Workers: 8,
		}
		err := pipe.Run(context.Background())
		if err == nil {
			t.Fatalf("attempt %d: Run() = nil, want the first worker error", attempt)
		}
		if !strings.Contains(err.Error(), "failed") && !strings.Contains(err.Error(), "query error") {
			t.Fatalf("attempt %d: Run() = %v, want a worker error", attempt, err)
		}
	}
}
