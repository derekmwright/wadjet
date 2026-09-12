package worker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BOTH BRANCHES OF THE BREAKER DECISION INITIALIZE THEIR SINK — #1058.
//
// A fragment's breaker-consume phase runs through `exec.Pipeline.Run` at k == 1
// and through `runBreakerConsumeParallel` at k > 1, and the two have to set the
// operator up identically. The parallel branch initialized the CLONED sinks and
// left the primary as its constructor made it, which is where #1058 lived: a
// `HashAggregate` built by the worker's fragment builder is initialized nowhere
// else, so its `strNullGroupIdx` held the zero value — the VALID slot 0 — and
// every NULL key of a single STRING or BYTES GROUP BY in the primary's morsel
// share bound whichever group the primary minted first.
//
// THIS GATE HOLDS THE CALL, NOT THE ANSWER, and that is the point. #1058's fix
// also gives `NewHashAggregate` the sentinel, so an answer-level census passes
// with `sink.Init` reverted — the guard covers the state the omission produced
// TODAY. The next field whose zero value is not its resting value would have no
// such guard, and nothing would fail. So the assertion here is the call itself,
// recorded by a stub sink, made from both branches:
// `TestC3ANullGroupKeyIsItsOwnGroupOnEveryArm` proves the ANSWER and this
// proves the SETUP. It is the same shape as
// `exec.TestCloneSinkCallersConsultTheCloneFence`, one decision over.
//
// `drainThroughBreaker` — the third consumer, for the breakers at index > 0 of
// a multi-breaker fragment — is NOT held here: the planner emits no such
// fragment, and its comment records that the constructor sentinel is what
// covers a HashAggregate reached that way.
func TestBothBranchesOfTheBreakerDecisionInitializeTheirSink(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeInt64},
	}
	rows := [][2]int64{{1, 10}, {2, 20}}

	t.Run("the PARALLEL branch (k>1, runBreakerConsumeParallel)", func(t *testing.T) {
		sink := &initRecordingSink{}
		executor := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
		src := &staticBatchSource{schema: schema, rows: rows, count: 6}
		task := distributed.Task{ID: "init-par", QueryID: "q-init", StageID: "agg-0", StageType: "aggregate"}
		before := MorselParallelBreakerRuns.Load()
		if err := executor.runBreakerConsumeParallel(ctx, task, src, nil, sink, 4, nil); err != nil {
			t.Fatalf("runBreakerConsumeParallel: %v", err)
		}
		// The branch really ran — a gate over a branch says nothing about a
		// branch that did not (ADR-0027).
		if MorselParallelBreakerRuns.Load() == before {
			t.Fatal("the parallel branch did not run, so this cell proves nothing")
		}
		if n := sink.inits.Load(); n != 1 {
			t.Errorf("the primary sink was initialized %d times, want exactly 1: the "+
				"k>1 branch must set the operator up exactly as exec.Pipeline.Run does "+
				"at k==1 (#1058)", n)
		}
		// The clones are initialized too, and by their own path — so a run
		// that initialized only clones cannot pass the assertion above.
		if n := sink.cloneInits.Load(); n != 3 {
			t.Errorf("cloned sinks initialized %d times, want 3 (k-1)", n)
		}
		if !sink.initBeforeConsume.Load() {
			t.Error("the primary consumed a batch before it was initialized (#1058)")
		}
	})

	t.Run("the SERIAL branch (k==1, exec.Pipeline.Run)", func(t *testing.T) {
		sink := &initRecordingSink{}
		src := &staticBatchSource{schema: schema, rows: rows, count: 3}
		pipe := &exec.Pipeline{Source: src, Sink: sink}
		if err := pipe.Run(ctx); err != nil {
			t.Fatalf("pipeline run: %v", err)
		}
		if n := sink.inits.Load(); n != 1 {
			t.Errorf("the serial branch initialized the sink %d times, want 1", n)
		}
		if !sink.initBeforeConsume.Load() {
			t.Error("the serial branch consumed before initializing")
		}
	})
}

// initRecordingSink is a MergeableSink that records whether Init was called on
// it before its first Consume, and how many of its clones were initialized.
// It aggregates nothing: the question is the SETUP, not the answer.
type initRecordingSink struct {
	inits             atomic.Int32
	cloneInits        atomic.Int32
	consumes          atomic.Int32
	initBeforeConsume atomic.Bool
	parent            *initRecordingSink
}

func (s *initRecordingSink) Init(context.Context) error {
	if s.parent != nil {
		s.parent.cloneInits.Add(1)
		return nil
	}
	s.inits.Add(1)
	if s.consumes.Load() == 0 {
		s.initBeforeConsume.Store(true)
	}
	return nil
}

func (s *initRecordingSink) Consume(_ context.Context, b *batch.RecordBatch) error {
	root := s
	if s.parent != nil {
		root = s.parent
	}
	root.consumes.Add(1)
	if b.ActiveLen() == 0 {
		return nil
	}
	return nil
}

func (s *initRecordingSink) Finalize(context.Context) error { return nil }
func (s *initRecordingSink) Close() error                   { return nil }

func (s *initRecordingSink) Next(context.Context) (*batch.RecordBatch, error) { return nil, nil }

func (s *initRecordingSink) CloneSink() exec.SinkSource {
	root := s
	if s.parent != nil {
		root = s.parent
	}
	return &initRecordingSink{parent: root}
}

func (s *initRecordingSink) MergeSink(exec.SinkSource) {}
