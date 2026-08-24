package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// countingSource records how often it was closed.
type countingSource struct{ closes int }

func (s *countingSource) Init(context.Context) error { return nil }

func (s *countingSource) Next(context.Context) (*batch.RecordBatch, error) { return nil, nil }

func (s *countingSource) Close() error { s.closes++; return nil }

// countingOp records how often it was closed.
type countingOp struct{ closes int }

func (o *countingOp) Init(context.Context) error { return nil }

func (o *countingOp) Execute(_ context.Context, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	return b, nil
}

func (o *countingOp) Close() error { o.closes++; return nil }

// TestJoinFlushSourceCloseWithoutInit is #510's regression.
//
// Both flush wrappers assign their pipelineSource in Init and delegated
// Close to it unconditionally, so a source that is constructed and then
// closed without an intervening Init dereferenced a nil pointer — a
// segfault, which is the whole SERVER process, not one query. The soak
// caught it two setOpSourceAdapter levels deep on a RIGHT/FULL OUTER JOIN
// under a set operation.
//
// Closing has to release what construction built, so "nil-check and return
// nil" is not the fix: the inner source and operators must still be closed.
func TestJoinFlushSourceCloseWithoutInit(t *testing.T) {
	t.Run("joinFlushSource", func(t *testing.T) {
		inner := &countingSource{}
		op := &countingOp{}
		s := &joinFlushSource{inner: inner, innerOps: []exec.UnaryOperator{op}}
		if err := s.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		if inner.closes != 1 || op.closes != 1 {
			t.Fatalf("closes: source=%d op=%d, want 1 and 1 — Close must release what "+
				"construction built, not just avoid the nil", inner.closes, op.closes)
		}
	})

	t.Run("rightSemiFlushSource", func(t *testing.T) {
		inner := &countingSource{}
		op := &countingOp{}
		s := &rightSemiFlushSource{inner: inner, innerOps: []exec.UnaryOperator{op}}
		if err := s.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		if inner.closes != 1 || op.closes != 1 {
			t.Fatalf("closes: source=%d op=%d, want 1 and 1", inner.closes, op.closes)
		}
	})
}

// TestJoinFlushSourceCloseUnderSetOpAdapters reproduces the filed stack's
// shape: two nested setOpSourceAdapter Closes wrapping an un-Init'd
// joinFlushSource, driven by a Pipeline.Close the way the coordinator's
// local fast path drives it.
func TestJoinFlushSourceCloseUnderSetOpAdapters(t *testing.T) {
	inner := &countingSource{}
	flush := &joinFlushSource{inner: inner, innerOps: []exec.UnaryOperator{&countingOp{}}}
	nested := &setOpSourceAdapter{
		leftSource:  flush,
		rightSource: &countingSource{},
		op:          "union",
	}
	outer := &setOpSourceAdapter{
		leftSource:  nested,
		rightSource: &countingSource{},
		op:          "union",
	}
	pipe := &exec.Pipeline{Source: outer, Sink: &exec.CollectSink{}}
	if err := pipe.Close(); err != nil {
		t.Fatalf("Pipeline.Close() = %v, want nil", err)
	}
	if inner.closes != 1 {
		t.Fatalf("inner source closes = %d, want 1", inner.closes)
	}
}

// TestPipelineSourceCloseIsNilSafe pins the receiver-level guard directly:
// pipelineSource.Close was the immediate crash site, reached with a nil
// receiver from a wrapper that had never run Init.
func TestPipelineSourceCloseIsNilSafe(t *testing.T) {
	var ps *pipelineSource
	if err := ps.Close(); err != nil {
		t.Fatalf("(*pipelineSource)(nil).Close() = %v, want nil", err)
	}
	if err := (&pipelineSource{}).Close(); err != nil {
		t.Fatalf("zero pipelineSource Close() = %v, want nil", err)
	}
}
