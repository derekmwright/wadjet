// This file holds pipeline source for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
)

// pipelineSource wraps a Source + UnaryOps into a single Source.
//
// It honours the bounded-output protocol (exec.BoundedOutputOperator, #317):
// an operator whose output for one input batch can be far larger than that
// batch — a hash-join probe fans one probe row out to every build row sharing
// its key — emits a bounded slice and suspends the rest, and a Next that finds
// pending output resumes it instead of pulling new input. Being a pull driver
// makes that natural: one Next, one batch.
//
// Resumption goes DEEPEST first. A suspended operator's pending output was
// produced from an input the operators after it have not seen yet, so it must
// drain before the operator above it is asked for its next slice.
type pipelineSource struct {
	source  exec.Source
	ops     []exec.UnaryOperator
	bounded []exec.BoundedOutputOperator // parallel to ops; nil = single-shot op
	inited  bool
	// flushIdx is the operator whose SPILLED partitions this source is
	// draining now that its input is exhausted; len(ops) means every one of
	// them has been drained. See nextFlushed (#1010).
	flushIdx int
	// drained latches the input's end. Once the source has answered nil it is
	// never pulled again: a flushed batch is a real return, so the consumer
	// calls Next once more, and an exhausted source is not owed a second
	// question.
	drained bool
}

func (ps *pipelineSource) Init(ctx context.Context) error {
	if ps.inited {
		return nil
	}
	ps.inited = true
	if err := ps.source.Init(ctx); err != nil {
		return err
	}
	for _, op := range ps.ops {
		if err := op.Init(ctx); err != nil {
			return err
		}
	}
	// The opt-in is this driver's promise to drain pending output before
	// supplying the next input batch.
	exec.EnableBoundedOutput(ps.ops)
	ps.bounded = make([]exec.BoundedOutputOperator, len(ps.ops))
	for i, op := range ps.ops {
		if bo, ok := op.(exec.BoundedOutputOperator); ok {
			ps.bounded[i] = bo
		}
	}
	return nil
}

func (ps *pipelineSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		if i := ps.pendingFrom(); i >= 0 {
			out, err := ps.bounded[i].NextOutput(ctx)
			if err != nil {
				return nil, err
			}
			if out == nil {
				continue // that operator finished; look for the next one
			}
			b, err := ps.runFrom(ctx, i+1, out)
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			continue
		}
		if ps.drained {
			return ps.nextFlushed(ctx)
		}
		b, err := ps.source.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			ps.drained = true
			return ps.nextFlushed(ctx)
		}
		b, err = ps.runFrom(ctx, 0, b)
		if err != nil {
			return nil, err
		}
		if b != nil {
			return b, nil
		}
	}
}

// nextFlushed drains every nested operator's spilled partitions after input
// exhaustion, one batch per call. Use exec.FlushableOperator in ascending order,
// pushing each flushed batch through every operator above its producer.
// These ps.ops are absent from the outer Pipeline's flushSpilledOps, including
// join builds, set-operation arms and lateral inner chains; omitting this drain
// silently drops spilled rows (#1010). joinFlushSource covers only a RIGHT/FULL
// join's own probe (#550), not every nested chain.
func (ps *pipelineSource) nextFlushed(ctx context.Context) (*batch.RecordBatch, error) {
	for ps.flushIdx < len(ps.ops) {
		fo, ok := ps.ops[ps.flushIdx].(exec.FlushableOperator)
		if !ok || !fo.HasPendingFlush() {
			ps.flushIdx++
			continue
		}
		b, err := fo.NextFlush(ctx)
		if err != nil {
			return nil, fmt.Errorf("flushing spilled data: %w", err)
		}
		if b == nil {
			ps.flushIdx++
			continue
		}
		out, err := ps.runFrom(ctx, ps.flushIdx+1, b)
		if err != nil {
			return nil, err
		}
		if out == nil {
			continue
		}
		return out, nil
	}
	return nil, nil
}

// pendingFrom returns the index of the deepest operator with output still to
// emit, or -1. nil bounded (Init not run) means nothing ever suspends.
func (ps *pipelineSource) pendingFrom() int {
	for i := len(ps.bounded) - 1; i >= 0; i-- {
		if bo := ps.bounded[i]; bo != nil && bo.HasPendingOutput() {
			return i
		}
	}
	return -1
}

// runFrom pushes b through ops[i:] and returns what comes out the end. An
// operator that suspends keeps its remainder; the next Next resumes it.
func (ps *pipelineSource) runFrom(ctx context.Context, i int, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	for ; i < len(ps.ops); i++ {
		op := ps.ops[i]
		exec.FlattenForConsumer(b, op)
		var err error
		b, err = op.Execute(ctx, b)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}
	}
	return b, nil
}

// Close is nil-receiver and nil-source safe. Wrappers assign their
// pipelineSource in Init and delegate their own Close to it, so a source
// closed without ever being initialized arrives here as a nil receiver —
// which used to be a segfault, i.e. the whole server (#510). Every Close in
// the teardown path has to be reachable from a half-built plan.
func (ps *pipelineSource) Close() error {
	if ps == nil || ps.source == nil {
		return nil
	}
	err := ps.source.Close()
	for _, op := range ps.ops {
		if e := op.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
