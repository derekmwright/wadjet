package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A NESTED CHAIN'S DRIVER PULLS ITS SOURCE UNTIL IT ENDS, AND NOT AFTER —
// arc N1 round 2 (the gate `046292a4` owed).
//
// `pipelineSource.nextFlushed` returns a REAL batch, so the consumer calls
// `Next` again — and before the `drained` latch that call asked the source for
// another batch before looking at the flush. Every source in the tree happens
// to answer nil a second time, so nothing failed; "already returned nil" is
// not part of the `exec.Source` contract, and a driver has no business relying
// on it. `n1CountingSource` is the contract written down: it fails the test if
// it is pulled after it has ended.
//
// The cell also holds the drain itself, because the two are one behaviour: the
// flushable operator's batches come out AFTER the source's, exactly once, in
// order, and through the operators above it.
func TestN1APipelineSourceDrainsWithoutRepullingItsSource(t *testing.T) {
	ctx := context.Background()
	schema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}}
	src := &n1CountingSource{t: t, schema: schema, rows: []int64{1, 2}}
	flusher := &n1FlushOp{schema: schema, rows: []int64{3, 4}}
	ps := &pipelineSource{source: src, ops: []exec.UnaryOperator{flusher}}
	if err := ps.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	var got []int64
	for {
		b, err := ps.Next(ctx)
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		for i := 0; i < b.Len; i++ {
			got = append(got, b.Columns[0].Int64Data[i])
		}
	}
	// One more, the way a wrapper that has already seen nil may ask again.
	if b, err := ps.Next(ctx); err != nil || b != nil {
		t.Fatalf("a second end-of-input call answered (%v, %v), want (nil, nil)", b, err)
	}
	want := []int64{1, 2, 3, 4}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v — the source's rows then the operator's flushed ones", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if src.pullsAfterEnd != 0 {
		t.Errorf("the source was pulled %d times after it ended", src.pullsAfterEnd)
	}
	if flusher.flushes != 2 {
		t.Errorf("the operator's flush was called %d times, want 2 — one per held batch, "+
			"and no call after HasPendingFlush went false", flusher.flushes)
	}
}

// n1CountingSource emits one row per Next and then ends — and RECORDS every
// pull that arrives after it has ended, which is the property under test.
type n1CountingSource struct {
	t             *testing.T
	schema        []parquet.Column
	rows          []int64
	idx           int
	ended         bool
	pullsAfterEnd int
}

func (s *n1CountingSource) Init(context.Context) error { return nil }

func (s *n1CountingSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.ended {
		s.pullsAfterEnd++
		return nil, nil
	}
	if s.idx >= len(s.rows) {
		s.ended = true
		return nil, nil
	}
	b := n1OneRow(s.schema, s.rows[s.idx])
	s.idx++
	return b, nil
}

func (s *n1CountingSource) Close() error { return nil }

// n1FlushOp passes its input through and holds rows of its own to FLUSH, the
// way a hash join holds the partitions it evicted.
type n1FlushOp struct {
	schema  []parquet.Column
	rows    []int64
	idx     int
	flushes int
}

func (o *n1FlushOp) Init(context.Context) error { return nil }

func (o *n1FlushOp) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	return in, nil
}

func (o *n1FlushOp) Close() error { return nil }

func (o *n1FlushOp) HasPendingFlush() bool { return o.idx < len(o.rows) }

func (o *n1FlushOp) NextFlush(context.Context) (*batch.RecordBatch, error) {
	o.flushes++
	if o.idx >= len(o.rows) {
		return nil, nil
	}
	b := n1OneRow(o.schema, o.rows[o.idx])
	o.idx++
	return b, nil
}

func n1OneRow(schema []parquet.Column, v int64) *batch.RecordBatch {
	b := batch.NewRecordBatch(schema, 1)
	b.Columns[0].SetValue(0, v)
	return b
}
