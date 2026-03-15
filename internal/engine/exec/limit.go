package exec

import (
	"context"
	"sync/atomic"

	"github.com/derekmwright/caelum/internal/engine/batch"
)

// Limit is a UnaryOperator that passes through at most N rows.
type Limit struct {
	Max     int64
	passed  atomic.Int64
}

func NewLimit(n int64) *Limit {
	return &Limit{Max: n}
}

func (l *Limit) Init(_ context.Context) error {
	l.passed.Store(0)
	return nil
}

func (l *Limit) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	current := l.passed.Load()
	if current >= l.Max {
		return nil, nil
	}

	remaining := l.Max - current
	activeLen := int64(in.ActiveLen())

	if activeLen <= remaining {
		l.passed.Add(activeLen)
		return in, nil
	}

	// Truncate to remaining
	sel := make([]uint16, 0, remaining)
	if in.Sel != nil {
		for _, idx := range in.Sel {
			if int64(len(sel)) >= remaining {
				break
			}
			sel = append(sel, idx)
		}
	} else {
		for i := 0; i < int(remaining); i++ {
			sel = append(sel, uint16(i))
		}
	}

	in.Sel = sel
	l.passed.Add(int64(len(sel)))
	return in, nil
}

func (l *Limit) Close() error { return nil }

// TopN is a Sink that combines sort + limit efficiently by keeping only the
// top N rows in a heap. For small N this is much more efficient than full sort.
type TopN struct {
	Keys  []SortKey
	N     int
	inner *Sort
}

func NewTopN(keys []SortKey, n int) *TopN {
	return &TopN{
		Keys:  keys,
		N:     n,
		inner: NewSort(keys),
	}
}

func (t *TopN) Init(ctx context.Context) error {
	return t.inner.Init(ctx)
}

func (t *TopN) Consume(ctx context.Context, b *batch.RecordBatch) error {
	return t.inner.Consume(ctx, b)
}

func (t *TopN) Finalize(ctx context.Context) error {
	if err := t.inner.Finalize(ctx); err != nil {
		return err
	}
	// Truncate sorted batches to N rows
	remaining := t.N
	for i, b := range t.inner.sorted {
		if remaining <= 0 {
			t.inner.sorted = t.inner.sorted[:i]
			break
		}
		if b.Len > remaining {
			// Truncate this batch via selection vector
			sel := make([]uint16, remaining)
			for j := range sel {
				sel[j] = uint16(j)
			}
			b.Sel = sel
			t.inner.sorted = t.inner.sorted[:i+1]
			break
		}
		remaining -= b.ActiveLen()
	}
	return nil
}

func (t *TopN) Close() error { return t.inner.Close() }

func (t *TopN) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return t.inner.Next(ctx)
}
