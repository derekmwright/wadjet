package exec

import (
	"context"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// Limit is a UnaryOperator that passes through at most N rows,
// optionally skipping the first Offset rows.
type Limit struct {
	Max     int64
	Offset  int64
	seen    atomic.Int64 // total rows seen (for offset tracking)
	passed  atomic.Int64 // rows passed through after offset
}

func NewLimit(n, offset int64) *Limit {
	return &Limit{Max: n, Offset: offset}
}

func (l *Limit) Init(_ context.Context) error {
	l.seen.Store(0)
	l.passed.Store(0)
	return nil
}

func (l *Limit) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if l.passed.Load() >= l.Max {
		return nil, nil
	}

	activeLen := int64(in.ActiveLen())

	// Handle OFFSET: skip rows until we've seen enough
	if l.Offset > 0 {
		seen := l.seen.Load()
		if seen+activeLen <= l.Offset {
			// Entire batch is within offset — skip it
			l.seen.Add(activeLen)
			return nil, nil
		}
		if seen < l.Offset {
			// Partial skip: trim the front of this batch
			skip := int(l.Offset - seen)
			l.seen.Store(l.Offset)
			sel := make([]uint16, 0, activeLen-int64(skip))
			if in.Sel != nil {
				for i := skip; i < len(in.Sel); i++ {
					sel = append(sel, in.Sel[i])
				}
			} else {
				for i := skip; i < int(activeLen); i++ {
					sel = append(sel, uint16(i))
				}
			}
			in.Sel = sel
			activeLen = int64(len(sel))
			if activeLen == 0 {
				return nil, nil
			}
		}
	}

	remaining := l.Max - l.passed.Load()
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

// Clone returns the same Limit instance. Limit uses atomic counters for
// seen/passed tracking, making it safe for concurrent Execute calls from
// multiple pipeline workers.
func (l *Limit) Clone() UnaryOperator {
	return l
}

// Done returns true when the limit has been satisfied, enabling pipeline
// early termination (LIMIT pushdown).
func (l *Limit) Done() bool {
	return l.passed.Load() >= l.Max
}

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

// InnerSort returns the underlying Sort for use with sortSourceAdapter.
func (t *TopN) InnerSort() *Sort {
	return t.inner
}
