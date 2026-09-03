package exec

import (
	"context"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// NoLimit is the Max of a Limit that only skips rows: `OFFSET n` with no
// LIMIT. Any negative Max means unbounded.
const NoLimit = int64(-1)

// Limit is a UnaryOperator that passes through at most Max rows, optionally
// skipping the first Offset rows. A negative Max is unbounded, which is how
// OFFSET without LIMIT is expressed.
//
// ONE Limit instance serves every morsel-parallel worker of a pipeline —
// Clone returns the receiver — so both counters are written concurrently.
// They are therefore CLAIM counters, not running totals: each Execute takes
// its batch's half-open interval of the input stream with a single Add and
// decides from the interval it was given. A Load followed by an Add would
// let two workers decide against the same stale value, and #567 is what that
// costs (see Execute).
type Limit struct {
	Max    int64
	Offset int64
	// seen claims positions in the operator's INPUT stream, for OFFSET.
	// Grows past Offset; only the claim boundaries are read.
	seen atomic.Int64
	// passed claims positions in the post-OFFSET stream, for Max. Counts
	// rows CLAIMED, so it can exceed Max by up to one batch — Done only
	// asks whether it has reached Max, which stays correct.
	passed atomic.Int64
}

func NewLimit(n, offset int64) *Limit {
	return &Limit{Max: n, Offset: offset}
}

// bounded reports whether Max caps the number of rows passed through.
func (l *Limit) bounded() bool { return l.Max >= 0 }

func (l *Limit) Init(_ context.Context) error {
	l.seen.Store(0)
	l.passed.Store(0)
	return nil
}

// AcceptsViews: Limit manipulates only the selection vector and row counts —
// it never reads column storage, so view columns pass through untouched.
func (l *Limit) AcceptsViews() bool { return true }

// Execute applies OFFSET and then Max to one batch.
//
// Both counters are advanced with a single atomic Add whose RESULT is the
// decision input, so the batch owns the interval [end-n, end) of the stream
// and no other worker can be given the same positions. Claims partition the
// stream however the workers interleave, which is what makes the row COUNT
// exact under morsel parallelism: every batch contributes exactly its
// interval's overlap with [Offset, ∞) and then with [0, Max).
//
// The earlier shape — `seen := l.seen.Load()`, decide, `l.seen.Add(n)` — is
// not that. Two workers reading the same `seen` both measured their batch
// against it, and the two ways that goes are both wrong answers:
//
//   - OVER-SKIP. The two-path fixture's `nation` is 25 rows in three files,
//     so three batches of 9/9/7. With `OFFSET 20`, two workers reading
//     `seen == 9` each found 9+9 and 9+7 to be within the offset and dropped
//     their batch whole. Nothing was left, and
//     `SELECT COUNT(*) FROM (SELECT n_nationkey FROM nation OFFSET 20) u`
//     answered 0 where the answer is 5 — silently, on the coordinator's
//     default route, roughly once in 1,600 executions of that shape
//     (#567, #765).
//   - UNDER-SKIP. Two workers in the partial-skip branch both trimmed
//     `Offset - seen` rows from the same stale `seen` and both stored
//     `l.Offset`, so fewer than Offset rows were skipped in total.
//
// `remaining := l.Max - l.passed.Load()` had the identical shape and
// over-DELIVERED: two workers each seeing the whole budget passed their
// whole batch, so a `LIMIT 3` returned six rows (#845, the OFFSET twin —
// same operator, same read-modify-write, opposite direction).
//
// WHICH rows an unordered OFFSET or LIMIT drops is unspecified (ADR-0013
// classes 1 and 3) and still is — the claim order varies with scheduling.
// HOW MANY never was.
func (l *Limit) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if l.bounded() && l.passed.Load() >= l.Max {
		return nil, nil
	}

	activeLen := int64(in.ActiveLen())

	// OFFSET: claim this batch's positions in the input stream, then decide
	// from the claim.
	if l.Offset > 0 {
		end := l.seen.Add(activeLen)
		start := end - activeLen
		if end <= l.Offset {
			// Every claimed position is inside the offset — skip the batch.
			return nil, nil
		}
		if start < l.Offset {
			// Partial skip: trim the front of this batch.
			skip := int(l.Offset - start)
			sel := make([]uint32, 0, activeLen-int64(skip))
			if in.Sel != nil {
				for i := skip; i < len(in.Sel); i++ {
					sel = append(sel, in.Sel[i])
				}
			} else {
				for i := skip; i < int(activeLen); i++ {
					sel = append(sel, uint32(i))
				}
			}
			in.Sel = sel
			activeLen = int64(len(sel))
			if activeLen == 0 {
				return nil, nil
			}
		}
	}

	if !l.bounded() {
		l.passed.Add(activeLen)
		return in, nil
	}

	// Max: same claim, over the post-OFFSET stream.
	end := l.passed.Add(activeLen)
	start := end - activeLen
	if start >= l.Max {
		// Every claimed position is past the limit.
		return nil, nil
	}
	if end <= l.Max {
		return in, nil
	}

	// Truncate to the part of the claim that fits.
	remaining := l.Max - start
	sel := make([]uint32, 0, remaining)
	if in.Sel != nil {
		for _, idx := range in.Sel {
			if int64(len(sel)) >= remaining {
				break
			}
			sel = append(sel, idx)
		}
	} else {
		for i := 0; i < int(remaining); i++ {
			sel = append(sel, uint32(i))
		}
	}

	in.Sel = sel
	return in, nil
}

func (l *Limit) Close() error { return nil }

// Clone returns the same Limit instance: OFFSET and Max are properties of the
// whole stream, so every morsel-parallel worker must count against one set of
// claim counters rather than its own.
func (l *Limit) Clone() UnaryOperator {
	return l
}

// Done returns true when the limit has been satisfied, enabling pipeline
// early termination (LIMIT pushdown). passed counts CLAIMED positions, so it
// can overshoot Max — but it only overshoots once the claims cover [0, Max),
// which is exactly when there is nothing left to pass.
func (l *Limit) Done() bool {
	return l.bounded() && l.passed.Load() >= l.Max
}

// TopN is a Sink that combines sort + limit efficiently by keeping only the
// top N rows in a heap. For small N this is much more efficient than full sort.
type TopN struct {
	Keys  []SortKey
	N     int
	inner *Sort
}

func NewTopN(keys []SortKey, n int) *TopN {
	s := NewSort(keys)
	s.Limit = n
	return &TopN{
		Keys:  keys,
		N:     n,
		inner: s,
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
			sel := make([]uint32, remaining)
			for j := range sel {
				sel[j] = uint32(j)
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
