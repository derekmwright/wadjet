package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// MIN() and MAX() over ARRAY, ROW, MAP and VECTOR.
//
// The six MIN/MAX resolvers in kernel/agg.go fill an Accumulator slot —
// MinI64/MinF64/MinDec/MinStr — and a container fits none of them: its value
// is a whole nested structure, not a scalar. So the resolvers answered nil,
// the row updater was never called, HasMin/HasMax stayed false, and
// MIN(arr_col) finalized to NULL on every input. Silently (#426).
//
// It is a gap rather than a position. PostgreSQL orders arrays —
// min(anyarray)/max(anyarray) exist and use array_smaller/array_larger over
// the same lexicographic array_cmp — and #415 gave all four containers that
// total order here (kernel.CompareValuesAt), which is already what ORDER BY,
// a sort-merge join on a container key, and PARTITION BY use. Declining only
// in the aggregate makes one operator disagree with the rest of the engine
// about whether two containers are comparable. ROW/MAP/VECTOR have no
// PostgreSQL equivalent to follow, but they have a defined total order here
// too (ADR-0012 records MAP's and VECTOR's as WADJET-DEFINED), so answering
// is the consistent choice.
//
// The state is the shape MIN_BY/MAX_BY already needed: a RETAINED value
// rather than a scalar slot. It differs in keeping that value as a one-row
// VECTOR instead of a boxed `any`, because the comparison is
// kernel.CompareValuesAt, which reads vectors — and because the copy is then
// batch.AppendFrom, the engine's own nested-aware deep copy, rather than a
// GetValue box that would have to be re-parsed to compare.
type containerMinMaxState struct {
	// best holds exactly one row: the extreme value seen so far, deep-copied
	// out of the input batch. It must be a copy — the input batch's arenas
	// are rewritten by the next producer that reuses its backing (the scan
	// row-group pool, Project's BatchPool, the join-emit reuse of ADR-0016),
	// and this value outlives every one of them.
	best *batch.Vector
	// isMin selects the direction. One state serves both so the merge and
	// the finalize have a single shape.
	isMin bool
}

// isContainerMinMax reports whether aggregate fn over an input column of typ
// takes the boxed path rather than the Accumulator.
func isContainerMinMax(fn AggFunc, typ batch.TypeID) bool {
	return (fn == AggMin || fn == AggMax) && batch.IsContainerType(typ)
}

// observe folds one non-NULL row into the state. The caller has already
// checked the column's null bit — SQL MIN/MAX ignore NULL inputs, and a
// group that sees only NULLs answers NULL, which is what a nil best means.
func (s *containerMinMaxState) observe(src *batch.Vector, row int) {
	// Resolve a late-materialization view: CompareValuesAt indexes the
	// vector's own storage, and a view has none. In practice src.Base is
	// nil here today — HashAggregate does not implement ViewAware, so
	// Consume flattens every view before updateGroup ever calls observe
	// (FlattenForConsumer(b, nil), which flattens unconditionally for a
	// non-view-aware consumer). The caller's null check above
	// (v.Nulls.IsNullFast(row)) only covers the view's OWN override bits,
	// not Base's — it does NOT establish that following the index to Base
	// here is safe. A future view-aware caller that reaches this branch
	// with src.Base != nil must re-audit its own null check accordingly.
	if src.Base != nil {
		row = int(src.Indices[row])
		src = src.Base
	}
	if s.best == nil {
		s.best = copyContainerRow(src, row)
		return
	}
	c := kernel.CompareValuesAt(s.best, 0, src, row)
	if (s.isMin && c > 0) || (!s.isMin && c < 0) {
		s.best = copyContainerRow(src, row)
	}
}

// merge folds another state (a parallel clone's, or a partial's) into s.
// Keeping the better of two extremes is exactly the algebra that makes
// MIN/MAX decomposable in the first place, so this is also what the
// distributed partial→final split relies on.
func (s *containerMinMaxState) merge(o *containerMinMaxState) {
	if o == nil || o.best == nil {
		return
	}
	if s.best == nil {
		s.best = o.best
		return
	}
	c := kernel.CompareValuesAt(s.best, 0, o.best, 0)
	if (s.isMin && c > 0) || (!s.isMin && c < 0) {
		s.best = o.best
	}
}

// value boxes the retained row for the output column, or nil when the group
// never saw a non-NULL input.
func (s *containerMinMaxState) value() any {
	if s == nil || s.best == nil {
		return nil
	}
	return s.best.GetValue(0)
}

// memBytes reports best's actual retained heap footprint. HashAggregate's
// per-group flat charge (extraStateBytes += len(h.Aggs) * 80, sized for a
// scalar box) undercounts a container's retained value by an order of
// magnitude once its payload has any size to it — best is a whole copied
// nested structure, not a scalar slot. Callers delta-adjust extraStateBytes
// against this whenever best is set or replaced (observe, merge), the same
// pattern already used for distinctSets.
func (s *containerMinMaxState) memBytes() int64 {
	if s == nil || s.best == nil {
		return 0
	}
	return s.best.MemBytes()
}

// copyContainerRow materializes src[row] as a standalone one-row vector.
// NewVectorLike + AppendFrom is the engine's nested-aware pair: it rebuilds
// the child/children shape, the ROW field names and the VECTOR dimension,
// and copies element storage rather than aliasing it.
func copyContainerRow(src *batch.Vector, row int) *batch.Vector {
	dst := batch.NewVectorLike(src)
	dst.AppendFrom(src, row)
	return dst
}
