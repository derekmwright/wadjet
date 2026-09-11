package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// containerMinMaxState retains MIN/MAX for ARRAY, ROW, MAP and VECTOR (#426).
// All four use kernel.CompareValuesAt's total order, shared with ORDER BY,
// sort-merge join and PARTITION BY (#415; ADR-0012 for WADJET-DEFINED orders).
// Containers do not fit scalar Accumulator slots: retain the extreme as a one-row
// vector, deep-copied with batch.AppendFrom, so comparison needs no box re-parsing.
// The retained value must outlive reuse of its input batch's backing.
// See docs/internals/container-min-max-state.md for the design.
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
