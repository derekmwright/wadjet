// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE DECLARATION WALK BY POSITION (ADR-0047 stage 2).
//
// The emitted walks (emittedColTypes, emittedColDecimal, inputColShapes,
// emittedColIntWidth) answer "what does this node emit under NAME". A name is
// not a column: a join emits one from each arm, a projection's expression is
// emitted under its text, and a qualified name read back from text loses which
// relation it meant. outputs answers the positional question instead — what
// each output POSITION of the node declares, in order, with the binding that
// position is — and a bound column reference (plansql.ColRef.Bound) is
// declared by the position whose binding it holds (ColDecls.boundPos).
//
// A node's OWN positions — a scan's columns, a projection's items, an
// aggregate's keys and aggregates, a window's slots — are read from its name
// index, where each name is that node's own and unique; a join's positions are
// its arms' positions, concatenated, so two arms' columns of one name stay two
// declarations; a Filter, Sort, Limit or Distinct passes its input's through.
// A node whose output cannot be enumerated has no list, and a reference into
// it keeps the name rules.

// declPos is one output position: the binding it is and its declaration.
// The integer width is read on demand from the node that owns the position
// (owner, key): the width walk is a walk of its own, and asking it for every
// node of every walk is what a reference that never needs a width must not
// pay for (TestArcCI2DeclarationPlanningBound).
type declPos struct {
	ID    plansql.Binding
	Col   parquet.Column
	ok    bool
	owner *logical.Node
	key   string
}

// outputColumns is the node's logical output list, once per node per walk.
func (w *declWalk) outputColumns(n *logical.Node) []logical.OutputColumn {
	if n == nil {
		return nil
	}
	if w.colsDone[n] {
		return w.cols[n]
	}
	out := logical.OutputColumnsWith(n, w.outputColumns)
	if w.cols == nil {
		w.cols = map[*logical.Node][]logical.OutputColumn{}
		w.colsDone = map[*logical.Node]bool{}
	}
	w.cols[n], w.colsDone[n] = out, true
	return out
}

// outputs is the node's output positions with their declarations, once per
// node per walk (the memo the name walks keep, #1034). A caller never modifies
// the slice it is handed.
func (w *declWalk) outputs(n *logical.Node) []declPos {
	if n == nil {
		return nil
	}
	if w.outsDone[n] {
		return w.outs[n]
	}
	if w.computed != nil {
		w.computed("outputs", n)
	}
	out := w.outputsUncached(n)
	if w.outs == nil {
		w.outs = map[*logical.Node][]declPos{}
		w.outsDone = map[*logical.Node]bool{}
	}
	w.outs[n], w.outsDone[n] = out, true
	return out
}

func (w *declWalk) outputsUncached(n *logical.Node) []declPos {
	cols := w.outputColumns(n)
	if cols == nil {
		return nil
	}
	var out []declPos
	switch n.Type {
	case logical.NodeFilter, logical.NodeSort, logical.NodeLimit, logical.NodeDistinct:
		out = append([]declPos(nil), w.outputs(n.Children[0])...)
	case logical.NodeJoin:
		left := w.outputs(n.Children[0])
		if jt := strings.ToLower(n.JoinType); jt == "semi" || jt == "anti" {
			out = append([]declPos(nil), left...)
			break
		}
		right := w.outputs(n.Children[1])
		if left != nil && right != nil {
			out = append(append(make([]declPos, 0, len(left)+len(right)), left...), right...)
		}
	case logical.NodeWindow:
		in := w.outputs(n.Children[0])
		if in == nil {
			return nil
		}
		own := w.ownPositions(n, cols[len(in):])
		out = append(append(make([]declPos, 0, len(cols)), in...), own...)
	default:
		out = w.ownPositions(n, cols)
	}
	if len(out) != len(cols) {
		return nil
	}
	for i := range out {
		out[i].ID = cols[i].ID
	}
	return out
}

// ownPositions declares the positions a node emits under its own names.
func (w *declWalk) ownPositions(n *logical.Node, cols []logical.OutputColumn) []declPos {
	// Read-only views of this node's memoized name index: the walk asks every
	// node for its positions, and copying four maps per node per walk was
	// the cost the depth bound measured (TestArcCI2DeclarationPlanningBound).
	shapes := memoView(w, &w.shapes, n, "shapes", w.inputColShapesUncached)
	d := ColDecls{
		Types:  memoView(w, &w.types, n, "types", w.emittedColTypesUncached),
		Fields: shapeFields(shapes),
		Elems:  shapeElems(shapes),
		Dec:    memoView(w, &w.dec, n, "decimal", w.emittedColDecimalUncached),
	}
	out := make([]declPos, len(cols))
	for i, c := range cols {
		key := strings.ToLower(strings.TrimSpace(c.Name))
		if col, ok := d.atKey(key); ok {
			out[i].Col, out[i].ok = col, true
		}
		out[i].owner, out[i].key = n, key
	}
	return out
}

// posWidth is the integer width of position p, from its owner's width walk.
func (w *declWalk) posWidth(p declPos) (intWidth, bool) {
	if w == nil || p.owner == nil {
		return intWidthUnknown, false
	}
	wd, ok := memoView(w, &w.widths, p.owner, "widths", w.emittedColIntWidthUncached)[p.key]
	return wd, ok
}

// boundPos is the input position a bound column reference names.
func (d ColDecls) boundPos(n *plansql.ColRef) (declPos, bool) {
	if n == nil || n.Bound == nil || n.Bound.Output || len(d.pos) == 0 {
		return declPos{}, false
	}
	for _, p := range d.pos {
		if p.ID.Rel == n.Bound.Rel && p.ID.Ord == n.Bound.Ord && !p.ID.Output {
			return p, true
		}
	}
	return declPos{}, false
}
