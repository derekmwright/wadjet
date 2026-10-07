// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A NODE'S OUTPUT AS AN ORDERED IDENTITY LIST (ADR-0047 stage 2).
//
// The binder resolves a column reference to a relation instance and a position
// in that instance's column list (plansql.ColRef.Bound). OutputColumns answers
// the other half: which of those bindings each of a node's output positions
// IS, in the node's own order. A consumer holding a bound reference finds its
// column by identity — the position whose ID equals the binding — instead of
// by a name that two relations, or an expression's text, can spell alike.
//
// The list is structural and cheap: a scan, a derived table's root or a CTE
// reference answers for its instance (Rel, RelCols); a join is its arms in
// order; a Filter, Sort, Limit or Distinct is its input's; a projection or a
// GROUP BY key that IS a bound column reference carries that column's
// binding; everything a node computes has the zero ID. A nil list means the
// node's output is not enumerable here (an unexpanded star, a node kind the
// list does not describe), and its consumers keep the name rules.

// OutputColumn is one output position: the binding it is (zero Rel = none)
// and the name the node emits it under.
type OutputColumn struct {
	ID   plansql.Binding
	Name string
}

// HasID reports whether the position carries an input identity.
func (c OutputColumn) HasID() bool { return c.ID.Rel != 0 && !c.ID.Output }

// OutputColumns is n's output in order, or nil when it cannot be enumerated.
func (n *Node) OutputColumns() []OutputColumn {
	return OutputColumnsWith(n, (*Node).OutputColumns)
}

// OutputColumnsWith is OutputColumns with the children's lists asked of child,
// so a walk that memoizes per node (the physical declaration walk) builds every
// node's list once. A list child returns is never modified.
func OutputColumnsWith(n *Node, child func(*Node) []OutputColumn) []OutputColumn {
	if n == nil {
		return nil
	}
	var out []OutputColumn
	switch n.Type {
	case NodeScan:
		out = make([]OutputColumn, len(n.ScanColumns))
		for i, c := range n.ScanColumns {
			out[i] = OutputColumn{Name: c}
		}
	case NodeProject:
		if HasStarProjection(n) {
			return nil
		}
		out = make([]OutputColumn, len(n.Projections))
		for i, p := range n.Projections {
			name := p.Alias
			if name == "" {
				name = p.Column
			}
			if name == "" {
				name = strings.TrimSpace(p.Expr)
			}
			out[i] = OutputColumn{Name: name}
			if !p.IsAgg {
				if b := boundColumnOf(p.ASTExpr); b != nil {
					out[i].ID = *b
				}
			}
		}
	case NodeFilter, NodeSort, NodeLimit, NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		out = cloneOutputColumns(child(n.Children[0]))
		if out == nil {
			return nil
		}
	case NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		left := child(n.Children[0])
		if left == nil {
			return nil
		}
		if jt := strings.ToLower(n.JoinType); jt == "semi" || jt == "anti" {
			out = cloneOutputColumns(left)
			break
		}
		right := child(n.Children[1])
		if right == nil {
			return nil
		}
		out = make([]OutputColumn, 0, len(left)+len(right))
		out = append(append(out, left...), right...)
	case NodeAggregate:
		out = make([]OutputColumn, 0, len(n.GroupBy)+len(n.AggExprs))
		for i, g := range n.GroupBy {
			c := OutputColumn{Name: g}
			if i < len(n.GroupByExprs) {
				if b := boundColumnOf(n.GroupByExprs[i]); b != nil {
					c.ID = *b
				}
			}
			out = append(out, c)
		}
		for _, a := range n.AggExprs {
			out = append(out, OutputColumn{Name: a.OutputCol})
		}
	case NodeWindow:
		if len(n.Children) != 1 {
			return nil
		}
		in := child(n.Children[0])
		if in == nil {
			return nil
		}
		out = make([]OutputColumn, 0, len(in)+len(n.WindowExprs))
		out = append(out, in...)
		for _, w := range n.WindowExprs {
			out = append(out, OutputColumn{Name: w.OutputCol})
		}
	default:
		return nil
	}
	if n.Rel != 0 {
		// The node answers for an instance: each position is the instance's
		// column of that name, and a name the list holds twice — or not at
		// all — names no one column, so it keeps the zero ID.
		for i := range out {
			out[i].ID = plansql.Binding{}
			if ord := soleFoldIndex(n.RelCols, out[i].Name); ord >= 0 {
				out[i].ID = plansql.Binding{Rel: n.Rel, Ord: ord}
			}
		}
	}
	return out
}

// OutputIDs is OutputColumns' identity half, index-aligned with it.
func (n *Node) OutputIDs() []plansql.Binding {
	cols := n.OutputColumns()
	if cols == nil {
		return nil
	}
	ids := make([]plansql.Binding, len(cols))
	for i, c := range cols {
		ids[i] = c.ID
	}
	return ids
}

// boundColumnOf is the INPUT binding of an expression that IS a column
// reference (parentheses allowed), or nil.
func boundColumnOf(e plansql.Node) *plansql.Binding {
	for {
		switch x := e.(type) {
		case *plansql.ParenNode:
			e = x.Inner
		case *plansql.ColRef:
			if x.Bound == nil || x.Bound.Output {
				return nil
			}
			return x.Bound
		default:
			return nil
		}
	}
}

func cloneOutputColumns(in []OutputColumn) []OutputColumn {
	if in == nil {
		return nil
	}
	return append([]OutputColumn(nil), in...)
}

// soleFoldIndex is the one position of list whose name folds to name, or -1.
func soleFoldIndex(list []string, name string) int {
	at := -1
	for i, c := range list {
		if strings.EqualFold(c, name) {
			if at >= 0 {
				return -1
			}
			at = i
		}
	}
	return at
}
