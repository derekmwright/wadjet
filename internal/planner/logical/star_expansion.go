package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ExpandStarProjections rewrites every `*` / `alias.*` select item into one
// projection per column of the star's source, in schema order.
//
// A star that shares its SELECT list with another item — `SELECT t.*, ctid` is
// how DataGrip opens a table — reaches the planner as a Projection carrying the
// literal expression "*" and no column reference at all. That is why it must be
// expanded HERE, before computeRequiredColumns: the pruner collects the columns
// each Project names, the star named none, so the scan below was narrowed to
// the SIBLING item's columns and every column the star contributed read back
// NULL. The single-process path returned those NULLs (#315); the distributed
// path only escaped because an unknown name trips the worker's all-or-nothing
// parquet projection guard, which falls back to full width.
//
// A star's source columns come from the scan's catalog-annotated schema
// (ScanColumns, populated by physical.AnnotateScanColumns), so this resolves
// only when a single base-table scan sits below the projection — the shape
// clients send. A star over a join or a derived table is left alone: its column
// set is not knowable here, and guessing it would silently change which columns
// a query returns.
func ExpandStarProjections(n *Node) {
	if n == nil {
		return
	}
	for _, child := range n.Children {
		ExpandStarProjections(child)
	}
	if n.Type != NodeProject || len(n.Children) == 0 || !HasStarProjection(n) {
		return
	}
	scan := loneScan(n.Children[0])
	if scan == nil || len(scan.ScanColumns) == 0 {
		return
	}

	expanded := make([]Projection, 0, len(n.Projections)+len(scan.ScanColumns))
	for _, proj := range n.Projections {
		if !isStarProjection(proj) {
			expanded = append(expanded, proj)
			continue
		}
		for _, col := range scan.ScanColumns {
			expanded = append(expanded, Projection{
				Column:  col,
				Alias:   col,
				Expr:    col,
				ASTExpr: &plansql.ColRef{Column: col},
			})
		}
	}
	n.Projections = expanded
}

// HasStarProjection reports whether node is a Project that still carries an
// unexpanded `*` or `alias.*` select item.
func HasStarProjection(n *Node) bool {
	if n == nil || n.Type != NodeProject {
		return false
	}
	for _, proj := range n.Projections {
		if isStarProjection(proj) {
			return true
		}
	}
	return false
}

// isStarProjection reports whether proj is `*` or a qualified `alias.*`.
func isStarProjection(proj Projection) bool {
	e := strings.TrimSpace(proj.Expr)
	if e == "" {
		e = strings.TrimSpace(proj.Column)
	}
	return e == "*" || strings.HasSuffix(e, ".*")
}

// loneScan returns the single scan node under n, or nil when n reads from
// none or from more than one.
func loneScan(n *Node) *Node {
	var found *Node
	count := 0
	var walk func(*Node)
	walk = func(cur *Node) {
		if cur == nil {
			return
		}
		if cur.Type == NodeScan {
			found = cur
			count++
		}
		for _, child := range cur.Children {
			walk(child)
		}
	}
	walk(n)
	if count != 1 {
		return nil
	}
	return found
}
