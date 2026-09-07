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
// clients send. A star over a join — or over a derived table whose own FROM is
// a join — is left alone: its column set is not knowable here, and guessing it
// would silently change which columns a query returns.
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
	lone := loneScan(n.Children[0])

	expanded := make([]Projection, 0, len(n.Projections)+8)
	changed := false
	for _, proj := range n.Projections {
		if !isStarProjection(proj) {
			expanded = append(expanded, proj)
			continue
		}
		// A QUALIFIED star names its own relation, so it expands wherever
		// that relation's scan is — a join below does not make `o.*`
		// unknowable, only `*` (#955's rule, applied to the shape a lateral
		// produces). `SELECT o.*, s.n` was `column "o.*" does not exist in
		// the input schema` on the single-process arms and, on the DAG, a
		// column whose NAME and VALUE were both the string `*`.
		src := lone
		if qual := starQualifier(proj); qual != "" {
			src = scanNamed(n.Children[0], qual)
		}
		if src == nil || len(src.ScanColumns) == 0 {
			expanded = append(expanded, proj)
			continue
		}
		changed = true
		qual := starQualifier(proj)
		for _, col := range src.ScanColumns {
			// A QUALIFIED star expands to QUALIFIED references. The bare name
			// binds the FIRST column of that name in the join's output, which
			// for `SELECT o.*, li.amount FROM o JOIN li` is li's `id`: the
			// star's own relation was named and the expansion has to keep
			// naming it. The published name stays the column's own, which is
			// what PostgreSQL publishes.
			ref := &plansql.ColRef{Column: col}
			expr := col
			if qual != "" {
				ref.Table = qual
				expr = qual + "." + col
			}
			expanded = append(expanded, Projection{
				Column:  expr,
				Alias:   col,
				Expr:    expr,
				ASTExpr: ref,
			})
		}
	}
	if !changed {
		return
	}
	n.Projections = expanded
}

// starQualifier is the relation a QUALIFIED star names, or "" for a bare `*`.
func starQualifier(proj Projection) string {
	e := strings.TrimSpace(proj.Expr)
	if e == "" {
		e = strings.TrimSpace(proj.Column)
	}
	if !strings.HasSuffix(e, ".*") {
		return ""
	}
	return strings.TrimSpace(strings.TrimSuffix(e, ".*"))
}

// scanNamed is the scan under n that answers to alias, or nil. A star can only
// be expanded from a BASE-TABLE scan's catalog-annotated schema: a lateral's
// own output is a projection this pass cannot enumerate, and it carries the
// correlation slot the join is about to drop, so naming its columns here would
// publish one.
func scanNamed(n *Node, alias string) *Node {
	var found *Node
	var walk func(*Node)
	walk = func(cur *Node) {
		if cur == nil || found != nil {
			return
		}
		// NOT into a decorrelated LATERAL. `setSubtreeAlias` puts the
		// lateral's alias on its SCAN too, so `s.*` would expand to the inner
		// TABLE's columns — which the lateral does not publish (its
		// projection does) and which include the correlation slot the join is
		// about to drop. Measured: `SELECT s.*, o.id` came back as the inner
		// table's four columns, all NULL, on the single-process arms and as
		// `id,__key_0` on the DAG.
		if cur.LateralSubtree {
			return
		}
		if cur.Type == NodeScan {
			for _, name := range cur.ScopeNames() {
				if strings.EqualFold(name, alias) {
					found = cur
					return
				}
			}
		}
		for _, child := range cur.Children {
			walk(child)
		}
	}
	walk(n)
	return found
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
