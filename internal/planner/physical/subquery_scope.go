// This file holds subquery scope for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// collectOuterColumns recursively collects a column-name→table mapping from
// scan nodes in a logical plan subtree. Used to resolve unqualified column
// references in correlated subqueries.
func collectOuterColumns(node *logical.Node) map[string]string {
	colMap := make(map[string]string)
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan {
			// What the ENCLOSING query calls this scan, which inside a
			// derived table is the derived alias (#489).
			tableID := strings.ToLower(n.OuterTableID())
			for _, col := range n.ScanColumns {
				colMap[strings.ToLower(col)] = tableID
			}
		}
		// A CTE reference's OUTPUT columns answer to the CTE's scope, and
		// those names are the CTE's own — `did`, not the `g` the scan below
		// emits. Read off the subtree root for collectTableAliases' reason
		// (#535).
		if scope := cteScopeID(n); scope != "" {
			for _, col := range cteOutputNames(n) {
				colMap[strings.ToLower(col)] = scope
			}
		}
		for _, child := range n.Children {
			walk(child)
		}
	}
	walk(node)
	return colMap
}

// cteScopeID is the name an enclosing query calls this CTE reference by: the
// reference's own alias where it has one, else the CTE's name.
func cteScopeID(n *logical.Node) string {
	if n == nil {
		return ""
	}
	if n.CTERefAlias != "" {
		return strings.ToLower(n.CTERefAlias)
	}
	return strings.ToLower(n.CTEName)
}

// cteOutputNames lists the column names a CTE subtree PUBLISHES, for the
// shapes a CTE body ends in. It answers only where the answer is exact — a
// Project's aliases and an Aggregate's keys and outputs — and nothing at all
// otherwise, because a wrong name here would attribute an outer column to a
// scope that does not carry it.
func cteOutputNames(n *logical.Node) []string {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeProject:
		out := make([]string, 0, len(n.Projections))
		for _, p := range n.Projections {
			name := p.Alias
			if name == "" {
				name = p.Column
			}
			if name != "" && name != "*" {
				out = append(out, name)
			}
		}
		return out
	case logical.NodeAggregate:
		out := make([]string, 0, len(n.GroupBy)+len(n.AggExprs))
		out = append(out, n.GroupBy...)
		for _, a := range n.AggExprs {
			if a.OutputCol != "" {
				out = append(out, a.OutputCol)
			}
		}
		return out
	case logical.NodeScan:
		return n.ScanColumns
	}
	if len(n.Children) == 1 {
		return cteOutputNames(n.Children[0])
	}
	return nil
}

// subqueryInnerColumns returns a resolver that reports a relation's columns, so
// correlation analysis can bind an unqualified name inside a subquery to the
// subquery's own FROM before considering the outer query — the SQL scoping
// rule. Without it, a name that also exists in the outer scope is claimed by
// the outer scope unless the outer table's identifier happens to be spelled the
// same as an inner table, which turns an ordinary uncorrelated subquery into a
// per-row correlated one (issue #334).
//
// A CTE reference is a relation with a schema exactly as a base table is, so
// the WITH items in scope are part of the resolver and not an exception to it
// (#955). While they were, `WITH c AS (SELECT id, … FROM t) SELECT (SELECT
// MAX(v) FROM c WHERE id < 4000) FROM d` read that `id` as d's, substituted the
// outer row's value into the predicate — making it constant TRUE — and answered
// the unfiltered aggregate on every arm in silence. A derived table needs
// nothing here: it carries its own body, and the classifier reads it.
//
// A relation this cannot name resolves to nil, which leaves the name to the
// identifier-comparison fallback rather than silently declaring it inner.
func (p *Planner) subqueryInnerColumns() plansql.TableColumns {
	return plansql.CTEColumns(p.ctes, p.catalogColumns())
}

// catalogColumns is the base of subqueryInnerColumns' resolver: a relation's
// declared schema, from the catalog.
func (p *Planner) catalogColumns() plansql.TableColumns {
	if p.catalog == nil {
		return nil
	}
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	return func(table string) []string {
		t, err := p.catalog.GetTable(ctx, table)
		if err != nil || t == nil {
			return nil
		}
		cols := make([]string, len(t.Schema.Columns))
		for i, c := range t.Schema.Columns {
			cols[i] = c.Name
		}
		return cols
	}
}
