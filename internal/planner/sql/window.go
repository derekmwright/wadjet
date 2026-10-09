// SPDX-License-Identifier: MIT

package sql

import "strings"

// WindowSpec describes a SELECT-list item that IS a window call.
//
// The call's TERMS — its arguments, PARTITION BY and ORDER BY — are not
// here: they are the item's own `*WindowFuncNode` (SelectColumn.ASTExpr),
// the tree the binder stamps and every rewrite edits. A second, TEXT copy of
// them was re-parsed by the planner with no binding on it, and had to be
// kept in step by every pass that edited the tree (ADR-0047 §Binding and
// window terms).
type WindowSpec struct {
	FuncName string
	Alias    string       // output column name
	Frame    *WindowFrame // optional frame specification (the node's own)
}

// WindowOutputName is the name a SELECT-list window column is published under.
//
// The alias where the query gave one, and otherwise the FUNCTION's name, which
// is what PostgreSQL 17 calls it:
//
//	SELECT SUM(a) OVER () FROM t              -- PostgreSQL: "sum"
//	SELECT ROW_NUMBER() OVER (ORDER BY id)…   -- PostgreSQL: "row_number"
//
// FIVE places name this column and they have to agree: the logical builder's
// projection (which decides what the operator emits), the embedded API's
// deriveColumns (the single-process result schema), the binder's blockOutputs
// (a derived table's namespace), and the two positional-ORDER-BY resolvers.
// They did not: an unaliased window was `sum(a) OVER (...)` in four of them and
// the empty string in the fifth, so the projection published nothing, the
// result schema asked for the text, and `ORDER BY 1` rewrote to a name with
// parentheses in it that no sort key could resolve.
//
// It lives here rather than in the logical package because the parser cannot
// import the planner, and the positional resolvers are in the parser.
func WindowOutputName(col SelectColumn) string {
	if col.Alias != "" {
		return col.Alias
	}
	if col.WindowSpec == nil {
		return strings.TrimSpace(col.Expr)
	}
	if col.WindowSpec.Alias != "" {
		return col.WindowSpec.Alias
	}
	if fn := strings.ToLower(strings.TrimSpace(col.WindowSpec.FuncName)); fn != "" {
		return fn
	}
	return strings.TrimSpace(col.Expr)
}
