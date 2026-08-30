package sql

import "strings"

// WindowSpec describes a window function specification.
type WindowSpec struct {
	FuncName    string
	Args        string // raw arg string (e.g., "amount", "*", "")
	PartitionBy []string
	OrderBy     []WindowOrderItem
	Alias       string       // output column name
	Frame       *WindowFrame // optional frame specification
}

// WindowOrderItem describes a column + direction in a window ORDER BY.
type WindowOrderItem struct {
	Column     string
	Desc       bool
	NullsFirst *bool
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
