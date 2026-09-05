package sql

import "strings"

// UnnamedOutputColumn is the name PostgreSQL gives an output column that has
// no natural one: an operator expression, a literal, a predicate, a scalar
// subquery over an unnamed item. It is spelled exactly as PostgreSQL spells
// it, question marks included, because it is what a client reads out of
// RowDescription and what a query has to quote to refer to the column.
const UnnamedOutputColumn = "?column?"

// OutputColumnName is the name PostgreSQL publishes an unaliased SELECT item
// under — its `FigureColname`, decided from the parsed AST and never from the
// expression's TEXT.
//
// The whole rule, measured on PostgreSQL 17 over 49 spellings (#732):
//
//	g, t.g, (g)                       → g            (the column)
//	(c_row).b                         → b            (the FIELD)
//	abs(g), count(*), sum(g) OVER ()  → abs, count, sum   (the function)
//	CASE …, COALESCE, NULLIF, GREATEST → case, coalesce, …
//	EXISTS (…)                        → exists
//	ARRAY[…]                          → array
//	EXTRACT(YEAR FROM d)              → extract
//	CAST(g AS int), g::int            → g            (the ARGUMENT)
//	CAST('2020-01-01' AS date)        → date         (the TYPE, only when the
//	                                                  argument has no name)
//	(SELECT g FROM … LIMIT 1)         → g            (the subquery's column)
//	g + 1, -g, 1, 'abc', g IS NULL,
//	g = 1, g BETWEEN 1 AND 2,
//	g IN (1,2), g || 'x', (SELECT 1)  → ?column?
//
// A CAST is the one PostgreSQL gets asked about most and the one most often
// guessed wrong: it is named after its ARGUMENT, and reaches for the type only
// when the argument itself is unnamed. The brief for arc E3 said "a CAST → the
// TYPE" and the measurement said otherwise.
//
// Several `?column?` in one SELECT list is legal and is what PostgreSQL does:
// `SELECT g + 1, g + 2` returns two columns of that name. Output slots have
// identity by POSITION (#556/#557), so a duplicate published name is not a
// collision.
//
// It returns "" for a STAR, which has no single name.
func OutputColumnName(col SelectColumn) string {
	if col.Alias != "" {
		return col.Alias
	}
	if col.Star {
		return ""
	}
	// The parser's answer, taken on the item AS WRITTEN, wins over a
	// re-derivation here: the planner rewrites SELECT items, and the name is a
	// property of the query rather than of the tree the planner ended up with.
	if col.PublishedName != "" {
		return col.PublishedName
	}
	if col.ASTExpr != nil {
		return exprOutputName(col.ASTExpr)
	}
	// A column built without an AST — the DML doors and a few synthetic
	// callers do that — keeps the name it always had.
	if col.ColumnRef != "" {
		return col.ColumnRef
	}
	return col.Expr
}

// exprOutputName is OutputColumnName's recursion over the AST.
func exprOutputName(n Node) string {
	switch e := n.(type) {
	case *ParenNode:
		// Parentheses are transparent, exactly as they are in PostgreSQL.
		return exprOutputName(e.Inner)
	case *ColRef:
		// A ROW field path is `{Table: container, Column: field}` and both
		// readings name the same string here: PostgreSQL labels `t.g` `g` and
		// `(c_row).b` `b`.
		return e.Column
	case *FuncCallNode:
		if e.OutputLabel != "" {
			return e.OutputLabel
		}
		return funcOutputLabel(e.Name)
	case *CastNode:
		// The ARGUMENT's name, and the TYPE only when the argument has none.
		if name := exprOutputName(e.Inner); name != "" && name != UnnamedOutputColumn {
			return name
		}
		return castTypeOutputName(e.TypeName)
	case *CaseNode:
		return "case"
	case *WindowFuncNode:
		if e.Func != nil {
			return strings.ToLower(e.Func.Name)
		}
		return UnnamedOutputColumn
	case *ExistsNode:
		return "exists"
	case *ArrayLitNode:
		return "array"
	case *SubqueryNode:
		// A scalar subquery takes the name of its own single output column,
		// which is that block's answer to this same question.
		if name := subqueryOutputName(e.SQL); name != "" {
			return name
		}
		return UnnamedOutputColumn
	}
	return UnnamedOutputColumn
}

// subqueryOutputName is the published name of a scalar subquery's ONE output
// column. It parses the block; a block that does not parse, or does not have
// exactly one non-star item, has no name to lend.
func subqueryOutputName(sql string) string {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return ""
	}
	parsed, err := Parse(sql)
	if err != nil || parsed == nil || parsed.SelectInfo == nil {
		return ""
	}
	if len(parsed.SelectInfo.Columns) != 1 {
		return ""
	}
	return OutputColumnName(parsed.SelectInfo.Columns[0])
}

// castTypeOutputName is the label PostgreSQL gives a cast whose ARGUMENT has
// no name: the type's own name, folded, with any parameterization dropped —
// `CAST('x' AS varchar(4))` is `varchar`, and `AS double precision` is
// `float8`, which is the type's real name rather than its SQL spelling.
func castTypeOutputName(typeName string) string {
	t := strings.ToLower(strings.TrimSpace(typeName))
	if i := strings.IndexByte(t, '('); i > 0 {
		t = strings.TrimSpace(t[:i])
	}
	// PostgreSQL labels the cast with the type's INTERNAL name, not its SQL
	// spelling: `CAST(1 AS real)` is `float4` and `CAST(1 AS boolean)` is
	// `bool`. Every row here was read off postgres:17-alpine.
	switch t {
	case "int", "integer":
		return "int4"
	case "bigint":
		return "int8"
	case "smallint":
		return "int2"
	case "real":
		return "float4"
	case "double", "double precision", "float":
		return "float8"
	case "boolean":
		return "bool"
	case "decimal":
		return "numeric"
	case "character varying":
		return "varchar"
	case "character":
		return "bpchar"
	case "timestamp without time zone":
		return "timestamp"
	case "timestamp with time zone":
		return "timestamptz"
	case "time without time zone":
		return "time"
	case "time with time zone":
		return "timetz"
	}
	return t
}

// funcOutputLabel is a call's published name: the function's own name, except
// where PostgreSQL RESOLVES the written call to a differently named function
// and labels the column after the one it resolved to.
//
// One entry, and it is measured rather than reasoned about: `SELECT trim(' a ')`
// and the SQL-standard `TRIM(' a ')` are both `btrim` on postgres:17-alpine,
// while `ltrim` and `rtrim` keep their own names. A second entry belongs here
// only with the same measurement beside it.
func funcOutputLabel(name string) string {
	n := strings.ToLower(name)
	if n == "trim" {
		return "btrim"
	}
	return n
}
