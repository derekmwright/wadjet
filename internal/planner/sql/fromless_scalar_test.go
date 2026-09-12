package sql

import "testing"

// A FROM-LESS SCALAR SUBQUERY IS ITS SELECT EXPRESSION, AND EVERY CLAUSE THAT
// CAN CHANGE THE ANSWER KEEPS IT A SUBQUERY (#1044).
//
// The boundary is the claim here, so each excluded clause is attempted from
// the inside: a rewrite that fired on `(SELECT u.id LIMIT 0)` would answer the
// value where PostgreSQL answers NULL.
func TestAFromlessScalarSubqueryIsItsSelectExpression(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		// want is the rendered expression the subquery becomes, or "" when
		// the subquery must stay a subquery.
		want string
	}{
		{"a qualified outer column", `SELECT u.x`, `u.x`},
		{"a bare name", `SELECT x`, `x`},
		{"an arithmetic expression", `SELECT u.x + 1`, `(u.x + 1)`},
		{"a literal", `SELECT 1`, `1`},
		{"a function call", `SELECT upper(u.nm)`, `upper(u.nm)`},
		{"a cast", `SELECT CAST(u.v AS DECIMAL(10,2))`, `cast(u.v as decimal(10, 2))`},
		{"a CASE", `SELECT CASE WHEN u.x > 1 THEN 'big' ELSE 'small' END`,
			`case when u.x > 1 then 'big' else 'small' end`},
		{"lower case select", `select u.x`, `u.x`},
		// The excluded clauses, each measured on PostgreSQL 17.11 in this
		// arc's ROUND0 — see fromless_scalar.go for what each one answers.
		{"a FROM clause", `SELECT u.x FROM t`, ``},
		{"a WHERE clause", `SELECT u.x WHERE 1=0`, ``},
		{"a LIMIT", `SELECT u.x LIMIT 0`, ``},
		{"an OFFSET", `SELECT u.x OFFSET 1`, ``},
		{"an ORDER BY", `SELECT u.x ORDER BY 1`, ``},
		{"DISTINCT", `SELECT DISTINCT u.x`, ``},
		{"two columns", `SELECT u.x, u.y`, ``},
		{"a star", `SELECT *`, ``},
		{"an aggregate", `SELECT MAX(u.x)`, ``},
		{"an aggregate under arithmetic", `SELECT 1 + MAX(u.x)`, ``},
		{"a window function", `SELECT SUM(u.x) OVER ()`, ``},
		{"a window function under arithmetic", `SELECT 1 + SUM(u.x) OVER ()`, ``},
		{"a set operation", `SELECT 1 UNION ALL SELECT 2`, ``},
		{"a CTE", `WITH c AS (SELECT 1 AS k) SELECT 2`, ``},
		{"text that does not parse", `SELECT FROM WHERE`, ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := fromlessScalarExpr(tc.sql)
			if tc.want == "" {
				if ok {
					t.Fatalf("fromlessScalarExpr(%q) rewrote to %s; this clause changes the "+
						"answer and must keep the subquery", tc.sql, got)
				}
				return
			}
			if !ok {
				t.Fatalf("fromlessScalarExpr(%q) declined; want %s", tc.sql, tc.want)
			}
			if got.String() != tc.want {
				t.Errorf("fromlessScalarExpr(%q)\n  got  %s\n  want %s", tc.sql, got, tc.want)
			}
		})
	}
}

// The rewrite runs at the PARSER, so a scalar subquery in an expression
// position is already the expression by the time any reader sees the tree —
// and the SELECT item's own rendering is the expression's, parenthesised so
// that re-parsing it means the same thing.
func TestTheParserRewritesAFromlessScalarSubqueryInPlace(t *testing.T) {
	for _, tc := range []struct{ name, sql, wantExpr string }{
		{"in the select list", `SELECT (SELECT u.x) AS v FROM t u`, `u.x`},
		{"nested two deep", `SELECT (SELECT (SELECT u.x)) AS v FROM t u`, `u.x`},
		{"inside an aggregate argument", `SELECT SUM((SELECT u.x)) AS v FROM t u`, `sum(u.x)`},
		{"precedence is kept", `SELECT (SELECT u.x + 1) * 2 AS v FROM t u`, `(u.x + 1) * 2`},
		{"a subquery WITH a FROM stays one",
			`SELECT (SELECT MAX(y.v) FROM y WHERE y.k = u.x) AS v FROM t u`,
			`(SELECT MAX(y.v) FROM y WHERE y.k = u.x)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pq, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse %q: %v", tc.sql, err)
			}
			info, err := ExtractSelect(pq)
			if err != nil {
				t.Fatalf("extract %q: %v", tc.sql, err)
			}
			if len(info.Columns) != 1 {
				t.Fatalf("parsed %d columns, want 1", len(info.Columns))
			}
			if got := info.Columns[0].Expr; got != tc.wantExpr {
				t.Errorf("column text\n  got  %s\n  want %s", got, tc.wantExpr)
			}
			if info.Columns[0].ASTExpr == nil {
				t.Fatalf("column has no AST")
			}
			if got := info.Columns[0].ASTExpr.String(); got != tc.wantExpr {
				t.Errorf("column AST\n  got  %s\n  want %s", got, tc.wantExpr)
			}
		})
	}
}

// The PUBLISHED NAME of a rewritten item is the one PostgreSQL gives it, which
// is the name the subquery's own output column had — `x` for `(SELECT u.x)`,
// `?column?` for `(SELECT 1)`. Both were measured on PostgreSQL 17.11.
func TestARewrittenItemKeepsPostgresOutputName(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT (SELECT u.x) FROM t u`, "x"},
		{`SELECT (SELECT 1) FROM t u`, UnnamedOutputColumn},
		{`SELECT (SELECT u.x + 1) FROM t u`, UnnamedOutputColumn},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			pq, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := OutputColumnName(pq.SelectInfo.Columns[0]); got != tc.want {
				t.Errorf("OutputColumnName = %q, want %q (PostgreSQL 17.11)", got, tc.want)
			}
		})
	}
}
