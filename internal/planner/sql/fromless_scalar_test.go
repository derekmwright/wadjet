package sql

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A FROM-LESS SCALAR SUBQUERY IS ITS SELECT EXPRESSION, AND EVERY CLAUSE THAT
// CAN CHANGE THE ANSWER KEEPS IT A SUBQUERY (#1044).
//
// The boundary is the claim here, so each excluded clause is attempted from
// the inside: a rewrite that fired on `(SELECT u.id LIMIT 0)` would answer the
// value where PostgreSQL answers NULL.
func TestAFromlessScalarSubqueryIsItsSelectExpression(t *testing.T) {
	scope := map[string]bool{"u": true}
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
		{"an aggregate", `SELECT MAX(u.x)`, ``},
		{"an aggregate under arithmetic", `SELECT 1 + MAX(u.x)`, ``},
		{"a window function", `SELECT SUM(u.x) OVER ()`, ``},
		{"a window function under arithmetic", `SELECT 1 + SUM(u.x) OVER ()`, ``},
		{"a set operation", `SELECT 1 UNION ALL SELECT 2`, ``},
		{"a CTE", `WITH c AS (SELECT 1 AS k) SELECT 2`, ``},
		{"text that does not parse", `SELECT FROM WHERE`, ``},
		// THE SCOPE TEST — the round-2 block (B1). A reference the enclosing
		// block does NOT supply keeps its subquery, whatever else is true of
		// it, because the block that does supply it is one whose text a
		// per-row re-run will REBUILD.
		{"a qualifier this block does not have", `SELECT z.x`, ``},
		{"one qualifier in scope and one not", `SELECT u.x + z.y`, ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := fromlessScalarExpr(tc.sql, scope, true)
			if tc.want == "" {
				if ok {
					t.Fatalf("fromlessScalarExpr(%q) rewrote to %s; this clause or scope "+
						"changes the answer and must keep the subquery", tc.sql, got)
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

// AN UNQUALIFIED NAME NEEDS THE BLOCK TO HAVE A FROM CLAUSE. SQL scopes
// innermost-first and a FROM-less subquery has no scope of its own, so the
// innermost block that can supply a bare name is the enclosing one — but only
// if that block reads a relation at all.
func TestABareNameNeedsTheEnclosingBlockToReadSomething(t *testing.T) {
	if _, ok := fromlessScalarExpr(`SELECT x`, nil, false); ok {
		t.Error("a bare name was rewritten into a block with no FROM clause")
	}
	if _, ok := fromlessScalarExpr(`SELECT 1`, nil, false); !ok {
		t.Error("a subquery with no column reference needs no scope and must rewrite")
	}
}

// THE REWRITE RUNS AFTER THE BLOCK IS PARSED, so a scalar subquery in an
// expression position is already the expression by the time any reader sees
// the tree — and the SELECT item's own rendering is the expression's,
// parenthesised only where an operator's String() would otherwise re-parse as
// something else.
func TestTheBlockRewritesAFromlessScalarSubqueryInPlace(t *testing.T) {
	for _, tc := range []struct{ name, sql, wantExpr string }{
		{"in the select list", `SELECT (SELECT u.x) AS v FROM t u`, `u.x`},
		{"nested two deep", `SELECT (SELECT (SELECT u.x)) AS v FROM t u`, `u.x`},
		{"inside an aggregate argument", `SELECT SUM((SELECT u.x)) AS v FROM t u`, `sum(u.x)`},
		{"precedence is kept", `SELECT (SELECT u.x + 1) * 2 AS v FROM t u`, `(u.x + 1) * 2`},
		{"a subquery WITH a FROM stays one",
			`SELECT (SELECT MAX(y.v) FROM y WHERE y.k = u.x) AS v FROM t u`,
			`(SELECT MAX(y.v) FROM y WHERE y.k = u.x)`},
		// B1: the enclosing block here is the SUBQUERY, whose FROM is `x` —
		// so `u.id` is not this block's and the node stays, which is what
		// keeps ADR-0021 §1c's refusal for the whole shape.
		{"inside a subquery that HAS a FROM",
			`SELECT (SELECT (SELECT u.id) FROM x WHERE x.id=1) AS v FROM t u`,
			`(SELECT (SELECT u.id) FROM x WHERE x.id=1)`},
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

// THE PUBLISHED NAME OF A REWRITTEN ITEM IS THE SUBQUERY'S OWN, ALIAS INCLUDED
// (round-2 review, B2).
//
// PostgreSQL names a SubLink's column after the subquery's target list, so
// `SELECT (SELECT 1 AS zzz) FROM u` publishes `zzz` and not `?column?`. A
// rewrite that returned the inner expression and dropped the alias published
// the expression's name instead — `id` for `(SELECT u.id AS zzz)`, `name` for
// `(SELECT u.name AS nm)` — which a BI client binds a result set to. Running
// the rewrite AFTER the parse is what keeps it: PublishedName is stamped on
// the item as written, before anything is rewritten.
func TestARewrittenItemKeepsPostgresOutputName(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{`SELECT (SELECT u.x) FROM t u`, "x"},
		{`SELECT (SELECT 1) FROM t u`, UnnamedOutputColumn},
		{`SELECT (SELECT u.x + 1) FROM t u`, UnnamedOutputColumn},
		// The aliased spellings, all four measured on PostgreSQL 17.11.
		{`SELECT (SELECT 1 AS zzz) FROM t u`, "zzz"},
		{`SELECT (SELECT u.x AS zzz) FROM t u`, "zzz"},
		{`SELECT (SELECT u.nm AS nm2) FROM t u`, "nm2"},
		{`SELECT (SELECT u.x + 1 AS zzz) FROM t u`, "zzz"},
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

// A STAR NEEDS A RELATION: `SELECT *` with no FROM clause is 42601 on
// PostgreSQL 17.11, in the SELECT list and inside a scalar subquery alike.
func TestAStarWithNoRelationIsRefused(t *testing.T) {
	for _, sql := range []string{
		`SELECT *`,
		`SELECT * UNION ALL SELECT 1`,
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err == nil {
				t.Fatalf("parsed %q; PostgreSQL 17.11 raises 42601", sql)
			}
			if got := sqlerr.StateOf(err); got != "42601" {
				t.Errorf("SQLSTATE %q, want 42601\n  %v", got, err)
			}
		})
	}
	// A star inside a scalar subquery is refused where that subquery is
	// parsed — the enclosing statement parses, and the subquery's own parse
	// raises when the runner reaches it. The wadjet-level cell for that is
	// the two-path gate's; here the statement is only required to survive its
	// own parse.
	if _, err := Parse(`SELECT id, (SELECT *) FROM t u`); err != nil {
		t.Errorf("a nested star must not fail the enclosing parse: %v", err)
	}
	// The control: a star over a relation is ordinary SQL.
	if _, err := Parse(`SELECT * FROM t`); err != nil {
		t.Errorf("SELECT * FROM t: %v", err)
	}
}
