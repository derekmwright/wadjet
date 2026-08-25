package sql

import "testing"

// TestUnaliasedFunctionCallIsLabelledByItsName pins PostgreSQL's naming rule
// for an unaliased SELECT item that is a function call: the column is labelled
// with the FUNCTION's name (`upper`, `concat`, `coalesce`), not with the
// expression's text and certainly not with a fragment of it.
//
// Before #513 the name came from the expression text with everything up to the
// first '.' stripped, on the assumption that a qualifier preceded it. For a
// call over a qualified column that assumption is false, so `UPPER(t0.c0)` was
// labelled `c0)` — parenthesis included — and that string was the key of the
// result-row map and the name in the wire RowDescription. Every value in the
// table below was read off postgres:17-alpine.
func TestUnaliasedFunctionCallIsLabelledByItsName(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		{"qualified argument", "SELECT UPPER(t0.c0) FROM t0", "upper"},
		{"bare argument", "SELECT UPPER(c0) FROM t0", "upper"},
		{"several arguments", "SELECT CONCAT(t0.c0, t0.c1) FROM t0", "concat"},
		{"literal argument", "SELECT CONCAT(t0.c0, 'z') FROM t0", "concat"},
		{"length", "SELECT LENGTH(t0.c0) FROM t0", "length"},
		{"abs", "SELECT ABS(t0.c1) FROM t0", "abs"},
		{"coalesce", "SELECT COALESCE(t0.c0, 'q') FROM t0", "coalesce"},
		{"substr", "SELECT SUBSTR(t0.c0, 1, 1) FROM t0", "substr"},
		{"nullif", "SELECT NULLIF(t0.c0, 'z') FROM t0", "nullif"},
		{"nested calls take the OUTER name", "SELECT UPPER(LOWER(t0.c0)) FROM t0", "upper"},
		// Parentheses are transparent in PostgreSQL's rule too.
		{"parenthesized", "SELECT (UPPER(t0.c0)) FROM t0", "upper"},
		// Niladic session/date functions were already labelled this way; the
		// general rule subsumes the special case rather than replacing it.
		{"current_user", "SELECT current_user", "current_user"},
		{"now", "SELECT NOW()", "now"},
		{"current_date", "SELECT CURRENT_DATE", "current_date"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.sql, err)
			}
			if got := parsed.SelectInfo.Columns[0].Alias; got != tc.want {
				t.Errorf("label for %s: got %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestExplicitAliasWinsOverTheFunctionLabel: the label is only a DEFAULT.
func TestExplicitAliasWinsOverTheFunctionLabel(t *testing.T) {
	for _, sql := range []string{
		"SELECT UPPER(t0.c0) AS shouted FROM t0",
		"SELECT UPPER(t0.c0) shouted FROM t0",
	} {
		parsed, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		if got := parsed.SelectInfo.Columns[0].Alias; got != "shouted" {
			t.Errorf("%s: alias %q, want %q", sql, got, "shouted")
		}
	}
}

// TestNonFunctionItemsKeepTheirOwnNaming: the rule is deliberately narrow.
// An AGGREGATE is excluded because its output name is load-bearing inside the
// planner (it IS the Aggregate node's OutputCol), and an operator expression
// or a cast is named by rules of its own — PostgreSQL calls the first
// `?column?` and the second after the cast's argument, and neither is what
// this change is about.
func TestNonFunctionItemsKeepTheirOwnNaming(t *testing.T) {
	for _, sql := range []string{
		"SELECT COUNT(t0.c0) FROM t0",
		"SELECT SUM(t0.c1) FROM t0",
		"SELECT t0.c1 + 1 FROM t0",
		"SELECT t0.c0 FROM t0",
		"SELECT CAST(t0.c1 AS BIGINT) FROM t0",
	} {
		parsed, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		if got := parsed.SelectInfo.Columns[0].Alias; got != "" {
			t.Errorf("%s: got the label %q, want none", sql, got)
		}
	}
}
