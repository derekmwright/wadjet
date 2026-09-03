package sql

import "testing"

// TestParseGroupingFunctionCall is the parser half of #804's gate: GROUPING
// was lexed only as the `GROUPING SETS` keyword, so `SELECT GROUPING(g)`
// failed with `unexpected token "GROUPING"` before any planning happened.
func TestParseGroupingFunctionCall(t *testing.T) {
	cases := []struct {
		name     string
		sql      string
		colIndex int
		wantArgs []string
		wantName string
	}{
		{
			name:     "one argument, unaliased",
			sql:      "SELECT GROUPING(g), COUNT(*) FROM t GROUP BY ROLLUP(g)",
			colIndex: 0,
			wantArgs: []string{"g"},
			// PostgreSQL names an unaliased GROUPING(...) column `grouping`.
			wantName: "grouping",
		},
		{
			name:     "one argument, aliased",
			sql:      "SELECT GROUPING(g) AS gg FROM t GROUP BY CUBE(g)",
			colIndex: 0,
			wantArgs: []string{"g"},
			wantName: "gg",
		},
		{
			name:     "two arguments keep their order",
			sql:      "SELECT GROUPING(g, h) AS gh FROM t GROUP BY CUBE(g, h)",
			colIndex: 0,
			wantArgs: []string{"g", "h"},
			wantName: "gh",
		},
		{
			name:     "the reversed spelling is a different call",
			sql:      "SELECT GROUPING(h, g) AS hg FROM t GROUP BY CUBE(g, h)",
			colIndex: 0,
			wantArgs: []string{"h", "g"},
			wantName: "hg",
		},
		{
			name:     "lower case",
			sql:      "SELECT grouping(g) AS gg FROM t GROUP BY ROLLUP(g)",
			colIndex: 0,
			wantArgs: []string{"g"},
			wantName: "gg",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.sql, err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("ExtractSelect(%q): %v", tc.sql, err)
			}
			col := info.Columns[tc.colIndex]
			if col.AggFunc != "grouping" {
				t.Fatalf("AggFunc = %q, want %q", col.AggFunc, "grouping")
			}
			if len(col.AggArgs) != len(tc.wantArgs) {
				t.Fatalf("got %d args, want %d", len(col.AggArgs), len(tc.wantArgs))
			}
			for i, want := range tc.wantArgs {
				if got := Unparen(col.AggArgs[i]).String(); got != want {
					t.Errorf("arg %d = %q, want %q — argument ORDER is the answer", i, got, want)
				}
			}
			if col.Alias != tc.wantName {
				t.Errorf("column name = %q, want %q", col.Alias, tc.wantName)
			}
		})
	}
}

// TestGroupingSetsClauseStillParses is the boundary fixture: the GROUP BY
// clause consumes `GROUPING SETS` before any expression is parsed, so
// teaching the expression parser about GROUPING( must not disturb it.
func TestGroupingSetsClauseStillParses(t *testing.T) {
	for _, sql := range []string{
		"SELECT g, h, COUNT(*) FROM t GROUP BY GROUPING SETS ((g, h), (g), ())",
		"SELECT g, COUNT(*) FROM t GROUP BY ROLLUP(g)",
		"SELECT g, h, COUNT(*) FROM t GROUP BY CUBE(g, h)",
		// And both at once: the clause spelling and the function spelling.
		"SELECT g, GROUPING(g) AS gg, COUNT(*) FROM t GROUP BY GROUPING SETS ((g), ())",
	} {
		parsed, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		info, err := ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("ExtractSelect(%q): %v", sql, err)
		}
		if len(info.GroupingSets) == 0 {
			t.Errorf("%q: parsed no grouping sets", sql)
		}
	}
}

// TestGroupingWithoutParenIsAName pins the other half of the keyword's new
// behaviour: GROUPING not followed by '(' is a column NAME, which is what
// PostgreSQL calls the unaliased output column, so `ORDER BY grouping`
// resolves the way it does there.
func TestGroupingWithoutParenIsAName(t *testing.T) {
	const sql = "SELECT GROUPING(g), COUNT(*) AS n FROM t GROUP BY ROLLUP(g) ORDER BY grouping, n"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect(%q): %v", sql, err)
	}
	if len(info.OrderBy) != 2 {
		t.Fatalf("got %d ORDER BY terms, want 2", len(info.OrderBy))
	}
	// The rendered spelling is delimited — `grouping` is a keyword spelling,
	// so QuoteIdent quotes it to keep the render re-parseable — but what the
	// term MEANS is a reference to the column named `grouping`.
	ref, ok := Unparen(info.OrderBy[0].Expr).(*ColRef)
	if !ok {
		t.Fatalf("ORDER BY term 0 is %T, want a *ColRef", info.OrderBy[0].Expr)
	}
	if ref.Column != "grouping" {
		t.Errorf("ORDER BY term 0 references %q, want %q", ref.Column, "grouping")
	}
}
