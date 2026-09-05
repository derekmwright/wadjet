package sql

import "testing"

// TestOutputColumnNameMatchesPostgres drives PostgreSQL's whole `FigureColname`
// rule for an unaliased SELECT item (#732). Every `want` below was read off
// postgres:17-alpine with `\gdesc`, which is the same string a client reads out
// of RowDescription.
//
// The rule is measured rather than reasoned about because two of its cases are
// counter-intuitive and both had been guessed wrong in this repository's own
// notes: a CAST is named after its ARGUMENT (and reaches for the type only when
// the argument has no name), and the type it then reaches for is the type's
// INTERNAL name — `float4`, `int2`, `bool` — not the SQL spelling the query
// wrote.
func TestOutputColumnNameMatchesPostgres(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		// No natural name at all.
		{"SELECT g + 1 FROM t", "?column?"},
		{"SELECT -g FROM t", "?column?"},
		{"SELECT (g + 1) FROM t", "?column?"},
		{"SELECT 1 FROM t", "?column?"},
		{"SELECT 'abc' FROM t", "?column?"},
		{"SELECT g IS NULL FROM t", "?column?"},
		{"SELECT g = 1 FROM t", "?column?"},
		{"SELECT NOT (g = 1) FROM t", "?column?"},
		{"SELECT g BETWEEN 1 AND 2 FROM t", "?column?"},
		{"SELECT g IN (1,2) FROM t", "?column?"},
		{"SELECT s || 'x' FROM t", "?column?"},
		{"SELECT (SELECT 1) FROM t", "?column?"},
		// A column keeps its own name, qualified or parenthesised, and a ROW
		// field path takes the FIELD's.
		{"SELECT g FROM t", "g"},
		{"SELECT t.g FROM t", "g"},
		{"SELECT (g) FROM t", "g"},
		{"SELECT (c_row).b FROM t", "b"},
		{"SELECT c_row FROM t", "c_row"},
		// A call takes the function's name — aggregates and window functions
		// included, which is the half this rule adds.
		{"SELECT abs(g) FROM t", "abs"},
		{"SELECT upper(s) FROM t", "upper"},
		{"SELECT substring(s, 1, 1) FROM t", "substring"},
		{"SELECT count(*) FROM t", "count"},
		{"SELECT sum(g) FROM t", "sum"},
		{"SELECT AVG(g) FROM t", "avg"},
		{"SELECT MIN(g) FROM t", "min"},
		{"SELECT COUNT(DISTINCT g) FROM t", "count"},
		{"SELECT ROW_NUMBER() OVER () FROM t", "row_number"},
		{"SELECT SUM(g) OVER () FROM t", "sum"},
		{"SELECT now() FROM t", "now"},
		{"SELECT current_date FROM t", "current_date"},
		{"SELECT COALESCE(g, 0) FROM t", "coalesce"},
		{"SELECT NULLIF(g, 0) FROM t", "nullif"},
		{"SELECT GREATEST(g, 0) FROM t", "greatest"},
		{"SELECT EXTRACT(YEAR FROM d) FROM t", "extract"},
		// The other two constructs this parser REWRITES: PostgreSQL names the
		// column after what the query wrote, not after `strpos` / `element_at`.
		{"SELECT POSITION('a' IN s) FROM t", "position"},
		{"SELECT arr[1] FROM t", "arr"},
		{"SELECT t.arr[1] FROM t", "arr"},
		// CASE, EXISTS and an array literal are named after the construct.
		{"SELECT CASE WHEN g > 1 THEN 1 ELSE 0 END FROM t", "case"},
		{"SELECT EXISTS (SELECT 1) FROM t", "exists"},
		{"SELECT ARRAY[1,2] FROM t", "array"},
		// A CAST is named after its ARGUMENT…
		{"SELECT CAST(g AS bigint) FROM t", "g"},
		{"SELECT g::int FROM t", "g"},
		{"SELECT CAST(t.g AS text) FROM t", "g"},
		// …and reaches for the TYPE only when the argument has none, under the
		// type's internal name and without its parameterization.
		{"SELECT CAST('2020-01-01' AS date) FROM t", "date"},
		{"SELECT CAST('2020-01-01' AS timestamp) FROM t", "timestamp"},
		{"SELECT CAST(1 AS double precision) FROM t", "float8"},
		{"SELECT CAST(1 AS real) FROM t", "float4"},
		{"SELECT CAST(1 AS smallint) FROM t", "int2"},
		{"SELECT CAST(1 AS boolean) FROM t", "bool"},
		{"SELECT CAST(1 AS varchar(4)) FROM t", "varchar"},
		{"SELECT CAST(1 AS numeric(10,2)) FROM t", "numeric"},
		{"SELECT CAST(1 AS bigint) FROM t", "int8"},
		// A scalar subquery lends the name of its own one output column.
		{"SELECT (SELECT g FROM t LIMIT 1) FROM t", "g"},
		// An explicit alias always wins.
		{"SELECT g + 1 AS k FROM t", "k"},
		{"SELECT COUNT(*) AS n FROM t", "n"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			parsed, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.sql, err)
			}
			if got := OutputColumnName(parsed.SelectInfo.Columns[0]); got != tc.want {
				t.Errorf("OutputColumnName(%s) = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestSeveralUnnamedItemsEachTakeTheSameName: `?column?` twice in one list is
// what PostgreSQL answers, and it is legal because output slots have identity
// by POSITION (#556/#557).
func TestSeveralUnnamedItemsEachTakeTheSameName(t *testing.T) {
	parsed, err := Parse("SELECT g + 1, g + 2 FROM t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	for i, col := range parsed.SelectInfo.Columns {
		if got := OutputColumnName(col); got != UnnamedOutputColumn {
			t.Errorf("item %d: %q, want %q", i, got, UnnamedOutputColumn)
		}
	}
}
