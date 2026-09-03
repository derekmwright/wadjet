package sql

import (
	"strings"
	"testing"
)

// TestParseRejectsTrailingInput pins the end-of-statement guard (#337).
//
// Before it, every one of these parsed "successfully": the parser returned as
// soon as it had a shape it recognised and never asked what was left, so the
// remainder was discarded in silence and the query answered as though the
// user had not written it. `... WHERE n_regionkey = 1 GARBAGE TOKENS HERE`
// came back with a count.
//
// The error has to name the token where parsing stopped, because that token
// IS the defect — "syntax error" alone leaves a client hunting a statement
// that is mostly fine.
func TestParseRejectsTrailingInput(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		stopsAt string // the token the error must name
	}{
		{
			name:    "outright garbage after WHERE",
			sql:     "SELECT COUNT(*) FROM nation WHERE n_regionkey = 1 GARBAGE TOKENS HERE",
			stopsAt: "GARBAGE",
		},
		{
			name:    "garbage after a bare SELECT",
			sql:     "SELECT 1 nonsense here",
			stopsAt: "here",
		},
		{
			name:    "garbage after ORDER BY",
			sql:     "SELECT n_name FROM nation ORDER BY n_name WAT",
			stopsAt: "WAT",
		},
		{
			name:    "garbage after LIMIT",
			sql:     "SELECT n_name FROM nation LIMIT 5 EXTRA",
			stopsAt: "EXTRA",
		},
		{
			// PostgreSQL and DuckDB both reject this: a set operation's arms
			// may not carry their own LIMIT unless parenthesised. Wadjet used
			// to run the left arm only and drop the UNION entirely.
			name:    "LIMIT before a set operation",
			sql:     "SELECT r_regionkey FROM region LIMIT 5 UNION SELECT r_regionkey FROM region",
			stopsAt: "UNION",
		},
		{
			// MySQL's two-argument LIMIT. Postgres and DuckDB reject it; we
			// used to silently apply `LIMIT 1` and drop the ", 2".
			name:    "MySQL LIMIT offset,count",
			sql:     "SELECT n_name FROM nation LIMIT 1, 2",
			stopsAt: ",",
		},
		{
			// Unimplemented, and now rejected rather than dropped: silently
			// ignoring it left `FROM nation` alone, answering 25.
			name:    "NATURAL JOIN",
			sql:     "SELECT COUNT(*) FROM nation NATURAL JOIN region",
			stopsAt: "NATURAL",
		},
		// `JOIN ... USING` is PARSED now (#655), so it is no longer an
		// example of input the parser stops at. Its own coverage is
		// TestParseJoinUsingDesugarsToItsConditions, which asserts the
		// desugaring the clause produces, and
		// coordinator.TestJoinUsingAndTheLeadingDotLiteral, which asserts the
		// rows on four arms.
		{
			// A named-window clause we do not implement. Dropping it left the
			// OVER references dangling.
			name:    "WINDOW clause",
			sql:     "SELECT n_name FROM nation WINDOW w AS (ORDER BY n_name)",
			stopsAt: "w",
		},
		{
			// A row-locking clause an analytics engine cannot honour. It used
			// to be dropped, which answers as though the lock were taken.
			name:    "FOR UPDATE",
			sql:     "SELECT n_name FROM nation FOR UPDATE",
			stopsAt: "UPDATE",
		},
		{
			// The user asked for a specific collation; ignoring it returns a
			// different order than the one requested.
			name:    "ORDER BY ... COLLATE",
			sql:     `SELECT n_name FROM nation ORDER BY n_name COLLATE "C"`,
			stopsAt: "COLLATE",
		},
		{
			// A user-supplied LIKE escape character. Dropping it evaluates a
			// different predicate than the one written.
			name:    "LIKE ... ESCAPE",
			sql:     `SELECT n_name FROM nation WHERE n_name LIKE 'A%' ESCAPE '\'`,
			stopsAt: "ESCAPE",
		},
		{
			name:    "trailing input inside EXPLAIN",
			sql:     "EXPLAIN SELECT COUNT(*) FROM nation WHERE n_regionkey = 1 GARBAGE",
			stopsAt: "GARBAGE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.sql)
			if err == nil {
				t.Fatalf("Parse(%q) returned no error — the trailing input was silently discarded", tt.sql)
			}
			if !strings.Contains(err.Error(), tt.stopsAt) {
				t.Errorf("Parse(%q) error does not name where parsing stopped (%q):\n  %v",
					tt.sql, tt.stopsAt, err)
			}
			if !strings.Contains(err.Error(), "position") {
				t.Errorf("Parse(%q) error gives no position:\n  %v", tt.sql, err)
			}
		})
	}
}

// TestParseAcceptsCompleteStatements is the guard's other half: it must not
// start rejecting statements the parser does consume in full.
func TestParseAcceptsCompleteStatements(t *testing.T) {
	tests := []string{
		"SELECT 1",
		"SELECT 1;",
		"SELECT 1;;",
		"SELECT 1 -- trailing comment",
		"SELECT 1 /* trailing block comment */",
		"SELECT n_name FROM nation",
		"SELECT n_name FROM nation WHERE n_regionkey = 1",
		"SELECT n_name FROM nation ORDER BY n_name LIMIT 5",
		"SELECT n_name FROM nation ORDER BY n_name LIMIT 5 OFFSET 2",
		"SELECT n_name FROM nation LIMIT 5 OFFSET 2 ROWS",
		"SELECT n_name FROM nation OFFSET 2 ROWS FETCH NEXT 5 ROWS ONLY",
		"SELECT n_name FROM nation FETCH FIRST 5 ROWS ONLY",
		"SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region",
		"SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1",
		"SELECT n_regionkey, COUNT(*) FROM nation GROUP BY n_regionkey HAVING COUNT(*) > 1 ORDER BY 1",
		"WITH x AS (SELECT 1 AS a) SELECT * FROM x",
		"SELECT COUNT(*) FROM nation JOIN region ON n_regionkey = r_regionkey",
		"SELECT ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY n_name) FROM nation",
		"EXPLAIN SELECT 1",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if _, err := Parse(sql); err != nil {
				t.Errorf("Parse(%q) rejected a complete statement: %v", sql, err)
			}
		})
	}
}
