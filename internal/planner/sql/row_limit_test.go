package sql

import "testing"

// TestARowLimitIsAppendedOnlyWhereItIsAnAppend pins the two halves of
// AppendRowLimit: it bounds what EXISTS and a scalar subquery READ, and it
// declines every spelling where a trailing LIMIT would mean something other
// than "read fewer rows".
//
// The decline is the half worth a test. `SELECT ... LIMIT 5 LIMIT 1` is a
// syntax error, and on a UNION a trailing LIMIT binds to the whole set
// operation rather than to the arm the text ends with — so an append there
// turns a bounded READ into a changed QUERY, which is the one thing this
// function must never do.
func TestARowLimitIsAppendedOnlyWhereItIsAnAppend(t *testing.T) {
	cases := []struct {
		name, sql string
		n         int
		want      string
	}{
		{"plain select", "SELECT 1 FROM t", 1, "SELECT 1 FROM t LIMIT 1"},
		{"two rows for the cardinality rule", "SELECT n FROM t", 2, "SELECT n FROM t LIMIT 2"},
		{"a WHERE is still an append", "SELECT n FROM t WHERE id = 1", 1,
			"SELECT n FROM t WHERE id = 1 LIMIT 1"},
		{"GROUP BY and HAVING are still an append", "SELECT g FROM t GROUP BY g HAVING count(*) > 1", 1,
			"SELECT g FROM t GROUP BY g HAVING count(*) > 1 LIMIT 1"},
		{"ORDER BY is still an append", "SELECT n FROM t ORDER BY n", 2,
			"SELECT n FROM t ORDER BY n LIMIT 2"},
		// The declines.
		{"its own LIMIT", "SELECT n FROM t LIMIT 5", 1, "SELECT n FROM t LIMIT 5"},
		{"its own OFFSET", "SELECT n FROM t OFFSET 2", 1, "SELECT n FROM t OFFSET 2"},
		{"a UNION", "SELECT n FROM t UNION SELECT n FROM u", 1,
			"SELECT n FROM t UNION SELECT n FROM u"},
		{"n <= 0 asks for no bound", "SELECT 1 FROM t", 0, "SELECT 1 FROM t"},
		{"text that does not parse", "NOT A SELECT", 1, "NOT A SELECT"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := WithRowLimit(c.sql, c.n); got != c.want {
				t.Errorf("WithRowLimit(%q, %d) = %q, want %q", c.sql, c.n, got, c.want)
			}
		})
	}
}
