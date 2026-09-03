package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestGroupingFunctionMatchesPostgresOnEveryArm is #804's gate.
//
// `SELECT GROUPING(a), ... GROUP BY ROLLUP(a)` did not parse at all —
// GROUPING was lexed only as the `GROUPING SETS` keyword, so the one function
// that tells a super-aggregate row from a data NULL was unreachable while
// ROLLUP / CUBE / GROUPING SETS all worked.
//
// Every `want` below is transcribed from a live PostgreSQL 17 run over the
// same rows (docs/testing: the probe is reproduced in the commit body). Both
// coordinator arms run every case: the stage DAG REFUSES a grouping-set
// aggregate and routes it to the coordinator-local pipeline (#778), and the
// suite asserts that routing happened rather than trusting the rows — a
// right-to-routed move is invisible to a row assertion alone.
func TestGroupingFunctionMatchesPostgresOnEveryArm(t *testing.T) {
	ctx := context.Background()
	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	arms := []struct {
		name  string
		coord *Coordinator
		run   func(sql string) (*oracle.Result, error)
	}{
		{"single", nil, func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"dag", coord, func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
	}

	// collslot: 240 rows, g = i%3 (80 each), h = i%4 (60 each). No NULLs in
	// either key, so a NULL in an output row can ONLY be a grouping-set NULL
	// — which is what makes the bitmask checkable against the key columns.
	cases := []struct {
		name string
		sql  string
		cols []string
		want string
	}{
		{
			name: "rollup/one-argument",
			sql: "SELECT g, GROUPING(g) AS gg, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP(g) ORDER BY gg, g",
			cols: []string{"g", "gg", "n"},
			want: "4 rows: 0|0|80;1|0|80;2|0|80;|1|240;",
		},
		{
			name: "rollup/unaliased-is-named-grouping",
			sql: "SELECT GROUPING(g), COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP(g) ORDER BY grouping, n",
			cols: []string{"grouping", "n"},
			want: "4 rows: 0|80;0|80;0|80;1|240;",
		},
		{
			name: "cube/two-arguments-and-bit-order",
			sql: "SELECT g, h, GROUPING(g) AS a, GROUPING(h) AS b, GROUPING(g,h) AS ab, " +
				"GROUPING(h,g) AS ba, COUNT(*) AS n FROM collslot " +
				"GROUP BY CUBE(g,h) ORDER BY ab, g, h",
			cols: []string{"g", "h", "a", "b", "ab", "ba", "n"},
			want: "20 rows: " +
				"0|0|0|0|0|0|20;0|1|0|0|0|0|20;0|2|0|0|0|0|20;0|3|0|0|0|0|20;" +
				"1|0|0|0|0|0|20;1|1|0|0|0|0|20;1|2|0|0|0|0|20;1|3|0|0|0|0|20;" +
				"2|0|0|0|0|0|20;2|1|0|0|0|0|20;2|2|0|0|0|0|20;2|3|0|0|0|0|20;" +
				"0||0|1|1|2|80;1||0|1|1|2|80;2||0|1|1|2|80;" +
				"|0|1|0|2|1|60;|1|1|0|2|1|60;|2|1|0|2|1|60;|3|1|0|2|1|60;" +
				"||1|1|3|3|240;",
		},
		{
			name: "grouping-sets/explicit",
			sql: "SELECT g, h, GROUPING(g,h) AS gh, COUNT(*) AS n FROM collslot " +
				"GROUP BY GROUPING SETS ((g,h),(g),(h),()) ORDER BY gh, g, h",
			cols: []string{"g", "h", "gh", "n"},
			want: "20 rows: " +
				"0|0|0|20;0|1|0|20;0|2|0|20;0|3|0|20;" +
				"1|0|0|20;1|1|0|20;1|2|0|20;1|3|0|20;" +
				"2|0|0|20;2|1|0|20;2|2|0|20;2|3|0|20;" +
				"0||1|80;1||1|80;2||1|80;" +
				"|0|2|60;|1|2|60;|2|2|60;|3|2|60;" +
				"||3|240;",
		},
		{
			// A call NESTED in a larger expression must substitute its
			// bitmask into that expression, not replace it: projecting the
			// slot for the whole column dropped the `+ 1` and answered 0.
			name: "nested/arithmetic",
			sql: "SELECT g, GROUPING(g) + 1 AS gg1, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP(g) ORDER BY gg1, g",
			cols: []string{"g", "gg1", "n"},
			want: "4 rows: 0|1|80;1|1|80;2|1|80;|2|240;",
		},
		{
			// Nested inside a CASE and beside a REAL aggregate in the same
			// column: the substitution has to coexist with the machinery that
			// hoists nested aggregates.
			name: "nested/case-and-a-real-aggregate",
			sql: "SELECT CASE WHEN GROUPING(g) = 1 THEN 'total' ELSE 'row' END AS label, " +
				"SUM(h) + GROUPING(g) AS s, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP(g) ORDER BY label, s",
			cols: []string{"label", "s", "n"},
			want: "4 rows: row|120|80;row|120|80;row|120|80;total|361|240;",
		},
		{
			// HAVING: the aggregate loop used to mint an AggExpr for a
			// function no kernel implements, so the column came back empty
			// and the predicate matched NO rows — silently.
			name: "having/only-the-detail-rows",
			sql: "SELECT g, COUNT(*) AS n FROM collslot GROUP BY ROLLUP(g) " +
				"HAVING GROUPING(g) = 0 ORDER BY g",
			cols: []string{"g", "n"},
			want: "3 rows: 0|80;1|80;2|80;",
		},
		{
			name: "having/only-the-total-row",
			sql: "SELECT COUNT(*) AS n FROM collslot GROUP BY ROLLUP(g) " +
				"HAVING GROUPING(g) = 1",
			cols: []string{"n"},
			want: "1 rows: 240;",
		},
		{
			// A COMPUTED grouping key: the argument is the derived term, and
			// it has to resolve through the same hidden group-key slot the
			// key itself uses (ADR-0026).
			name: "computed-key",
			sql: "SELECT g+1 AS k, GROUPING(g+1) AS gg, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP(g+1) ORDER BY gg, k",
			cols: []string{"k", "gg", "n"},
			want: "4 rows: 1|0|80;2|0|80;3|0|80;|1|240;",
		},
		{
			// The same call twice: one slot, two projections.
			name: "repeated-identical-call",
			sql: "SELECT GROUPING(g) AS a, GROUPING(g) AS b, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP(g) ORDER BY a, n",
			cols: []string{"a", "b", "n"},
			want: "4 rows: 0|0|80;0|0|80;0|0|80;1|1|240;",
		},
		{
			name: "plain-group-by/always-zero",
			sql: "SELECT g, GROUPING(g) AS gg, COUNT(*) AS n FROM collslot " +
				"GROUP BY g ORDER BY g",
			cols: []string{"g", "gg", "n"},
			want: "3 rows: 0|0|80;1|0|80;2|0|80;",
		},
	}

	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var before int64
					if arm.coord != nil {
						before = arm.coord.GroupingSetsLocalRoutes()
					}
					res, err := arm.run(tc.sql)
					if err != nil {
						t.Fatalf("%s: %v", tc.sql, err)
					}
					if got := dajDigest(res, tc.cols); got != tc.want {
						t.Errorf("%s\n got: %s\nwant: %s", tc.sql, got, tc.want)
					}
					// BOUNDARY, asserted rather than assumed: the DAG has no
					// stage that can compute a bitmask, so EVERY query with a
					// GROUPING call is refused and routed to the local
					// pipeline — the plain-GROUP-BY one included, because it
					// takes the same hidden slot rather than a constant fold
					// of its own. Rows alone cannot tell "the DAG executed
					// this" from "the DAG refused it", so the routing flag is
					// asserted beside them.
					if arm.coord != nil {
						moved := arm.coord.GroupingSetsLocalRoutes() - before
						if moved == 0 {
							t.Error("expected the DAG to refuse this query and route it local; the counter did not move")
						}
					}
				})
			}
		})
	}
}

// TestGroupingInOrderByIsRefused is the DEFERRAL, pinned in the corpus rather
// than left to be discovered. PostgreSQL accepts `ORDER BY GROUPING(g)` over a
// grouping-set query; wadjet refuses it, because ORDER BY resolution rejects
// any aggregate expression that is not itself a select item and GROUPING is
// carried through that machinery as one. The disposition is a LOUD refusal,
// not a wrong order, and selecting the call and ordering by its alias works —
// the suite above does exactly that. This test fails when the refusal is
// lifted, which is the signal to delete it.
func TestGroupingInOrderByIsRefused(t *testing.T) {
	ctx := context.Background()
	single := tmdStandalone(t, ctx)

	const sql = "SELECT g, COUNT(*) AS n FROM collslot GROUP BY ROLLUP(g) ORDER BY GROUPING(g), g"
	if _, err := tmdRunSingle(ctx, single, sql); err == nil {
		t.Fatal("ORDER BY GROUPING(...) now works — lift the deferral and delete this pin")
	}
}

// TestAggregatePlacementMatchesPostgres is the BOUNDARY gate: every position
// where an aggregate or grouping operation is illegal, on both arms, with the
// SQLSTATE and the wording PostgreSQL 17.11 uses.
//
// #804 widened the expression grammar to accept GROUPING(...) and did not
// bound WHERE it was legal, so two positions the parser newly reached answered
// a NUMBER where the parent commit raised a parse error: `WHERE GROUPING(g)=0`
// returned 0 rows and `SUM(GROUPING(g))` returned a column of NULLs. Probing
// the same positions with a PLAIN aggregate found the same silence predating
// this branch — `WHERE SUM(h) > 1` also returned 0 rows — so the rule that
// covers them is PostgreSQL's one rule, not a GROUPING special case.
func TestAggregatePlacementMatchesPostgres(t *testing.T) {
	ctx := context.Background()
	single := tmdStandalone(t, ctx)
	coord := tmdCluster(t, ctx)

	cases := []struct {
		name string
		sql  string
		// substr is a distinctive fragment of PostgreSQL's own message.
		substr string
	}{
		// GROUPING where no grouping-set membership exists to read.
		{"grouping/where", "SELECT g FROM collslot WHERE GROUPING(g) = 0 GROUP BY ROLLUP(g)",
			"grouping operations are not allowed in WHERE"},
		{"grouping/join-on",
			"SELECT a.g, COUNT(*) AS n FROM collslot a JOIN collslot b ON GROUPING(a.g) = 0 GROUP BY ROLLUP(a.g)",
			"grouping operations are not allowed in JOIN conditions"},
		// GROUPING inside an aggregate's arguments.
		{"grouping/inside-sum", "SELECT SUM(GROUPING(g)) AS s FROM collslot GROUP BY ROLLUP(g)",
			"aggregate function calls cannot be nested"},
		{"grouping/inside-count", "SELECT COUNT(GROUPING(g)) AS s FROM collslot GROUP BY ROLLUP(g)",
			"aggregate function calls cannot be nested"},
		{"grouping/inside-max-with-arithmetic",
			"SELECT MAX(GROUPING(g) + 1) AS s FROM collslot GROUP BY ROLLUP(g)",
			"aggregate function calls cannot be nested"},
		{"grouping/inside-sum-in-having",
			"SELECT g, COUNT(*) AS n FROM collslot GROUP BY ROLLUP(g) HAVING SUM(GROUPING(g)) > 0",
			"aggregate function calls cannot be nested"},
		// The SAME rule for a plain aggregate — the silence that predates
		// this branch, and the reason the fix is one rule.
		{"aggregate/where-sum", "SELECT g FROM collslot WHERE SUM(h) > 1 GROUP BY g",
			"aggregate functions are not allowed in WHERE"},
		{"aggregate/where-count-star", "SELECT g FROM collslot WHERE COUNT(*) > 1 GROUP BY g",
			"aggregate functions are not allowed in WHERE"},
		{"aggregate/join-on",
			"SELECT a.g FROM collslot a JOIN collslot b ON SUM(a.h) = 0 GROUP BY a.g",
			"aggregate functions are not allowed in JOIN conditions"},
		{"aggregate/nested", "SELECT SUM(COUNT(*)) AS s FROM collslot GROUP BY g",
			"aggregate function calls cannot be nested"},
		// The argument rule, which predates this round.
		{"grouping/argument-not-grouped", "SELECT GROUPING(h), COUNT(*) FROM collslot GROUP BY ROLLUP(g)",
			"must be grouping expressions"},
		{"grouping/argument-plain-group-by", "SELECT GROUPING(h), COUNT(*) FROM collslot GROUP BY g",
			"must be grouping expressions"},
		{"grouping/no-group-by", "SELECT GROUPING(g) FROM collslot",
			"must be grouping expressions"},
	}

	arms := []struct {
		name string
		run  func(sql string) (*oracle.Result, error)
	}{
		{"single", func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"dag", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
	}

	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					res, err := arm.run(tc.sql)
					if err == nil {
						t.Fatalf("%s: expected SQLSTATE 42803, got %d rows", tc.sql, len(res.Rows))
					}
					if got := sqlerr.StateOf(err); got != "42803" {
						t.Errorf("%s: SQLSTATE = %q, want 42803 (%v)", tc.sql, got, err)
					}
					if !strings.Contains(err.Error(), tc.substr) {
						t.Errorf("%s: message %q does not contain %q", tc.sql, err, tc.substr)
					}
				})
			}
		})
	}
}

// TestAggregatePlacementAcceptsWhatPostgresAccepts is the OTHER half: the
// refusal must not reach a level it does not own. Every shape here is legal in
// PostgreSQL 17.11 and must stay legal — an aggregate in a SUBQUERY inside
// WHERE most of all, since that is a different query level and is ordinary SQL.
func TestAggregatePlacementAcceptsWhatPostgresAccepts(t *testing.T) {
	ctx := context.Background()
	single := tmdStandalone(t, ctx)

	for _, sql := range []string{
		"SELECT g FROM collslot WHERE h > (SELECT AVG(h) FROM collslot) GROUP BY g ORDER BY g",
		"SELECT g FROM collslot t WHERE EXISTS (SELECT 1 FROM collslot u WHERE u.g = t.g GROUP BY u.g HAVING COUNT(*) > 1) GROUP BY g ORDER BY g",
		"SELECT g FROM collslot WHERE g IN (SELECT g FROM collslot GROUP BY g HAVING COUNT(*) > 0) GROUP BY g ORDER BY g",
		"SELECT g, COUNT(*) AS n FROM collslot WHERE h > 0 GROUP BY g HAVING COUNT(*) > 1 ORDER BY g",
		"SELECT g, SUM(h) OVER (PARTITION BY g) AS w FROM collslot ORDER BY g LIMIT 2",
		"SELECT a.g, COUNT(*) AS n FROM collslot a JOIN collslot b ON a.g = b.g GROUP BY a.g ORDER BY a.g LIMIT 2",
		"SELECT g, MAX(h) - MIN(h) AS d FROM collslot GROUP BY g ORDER BY g",
		"SELECT SUM(h) AS s FROM collslot",
	} {
		if _, err := tmdRunSingle(ctx, single, sql); err != nil {
			t.Errorf("PostgreSQL accepts this and wadjet must too: %s\n  got [%s] %v",
				sql, sqlerr.StateOf(err), err)
		}
	}
}
