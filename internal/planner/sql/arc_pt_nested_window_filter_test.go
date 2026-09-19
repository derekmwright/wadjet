// SPDX-License-Identifier: MIT

package sql

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ARC PT / #1125 — a WINDOW in a SUBQUERY's row-filtering clause.
//
// PostgreSQL raises 42P20 for a window function in a WHERE or a JOIN
// condition at EVERY query level (§4.2.8: a window is evaluated after the
// rows are selected, so it cannot select them). A subquery keeps its body as
// raw SQL here, so the rule reached only the clause the parser held in hand
// and `WHERE EXISTS (SELECT 1 FROM z WHERE SUM(x) OVER () > 0)` was answered.
//
// The refusal has to be made at PLAN TIME, not by the subquery executor: the
// decorrelator parses the same body and declines on any error, so a late
// refusal left the EXISTS standing as a filter expression — the single-process
// arms then refused through their subquery runner while a DAG fragment, which
// has no runner, failed with a message about the runner instead of about the
// window. The five-arm half of this gate is the EXISTS/*/winarg cells in
// internal/coordinator/arc_l1_lateral_scope_two_path_test.go.
//
// The controls are the discrimination: a window is LEGAL everywhere in a
// subquery except its row-filtering clauses, and a body that merely contains
// the letters `over` is not a window at all.
func TestArcPTAWindowInANestedFilteringClauseIs42P20(t *testing.T) {
	for _, c := range []struct {
		name  string
		sql   string
		state string // "" = this rule does not fire
		pg    string
	}{
		{"exists_in_where",
			`SELECT o.id AS a FROM ord o WHERE EXISTS (SELECT 1 FROM item z WHERE z.oid = o.id AND SUM(o.id) OVER () > 0)`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"not_exists_in_where",
			`SELECT o.id AS a FROM ord o WHERE NOT EXISTS (SELECT 1 FROM item z WHERE SUM(z.n) OVER () > 0)`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"in_subquery_in_where",
			`SELECT o.id AS a FROM ord o WHERE o.id IN (SELECT z.oid FROM item z WHERE SUM(z.n) OVER () > 0)`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"scalar_subquery_in_where",
			`SELECT o.id AS a FROM ord o WHERE (SELECT MAX(z.n) FROM item z WHERE SUM(z.n) OVER () > 0) > 0`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"exists_under_a_conjunction",
			`SELECT o.id AS a FROM ord o WHERE o.id > 0 AND EXISTS (SELECT 1 FROM item z WHERE SUM(z.n) OVER () > 0)`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"exists_in_a_join_condition",
			`SELECT o.id AS a FROM ord o JOIN item j ON EXISTS (SELECT 1 FROM item z WHERE SUM(z.n) OVER () > 0)`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"the_subquerys_own_join_condition",
			`SELECT o.id AS a FROM ord o WHERE EXISTS (SELECT 1 FROM item z JOIN item w ON (SUM(z.n) OVER ()) > 0)`,
			"42P20", "42P20 window functions are not allowed in JOIN conditions"},
		{"two_levels_down",
			`SELECT o.id AS a FROM ord o WHERE EXISTS (SELECT 1 FROM item z WHERE EXISTS (SELECT 1 FROM item w WHERE SUM(w.n) OVER () > 0))`,
			"42P20", "42P20 window functions are not allowed in WHERE"},

		// The controls. A window in any other clause of the same subquery is
		// legal, and this rule must not fire on it.
		{"control_window_in_the_subquerys_select_list",
			`SELECT o.id AS a FROM ord o WHERE EXISTS (SELECT SUM(z.n) OVER () FROM item z WHERE z.n > 0)`,
			"", "answers"},
		{"control_window_in_the_subquerys_order_by",
			`SELECT o.id AS a FROM ord o WHERE EXISTS (SELECT z.n FROM item z ORDER BY SUM(z.n) OVER ())`,
			"", "answers"},
		{"control_the_word_over_in_a_string",
			`SELECT o.id AS a FROM ord o WHERE EXISTS (SELECT 1 FROM item z WHERE z.name = 'over')`,
			"", "answers"},
		{"control_no_subquery_at_all",
			`SELECT o.id AS a FROM ord o WHERE o.id > 0`,
			"", "answers"},
		{"control_window_in_the_outer_select_list",
			`SELECT SUM(o.id) OVER () AS a FROM ord o WHERE EXISTS (SELECT 1 FROM item z WHERE z.n > 0)`,
			"", "answers"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse(c.sql)
			st := sqlerr.StateOf(err)
			if c.state == "" {
				if st == "42P20" {
					t.Fatalf("%s: refused %q where PostgreSQL 17.11 %s\n  SQL: %s",
						c.name, err, c.pg, c.sql)
				}
				return
			}
			if err == nil {
				t.Fatalf("%s: parsed where PostgreSQL 17.11 raises %s\n  SQL: %s",
					c.name, c.pg, c.sql)
			}
			if st != c.state {
				t.Fatalf("%s: SQLSTATE %q, want %q (PostgreSQL 17.11: %s)\n  err: %v\n  SQL: %s",
					c.name, st, c.state, c.pg, err, c.sql)
			}
		})
	}
}
