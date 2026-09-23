// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// PostgreSQL's PARENTHESISED ROW field path, at the layer that reads it.
//
// PostgreSQL requires `(c_row).b` and reads the unparenthesised `c_row.b` as
// `table.column`; wadjet reads both, and this asserts that reading BOTH does
// not mean holding two references. `(c_row).b` produces the same ColRef the
// bare spelling produces, so ADR-0022 rule 1's resolvers go on asking one
// question rather than acquiring a second spelling to disagree about.
//
// A container the reference QUALIFIES is read by row_field over the qualified
// reference's value — the qualifier is kept, never stripped (an earlier form
// stripped it and answered NULL on every arm). `a.b.c` stays a syntax error,
// which is ADR-0022's position.
func TestParenthesisedFieldPathIsTheSameReferenceAsTheBareOne(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		wantExpr  string // the rendered first select item
	}{
		{"bare", "SELECT c_row.b FROM nested", "c_row.b"},
		{"parenthesised", "SELECT (c_row).b FROM nested", "c_row.b"},
		{"parenthesised with an alias", "SELECT (c_row).b AS fb FROM nested", "c_row.b"},
		{"in arithmetic", "SELECT (c_row).b + 1 FROM nested", "c_row.b + 1"},
		{"under a cast", "SELECT (c_row).b::int FROM nested", "cast(c_row.b as int)"},
		{"as an aggregate argument", "SELECT count((c_row).b) FROM nested", "count(c_row.b)"},
		// Redundant parentheses are redundant. PostgreSQL answers this; the
		// first cut of the arm refused it as "not a composite type", which was
		// false about a container (round-3 review P2).
		{"redundant parentheses", "SELECT ((c_row)).b FROM nested", "c_row.b"},
		{"three parentheses", "SELECT (((c_row))).b FROM nested", "c_row.b"},
		// A TWO-PART container — relation-qualified, or itself a path — reads
		// the field from the reference's value (ADR-0022's 2026-09-23
		// amendment): the reference keeps its qualifier, so nothing needs a
		// three-part identity. pgJDBC's getPrimaryKeys spells `(result.KEYS).x`.
		{"a relation-qualified container", "SELECT (x.c_row).b FROM nested x", `(x.c_row)."b"`},
		{"a nested path", "SELECT ((c_rownest).s).x FROM nested", `(c_rownest.s)."x"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := q.SelectInfo.Columns[0].Expr
			if got != tc.wantExpr {
				t.Errorf("the first select item is %q, want %q — the parenthesised spelling "+
					"must become the SAME reference the bare one does\n  SQL: %s",
					got, tc.wantExpr, tc.sql)
			}
		})
	}

	// The clauses, because ADR-0022 rule 1's resolvers are per-clause and a
	// spelling that reached only the select list would pass the cases above.
	q, err := Parse("SELECT (c_row).b AS fb FROM nested WHERE (c_row).b > 1 " +
		"GROUP BY (c_row).b ORDER BY (c_row).b")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if w := q.SelectInfo.Where; w != "c_row.b > 1" {
		t.Errorf("WHERE is %q, want %q", w, "c_row.b > 1")
	}
	if g := q.SelectInfo.GroupBy; len(g) != 1 || g[0] != "c_row.b" {
		t.Errorf("GROUP BY is %v, want [c_row.b]", g)
	}
	if o := q.SelectInfo.OrderBy; len(o) != 1 || o[0].Column != "c_row.b" {
		t.Errorf("ORDER BY is %v, want one term c_row.b", o)
	}
}

func TestParenthesisedFieldPathRefusesWhatItCannotResolve(t *testing.T) {
	for _, tc := range []struct {
		name, sql, wantMsg, wantState string
	}{
		{
			// PostgreSQL 17, measured: `column notation .b applied to type
			// integer, which is not a composite type`, SQLSTATE 42809. The
			// parser knows the EXPRESSION but not its type, so it names the
			// expression where PostgreSQL names the type.
			name:      "field notation on something that is not a column",
			sql:       "SELECT (1+2).b",
			wantMsg:   "column notation .b applied to 1 + 2, which is not a composite type",
			wantState: "42809",
		},
		{
			// ADR-0022's position, unchanged: only a PARENTHESISED expression
			// takes a dot, so the unparenthesised three-part spelling is still
			// a syntax error rather than a guess.
			name:      "the unparenthesised three-part spelling",
			sql:       "SELECT n.c_row.b FROM nested n",
			wantMsg:   `syntax error at or near "."`,
			wantState: "42601",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.sql)
			if err == nil {
				t.Fatalf("parsed without error; want %q", tc.wantMsg)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("refusal is %q, want it to contain %q", err.Error(), tc.wantMsg)
			}
			if got := sqlerr.StateOf(err); got != tc.wantState {
				t.Errorf("SQLSTATE is %q, want %q", got, tc.wantState)
			}
		})
	}
}
