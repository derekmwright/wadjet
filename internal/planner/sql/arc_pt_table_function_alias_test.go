// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ARC PT / #1184 — the COLUMN-ALIAS LIST on a TABLE FUNCTION in FROM.
//
// PostgreSQL gives every FROM item ONE alias clause (§7.2.1.2/§7.2.1.4), and a
// function in FROM takes it with or without AS. Arc PS read the list on a base
// table, a derived table, a VALUES block and a WITH definition; the
// table-function item kept its own alias arm, which read the list only after
// AS and never without — so `FROM read_json(…) f(k, v)` was a syntax error for
// a statement PostgreSQL answers, and the list the AS spelling did parse was
// dropped downstream (the value half is in wadjet/arc_pt_table_function_alias_test.go).
//
// Every row is PostgreSQL 17.11's DOCUMENTED grammar, measured live: the
// accepted forms, the rejected ones, and the whitespace/case/quoting families
// at the boundary — never the issue's example spellings.
func TestArcPTParserReadsATableFunctionsColumnAliasList(t *testing.T) {
	for _, f := range []struct {
		name    string
		sql     string
		alias   string   // the relation alias the item carries
		cols    []string // the column-alias list, nil when none is written
		code    string   // non-empty when PostgreSQL refuses the FORM
		pg      string   // what PostgreSQL 17.11 does, measured
		refused bool
	}{
		// ---- accepted, with and without AS -----------------------------
		{name: "as_with_list", sql: `SELECT * FROM generate_series(1,3) AS g(x)`,
			alias: "g", cols: []string{"x"}, pg: "1;2;3"},
		{name: "bare_alias_with_list", sql: `SELECT * FROM generate_series(1,3) g(x)`,
			alias: "g", cols: []string{"x"}, pg: "1;2;3"},
		{name: "two_names", sql: `SELECT * FROM read_json('x.json') AS f(k, v)`,
			alias: "f", cols: []string{"k", "v"}, pg: "the list applies positionally"},
		{name: "bare_alias_two_names", sql: `SELECT * FROM read_json('x.json') f(k, v)`,
			alias: "f", cols: []string{"k", "v"}, pg: "the list applies positionally"},
		{name: "space_before_the_list", sql: `SELECT * FROM generate_series(1,3) AS g (x)`,
			alias: "g", cols: []string{"x"}, pg: "1;2;3"},
		{name: "padding_inside_the_list", sql: `SELECT * FROM generate_series(1,3) AS g(  x  )`,
			alias: "g", cols: []string{"x"}, pg: "1;2;3"},
		{name: "newline_before_the_list", sql: "SELECT * FROM generate_series(1,3) AS g\n(x)",
			alias: "g", cols: []string{"x"}, pg: "1;2;3"},
		{name: "lowercase_as", sql: `SELECT * FROM generate_series(1,3) as g(x)`,
			alias: "g", cols: []string{"x"}, pg: "1;2;3"},
		{name: "quoted_name_keeps_its_case", sql: `SELECT * FROM generate_series(1,3) AS g("X")`,
			alias: "g", cols: []string{"X"}, pg: `1;2;3, published as "X"`},
		{name: "unquoted_name_folds", sql: `SELECT * FROM generate_series(1,3) AS g(X)`,
			alias: "g", cols: []string{"x"}, pg: "1;2;3, published as x"},
		{name: "with_ordinality_two_names",
			sql:   `SELECT * FROM unnest(1,2) WITH ORDINALITY AS u(v, o)`,
			alias: "u", cols: []string{"v", "o"}, pg: "1|1;2|2"},
		{name: "with_ordinality_bare_alias",
			sql:   `SELECT * FROM unnest(1,2) WITH ORDINALITY u(v, o)`,
			alias: "u", cols: []string{"v", "o"}, pg: "1|1;2|2"},
		{name: "with_ordinality_short_list_renames_a_prefix",
			sql:   `SELECT * FROM unnest(1,2) WITH ORDINALITY AS u(v)`,
			alias: "u", cols: []string{"v"}, pg: "1|1;2|2, published as v and ordinality"},
		// An alias with NO list names the ONE column of a function that
		// returns a base type as well as the relation: PostgreSQL 17.11
		// publishes this item's column as `g`, and `generate_series` is then
		// no column of it (arc PC measured the name; this cell had asserted
		// only the values).
		{name: "alias_without_a_list", sql: `SELECT * FROM generate_series(1,3) AS g`,
			alias: "g", cols: []string{"g"}, pg: "1;2;3 under the column g"},
		{name: "no_alias_at_all", sql: `SELECT * FROM generate_series(1,3)`,
			alias: "generate_series", cols: nil, pg: "1;2;3"},

		// ---- refused ----------------------------------------------------
		// A list with no alias is not an alias clause on 17.11 either: the
		// parenthesized list after a complete call is a syntax error.
		{name: "list_without_an_alias", sql: `SELECT * FROM generate_series(1,3) (x)`,
			refused: true, code: "42601",
			pg: `42601 syntax error at or near "("`},
		{name: "as_with_a_list_and_no_alias", sql: `SELECT * FROM generate_series(1,3) AS (x)`,
			refused: true, code: "42601",
			pg: `42601 syntax error at or near ")"`},
		{name: "empty_list", sql: `SELECT * FROM generate_series(1,3) AS g()`,
			refused: true, code: "42601",
			pg: `42601 syntax error at or near ")"`},
		// A REPEATED name: this engine refuses at the list, PostgreSQL
		// accepts the list and refuses the reference (42702) — or, when the
		// repeat makes the list longer than the relation, 42P10. The
		// narrower refusal is arc PS's recorded divergence (ADR-0012) and it
		// covers a function's list the same way.
		{name: "repeated_name", sql: `SELECT * FROM generate_series(1,3) AS g(x, x)`,
			refused: true, code: "42701",
			pg: `42P10 table "g" has 1 columns available but 2 columns specified`},
	} {
		t.Run(f.name, func(t *testing.T) {
			parsed, err := Parse(f.sql)
			if f.refused {
				if err == nil {
					t.Fatalf("accepted a form PostgreSQL 17.11 refuses\n  SQL: %s\n  PostgreSQL 17.11: %s",
						f.sql, f.pg)
				}
				if got := sqlerr.StateOf(err); f.code != "" && got != f.code &&
					!(f.code == "42601" && got == "") {
					t.Errorf("SQLSTATE %q, want %q: %v", got, f.code, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused a spelling PostgreSQL 17.11 accepts: %v\n  SQL: %s\n  PostgreSQL 17.11: %s",
					err, f.sql, f.pg)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("ExtractSelect: %v", err)
			}
			if len(info.Tables) != 1 {
				t.Fatalf("expected one FROM item, got %d", len(info.Tables))
			}
			tr := info.Tables[0]
			if !tr.IsFunction {
				t.Fatalf("the FROM item is not a function item: %+v", tr)
			}
			if tr.Alias != f.alias {
				t.Errorf("alias %q, want %q", tr.Alias, f.alias)
			}
			if strings.Join(tr.ColumnAliases, ",") != strings.Join(f.cols, ",") {
				t.Errorf("column-alias list %v, want %v (PostgreSQL 17.11: %s)",
					tr.ColumnAliases, f.cols, f.pg)
			}
		})
	}
}
