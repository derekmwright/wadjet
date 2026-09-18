// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"
)

// ARC PS, #959 and #1158 — the COLUMN-ALIAS LIST, `FROM item [AS] a (c1, …)`.
//
// PostgreSQL §7.2.1.2 has ONE alias clause and every FROM item takes it. This
// parser read the list on a derived table, a table function and a VALUES list
// written with AS, and on nothing else — so a base table, a CTE reference and
// a VALUES list written WITHOUT AS were 42601 for statements PostgreSQL
// answers. #959 and #1158 are that one gap seen through different relation
// kinds, and the table below is the grammar, not either issue's examples.
//
// The shared runner and psWhere are in arc_ps_grammar_test.go.

func psColumnAliasForms() []psForm {
	return []psForm{
		// --- the list, on every relation kind, with and without AS --------
		{name: "base_table_with_as", sql: `SELECT * FROM zzp AS a(k, v)`, pg: "renames both columns"},
		{name: "base_table_without_as", sql: `SELECT * FROM zzp a(k, v)`, pg: "same"},
		{name: "base_table_shorter_list", sql: `SELECT * FROM zzp a(k)`,
			pg: "k and the relation's own second name — a SHORTER list renames a PREFIX"},
		{name: "base_table_space_before_list", sql: `SELECT * FROM zzp AS a (k, v)`, pg: "same"},
		{name: "base_table_newline_before_list", sql: "SELECT * FROM zzp AS a\n(k, v)", pg: "same"},
		{name: "base_table_quoted_alias", sql: `SELECT * FROM zzp "A"(k, v)`, pg: "same"},
		{name: "base_table_quoted_column_names", sql: `SELECT * FROM zzp a ("k", "v")`, pg: "same"},
		{name: "qualified_base_table", sql: `SELECT * FROM public.zzp a(k, v)`, pg: "same"},
		{name: "cte_reference_with_as",
			sql: `WITH w AS (SELECT 1 AS c) SELECT * FROM w AS z(x)`, pg: "renames the reference"},
		{name: "cte_reference_without_as",
			sql: `WITH w AS (SELECT 1 AS c) SELECT * FROM w z(x)`, pg: "same"},
		{name: "derived_table_with_as", sql: `SELECT * FROM (SELECT 1 AS c) AS a(x)`, pg: "x"},
		{name: "derived_table_without_as", sql: `SELECT * FROM (SELECT 1 AS c) a(x)`, pg: "x"},
		{name: "values_with_as", sql: `SELECT * FROM (VALUES (1),(2)) AS a(x)`, pg: "x = 1, 2"},
		{name: "values_without_as", sql: `SELECT * FROM (VALUES (1),(2)) a(x)`, pg: "x = 1, 2"},
		{name: "values_shorter_list", sql: `SELECT * FROM (VALUES (1,2)) a(x)`,
			pg: "x and PostgreSQL's default column2"},

		// --- where the renamed names are visible --------------------------
		{name: "renamed_name_in_select", sql: `SELECT k FROM zzp a(k, v)`, pg: "answers"},
		{name: "renamed_name_qualified", sql: `SELECT a.k FROM zzp a(k, v)`, pg: "answers"},
		{name: "renamed_name_in_where", sql: `SELECT COUNT(*) FROM zzp a(k, v) WHERE k = 1`, pg: "1"},
		{name: "renamed_name_in_join_on",
			sql: `SELECT COUNT(*) FROM zzp a(k, v) JOIN zzj b(m, w) ON a.k = b.m`, pg: "3"},
		{name: "renamed_name_in_group_by",
			sql: `SELECT COUNT(*) FROM zzp a(k, v) GROUP BY k`, pg: "one row per k"},
		{name: "renamed_name_in_order_by", sql: `SELECT * FROM zzp a(k, v) ORDER BY k`, pg: "sorted"},
		{name: "renamed_name_in_an_expression", sql: `SELECT k + 1 AS n FROM zzp a(k)`, pg: "answers"},
		{name: "qualified_star_over_a_renamed_relation", sql: `SELECT b.* FROM zzp b(k, v)`, pg: "k, v"},
		{name: "unrenamed_tail_name_still_visible", sql: `SELECT d92 FROM zzp a(k)`,
			pg: "answers — the list renamed only the first column"},
		{name: "renamed_away_name_is_gone", sql: `SELECT id FROM zzp a(k, v)`,
			pg: `42703 column "id" does not exist`},
		{name: "table_name_is_gone_once_aliased", sql: `SELECT zzp.id FROM zzp a(k, v)`,
			pg: "42P01 invalid reference to FROM-clause entry"},
		{name: "no_column_reference_at_all", sql: `SELECT 1 FROM zzp t(a, b)`,
			pg: "answers — #959's own spelling"},

		// --- rejected by PostgreSQL 17.11 ---------------------------------
		{name: "reject_empty_list", sql: `SELECT * FROM zzp a()`,
			reject: true, pg: `42601 syntax error at or near ")"`},
		{name: "reject_list_without_an_alias_name", sql: `SELECT * FROM zzp AS (k)`,
			reject: true, pg: `42601 syntax error at or near "("`},
		{name: "reject_reserved_word_as_a_column_alias", sql: `SELECT * FROM zzp a(select)`,
			reject: true, pg: `42601 syntax error at or near "select"`},
		// A DELIBERATE DIVERGENCE, recorded in docs/postgres-differences.md
		// and ADR-0012: PostgreSQL ACCEPTS a repeated name and refuses every
		// reference to it with 42702. This planner renames positionally and
		// has no scope that can hold two columns under one name, so it
		// refuses the LIST — a narrower answer than PostgreSQL's, never a
		// wrong one. Before this arc the same spelling published the SECOND
		// column's values under both names.
		{name: "reject_repeated_name_is_a_recorded_divergence", sql: `SELECT * FROM zzp a(k, k)`,
			reject: true, pg: "ACCEPTS; refuses a reference to k with 42702"},
	}
}

func TestArcPSParserReadsTheColumnAliasListGrammar(t *testing.T) {
	for _, f := range psColumnAliasForms() {
		t.Run(f.name, func(t *testing.T) {
			_, err := Parse(f.sql)
			if f.reject {
				if err == nil {
					t.Errorf("parsed a spelling this engine must refuse\n  SQL: %s\n  PostgreSQL 17.11: %s",
						f.sql, f.pg)
				}
				return
			}
			if err != nil {
				t.Errorf("refused a spelling PostgreSQL 17.11 accepts: %v\n  SQL: %s\n  PostgreSQL 17.11: %s",
					err, f.sql, f.pg)
			}
		})
	}
}

// TestArcPSNamedRelationColumnAliasesLowerToADerivedTable pins WHERE the
// rewrite happens, which is the whole of why the renamed names resolve.
//
// An alias clause with a column list opens a NEW relation namespace: the named
// columns are the relation's, and its own names are gone. That is a derived
// table, and lowering it to one in the PARSER is what puts the binder — which
// builds the enclosing query's scope from the parsed FROM item's memo
// (sub_block.go, #851) — and the logical builder on the same tree. Lowering it
// in the logical builder instead left the binder resolving against the base
// relation's names: measured, `SELECT k FROM zzp a(k, v)` stayed
// `unknown column "k"` and `SELECT id FROM zzp a(k, v)` answered NULL for a
// column PostgreSQL says does not exist.
func TestArcPSNamedRelationColumnAliasesLowerToADerivedTable(t *testing.T) {
	for _, tc := range []struct {
		name, sql, wantName string
		wantAliases         []string
	}{
		{"base table", `SELECT * FROM zzp a(k, v)`, `(SELECT * FROM zzp)`, []string{"k", "v"}},
		{"base table with AS", `SELECT * FROM zzp AS a(k)`, `(SELECT * FROM zzp)`, []string{"k"}},
		{"qualified base table", `SELECT * FROM public.zzp a(k)`,
			`(SELECT * FROM public.zzp)`, []string{"k"}},
		{"tablesample travels into the body",
			`SELECT * FROM zzp TABLESAMPLE BERNOULLI(10) a(k)`,
			`(SELECT * FROM zzp TABLESAMPLE BERNOULLI(10))`, []string{"k"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := psSelect(t, tc.sql)
			if len(info.Tables) != 1 {
				t.Fatalf("%d FROM items, want 1", len(info.Tables))
			}
			got := info.Tables[0]
			if got.Name != tc.wantName {
				t.Errorf("lowered FROM item\n  got  %s\n  want %s", got.Name, tc.wantName)
			}
			if strings.Join(got.ColumnAliases, ",") != strings.Join(tc.wantAliases, ",") {
				t.Errorf("column aliases = %v, want %v", got.ColumnAliases, tc.wantAliases)
			}
			if got.Alias == "" || got.Alias == got.Name {
				t.Errorf("the relation lost its alias: %q", got.Alias)
			}
		})
	}
	// A relation WITHOUT a list is NOT lowered: the rewrite costs a block, and
	// every statement that did not ask for one keeps the plan it had.
	info := psSelect(t, `SELECT * FROM zzp a`)
	if info.Tables[0].Name != "zzp" {
		t.Errorf("a relation with no column-alias list was lowered anyway: %q", info.Tables[0].Name)
	}
}

func psSelect(t *testing.T, sql string) *SelectInfo {
	t.Helper()
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("ExtractSelect(%q): %v", sql, err)
	}
	return info
}
