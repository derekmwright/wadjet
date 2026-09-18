// SPDX-License-Identifier: MIT

package sql

import "testing"

// ARC PS, #655 — `JOIN … USING (c)` and `NATURAL JOIN`.
//
// The table is PostgreSQL §7.2.1.1's joined-table grammar, measured on 17.11.
// The parser's half is what it ACCEPTS and how it desugars the CONDITION; the
// merged OUTPUT column is the star expansion's half
// (logical.usingJoinStarColumns), because it needs both arms' column lists.
//
// What this file asserts: every USING spelling PostgreSQL accepts parses, the
// condition is the per-column equality, the USING list reaches the parsed join
// so the expansion can read it, and the two shapes that are still refused are
// refused — one of them for a reason PostgreSQL shares.

func psJoinUsingForms() []psForm {
	return []psForm{
		{name: "inner", sql: `SELECT COUNT(*) FROM fa JOIN fb USING (id)`, pg: "1"},
		{name: "inner_aliased", sql: `SELECT COUNT(*) FROM fa a JOIN fb b USING (id)`, pg: "1"},
		{name: "left", sql: `SELECT COUNT(*) FROM fa LEFT JOIN fb USING (id)`, pg: "2"},
		{name: "right", sql: `SELECT COUNT(*) FROM fa RIGHT JOIN fb USING (id)`, pg: "2"},
		{name: "full", sql: `SELECT COUNT(*) FROM fa FULL JOIN fb USING (id)`, pg: "3"},
		{name: "left_outer_spelled_out", sql: `SELECT COUNT(*) FROM fa LEFT OUTER JOIN fb USING (id)`, pg: "2"},
		{name: "inner_spelled_out", sql: `SELECT COUNT(*) FROM fa INNER JOIN fb USING (id)`, pg: "1"},
		{name: "two_columns", sql: `SELECT COUNT(*) FROM zzp JOIN zzj USING (id, d92)`, pg: "0"},
		{name: "case_insensitive_column", sql: `SELECT COUNT(*) FROM fa JOIN fb USING (ID)`, pg: "1"},
		{name: "whitespace_inside_the_list", sql: "SELECT COUNT(*) FROM fa JOIN fb USING (\n  id\n)", pg: "1"},
		{name: "star_over_it", sql: `SELECT * FROM fa JOIN fb USING (id)`,
			pg: "THREE columns — id once and first, then a, then b"},
		{name: "qualified_star_over_it", sql: `SELECT fa.* FROM fa JOIN fb USING (id)`, pg: "fa's own two"},
		{name: "both_sides_still_addressable",
			sql: `SELECT fa.id, fb.id FROM fa JOIN fb USING (id)`, pg: "both resolve to their sides"},
		{name: "chained_using_over_the_merged_column",
			sql: `SELECT COUNT(*) FROM fa a JOIN fb b USING (id) JOIN fb c USING (id)`,
			pg:  "1 — after the first USING there is one merged id on the left"},
		{name: "using_then_on_in_the_same_from_item",
			sql: `SELECT COUNT(*) FROM fa a JOIN fb b USING (id) JOIN fb c ON c.id = a.id`, pg: "1"},

		// Rejected by PostgreSQL 17.11.
		{name: "reject_empty_list", sql: `SELECT * FROM fa JOIN fb USING ()`,
			reject: true, pg: `42601 syntax error at or near ")"`},
		{name: "reject_using_on_a_cross_join", sql: `SELECT * FROM fa CROSS JOIN fb USING (id)`,
			reject: true, pg: `42601 syntax error at or near "USING"`},
		{name: "reject_natural_cross_join", sql: `SELECT * FROM fa NATURAL CROSS JOIN fb`,
			reject: true, pg: `42601 syntax error at or near "CROSS"`},
		// PostgreSQL refuses this one too, with 42702 `common column name
		// "id" appears more than once in left table`: after a plain ON join
		// the left side carries two `id`s and neither is a merge. So the
		// bound is not merely conservative — it covers a case PostgreSQL also
		// declines.
		{name: "reject_using_after_an_ON_join_on_the_same_item",
			sql:    `SELECT COUNT(*) FROM fa a JOIN fb b ON a.id = b.id JOIN fb d USING (id)`,
			reject: true, pg: `42702 common column name "id" appears more than once in left table`},

		// REFUSED HERE, ANSWERED THERE — recorded residues, each loud (0A000)
		// and each with its mechanism in docs/postgres-differences.md.
		{name: "residue_natural_join", sql: `SELECT COUNT(*) FROM fa NATURAL JOIN fb`,
			reject: true, pg: "ANSWERS 1 — the keys are the shared column names, which needs the catalog"},
		{name: "residue_natural_left_join", sql: `SELECT COUNT(*) FROM fa NATURAL LEFT JOIN fb`,
			reject: true, pg: "ANSWERS 2"},
		{name: "residue_outer_join_chain",
			sql:    `SELECT COUNT(*) FROM fa a LEFT JOIN fb b USING (id) JOIN fb c USING (id)`,
			reject: true, pg: "ANSWERS 1 — an outer join's merged column is not always the left arm's"},
		{name: "residue_chain_over_a_different_column",
			sql:    `SELECT COUNT(*) FROM fa a JOIN fb b USING (id) JOIN fb c USING (b)`,
			reject: true, pg: "ANSWERS 1"},
	}
}

func TestArcPSParserReadsTheJoinUsingGrammar(t *testing.T) {
	for _, f := range psJoinUsingForms() {
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

// TestArcPSJoinUsingCarriesItsColumnList pins the two halves of USING and the
// seam between them: the parser desugars the CONDITION (which needs no
// catalog) and RECORDS the column list on the join, which is the only way the
// star expansion — the one layer that can read both arms' columns — can state
// the merged output column (ADR-0026 §9).
//
// Dropping the list is what made `SELECT *` over a USING join publish the join
// operator's stream, which carries the joined column TWICE.
func TestArcPSJoinUsingCarriesItsColumnList(t *testing.T) {
	for _, tc := range []struct {
		name, sql, wantCond string
		wantUsing           []string
	}{
		{"one column", `SELECT 1 FROM fa JOIN fb USING (id)`,
			`fa.id = fb.id`, []string{"id"}},
		{"aliased sides", `SELECT 1 FROM fa a JOIN fb b USING (id)`,
			`a.id = b.id`, []string{"id"}},
		{"two columns", `SELECT 1 FROM zzp JOIN zzj USING (id, d92)`,
			`zzp.id = zzj.id and zzp.d92 = zzj.d92`, []string{"id", "d92"}},
		{"folded to lower case", `SELECT 1 FROM fa JOIN fb USING (ID)`,
			`fa.id = fb.id`, []string{"id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := psSelect(t, tc.sql)
			if len(info.Joins) != 1 {
				t.Fatalf("%d joins, want 1", len(info.Joins))
			}
			j := info.Joins[0]
			if j.Condition != tc.wantCond {
				t.Errorf("desugared condition\n  got  %s\n  want %s", j.Condition, tc.wantCond)
			}
			if len(j.Using) != len(tc.wantUsing) {
				t.Fatalf("Using = %v, want %v", j.Using, tc.wantUsing)
			}
			for i, c := range tc.wantUsing {
				if j.Using[i] != c {
					t.Errorf("Using[%d] = %q, want %q", i, j.Using[i], c)
				}
			}
		})
	}
	// An ON join records NO list, so nothing above it merges anything.
	info := psSelect(t, `SELECT 1 FROM fa JOIN fb ON fa.id = fb.id`)
	if len(info.Joins[0].Using) != 0 {
		t.Errorf("an ON join recorded a USING list: %v", info.Joins[0].Using)
	}
}
