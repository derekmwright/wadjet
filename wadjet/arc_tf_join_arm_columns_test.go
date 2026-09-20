// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ARC TF round 2 / P1 — A READER USED AS A JOIN ARM IS HELD TO ITS COLUMNS.
//
// Round 1 made a reader's missing column `42703` where the consumer's INPUT is
// the relation, and left it a NULL for every row where the consumer sits above
// a JOIN: over a join the consumer's input is the join's output and a bare
// name there may belong to either arm, so no check was made at all. That is
// the silent answer ADR-0039's own rule forbids, and the round-1 review
// measured five more spellings of it than the ADR recorded.
//
// Two classes of name are CERTAIN directly above a join, and both are checked
// now (physical.stampTableFuncRequiredColumns):
//
//   - a reference QUALIFIED by the arm's own alias — nothing between the join
//     and the consumer has minted a column under that qualifier;
//   - a BARE reference no OTHER arm can provide, which is decidable only when
//     every other arm declares its columns.
//
// The JOIN's own condition is qualified per arm by construction and is read
// the same way. Every `code` below is PostgreSQL 17.11's class for the same
// statement, and every control is a shape that must keep ANSWERING.
func TestArcTFAReaderAsAJoinArmIsHeldToItsColumns(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "tf.json")
	if err := os.WriteFile(jsonPath,
		[]byte("{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE tkj (k BIGINT, m VARCHAR)`,
		`INSERT INTO tkj VALUES (1,'p'),(2,'q')`,
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	rj := "read_json('" + jsonPath + "')"

	for _, c := range []struct {
		name, sql, want, code, pg string
	}{
		// ---- the nine shapes the round-1 review measured as NULL or quiet -
		{name: "a_qualified_reference_in_the_select_list",
			sql:  `SELECT b.zz FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a`,
			code: "42703", pg: "42703"},
		{name: "a_bare_reference_in_the_select_list",
			sql:  `SELECT zz FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a`,
			code: "42703", pg: "42703"},
		{name: "the_reader_as_the_right_arm",
			sql:  `SELECT b.zz FROM tkj JOIN ` + rj + ` AS b ON tkj.k = b.a`,
			code: "42703", pg: "42703"},
		{name: "a_comma_item_beside_a_table",
			sql: `SELECT b.zz FROM ` + rj + ` AS b, tkj`, code: "42703", pg: "42703"},
		{name: "the_reader_on_the_null_extended_side_of_a_left_join",
			sql:  `SELECT b.zz FROM tkj LEFT JOIN ` + rj + ` AS b ON tkj.k = b.a`,
			code: "42703", pg: "42703"},
		{name: "an_order_by_over_the_arm",
			sql:  `SELECT b.a FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a ORDER BY b.zz`,
			code: "42703", pg: "42703"},
		{name: "the_join_condition_itself",
			sql:  `SELECT b.a FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.zz`,
			code: "42703", pg: "42703"},
		{name: "a_where_over_the_arm",
			sql:  `SELECT b.a FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a WHERE b.zz = 1`,
			code: "42703", pg: "42703"},
		{name: "an_aggregate_over_the_arm",
			sql:  `SELECT COUNT(b.zz) AS c FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a`,
			code: "42703", pg: "42703"},
		{name: "a_group_by_over_the_arm",
			sql: `SELECT b.zz AS g, COUNT(*) AS c FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a ` +
				`GROUP BY b.zz`,
			code: "42703", pg: "42703"},

		// ---- controls: every one of these must keep ANSWERING -------------
		{name: "control_a_qualified_reference_that_exists",
			sql:  `SELECT b.a, tkj.m FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a ORDER BY b.a`,
			want: "[a,m] 1|p;2|q", pg: "1|p;2|q"},
		{name: "control_a_bare_reference_that_exists",
			sql:  `SELECT a, m FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a ORDER BY a`,
			want: "[a,m] 1|p;2|q", pg: "1|p;2|q"},
		// A bare name the OTHER arm publishes is the other arm's, and the
		// reader must not be asked for it.
		{name: "control_a_bare_name_the_catalog_arm_publishes",
			sql:  `SELECT m FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a ORDER BY m`,
			want: "[m] p;q", pg: "p;q"},
		// TWO readers: no bare name is decidable, so a bare reference makes
		// NO check and answers as it did — the certainty rule, held to.
		{name: "control_two_readers_decline_the_bare_check",
			sql:  `SELECT b.a FROM ` + rj + ` AS b JOIN ` + rj + ` AS c ON b.a = c.a ORDER BY b.a`,
			want: "[a] 1;2", pg: "1;2"},
		{name: "control_a_derived_arm_beside_the_reader",
			sql: `SELECT b.a FROM ` + rj + ` AS b JOIN (SELECT k AS a FROM tkj) s ` +
				`ON s.a = b.a ORDER BY b.a`,
			want: "[a] 1;2", pg: "1;2"},
		{name: "control_an_expression_over_the_arm",
			sql:  `SELECT b.a + 1 AS v FROM ` + rj + ` AS b JOIN tkj ON tkj.k = b.a ORDER BY v`,
			want: "[v] 2;3", pg: "2;3"},
		{name: "control_a_group_by_expression_over_the_arm",
			sql: `SELECT b.a + 1 AS g, COUNT(*) AS c FROM ` + rj + ` AS b JOIN tkj ` +
				`ON tkj.k = b.a GROUP BY b.a + 1 ORDER BY g`,
			want: "[g,c] 2|1;3|1", pg: "2|1;3|1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if c.code != "" {
				if err == nil {
					t.Fatalf("ANSWERED %s where PostgreSQL 17.11 raises %s — a silent NULL is "+
						"what ADR-0039's rule forbids\n  SQL: %s", got, c.pg, c.sql)
				}
				if st := sqlerr.StateOf(err); st != c.code {
					t.Errorf("SQLSTATE %q, want %q: %v\n  SQL: %s", st, c.code, err, c.sql)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused a statement PostgreSQL 17.11 answers %s: %v\n  SQL: %s",
					c.pg, err, c.sql)
			}
			if got != c.want {
				t.Errorf("answered\n  got  %s\n  want %s (PostgreSQL 17.11: %s)\n  SQL: %s",
					got, c.want, c.pg, c.sql)
			}
		})
	}
}
