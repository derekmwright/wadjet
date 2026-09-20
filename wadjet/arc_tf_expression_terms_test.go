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

// ARC TF round 2 / B3 — A TERM IS A TERM, NOT A COLUMN NAME.
//
// The first-batch guard asks a file reader for the columns the operator above
// it needs, and the logical plan carries some of those needs as rendered TEXT:
// `GROUP BY a + 1` is the string "a + 1", `GROUP BY UPPER(b)` is "upper(b)",
// and an ordinal or a select alias arrives already resolved to the select
// item's text. Reading those as COLUMN NAMES asked the relation for a column
// called "a + 1" and refused eight spellings that answer on PostgreSQL 17.11
// and answered right before this arc — the certainty rule the guard's own
// header states, broken by the one arm that read text instead of an
// expression (round-1 review, B3).
//
// Every `want` is 17.11's answer over the same two rows. The unknown-column
// refusals are kept in the SAME table, because the fix must not be a
// widening: a term that names a column the reader does not publish is still
// 42703, and now names the COLUMN rather than the whole term.
func TestArcTFAnExpressionTermOverAReaderIsNotAColumnName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "tf.json")
	if err := os.WriteFile(jsonPath,
		[]byte("{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	csvPath := filepath.Join(dir, "tf.csv")
	if err := os.WriteFile(csvPath, []byte("a,b\n1,x\n2,y\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	zeekPath := filepath.Join(dir, "zeek.json")
	if err := os.WriteFile(zeekPath,
		[]byte("{\"id.orig_h\":\"10.0.0.1\",\"n\":1}\n{\"id.orig_h\":\"10.0.0.2\",\"n\":2}\n"),
		0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, r := range []struct{ kind, from string }{
		{"read_json", `read_json('` + jsonPath + `')`},
		{"read_csv", `read_csv('` + csvPath + `')`},
	} {
		for _, c := range []struct {
			name, sql, want, code, pg string
		}{
			// ---- the eight spellings the guard refused -------------------
			{name: "group_by_arithmetic",
				sql:  `SELECT a + 1 AS g, COUNT(*) AS c FROM ` + r.from + ` GROUP BY a + 1 ORDER BY g`,
				want: "[g,c] 2|1;3|1", pg: "2|1;3|1"},
			{name: "group_by_a_function_call",
				sql:  `SELECT UPPER(b) AS g, COUNT(*) AS c FROM ` + r.from + ` GROUP BY UPPER(b) ORDER BY g`,
				want: "[g,c] X|1;Y|1", pg: "X|1;Y|1"},
			{name: "group_by_a_cast",
				sql: `SELECT CAST(a AS VARCHAR) AS g, COUNT(*) AS c FROM ` + r.from +
					` GROUP BY CAST(a AS VARCHAR) ORDER BY g`,
				want: "[g,c] 1|1;2|1", pg: "1|1;2|1"},
			{name: "group_by_an_ordinal",
				sql:  `SELECT a + 1 AS g, COUNT(*) AS c FROM ` + r.from + ` GROUP BY 1 ORDER BY g`,
				want: "[g,c] 2|1;3|1", pg: "2|1;3|1"},
			{name: "group_by_a_select_alias",
				sql:  `SELECT a + 1 AS g, COUNT(*) AS c FROM ` + r.from + ` GROUP BY g ORDER BY g`,
				want: "[g,c] 2|1;3|1", pg: "2|1;3|1"},
			{name: "distinct_over_an_expression",
				sql:  `SELECT DISTINCT a + 1 AS v FROM ` + r.from + ` ORDER BY v`,
				want: "[v] 2;3", pg: "2;3"},
			{name: "two_expression_terms",
				sql: `SELECT a + 1 AS g, UPPER(b) AS h, COUNT(*) AS c FROM ` + r.from +
					` GROUP BY a + 1, UPPER(b) ORDER BY g`,
				want: "[g,h,c] 2|X|1;3|Y|1", pg: "2|X|1;3|Y|1"},
			{name: "a_column_beside_an_expression",
				sql: `SELECT b, a + 1 AS g, COUNT(*) AS c FROM ` + r.from +
					` GROUP BY b, a + 1 ORDER BY b`,
				want: "[b,g,c] x|2|1;y|3|1", pg: "x|2|1;y|3|1"},

			// ---- the spellings that always answered, kept as controls ----
			{name: "group_by_a_plain_column",
				sql:  `SELECT b, COUNT(*) AS c FROM ` + r.from + ` GROUP BY b ORDER BY b`,
				want: "[b,c] x|1;y|1", pg: "x|1;y|1"},
			{name: "group_by_two_plain_columns",
				sql:  `SELECT a, b, COUNT(*) AS c FROM ` + r.from + ` GROUP BY a, b ORDER BY a`,
				want: "[a,b,c] 1|x|1;2|y|1", pg: "1|x|1;2|y|1"},
			{name: "group_by_a_qualified_column",
				sql:  `SELECT f.b, COUNT(*) AS c FROM ` + r.from + ` AS f GROUP BY f.b ORDER BY f.b`,
				want: "[b,c] x|1;y|1", pg: "x|1;y|1"},
			{name: "order_by_an_expression",
				sql:  `SELECT a FROM ` + r.from + ` ORDER BY a + 1 DESC`,
				want: "[a] 2;1", pg: "2;1"},
			{name: "order_by_a_function_call",
				sql:  `SELECT b FROM ` + r.from + ` ORDER BY UPPER(b) DESC`,
				want: "[b] y;x", pg: "y;x"},
			{name: "an_aggregate_over_an_expression",
				sql:  `SELECT b, SUM(a * 2) AS s FROM ` + r.from + ` GROUP BY b ORDER BY b`,
				want: "[b,s] x|2;y|4", pg: "x|2;y|4"},
			{name: "a_having_over_an_expression",
				sql: `SELECT b, COUNT(*) AS c FROM ` + r.from +
					` GROUP BY b HAVING COUNT(*) > 0 ORDER BY b`,
				want: "[b,c] x|1;y|1", pg: "x|1;y|1"},

			// ---- the refusals the fix must KEEP -------------------------
			{name: "group_by_an_unknown_column",
				sql:  `SELECT COUNT(*) AS c FROM ` + r.from + ` GROUP BY zz`,
				code: "42703", pg: `42703 column "zz" does not exist`},
			{name: "group_by_an_expression_over_an_unknown_column",
				sql:  `SELECT zz + 1 AS g, COUNT(*) AS c FROM ` + r.from + ` GROUP BY zz + 1`,
				code: "42703", pg: `42703 column "zz" does not exist`},
			{name: "an_aggregate_over_an_unknown_column",
				sql:  `SELECT SUM(zz * 2) AS s FROM ` + r.from,
				code: "42703", pg: "42703"},
			{name: "order_by_an_expression_over_an_unknown_column",
				sql:  `SELECT a FROM ` + r.from + ` ORDER BY zz + 1`,
				code: "42703", pg: "42703"},
			{name: "a_predicate_over_an_unknown_column",
				sql:  `SELECT a FROM ` + r.from + ` WHERE zz + 1 > 0`,
				code: "42703", pg: "42703"},
		} {
			t.Run(r.kind+"/"+c.name, func(t *testing.T) {
				got, err := ptRenderQuery(ctx, db, c.sql)
				if c.code != "" {
					if err == nil {
						t.Fatalf("answered %s where PostgreSQL 17.11 raises %s\n  SQL: %s",
							got, c.pg, c.sql)
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

	// A DELIMITED dotted name is ONE column, and a term that is one must not
	// be read as alias.column — the Zeek shape the guard already had to get
	// right for a plain reference, now through a GROUP BY term.
	for _, c := range []struct{ name, sql, want string }{
		{"group_by_a_quoted_dotted_column",
			`SELECT "id.orig_h" AS h, COUNT(*) AS c FROM read_json('` + zeekPath +
				`') GROUP BY "id.orig_h" ORDER BY h`,
			"[h,c] 10.0.0.1|1;10.0.0.2|1"},
		{"group_by_an_expression_over_a_quoted_dotted_column",
			`SELECT UPPER("id.orig_h") AS h, COUNT(*) AS c FROM read_json('` + zeekPath +
				`') GROUP BY UPPER("id.orig_h") ORDER BY h`,
			"[h,c] 10.0.0.1|1;10.0.0.2|1"},
	} {
		t.Run("zeek/"+c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if err != nil {
				t.Fatalf("refused a statement PostgreSQL 17.11 answers: %v\n  SQL: %s", err, c.sql)
			}
			if got != c.want {
				t.Errorf("answered\n  got  %s\n  want %s\n  SQL: %s", got, c.want, c.sql)
			}
		})
	}
}
