// SPDX-License-Identifier: MIT

package wadjet

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ARC FR — A FILE READER IS A RELATION WITH A SCHEMA (#1230, #1231).
//
// ADR-0039 §3 deferred this: the binder ran BEFORE the table-function
// capability was authorized, so the planner could not open a reader's input
// to learn its columns without reading it for an identity that may not be
// allowed to. The order is the other way round now — every statement door
// calls `auth.AuthorizeTableFunctions` first — and the reader is an ordinary
// relation from plan time on.
//
// THE ORACLE. PostgreSQL has no `read_json`, `read_csv` or `read_parquet`, so
// every expectation below is 17.11's answer for an ORDINARY RELATION of the
// schema the reader publishes, measured live on postgres:17.11-alpine
// (--locale=C, text COLLATE "C"):
//
//	CREATE TABLE r1 (a bigint, b text);   1,p  2,q  3,r  4,s
//	CREATE TABLE r2 (c bigint, d text);   2,x  3,y
//	CREATE TABLE empt (a bigint, b text); no rows
//
//	pg_typeof over r1.a:  SUM numeric · MIN bigint · MAX bigint ·
//	                      AVG numeric · COUNT bigint
//	SELECT f.* FROM r1 AS f              → a, b
//	SELECT * FROM empt                   → zero rows, columns a and b
//	SELECT zz FROM empt                  → 42703
//	SELECT zz FROM r1                    → 42703
//	SELECT zz FROM r1 JOIN r2 …          → 42703
//	WITH t AS (SELECT * FROM r2) SELECT zz FROM r1 b JOIN t … → 42703
//	SELECT zz FROM (SELECT * FROM r2) t JOIN r1 b …           → 42703
//
// The readers infer BIGINT for a whole-number column, which is why r1.a is
// declared bigint above rather than integer: the oracle is asked about the
// type the relation actually publishes.
func TestArcFRAFileReaderIsARelationWithASchema(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	j1 := write("r1.json", "{\"a\":1,\"b\":\"p\"}\n{\"a\":2,\"b\":\"q\"}\n"+
		"{\"a\":3,\"b\":\"r\"}\n{\"a\":4,\"b\":\"s\"}\n")
	j2 := write("r2.json", "{\"c\":2,\"d\":\"x\"}\n{\"c\":3,\"d\":\"y\"}\n")
	c1 := write("r1.csv", "a,b\n1,p\n2,q\n3,r\n4,s\n")
	// A zero-BYTE input: it declares nothing at all.
	emptyJSON := write("empty.json", "")
	emptyCSV := write("empty.csv", "")
	// A CSV whose header row is its only row, and a Parquet file with a
	// footer and no row groups. Both DECLARE their columns and have no rows,
	// which is an empty relation and not an absent one.
	hdrCSV := write("hdr.csv", "a,b\n")

	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString},
	}}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	emptyPq := write("empty.parquet", buf.String())

	buf.Reset()
	w, err = parquet.NewWriter(&buf, parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString},
	}}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{
		{"a": int64(1), "b": "p"}, {"a": int64(2), "b": "q"},
		{"a": int64(3), "b": "r"}, {"a": int64(4), "b": "s"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p1 := write("r1.parquet", buf.String())

	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE frcat (id BIGINT, s VARCHAR)`,
		`INSERT INTO frcat VALUES (2,'a'),(3,'b')`,
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}

	rj1, rj2 := "read_json('"+j1+"')", "read_json('"+j2+"')"
	rc1 := "read_csv('" + c1 + "')"
	rp1 := "read_parquet('" + p1 + "')"

	for _, c := range []struct {
		issue, name, sql, want, code, pg string
	}{
		// ---- #1230: `f.*` expands from a reader's own column list -------
		{issue: "#1230", name: "a_qualified_star_over_read_json",
			sql:  `SELECT f.* FROM ` + rj1 + ` AS f ORDER BY f.a`,
			want: "[a,b] 1|p;2|q;3|r;4|s", pg: "a, b"},
		{issue: "#1230", name: "a_qualified_star_over_read_csv",
			sql:  `SELECT f.* FROM ` + rc1 + ` AS f ORDER BY f.a`,
			want: "[a,b] 1|p;2|q;3|r;4|s", pg: "a, b"},
		{issue: "#1230", name: "a_qualified_star_over_read_parquet",
			sql:  `SELECT f.* FROM ` + rp1 + ` AS f ORDER BY f.a`,
			want: "[a,b] 1|p;2|q;3|r;4|s", pg: "a, b"},
		{issue: "#1230", name: "a_qualified_star_beside_another_column",
			sql:  `SELECT f.*, f.a + 1 AS n FROM ` + rj1 + ` AS f ORDER BY f.a`,
			want: "[a,b,n] 1|p|2;2|q|3;3|r|4;4|s|5", pg: "a, b and the expression"},

		// ---- #1230: an EMPTY reader ------------------------------------
		// A relation with no rows still publishes its columns (17.11 over
		// `empt`), and a Parquet footer and a CSV header row are exactly the
		// declaration that makes that possible.
		{issue: "#1230", name: "an_empty_parquet_publishes_its_columns",
			sql:  `SELECT * FROM read_parquet('` + emptyPq + `')`,
			want: "[a,b] ", pg: "zero rows, columns a and b"},
		{issue: "#1230", name: "an_empty_parquet_counts_zero",
			sql:  `SELECT COUNT(*) AS n FROM read_parquet('` + emptyPq + `')`,
			want: "[n] 0", pg: "0"},
		{issue: "#1230", name: "an_unknown_column_over_an_empty_parquet",
			sql:  `SELECT zz FROM read_parquet('` + emptyPq + `')`,
			code: "42703", pg: "42703"},
		{issue: "#1230", name: "a_header_only_csv_publishes_its_columns",
			sql:  `SELECT * FROM read_csv('` + hdrCSV + `')`,
			want: "[a,b] ", pg: "zero rows, columns a and b"},
		{issue: "#1230", name: "an_unknown_column_over_a_header_only_csv",
			sql:  `SELECT zz FROM read_csv('` + hdrCSV + `')`,
			code: "42703", pg: "42703"},
		// A ZERO-BYTE input declares nothing, and a relation with no columns
		// is not an answer this engine has at any door. It is a named
		// refusal now; it used to be `XX000 the result has no columns at
		// all`, the engine reporting an internal invariant. The DIVERGENCE:
		// 17.11 permits a zero-column relation and answers zero rows of zero
		// columns — recorded in ADR-0012's list and on the differences page.
		{issue: "#1230", name: "a_zero_byte_json_is_a_named_refusal",
			sql:  `SELECT * FROM read_json('` + emptyJSON + `')`,
			code: "0A000", pg: "zero rows of zero columns (divergence, ADR-0012)"},
		{issue: "#1230", name: "a_zero_byte_csv_is_a_named_refusal",
			sql:  `SELECT * FROM read_csv('` + emptyCSV + `')`,
			code: "0A000", pg: "zero rows of zero columns (divergence, ADR-0012)"},

		// ---- #1231: an unknown column is 42703 through EVERY path -------
		{issue: "#1231", name: "unknown_column_direct",
			sql: `SELECT zz FROM ` + rj1, code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_qualified",
			sql: `SELECT f.zz FROM ` + rj1 + ` AS f`, code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_in_a_two_reader_join",
			sql:  `SELECT zz FROM ` + rj1 + ` r1 JOIN ` + rj2 + ` r2 ON r1.a = r2.c`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_in_a_three_reader_join",
			sql: `SELECT zz FROM ` + rj1 + ` r1 JOIN ` + rj2 + ` r2 ON r1.a = r2.c ` +
				`JOIN ` + rc1 + ` r3 ON r3.a = r1.a`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_with_a_catalog_table_through_a_cte",
			sql:  `WITH t AS (SELECT id FROM frcat) SELECT zz FROM ` + rj1 + ` b JOIN t ON b.a = t.id`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_with_a_reader_through_a_cte",
			sql:  `WITH t AS (SELECT * FROM ` + rj2 + `) SELECT zz FROM ` + rj1 + ` b JOIN t ON b.a = t.c`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_with_a_reader_through_a_cte_with_a_column_list",
			sql:  `WITH t AS (SELECT c, d FROM ` + rj2 + `) SELECT zz FROM ` + rj1 + ` b JOIN t ON b.a = t.c`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_with_a_reader_through_a_derived_table",
			sql:  `SELECT zz FROM (SELECT * FROM ` + rj2 + `) t JOIN ` + rj1 + ` b ON b.a = t.c`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_with_both_arms_through_derived_tables",
			sql:  `SELECT zz FROM (SELECT * FROM ` + rj1 + `) a JOIN (SELECT * FROM ` + rj2 + `) b ON a.a = b.c`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_through_a_cte_over_one_reader",
			sql:  `WITH t AS (SELECT * FROM ` + rj1 + `) SELECT zz FROM t`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_in_the_comma_spelling_of_two_readers",
			sql:  `SELECT zz FROM ` + rj1 + ` r1, ` + rj2 + ` r2 WHERE r1.a = r2.c`,
			code: "42703", pg: "42703"},
		{issue: "#1231", name: "unknown_column_under_a_left_join_of_two_readers",
			sql:  `SELECT zz FROM ` + rj1 + ` r1 LEFT JOIN ` + rj2 + ` r2 ON r1.a = r2.c`,
			code: "42703", pg: "42703"},

		// The MIRROR: every one of those paths still answers the columns
		// that ARE there. A new refusal that refuses a column that exists is
		// worse than the NULL it replaced.
		{issue: "#1231", name: "a_known_column_in_a_two_reader_join",
			sql:  `SELECT r1.a, r2.d FROM ` + rj1 + ` r1 JOIN ` + rj2 + ` r2 ON r1.a = r2.c ORDER BY r1.a`,
			want: "[a,d] 2|x;3|y", pg: "2|x and 3|y"},
		{issue: "#1231", name: "a_bare_known_column_in_a_two_reader_join",
			sql:  `SELECT a, d FROM ` + rj1 + ` r1 JOIN ` + rj2 + ` r2 ON r1.a = r2.c ORDER BY a`,
			want: "[a,d] 2|x;3|y", pg: "2|x and 3|y"},
		{issue: "#1231", name: "a_known_column_through_a_cte_over_a_reader",
			sql: `WITH t AS (SELECT * FROM ` + rj2 + `) SELECT b.a, t.d FROM ` + rj1 +
				` b JOIN t ON b.a = t.c ORDER BY b.a`,
			want: "[a,d] 2|x;3|y", pg: "2|x and 3|y"},
		{issue: "#1231", name: "a_known_column_through_a_derived_table_over_a_reader",
			sql: `SELECT t.c, b.b FROM (SELECT * FROM ` + rj2 + `) t JOIN ` + rj1 +
				` b ON b.a = t.c ORDER BY t.c`,
			want: "[c,b] 2|q;3|r", pg: "2|q and 3|r"},
		{issue: "#1231", name: "a_star_over_a_cte_over_a_reader",
			sql:  `WITH t AS (SELECT * FROM ` + rj2 + `) SELECT * FROM t ORDER BY c`,
			want: "[c,d] 2|x;3|y", pg: "2|x and 3|y"},
		{issue: "#1231", name: "a_column_alias_list_over_a_reader_still_renames",
			sql:  `SELECT k, v FROM ` + rj1 + ` AS f(k, v) ORDER BY k`,
			want: "[k,v] 1|p;2|q;3|r;4|s", pg: "the renamed columns"},
		{issue: "#1231", name: "the_renamed_away_name_is_unknown",
			sql: `SELECT a FROM ` + rj1 + ` AS f(k, v)`, code: "42703",
			pg: "42703 — AS f(k,v) publishes k and v"},
		{issue: "#1231", name: "an_over_long_alias_list_is_42P10_at_plan_time",
			sql: `SELECT * FROM ` + rj1 + ` AS f(k, v, w)`, code: "42P10",
			pg: `42P10 table "f" has 2 columns available but 3 columns specified`},
	} {
		t.Run(c.issue+"/"+c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if c.code != "" {
				if err == nil {
					t.Fatalf("answered %s where PostgreSQL 17.11 answers %s\n  SQL: %s",
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

	// THE DECLARATION. `pg_typeof` over a bigint column on 17.11: SUM
	// numeric, MIN bigint, MAX bigint, AVG numeric, COUNT bigint. The Go box
	// is the carrier each implies — and `MAX` over a read_csv integer column
	// used to box a STRING, which is #1230's fourth cell.
	for _, c := range []struct{ name, sql, box, pg string }{
		{"sum_over_a_json_reader_is_numeric",
			`SELECT SUM(a) AS v FROM ` + rj1, "string", "numeric"},
		{"sum_over_a_csv_reader_is_numeric",
			`SELECT SUM(a) AS v FROM ` + rc1, "string", "numeric"},
		{"sum_over_a_parquet_reader_is_numeric",
			`SELECT SUM(a) AS v FROM ` + rp1, "string", "numeric"},
		{"max_over_a_csv_reader_keeps_the_input_width",
			`SELECT MAX(a) AS v FROM ` + rc1, "int64", "bigint"},
		{"min_over_a_csv_reader_keeps_the_input_width",
			`SELECT MIN(a) AS v FROM ` + rc1, "int64", "bigint"},
		{"max_over_a_json_reader_keeps_the_input_width",
			`SELECT MAX(a) AS v FROM ` + rj1, "int64", "bigint"},
		{"max_over_a_parquet_reader_keeps_the_input_width",
			`SELECT MAX(a) AS v FROM ` + rp1, "int64", "bigint"},
		{"avg_over_a_reader_is_numeric",
			`SELECT AVG(a) AS v FROM ` + rj1, "string", "numeric"},
		{"count_over_a_reader_is_bigint",
			`SELECT COUNT(a) AS v FROM ` + rj1, "int64", "bigint"},
		{"a_readers_own_column_keeps_its_width",
			`SELECT a AS v FROM ` + rj1 + ` ORDER BY a LIMIT 1`, "int64", "bigint"},
		{"a_readers_text_column_is_text",
			`SELECT b AS v FROM ` + rj1 + ` ORDER BY a LIMIT 1`, "string", "text"},
	} {
		t.Run("decl/"+c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) == 0 || len(res.Columns) == 0 {
				t.Fatalf("no rows\n  SQL: %s", c.sql)
			}
			cells := res.Cells(0)
			if len(cells) == 0 {
				t.Fatalf("no cells\n  SQL: %s", c.sql)
			}
			if got := boxKind(cells[0]); got != c.box {
				t.Errorf("boxes %s, want %s (PostgreSQL 17.11 declares %s)\n  SQL: %s",
					got, c.box, c.pg, c.sql)
			}
		})
	}

	// SUM over a reader's integer column is EXACT, digit for digit — the
	// point of the declaration. 17.11 over `r1` answers 10.
	t.Run("decl/sum_is_exact", func(t *testing.T) {
		got, err := ptRenderQuery(ctx, db, `SELECT SUM(a) AS v FROM `+rj1)
		if err != nil {
			t.Fatal(err)
		}
		if got != "[v] 10" {
			t.Errorf("answered %s, want [v] 10 (PostgreSQL 17.11: 10)", got)
		}
	})

	// A LATER batch that disagrees with the schema the plan read never
	// answers a silent value. `physical.TestArcFRALaterBatchThatDisagrees…`
	// drives the backstop directly; this is the same question at the SQL
	// door, over a JSON file whose column `a` turns from a number into a
	// string after the first batch.
	//
	// THE PIN, pre-existing and measured identically at 0c0d33b6: the JSON
	// reader does not reach the backstop for this file — it writes the string
	// into the integer column's storage and the query fails as a recovered
	// panic rather than as a named type error. It is LOUD, which is the rule
	// this cell holds, and the class is the reader's own; recorded as a
	// filing candidate rather than chased here.
	t.Run("a_later_batch_that_disagrees_never_answers_a_silent_value", func(t *testing.T) {
		var b strings.Builder
		for i := 0; i < 2100; i++ {
			b.WriteString("{\"a\":1,\"b\":\"p\"}\n")
		}
		for i := 0; i < 100; i++ {
			b.WriteString("{\"a\":\"not-a-number\",\"b\":\"p\"}\n")
		}
		drift := write("drift.json", b.String())
		got, err := ptRenderQuery(ctx, db, `SELECT SUM(a) AS v FROM read_json('`+drift+`')`)
		if err == nil {
			t.Fatalf("answered %s over a file whose later rows carry another type; the "+
				"declaration every consumer was built from and the vector that arrived "+
				"describe one column two ways", got)
		}
	})
}
