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

// ARC TF — A TABLE FUNCTION IN FROM IS A RELATION.
//
// Every `want` and every `code` below is PostgreSQL 17.11's answer for the
// same shape, measured live in this arc's own container. PostgreSQL has no
// `read_json`, `read_csv` or `read_parquet`, so for those the shape is
// measured through the function PostgreSQL DOES have in FROM —
// `generate_series`, `unnest` — and the reader is held to the same rule.
//
// The rule, in three parts:
//
//  1. A reference to a column the function does not publish is 42703, with the
//     column NAMED. For a function whose SIGNATURE declares its columns that is
//     plan time, exactly as over a base table; for a reader, whose columns are
//     its input's, it is the FIRST BATCH — the binder runs before the
//     table-function capability is authorized, so it may not open the file to
//     find out (#943). It is never a NULL for every row (#1210).
//  2. A correlated reference from inside a scalar subquery over one BINDS the
//     outer row (#1203).
//  3. Its columns carry their TYPE into every consumer: an integer SUM is
//     bigint, never float8 (#1211, ADR-0024).
func TestArcTFATableFunctionInFromIsARelation(t *testing.T) {
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
	pqPath := filepath.Join(dir, "tf.parquet")
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeString},
	}}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{
		{"a": int64(1), "b": "x"}, {"a": int64(2), "b": "y"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pqPath, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	// An EMPTY reader, for the boundary a first-batch refusal has.
	emptyPath := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(emptyPath, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(ctx, `CREATE TABLE tfouter (n BIGINT, s VARCHAR)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, `INSERT INTO tfouter VALUES (1,'a'),(2,'b'),(3,'c')`); err != nil {
		t.Fatal(err)
	}

	rj := "read_json('" + jsonPath + "')"
	rc := "read_csv('" + csvPath + "')"
	rp := "read_parquet('" + pqPath + "')"

	for _, c := range []struct {
		name string
		sql  string
		want string
		code string
		pg   string
	}{
		// ---- 1. an unknown column is 42703, never a NULL (#1210) --------
		{name: "unknown_column_generate_series",
			sql: `SELECT zz FROM generate_series(1,2)`, code: "42703",
			pg: `42703 column "zz" does not exist`},
		{name: "unknown_column_unnest",
			sql: `SELECT zz FROM unnest(1,2)`, code: "42703", pg: "42703"},
		{name: "unknown_column_read_json",
			sql: `SELECT zz FROM ` + rj, code: "42703", pg: "42703"},
		{name: "unknown_column_read_csv",
			sql: `SELECT zz FROM ` + rc, code: "42703", pg: "42703"},
		{name: "unknown_column_read_parquet",
			sql: `SELECT zz FROM ` + rp, code: "42703", pg: "42703"},
		// The function's OWN column name after the alias list renamed it:
		// the list REPLACES the name, it does not add one.
		{name: "the_renamed_away_name_is_unknown",
			sql: `SELECT generate_series FROM generate_series(1,2) AS g(x)`, code: "42703",
			pg: `42703 — AS g(x) publishes x and nothing called generate_series`},
		{name: "unknown_column_qualified",
			sql: `SELECT f.zz FROM ` + rj + ` AS f`, code: "42703", pg: "42703"},
		{name: "unknown_column_in_a_where",
			sql: `SELECT a FROM ` + rj + ` WHERE zz = 1`, code: "42703", pg: "42703"},
		{name: "unknown_column_in_an_aggregate",
			sql: `SELECT COUNT(zz) AS n FROM ` + rj, code: "42703", pg: "42703"},
		{name: "unknown_column_in_an_order_by",
			sql: `SELECT a FROM ` + rj + ` ORDER BY zz`, code: "42703", pg: "42703"},
		{name: "unknown_column_in_a_group_by",
			sql: `SELECT COUNT(*) AS n FROM ` + rj + ` GROUP BY zz`, code: "42703", pg: "42703"},
		{name: "unknown_column_through_a_derived_table",
			sql: `SELECT zz FROM (SELECT * FROM ` + rj + `) s`, code: "42703", pg: "42703"},
		{name: "unknown_column_through_a_cte",
			sql: `WITH c AS (SELECT * FROM ` + rj + `) SELECT zz FROM c`, code: "42703", pg: "42703"},
		{name: "unknown_column_in_arithmetic",
			sql: `SELECT zz + 1 AS v FROM ` + rj, code: "42703", pg: "42703"},
		// A column the relation DOES publish still answers, on every spelling.
		{name: "a_known_column_answers",
			sql: `SELECT a FROM ` + rj + ` ORDER BY a`, want: "[a] 1;2", pg: "1;2"},
		{name: "a_known_column_qualified_answers",
			sql: `SELECT f.a FROM ` + rj + ` AS f ORDER BY f.a`, want: "[a] 1;2", pg: "1;2"},
		{name: "star_answers",
			sql: `SELECT * FROM ` + rc + ` ORDER BY a`, want: "[a,b] 1|x;2|y", pg: "the file's columns"},
		{name: "a_qualified_star_over_a_declared_function_answers",
			sql:  `SELECT g.* FROM generate_series(1,2) AS g`,
			want: "[generate_series] 1;2", pg: "1;2"},

		// ---- 2. correlation binds (#1203) -------------------------------
		{name: "correlated_count_over_generate_series",
			sql: `SELECT n, (SELECT COUNT(*) FROM generate_series(1,2) AS g(x) ` +
				`WHERE x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "correlated_count_qualified",
			sql: `SELECT n, (SELECT COUNT(*) FROM generate_series(1,2) AS g(x) ` +
				`WHERE x <= tfouter.n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "correlated_max_over_generate_series",
			sql: `SELECT n, (SELECT MAX(x) FROM generate_series(1,2) AS g(x) ` +
				`WHERE x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "correlated_over_unnest",
			sql: `SELECT n, (SELECT COUNT(*) FROM unnest(1,2) AS g(x) ` +
				`WHERE x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "correlated_over_read_json",
			sql: `SELECT n, (SELECT COUNT(*) FROM ` + rj + ` AS f(k,v) ` +
				`WHERE k <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|1;2|2;3|2", pg: "1|1;2|2;3|2"},
		{name: "correlated_over_a_stepped_series",
			sql: `SELECT n, (SELECT COUNT(*) FROM generate_series(2,0,-1) AS g(x) ` +
				`WHERE x <= n) AS c FROM tfouter ORDER BY n`,
			want: "[n,c] 1|2;2|3;3|3", pg: "1|2;2|3;3|3"},
		{name: "an_exists_over_a_table_function",
			sql: `SELECT n FROM tfouter WHERE EXISTS (SELECT 1 FROM generate_series(1,2) ` +
				`AS g(x) WHERE x = n) ORDER BY n`,
			want: "[n] 1;2", pg: "1;2"},
		{name: "an_in_over_a_table_function",
			sql:  `SELECT n FROM tfouter WHERE n IN (SELECT x FROM generate_series(1,2) AS g(x)) ORDER BY n`,
			want: "[n] 1;2", pg: "1;2"},

		// ---- 3. the column's TYPE reaches its consumers (#1211) ---------
		// The VALUES here; the DECLARATIONS are asserted by the type table
		// below and on the wire by the coordinator's five-arm gate.
		{name: "sum_over_a_series",
			sql: `SELECT SUM(x) AS s FROM generate_series(1,3) gs(x)`, want: "[s] 6", pg: "6"},
		{name: "arithmetic_over_a_series",
			sql:  `SELECT x * 2 AS v FROM generate_series(1,3) gs(x) ORDER BY v`,
			want: "[v] 2;4;6", pg: "2;4;6"},
		{name: "a_join_of_a_series_and_a_table",
			sql: `SELECT t.n, g.x FROM tfouter t JOIN generate_series(1,2) AS g(x) ` +
				`ON t.n = g.x ORDER BY t.n`,
			want: "[n,x] 1|1;2|2", pg: "1|1;2|2"},

		// ---- the empty relation still publishes its columns -------------
		{name: "an_empty_series_is_zero_rows_of_one_column",
			sql: `SELECT * FROM generate_series(1,0)`, want: "[generate_series] ",
			pg: "zero rows, one column named generate_series"},
		{name: "an_empty_series_under_an_alias_list",
			sql: `SELECT x FROM generate_series(3,1) AS g(x)`, want: "[x] ",
			pg: "zero rows, one column named x"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if c.code != "" {
				if err == nil {
					t.Fatalf("answered %s where PostgreSQL 17.11 raises %s\n  SQL: %s", got, c.pg, c.sql)
				}
				if st := sqlerr.StateOf(err); st != c.code {
					t.Errorf("SQLSTATE %q, want %q: %v\n  SQL: %s", st, c.code, err, c.sql)
				}
				// The refusal NAMES the column, which is the half that makes
				// it actionable — "zz" and not "a column".
				if !strings.Contains(err.Error(), "zz") &&
					!strings.Contains(err.Error(), "generate_series") {
					t.Errorf("the refusal does not name the column: %v", err)
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

	// THE DECLARATION, per consumer. PostgreSQL 17.11 resolves
	// generate_series(int4,int4) for a call whose arguments fit int4, so the
	// column is `integer` and an integer SUM is `bigint` — never float8,
	// which is what every aggregate over a table function's column declared
	// before this arc (#1211). The Go box is the carrier the declaration
	// implies: int32 for integer, int64 for bigint, a rendered string for
	// numeric.
	for _, c := range []struct {
		name, sql, box, pg string
	}{
		{"the_series_column_is_integer", `SELECT x AS v FROM generate_series(1,1) gs(x)`,
			"int32", "integer"},
		{"an_int8_series_column_is_bigint",
			`SELECT x AS v FROM generate_series(3000000000,3000000000) gs(x)`,
			"int64", "bigint"},
		{"sum_is_bigint", `SELECT SUM(x) AS v FROM generate_series(1,3) gs(x)`,
			"int64", "bigint"},
		{"min_keeps_the_input_width", `SELECT MIN(x) AS v FROM generate_series(1,3) gs(x)`,
			"int32", "integer"},
		{"max_keeps_the_input_width", `SELECT MAX(x) AS v FROM generate_series(1,3) gs(x)`,
			"int32", "integer"},
		{"count_is_bigint", `SELECT COUNT(x) AS v FROM generate_series(1,3) gs(x)`,
			"int64", "bigint"},
		{"avg_is_numeric", `SELECT AVG(x) AS v FROM generate_series(1,3) gs(x)`,
			"string", "numeric"},
		// PRE-EXISTING and GENERAL, not a table-function rule: `integer + 1`
		// declares bigint on this engine wherever the column comes from — a
		// base table's `int` column boxes int64 for the same expression
		// (measured) — and ADR-0024 §2b records the divergence. The cell is
		// here because that is the point: the table function's column is now
		// held to the SAME rule a base table's is.
		{"arithmetic_takes_the_same_width_a_base_table_column_takes",
			`SELECT x + 1 AS v FROM generate_series(1,1) gs(x)`,
			"int64", "integer (ADR-0024 §2b, pre-existing)"},
		{"sum_over_unnest_is_bigint", `SELECT SUM(v) AS v FROM unnest(1,2,3) AS u(v)`,
			"int64", "bigint"},
	} {
		t.Run("decl/"+c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) == 0 || len(res.Columns) == 0 {
				t.Fatalf("no rows\n  SQL: %s", c.sql)
			}
			var v any = res.Rows[0][res.Columns[0]]
			if len(res.RowValues) > 0 && len(res.RowValues[0]) > 0 {
				v = res.RowValues[0][0]
			}
			got := boxKind(v)
			if got != c.box {
				t.Errorf("boxes %s, want %s (PostgreSQL 17.11 declares %s)\n  SQL: %s",
					got, c.box, c.pg, c.sql)
			}
		})
	}
}

func boxKind(v any) string {
	switch v.(type) {
	case int32:
		return "int32"
	case int64:
		return "int64"
	case float64:
		return "float64"
	case string:
		return "string"
	case nil:
		return "nil"
	}
	return "other"
}
