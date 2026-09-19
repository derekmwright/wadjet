// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ARC PT / #1184 — the VALUE half: what a table function's column-alias list
// ANSWERS.
//
// The parser has read `AS f(k, v)` on a table-function item since #613, and
// dropped it everywhere but `unnest`: the list reached `Node.FuncColAliases`
// and only the unnest source looked at it. So `SELECT k FROM read_json(…) AS
// f(k, v)` planned a projection of a column no relation publishes and answered
// NULL for every row — a silent wrong value, not a refusal.
//
// The list is applied over the SOURCE (physical.withColumnAliases), because a
// table function's width is not knowable before it reads its input. Every
// `want` is PostgreSQL 17.11's answer over the same rows, measured live.
func TestArcPTATableFunctionsColumnAliasListIsApplied(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "pt.json")
	if err := os.WriteFile(jsonPath,
		[]byte("{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	csvPath := filepath.Join(dir, "pt.csv")
	if err := os.WriteFile(csvPath, []byte("a,b\n1,x\n2,y\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, c := range []struct {
		name string
		sql  string
		want string // columns and rows, or "" when the cell refuses
		code string
		pg   string
	}{
		// ---- the silent NULL, per function ------------------------------
		{name: "read_json_renamed_column_answers",
			sql:  `SELECT k FROM read_json('` + jsonPath + `') AS f(k, v)`,
			want: "[k] 1;2", pg: "1;2"},
		{name: "read_json_without_as",
			sql:  `SELECT k FROM read_json('` + jsonPath + `') f(k, v)`,
			want: "[k] 1;2", pg: "1;2"},
		{name: "read_json_star_publishes_the_new_names",
			sql:  `SELECT * FROM read_json('` + jsonPath + `') AS f(k, v)`,
			want: "[k,v] 1|x;2|y", pg: "the list's names, in order"},
		{name: "read_json_qualified_reference",
			sql:  `SELECT f.k FROM read_json('` + jsonPath + `') AS f(k, v)`,
			want: "[k] 1;2", pg: "1;2"},
		{name: "read_csv_renamed_column_answers",
			sql:  `SELECT k FROM read_csv('` + csvPath + `') AS f(k, v)`,
			want: "[k] 1;2", pg: "1;2"},
		{name: "generate_series_renamed_column_answers",
			sql:  `SELECT x FROM generate_series(1,3) AS g(x)`,
			want: "[x] 1;2;3", pg: "1;2;3"},
		{name: "unnest_renamed_column_answers",
			sql:  `SELECT v FROM unnest(1,2,3) AS u(v)`,
			want: "[v] 1;2;3", pg: "1;2;3"},

		// ---- the list travels through the operators above it ------------
		{name: "a_predicate_over_a_renamed_column",
			sql:  `SELECT k FROM read_json('` + jsonPath + `') AS f(k, v) WHERE v = 'y'`,
			want: "[k] 2", pg: "2"},
		{name: "an_aggregate_over_a_renamed_column",
			sql:  `SELECT SUM(k) AS s FROM read_json('` + jsonPath + `') AS f(k, v)`,
			want: "[s] 3", pg: "3"},
		{name: "an_order_by_over_a_renamed_column",
			sql:  `SELECT x FROM generate_series(1,3) AS g(x) ORDER BY x DESC`,
			want: "[x] 3;2;1", pg: "3;2;1"},
		{name: "a_group_by_over_a_renamed_column",
			sql:  `SELECT x FROM generate_series(1,3) AS g(x) GROUP BY x ORDER BY x`,
			want: "[x] 1;2;3", pg: "1;2;3"},
		{name: "a_join_of_two_renamed_functions",
			sql: `SELECT x, y FROM generate_series(1,2) AS g(x) ` +
				`JOIN generate_series(1,2) AS h(y) ON x = y ORDER BY x`,
			want: "[x,y] 1|1;2|2", pg: "1|1;2|2"},

		// ---- positional rules -------------------------------------------
		{name: "a_short_list_renames_a_prefix",
			sql:  `SELECT * FROM read_json('` + jsonPath + `') AS f(k)`,
			want: "[k,b] 1|x;2|y", pg: "the first column renamed, the rest kept"},
		{name: "ordinality_takes_the_second_name",
			sql:  `SELECT * FROM unnest(7,8) WITH ORDINALITY AS u(v, o)`,
			want: "[v,o] 7|1;8|2", pg: "7|1;8|2"},
		{name: "ordinality_keeps_its_own_name_under_a_short_list",
			sql:  `SELECT * FROM unnest(7,8) WITH ORDINALITY AS u(v)`,
			want: "[v,ordinality] 7|1;8|2", pg: "7|1;8|2, published as v and ordinality"},

		// ---- more names than the relation has ---------------------------
		{name: "too_many_names_read_json",
			sql:  `SELECT * FROM read_json('` + jsonPath + `') AS f(k, v, w)`,
			code: "42P10",
			pg:   `42P10 table "f" has 2 columns available but 3 columns specified`},
		{name: "too_many_names_generate_series",
			sql:  `SELECT * FROM generate_series(1,3) AS g(x, y)`,
			code: "42P10",
			pg:   `42P10 table "g" has 1 columns available but 2 columns specified`},
		{name: "too_many_names_unnest_with_ordinality",
			sql:  `SELECT * FROM unnest(1,2) WITH ORDINALITY AS u(v, o, z)`,
			code: "42P10",
			pg:   `42P10 table "u" has 2 columns available but 3 columns specified`},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if c.code != "" {
				if err == nil {
					t.Fatalf("answered %s where PostgreSQL 17.11 raises %s\n  SQL: %s", got, c.pg, c.sql)
				}
				if st := sqlerr.StateOf(err); st != c.code {
					t.Errorf("SQLSTATE %q, want %q: %v", st, c.code, err)
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

// ptRenderQuery renders a result as "[col,col] v|v;v|v" — the column NAMES are
// half of what this gate asserts, because a dropped rename publishes the
// relation's own names beside NULL values.
func ptRenderQuery(ctx context.Context, db *DB, sql string) (string, error) {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	rows := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		vals := make([]string, 0, len(res.Columns))
		for _, col := range res.Columns {
			v, ok := row[col]
			if !ok || v == nil {
				vals = append(vals, "NULL")
				continue
			}
			vals = append(vals, fmt.Sprintf("%v", v))
		}
		rows = append(rows, strings.Join(vals, "|"))
	}
	return "[" + strings.Join(res.Columns, ",") + "] " + strings.Join(rows, ";"), nil
}
