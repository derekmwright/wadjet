// SPDX-License-Identifier: MIT

package wadjet

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ARC FR — A JOIN BETWEEN TWO FILE READERS KEYS ON ITS CONDITION (#1229).
//
// `r1` publishes a, b and holds a = 1, 2, 3, 4; `r2` publishes c, d and holds
// c = 2, 3. PostgreSQL 17.11 over two ordinary relations of those schemas
// answers TWO rows for the inner equi-join and FOUR for the LEFT join, and the
// OPERAND ORDER of the ON clause changes nothing: `ON r2.c = r1.a` and
// `ON r1.a = r2.c` are the same condition.
//
// This engine answered EIGHT — the cross product — for every spelling that
// writes the right arm's column first. The mechanism is positional, not
// typed: `SubtreeNaming.ownsKey` decided a key's side from the column SETS,
// and a reader whose schema the plan could not read contributes an EMPTY set,
// so NEITHER arm owned EITHER key, `assignJoinKeySides` left the pair in its
// written order, and each key was then resolved against the arm that does not
// have it. Two misses encode the same flag byte for every row, so every probe
// row matched every build row and the ON was silently gone.
//
// The gate runs with WADJET_TEST_NO_READER_SCHEMA=1, which makes a reader
// publish NO plan-time schema. That is what the engine did for every reader
// before this arc, and it is what this cell must keep exercising afterwards:
// the repair is the QUALIFIER deciding the side, and it must hold for a
// relation whose column list is unknown — otherwise the plan-time schema
// would merely be hiding the defect.
func TestArcFRAJoinBetweenTwoReadersKeysOnItsCondition(t *testing.T) {
	t.Setenv("WADJET_TEST_NO_READER_SCHEMA", "1")
	ctx := context.Background()
	dir := t.TempDir()

	writeFile := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	j1 := writeFile("r1.json", "{\"a\":1,\"b\":\"p\"}\n{\"a\":2,\"b\":\"q\"}\n"+
		"{\"a\":3,\"b\":\"r\"}\n{\"a\":4,\"b\":\"s\"}\n")
	j2 := writeFile("r2.json", "{\"c\":2,\"d\":\"x\"}\n{\"c\":3,\"d\":\"y\"}\n")
	c1 := writeFile("r1.csv", "a,b\n1,p\n2,q\n3,r\n4,s\n")
	c2 := writeFile("r2.csv", "c,d\n2,x\n3,y\n")

	writeParquet := func(name string, cols []parquet.Column, rows []map[string]any) string {
		var buf bytes.Buffer
		w, err := parquet.NewWriter(&buf, parquet.Schema{Columns: cols}, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return writeFile(name, buf.String())
	}
	p1 := writeParquet("r1.parquet",
		[]parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString}},
		[]map[string]any{{"a": int64(1), "b": "p"}, {"a": int64(2), "b": "q"},
			{"a": int64(3), "b": "r"}, {"a": int64(4), "b": "s"}})
	p2 := writeParquet("r2.parquet",
		[]parquet.Column{{Name: "c", Type: parquet.TypeInt64}, {Name: "d", Type: parquet.TypeString}},
		[]map[string]any{{"c": int64(2), "d": "x"}, {"c": int64(3), "d": "y"}})

	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE frl (a BIGINT, b VARCHAR)`,
		`INSERT INTO frl VALUES (1,'p'),(2,'q'),(3,'r'),(4,'s')`,
		`CREATE TABLE frr (c BIGINT, d VARCHAR)`,
		`INSERT INTO frr VALUES (2,'x'),(3,'y')`,
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}

	readers := []struct{ name, left, right string }{
		{"read_json_x_read_json", "read_json('" + j1 + "')", "read_json('" + j2 + "')"},
		{"read_csv_x_read_csv", "read_csv('" + c1 + "')", "read_csv('" + c2 + "')"},
		{"read_json_x_read_csv", "read_json('" + j1 + "')", "read_csv('" + c2 + "')"},
		{"read_parquet_x_read_parquet", "read_parquet('" + p1 + "')", "read_parquet('" + p2 + "')"},
		{"read_csv_x_read_parquet", "read_csv('" + c1 + "')", "read_parquet('" + p2 + "')"},
		// The CONTROL: two catalog relations of the same schema. PostgreSQL's
		// own answer for every row below, and the shape that was already
		// right at base — so a cell that regresses here is not a reader rule.
		{"catalog_x_catalog", "frl", "frr"},
	}

	// Every `want` is PostgreSQL 17.11's answer for two ordinary relations of
	// these schemas holding these rows.
	for _, shape := range []struct {
		name, tmpl, want, pg string
	}{
		{"inner_left_arm_first",
			"SELECT COUNT(*) AS n FROM %[1]s r1 JOIN %[2]s r2 ON r1.a = r2.c",
			"[n] 2", "2"},
		{"inner_right_arm_first",
			"SELECT COUNT(*) AS n FROM %[1]s r1 JOIN %[2]s r2 ON r2.c = r1.a",
			"[n] 2", "2"},
		{"the_rows_right_arm_first",
			"SELECT r1.a, r2.c FROM %[1]s r1 JOIN %[2]s r2 ON r2.c = r1.a ORDER BY r1.a",
			"[a,c] 2|2;3|3", "2|2 and 3|3"},
		{"left_join_right_arm_first",
			"SELECT COUNT(*) AS n FROM %[1]s r1 LEFT JOIN %[2]s r2 ON r2.c = r1.a",
			"[n] 4", "4 — one row per left row"},
		{"left_join_left_arm_first",
			"SELECT COUNT(*) AS n FROM %[1]s r1 LEFT JOIN %[2]s r2 ON r1.a = r2.c",
			"[n] 4", "4"},
		{"comma_and_where_right_arm_first",
			"SELECT COUNT(*) AS n FROM %[1]s r1, %[2]s r2 WHERE r2.c = r1.a",
			"[n] 2", "2"},
		{"comma_and_where_left_arm_first",
			"SELECT COUNT(*) AS n FROM %[1]s r1, %[2]s r2 WHERE r1.a = r2.c",
			"[n] 2", "2"},
		{"arms_swapped_right_arm_first",
			"SELECT COUNT(*) AS n FROM %[2]s r2 JOIN %[1]s r1 ON r1.a = r2.c",
			"[n] 2", "2"},
		{"arms_swapped_left_arm_first",
			"SELECT COUNT(*) AS n FROM %[2]s r2 JOIN %[1]s r1 ON r2.c = r1.a",
			"[n] 2", "2"},
		{"the_right_arm_through_a_cte",
			"WITH t AS (SELECT * FROM %[2]s) SELECT COUNT(*) AS n " +
				"FROM %[1]s r1 JOIN t r2 ON r2.c = r1.a",
			"[n] 2", "2"},
		{"the_left_arm_through_a_cte",
			"WITH t AS (SELECT * FROM %[1]s) SELECT COUNT(*) AS n " +
				"FROM t r1 JOIN %[2]s r2 ON r2.c = r1.a",
			"[n] 2", "2"},
		{"the_right_arm_through_a_derived_table",
			"SELECT COUNT(*) AS n FROM %[1]s r1 JOIN (SELECT * FROM %[2]s) r2 ON r2.c = r1.a",
			"[n] 2", "2"},
		{"both_arms_through_derived_tables",
			"SELECT COUNT(*) AS n FROM (SELECT * FROM %[1]s) r1 JOIN " +
				"(SELECT * FROM %[2]s) r2 ON r2.c = r1.a",
			"[n] 2", "2"},
		{"two_conjuncts_right_arm_first",
			"SELECT COUNT(*) AS n FROM %[1]s r1 JOIN %[2]s r2 ON r2.c = r1.a AND r2.d <> 'zz'",
			"[n] 2", "2"},
		// The CROSS product is still spelled the two ways that MEAN it. A
		// backstop that refuses an unresolved key pair must not reach these.
		{"an_explicit_cross_join_is_still_eight",
			"SELECT COUNT(*) AS n FROM %[1]s r1 CROSS JOIN %[2]s r2",
			"[n] 8", "8"},
		{"an_on_true_sentinel_is_still_eight",
			"SELECT COUNT(*) AS n FROM %[1]s r1 JOIN %[2]s r2 ON 1 = 1",
			"[n] 8", "8"},
	} {
		for _, r := range readers {
			t.Run(shape.name+"/"+r.name, func(t *testing.T) {
				sql := fmt.Sprintf(shape.tmpl, r.left, r.right)
				got, err := ptRenderQuery(ctx, db, sql)
				if err != nil {
					t.Fatalf("refused a statement PostgreSQL 17.11 answers %s: %v\n  SQL: %s",
						shape.pg, err, sql)
				}
				if got != shape.want {
					t.Errorf("answered\n  got  %s\n  want %s (PostgreSQL 17.11: %s)\n  SQL: %s",
						got, shape.want, shape.pg, sql)
				}
			})
		}
	}

	// A join key naming a column the arm does not publish is refused, NAMING
	// it — on both operand orders. PostgreSQL 17.11 raises 42703 for the same
	// reference over an ordinary relation.
	for _, c := range []struct{ name, sql, col string }{
		{"an_unknown_left_key", "SELECT COUNT(*) AS n FROM read_json('" + j1 +
			"') r1 JOIN read_json('" + j2 + "') r2 ON r1.zz = r2.c", "zz"},
		{"an_unknown_right_key", "SELECT COUNT(*) AS n FROM read_json('" + j1 +
			"') r1 JOIN read_json('" + j2 + "') r2 ON r2.zz = r1.a", "zz"},
		{"an_unknown_right_key_written_second", "SELECT COUNT(*) AS n FROM read_json('" + j1 +
			"') r1 JOIN read_json('" + j2 + "') r2 ON r1.a = r2.zz", "zz"},
	} {
		t.Run("refusal/"+c.name, func(t *testing.T) {
			got, err := ptRenderQuery(ctx, db, c.sql)
			if err == nil {
				t.Fatalf("answered %s where PostgreSQL 17.11 raises 42703\n  SQL: %s", got, c.sql)
			}
			if !strings.Contains(err.Error(), c.col) {
				t.Errorf("the refusal does not name the column %q: %v", c.col, err)
			}
		})
	}
}
