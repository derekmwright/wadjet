// SPDX-License-Identifier: MIT

package wadjet

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// The statement SHAPES of the bytea LIKE matrix (#1593): the operand reaches
// the predicate through a scalar subquery, a derived table, a CTE, a join, a
// CASE, an aggregate FILTER, a GROUP BY key, a HAVING, over an EMPTY table, and
// as a quoted literal over a stored column (the scan's pushdown candidate).
// lkall holds every content pair of bytea_like_matrix_test.go, in order.

type lkShape struct{ name, sql string }

var lkShapes = []lkShape{
	{"scan-prefix", `SELECT id FROM lkall WHERE vb LIKE '\x4125' ORDER BY id`},
	{"scan-prefix-qlit", `SELECT id FROM lkall WHERE vb LIKE 'A%' ORDER BY id`},
	{"scan-escaped-percent", `SELECT id FROM lkall WHERE vb LIKE '\x5c25' ORDER BY id`},
	{"scan-escaped-underscore", `SELECT id FROM lkall WHERE vb LIKE '\x5c5f' ORDER BY id`},
	{"scan-escaped-backslash", `SELECT id FROM lkall WHERE vb LIKE '\x5c5c' ORDER BY id`},
	{"scan-any-one-byte", `SELECT id FROM lkall WHERE vb LIKE '\x5f' ORDER BY id`},
	{"scan-any-run", `SELECT id FROM lkall WHERE vb LIKE '\x25' ORDER BY id`},
	{"scan-not-like", `SELECT id FROM lkall WHERE vb NOT LIKE '\x4125' ORDER BY id`},
	{"scan-suffix", `SELECT id FROM lkall WHERE vb LIKE '\x255f' ORDER BY id`},
	{"scan-contains", `SELECT id FROM lkall WHERE vb LIKE '\x254125' ORDER BY id`},
	{"scan-exact", `SELECT id FROM lkall WHERE vb LIKE '\x41' ORDER BY id`},
	{"scan-empty-pattern", `SELECT id FROM lkall WHERE vb LIKE '\x' ORDER BY id`},
	{"scan-escape-bang", `SELECT id FROM lkall WHERE vb LIKE '\x2141' ESCAPE '!' ORDER BY id`},
	{"scan-escape-empty", `SELECT id FROM lkall WHERE vb LIKE '\x5c41' ESCAPE '' ORDER BY id`},
	{"scan-escape-bytes", `SELECT id FROM lkall WHERE vb LIKE decode('2141','hex') ESCAPE decode('21','hex') ORDER BY id`},
	{"scan-trailing-escape", `SELECT id FROM lkall WHERE vb LIKE '\x255c' ORDER BY id`},
	{"scan-bad-literal", `SELECT id FROM lkall WHERE vb LIKE '\%' ORDER BY id`},
	{"scan-text-pattern", `SELECT id FROM lkall WHERE vb LIKE CAST('A%' AS TEXT) ORDER BY id`},
	{"scan-text-column-pattern", `SELECT id FROM lkall WHERE vb LIKE pt ORDER BY id`},
	{"scan-text-value", `SELECT id FROM lkall WHERE vt LIKE decode('41','hex') ORDER BY id`},
	{"scan-int-pattern", `SELECT id FROM lkall WHERE vb LIKE 1 ORDER BY id`},
	{"scan-ilike", `SELECT id FROM lkall WHERE vb ILIKE '\x4125' ORDER BY id`},
	{"scan-text-control", `SELECT id FROM lkall WHERE vt LIKE 'A%' ORDER BY id`},
	{"scalar-subquery-pattern", `SELECT id FROM lkall WHERE vb LIKE (SELECT pb FROM lkall WHERE id = 3) ORDER BY id`},
	{"scalar-subquery-lone-escape", `SELECT count(*) FROM lkall WHERE vb LIKE (SELECT pb FROM lkall WHERE id = 10)`},
	{"scalar-subquery-text", `SELECT id FROM lkall WHERE vb LIKE (SELECT pt FROM lkall WHERE id = 3) ORDER BY id`},
	{"scalar-subquery-literal", `SELECT id FROM lkall WHERE vb LIKE (SELECT 'A%') ORDER BY id`},
	{"udf-body-bytes", `SELECT lk_udf_f(vb, pb) FROM lk0`},
	{"scalar-subquery-value", `SELECT (SELECT vb FROM lkall WHERE id = 3) LIKE (SELECT pb FROM lkall WHERE id = 3)`},
	{"derived-table", `SELECT d.vb LIKE d.pb FROM (SELECT * FROM lkall WHERE id = 3) d`},
	{"derived-table-where", `SELECT d.id FROM (SELECT * FROM lkall WHERE id = 3 OFFSET 0) d WHERE d.vb LIKE d.pb`},
	{"cte", `WITH c AS (SELECT * FROM lkall WHERE id = 3) SELECT vb LIKE pb FROM c`},
	{"cte-text-pattern", `WITH c AS (SELECT * FROM lkall WHERE id = 3) SELECT vb LIKE pt FROM c`},
	{"join-on", `SELECT a.id FROM lkall a JOIN lkall c ON a.vb LIKE c.pb WHERE c.id = 3 ORDER BY a.id`},
	{"case-value", `SELECT CASE WHEN vb LIKE '\x4125' THEN 1 ELSE 0 END FROM lkall ORDER BY id`},
	{"case-pattern", `SELECT id FROM lkall WHERE vb LIKE CASE WHEN id = 3 THEN pb ELSE decode('41','hex') END ORDER BY id`},
	{"coalesce-pattern", `SELECT id FROM lkall WHERE vb LIKE coalesce(decode('41','hex'), pb) ORDER BY id`},
	{"concat-pattern", `SELECT id FROM lkall WHERE vb LIKE decode('41','hex') || decode('25','hex') ORDER BY id`},
	{"aggregate-filter", `SELECT count(*) FILTER (WHERE vb LIKE '\x4125') FROM lkall`},
	{"group-by-key", `SELECT vb LIKE '\x4125' AS m, count(*) FROM lkall GROUP BY 1 ORDER BY 1`},
	{"having", `SELECT count(*) FROM lkall GROUP BY id HAVING count(*) FILTER (WHERE vb LIKE '\x4125') > 0 ORDER BY id`},
	{"in-subquery", `SELECT id FROM lkall WHERE (vb LIKE '\x4125') IN (SELECT true) ORDER BY id`},
	{"empty-table-mixed", `SELECT * FROM lkempty WHERE vb LIKE pt`},
	{"empty-table-bytes", `SELECT * FROM lkempty WHERE vb LIKE pb`},
	{"where-false-mixed", `SELECT id FROM lkall WHERE false AND vb LIKE pt`},
	{"where-false-bad-literal", `SELECT id FROM lkall WHERE false AND vb LIKE '\%'`},
	{"null-value-column", `SELECT id FROM lkall WHERE nb LIKE '\x4125' ORDER BY id`},
	{"order-by-like", `SELECT id FROM lkall ORDER BY vb LIKE '\x4125', id`},
}

func lkShapeSetup(t *testing.T, r lkRunner) {
	for _, q := range []string{
		`DROP TABLE IF EXISTS lkempty`,
		`CREATE TABLE lkempty (vb BYTES, pb BYTES, pt TEXT)`,
		// The same function in each dialect; the one the server does not
		// parse fails and is ignored.
		`CREATE FUNCTION lk_udf_f(a, b) AS a LIKE b`,
		`CREATE OR REPLACE FUNCTION lk_udf_f(a bytea, b bytea) RETURNS boolean LANGUAGE sql AS 'SELECT a LIKE b'`,
	} {
		_ = r.exec(q)
	}
}

func lkShapeAnswer(r lkRunner, sql string) string {
	rows, err := r.rows(sql)
	if err != nil {
		return "ERR " + lkState(err)
	}
	return rows
}

// lkShapeResidual pins a shape whose answer still differs from PostgreSQL's,
// at what the engine answers. `(SELECT 'A%')` is a FROM-less scalar subquery
// the parser folds into its literal before any plan sees it, so the operand is
// an `unknown` quoted literal read by byteain where PostgreSQL types the
// subquery's column text and refuses 42883 (the same fold that makes
// `id IN (SELECT '12')` answer as `id IN ('12')`, docs/postgres-differences.md).
// The answer is the one the base tree gave; this change did not move it.
//
// `udf-body-bytes` is a CREATE FUNCTION body's LIKE: the engine's function
// parameters are untyped, so the body has no declared operand type and reads
// the text matcher (`t` for 5c41 LIKE 5c41, PostgreSQL's declared bytea
// parameters read bytea's, `f`). Also the base's answer.
var lkShapeResidual = map[string]string{
	"udf-body-bytes":          "true",
	"scalar-subquery-literal": "1;2;5;7;11;13;14;20;21;22;26;27;28;29;32;33;34;35;36",
}

// TestByteaLike1593Shapes: the statement shapes a bytea LIKE reaches an
// engine through — a stored column against a quoted literal (the scan's
// pushdown candidate), a scalar subquery, a derived table, a CTE, a join, a
// CASE, an aggregate FILTER, a GROUP BY key, HAVING, an empty table and WHERE
// false (PostgreSQL refuses while it analyses the statement, not per row).
func TestByteaLike1593Shapes(t *testing.T) {
	r, _ := lkOpen(t, 0)
	lkSetup(t, r)
	lkShapeSetup(t, r)
	for _, s := range lkShapes {
		want, ok := lkShapePG[s.name]
		if !ok {
			t.Fatalf("%s: no pinned answer", s.name)
		}
		if res, known := lkShapeResidual[s.name]; known && os.Getenv("LK_PG_DSN") == "" {
			want = res
		}
		if got := lkShapeAnswer(r, s.sql); got != want {
			t.Errorf("%s: %s\n  got  %s\n  want %s", s.name, s.sql, got, want)
		}
	}
}

func TestBytea1593ShapesDump(t *testing.T) {
	out := os.Getenv("LK_SHAPES_DUMP")
	if out == "" {
		t.Skip("LK_SHAPES_DUMP not set")
	}
	r, _ := lkOpen(t, 0)
	lkSetup(t, r)
	lkShapeSetup(t, r)
	var lines []string
	for _, s := range lkShapes {
		lines = append(lines, fmt.Sprintf("%s\t%s", s.name, lkShapeAnswer(r, s.sql)))
	}
	sort.Strings(lines)
	if err := os.WriteFile(out, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}
