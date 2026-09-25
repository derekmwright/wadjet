// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// Arc CW round 4, the embedded engine's half (the coordinator's
// TestArcCW4* gates hold the same cells on every arm). Each cell is PostgreSQL
// 17.11's answer, and each failed at the round-3 tip 3fdec99a:
//
//	subquery   a subquery that RETURNS an array reached the cast and the
//	           comparators with no declared element: `{1704070800000}`, a DATE
//	           array never equal, `{}` equal to `{""}` (B1, B4)
//	decimal    `CAST(v AS DECIMAL(9,4)[])` declared text[], so a DECIMAL element
//	           keyed and compared as its text across two scales (B3)
//	nested     a multi-dimensional array cast into T[] became a 1-D text[] of its
//	           inner arrays' text, and ordered nested-lexicographically (B2, P2)
//	case       `CASE MIN(x) WHEN MAX(x)` compared an unreplaced aggregate call
//	colcol     `WHERE v > w` over two array columns refused ("could not resolve
//	           kernel")
//	setop      an empty FIRST arm of a UNION ALL left the parent reading NULL
func TestArcCW4ContainersAnswerAsPostgreSQLOnTheEmbeddedEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE cw4 (id BIGINT, d DATE, ts TIMESTAMP)",
		"INSERT INTO cw4 VALUES (1, '2024-01-10', '2024-01-01 09:00:00'), (2, '2024-01-11', NULL)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	dec := func(v, ps string) string { return "CAST(" + v + " AS DECIMAL(" + ps + "))" }
	cells := []struct{ name, sql, want string }{
		{"subquery/cast-text", "SELECT CAST((SELECT ARRAY[ts] FROM cw4 WHERE id = 1) AS TEXT)", `{"2024-01-01 09:00:00"}`},
		{"subquery/cast-json", "SELECT CAST((SELECT ARRAY[ts] FROM cw4 WHERE id = 1) AS JSON)", `["2024-01-01T09:00:00"]`},
		{"subquery/date-equal", "SELECT ARRAY[CAST('2024-01-10' AS DATE)] = (SELECT ARRAY[d] FROM cw4 WHERE id = 1)", "true"},
		{"subquery/date-in", "SELECT ARRAY[CAST('2024-01-10' AS DATE)] IN (SELECT ARRAY[d] FROM cw4)", "true"},
		{"subquery/date-any", "SELECT COUNT(*) FROM cw4 WHERE ARRAY[d] = ANY (SELECT ARRAY[d] FROM cw4 WHERE id = 1)", "1"},
		{"subquery/empty-vs-empty-string", "SELECT CAST(ARRAY[] AS TEXT[]) = (SELECT ARRAY[''] FROM cw4 WHERE id = 1)", "false"},
		{"subquery/decimal-order", "SELECT ARRAY[" + dec("10", "5,2") + "] > (SELECT ARRAY[" + dec("9.5", "5,2") + "] FROM cw4 WHERE id = 1)", "true"},
		{"decimal/cast-declares", "SELECT CAST(CAST(ARRAY[10] AS DECIMAL(9,4)[]) AS TEXT)", "{10.0000}"},
		{"decimal/numeric-bare", "SELECT CAST(CAST(ARRAY[10] AS NUMERIC[]) AS TEXT)", "{10}"},
		{"decimal/eq-two-scales", "SELECT ARRAY[" + dec("10", "5,2") + "] = CAST(ARRAY[" + dec("10", "5,2") + "] AS DECIMAL(9,4)[])", "true"},
		{"decimal/join-two-scales", "SELECT COUNT(*) FROM (SELECT ARRAY[" + dec("10", "5,2") + "] AS v FROM cw4) x JOIN (SELECT CAST(ARRAY[" + dec("10", "5,2") + "] AS DECIMAL(9,4)[]) AS w FROM cw4) y ON x.v = y.w", "4"},
		{"decimal/union-two-scales", "SELECT COUNT(*) FROM (SELECT ARRAY[" + dec("10", "5,2") + "] AS v FROM cw4 UNION SELECT CAST(ARRAY[" + dec("10", "5,2") + "] AS DECIMAL(9,4)[]) FROM cw4) z", "1"},
		{"nested/cast-text-array", "SELECT CAST(CAST(ARRAY[ARRAY[1,2],ARRAY[3,4]] AS TEXT[]) AS TEXT)", "{{1,2},{3,4}}"},
		{"nested/cast-int-array", "SELECT CAST(CAST(ARRAY[ARRAY[1.5,2.5]] AS INT[]) AS TEXT)", "{{2,3}}"},
		{"nested/array-length", "SELECT array_length(CAST(ARRAY[ARRAY[1,2],ARRAY[3,4]] AS TEXT[]), 1)", "2"},
		{"nested/array-cmp-flattened", "SELECT ARRAY[ARRAY[1,2],ARRAY[3,4]] > ARRAY[ARRAY[1,2,3]]", "true"},
		{"nested/array-cmp-order-by", "SELECT k FROM (SELECT 1 AS k, ARRAY[ARRAY[1,2],ARRAY[3,4]] AS v UNION ALL SELECT 2, ARRAY[ARRAY[1,2,3]]) q ORDER BY v LIMIT 1", "2"},
		{"case/aggregate-subject", "SELECT CASE MIN(id) WHEN MIN(id) THEN 1 ELSE 0 END FROM cw4", "1"},
		{"case/aggregate-array-subject", "SELECT CASE MIN(ARRAY[d]) WHEN MIN(ARRAY[d]) THEN 1 ELSE 0 END FROM cw4", "1"},
		{"colcol/array-gt", "SELECT COUNT(*) FROM (SELECT ARRAY[id * 2] AS v, ARRAY[id + 1] AS w FROM cw4) q WHERE v > w", "1"},
		{"setop/empty-first-arm", "SELECT CAST(x AS TEXT) FROM (SELECT d AS x FROM cw4 WHERE id < 0 UNION ALL SELECT d FROM cw4 WHERE id = 2) q", "2024-01-11"},
		{"setop/empty-first-arm-count", "SELECT count(x) FROM (SELECT ARRAY[id] AS x FROM cw4 WHERE id < 0 UNION ALL SELECT ARRAY[id] FROM cw4) q", "2"},
	}
	for _, c := range cells {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s refused: %v", c.name, c.sql, err)
			continue
		}
		if len(res.Rows) != 1 {
			t.Errorf("%s: %s answered %d rows", c.name, c.sql, len(res.Rows))
			continue
		}
		if got := fmt.Sprint(res.Cells(0)[0]); got != c.want {
			t.Errorf("%s: %s\n  got %s, want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
}
