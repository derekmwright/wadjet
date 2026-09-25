// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// Arc CW round 5 on the embedded engine (the door an embedded user has):
//
//	lateral   an equality over a table-less LATERAL body was lifted onto the
//	          LATERAL "join", which is a projection and consults no condition
//	          — the NULL rows (and every row of `WHERE o.x = t.w` over
//	          `o.x + 1 AS w`) came back (review N2)
//	correlated a correlated equality over a CREATE TABLE AS array column
//	          refused 0A000 (an ARRAY outer value had no literal; review P2)
//	nested    multi-dimensional arrays answer as they did before the arc:
//	          unnest of a constructed one refuses, a 2-D text literal cast to
//	          INT[] is its text, and a projection that never reads it counts
//	          (review B3, B4)
//	unify     two element TYPES meet at one type on the join key and the set
//	          operations, and CASE keeps the wider DECIMAL scale (B1, B5)
func TestArcCW5ContainersAnswerAsPostgreSQLOnTheEmbeddedEngine(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE cw5 (id BIGINT, x BIGINT)",
		"INSERT INTO cw5 VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)",
		"CREATE TABLE cw5a AS SELECT id, ARRAY[id, x] AS ai FROM cw5",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	cells := []struct{ name, sql, want string }{
		{"lateral/on-null-keys", "SELECT count(*) FROM cw5 o JOIN LATERAL (SELECT o.x AS w) t ON o.x = t.w", "2"},
		{"lateral/where-shifted", "SELECT count(*) FROM cw5 o CROSS JOIN LATERAL (SELECT o.x + 1 AS w) t WHERE o.x = t.w", "0"},
		{"lateral/where-array", "SELECT count(*) FROM cw5 o CROSS JOIN LATERAL (SELECT ARRAY[o.x] AS w) t WHERE ARRAY[o.x] = t.w", "4"},
		{"correlated/ctas-array-equality", "SELECT sum((SELECT count(*) FROM cw5a c2 WHERE c2.ai = cw5a.ai)) FROM cw5a", "4"},
		{"nested/text-literal-passes-through", "SELECT CAST('{{1,2},{3,4}}' AS INT[])", "{{1,2},{3,4}}"},
		{"nested/count-over-unread-projection", "SELECT count(*) FROM (SELECT CAST('{{1,2},{3,4}}' AS INT[]) AS a FROM cw5 WHERE id < 3) z", "2"},
		{"unify/int-float8-join", "SELECT count(*) FROM (SELECT ARRAY[CAST(id AS INT)] AS a FROM cw5) p JOIN (SELECT ARRAY[CAST(id AS DOUBLE)] AS b FROM cw5) q ON p.a = q.b", "4"},
		{"unify/int-numeric-union", "SELECT count(*) FROM (SELECT ARRAY[CAST(id AS INT)] FROM cw5 UNION SELECT ARRAY[CAST(id AS DECIMAL(9,2))] FROM cw5) z", "4"},
		{"unify/case-keeps-scale", "SELECT CAST(CASE WHEN id = 1 THEN ARRAY[CAST(1 AS DECIMAL(5,2))] ELSE ARRAY[CAST(1.2345 AS DECIMAL(9,4))] END AS TEXT) FROM cw5 WHERE id = 2", "{1.2345}"},
		{"unify/case-int-into-numeric", "SELECT CASE WHEN id = 2 THEN ARRAY[CAST(id AS INT)] ELSE ARRAY[CAST(1.5 AS DECIMAL(9,2))] END = ARRAY[CAST(2 AS DECIMAL(9,2))] FROM cw5 WHERE id = 2", "true"},
		{"setop/three-arms-unaliased", "SELECT count(*) FROM (SELECT ARRAY[1] FROM cw5 WHERE id = 1 UNION SELECT ARRAY[2] FROM cw5 WHERE id = 1 UNION SELECT ARRAY[3] FROM cw5 WHERE id = 1) z", "3"},
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
	// The unnest of a CONSTRUCTED multi-dimensional array refuses as it did
	// before the arc (PostgreSQL yields its four leaves; this engine would
	// yield its two inner arrays).
	for _, q := range []string{
		"SELECT count(*) FROM (SELECT unnest(ARRAY[ARRAY[1,2],ARRAY[3,4]]) AS u) z",
		"SELECT sum(u) FROM (SELECT unnest(ARRAY[ARRAY[1,2],ARRAY[3,4]]) AS u) z",
	} {
		_, err := db.Query(ctx, q)
		if err == nil || !strings.Contains(err.Error(), "element type of its array argument is not known") {
			t.Errorf("nested/unnest-refuses: %s answered %v, want the 0A000 refusal", q, err)
		}
	}
}
