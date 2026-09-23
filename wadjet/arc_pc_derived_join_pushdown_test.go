// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestAPredicateInADerivedJoinReachesTheSideItReads: a derived table's alias
// is stamped on every scan inside it, so it named BOTH sides of a join inside
// the derived table, and a WHERE predicate over the RIGHT side's bare column —
// or over a relation aliased like the derived table itself, pgJDBC's
// getColumns shape — was pushed onto the LEFT side's scan and failed `filter
// column … does not exist in the input schema` where PostgreSQL 17.11 answers
// (arc PC; every answer below measured there over the same rows).
func TestAPredicateInADerivedJoinReachesTheSideItReads(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, sql := range []string{
		"CREATE TABLE t1 (a BIGINT, k TEXT)", "CREATE TABLE t2 (b BIGINT)", "CREATE TABLE t3 (c3 BIGINT, m TEXT)",
		"INSERT INTO t1 VALUES (1,'r'),(2,'z'),(3,'r')", "INSERT INTO t2 VALUES (1),(2),(3)",
		"INSERT INTO t3 VALUES (1,'p'),(3,'q')",
	} {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for _, tc := range []struct{ sql, want string }{
		{"SELECT * FROM (SELECT y.a, x.b FROM t2 x JOIN t1 y ON y.a = x.b WHERE k = 'r') d ORDER BY 1", "1|1;3|3"},
		{"SELECT * FROM (SELECT c.a, x.b FROM t2 x JOIN t1 c ON c.a = x.b WHERE c.k = 'r') c ORDER BY 1", "1|1;3|3"},
		{"SELECT * FROM (SELECT x.b FROM t2 x JOIN t1 y ON y.a = x.b WHERE b > 1) d ORDER BY 1", "2;3"},
		{"SELECT * FROM (SELECT y.a, z.m FROM t2 x JOIN t1 y ON y.a = x.b JOIN t3 z ON z.c3 = y.a " +
			"WHERE k = 'r' AND m = 'q') d ORDER BY 1", "3|q"},
		{"SELECT * FROM (SELECT * FROM (SELECT y.a FROM t2 x JOIN t1 y ON y.a = x.b WHERE k = 'r') i " +
			"WHERE a > 1) o ORDER BY 1", "3"},
		{"SELECT * FROM (SELECT y.a, x.b FROM t2 x LEFT JOIN t1 y ON y.a = x.b AND k = 'r') d ORDER BY 2",
			"1|1;NULL|2;3|3"},
		{"SELECT * FROM (SELECT d.a FROM t2 x JOIN t1 d ON d.a = x.b WHERE d.k = 'z') d", "2"},
	} {
		res, err := db.Query(ctx, tc.sql)
		if err != nil {
			t.Errorf("%s: %v — PostgreSQL 17.11 answers %s", tc.sql, err, tc.want)
			continue
		}
		var rows []string
		for _, r := range res.Rows {
			var cells []string
			for _, c := range res.Columns {
				v := r[c]
				if v == nil {
					cells = append(cells, "NULL")
					continue
				}
				cells = append(cells, fmt.Sprint(v))
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
		if got := strings.Join(rows, ";"); got != tc.want {
			t.Errorf("%s: %s — PostgreSQL 17.11 answers %s", tc.sql, got, tc.want)
		}
	}
}
