// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A CREATE TABLE … AS over a typed NULL (#1436 round 8). The arc's rule for a
// NULL literal under a CAST is scoped to the LAG / LEAD default: elsewhere a
// bare NUMERIC cast of NULL keeps its planner-wide declaration, so a table
// created from one stores what is later written into it exactly, as it did at
// 8b00b112 (c1/c6/c8/c9: PostgreSQL stores 1.25, 2.5, 3.75 and 0.75); the
// round-6 declaration made the column DECIMAL(38,0) and stored 1, 3, 4 and 1.
// A LAG over a bigint expression with a typed-NULL default is numeric and is
// stored exactly past 2^53 (c15, PostgreSQL's 10000000000000001).
//
// c10 PINS a documented gap (aggregates-windows catalog, ADR-0024): a column
// created from a numeric LAG / LEAD result over a bigint takes scale 0, so a
// later 0.75 stores 1 where PostgreSQL's column is unconstrained and stores
// 0.75. 8b00b112 refused the CTAS itself. The day it stores 0.75 this cell
// fails and is deleted as the proof.
func TestArcWDTypedNullCreateTableAs(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		"CREATE TABLE e_t (id BIGINT, b BIGINT, n NUMERIC(10,2), d DOUBLE)",
		"INSERT INTO e_t VALUES (1, 10, 1.25, 1.5), (2, 20, 2.25, 2.5), (3, NULL, NULL, NULL)",
		"CREATE TABLE c1 AS SELECT CAST(NULL AS NUMERIC) AS v",
		"INSERT INTO c1 VALUES (1.25)",
		"UPDATE c1 SET v = 2.5 WHERE v IS NULL",
		"CREATE TABLE c6 AS SELECT NULL::numeric AS v",
		"INSERT INTO c6 VALUES (3.75)",
		"CREATE TABLE c8 AS SELECT id, CASE WHEN id > 1 THEN CAST(NULL AS NUMERIC) END AS v FROM e_t",
		"INSERT INTO c8 VALUES (9, 0.75)",
		"CREATE TABLE c9 AS SELECT id, CAST(NULL AS DECIMAL) AS v FROM e_t",
		"INSERT INTO c9 VALUES (9, 0.75)",
		"CREATE TABLE c10 AS SELECT id, LAG(b, 1, CAST(NULL AS NUMERIC)) OVER (ORDER BY id) AS v FROM e_t",
		"INSERT INTO c10 VALUES (9, 0.75)",
		"CREATE TABLE c15 AS SELECT id, LAG(b * 1000000000000000 + 1, 1, CAST(NULL AS NUMERIC)) OVER (ORDER BY id) AS v FROM e_t",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, c := range []struct{ sql, want string }{
		{"SELECT v FROM c1 ORDER BY v", "1.25 2.5"},
		{"SELECT v FROM c6 WHERE v IS NOT NULL", "3.75"},
		{"SELECT v FROM c8 WHERE id = 9", "0.75"},
		{"SELECT v FROM c9 WHERE id = 9", "0.75"},
		{"SELECT v FROM c15 ORDER BY id", "NULL 10000000000000001 20000000000000001"},
		{"SELECT v FROM c10 WHERE id = 9", "1"}, // the documented gap (PostgreSQL 0.75)
	} {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		var got []string
		for i := range res.Rows {
			for _, v := range res.Cells(i) {
				if v == nil {
					got = append(got, "NULL")
				} else {
					got = append(got, fmt.Sprint(v))
				}
			}
		}
		if g := strings.Join(got, " "); g != c.want {
			t.Errorf("%s\n  got  %s\n  want %s", c.sql, g, c.want)
		}
	}
}
