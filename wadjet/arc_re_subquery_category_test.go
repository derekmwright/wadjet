// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A SCALAR SUBQUERY ROUNDS BY ITS OWN POSTGRESQL TYPE (#381).
//
// `(SELECT sqrt(6.25 + id * 0) FROM t …)` and `(SELECT 5 / 2.0 + id * 0 …)`
// are numeric to PostgreSQL and a double here (ADR-0024 §2c). ROUND and the
// integer CAST over the subquery read its category from the subquery's
// declared column (physical.Planner.SubqueryOutputColumn), which carried the
// FLOAT64 alone: at 1f580f7d they rounded the subquery half to even and
// answered 2 where PostgreSQL 17.11 answers 3 (89cea148 answered 3 for
// round, 2 for the CAST). A subquery over a double precision column stays
// float8 and answers 2. Every want is PostgreSQL 17.11's.
func TestArcREScalarSubqueryRoundsByItsCategory(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		"CREATE TABLE sq_a (id BIGINT, f DOUBLE PRECISION, n NUMERIC(38,16))",
		"INSERT INTO sq_a VALUES (1, 0.5, 0.5), (3, 2.5, 2.5)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, c := range []struct{ q, w string }{
		{"SELECT round((SELECT sqrt(6.25 + id * 0) FROM sq_a WHERE id = 3))", "3"},
		{"SELECT round((SELECT 5 / 2.0 + id * 0 FROM sq_a WHERE id = 3))", "3"},
		{"SELECT CAST((SELECT sqrt(6.25 + id * 0) FROM sq_a WHERE id = 3) AS INTEGER)", "3"},
		{"SELECT id FROM sq_a WHERE id = round((SELECT 5 / 2.0 + id * 0 FROM sq_a WHERE id = 3))", "3"},
		{"SELECT round((SELECT n FROM sq_a WHERE id = 3))", "3"},
		{"SELECT round((SELECT f FROM sq_a WHERE id = 3))", "2"},
		{"SELECT CAST((SELECT f FROM sq_a WHERE id = 3) AS INTEGER)", "2"},
	} {
		res, err := db.Query(ctx, c.q)
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		var rows []string
		for i := range res.Rows {
			var f []string
			for _, v := range res.Cells(i) {
				f = append(f, reText(v))
			}
			rows = append(rows, strings.Join(f, ","))
		}
		if got := strings.Join(rows, "; "); got != c.w {
			t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.q, got, c.w)
		}
	}
}
