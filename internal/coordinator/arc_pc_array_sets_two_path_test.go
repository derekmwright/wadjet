// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// Arc PC round 2 on BOTH execution paths: a quantified comparison over an
// ARRAY-valued expression compares with each element, and a set-returning
// SELECT item (unnest, generate_subscripts) expands each row. The stage DAG
// re-plans a worker's fragment from the logical plan, so an item the local
// planner expands must expand there too — the same answer on both arms, and
// a nonzero one, since two arms agreeing on nothing is no evidence.
func TestArcPCArraySetsAgreeOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	for _, c := range []struct{ name, sql string }{
		{"unnest and subscripts", `SELECT id, unnest(c_arr) AS e, generate_subscripts(c_arr, 1) AS o FROM typemx_nested WHERE id < 40 ORDER BY id, o`},
		{"unnest counted", `SELECT count(*) AS c FROM (SELECT unnest(c_arr) AS e FROM typemx_nested) s`},
		{"unnest grouped", `SELECT o, count(*) AS c FROM (SELECT generate_subscripts(c_arr, 1) AS o FROM typemx_nested) s GROUP BY o ORDER BY o`},
		{"= ANY array column", `SELECT count(*) AS c FROM typemx_nested WHERE 'a00005-1' = ANY(c_arr)`},
		{"<> ALL array column", `SELECT count(*) AS c FROM typemx_nested WHERE 'zz' <> ALL(c_arr)`},
		{"= ANY projected", `SELECT id, 'b' = ANY(c_arr) AS c FROM typemx_nested WHERE id BETWEEN 100 AND 104 ORDER BY id`},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			aRes, aErr := tmdRunSingle(ctx, single, c.sql)
			bRes, bErr := tmdRunDAG(ctx, coord, c.sql)
			if aErr != nil {
				t.Fatalf("single-process: %v\n  SQL: %s", aErr, c.sql)
			}
			if bErr != nil {
				t.Fatalf("the stage DAG refused what the single-process engine answered (%d rows): %v\n  SQL: %s",
					len(aRes.Rows), bErr, c.sql)
			}
			if len(aRes.Rows) == 0 {
				t.Fatalf("no rows: the gate needs a nonempty answer\n  SQL: %s", c.sql)
			}
			if diff := oracle.Compare(aRes, bRes, oracle.CompareSpec{Mode: oracle.CmpOrdered}); diff != "" {
				t.Errorf("TWO-PATH DIVERGENCE\n  SQL: %s\n  %s\n  single: %s\n  dag:    %s",
					c.sql, diff, tmdRender(aRes, 6), tmdRender(bRes, 6))
			}
			if countIsZero(aRes) && aRes.Columns[len(aRes.Columns)-1] == "c" && len(aRes.Columns) == 1 {
				t.Errorf("a zero count\n  SQL: %s", c.sql)
			}
		})
	}
}
