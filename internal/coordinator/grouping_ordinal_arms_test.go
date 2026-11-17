// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// AN ORDINAL OR AN OUTPUT ALIAS INSIDE A GROUPING CONSTRUCT, ON ALL FIVE ARMS
// (#1480). The parser resolves a ROLLUP / CUBE / GROUPING SETS term naming a
// select item exactly as it resolves a plain GROUP BY term, and renames the
// grouping sets with it; the DAG routes grouping sets to the coordinator-local
// pipeline (dagplan.ErrGroupingSetsDistributed), which runs the same plan, so
// every arm moves together. The embedded table is
// wadjet.TestGroupByOrdinalInGroupingConstructs; these are the issue's
// statement, four more and the plain GROUP BY 1 control over the arc SS
// fixture (ss_t). Every want is PostgreSQL 17.11's.
func TestGroupingOrdinalFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the arc SS fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := ssArms(t, ctx)
	cells := []struct {
		name, sql string
		ordered   bool
		want      string
	}{
		{"issue", "SELECT t.i AS k, count(*) FROM ss_t t GROUP BY ROLLUP (1)", false,
			"{int,int} rows=7 -7,1 | 0,1 | 1,1 | 3,1 | 5,1 | NULL,1 | NULL,6"},
		{"cubeTwoOrdinals", "SELECT t.i AS k, t.b, count(*) AS c, GROUPING(t.i, t.b) AS g FROM ss_t t GROUP BY CUBE (1, 2)", false,
			"{int,int,int,int} rows=19 -7,-70,1,0 | -7,NULL,1,1 | 0,0,1,0 | 0,NULL,1,1 | 1,1,1,0 | 1,NULL,1,1 | 3,30,1,0 | 3,NULL,1,1 | " +
				"5,9000000000,1,0 | 5,NULL,1,1 | NULL,-70,1,2 | NULL,0,1,2 | NULL,1,1,2 | NULL,30,1,2 | NULL,9000000000,1,2 | " +
				"NULL,NULL,1,0 | NULL,NULL,1,1 | NULL,NULL,1,2 | NULL,NULL,6,3"},
		{"setsOrdinalExpr", "SELECT t.i + 1, count(*) AS c FROM ss_t t GROUP BY GROUPING SETS ((1), ()) ORDER BY 1, 2", true,
			"{int,int} rows=7 -6,1 | 1,1 | 2,1 | 4,1 | 6,1 | NULL,1 | NULL,6"},
		{"rollupAliasHaving", "SELECT t.i AS k, count(*) AS c FROM ss_t t GROUP BY ROLLUP (k) HAVING count(*) < 6", false,
			"{int,int} rows=6 -7,1 | 0,1 | 1,1 | 3,1 | 5,1 | NULL,1"},
		{"rollupOrdinalZero", "SELECT t.i AS k, count(*) AS c FROM ss_t t GROUP BY ROLLUP (0)", false,
			"ERR 42P10"},
		{"keepPlainOrdinal", "SELECT t.i AS k, count(*) FROM ss_t t GROUP BY 1", false,
			"{int,int} rows=6 -7,1 | 0,1 | 1,1 | 3,1 | 5,1 | NULL,1"},
	}
	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql, tc.ordered)
				if err != nil {
					got = "ERR " + sqlerr.StateOf(err)
				}
				t.Logf("%-12s %s", arm.name, got)
				if got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, tc.want)
				}
			}
		})
	}
}
