// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"os"
	"testing"
	"time"
)

// A RESIDUAL OF #1700, PINNED: a query block whose GROUP BY is only empty
// grouping sets (`GROUPING SETS (())`, `GROUPING SETS ((), ())`) and whose
// select list calls no aggregate builds NO Aggregate node at all.
//
// Mechanism (located, not fixed here): logical.BuildFromSelect builds the
// aggregate only under `if hasAgg || len(info.GroupBy) > 0`
// (internal/planner/logical/builder.go). An empty grouping set contributes no
// GROUP BY term and `SELECT 1` no aggregate, so the condition is false, the
// GroupingSets the parser recorded are never read, and each input row passes
// straight to the projection: `SELECT 1 FROM gs_i x GROUP BY GROUPING SETS
// (())` is four rows where PostgreSQL 17.11 answers one, and over an empty
// input it is no row where PostgreSQL answers one (the empty set is a group of
// every row, which exists over none). An EXISTS body of that shape is
// therefore FALSE over an empty input on every arm. It is neither the
// correlated re-run renderer nor the drain: the uncorrelated and top-level
// spellings move the same way, and ROLLUP (x.a), which carries a GROUP BY
// term, answers right. `count(*)` in the select list (hasAgg) answers right.
//
// Each cell's want is the engine's answer today; pg is PostgreSQL 17.11's
// (measured on postgres:17-alpine, --locale=C, UTF8). A cell whose arm answers
// PostgreSQL's has had its mechanism fixed: re-measure and drop the pin.
func TestGroupingSetEmptySetWithoutAggregateResidual(t *testing.T) {
	if testing.Short() && os.Getenv("GS_ARMS") == "" {
		t.Skip("-short: five arms (GS_ARMS=1 runs it)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	cells := []struct{ name, sql, pin, pg string }{
		{"exists_setsE_empty", `SELECT o.k, (SELECT count(*) FROM gs_i y WHERE EXISTS (SELECT 1 FROM gs_i x WHERE x.k = o.k AND x.k = 99 GROUP BY GROUPING SETS (()))) FROM gs_i o`,
			"1,0 | 1,0 | 1,0 | 2,0", "1,4 | 1,4 | 1,4 | 2,4"},
		{"exists_setsEE_empty", `SELECT o.k, (SELECT count(*) FROM gs_i y WHERE EXISTS (SELECT 1 FROM gs_i x WHERE x.k = o.k AND x.k = 99 GROUP BY GROUPING SETS ((),()))) FROM gs_i o`,
			"1,0 | 1,0 | 1,0 | 2,0", "1,4 | 1,4 | 1,4 | 2,4"},
		{"notexists_setsE_empty", `SELECT o.k, (SELECT count(*) FROM gs_i y WHERE NOT EXISTS (SELECT 1 FROM gs_i x WHERE x.k = o.k AND x.k = 99 GROUP BY GROUPING SETS (()))) FROM gs_i o`,
			"1,4 | 1,4 | 1,4 | 2,4", "1,0 | 1,0 | 1,0 | 2,0"},
		{"exists_onelevelup_setsE_empty", `SELECT o.k, EXISTS (SELECT 1 FROM gs_i x WHERE x.k = o.k AND x.k = 99 GROUP BY GROUPING SETS (())) FROM gs_i o`,
			"1,f | 1,f | 1,f | 2,f", "1,t | 1,t | 1,t | 2,t"},
		{"exists_setsE_matching", `SELECT o.k, (SELECT count(*) FROM gs_i y WHERE EXISTS (SELECT 1 FROM gs_i x WHERE x.k = o.k AND x.k = 2 GROUP BY GROUPING SETS (()))) FROM gs_i o`,
			"1,0 | 1,0 | 1,0 | 2,4", "1,4 | 1,4 | 1,4 | 2,4"},
		{"exists_uncorrelated_setsE_empty", `SELECT o.k, (SELECT count(*) FROM gs_i y WHERE EXISTS (SELECT 1 FROM gs_i x WHERE x.k = 99 GROUP BY GROUPING SETS (()))) FROM gs_i o`,
			"1,0 | 1,0 | 1,0 | 2,0", "1,4 | 1,4 | 1,4 | 2,4"},
		{"toplevel_exists_setsE", `SELECT EXISTS (SELECT 1 FROM gs_i x WHERE x.k = 99 GROUP BY GROUPING SETS (()))`,
			"f", "t"},
		{"derived_setsE_empty", `SELECT count(*) FROM (SELECT 1 FROM gs_i x WHERE x.k = 99 GROUP BY GROUPING SETS (())) s`,
			"0", "1"},
		{"setsE_all_rows", `SELECT 1 FROM gs_i x GROUP BY GROUPING SETS (())`,
			"1 | 1 | 1 | 1", "1"},
		// The neighbours that answer right, kept so the pin's boundary is
		// visible: a GROUP BY term, or an aggregate, builds the Aggregate.
		{"exists_rollupA_empty", `SELECT o.k, (SELECT count(*) FROM gs_i y WHERE EXISTS (SELECT 1 FROM gs_i x WHERE x.k = o.k AND x.k = 99 GROUP BY ROLLUP (x.a))) FROM gs_i o`,
			"1,4 | 1,4 | 1,4 | 2,4", "1,4 | 1,4 | 1,4 | 2,4"},
		{"toplevel_exists_setsE_count", `SELECT EXISTS (SELECT count(*) FROM gs_i x WHERE x.k = 99 GROUP BY GROUPING SETS (()))`,
			"t", "t"},
		{"exists_plain_groupby_empty", `SELECT o.k, (SELECT count(*) FROM gs_i y WHERE EXISTS (SELECT 1 FROM gs_i x WHERE x.k = o.k AND x.k = 99 GROUP BY x.a)) FROM gs_i o`,
			"1,0 | 1,0 | 1,0 | 2,0", "1,0 | 1,0 | 1,0 | 2,0"},
	}
	for _, arm := range gsArms(t, ctx) {
		for _, c := range cells {
			if got := arm.run(c.sql); got != c.pin {
				t.Errorf("%s [%s]: %s\n got    %s\n pinned %s (PostgreSQL 17.11 answers %s)", c.name, arm.name, c.sql, got, c.pin, c.pg)
			}
		}
	}
}
