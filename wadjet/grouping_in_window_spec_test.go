// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// GROUPING() INSIDE A WINDOW'S SPEC READS THE GROUPING SET (#1590). The
// window-spec aggregate hoist took GROUPING(k) for an aggregate and published
// it as one; the physical planner maps an aggregate name it does not know to
// COUNT, so `count(*) OVER (PARTITION BY GROUPING(k))` partitioned on count(k)
// per group: a real NULL group (count(k) = 0) and a group of two rows each
// landed in a partition of their own, for a plain column and a computed key
// alike. The embedded engine's subset of
// coordinator.TestArc1590bGroupingInWindowEveryArm; every want is PostgreSQL
// 17.11's (postgres:17-alpine, --locale=C), on the plain engine and with every
// aggregate forced to drain to disk.
func TestGroupingInWindowSpecReadsTheGroupingSet(t *testing.T) {
	ctx := context.Background()
	cells := []struct {
		name, sql, pg string
		ordered       bool // compared in arrival order (every ORDER BY here is total)
	}{
		{"col/part", `SELECT GROUPING(s) AS g, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t GROUP BY ROLLUP (s)`,
			"0,1,6 | 0,1,6 | 0,1,6 | 0,1,6 | 0,1,6 | 0,2,6 | 1,7,1", false},
		{"coalesce/part-only", `SELECT count(*) AS n, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi'))`,
			"1,5 | 1,5 | 1,5 | 1,5 | 3,5 | 7,1", false},
		{"concat/order", `SELECT s || 'a' AS k, row_number() OVER (ORDER BY GROUPING(s || 'a'), s || 'a') AS r FROM gw_t GROUP BY ROLLUP (s || 'a')`,
			"Aa,1 | NULL,6 | NULL,7 | aba,2 | hia,3 | hxa,4 | za,5", false},
		{"lower/arg-sum", `SELECT lower(s) AS k, sum(GROUPING(lower(s))) OVER () AS c FROM gw_t GROUP BY ROLLUP (lower(s))`,
			"NULL,1 | NULL,1 | a,1 | ab,1 | hi,1 | hx,1 | z,1", false},
		{"arith/plain", `SELECT n + 1 AS k, count(*) OVER (PARTITION BY GROUPING(n + 1)) AS c FROM gw_t GROUP BY n + 1`,
			"11,7 | 21,7 | 31,7 | 41,7 | 61,7 | 71,7 | NULL,7", false},
		{"two-args", `SELECT GROUPING(s, t) AS g, count(*) OVER (PARTITION BY GROUPING(s, t)) AS c FROM gw_t GROUP BY ROLLUP (s, t)`,
			"0,7 | 0,7 | 0,7 | 0,7 | 0,7 | 0,7 | 0,7 | 1,6 | 1,6 | 1,6 | 1,6 | 1,6 | 1,6 | 3,1", false},
		{"sets-missing-key", `SELECT s, t, count(*) OVER (PARTITION BY GROUPING(t)) AS c FROM gw_t GROUP BY GROUPING SETS ((s), (t))`,
			"A,NULL,6 | NULL,NULL,3 | NULL,NULL,6 | NULL,x,3 | NULL,y,3 | ab,NULL,6 | hi,NULL,6 | hx,NULL,6 | z,NULL,6", false},
		{"first-value", `SELECT s, first_value(count(*)) OVER (PARTITION BY GROUPING(s) ORDER BY count(*) DESC, s) AS f FROM gw_t GROUP BY ROLLUP (s)`,
			"A,2 | NULL,2 | NULL,7 | ab,2 | hi,2 | hx,2 | z,2", false},
		{"case-part", `SELECT s, count(*) OVER (PARTITION BY CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE 'one' END) AS c FROM gw_t GROUP BY ROLLUP (s)`,
			"A,6 | NULL,1 | NULL,6 | ab,6 | hi,6 | hx,6 | z,6", false},
		{"one-group", `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id IN (1, 7) GROUP BY ROLLUP (s)`,
			"NULL,2,1 | hi,2,1", false},
		{"empty", `SELECT s, count(*) AS n, count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t WHERE id > 100 GROUP BY ROLLUP (s)`,
			"NULL,0,1", false},
		// The query's ORDER BY GROUPING(k) with no GROUPING(k) in the SELECT
		// list sorts on GROUPING's slot: it was refused 0A000, or, beside a
		// window-spec GROUPING, sorted on count(k).
		{"order/only", `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY GROUPING(s) DESC, s NULLS FIRST`,
			"NULL,7 | NULL,1 | A,1 | ab,1 | hi,2 | hx,1 | z,1", true},
		{"order/secondary", `SELECT s, count(*) AS n FROM gw_t GROUP BY ROLLUP (s) ORDER BY s NULLS FIRST, GROUPING(s)`,
			"NULL,1 | NULL,7 | A,1 | ab,1 | hi,2 | hx,1 | z,1", true},
		{"order/beside-win-computed", `SELECT COALESCE(s,'hi') AS k, count(*) OVER (PARTITION BY GROUPING(COALESCE(s,'hi'))) AS c FROM gw_t GROUP BY ROLLUP (COALESCE(s,'hi')) ORDER BY GROUPING(COALESCE(s,'hi')), COALESCE(s,'hi') NULLS FIRST`,
			"A,5 | ab,5 | hi,5 | hx,5 | z,5 | NULL,1", true},
		{"order/plain", `SELECT id AS k, count(*) OVER (PARTITION BY GROUPING(id)) AS c FROM gw_t GROUP BY id ORDER BY GROUPING(id), id`,
			"1,7 | 2,7 | 3,7 | 4,7 | 5,7 | 6,7 | 7,7", true},
		// PostgreSQL refuses a GROUPING over no GROUP BY, in a window spec too
		{"nogroup-window", `SELECT count(*) OVER (PARTITION BY GROUPING(s)) AS c FROM gw_t`, "ERR 42803", false},
		// controls, right before the fix too
		{"control/proj", `SELECT s AS k, GROUPING(s) AS g, count(*) AS n FROM gw_t GROUP BY ROLLUP (s)`,
			"A,0,1 | NULL,0,1 | NULL,1,7 | ab,0,1 | hi,0,2 | hx,0,1 | z,0,1", false},
		{"control/case-label-window", `SELECT CASE WHEN GROUPING(s) = 1 THEN 'ALL' ELSE s END AS k, count(*) AS n, sum(count(*)) OVER (PARTITION BY GROUPING(s)) AS tot FROM gw_t GROUP BY ROLLUP (s)`,
			"A,1,7 | ALL,7,7 | NULL,1,7 | ab,1,7 | hi,2,7 | hx,1,7 | z,1,7", false},
	}
	for _, arm := range []string{"plain", "spilled"} {
		t.Run(arm, func(t *testing.T) {
			cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
			if arm == "spilled" {
				cfg.MemoryBudget = 1 << 16
				cfg.SpillDir = t.TempDir()
				defer exec.ForceAggDrainEvery(exec.ForceAggDrainEvery(1))
			}
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, q := range []string{
				"CREATE TABLE gw_t (id BIGINT, s TEXT, t TEXT, n BIGINT)",
				"INSERT INTO gw_t VALUES (1,'hi','x',10),(2,'hx','y',20),(3,'A','x',30),(4,'ab','y',40),(5,NULL,'x',NULL),(6,'z',NULL,60),(7,'hi','y',70)",
			} {
				if _, err := db.Query(ctx, q); err != nil {
					t.Fatalf("%s: %v", q, err)
				}
			}
			// Grouping sets drain through the raw-row branch (#1700), the
			// plain GROUP BY cell through the external merge: either counts.
			before := exec.ForcedAggDrains.Load() + exec.ForcedRawRowDrains.Load()
			for _, c := range cells {
				res, err := db.Query(ctx, c.sql)
				if state, isErr := strings.CutPrefix(c.pg, "ERR "); isErr {
					if err == nil || sqlerr.StateOf(err) != state {
						t.Errorf("%s: %s\n  got %v\n  want SQLSTATE %s (PostgreSQL 17.11)", c.name, c.sql, err, state)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s: %s\n  ERR %s %v\n  want %s (PostgreSQL 17.11)", c.name, c.sql, sqlerr.StateOf(err), err, c.pg)
					continue
				}
				rows := make([]string, len(res.Rows))
				for i := range res.Rows {
					var cs []string
					for _, v := range res.Cells(i) {
						if v == nil {
							cs = append(cs, "NULL")
							continue
						}
						cs = append(cs, fmt.Sprint(v))
					}
					rows[i] = strings.Join(cs, ",")
				}
				if !c.ordered {
					sort.Strings(rows)
				}
				if got := strings.Join(rows, " | "); got != c.pg {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.pg)
				}
			}
			if arm == "spilled" && exec.ForcedAggDrains.Load()+exec.ForcedRawRowDrains.Load() == before {
				t.Error("no aggregate drained to disk: the spilled arm did not exercise the spill path")
			}
		})
	}
}
