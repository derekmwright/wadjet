// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// TestArcGKCTASNumericTextEveryArm pins numeric-decimal r24 after embedded
// CTAS: single and spilled512k create and read their own table; the DAG arms
// read the embedded-created column with its actual schema and stored rows.
func TestArcGKCTASNumericTextEveryArm(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, selectSQL, want string }{
		// mixedLength: PostgreSQL {int,int} rows=3 4,3 | 5,2 | NULL,1;
		// base agrees through a text column; tip declares numeric, as PostgreSQL
		// does, and its trimmed CAST text is numeric-decimal r24.
		{"mixedLength", "SELECT 2 * t.n AS k FROM ss_t t GROUP BY 2 * n",
			"{int,int} rows=5 1,1 | 2,2 | 3,1 | 4,1 | NULL,1"},
		// alikeLength: PostgreSQL {int,int} rows=3 4,3 | 5,2 | NULL,1;
		// base and tip both declare numeric and give this same r24 answer.
		{"alikeLength", "SELECT 2 * n AS k FROM ss_t t GROUP BY 2 * n",
			"{int,int} rows=5 1,1 | 2,2 | 3,1 | 4,1 | NULL,1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const tableName = "gk_ctas_numeric"
			const consume = "SELECT length(CAST(k AS TEXT)), count(*) FROM " + tableName + " GROUP BY 1 ORDER BY 1"
			check := func(t *testing.T, got string, err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("%s: %s", consume, got)
				if got != tc.want {
					t.Errorf("got %s; want %s (numeric-decimal r24)", got, tc.want)
				}
			}
			var table tmdTable
			for i, budget := range []int64{0, 512 * 1024} {
				t.Run([]string{"single", "spilled512k"}[i], func(t *testing.T) {
					db := ssStandalone(t, ctx, budget)
					if _, err := db.Query(ctx, "CREATE TABLE "+tableName+" AS "+tc.selectSQL); err != nil {
						t.Fatal(err)
					}
					got, err := ssRunSingle(ctx, db, consume, true)
					check(t, got, err)
					if i == 0 {
						res, err := db.Query(ctx, "SELECT k FROM "+tableName)
						if err != nil {
							t.Fatal(err)
						}
						table = tmdTable{tableName, parquet.Schema{Columns: res.OutputSchema}, res.Rows}
					}
				})
			}
			if table.name == "" {
				t.Fatal("embedded CTAS did not produce the DAG fixture")
			}
			for _, name := range []string{"dag", "dag-shuffled", "dag-morsel4"} {
				t.Run(name, func(t *testing.T) {
					infra := tmdInfra(t, ctx)
					tmdWriteTableList(t, ctx, infra, nil, []tmdTable{table})
					var c *Coordinator
					switch name {
					case "dag":
						c = tmdCoordinator(t, ctx, infra)
					case "dag-shuffled":
						c = tmdCoordinator(t, ctx, infra, func(cfg *Config) { cfg.BroadcastBytesOverride = 1 })
					case "dag-morsel4":
						c = tmdCoordinatorWithWorkers(t, ctx, infra, func(cfg *worker.Config) { cfg.MorselWorkers = 4 })
					}
					got, err := ssRunDAG(ctx, c, consume, true)
					check(t, got, err)
				})
			}
		})
	}
}
