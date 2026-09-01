package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// GROUPING SETS / ROLLUP / CUBE, on three arms against live PostgreSQL 17.
//
// The stage DAG carried no representation for the construct AT ALL — no Stage
// field, no `distributed.OpSpec` tag, no worker read — so `walkStages` ran the
// UNION of the sets' terms as a plain GROUP BY and returned it as the answer:
//
//	GROUP BY GROUPING SETS ((g), (h))   PG 7 rows, DAG 12 — the CROSS PRODUCT
//	GROUP BY ROLLUP (g)                 PG 4 rows, DAG 3 — no grand total
//
// Silently, and for PLAIN column keys as much as computed ones, which is wider
// than #778's filing said. The single-process path had the other half of the
// defect: `info.GroupByExprs` was populated for a simple GROUP BY and not for
// any of the three grouping constructs, so `buildAggregate` could not tell a
// derived key from a column of its input and refused `ROLLUP (g + 1)` outright
// with `GROUP BY key "g + 1" is not a column of its input`.
//
// Both halves are fixed here, and the two fixes meet: the parser carries each
// term's PARSED form, so a derived key is materialized into its hidden slot the
// way every other derived key is; and the DAG REFUSES the construct with a typed
// error that routes the query onto the coordinator-local single-process
// pipeline, which is the only engine in the process whose HashAggregate knows
// what a grouping set is.
//
// Every `want` is PostgreSQL 17's over the same rows, whole result, ordered.
func TestGroupingSetsMatchPostgresOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	arms := []struct {
		name  string
		run   func(string) (*oracle.Result, error)
		coord *Coordinator
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }, nil},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }, coord},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }, coordB},
	}

	// collslot's g is i%3 (80 rows each over 240) and h is i%4 (60 each); the
	// two are chosen so no super-aggregate count can be confused with a
	// per-group one, and so the CROSS PRODUCT the DAG used to answer (12 rows
	// of 20) is a different SHAPE and not only a different row count.
	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string
	}{
		// --- computed keys: LOUD on the single path, silently short on the DAG.
		{
			name: "grouping-sets/computed-keys",
			sql: "SELECT g + 1 AS k, h + 1 AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY GROUPING SETS ((g + 1), (h + 1)) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "7 rows: 1||80;2||80;3||80;|1|60;|2|60;|3|60;|4|60;",
		},
		{
			name: "rollup/computed-key",
			sql: "SELECT g + 1 AS k, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP (g + 1) ORDER BY k, n",
			cols: []string{"k", "n"},
			want: "4 rows: 1|80;2|80;3|80;|240;",
		},
		{
			name: "cube/two-computed-keys",
			sql: "SELECT g + 1 AS k, h + 1 AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY CUBE (g + 1, h + 1) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "20 rows: 1|1|20;1|2|20;1|3|20;1|4|20;1||80;2|1|20;2|2|20;2|3|20;2|4|20;2||80;" +
				"3|1|20;3|2|20;3|3|20;3|4|20;3||80;|1|60;|2|60;|3|60;|4|60;||240;",
		},
		{
			name: "rollup/two-computed-keys",
			sql: "SELECT g + 1 AS k, h + 1 AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP (g + 1, h + 1) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "16 rows: 1|1|20;1|2|20;1|3|20;1|4|20;1||80;2|1|20;2|2|20;2|3|20;2|4|20;2||80;" +
				"3|1|20;3|2|20;3|3|20;3|4|20;3||80;||240;",
		},
		{
			// A PLAIN key beside a computed one: the two are materialized
			// differently (the plain one is already a column, the computed one
			// goes into a hidden slot), and a set has to index BOTH.
			name: "rollup/plain-key-then-computed-key",
			sql: "SELECT g AS k, h + 1 AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY ROLLUP (g, h + 1) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "16 rows: 0|1|20;0|2|20;0|3|20;0|4|20;0||80;1|1|20;1|2|20;1|3|20;1|4|20;1||80;" +
				"2|1|20;2|2|20;2|3|20;2|4|20;2||80;||240;",
		},
		{
			name: "grouping-sets/explicit-empty-set",
			sql: "SELECT g + 1 AS k, h + 1 AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY GROUPING SETS ((g + 1, h + 1), (g + 1), ()) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "16 rows: 1|1|20;1|2|20;1|3|20;1|4|20;1||80;2|1|20;2|2|20;2|3|20;2|4|20;2||80;" +
				"3|1|20;3|2|20;3|3|20;3|4|20;3||80;||240;",
		},
		{
			// A NON-COUNT aggregate over the super-aggregate row: a dropped
			// grand total is invisible to a gate that only counts rows, and a
			// SUM says what was actually summed over.
			name: "rollup/sum-over-the-super-aggregate",
			sql: "SELECT g + 1 AS k, SUM(h) AS s FROM collslot " +
				"GROUP BY ROLLUP (g + 1) ORDER BY k, s",
			cols: []string{"k", "s"},
			want: "4 rows: 1|120;2|120;3|120;|360;",
		},
		// --- a STRING key, and a NULL-heavy one over typemx (g is NULL every
		// thirteenth row), where the super-aggregate NULL and the GROUP's own
		// NULL land in the same column and only the count tells them apart.
		{
			name: "rollup/null-bearing-computed-key",
			sql: "SELECT g + 1 AS k, COUNT(*) AS n FROM typemx " +
				"GROUP BY ROLLUP (g + 1) ORDER BY k, n",
			cols: []string{"k", "n"},
			want: "9 rows: 1|660;2|660;3|659;4|659;5|659;6|659;7|660;|384;|5000;",
		},
		{
			name: "rollup/string-computed-key",
			sql: "SELECT c_str || 'x' AS k, COUNT(*) AS n FROM typemx WHERE id < 12 " +
				"GROUP BY ROLLUP (c_str || 'x') ORDER BY k, n",
			cols: []string{"k", "n"},
			want: "13 rows: s-000000x|1;s-000001x|1;s-000002x|1;s-000003x|1;s-000004x|1;" +
				"s-000005x|1;s-000006x|1;s-000007x|1;s-000008x|1;s-000009x|1;s-000010x|1;" +
				"s-000011x|1;|12;",
		},
		// --- PLAIN keys. #778 said these worked; they did on the single path
		// and never on the DAG, which is why they are gated here and not
		// treated as controls.
		{
			name: "grouping-sets/plain-keys",
			sql: "SELECT g AS k, h AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY GROUPING SETS ((g), (h)) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "7 rows: 0||80;1||80;2||80;|0|60;|1|60;|2|60;|3|60;",
		},
		{
			name: "rollup/plain-key",
			sql:  "SELECT g AS k, COUNT(*) AS n FROM collslot GROUP BY ROLLUP (g) ORDER BY k, n",
			cols: []string{"k", "n"},
			want: "4 rows: 0|80;1|80;2|80;|240;",
		},
		{
			name: "cube/plain-keys",
			sql: "SELECT g AS k, h AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY CUBE (g, h) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "20 rows: 0|0|20;0|1|20;0|2|20;0|3|20;0||80;1|0|20;1|1|20;1|2|20;1|3|20;1||80;" +
				"2|0|20;2|1|20;2|2|20;2|3|20;2||80;|0|60;|1|60;|2|60;|3|60;||240;",
		},
		{
			name: "grouping-sets/plain-keys-explicit-empty-set",
			sql: "SELECT g AS k, h AS j, COUNT(*) AS n FROM collslot " +
				"GROUP BY GROUPING SETS ((g, h), (g), ()) ORDER BY k, j, n",
			cols: []string{"k", "j", "n"},
			want: "16 rows: 0|0|20;0|1|20;0|2|20;0|3|20;0||80;1|0|20;1|1|20;1|2|20;1|3|20;1||80;" +
				"2|0|20;2|1|20;2|2|20;2|3|20;2||80;||240;",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := int64(0)
				if arm.coord != nil {
					before = arm.coord.GroupingSetsLocalRoutes()
				}
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
				// WHICH mechanism answered, not only what it answered. The DAG
				// has no grouping-set representation, so a right answer there
				// can only have come from the typed refusal routing the query
				// local — and if some later change makes the DAG answer these
				// by accident, this is the assertion that says the refusal
				// stopped firing and must be re-examined rather than trusted.
				if arm.coord != nil && arm.coord.GroupingSetsLocalRoutes() == before {
					t.Errorf("%s arm answered without the grouping-sets refusal firing; either "+
						"the DAG grew a real lowering (delete refuseGroupingSets and gate it) "+
						"or the query never reached PlanDistributed\n  SQL: %s", arm.name, tc.sql)
				}
			}
		})
	}

	// The control that must keep NOT routing: a plain GROUP BY over the same
	// columns is not a grouping-set query and has to stay on the DAG. Without
	// it a refusal widened by accident — every aggregate routed local — would
	// pass every assertion above.
	t.Run("ctl/a-plain-group-by-still-runs-on-the-dag", func(t *testing.T) {
		sql := "SELECT g AS k, h AS j, COUNT(*) AS n FROM collslot GROUP BY g, h ORDER BY k, j"
		for _, arm := range arms {
			if arm.coord == nil {
				continue
			}
			before := arm.coord.GroupingSetsLocalRoutes()
			res, err := arm.run(sql)
			if err != nil {
				t.Fatalf("%s arm refused the query: %v\n  SQL: %s", arm.name, err, sql)
			}
			if len(res.Rows) != 12 {
				t.Errorf("%s arm returned %d rows, PostgreSQL 17 answers 12\n  SQL: %s",
					arm.name, len(res.Rows), sql)
			}
			if arm.coord.GroupingSetsLocalRoutes() != before {
				t.Errorf("%s arm routed a PLAIN GROUP BY to the local pipeline — "+
					"refuseGroupingSets is firing on shapes the DAG executes correctly\n  SQL: %s",
					arm.name, sql)
			}
		}
	})
}
