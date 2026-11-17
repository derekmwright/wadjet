// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A COMPUTED SECOND OR THIRD AGGREGATE ARGUMENT, ON ALL FIVE ARMS (#1694).
//
// The embedded engine materializes every aggregate argument that is not a
// bare column below the aggregate (physical buildAggregate), whatever its
// position. At base b0e0f116 only the first was: a computed later argument was
// refused ungrouped (`aggregate input "..." is not a column of its input`) and
// answered NO ROWS grouped, because the partitioned aggregate's drain dropped
// the refusal. Both moved on the single and spilled arms, and the ROLLUP cell
// moved on the stage-DAG arms too (it answers there at the tip and was refused
// at base).
//
// The stage DAG ships aggregate inputs its own way: the worker's
// pre-aggregate projection materializes the first argument and passes later
// ones through only as bare columns (docs/internals/
// worker-aggregate-extra-argument-carriage.md). A computed later argument is
// therefore still REFUSED there, loudly and at base too, and so is the same
// call over a subquery that projects the argument first: those cells are a
// residual pinned as refusals, not answers ("dagRefuses"). No arm answers any
// cell wrongly or with no rows.
//
// Table pt1694 is the issue's: (1,1,10,100,t1), (2,1,20,NULL,t2),
// (3,2,NULL,300,t3), (4,2,40,400,NULL), (5,1,50,500,t5) over (id, g, x, y,
// t). MIN_BY, MAX_BY and OHLCV have no PostgreSQL counterpart, so each want is
// the same call over the projected subquery, which is also run and compared
// on each embedded arm; CORR's want is also PostgreSQL 17.11's.
func acTable() tmdTable {
	ts := func(s int) time.Time { return time.Date(2024, 1, 1, 0, 0, s, 0, time.UTC) }
	return tmdTable{
		name: "pt1694",
		schema: parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt32},
			{Name: "g", Type: parquet.TypeInt32, Nullable: true},
			{Name: "x", Type: parquet.TypeInt32, Nullable: true},
			{Name: "y", Type: parquet.TypeInt32, Nullable: true},
			{Name: "t", Type: parquet.TypeTimestamp, Nullable: true},
		}},
		rows: []map[string]any{
			{"id": int32(1), "g": int32(1), "x": int32(10), "y": int32(100), "t": ts(1)},
			{"id": int32(2), "g": int32(1), "x": int32(20), "y": nil, "t": ts(2)},
			{"id": int32(3), "g": int32(2), "x": nil, "y": int32(300), "t": ts(3)},
			{"id": int32(4), "g": int32(2), "x": int32(40), "y": int32(400), "t": nil},
			{"id": int32(5), "g": int32(1), "x": int32(50), "y": int32(500), "t": ts(5)},
		},
	}
}

type acArm struct {
	name string
	run  func(string) (*oracle.Result, []parquet.Column, error)
}

func acArms(t *testing.T, ctx context.Context) []acArm {
	t.Helper()
	tbl := acTable()
	open := func(budget int64) *wadjet.DB {
		db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test",
			MemoryBudget: budget, SpillDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		for _, s := range []string{
			"CREATE TABLE pt1694 (id INT, g INT, x INT, y INT, t TIMESTAMP)",
			"INSERT INTO pt1694 VALUES (1,1,10,100,TIMESTAMP '2024-01-01 00:00:01'),(2,1,20,NULL,TIMESTAMP '2024-01-01 00:00:02')," +
				"(3,2,NULL,300,TIMESTAMP '2024-01-01 00:00:03'),(4,2,40,400,NULL),(5,1,50,500,TIMESTAMP '2024-01-01 00:00:05')",
		} {
			if _, err := db.Query(ctx, s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		return db
	}
	single, spilled := open(0), open(512*1024)
	dag := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, []tmdTable{tbl})
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := dag(nil)
	coordB := dag(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := dag(func(w *worker.Config) { w.MorselWorkers = 4 })
	runSingle := func(db *wadjet.DB, drain bool) func(string) (*oracle.Result, []parquet.Column, error) {
		return func(sql string) (*oracle.Result, []parquet.Column, error) {
			if drain {
				prev := exec.ForceAggDrainEvery(1)
				defer exec.ForceAggDrainEvery(prev)
			}
			res, err := tmdRunSingle(ctx, db, sql)
			if err != nil {
				return nil, nil, err
			}
			cols, err := tmdRunSingleDeclAll(ctx, db, sql)
			return res, cols, err
		}
	}
	runDAG := func(c *Coordinator) func(string) (*oracle.Result, []parquet.Column, error) {
		return func(sql string) (*oracle.Result, []parquet.Column, error) {
			res, err := tmdRunDAG(ctx, c, sql)
			if err != nil {
				return nil, nil, err
			}
			cols, err := tmdRunDAGDeclAll(ctx, c, sql)
			return res, cols, err
		}
	}
	return []acArm{
		{"single", runSingle(single, false)},
		{"spilled", runSingle(spilled, true)},
		{"dag", runDAG(coord)},
		{"dag-shuffled", runDAG(coordB)},
		{"dag-morsel4", runDAG(coordM)},
	}
}

func acDecl(cols []parquet.Column) string {
	var s []string
	for _, c := range cols {
		s = append(s, c.Type.String())
	}
	return strings.Join(s, ",")
}

func TestAggComputedArgumentsFiveArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := acArms(t, ctx)
	const notAColumn = "is not a column of its input"
	cells := []struct {
		name, sql, control, want, decl string
		dagRefuses                     bool
	}{
		{"maxByCaseGrouped",
			"SELECT g, max_by(x, CASE WHEN id IN (2,4) THEN y END) FROM pt1694 GROUP BY g",
			"SELECT g, max_by(x, e) FROM (SELECT g, x, CASE WHEN id IN (2,4) THEN y END AS e FROM pt1694) q GROUP BY g",
			"rows=2 1,NULL | 2,40", "INT32,INT32", true},
		{"minByCaseUngrouped",
			"SELECT min_by(x, CASE WHEN id > 1 THEN y END) FROM pt1694",
			"SELECT min_by(x, e) FROM (SELECT x, CASE WHEN id > 1 THEN y END AS e FROM pt1694) q",
			"rows=1 40", "INT32", true},
		{"corrArithGrouped",
			"SELECT g, corr(x, y + 0) FROM pt1694 GROUP BY g",
			"SELECT g, corr(x, e) FROM (SELECT g, x, y + 0 AS e FROM pt1694) q GROUP BY g",
			"rows=2 1,1 | 2,NULL", "INT32,FLOAT64", true},
		{"ohlcvComputedVolume",
			"SELECT ohlcv(t, x, y * 2) FROM pt1694",
			"SELECT ohlcv(t, x, e) FROM (SELECT t, x, y * 2 AS e FROM pt1694) q",
			"rows=1 {close:50 high:50 low:10 open:10 volume:1200 vwap:43.3333}", "ROW", true},
		{"ohlcvComputedTsRollup",
			"SELECT g, ohlcv(t + INTERVAL '1 second', x, y) FROM pt1694 GROUP BY ROLLUP(g)",
			"SELECT g, ohlcv(e, x, y) FROM (SELECT g, t + INTERVAL '1 second' AS e, x, y FROM pt1694) q GROUP BY ROLLUP(g)",
			"rows=3 1,{close:50 high:50 low:10 open:10 volume:600 vwap:43.3333} | 2,NULL | " +
				"NULL,{close:50 high:50 low:10 open:10 volume:600 vwap:43.3333}", "INT32,ROW", false},
		{"maxByBothComputedHaving",
			"SELECT g, max_by(x + 1, abs(y - 300)) FROM pt1694 GROUP BY g HAVING count(*) > 1",
			"SELECT g, max_by(a, b) FROM (SELECT g, id, x + 1 AS a, abs(y - 300) AS b FROM pt1694) q GROUP BY g HAVING count(*) > 1",
			"rows=2 1,11 | 2,41", "INT32,INT64", true},
		// The DAG names a computed first argument by its text, so a later
		// argument spelled the same way finds it: this one answers on every
		// arm, and answered no rows on the embedded arms at base.
		{"sharedExpression",
			"SELECT g, sum(y + 1), max_by(x, y + 1) FROM pt1694 GROUP BY g", "",
			"rows=2 1,602,50 | 2,702,40", "INT32,INT64,INT32", false},
		// Must not move: single-argument aggregates over a computed argument
		// (#1558's NULL propagation among them) and bare-column multi-argument
		// aggregates.
		{"keepSingleArgComputed",
			"SELECT sum(x + 1), avg(CASE WHEN id > 1 THEN x END) FROM pt1694", "",
			"rows=1 124,36.6667", "INT64,DECIMAL", false},
		{"keepNullPropagation",
			"SELECT g, sum(x + NULL), sum(x + 1) FROM pt1694 GROUP BY g", "",
			"rows=2 1,NULL,83 | 2,NULL,41", "INT32,FLOAT64,INT64", false},
		{"keepBareColumns",
			"SELECT g, min_by(x, y), max_by(x, y), corr(x, y) FROM pt1694 GROUP BY g", "",
			"rows=2 1,10,50,1 | 2,40,40,NULL", "INT32,INT32,INT32,FLOAT64", false},
	}
	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				dag := strings.HasPrefix(arm.name, "dag")
				res, cols, err := arm.run(tc.sql)
				if dag && tc.dagRefuses {
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  answered %s; the stage DAG refused this at base and does not materialize the argument",
							tc.sql, arm.name, afRender(res))
					} else if !strings.Contains(err.Error(), notAColumn) {
						t.Errorf("%s\n  arm  %s\n  refused %v, want %q", tc.sql, arm.name, err, notAColumn)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v", tc.sql, arm.name, err)
					continue
				}
				if got := afRender(res); got != tc.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s", tc.sql, arm.name, got, tc.want)
				}
				if got := acDecl(cols); got != tc.decl {
					t.Errorf("%s\n  arm  %s\n  declared %s, want %s", tc.sql, arm.name, got, tc.decl)
				}
				if tc.control == "" || dag {
					continue
				}
				cres, ccols, err := arm.run(tc.control)
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  control refused: %v", tc.control, arm.name, err)
					continue
				}
				if c := afRender(cres); c != tc.want || acDecl(ccols) != tc.decl {
					t.Errorf("arm %s: the projected subquery answers %s %s\n  %s", arm.name, c, acDecl(ccols), tc.control)
				}
			}
		})
	}
}
