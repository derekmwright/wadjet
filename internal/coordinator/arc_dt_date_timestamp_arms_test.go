// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// dtTable is the `dt_pair` fixture: a DATE and a TIMESTAMP per row, built so
// PostgreSQL's cross-type rule (the DATE promoted to a midnight TIMESTAMP)
// and every other reading answer differently —
//
//	id | d          | ts
//	1  | 2024-01-02 | 2024-01-02 00:00:00   the midnight twin of its own date
//	2  | 2024-03-04 | 2024-03-04 12:00:00   same calendar day, NOT midnight
//	3  | 2024-05-06 | 2024-01-02 00:00:00   the midnight twin of row 1's date
//	4  | NULL       | NULL
//
// so a reading that truncates the TIMESTAMP to its day matches row 2, a
// reading that compares day counts with milliseconds matches nothing, and
// only the promotion matches rows 1 and 3 and never row 2.
func dtTable() tmdTable {
	ms := func(s string) int64 {
		tm, err := time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			panic(err)
		}
		return tm.UnixMilli()
	}
	return tmdTable{
		name: "dt_pair",
		schema: parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "d", Type: parquet.TypeDate, Nullable: true},
			{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		}},
		rows: []map[string]any{
			{"id": int64(1), "d": "2024-01-02", "ts": ms("2024-01-02 00:00:00")},
			{"id": int64(2), "d": "2024-03-04", "ts": ms("2024-03-04 12:00:00")},
			{"id": int64(3), "d": "2024-05-06", "ts": ms("2024-01-02 00:00:00")},
			{"id": int64(4), "d": nil, "ts": nil},
		},
	}
}

// dtPGFixture is the same fixture as PostgreSQL DDL, for the oracle run.
const dtPGFixture = `DROP TABLE IF EXISTS dt_pair;
CREATE TABLE dt_pair (id bigint, d date, ts timestamp);
INSERT INTO dt_pair VALUES (1,'2024-01-02','2024-01-02 00:00:00'),(2,'2024-03-04','2024-03-04 12:00:00'),(3,'2024-05-06','2024-01-02 00:00:00'),(4,NULL,NULL);`

// dtStandalone is one embedded engine over `dt_pair` (budget 0 = none).
func dtStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	tbl := dtTable()
	if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
		t.Fatalf("create %s: %v", tbl.name, err)
	}
	ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
	if err := ing.Ingest(ctx, tbl.rows); err != nil {
		t.Fatalf("ingest %s: %v", tbl.name, err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush %s: %v", tbl.name, err)
	}
	return db
}

// dtArms is the five arms over `dt_pair`: one process, one process under a
// 512 KiB budget, the DAG, the DAG with every join shuffled (the build is
// hash-PARTITIONED by its key, so both sides' keys must hash alike), and the
// DAG with four morsel workers.
func dtArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	single := dtStandalone(t, ctx, 0)
	spilled := dtStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, []tmdTable{dtTable()})
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	runSingle := func(db *wadjet.DB) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, db, sql) }
	}
	runDAG := func(c *Coordinator) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, c, sql) }
	}
	return []brArm{
		{"single", runSingle(single)},
		{"spilled512k", runSingle(spilled)},
		{"dag", runDAG(coord)},
		{"dag-shuffled", runDAG(coordB)},
		{"dag-morsel4", runDAG(coordM)},
	}
}

// dtCell is one generated cell; its SQL is PostgreSQL's spelling too.
type dtCell struct{ name, sql string }

// dtSide is one direction of the pair: an outer operand of type `outer`
// against a body selecting the other type.
type dtSide struct {
	name   string
	outers [][2]string // name, expression over a
	bodies [][2]string // name, subquery body (may reference a)
}

// dtSides: a DATE outer against a TIMESTAMP body, and a TIMESTAMP outer
// against a DATE body. Each body holds a member that matches only under the
// promotion, and the non-midnight 2024-03-04 12:00:00 wherever the reading
// that truncates would match it.
func dtSides() []dtSide {
	return []dtSide{
		{
			name: "dateOuter",
			outers: [][2]string{
				{"col", "a.d"},
				{"typedLit", "DATE '2024-01-02'"},
				{"castExpr", "CAST(a.ts AS DATE)"},
			},
			bodies: [][2]string{
				{"col", "SELECT r.ts FROM dt_pair r"},
				{"castCol", "SELECT CAST(r.d AS TIMESTAMP) FROM dt_pair r"},
				{"lit", "SELECT TIMESTAMP '2024-01-02 00:00:00' FROM dt_pair r WHERE r.id = 1"},
				{"fromless", "SELECT TIMESTAMP '2024-01-02 00:00:00'"},
				{"unionAllLits", "SELECT TIMESTAMP '2024-01-02 00:00:00' UNION ALL SELECT TIMESTAMP '2024-03-04 12:00:00'"},
				{"unionCols", "SELECT r.ts FROM dt_pair r WHERE r.id = 1 UNION SELECT r.ts FROM dt_pair r WHERE r.id = 2"},
				{"nonMidnight", "SELECT r.ts FROM dt_pair r WHERE r.id = 2"},
				{"nullMember", "SELECT r.ts FROM dt_pair r WHERE r.id IN (3, 4)"},
				{"correlated", "SELECT r.ts FROM dt_pair r WHERE r.id = a.id"},
				{"derived", "SELECT x.ts FROM (SELECT r.ts FROM dt_pair r WHERE r.id <= 3) x"},
				{"grouped", "SELECT max(r.ts) AS m FROM dt_pair r GROUP BY r.id"},
			},
		},
		{
			name: "tsOuter",
			outers: [][2]string{
				{"col", "a.ts"},
				{"typedLit", "TIMESTAMP '2024-01-02 00:00:00'"},
				{"castExpr", "CAST(a.d AS TIMESTAMP)"},
			},
			bodies: [][2]string{
				{"col", "SELECT r.d FROM dt_pair r"},
				{"castCol", "SELECT CAST(r.ts AS DATE) FROM dt_pair r"},
				{"lit", "SELECT DATE '2024-03-04' FROM dt_pair r WHERE r.id = 1"},
				{"fromless", "SELECT DATE '2024-01-02'"},
				{"unionAllLits", "SELECT DATE '2024-01-02' UNION ALL SELECT DATE '2024-03-04'"},
				{"unionCols", "SELECT r.d FROM dt_pair r WHERE r.id = 1 UNION SELECT r.d FROM dt_pair r WHERE r.id = 2"},
				{"nullMember", "SELECT r.d FROM dt_pair r WHERE r.id IN (1, 4)"},
				{"correlated", "SELECT r.d FROM dt_pair r WHERE r.id = a.id"},
				{"derived", "SELECT x.d FROM (SELECT r.d FROM dt_pair r WHERE r.id <= 3) x"},
				{"grouped", "SELECT max(r.d) AS m FROM dt_pair r GROUP BY r.id"},
			},
		},
	}
}

// dtMemberCells is the membership table: IN / NOT IN / = ANY / <> ALL, every
// outer against every body, both directions.
func dtMemberCells() []dtCell {
	var out []dtCell
	for _, side := range dtSides() {
		for _, op := range smMemberOps() {
			for _, o := range side.outers {
				for _, b := range side.bodies {
					out = append(out, dtCell{
						name: "member/" + side.name + "/" + op.name + "/" + o[0] + "/" + b[0],
						sql:  "SELECT a.id FROM dt_pair a WHERE " + op.pred(o[1], b[1]),
					})
				}
			}
		}
	}
	return out
}

// dtOtherCells are the other carriers of the pair: the correlated equality
// key (EXISTS, NOT EXISTS, LATERAL), a scalar subquery, the direct comparison
// (the control: it already promotes at base), a JOIN key of every kind, a
// set operation mixing the two, and CASE / COALESCE / GREATEST / LEAST /
// NULLIF over both.
func dtOtherCells() []dtCell {
	const w = "SELECT a.id FROM dt_pair a WHERE "
	c := func(name, sql string) dtCell { return dtCell{name: name, sql: sql} }
	return []dtCell{
		c("key/exists/tsEqOuterD", w+"EXISTS (SELECT 1 FROM dt_pair r WHERE r.ts = a.d)"),
		c("key/exists/outerDEqTs", w+"EXISTS (SELECT 1 FROM dt_pair r WHERE a.d = r.ts)"),
		c("key/exists/dEqOuterTs", w+"EXISTS (SELECT 1 FROM dt_pair r WHERE r.d = a.ts)"),
		c("key/exists/withFilter", w+"EXISTS (SELECT 1 FROM dt_pair r WHERE r.ts = a.d AND r.id <> a.id)"),
		c("key/notExists/tsEqOuterD", w+"NOT EXISTS (SELECT 1 FROM dt_pair r WHERE r.ts = a.d)"),
		c("key/notExists/dEqOuterTs", w+"NOT EXISTS (SELECT 1 FROM dt_pair r WHERE r.d = a.ts)"),
		c("key/lateral/dOuter", "SELECT a.id, l.rid FROM dt_pair a, LATERAL (SELECT r.id AS rid FROM dt_pair r WHERE r.ts = a.d) l"),
		c("key/lateral/tsOuter", "SELECT a.id, l.rid FROM dt_pair a, LATERAL (SELECT r.id AS rid FROM dt_pair r WHERE r.d = a.ts) l"),

		c("scalar/dEqTs", w+"a.d = (SELECT r.ts FROM dt_pair r WHERE r.id = 3)"),
		c("scalar/tsEqD", w+"a.ts = (SELECT r.d FROM dt_pair r WHERE r.id = 1)"),
		c("scalar/tsEqDRow2", w+"a.ts = (SELECT r.d FROM dt_pair r WHERE r.id = 2)"),
		c("scalar/dLtTs", w+"a.d < (SELECT r.ts FROM dt_pair r WHERE r.id = 2)"),
		c("scalar/correlated", w+"a.d = (SELECT max(r.ts) FROM dt_pair r WHERE r.id = a.id)"),
		c("scalar/aggEq", w+"a.d = (SELECT min(r.ts) FROM dt_pair r)"),
		c("scalar/aggLt", w+"a.d < (SELECT max(r.ts) FROM dt_pair r)"),
		c("scalar/aggTsEqD", w+"a.ts = (SELECT min(r.d) FROM dt_pair r)"),

		c("direct/eq", w+"a.d = a.ts"),
		c("direct/ne", w+"a.d <> a.ts"),
		c("direct/lt", w+"a.d < a.ts"),
		c("direct/ge", w+"a.d >= a.ts"),
		c("direct/tsGtD", w+"a.ts > a.d"),
		c("direct/tsBetweenD", w+"a.ts BETWEEN a.d AND a.d"),
		c("direct/dBetweenTs", w+"a.d BETWEEN a.ts AND a.ts"),
		c("direct/dEqTsLit", w+"a.d = TIMESTAMP '2024-01-02 00:00:00'"),
		c("direct/dEqTsLitNonMidnight", w+"a.d = TIMESTAMP '2024-03-04 12:00:00'"),
		c("direct/tsEqDLit", w+"a.ts = DATE '2024-01-02'"),
		c("direct/tsEqDLitRow2", w+"a.ts = DATE '2024-03-04'"),
		c("direct/dInTsList", w+"a.d IN (TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-03-04 12:00:00')"),
		c("direct/dNotInTsList", w+"a.d NOT IN (TIMESTAMP '2024-01-02 00:00:00', TIMESTAMP '2024-03-04 12:00:00')"),
		c("direct/tsInDList", w+"a.ts IN (DATE '2024-01-02', DATE '2024-03-04')"),
		c("direct/project", "SELECT a.id, CASE WHEN a.d = a.ts THEN 1 WHEN NOT (a.d = a.ts) THEN 0 END AS eq FROM dt_pair a"),

		c("join/inner/dEqTs", "SELECT a.id, r.id FROM dt_pair a JOIN dt_pair r ON a.d = r.ts"),
		c("join/inner/tsEqD", "SELECT a.id, r.id FROM dt_pair a JOIN dt_pair r ON a.ts = r.d"),
		c("join/inner/extraCond", "SELECT a.id, r.id FROM dt_pair a JOIN dt_pair r ON a.d = r.ts AND a.id <> r.id"),
		c("join/left/dEqTs", "SELECT a.id, r.id FROM dt_pair a LEFT JOIN dt_pair r ON a.d = r.ts"),
		c("join/right/dEqTs", "SELECT a.id, r.id FROM dt_pair a RIGHT JOIN dt_pair r ON a.d = r.ts"),
		c("join/full/dEqTs", "SELECT a.id, r.id FROM dt_pair a FULL JOIN dt_pair r ON a.d = r.ts"),
		c("join/comma/dEqTs", "SELECT a.id, r.id FROM dt_pair a, dt_pair r WHERE a.d = r.ts"),
		c("join/count", "SELECT count(*) AS n FROM dt_pair a JOIN dt_pair r ON r.ts = a.d"),

		c("setop/groupUnionAll", "SELECT v, count(*) AS n FROM (SELECT d AS v FROM dt_pair UNION ALL SELECT ts FROM dt_pair) u GROUP BY v"),
		c("setop/union", "SELECT v FROM (SELECT d AS v FROM dt_pair UNION SELECT ts FROM dt_pair) u"),
		c("setop/unionTsFirst", "SELECT v FROM (SELECT ts AS v FROM dt_pair UNION SELECT d FROM dt_pair) u"),
		c("setop/distinctUnionAll", "SELECT DISTINCT v FROM (SELECT d AS v FROM dt_pair UNION ALL SELECT ts FROM dt_pair) u"),
		c("setop/intersect", "SELECT d FROM dt_pair INTERSECT SELECT ts FROM dt_pair"),
		c("setop/except", "SELECT d FROM dt_pair EXCEPT SELECT ts FROM dt_pair"),
		c("setop/memberMixedBody", w+"a.d IN (SELECT r.d FROM dt_pair r WHERE r.id = 3 UNION ALL SELECT r.ts FROM dt_pair r WHERE r.id = 1)"),

		c("cond/caseDFirst", "SELECT a.id, CASE WHEN a.id = 1 THEN a.d ELSE a.ts END AS v FROM dt_pair a"),
		c("cond/caseTsFirst", "SELECT a.id, CASE WHEN a.id = 1 THEN a.ts ELSE a.d END AS v FROM dt_pair a"),
		c("cond/coalesceDTs", "SELECT a.id, COALESCE(a.d, a.ts) AS v FROM dt_pair a"),
		c("cond/coalesceTsD", "SELECT a.id, COALESCE(a.ts, a.d) AS v FROM dt_pair a"),
		c("cond/coalesceNullD", "SELECT a.id, COALESCE(CASE WHEN a.id = 2 THEN NULL ELSE a.d END, a.ts) AS v FROM dt_pair a"),
		c("cond/greatest", "SELECT a.id, GREATEST(a.d, a.ts) AS v FROM dt_pair a"),
		c("cond/least", "SELECT a.id, LEAST(a.d, a.ts) AS v FROM dt_pair a"),
		c("cond/nullif", "SELECT a.id, NULLIF(a.d, a.ts) AS v FROM dt_pair a"),
		c("cond/caseInWhere", w+"CASE WHEN a.id = 1 THEN a.d ELSE a.ts END = TIMESTAMP '2024-01-02 00:00:00'"),

		c("issue/1378/castBody", "SELECT count(*) AS n FROM dt_pair a WHERE a.d IN (SELECT CAST(r.d AS TIMESTAMP) FROM dt_pair r)"),
		c("issue/1378/fromless", "SELECT count(*) AS n FROM dt_pair a WHERE a.d IN (SELECT TIMESTAMP '2024-01-02 00:00:00' UNION ALL SELECT TIMESTAMP '2024-03-04 00:00:00')"),
	}
}

// dtKeptRefusals are the cells this engine refuses where PostgreSQL 17.11
// answers — each a catalogued divergence (docs/adr/0012-divergences/
// temporal.md) with its mechanism, asserted on every arm so a refusal that
// starts answering FAILS here and is re-measured against the row it
// replaces:
//
//   - setop/: a set operation whose column is DATE in one arm and TIMESTAMP
//     in another has no common carrier (physical.setOpCarrierGap); PostgreSQL
//     types it timestamp.
//   - cond/: CASE / COALESCE / GREATEST / LEAST whose arms are DATE and
//     TIMESTAMP (physical.temporalFoldGap); at v0.25.2 they answered a day
//     count in a TIMESTAMP column, or 22003.
func dtKeptRefusals() map[string][2]string {
	gap := [2]string{"0A000", "no common carrier for that pair"}
	fold := [2]string{"0A000", "does not yet carry a DATE arm into a TIMESTAMP result"}
	return map[string][2]string{
		"setop/groupUnionAll":    gap,
		"setop/union":            gap,
		"setop/unionTsFirst":     gap,
		"setop/distinctUnionAll": gap,
		"setop/intersect":        gap,
		"setop/except":           gap,
		"setop/memberMixedBody":  gap,
		"cond/caseDFirst":        fold,
		"cond/caseTsFirst":       fold,
		"cond/coalesceDTs":       fold,
		"cond/coalesceTsD":       fold,
		"cond/coalesceNullD":     fold,
		"cond/greatest":          fold,
		"cond/least":             fold,
		"cond/caseInWhere":       fold,
	}
}

// dtCells is the whole generated table.
func dtCells() []dtCell {
	return append(dtMemberCells(), dtOtherCells()...)
}

// dtPGAnswers reads PostgreSQL 17.11's answer for every cell, measured once
// over dtPGFixture (testdata/arc_dt_date_timestamp_pg17.tsv).
func dtPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_dt_date_timestamp_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestArcDTGenerate dumps the cells as name<TAB>sql for the oracle run
// (DT_GEN=<path>); skipped otherwise.
func TestArcDTGenerate(t *testing.T) {
	path := os.Getenv("DT_GEN")
	if path == "" {
		t.Skip("DT_GEN unset")
	}
	var b strings.Builder
	for _, c := range dtCells() {
		b.WriteString(c.name + "\t" + c.sql + "\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A DATE MEETS A TIMESTAMP AS POSTGRESQL'S CROSS-TYPE OPERATORS MEET IT: the
// DATE is promoted to the TIMESTAMP at its midnight and the two instants are
// compared (#1378). At v0.25.2 the DIRECT comparison already promoted, and
// every carrier that turns the pair into a KEY did not: a membership answered
// 0 rows on every arm (the semi join keyed a day count against milliseconds,
// the IN set held members of the other type), NOT IN kept the rows it should
// have removed, the EXISTS key and the JOIN key matched nothing. Every cell
// on five arms against PostgreSQL 17.11's full sorted rows.
func TestArcDTDateTimestampEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the DATE / TIMESTAMP pair table")
	}
	answers := dtPGAnswers(t)
	cells := dtCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := dtArms(t, ctx)
	kept := dtKeptRefusals()
	for _, tc := range cells {
		want := answers[tc.name]
		state, msg := "", ""
		if rest, ok := strings.CutPrefix(want, "ERR "); ok {
			state, msg, _ = strings.Cut(rest, " ")
		}
		if k, ok := kept[tc.name]; ok {
			if state != "" {
				t.Fatalf("cell %s is a kept refusal but PostgreSQL refuses it too: %s", tc.name, want)
			}
			state, msg = k[0], k[1]
		}
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				res *oracle.Result
				err error
			}
			results := make([]result, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i].res, results[i].err = arm.run(tc.sql)
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				res, err := results[i].res, results[i].err
				if state != "" {
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (PostgreSQL 17.11)", tc.sql, arm.name, brRender(res), state, msg)
						continue
					}
					if st := sqlerr.StateOf(err); st != state || !strings.Contains(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q", tc.sql, arm.name, st, err, state, msg)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, err, want)
					continue
				}
				if got := brRender(res); strings.TrimSpace(got) != strings.TrimSpace(want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, want)
				}
			}
		})
	}
}
