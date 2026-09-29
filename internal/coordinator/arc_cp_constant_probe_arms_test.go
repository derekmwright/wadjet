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

// cpTables is the constant-probe fixture. cp_t is the JOINED table and the
// subquery's body table at once: the outer query reads id and g of it, the
// body reads v, which no outer scan reads — so a body that replays the outer
// scans' shared cache has no v, and the two readings differ. v is NULL-free
// (NOT IN / <> ALL decide on the value); cp_u carries a NULL for the
// UNION body and is the third relation of the three-way join.
func cpTables() []tmdTable {
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true} }
	t := tmdTable{name: "cp_t", schema: parquet.Schema{Columns: []parquet.Column{i64("id"), i64("g"), i64("v")}}}
	for i, v := range []int64{12, 13, 14, 15} {
		t.rows = append(t.rows, map[string]any{"id": int64(i + 1), "g": int64(i/2 + 1), "v": v})
	}
	u := tmdTable{name: "cp_u", schema: parquet.Schema{Columns: []parquet.Column{i64("id"), i64("w")}}}
	u.rows = []map[string]any{{"id": int64(1), "w": int64(12)}, {"id": int64(2), "w": int64(20)}, {"id": int64(3), "w": nil}}
	return []tmdTable{t, u}
}

// cpPGFixture is the same fixture as PostgreSQL DDL + INSERT.
const cpPGFixture = `DROP TABLE IF EXISTS cp_t; DROP TABLE IF EXISTS cp_u;
CREATE TABLE cp_t (id bigint, g bigint, v bigint);
INSERT INTO cp_t VALUES (1,1,12),(2,1,13),(3,2,14),(4,2,15);
CREATE TABLE cp_u (id bigint, w bigint);
INSERT INTO cp_u VALUES (1,12),(2,20),(3,NULL);`

// cpCell is one generated statement.
type cpCell struct{ name, sql string }

// cpProbe is one outer probe spelling: a constant (matching the body's 12,
// or 99 matching nothing) or an outer column (the controls).
type cpProbe struct{ name, match, miss string }

func cpProbes() []cpProbe {
	return []cpProbe{
		{"int", "12", "99"},
		{"quoted", "'12'", "'99'"},
		{"cast", "CAST('12' AS BIGINT)", "CAST('99' AS BIGINT)"},
		{"arith", "10 + 2", "90 + 9"},
	}
}

// cpBody is the body of one operator family over one body kind: in is the
// IN-family body (one column), ex the EXISTS body for probe P, sc a scalar
// body (one row, one column). {P} is the probe's place in ex.
type cpBody struct{ name, in, ex, sc string }

func cpBodies() []cpBody {
	return []cpBody{
		{"scan", "SELECT q.v FROM cp_t q", "SELECT 1 FROM cp_t q WHERE q.v = {P}", "SELECT min(q.v) FROM cp_t q"},
		{"filtered", "SELECT q.v FROM cp_t q WHERE q.id <= 2", "SELECT 1 FROM cp_t q WHERE q.v = {P} AND q.id <= 2", "SELECT q.v FROM cp_t q WHERE q.id = 1"},
		{"agg", "SELECT min(q.v) FROM cp_t q GROUP BY q.g", "SELECT 1 FROM cp_t q GROUP BY q.g HAVING min(q.v) = {P}", "SELECT min(q.v) FROM cp_t q WHERE q.g = 1"},
		{"union", "SELECT q.v FROM cp_t q UNION SELECT u.w FROM cp_u u", "SELECT q.v FROM cp_t q WHERE q.v = {P} UNION SELECT u.w FROM cp_u u WHERE u.w = {P}",
			"SELECT min(s.x) FROM (SELECT q.v FROM cp_t q UNION SELECT u.w FROM cp_u u) s(x)"},
		{"other", "SELECT u.w FROM cp_u u", "SELECT 1 FROM cp_u u WHERE u.w = {P}", "SELECT min(u.w) FROM cp_u u"},
	}
}

// cpOps are the eight predicate families over probe P and body b.
func cpOps() []struct {
	name string
	pred func(p string, b cpBody) string
} {
	return []struct {
		name string
		pred func(p string, b cpBody) string
	}{
		{"in", func(p string, b cpBody) string { return p + " IN (" + b.in + ")" }},
		{"notIn", func(p string, b cpBody) string { return p + " NOT IN (" + b.in + ")" }},
		{"eqAny", func(p string, b cpBody) string { return p + " = ANY (" + b.in + ")" }},
		{"neAll", func(p string, b cpBody) string { return p + " <> ALL (" + b.in + ")" }},
		{"exists", func(p string, b cpBody) string { return "EXISTS (" + strings.ReplaceAll(b.ex, "{P}", p) + ")" }},
		{"notExists", func(p string, b cpBody) string { return "NOT EXISTS (" + strings.ReplaceAll(b.ex, "{P}", p) + ")" }},
		{"scalarEq", func(p string, b cpBody) string { return p + " = (" + b.sc + ")" }},
		{"isNotNull", func(p string, b cpBody) string {
			return "(" + strings.ReplaceAll(strings.ReplaceAll(b.ex, "SELECT 1 FROM", "SELECT max(1) FROM"), "{P}", p) + ") IS NOT NULL"
		}},
	}
}

// cpPositions place predicate X over a join of cp_t with itself.
func cpPositions() []struct{ name, sql string } {
	const sel = "SELECT a.id, b.id FROM "
	return []struct{ name, sql string }{
		{"inner", sel + "cp_t a JOIN cp_t b ON a.id = b.id WHERE X"},
		{"left", sel + "cp_t a LEFT JOIN cp_t b ON a.id = b.id + 1 WHERE X"},
		{"right", sel + "cp_t a RIGHT JOIN cp_t b ON a.id + 1 = b.id WHERE X"},
		{"full", sel + "cp_t a FULL JOIN cp_t b ON a.id = b.id + 1 WHERE X"},
		{"onInner", sel + "cp_t a JOIN cp_t b ON a.id = b.id AND X"},
		{"onLeft", sel + "cp_t a LEFT JOIN cp_t b ON a.id = b.id AND X"},
		{"comma", sel + "cp_t a, cp_t b WHERE a.id = b.id AND X"},
		{"threeWay", "SELECT a.id, b.id, c.id FROM cp_t a JOIN cp_t b ON a.id = b.id JOIN cp_u c ON c.id = a.id WHERE X"},
		{"derived", "SELECT a.id, b.id FROM (SELECT id FROM cp_t) a JOIN (SELECT id FROM cp_t) b ON a.id = b.id WHERE X"},
		{"cte", "WITH j AS (SELECT a.id AS x, b.id AS y FROM cp_t a JOIN cp_t b ON a.id = b.id WHERE X) SELECT x, y FROM j"},
		{"andCol", sel + "cp_t a JOIN cp_t b ON a.id = b.id WHERE X AND a.id + b.id <= 6"},
		{"orCol", sel + "cp_t a JOIN cp_t b ON a.id = b.id WHERE (X OR a.id + b.id = 2)"},
		{"negated", sel + "cp_t a JOIN cp_t b ON a.id = b.id WHERE NOT (X)"},
		{"having", "SELECT a.g, count(*) FROM cp_t a JOIN cp_t b ON a.id = b.id GROUP BY a.g HAVING X"},
	}
}

// cpCells is the coverage table, generated:
//
//   - op/<op>/<probe>/<match|miss>: every predicate family × every constant
//     probe spelling, matching and missing, WHERE above an INNER self-join,
//     over the scan body;
//   - col/<op>/<left|right>: the same families with an outer COLUMN probe
//     (a.v / b.v; the EXISTS families correlate on it) — controls;
//   - pos/<position>/<op>/<match|miss>: every position × every family with
//     the integer probe;
//   - body/<body>/<op>/<match|miss>: every body kind × every family with the
//     integer probe above the INNER self-join (`other` reads cp_u, which no
//     outer scan shares — a control).
func cpCells() []cpCell {
	var out []cpCell
	scan := cpBodies()[0]
	inner := cpPositions()[0].sql
	at := func(pos, x string) string { return strings.Replace(pos, "X", x, 1) }
	for _, op := range cpOps() {
		for _, pr := range cpProbes() {
			out = append(out,
				cpCell{"op/" + op.name + "/" + pr.name + "/match", at(inner, op.pred(pr.match, scan))},
				cpCell{"op/" + op.name + "/" + pr.name + "/miss", at(inner, op.pred(pr.miss, scan))})
		}
	}
	for _, op := range cpOps() {
		for _, side := range []string{"left", "right"} {
			col := map[string]string{"left": "a.v", "right": "b.v"}[side]
			pos := strings.Replace(inner, "SELECT a.id, b.id", "SELECT a.id, b.id, a.v, b.v", 1)
			out = append(out, cpCell{"col/" + op.name + "/" + side, at(pos, op.pred(col, cpBodies()[1]))})
		}
	}
	for _, pos := range cpPositions() {
		for _, op := range cpOps() {
			out = append(out,
				cpCell{"pos/" + pos.name + "/" + op.name + "/match", at(pos.sql, op.pred("12", scan))},
				cpCell{"pos/" + pos.name + "/" + op.name + "/miss", at(pos.sql, op.pred("99", scan))})
		}
	}
	for _, b := range cpBodies() {
		for _, op := range cpOps() {
			out = append(out,
				cpCell{"body/" + b.name + "/" + op.name + "/match", at(inner, op.pred("12", b))},
				cpCell{"body/" + b.name + "/" + op.name + "/miss", at(inner, op.pred("99", b))})
		}
	}
	// The issues' own statements, over this fixture.
	out = append(out,
		cpCell{"issue/1382", "SELECT a.id, b.id FROM cp_t a JOIN cp_t b ON a.id = b.id WHERE CAST('12' AS BIGINT) IN (SELECT q.v FROM cp_t q)"},
		cpCell{"issue/1418", "SELECT a.id, b.id FROM cp_t a JOIN cp_t b ON a.id = b.id WHERE EXISTS (SELECT 1 FROM cp_t q WHERE q.v = 12)"},
		// The same cache, reached without a JOIN: the outer tree scans cp_t
		// twice through a decorrelated semi join, or a UNION ALL of it.
		cpCell{"nojoin/semi", "SELECT a.id FROM cp_t a WHERE a.id IN (SELECT id FROM cp_t) AND 12 IN (SELECT q.v FROM cp_t q)"},
		cpCell{"nojoin/unionAll", "SELECT u.id FROM (SELECT id FROM cp_t UNION ALL SELECT id FROM cp_t) u WHERE EXISTS (SELECT 1 FROM cp_t q WHERE q.v = 12)"},
		cpCell{"nojoin/selectList", "SELECT a.id, (SELECT min(q.v) FROM cp_t q WHERE q.v > 12) FROM cp_t a JOIN cp_t b ON a.id = b.id"},
	)
	// sel/<form>/<match|miss>: the predicate as a SELECT-list VALUE over the
	// self-join. At v0.25.2 the single-process arms and the DAG's local
	// fallback for a SELECT list no stage computes both replayed the cache:
	// IN / NOT IN answered NULL and CASE-over-IN 'n' on every arm (silent
	// wrong values), EXISTS and the filtered scalar failed.
	const selList = "SELECT a.id, X FROM cp_t a JOIN cp_t b ON a.id = b.id"
	for _, f := range []struct{ name, x, match string }{
		{"in", "P IN (SELECT q.v FROM cp_t q)", "12"},
		{"notIn", "P NOT IN (SELECT q.v FROM cp_t q)", "12"},
		{"caseIn", "CASE WHEN P IN (SELECT q.v FROM cp_t q) THEN 'y' ELSE 'n' END", "12"},
		{"exists", "EXISTS (SELECT 1 FROM cp_t q WHERE q.v = P)", "12"},
		{"scalarEq", "P = (SELECT min(q.v) FROM cp_t q WHERE q.v >= 12)", "12"},
		// cp_u's NULL member: NOT IN answers NULL where no member matches;
		// 13 is a member of cp_t only, so a body that lost cp_t's v answers
		// NULL where PostgreSQL answers false.
		{"nullNotIn", "P NOT IN (SELECT q.v FROM cp_t q UNION ALL SELECT u.w FROM cp_u u)", "13"},
	} {
		out = append(out,
			cpCell{"sel/" + f.name + "/match", at(selList, strings.ReplaceAll(f.x, "P", f.match))},
			cpCell{"sel/" + f.name + "/miss", at(selList, strings.ReplaceAll(f.x, "P", "99"))})
	}
	return out
}

func brRenderOrNil(res *oracle.Result) string {
	if res == nil {
		return "<nil>"
	}
	return brRender(res)
}

// cpPGAnswers reads PostgreSQL 17.11's answer for every generated cell
// (testdata/arc_cp_constant_probe_pg17.tsv).
func cpPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_cp_constant_probe_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
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
	return out
}

// TestArcCPGenerate dumps the cells as name<TAB>sql for the oracle run
// (CP_GEN=<path>); skipped otherwise.
func TestArcCPGenerate(t *testing.T) {
	path := os.Getenv("CP_GEN")
	if path == "" {
		t.Skip("CP_GEN unset")
	}
	var b strings.Builder
	for _, c := range cpCells() {
		b.WriteString(c.name + "\t" + c.sql + "\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// cpStandalone is one embedded engine over the fixture (budget 0 = none).
func cpStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range cpTables() {
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
	}
	return db
}

// cpArms is the five arms over the fixture.
func cpArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, cpTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := cpStandalone(t, ctx, 0), cpStandalone(t, ctx, 512*1024)
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

// cpRefused is the cell's LOUD refusal on one arm, or "" where the arm
// answers as PostgreSQL does. Two refusals stand, each outside this table's
// seam (the duplicate-scan cache) and each a pin that fails the moment the
// arm starts answering:
//
//   - pos/onLeft/ — an uncorrelated subquery as an ON conjunct of a LEFT
//     JOIN, every arm: the join's residual compiler has no subquery runner
//     for an outer join's ON (PostgreSQL answers).
//   - DAG arms only — a subquery predicate the stage planner does not
//     resolve before dispatch (a scalar subquery under IS NOT NULL; an IN /
//     ANY / ALL / scalar comparison under OR or NOT) reaches a worker's
//     filter, which has no subquery runner (PostgreSQL and the single-process
//     arms answer).
func cpRefused(name, arm string) string {
	if strings.HasPrefix(name, "pos/onLeft/") {
		return "on a left join is not evaluable at the join"
	}
	if !strings.HasPrefix(arm, "dag") {
		return ""
	}
	parts := strings.Split(name, "/")
	if len(parts) >= 3 && (parts[0] == "op" || parts[0] == "pos" || parts[0] == "body") {
		op := parts[1]
		if parts[0] != "op" {
			op = parts[2]
		}
		underOrNot := parts[0] == "pos" && (parts[1] == "orCol" || parts[1] == "negated")
		if op == "isNotNull" || (underOrNot && op != "exists" && op != "notExists") {
			return "a SubqueryRunner"
		}
	}
	return ""
}

// AN UNCORRELATED SUBQUERY PREDICATE WHOSE PROBE NAMES NO OUTER COLUMN
// ANSWERS AS POSTGRESQL DOES OVER A JOIN, ON EVERY ARM (#1382 #1418). The
// generated coverage table (cpCells) over cp_t joined with itself, the body
// reading a column no outer scan reads, against PostgreSQL 17.11's full
// sorted rows for every cell. At v0.25.2 the single-process arms replayed
// the outer self-join's duplicate-scan cache for the body's own scan of
// cp_t — a cache holding only the outer's columns — so every matching
// membership answered 0 rows (or every row, negated), every EXISTS body with
// a filter failed with `filter column "q.v" does not exist`, and the DAG
// arms answered.
func TestArcCPConstantProbeEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the constant-probe table")
	}
	answers := cpPGAnswers(t)
	cells := cpCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := cpArms(t, ctx)
	nonEmpty, refusals := 0, 0
	for _, tc := range cells {
		want := answers[tc.name]
		if !strings.HasPrefix(want, "rows=0") && !strings.HasPrefix(want, "ERR") {
			nonEmpty++
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
				if pin := cpRefused(tc.name, arm.name); pin != "" {
					refusals++
					if err == nil || !strings.Contains(err.Error(), pin) {
						t.Errorf("%s\n  arm  %s\n  got  %v %v\n  want the loud refusal %q (PostgreSQL 17.11: %s) — an arm that answers deletes this pin",
							tc.sql, arm.name, brRenderOrNil(res), err, pin, want)
					}
					continue
				}
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					state, msg, _ := strings.Cut(rest, " ")
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (PostgreSQL 17.11)", tc.sql, arm.name, brRender(res), state, msg)
					} else if st := sqlerr.StateOf(err); st != state || !strings.Contains(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q", tc.sql, arm.name, st, err, state, msg)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, err, want)
					continue
				}
				if got := brRender(res); got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, want)
				}
			}
		})
	}
	if len(cells) < 250 || nonEmpty < 120 || refusals != 16*5+64*3 {
		t.Fatalf("%d cells, %d with rows, %d pinned refusals: the table must discriminate", len(cells), nonEmpty, refusals)
	}
}
