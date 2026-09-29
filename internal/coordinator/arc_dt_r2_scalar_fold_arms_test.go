// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
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

// dtr2MS is a wall-clock literal's epoch milliseconds.
func dtr2MS(s string) int64 {
	for _, layout := range []string{"2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
		if tm, err := time.Parse(layout, s); err == nil {
			return tm.UnixMilli()
		}
	}
	panic(s)
}

// dtr2Tables is round 2's fixture beside dt_pair (dtTable):
//
//   - dtb_t / dtb_d, the EPOCH BAND: TIMESTAMPs at -500 001, -500 000, -1,
//     0, 1, 499 999 and 500 000 epoch milliseconds and 1969-12-31 00:00, the
//     DATEs 1969-12-31 and 1970-01-01. compare()'s magnitude guess read an
//     int64 inside +/-500 000 as a DAY count, so every TIMESTAMP inside the
//     band except 0 names a different day under that reading (-1 ms is day
//     -1, 1969-12-31; 1 ms is day 1, 1970-01-02), and the two outside it are
//     the controls.
//   - dr_d / dr_t, round 1's review fixture: pre-1970, 1000-01-01,
//     9999-12-31, duplicates and NULLs on both sides.
func dtr2Tables() []tmdTable {
	idTS := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}
	idD := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
	}}
	var bt, bd, rt, rd []map[string]any
	for i, ms := range []any{int64(-500_001), int64(-500_000), int64(-1), int64(0), int64(1), int64(499_999), int64(500_000), int64(-86_400_000), nil} {
		bt = append(bt, map[string]any{"id": int64(i + 1), "ts": ms})
	}
	for i, d := range []any{"1969-12-31", "1970-01-01", nil} {
		bd = append(bd, map[string]any{"id": int64(i + 1), "d": d})
	}
	for i, d := range dtr2ReviewDates {
		rd = append(rd, map[string]any{"id": int64(i + 1), "d": d})
	}
	for i, s := range dtr2ReviewStamps {
		var v any
		if s != nil {
			v = dtr2MS(s.(string))
		}
		rt = append(rt, map[string]any{"id": int64(i + 1), "ts": v})
	}
	return []tmdTable{dtTable(), {"dtb_t", idTS, bt}, {"dtb_d", idD, bd}, {"dr_t", idTS, rt}, {"dr_d", idD, rd}}
}

var (
	dtr2ReviewDates  = []any{"2024-01-02", "2024-03-04", "1969-12-31", "1970-01-01", "1900-01-01", "1000-01-01", nil, "2024-01-02", "9999-12-31", "1969-12-30"}
	dtr2ReviewStamps = []any{"2024-01-02 00:00:00", "2024-03-04 12:00:00", "1969-12-31 00:00:00", "1969-12-31 23:59:59.999", "1970-01-01 00:00:00.001",
		"1900-01-01 00:00:00", "1000-01-01 00:00:00", nil, "2024-01-01 23:59:59.999", "9999-12-31 00:00:00", "1969-12-30 00:00:00.001", "2024-01-02 00:00:00"}
)

// dtr2PGFixture is dtr2Tables as PostgreSQL DDL (dt_pair is dtPGFixture).
func dtr2PGFixture() string {
	var b strings.Builder
	b.WriteString(dtPGFixture + "\n")
	b.WriteString("DROP TABLE IF EXISTS dtb_t; DROP TABLE IF EXISTS dtb_d; DROP TABLE IF EXISTS dr_t; DROP TABLE IF EXISTS dr_d;\n")
	b.WriteString("CREATE TABLE dtb_t (id bigint, ts timestamp); CREATE TABLE dtb_d (id bigint, d date);\n")
	b.WriteString("CREATE TABLE dr_t (id bigint, ts timestamp); CREATE TABLE dr_d (id bigint, d date);\n")
	for i, ms := range []any{int64(-500_001), int64(-500_000), int64(-1), int64(0), int64(1), int64(499_999), int64(500_000), int64(-86_400_000), nil} {
		if ms == nil {
			fmt.Fprintf(&b, "INSERT INTO dtb_t VALUES (%d, NULL);\n", i+1)
			continue
		}
		fmt.Fprintf(&b, "INSERT INTO dtb_t VALUES (%d, TIMESTAMP '1970-01-01' + %d * interval '1 millisecond');\n", i+1, ms)
	}
	b.WriteString("INSERT INTO dtb_d VALUES (1, '1969-12-31'), (2, '1970-01-01'), (3, NULL);\n")
	for i, d := range dtr2ReviewDates {
		if d == nil {
			fmt.Fprintf(&b, "INSERT INTO dr_d VALUES (%d, NULL);\n", i+1)
		} else {
			fmt.Fprintf(&b, "INSERT INTO dr_d VALUES (%d, '%s');\n", i+1, d)
		}
	}
	for i, s := range dtr2ReviewStamps {
		if s == nil {
			fmt.Fprintf(&b, "INSERT INTO dr_t VALUES (%d, NULL);\n", i+1)
		} else {
			fmt.Fprintf(&b, "INSERT INTO dr_t VALUES (%d, '%s');\n", i+1, s)
		}
	}
	return b.String()
}

// dtr2Standalone is one embedded engine over dtr2Tables (budget 0 = none).
func dtr2Standalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range dtr2Tables() {
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

// dtr2Arms is dtArms over dtr2Tables.
func dtr2Arms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	single := dtr2Standalone(t, ctx, 0)
	spilled := dtr2Standalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, dtr2Tables())
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

// dtr2Ops are the six comparison operators.
var dtr2Ops = []struct{ name, op string }{
	{"eq", "="}, {"ne", "<>"}, {"lt", "<"}, {"le", "<="}, {"gt", ">"}, {"ge", ">="},
}

// dtr2ScalarCells is item 1's family: a DATE scalar subquery against a
// TIMESTAMP column across the epoch band — the subquery on the right, on the
// left, and correlated (dtb_t row id reads DATE (id+1)%2+1: an odd id —
// epoch ms -1 among them — meets 1969-12-31) — for each of the six
// operators and both DATEs; the mirror (a TIMESTAMP scalar subquery against the DATE column) at
// the band's edges; BETWEEN with a subquery bound; and round 1's review
// cells over dr_t / dr_d verbatim.
func dtr2ScalarCells() []dtCell {
	var out []dtCell
	add := func(name, sql string) { out = append(out, dtCell{name, sql}) }
	for _, o := range dtr2Ops {
		for _, k := range []int{1, 2} {
			sub := fmt.Sprintf("(SELECT d FROM dtb_d WHERE id = %d)", k)
			add(fmt.Sprintf("b1/band/right/%s/d%d", o.name, k), fmt.Sprintf("SELECT id FROM dtb_t WHERE ts %s %s", o.op, sub))
			add(fmt.Sprintf("b1/band/left/%s/d%d", o.name, k), fmt.Sprintf("SELECT id FROM dtb_t WHERE %s %s ts", sub, o.op))
		}
		add("b1/band/corr/"+o.name, fmt.Sprintf("SELECT t.id FROM dtb_t t WHERE t.ts %s (SELECT b.d FROM dtb_d b WHERE b.id = (t.id + 1) %% 2 + 1)", o.op))
		add("b1/band/corrLeft/"+o.name, fmt.Sprintf("SELECT t.id FROM dtb_t t WHERE (SELECT max(b.d) FROM dtb_d b WHERE b.id = (t.id + 1) %% 2 + 1) %s t.ts", o.op))
		for _, k := range []int{2, 3, 5, 7} {
			sub := fmt.Sprintf("(SELECT ts FROM dtb_t WHERE id = %d)", k)
			add(fmt.Sprintf("b1/mirror/right/%s/t%d", o.name, k), fmt.Sprintf("SELECT id FROM dtb_d WHERE d %s %s", o.op, sub))
			add(fmt.Sprintf("b1/mirror/left/%s/t%d", o.name, k), fmt.Sprintf("SELECT id FROM dtb_d WHERE %s %s d", sub, o.op))
		}
		add("b1/mirror/corr/"+o.name, fmt.Sprintf("SELECT b.id FROM dtb_d b WHERE b.d %s (SELECT t.ts FROM dtb_t t WHERE t.id = b.id + 2)", o.op))
	}
	add("b1/between/dd", "SELECT id FROM dtb_t WHERE ts BETWEEN (SELECT d FROM dtb_d WHERE id = 1) AND (SELECT d FROM dtb_d WHERE id = 2)")
	add("b1/between/litLow", "SELECT id FROM dtb_t WHERE ts BETWEEN TIMESTAMP '1969-12-31 23:59:59' AND (SELECT d FROM dtb_d WHERE id = 2)")
	add("b1/between/notBetween", "SELECT id FROM dtb_t WHERE ts NOT BETWEEN (SELECT d FROM dtb_d WHERE id = 2) AND TIMESTAMP '1970-01-01 00:08:20'")
	add("b1/review/eq", "SELECT id FROM dr_t WHERE ts = (SELECT d FROM dr_d WHERE id = 3)")
	add("b1/review/ne", "SELECT id FROM dr_t WHERE ts <> (SELECT d FROM dr_d WHERE id = 3)")
	add("b1/review/gt", "SELECT id FROM dr_t WHERE ts > (SELECT d FROM dr_d WHERE id = 3)")
	add("b1/review/min", "SELECT id FROM dr_t WHERE ts = (SELECT min(d) FROM dr_d WHERE id = 3)")
	add("b1/review/corr", "SELECT b.id FROM dr_t b WHERE b.ts = (SELECT max(a.d) FROM dr_d a WHERE a.id = b.id - 1)")
	add("b1/review/eq1970", "SELECT id FROM dr_t WHERE ts = (SELECT d FROM dr_d WHERE id = 4)")
	add("b1/review/lt1969_30", "SELECT id FROM dr_t WHERE ts < (SELECT d FROM dr_d WHERE id = 10)")
	add("b1/review/eq9999", "SELECT id FROM dr_t WHERE ts = (SELECT d FROM dr_d WHERE id = 9)")
	add("b1/review/dEqTs", "SELECT id FROM dr_d WHERE d = (SELECT ts FROM dr_t WHERE id = 3)")
	return out
}

// dtr2FoldCells is item 2's family: a CASE / COALESCE / GREATEST / LEAST
// whose arms are a DATE and a TIMESTAMP, one arm a column and the other a
// scalar subquery, a correlated one, a window call or a function — both ways
// round, a window value function, a union / CTE / derived-table body, a
// function of a subquery, an aggregate arm, the WHERE / ORDER BY / GROUP BY /
// HAVING positions — plus NULLIF (typed by its first argument, answered as
// PostgreSQL does: the control).
func dtr2FoldCells() []dtCell {
	tsArms := map[string]string{
		"sub":  "(SELECT max(r.ts) FROM dt_pair r)",
		"corr": "(SELECT max(r.ts) FROM dt_pair r WHERE r.id = a.id)",
		"win":  "max(a.ts) OVER ()",
		"fn":   "date_trunc('hour', a.ts)",
	}
	dArms := map[string]string{
		"sub":  "(SELECT max(r.d) FROM dt_pair r)",
		"corr": "(SELECT max(r.d) FROM dt_pair r WHERE r.id = a.id)",
		"win":  "max(a.d) OVER ()",
	}
	shapes := func(col, x string) map[string]string {
		return map[string]string{
			"coalesceColFirst": fmt.Sprintf("COALESCE(%s, %s)", col, x),
			"coalesceArmFirst": fmt.Sprintf("COALESCE(%s, %s)", x, col),
			"caseColFirst":     fmt.Sprintf("CASE WHEN a.id = 1 THEN %s ELSE %s END", col, x),
			"caseArmFirst":     fmt.Sprintf("CASE WHEN a.id = 1 THEN %s ELSE %s END", x, col),
			"greatest":         fmt.Sprintf("GREATEST(%s, %s)", col, x),
			"least":            fmt.Sprintf("LEAST(%s, %s)", x, col),
			"nullifColFirst":   fmt.Sprintf("NULLIF(%s, %s)", col, x),
			"nullifArmFirst":   fmt.Sprintf("NULLIF(%s, %s)", x, col),
		}
	}
	var out []dtCell
	for _, side := range []struct {
		name, col string
		arms      map[string]string
	}{{"dCol", "a.d", tsArms}, {"tsCol", "a.ts", dArms}} {
		for an, x := range side.arms {
			for sn, e := range shapes(side.col, x) {
				// NULLIF is declared by its FIRST argument, and one whose
				// first argument is a TIMESTAMP answers its epoch number
				// whatever the second is — `NULLIF(ts, TIMESTAMP '…')`
				// alike — which is outside the pair (a filing candidate);
				// only the DATE-first NULLIF is this family's control.
				if strings.HasPrefix(sn, "nullif") && (side.name == "dCol") != (sn == "nullifColFirst") {
					continue
				}
				out = append(out, dtCell{fmt.Sprintf("b3/%s/%s/%s", side.name, an, sn),
					fmt.Sprintf("SELECT a.id, %s FROM dt_pair a", e)})
			}
		}
	}
	out = append(out,
		dtCell{"b3/where/coalesceSub", "SELECT a.id FROM dt_pair a WHERE COALESCE(a.d, (SELECT max(r.ts) FROM dt_pair r)) > TIMESTAMP '2024-03-04 06:00:00'"},
		dtCell{"b3/where/caseWin", "SELECT id FROM (SELECT a.id, CASE WHEN a.id = 1 THEN a.d ELSE max(a.ts) OVER () END AS c FROM dt_pair a) s WHERE c > TIMESTAMP '2024-03-04 06:00:00'"},
		dtCell{"b3/nested/caseInCoalesce", "SELECT a.id, COALESCE(a.d, CASE WHEN a.id > 0 THEN (SELECT max(r.ts) FROM dt_pair r) END) FROM dt_pair a"},
		dtCell{"b3/shape/lag", "SELECT a.id, COALESCE(a.d, lag(a.ts) OVER (ORDER BY a.id)) FROM dt_pair a"},
		dtCell{"b3/shape/firstValue", "SELECT a.id, COALESCE(a.d, first_value(a.ts) OVER (ORDER BY a.id)) FROM dt_pair a"},
		dtCell{"b3/shape/unionBody", "SELECT a.id, COALESCE(a.d, (SELECT ts FROM dt_pair WHERE id = 2 UNION SELECT ts FROM dt_pair WHERE id = 2)) FROM dt_pair a"},
		dtCell{"b3/shape/cteBody", "WITH m AS (SELECT max(ts) AS mt FROM dt_pair) SELECT a.id, COALESCE(a.d, (SELECT mt FROM m)) FROM dt_pair a"},
		dtCell{"b3/shape/derivedBody", "SELECT a.id, COALESCE(a.d, (SELECT x FROM (SELECT max(ts) AS x FROM dt_pair) s)) FROM dt_pair a"},
		dtCell{"b3/shape/greatest3", "SELECT a.id, GREATEST(a.d, a.d, (SELECT max(ts) FROM dt_pair)) FROM dt_pair a"},
		dtCell{"b3/shape/fnOfSub", "SELECT a.id, COALESCE(a.d, date_trunc('day', (SELECT max(ts) FROM dt_pair))) FROM dt_pair a"},
		dtCell{"b3/shape/aggArm", "SELECT COALESCE(max(a.d), (SELECT max(ts) FROM dt_pair)) FROM dt_pair a"},
		dtCell{"b3/position/orderBy", "SELECT a.id FROM dt_pair a ORDER BY COALESCE(a.d, (SELECT max(ts) FROM dt_pair)), a.id"},
		dtCell{"b3/position/groupBy", "SELECT COALESCE(a.d, (SELECT max(ts) FROM dt_pair)) AS c, count(*) FROM dt_pair a GROUP BY 1"},
		dtCell{"b3/position/having", "SELECT a.id FROM dt_pair a GROUP BY a.id, a.d HAVING COALESCE(a.d, (SELECT max(ts) FROM dt_pair)) > TIMESTAMP '2024-01-01'"},
	)
	return out
}

// dtr2Refused reports whether a fold cell is refused: every shape but NULLIF,
// whose declared type is its first argument's (PostgreSQL answers it and so
// does this engine).
func dtr2Refused(name string) bool {
	return strings.HasPrefix(name, "b3/") && !strings.Contains(name, "/nullif")
}

// dtr2SpecOnly names the cells PostgreSQL has no spelling for (none today:
// IF is a keyword to this parser, `IF(` is 42601 before any typing, so it
// has no fold of its own to refuse).
func dtr2SpecOnly(string) bool { return false }

// dtr2NoRunner names the cells the three DAG arms cannot compile today: a
// BETWEEN bound or a CASE / COALESCE arm that is a subquery inside a WHERE is
// not lowered by the stage DAG (compile filter …: subqueries require a
// SubqueryRunner — the #1364 / #1384 family). PINNED there — a DAG arm that
// starts answering fails, and the pin is deleted as the proof.
func dtr2NoRunner(name string) bool {
	return strings.HasPrefix(name, "b1/between/")
}

func dtr2Cells() []dtCell { return append(dtr2ScalarCells(), dtr2FoldCells()...) }

// dtr2PGAnswers reads testdata/arc_dt_r2_pg17.tsv (name<TAB>answer).
func dtr2PGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_dt_r2_pg17.tsv")
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

// TestArcDTR2Generate dumps the cells as name<TAB>sql and the PostgreSQL
// fixture (DT_R2_GEN=<dir>); skipped otherwise.
func TestArcDTR2Generate(t *testing.T) {
	dir := os.Getenv("DT_R2_GEN")
	if dir == "" {
		t.Skip("DT_R2_GEN unset")
	}
	var b strings.Builder
	for _, c := range dtr2Cells() {
		if dtr2SpecOnly(c.name) {
			continue
		}
		b.WriteString(c.name + "\t" + c.sql + "\n")
	}
	if err := os.WriteFile(dir+"/cells.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/fixture.sql", []byte(dtr2PGFixture()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// A DATE SCALAR SUBQUERY AGAINST A TIMESTAMP, AND A CHOICE WHOSE ARM IS NOT A
// COLUMN, ON FIVE ARMS (#1378 round 2).
//
// Item 1: the comparison's kernel read a DATE side only as an int64 box; a
// DATE scalar subquery hands its value over as ISO text, so the pair fell
// through to compare()'s magnitude guess, which reads an int64 inside
// +/-500 000 as a DAY count — `ts = (SELECT d …)` for d = 1969-12-31
// matched the TIMESTAMP 1969-12-31 23:59:59.999 (ms -1, "day -1") on the
// single-process arms, and on all five when correlated. The kernel now asks
// batch.TemporalCommonType and converts with batch.DateMidnightMillis, every
// spelling of each side read by its declaration. Every cell against
// PostgreSQL 17.11's sorted rows.
//
// Item 2: the choice-fold refusal skipped an arm the binder's fold typing
// could not type — a scalar subquery, a window call — so the first arm's
// type declared the column and a DATE-first COALESCE answered day counts
// where PostgreSQL answers timestamps. The refusal now types every arm by
// its declaration; each refused cell asserts 0A000 and the one message on
// every arm.
func TestArcDTR2ScalarAndFoldArmsEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the DATE / TIMESTAMP scalar and fold table")
	}
	answers := dtr2PGAnswers(t)
	cells := dtr2Cells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok && !dtr2SpecOnly(c.name) {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := dtr2Arms(t, ctx)
	const foldMsg = "does not yet carry a DATE arm into a TIMESTAMP result"
	const runnerMsg = "subqueries require a SubqueryRunner"
	for _, tc := range cells {
		want := answers[tc.name]
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
				switch {
				case dtr2Refused(tc.name):
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want 0A000 %q (PostgreSQL 17.11: %s)", tc.sql, arm.name, brRender(res), foldMsg, want)
					} else if st := sqlerr.StateOf(err); st != "0A000" || !strings.Contains(err.Error(), foldMsg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want 0A000 %q", tc.sql, arm.name, st, err, foldMsg)
					}
				case dtr2NoRunner(tc.name) && strings.HasPrefix(arm.name, "dag"):
					if err == nil || !strings.Contains(err.Error(), runnerMsg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  PINNED %q (#1364 / #1384): a DAG arm that answers deletes this pin (PostgreSQL 17.11: %s)",
							tc.sql, arm.name, brRenderOrNil(res), err, runnerMsg, want)
					}
				case err != nil:
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, err, want)
				default:
					if got := brRender(res); strings.TrimSpace(got) != strings.TrimSpace(want) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", tc.sql, arm.name, got, want)
					}
				}
			}
		})
	}
}
