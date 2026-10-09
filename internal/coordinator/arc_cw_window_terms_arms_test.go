// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A WINDOW'S TERMS ARE JUDGED LIKE A SELECT ITEM ABOVE A GROUP BY, AND AN
// ITEM MIXING AN AGGREGATE AND A WINDOW FUNCTION IS COMPUTED, ON EVERY ARM
// (#1651, #1646; ADR-0047 §Binding and window terms).
//
// The coverage table is generated: every window-function class (an
// aggregate as a window, ranking, LAG with a default, FIRST_VALUE /
// NTH_VALUE, a ROWS frame) × the term's position (argument, LAG's default,
// PARTITION BY, ORDER BY, frame offset) × the reference class (a group key,
// the key qualified, an expression over the key, an aggregate call, an
// expression over one, an ungrouped column bare / qualified / inside an
// expression, a SELECT output alias), each as a bare window item and nested
// inside a larger expression; a GROUP BY over an expression; a block grouped
// by an aggregate alone; a window in the block's ORDER BY; the mixed items of
// #1646; and the two issues' statements. testdata/arc_cw_window_terms_pg17.tsv
// is PostgreSQL 17.11's answer for every cell (TestArcCWMeasure regenerates
// it).
//
// At 542b4f37 the grouped check did not look inside a window's terms: an
// ungrouped argument read NULL (`sum(f) OVER (PARTITION BY a.i) … GROUP BY
// i` answered three NULLs where PostgreSQL raises 42803), an ungrouped
// PARTITION BY / ORDER BY reached the window operator and failed there with
// no SQLSTATE, and `MAX(b) * 2 + ROW_NUMBER() OVER (ORDER BY g)` answered NULL
// on every row.
//
// wd_t has two rows in group g = 1, so a term read from the input rows
// instead of the groups answers differently from one read from the groups.

func cwTables() []tmdTable {
	i32 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt32, Nullable: true} }
	rv := tmdTable{name: "rv_a", schema: parquet.Schema{Columns: []parquet.Column{i32("id"), i32("i"),
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true}}}}
	for _, r := range [][3]any{{int32(1), int32(2), 0.5}, {int32(2), int32(4), 1.5}, {int32(3), int32(1), 2.5}} {
		rv.rows = append(rv.rows, map[string]any{"id": r[0], "i": r[1], "f": r[2]})
	}
	wd := tmdTable{name: "wd_t", schema: parquet.Schema{Columns: []parquet.Column{i32("id"), i32("g"),
		{Name: "b", Type: parquet.TypeInt64, Nullable: true}}}}
	for _, r := range [][3]any{{int32(1), int32(1), int64(20)}, {int32(2), int32(2), int64(50)},
		{int32(3), int32(3), int64(60)}, {int32(4), int32(1), int64(10)}} {
		wd.rows = append(wd.rows, map[string]any{"id": r[0], "g": r[1], "b": r[2]})
	}
	return []tmdTable{rv, wd}
}

const cwPGFixture = `DROP TABLE IF EXISTS rv_a;
CREATE TABLE rv_a (id integer, i integer, f double precision);
INSERT INTO rv_a VALUES (1,2,0.5),(2,4,1.5),(3,1,2.5);
DROP TABLE IF EXISTS wd_t;
CREATE TABLE wd_t (id integer, g integer, b bigint);
INSERT INTO wd_t VALUES (1,1,20),(2,2,50),(3,3,60),(4,1,10);`

type cwCell struct{ name, sql string }

// cwOracle is the statement PostgreSQL answers in a cell's place where the
// cell's own spelling has no PostgreSQL form (QUALIFY): the same filter over
// a derived table. Filled by cwCells.
var cwOracle = map[string]string{}

// cwRefs are the reference classes over `wd_t w GROUP BY g`.
var cwRefs = []struct{ name, x string }{
	{"key", "g"}, {"keyQual", "w.g"}, {"keyExpr", "g + 1"},
	{"agg", "max(b)"}, {"aggExpr", "max(b) * 2"},
	{"ungrouped", "b"}, {"ungroupedQual", "w.b"}, {"ungroupedExpr", "g + b"},
	{"outputAlias", "k"},
}

// cwTemplates are the function class × term position; X is the term.
var cwTemplates = []struct{ name, win string }{
	{"arg/sum", "sum(X) OVER ()"},
	{"arg/sumPart", "sum(X) OVER (PARTITION BY g)"},
	{"arg/lag", "lag(X, 1, 0) OVER (ORDER BY g)"},
	{"arg/firstValue", "first_value(X) OVER (ORDER BY g)"},
	{"arg/nthValue", "nth_value(X, 2) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"},
	{"default/lag", "lag(g, 1, X) OVER (ORDER BY g)"},
	{"partition/rank", "rank() OVER (PARTITION BY X ORDER BY g)"},
	{"partition/sum", "sum(g) OVER (PARTITION BY X)"},
	{"order/rank", "rank() OVER (ORDER BY X)"},
	{"order/sum", "sum(g) OVER (ORDER BY X)"},
	{"order/rowNumberDesc", "row_number() OVER (ORDER BY X DESC)"},
	{"order/lag", "lag(g) OVER (ORDER BY X)"},
	{"frame/rows", "sum(g) OVER (ORDER BY g ROWS BETWEEN X PRECEDING AND CURRENT ROW)"},
}

func cwCells() []cwCell {
	var out []cwCell
	for _, tp := range cwTemplates {
		for _, r := range cwRefs {
			win := strings.ReplaceAll(tp.win, "X", r.x)
			for _, form := range []struct{ name, item string }{{"bare", win}, {"nested", "(" + win + ") + 0"}} {
				out = append(out, cwCell{tp.name + "/" + r.name + "/" + form.name,
					"SELECT g AS k, " + form.item + " AS w FROM wd_t w GROUP BY g ORDER BY g"})
			}
		}
	}
	// A GROUP BY over an EXPRESSION: the expression is a key, the column
	// under it is not.
	for _, tp := range []struct{ name, win string }{
		{"arg/sum", "sum(X) OVER ()"}, {"partition/rank", "rank() OVER (PARTITION BY X ORDER BY g + 1)"},
		{"order/rank", "rank() OVER (ORDER BY X)"}, {"default/lag", "lag(g + 1, 1, X) OVER (ORDER BY g + 1)"},
	} {
		for _, r := range []struct{ name, x string }{
			{"keyIsExpr", "g + 1"}, {"overKeyExpr", "(g + 1) * 2"}, {"columnUnderKey", "g"}, {"agg", "max(b)"},
		} {
			win := strings.ReplaceAll(tp.win, "X", r.x)
			out = append(out, cwCell{"grpExpr/" + tp.name + "/" + r.name,
				"SELECT g + 1 AS k, " + win + " AS w FROM wd_t w GROUP BY g + 1 ORDER BY 1"})
		}
	}
	// An aggregate with no GROUP BY groups the whole input into one row.
	out = append(out,
		cwCell{"implicit/aggArg", "SELECT max(b) AS m, sum(max(b)) OVER () AS w FROM wd_t w"},
		cwCell{"implicit/ungroupedArg", "SELECT max(b) AS m, sum(b) OVER () AS w FROM wd_t w"},
		cwCell{"implicit/ungroupedOrder", "SELECT max(b) AS m, rank() OVER (ORDER BY g) AS w FROM wd_t w"},
		cwCell{"implicit/countPlusRowNumber", "SELECT count(*) + row_number() OVER () AS w FROM wd_t w"},
		cwCell{"implicit/windowOnly", "SELECT sum(b) OVER () AS w FROM wd_t w ORDER BY w"},
		// A window in the block's ORDER BY is judged like one in the list.
		cwCell{"clause/orderByUngrouped", "SELECT g FROM wd_t w GROUP BY g ORDER BY sum(b) OVER (), g"},
		cwCell{"clause/orderByAggTerm", "SELECT g FROM wd_t w GROUP BY g ORDER BY sum(g) OVER (ORDER BY max(b)) DESC"},
		cwCell{"clause/orderByKey", "SELECT g FROM wd_t w GROUP BY g ORDER BY rank() OVER (ORDER BY g DESC)"},
	)
	// #1646: an item holding both an aggregate and a window function.
	for _, m := range []struct{ name, item string }{
		{"issue", "MAX(b) * 2 + ROW_NUMBER() OVER (ORDER BY g)"},
		{"windowFirst", "ROW_NUMBER() OVER (ORDER BY g) + COUNT(*)"},
		{"windowMinusAgg", "SUM(g) OVER () - MAX(b)"},
		{"coalesceRank", "COALESCE(MAX(b), 0) + RANK() OVER (ORDER BY g)"},
		{"aggPlusLagOfAgg", "MAX(b) + LAG(MAX(b), 1, 0) OVER (ORDER BY g)"},
		{"twoAggsOneWindow", "MAX(b) - MIN(b) + DENSE_RANK() OVER (ORDER BY MAX(b))"},
		{"aggTimesWindow", "COUNT(*) * SUM(COUNT(*)) OVER ()"},
		{"keyPlusAggPlusWindow", "g + MAX(b) + ROW_NUMBER() OVER (ORDER BY g)"},
		{"caseOverBoth", "CASE WHEN MAX(b) > 20 THEN ROW_NUMBER() OVER (ORDER BY g) ELSE 0 END"},
		{"ungroupedBeside", "MAX(b) + SUM(b) OVER ()"},
		{"aggInsideWindow", "sum(MAX(b) * 2) OVER (ORDER BY g)"},
	} {
		out = append(out, cwCell{"mixed/" + m.name, "SELECT g, " + m.item + " AS w FROM wd_t GROUP BY g ORDER BY g"})
	}
	// QUALIFY (round 2): a window term in the clause is judged like one in
	// the list, and a term mixing an aggregate and a window is computed.
	// PostgreSQL has no QUALIFY, so each cell's oracle is the same filter
	// over a derived table (cwOracle), measured on PostgreSQL.
	for _, tp := range []struct{ name, win, cond string }{
		{"rank", "rank() OVER (ORDER BY X)", "<= 2"},
		{"lag", "lag(X) OVER (ORDER BY g)", "IS NULL"},
		{"countPart", "count(*) OVER (PARTITION BY X)", "= 1"},
		{"sumOrd", "sum(X) OVER (ORDER BY g)", "> 25"},
	} {
		for _, r := range cwRefs {
			win := strings.ReplaceAll(tp.win, "X", r.x)
			name := "qualify/" + tp.name + "/" + r.name
			out = append(out, cwCell{name, "SELECT g AS k FROM wd_t w GROUP BY g QUALIFY " + win + " " + tp.cond + " ORDER BY g"})
			cwOracle[name] = "SELECT k FROM (SELECT g AS k, " + win + " AS q FROM wd_t w GROUP BY g) s WHERE q " + tp.cond + " ORDER BY k"
		}
	}
	for _, m := range []struct{ name, sql, oracle string }{
		{"mixedAlias", "SELECT g, max(b) + row_number() OVER (ORDER BY g) AS w FROM wd_t GROUP BY g QUALIFY w > 50 ORDER BY g",
			"SELECT g, w FROM (SELECT g, max(b) + row_number() OVER (ORDER BY g) AS w FROM wd_t GROUP BY g) s WHERE w > 50 ORDER BY g"},
		{"winPlusAgg", "SELECT g FROM wd_t GROUP BY g QUALIFY row_number() OVER (ORDER BY g) + max(b) > 50 ORDER BY g",
			"SELECT g FROM (SELECT g, row_number() OVER (ORDER BY g) + max(b) AS q FROM wd_t GROUP BY g) s WHERE q > 50 ORDER BY g"},
		{"aggPlusWin", "SELECT g FROM wd_t GROUP BY g QUALIFY max(b) + row_number() OVER (ORDER BY g) > 50 ORDER BY g",
			"SELECT g FROM (SELECT g, max(b) + row_number() OVER (ORDER BY g) AS q FROM wd_t GROUP BY g) s WHERE q > 50 ORDER BY g"},
		{"aggSelectedAndQualified", "SELECT g, max(b) AS m FROM wd_t GROUP BY g QUALIFY max(b) + row_number() OVER (ORDER BY g) > 50 ORDER BY g",
			"SELECT g, m FROM (SELECT g, max(b) AS m, max(b) + row_number() OVER (ORDER BY g) AS q FROM wd_t GROUP BY g) s WHERE q > 50 ORDER BY g"},
		{"aggExprAlias", "SELECT g, max(b) * 2 AS m FROM wd_t GROUP BY g QUALIFY m > 50 AND row_number() OVER (ORDER BY g) > 0 ORDER BY g",
			"SELECT g, m FROM (SELECT g, max(b) * 2 AS m, row_number() OVER (ORDER BY g) AS q FROM wd_t GROUP BY g) s WHERE m > 50 AND q > 0 ORDER BY g"},
		{"winOverAgg", "SELECT g FROM wd_t GROUP BY g QUALIFY sum(max(b)) OVER (ORDER BY g) > 50 ORDER BY g",
			"SELECT g FROM (SELECT g, sum(max(b)) OVER (ORDER BY g) AS q FROM wd_t GROUP BY g) s WHERE q > 50 ORDER BY g"},
		{"mixedUngrouped", "SELECT g FROM wd_t GROUP BY g QUALIFY b + row_number() OVER (ORDER BY g) > 50 ORDER BY g",
			"SELECT g FROM (SELECT g, b + row_number() OVER (ORDER BY g) AS q FROM wd_t GROUP BY g) s WHERE q > 50 ORDER BY g"},
		{"ungroupedNoGroup", "SELECT id, b FROM wd_t QUALIFY b + row_number() OVER (ORDER BY id) > 50 ORDER BY id",
			"SELECT id, b FROM (SELECT id, b, b + row_number() OVER (ORDER BY id) AS q FROM wd_t) s WHERE q > 50 ORDER BY id"},
	} {
		out = append(out, cwCell{"qualifyMixed/" + m.name, m.sql})
		cwOracle["qualifyMixed/"+m.name] = m.oracle
	}
	// The two issues' statements and the brief's rows, verbatim.
	out = append(out,
		cwCell{"issue/1651/arg", "SELECT sum(f) OVER (PARTITION BY a.i) AS w FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/1651/argQual", "SELECT sum(a.f) OVER (PARTITION BY a.i) FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/1651/partition", "SELECT sum(i) OVER (PARTITION BY f) FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/1651/order", "SELECT sum(i) OVER (ORDER BY f) FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/1651/keyExpr", "SELECT sum(i) OVER () FROM rv_a a GROUP BY i + 1 ORDER BY 1"},
		cwCell{"issue/ctl/frame", "SELECT sum(i) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/ctl/keyArg", "SELECT sum(i) OVER (PARTITION BY a.i) AS w FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/ctl/aggArg", "SELECT i, sum(max(f)) OVER () FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/ctl/aggPartition", "SELECT i, row_number() OVER (PARTITION BY max(f) ORDER BY i) FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/ctl/keyExprArg", "SELECT sum(i + 1) OVER () FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/ctl/keyExprArgExprKey", "SELECT sum(i + 1) OVER () FROM rv_a a GROUP BY i + 1 ORDER BY 1"},
		cwCell{"issue/ctl/outputAlias", "SELECT i AS k, sum(i) OVER (PARTITION BY k) FROM rv_a a GROUP BY i ORDER BY i"},
		cwCell{"issue/1646/mixed", "SELECT g, MAX(b) * 2 + ROW_NUMBER() OVER (ORDER BY g) AS w FROM wd_t GROUP BY g ORDER BY g"},
		cwCell{"issue/1646/aggInsideWindow", "SELECT g, sum(MAX(b) * 2) OVER (ORDER BY g) AS w FROM wd_t GROUP BY g ORDER BY g"},
		cwCell{"issue/1651/filed", "SELECT sum(b) OVER (PARTITION BY w.g) AS w FROM wd_t w GROUP BY g ORDER BY g"},
	)
	return out
}

// cwColumnRe is the column a refusal names: `column "a.f" must appear …`.
var cwColumnRe = regexp.MustCompile(`column "([^"]+)"`)

// cwErr renders a refusal as ERR <SQLSTATE> and, when the message names a
// column, that column's own name (the qualifier is the message's wording, not
// the answer).
func cwErr(state, msg string) string {
	if state == "" {
		state = "-"
	}
	if m := cwColumnRe.FindStringSubmatch(msg); m != nil {
		name := m[1]
		if i := strings.LastIndexByte(name, '.'); i >= 0 {
			name = name[i+1:]
		}
		return "ERR " + state + " " + strings.ToLower(name)
	}
	return "ERR " + state
}

func cwRunSingle(ctx context.Context, db *wadjet.DB, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return cwErr(sqlerr.StateOf(err), err.Error())
	}
	cols := make([]parquet.Column, len(res.ColumnMetas))
	for i, m := range res.ColumnMetas {
		cols[i] = parquet.Column{Name: m.Name, Type: m.TypeID, Precision: m.Precision, Scale: m.Scale}
	}
	cells := make([][]any, len(res.Rows))
	for i := range res.Rows {
		cells[i] = res.Cells(i)
	}
	return ftEngineRender(cols, cells)
}

func cwRunDAG(ctx context.Context, c *Coordinator, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return cwErr(sqlerr.StateOf(err), err.Error())
	}
	if res.Error != "" {
		return cwErr("", res.Error)
	}
	cols := append([]parquet.Column(nil), res.OutputSchema()...)
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return cwErr(sqlerr.StateOf(berr), berr.Error())
			}
			if bb == nil {
				break
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return cwErr(sqlerr.StateOf(rerr), rerr.Error())
		}
		for _, r := range rows {
			row := make([]any, len(cols))
			for j, c := range cols {
				row[j] = r[c.Name]
			}
			cells = append(cells, row)
		}
	}
	return ftEngineRender(cols, cells)
}

const cwAnswersPath = "testdata/arc_cw_window_terms_pg17.tsv"

func cwReadTSV(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
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

// TestArcCWMeasure runs every cell against PostgreSQL (CW_PG_DSN, e.g.
// postgres://wadjet:wadjet@127.0.0.1:57967/wadjet_oracle) and rewrites the
// answer file; skipped otherwise.
func TestArcCWMeasure(t *testing.T) {
	dsn := os.Getenv("CW_PG_DSN")
	if dsn == "" {
		t.Skip("CW_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s';"+cwPGFixture).ReadAll(); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 answers for arc_cw_window_terms_arms_test.go (TestArcCWMeasure).\n")
	for _, c := range cwCells() {
		q := c.sql
		if o, ok := cwOracle[c.name]; ok {
			q = o
		}
		r := conn.ExecParams(ctx, q, nil, nil, nil, nil).Read()
		var ans string
		if r.Err != nil {
			pe, ok := r.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("%s: %v", c.name, r.Err)
			}
			ans = cwErr(pe.Code, pe.Message)
		} else {
			header := make([]string, len(r.FieldDescriptions))
			for i, fd := range r.FieldDescriptions {
				header[i] = fd.Name + ":" + ftPGTypeName(fd.DataTypeOID)
			}
			rows := make([]string, len(r.Rows))
			for i, row := range r.Rows {
				f := make([]string, len(row))
				for j, v := range row {
					if v == nil {
						f[j] = "NULL"
					} else {
						f[j] = string(v)
					}
				}
				rows[i] = strings.Join(f, ",")
			}
			ans = ftRender(header, rows)
		}
		b.WriteString(c.name + "\t" + ans + "\n")
	}
	if err := os.WriteFile(cwAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func cwStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range cwTables() {
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

type cwArm struct {
	name string
	run  func(string) string
}

func cwArms(t *testing.T, ctx context.Context) []cwArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, cwTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := cwStandalone(t, ctx, 0), cwStandalone(t, ctx, 512*1024)
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	var spillMu sync.Mutex
	return []cwArm{
		{"single", func(sql string) string { return cwRunSingle(ctx, single, sql) }},
		{"spilled512k", func(sql string) string {
			spillMu.Lock()
			defer spillMu.Unlock()
			restoreDrain := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(2)
			defer func() { restoreRuns(); exec.ForceAggDrainEvery(restoreDrain) }()
			return cwRunSingle(ctx, spilled, sql)
		}},
		{"dag", func(sql string) string { return cwRunDAG(ctx, coord, sql) }},
		{"dag-shuffled", func(sql string) string { return cwRunDAG(ctx, coordB, sql) }},
		{"dag-morsel4", func(sql string) string { return cwRunDAG(ctx, coordM, sql) }},
	}
}

// cwRunAll answers every cell on every arm, cells in parallel per arm.
func cwRunAll(t *testing.T, arms []cwArm, cells []cwCell) map[string][]string {
	t.Helper()
	got := make(map[string][]string, len(cells))
	for _, c := range cells {
		got[c.name] = make([]string, len(arms))
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, arm := range arms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, c := range cells {
				ans := arm.run(c.sql)
				mu.Lock()
				got[c.name][i] = ans
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return got
}

// TestArcCWDump writes every arm's answer for every cell (CW_DUMP=<path>):
// the base / tip measurement the landing notes compare. Skipped otherwise.
func TestArcCWDump(t *testing.T) {
	path := os.Getenv("CW_DUMP")
	if path == "" {
		t.Skip("CW_DUMP unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := cwArms(t, ctx)
	cells := cwCells()
	got := cwRunAll(t, arms, cells)
	var b strings.Builder
	for _, c := range cells {
		for i, arm := range arms {
			b.WriteString(c.name + "\t" + arm.name + "\t" + got[c.name][i] + "\n")
		}
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// cwKept is the answer an arm keeps where it is not PostgreSQL's, keyed
// "<cell>" (every arm) or "<cell>@dag" (the three stage-DAG arms), with why.
// A kept line that starts agreeing with PostgreSQL FAILS: delete it then.
// Every kept line is the arm's answer at 542b4f37 too (measured; the base
// dump in the arc notes): none of them is this arc's seam.
const (
	whyInt4Arith = "numeric-decimal r1: int4 arithmetic (here `g + 1` and a window value `+ 0`) declares bigint"
	whyNthDecl   = "filing candidate CW-F1: a nested NTH_VALUE / LAG with a computed value beside its other arguments " +
		"declares double precision on the single-process arms (windowComputedArgDecl compares the tree with the whole argument list)"
	whyFrame = "filing candidate CW-F2: a frame offset that is not a constant is a syntax error (42601) where PostgreSQL " +
		"raises 42P10 (a variable) or 42803 (an aggregate) — refused either way"
	whySortRefusal = "aggregates-windows r15's family: a window call as a sort term over a GROUP BY is refused 0A000 " +
		"(filing candidate CW-F3)"
)

var cwKept = map[string]struct{ want, why string }{
	"arg/lag/key/nested":                      {"k:integer,w:bigint | 1,0; 2,1; 3,2", whyInt4Arith},
	"arg/lag/keyQual/nested":                  {"k:integer,w:bigint | 1,0; 2,1; 3,2", whyInt4Arith},
	"arg/lag/keyExpr/bare":                    {"k:integer,w:bigint | 1,0; 2,2; 3,3", whyInt4Arith},
	"arg/lag/keyExpr/nested":                  {"k:integer,w:bigint | 1,0; 2,2; 3,3", whyInt4Arith},
	"arg/firstValue/key/nested":               {"k:integer,w:bigint | 1,1; 2,1; 3,1", whyInt4Arith},
	"arg/firstValue/keyQual/nested":           {"k:integer,w:bigint | 1,1; 2,1; 3,1", whyInt4Arith},
	"arg/firstValue/keyExpr/bare":             {"k:integer,w:bigint | 1,2; 2,2; 3,2", whyInt4Arith},
	"arg/firstValue/keyExpr/nested":           {"k:integer,w:bigint | 1,2; 2,2; 3,2", whyInt4Arith},
	"arg/nthValue/key/nested":                 {"k:integer,w:bigint | 1,2; 2,2; 3,2", whyInt4Arith},
	"arg/nthValue/keyQual/nested":             {"k:integer,w:bigint | 1,2; 2,2; 3,2", whyInt4Arith},
	"arg/nthValue/keyExpr/bare":               {"k:integer,w:bigint | 1,3; 2,3; 3,3", whyInt4Arith},
	"arg/nthValue/keyExpr/nested@dag":         {"k:integer,w:bigint | 1,3; 2,3; 3,3", whyInt4Arith},
	"arg/nthValue/keyExpr/nested@single":      {"k:integer,w:double precision | 1,3; 2,3; 3,3", whyNthDecl},
	"arg/nthValue/keyExpr/nested@spilled512k": {"k:integer,w:double precision | 1,3; 2,3; 3,3", whyNthDecl},
	"arg/nthValue/aggExpr/nested@single":      {"k:integer,w:double precision | 1,100; 2,100; 3,100", whyNthDecl},
	"arg/nthValue/aggExpr/nested@spilled512k": {"k:integer,w:double precision | 1,100; 2,100; 3,100", whyNthDecl},
	"default/lag/key/nested":                  {"k:integer,w:bigint | 1,1; 2,1; 3,2", whyInt4Arith},
	"default/lag/keyQual/nested":              {"k:integer,w:bigint | 1,1; 2,1; 3,2", whyInt4Arith},
	"default/lag/keyExpr/bare":                {"k:integer,w:bigint | 1,2; 2,1; 3,2", whyInt4Arith},
	"default/lag/keyExpr/nested":              {"k:integer,w:bigint | 1,2; 2,1; 3,2", whyInt4Arith},
	"order/lag/key/nested":                    {"k:integer,w:bigint | 1,NULL; 2,1; 3,2", whyInt4Arith},
	"order/lag/keyQual/nested":                {"k:integer,w:bigint | 1,NULL; 2,1; 3,2", whyInt4Arith},
	"order/lag/keyExpr/nested":                {"k:integer,w:bigint | 1,NULL; 2,1; 3,2", whyInt4Arith},
	"order/lag/agg/nested":                    {"k:integer,w:bigint | 1,NULL; 2,1; 3,2", whyInt4Arith},
	"order/lag/aggExpr/nested":                {"k:integer,w:bigint | 1,NULL; 2,1; 3,2", whyInt4Arith},
	"frame/rows/key/bare":                     {"ERR 42601", whyFrame},
	"frame/rows/key/nested":                   {"ERR 42601", whyFrame},
	"frame/rows/keyQual/bare":                 {"ERR 42601", whyFrame},
	"frame/rows/keyQual/nested":               {"ERR 42601", whyFrame},
	"frame/rows/keyExpr/bare":                 {"ERR 42601", whyFrame},
	"frame/rows/keyExpr/nested":               {"ERR 42601", whyFrame},
	"frame/rows/agg/bare":                     {"ERR 42601", whyFrame},
	"frame/rows/agg/nested":                   {"ERR 42601", whyFrame},
	"frame/rows/aggExpr/bare":                 {"ERR 42601", whyFrame},
	"frame/rows/aggExpr/nested":               {"ERR 42601", whyFrame},
	"frame/rows/ungrouped/bare":               {"ERR 42601", whyFrame},
	"frame/rows/ungrouped/nested":             {"ERR 42601", whyFrame},
	"frame/rows/ungroupedQual/bare":           {"ERR 42601", whyFrame},
	"frame/rows/ungroupedQual/nested":         {"ERR 42601", whyFrame},
	"frame/rows/ungroupedExpr/bare":           {"ERR 42601", whyFrame},
	"frame/rows/ungroupedExpr/nested":         {"ERR 42601", whyFrame},
	"frame/rows/outputAlias/bare":             {"ERR 42601", whyFrame},
	"frame/rows/outputAlias/nested":           {"ERR 42601", whyFrame},
	"grpExpr/arg/sum/keyIsExpr":               {"k:bigint,w:bigint | 2,9; 3,9; 4,9", whyInt4Arith},
	"grpExpr/arg/sum/overKeyExpr":             {"k:bigint,w:double precision | 2,18; 3,18; 4,18", whyInt4Arith},
	"grpExpr/arg/sum/agg":                     {"k:bigint,w:numeric | 2,130; 3,130; 4,130", whyInt4Arith},
	"grpExpr/partition/rank/keyIsExpr":        {"k:bigint,w:bigint | 2,1; 3,1; 4,1", whyInt4Arith},
	"grpExpr/partition/rank/overKeyExpr":      {"k:bigint,w:bigint | 2,1; 3,1; 4,1", whyInt4Arith},
	"grpExpr/partition/rank/agg":              {"k:bigint,w:bigint | 2,1; 3,1; 4,1", whyInt4Arith},
	"grpExpr/order/rank/keyIsExpr":            {"k:bigint,w:bigint | 2,1; 3,2; 4,3", whyInt4Arith},
	"grpExpr/order/rank/overKeyExpr":          {"k:bigint,w:bigint | 2,1; 3,2; 4,3", whyInt4Arith},
	"grpExpr/order/rank/agg":                  {"k:bigint,w:bigint | 2,1; 3,2; 4,3", whyInt4Arith},
	"grpExpr/default/lag/keyIsExpr":           {"k:bigint,w:bigint | 2,2; 3,2; 4,3", whyInt4Arith},
	"grpExpr/default/lag/overKeyExpr":         {"k:bigint,w:bigint | 2,4; 3,2; 4,3", whyInt4Arith},
	"grpExpr/default/lag/agg":                 {"k:bigint,w:bigint | 2,20; 3,2; 4,3", whyInt4Arith},
	"clause/orderByAggTerm":                   {"ERR 0A000", whySortRefusal},
	"clause/orderByKey":                       {"ERR 0A000", whySortRefusal},
}

func cwKeptFor(cell, arm string) (string, string, bool) {
	if k, ok := cwKept[cell+"@"+arm]; ok {
		return k.want, k.why, true
	}
	if strings.HasPrefix(arm, "dag") {
		if k, ok := cwKept[cell+"@dag"]; ok {
			return k.want, k.why, true
		}
	}
	if k, ok := cwKept[cell]; ok {
		return k.want, k.why, true
	}
	return "", "", false
}

func TestArcCWWindowTermsEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the window-term table")
	}
	answers := cwReadTSV(t, cwAnswersPath)
	cells := cwCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	known := map[string]bool{}
	for _, c := range cells {
		known[c.name] = true
	}
	for k := range cwKept {
		cell, _, _ := strings.Cut(k, "@")
		if !known[cell] {
			t.Fatalf("kept line %s names no generated cell", k)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := cwArms(t, ctx)
	got := cwRunAll(t, arms, cells)
	agree, kept, refused := 0, 0, 0
	for _, c := range cells {
		pg := answers[c.name]
		if strings.HasPrefix(pg, "ERR ") {
			refused++
		}
		for i, arm := range arms {
			ans := got[c.name][i]
			if want, why, ok := cwKeptFor(c.name, arm.name); ok {
				kept++
				if ans == pg {
					t.Errorf("%s [%s]: the kept line now agrees with PostgreSQL (%s): delete it\n  sql %s", c.name, arm.name, why, c.sql)
				} else if ans != want {
					t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  kept %s (%s)", c.name, arm.name, c.sql, ans, want, why)
				}
				continue
			}
			if ans != pg {
				t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, ans, pg)
				continue
			}
			agree++
		}
	}
	if len(cells) < 250 || refused < 60 || agree < 1000 {
		t.Fatalf("%d cells (%d PostgreSQL refusals), %d (cell, arm) agreeing, %d kept: the table must discriminate",
			len(cells), refused, agree, kept)
	}
	t.Logf("%d cells × %d arms: %d agree with PostgreSQL 17.11, %d kept", len(cells), len(arms), agree, kept)
}
