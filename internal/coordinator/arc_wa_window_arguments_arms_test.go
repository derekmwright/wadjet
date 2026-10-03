// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"os"
	"regexp"
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

// waTable is the window-argument fixture. Three partitions of sizes 3, 2 and
// 1, a NULL value inside the first, and `o` a per-row offset / N that is 0,
// positive, NULL and negative on different rows — so a literal argument that
// is read as no column (NULL), an offset of 0 read as 1 (the neighbour), a
// negative offset read as positive, and a per-row offset read as a constant
// each answer a different row set from PostgreSQL's.
//
//	id | g | x    | o
//	1  | 1 | 10   | 0
//	2  | 1 | 20   | 1
//	3  | 1 | NULL | 2
//	4  | 2 | 40   | 1
//	5  | 2 | 50   | NULL
//	6  | 3 | 60   | -1
func waTables() []tmdTable {
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true} }
	// o is an INTEGER: PostgreSQL's LAG / LEAD / NTILE / NTH_VALUE take an
	// int4 argument, and a bigint one is 42883 before any row is read.
	o := parquet.Column{Name: "o", Type: parquet.TypeInt32, Nullable: true}
	t := tmdTable{name: "wa_t", schema: parquet.Schema{Columns: []parquet.Column{i64("id"), i64("g"), i64("x"), o}}}
	for _, r := range [][4]any{
		{int64(1), int64(1), int64(10), int32(0)},
		{int64(2), int64(1), int64(20), int32(1)},
		{int64(3), int64(1), nil, int32(2)},
		{int64(4), int64(2), int64(40), int32(1)},
		{int64(5), int64(2), int64(50), nil},
		{int64(6), int64(3), int64(60), int32(-1)},
	} {
		t.rows = append(t.rows, map[string]any{"id": r[0], "g": r[1], "x": r[2], "o": r[3]})
	}
	return []tmdTable{t}
}

// waPGFixture is the same fixture as PostgreSQL DDL + INSERT.
const waPGFixture = `DROP TABLE IF EXISTS wa_t;
CREATE TABLE wa_t (id bigint, g bigint, x bigint, o integer);
INSERT INTO wa_t VALUES (1,1,10,0),(2,1,20,1),(3,1,NULL,2),(4,2,40,1),(5,2,50,NULL),(6,3,60,-1);`

type waCell struct{ name, sql string }

// waWindows are the OVER clauses. `all` and `part` have no ORDER BY, so a
// value function over a COLUMN is order-dependent there (ADR-0013) and is
// generated only over a constant.
var waWindows = []struct{ name, over string }{
	{"all", "()"},
	{"part", "(PARTITION BY g)"},
	{"ord", "(ORDER BY id)"},
	{"both", "(PARTITION BY g ORDER BY id)"},
	{"frame", "(ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"},
}

// waCells is the coverage table, generated (the notes enumerate it once):
//
//   - arg/<fn>/<value>/<window>: every function taking a VALUE argument ×
//     every value spelling × every window (#1394's seam);
//   - off/<fn>/<offset>/<window>: LAG / LEAD × every offset spelling (#1399's
//     seam);
//   - def/<fn>/<offset>/<default>/<window>: LAG / LEAD × offset × default;
//   - n/<fn>/<n>/<window>: NTILE / NTH_VALUE × every n spelling, and over an
//     empty input;
//   - ctl/: the argument-less functions and COUNT(*), controls;
//   - issue/: the two issues' own statements, verbatim.
func waCells() []waCell {
	var out []waCell
	values := []struct{ name, v string }{
		{"col", "x"}, {"int", "2"}, {"dec", "2.5"}, {"neg", "-2.5"}, {"text", "'b'"},
		{"null", "NULL"}, {"date", "DATE '2020-01-02'"}, {"arith", "2.5 * 1"},
		{"colexpr", "x + 1"}, {"cast", "CAST(2 AS BIGINT)"},
	}
	fns := []struct {
		name, pre, post string
		value           bool // a value function: order-dependent over a column
	}{
		{"sum", "SUM(", ")", false}, {"avg", "AVG(", ")", false},
		{"min", "MIN(", ")", false}, {"max", "MAX(", ")", false},
		{"count", "COUNT(", ")", false},
		{"first_value", "FIRST_VALUE(", ")", true}, {"last_value", "LAST_VALUE(", ")", true},
		{"nth_value", "NTH_VALUE(", ", 2)", true},
		{"lag", "LAG(", ")", true}, {"lead", "LEAD(", ")", true},
	}
	for _, f := range fns {
		for _, v := range values {
			for _, w := range waWindows {
				// Without an ORDER BY a value function over a COLUMN, and
				// LAG / LEAD over anything (which row has no neighbour), is
				// order-dependent (ADR-0013); FIRST_VALUE / LAST_VALUE /
				// NTH_VALUE over a constant are not.
				columnValued := v.name == "col" || v.name == "colexpr"
				unordered := w.name == "all" || w.name == "part"
				if unordered && f.value && (columnValued || f.name == "lag" || f.name == "lead") {
					continue
				}
				out = append(out, waCell{"arg/" + f.name + "/" + v.name + "/" + w.name,
					"SELECT id, " + f.pre + v.v + f.post + " OVER " + w.over + " FROM wa_t"})
			}
		}
	}
	offsets := []struct{ name, v string }{
		{"omit", ""}, {"0", "0"}, {"1", "1"}, {"2", "2"}, {"10", "10"},
		{"neg1", "-1"}, {"neg2", "-2"}, {"paren_neg1", "-(1)"},
		{"null", "NULL"}, {"null_int", "CAST(NULL AS INTEGER)"},
		{"q0", "'0'"}, {"q_sp2", "' 2 '"}, {"cast_int0", "CAST(0 AS INTEGER)"},
		{"dec", "1.5"}, {"big", "2147483648"}, {"q_dec", "'1.5'"}, {"q_bad", "'a'"},
		{"expr", "1 + 1"}, {"col", "o"}, {"cast_big", "CAST(0 AS BIGINT)"},
		// A CONSTANT expression is folded at plan time — only a per-row
		// argument is refused — and the int4 minimum is one signed
		// literal.
		{"sub", "2 - 1"}, {"abs", "abs(-1)"}, {"cast_expr", "CAST(1 + 0 AS INTEGER)"},
		{"expr_neg", "-(1 + 1)"}, {"expr_null", "NULL + 1"}, {"expr_ovf", "2147483647 + 1"},
		{"expr_bigint", "2147483648 - 1"}, {"expr_num", "1.5 + 0.5"}, {"div0", "1 / 0"},
		{"div", "7 / 4"}, {"pow", "2 ^ 0"}, {"len", "length('a')"}, {"q_plus", "'1' + 1"},
		{"colexpr", "o + 1"}, {"subq", "(SELECT 1)"},
		{"int4min", "-2147483648"}, {"paren_int4min", "-(2147483648)"}, {"neg_int4min", "-(-2147483648)"},
	}
	for _, f := range []string{"lag", "lead"} {
		for _, o := range offsets {
			for _, w := range waWindows {
				if w.name == "all" || w.name == "part" {
					// Order-dependent unless the offset reads the current row.
					if o.name != "0" && o.name != "null" && o.name != "q0" && o.name != "cast_int0" {
						continue
					}
				}
				arg := "x"
				if o.v != "" {
					arg += ", " + o.v
				}
				out = append(out, waCell{"off/" + f + "/" + o.name + "/" + w.name,
					"SELECT id, " + strings.ToUpper(f) + "(" + arg + ") OVER " + w.over + " FROM wa_t"})
			}
		}
		// An offset at and past the input's edge (wa_t has 6 rows): every
		// row answers the default. The spilled streamer used to size a ring
		// by the offset, so ±2147483647 asked for 32 GiB and aborted.
		for _, o := range []struct{ name, v string }{
			{"max", "2147483647"}, {"negmax", "-2147483647"}, {"rows_p1", "7"}, {"neg_rows_p1", "-7"},
			{"rows", "6"}, {"neg_rows", "-6"}, {"rows_m1", "5"}, {"neg_rows_m1", "-5"},
		} {
			for _, w := range waWindows[2:4] {
				out = append(out, waCell{"ring/" + f + "/" + o.name + "/" + w.name,
					"SELECT id, " + strings.ToUpper(f) + "(x, " + o.v + ") OVER " + w.over + " FROM wa_t"})
			}
			out = append(out, waCell{"ring/" + f + "/" + o.name + "/def",
				"SELECT id, " + strings.ToUpper(f) + "(x, " + o.v + ", 99) OVER (ORDER BY id) FROM wa_t"})
		}
		// A LITERAL value at offset 0 — the two issues in one cell.
		out = append(out, waCell{"off/" + f + "/0/literal",
			"SELECT id, " + strings.ToUpper(f) + "(2.5, 0) OVER (ORDER BY id) FROM wa_t"})
	}
	defaults := []struct{ name, v string }{
		{"int", "99"}, {"neg", "-5"}, {"null", "NULL"}, {"dec", "2.5"}, {"text", "'a'"},
		{"q_num", "'7'"}, {"col", "id"}, {"cast", "CAST(7 AS BIGINT)"}, {"colexpr", "id * 100"},
	}
	for _, f := range []string{"lag", "lead"} {
		for _, o := range []string{"1", "0", "10"} {
			for _, d := range defaults {
				for _, w := range waWindows[2:4] {
					out = append(out, waCell{"def/" + f + "/" + o + "/" + d.name + "/" + w.name,
						"SELECT id, " + strings.ToUpper(f) + "(x, " + o + ", " + d.v + ") OVER " + w.over + " FROM wa_t"})
				}
			}
		}
	}
	ns := []struct{ name, v string }{
		{"1", "1"}, {"2", "2"}, {"4", "4"}, {"10", "10"}, {"0", "0"}, {"neg1", "-1"},
		{"null", "NULL"}, {"q2", "'2'"}, {"dec", "2.5"}, {"expr", "1 + 1"}, {"col", "o"},
		{"cast", "CAST(2 AS INTEGER)"},
		{"expr0", "1 + 0"}, {"sub", "2 - 1"}, {"abs", "abs(-2)"}, {"zero_expr", "1 - 1"},
		{"int4min", "-2147483648"}, {"colexpr", "o + 1"},
	}
	for _, n := range ns {
		for _, w := range waWindows[2:] {
			out = append(out,
				waCell{"n/ntile/" + n.name + "/" + w.name, "SELECT id, NTILE(" + n.v + ") OVER " + w.over + " FROM wa_t"},
				waCell{"n/nth_value/" + n.name + "/" + w.name, "SELECT id, NTH_VALUE(x, " + n.v + ") OVER " + w.over + " FROM wa_t"},
				waCell{"n/nth_literal/" + n.name + "/" + w.name, "SELECT id, NTH_VALUE(7, " + n.v + ") OVER " + w.over + " FROM wa_t"})
		}
	}
	// PostgreSQL raises a non-positive N only when a row is evaluated.
	for _, n := range []string{"0", "-1"} {
		out = append(out,
			waCell{"n/ntile/" + n + "/empty", "SELECT id, NTILE(" + n + ") OVER (ORDER BY id) FROM wa_t WHERE id < 0"},
			waCell{"n/nth_value/" + n + "/empty", "SELECT id, NTH_VALUE(x, " + n + ") OVER (ORDER BY id) FROM wa_t WHERE id < 0"})
	}
	for _, w := range waWindows {
		out = append(out,
			waCell{"ctl/count_star/" + w.name, "SELECT id, COUNT(*) OVER " + w.over + " FROM wa_t"})
		if w.name != "all" && w.name != "part" {
			out = append(out, waCell{"ctl/ranks/" + w.name,
				"SELECT id, ROW_NUMBER() OVER " + w.over + ", RANK() OVER (ORDER BY g), DENSE_RANK() OVER (ORDER BY g) FROM wa_t"})
		}
	}
	out = append(out,
		waCell{"issue/1394", "SELECT id, SUM(2.5) OVER (), FIRST_VALUE(2.5) OVER (ORDER BY id), MAX(2.5) OVER (), " +
			"SUM(2) OVER (), SUM(2.5 * 1) OVER () FROM wa_t"},
		// A folded offset in a column the outer query reads by count only.
		waCell{"fold/count_lag_expr", "SELECT count(*) FROM (SELECT LAG(x, 1 + 1) OVER (ORDER BY id) l FROM wa_t) s"},
		waCell{"issue/1399/lag", "SELECT id, LAG(x, 0) OVER (ORDER BY id) FROM wa_t"},
		waCell{"issue/1399/lead", "SELECT id, LEAD(x, 0) OVER (ORDER BY id) FROM wa_t"},
		waCell{"issue/1399/partitioned", "SELECT id, LAG(x, 0) OVER (PARTITION BY id ORDER BY id) FROM wa_t"},
	)
	return out
}

// waPGAnswers reads PostgreSQL 17.11's answer for every generated cell
// (testdata/arc_wa_window_arguments_pg17.tsv).
func waPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_wa_window_arguments_pg17.tsv")
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

// TestArcWAGenerate dumps the cells as name<TAB>sql for the oracle run
// (WA_GEN=<path>); skipped otherwise.
func TestArcWAGenerate(t *testing.T) {
	path := os.Getenv("WA_GEN")
	if path == "" {
		t.Skip("WA_GEN unset")
	}
	var b strings.Builder
	for _, c := range waCells() {
		b.WriteString(c.name + "\t" + c.sql + "\n")
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// waStandalone is one embedded engine over the fixture (budget 0 = none).
func waStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range waTables() {
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

// waArms is the five arms over the fixture.
func waArms(t *testing.T, ctx context.Context) []brArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, waTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single, spilled := waStandalone(t, ctx, 0), waStandalone(t, ctx, 512*1024)
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

// waKept is a cell whose answer here is NOT PostgreSQL's, by a disposition the
// ADR-0012 divergence catalog records (docs/adr/0012-divergences/
// aggregates-windows.md), or a base-identical filing candidate outside this
// arc's two issues, pinned so a change to it is seen. want is this engine's
// answer in brRender form, or "ERR <SQLSTATE> <substring>".
type waKept struct{ want, why string }

func waKeptCells() map[string]waKept {
	kept := map[string]waKept{}
	for _, c := range waCells() {
		parts := strings.Split(c.name, "/")
		switch {
		// A TEXT or NULL literal to a value function: PostgreSQL cannot
		// resolve the polymorphic type of an `unknown` (42804); here the
		// literal is text, or NULL, and answered.
		case parts[0] == "arg" && (parts[2] == "text" || parts[2] == "null") &&
			(parts[1] == "first_value" || parts[1] == "last_value" || parts[1] == "nth_value" ||
				parts[1] == "lag" || parts[1] == "lead"):
			kept[c.name] = waKept{"", "kept superset: an unknown-typed literal argument is read as text / NULL (aggregates-windows.md)"}
		// A per-row integer argument (a column, a column expression, a
		// subquery) is refused, loudly: the operator takes the offset / N as
		// one constant. A constant expression is folded and agrees.
		case (parts[0] == "off" || parts[0] == "n") && (parts[2] == "col" || parts[2] == "colexpr" || parts[2] == "subq"):
			kept[c.name] = waKept{"ERR 0A000 must be a constant here", "refusal: a per-row integer argument (aggregates-windows.md)"}
		// The planner's constant fold types `'1' + 1` (an unknown literal
		// plus an integer) as a numeric where PostgreSQL resolves the
		// unknown to int4: the folded argument is refused 42883, loudly.
		case parts[0] == "off" && parts[2] == "q_plus":
			kept[c.name] = waKept{"ERR 42883 does not exist", "filing candidate: the constant fold types an unknown literal plus an integer as numeric"}
		// AVG over a column at scale 4 here, PostgreSQL's display scale
		// there, where the digits differ past the fourth place: the
		// catalogued value divergence r1 (aggregates-windows.md).
		case c.name == "arg/avg/col/ord" || c.name == "arg/avg/colexpr/ord":
			kept[c.name] = waKept{"", "value divergence r1: AVG answers at scale s+4"}
		}
	}
	// The ANSWERED kept cells pin this engine's rows, measured at the tip.
	for name, rows := range waKeptRows {
		k := kept[name]
		k.want = rows
		kept[name] = k
	}
	return kept
}

// A WINDOW FUNCTION READS ITS ARGUMENT LIST AS POSTGRESQL DOES, ON EVERY ARM
// (#1394 #1399). The generated coverage table (waCells) against PostgreSQL
// 17.11's full sorted rows for every cell. At v0.25.2 a bare LITERAL argument
// was never materialized as an input column, so `SUM(2.5) OVER ()`,
// `FIRST_VALUE(2.5) OVER (…)`, `LAG(5) OVER (…)` and `SUM(2) OVER ()`
// answered NULL on every row and every arm; and an explicit LAG / LEAD offset
// of 0 (or any the planner's strconv.Atoi could not read — NULL, `1 + 1`, a
// column) reached the operator as its zero value and was read as the default
// 1, so `LAG(x, 0)` answered the previous row.
func TestArcWAWindowArgumentsEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the window-argument table")
	}
	answers := waPGAnswers(t)
	kept := waKeptCells()
	cells := waCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	for name := range kept {
		if _, ok := answers[name]; !ok {
			t.Fatalf("kept cell %s is not generated", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	t.Cleanup(cancel)
	arms := waArms(t, ctx)
	agreeing, keptSeen := 0, 0
	for _, tc := range cells {
		want := answers[tc.name]
		why := "PostgreSQL 17.11"
		if k, ok := kept[tc.name]; ok {
			want, why = k.want, k.why
			keptSeen++
		} else {
			agreeing++
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
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					state, msg, _ := strings.Cut(rest, " ")
					if state == "-" {
						state = "" // a runtime failure carrying no SQLSTATE (a pinned filing candidate)
					}
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q (%s)", tc.sql, arm.name, brRender(res), state, msg, why)
					} else if st := sqlerr.StateOf(err); st != state || !waErrorAgrees(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q (%s)", tc.sql, arm.name, st, err, state, msg, why)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s (%s)", tc.sql, arm.name, err, want, why)
					continue
				}
				if got := brRender(res); waNormalize(tc.name, got) != waNormalize(tc.name, want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got, want, why)
				}
			}
		})
	}
	if len(cells) < 700 || agreeing < 600 || keptSeen != len(kept) {
		t.Fatalf("%d cells, %d agreeing with PostgreSQL, %d kept of %d: the table must discriminate",
			len(cells), agreeing, keptSeen, len(kept))
	}
}

// waErrorAgrees compares an error's text with PostgreSQL's message. A
// "function f(t1, t2) does not exist" message names the argument TYPES, which
// the parser that raises it here does not know yet; for that message the
// function's name and "does not exist" are what must agree.
func waErrorAgrees(got, pgMsg string) bool {
	if name, rest, ok := strings.Cut(pgMsg, "("); ok && strings.HasPrefix(name, "function ") &&
		strings.HasSuffix(rest, "does not exist") {
		return strings.Contains(got, name) && strings.Contains(got, "does not exist")
	}
	return strings.Contains(got, pgMsg)
}

// waNormalize strips trailing fractional zeros from an AVG cell's rendering,
// and from a LAG / LEAD cell whose default widens it to numeric.
// AVG over NUMERIC answers at scale s+4 here and at PostgreSQL's display scale
// there — the catalogued value divergence r1 (aggregates-windows.md), which is
// not this table's question; the digits are.
var (
	waTrailingZeros = regexp.MustCompile(`(\.\d*?)0+(,| |$)`)
	waBareDot       = regexp.MustCompile(`\.(,| |$)`)
)

func waNormalize(name, rendered string) string {
	// A LAG / LEAD whose default widens the result to numeric answers at one
	// scale per column — the same catalogued divergence (numeric-decimal r18),
	// since #1435 made the result PostgreSQL's numeric.
	widened := strings.HasPrefix(name, "def/") && strings.Contains(name, "/dec/")
	if !strings.HasPrefix(name, "arg/avg/") && !widened {
		return rendered
	}
	return waBareDot.ReplaceAllString(waTrailingZeros.ReplaceAllString(rendered, "$1$2"), "$1")
}

// waKeptRows is this engine's answer for each ANSWERED kept cell (waKeptCells
// says why each is kept), measured on all five arms at the arc's tip.
var waKeptRows = map[string]string{
	"arg/avg/col/ord":            "rows=6 1,10.0000 | 2,15.0000 | 3,15.0000 | 4,23.3333 | 5,30.0000 | 6,36.0000",
	"arg/avg/colexpr/ord":        "rows=6 1,11.0000 | 2,16.0000 | 3,16.0000 | 4,24.3333 | 5,31.0000 | 6,37.0000",
	"arg/first_value/null/all":   "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/first_value/null/both":  "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/first_value/null/frame": "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/first_value/null/ord":   "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/first_value/null/part":  "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/first_value/text/all":   "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/first_value/text/both":  "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/first_value/text/frame": "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/first_value/text/ord":   "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/first_value/text/part":  "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/lag/null/both":          "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/lag/null/frame":         "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/lag/null/ord":           "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/lag/text/both":          "rows=6 1,NULL | 2,b | 3,b | 4,NULL | 5,b | 6,NULL",
	"arg/lag/text/frame":         "rows=6 1,NULL | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/lag/text/ord":           "rows=6 1,NULL | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/last_value/null/all":    "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/last_value/null/both":   "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/last_value/null/frame":  "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/last_value/null/ord":    "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/last_value/null/part":   "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/last_value/text/all":    "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/last_value/text/both":   "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/last_value/text/frame":  "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/last_value/text/ord":    "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/last_value/text/part":   "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/lead/null/both":         "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/lead/null/frame":        "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/lead/null/ord":          "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/lead/text/both":         "rows=6 1,b | 2,b | 3,NULL | 4,b | 5,NULL | 6,NULL",
	"arg/lead/text/frame":        "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,NULL",
	"arg/lead/text/ord":          "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,NULL",
	"arg/nth_value/null/all":     "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/nth_value/null/both":    "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/nth_value/null/frame":   "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/nth_value/null/ord":     "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/nth_value/null/part":    "rows=6 1,NULL | 2,NULL | 3,NULL | 4,NULL | 5,NULL | 6,NULL",
	"arg/nth_value/text/all":     "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/nth_value/text/both":    "rows=6 1,NULL | 2,b | 3,b | 4,NULL | 5,b | 6,NULL",
	"arg/nth_value/text/frame":   "rows=6 1,NULL | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/nth_value/text/ord":     "rows=6 1,NULL | 2,b | 3,b | 4,b | 5,b | 6,b",
	"arg/nth_value/text/part":    "rows=6 1,b | 2,b | 3,b | 4,b | 5,b | 6,NULL",
}
