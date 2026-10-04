// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A VALUE ROUNDS BY ITS DECLARED TYPE (#381, #1542), on five arms.
//
// PostgreSQL 17.11 rounds a double precision or real value half TO EVEN —
// round(float8) and the float8 → integer cast are both rint() — and a numeric
// value half AWAY from zero. At 89cea148 ROUND chose half to even only when
// its argument was spelled as a CAST to a float type, so `round(f)` over a
// DOUBLE PRECISION column answered 3, 1, -3 for 2.5, 0.5, -2.5 (PostgreSQL 2,
// 0, -2), as did a float expression, a scalar subquery, an aggregate, a
// derived table's float column and a REAL column; and the cast of a float
// array to an integer array cast each element as a bare literal, so
// `ARRAY[2.5::float8, 1.5::float8]::bigint[]` was {3,2} (PostgreSQL {2,2}).
// The rule is now decided in one place (expr.roundsHalfEven) from the
// operand's PostgreSQL category — which, for a column a previous operator
// materialized, is the PLAN's (expr.WithInputPGCategories), because the batch
// holds `5 / 2.0 AS x` and `f AS x` as the same FLOAT64.
//
// The table is generated: operand kind {a DOUBLE PRECISION / REAL / NUMERIC
// column, float8 / float4 / numeric literals, float expressions, a scalar
// subquery, an aggregate, a derived table's float column and a derived
// table's float-carried numeric, a CTE, a set operation} × site {round,
// CAST to SMALLINT / INTEGER / BIGINT, ::, the cast of a one-element array to
// BIGINT[] / INTEGER[], and the consumers WHERE, GROUP BY, an aggregate's
// argument and a window's PARTITION BY over round()} × 15 values (the half-way
// values, the doubles either side of 2.5, 2^52 + 0.5, 1e300, NaN,
// ±Infinity, NULL). Every want is PostgreSQL 17.11's answer to the same
// statement over the same rows (testdata/arc_re_float_round_pg17.tsv; RE_GEN
// regenerates the statements and the fixture).

var reValues = []string{
	"0.5", "1.5", "2.5", "3.5", "-0.5", "-1.5", "-2.5",
	"2.4999999999999996", "2.5000000000000004", "4503599627370496.5",
	"1e300", "NaN", "Infinity", "-Infinity", "NULL",
}

// reFinite reports whether v is a finite number (a NUMERIC column and a bare
// literal hold it); reReal whether float4 holds it (float4in refuses 1e300).
func reFinite(v string) bool {
	switch v {
	case "NaN", "Infinity", "-Infinity", "NULL", "1e300":
		return false
	}
	return true
}

func reReal(v string) bool { return v != "1e300" }

// reHalf reports whether v is a half-way value k/2 with k an integer, which
// `k / 2.0` spells as a float-carried numeric; reTwice is that k.
func reHalf(v string) bool {
	f, err := strconv.ParseFloat(v, 64)
	return err == nil && reFinite(v) && math.Abs(f) < 100 && f*2 == math.Trunc(f*2) && f != math.Trunc(f)
}

func reTwice(v string) string {
	f, _ := strconv.ParseFloat(v, 64)
	return strconv.Itoa(int(f * 2))
}

func reTables() []tmdTable {
	elem := &parquet.Column{Name: "element", Type: parquet.TypeFloat64, Nullable: true}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "n", Type: parquet.TypeDecimal, Precision: 38, Scale: 16, Nullable: true},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "af", Type: parquet.TypeArray, Nullable: true, ElementType: elem},
	}}
	var rows []map[string]any
	for k, v := range reValues {
		row := map[string]any{"id": int64(k + 1), "f": nil, "r": nil, "n": nil, "i": int32(k + 1), "af": nil}
		if v != "NULL" {
			f, _ := strconv.ParseFloat(v, 64)
			row["f"] = f
			row["af"] = []any{f, 1.5}
			if reReal(v) {
				r, _ := strconv.ParseFloat(v, 32)
				row["r"] = float32(r)
			}
			if reFinite(v) {
				row["n"] = v
			}
		}
		rows = append(rows, row)
	}
	return []tmdTable{{"re_v", schema, rows}}
}

// rePGFixture is the same table in PostgreSQL.
func rePGFixture() string {
	var b strings.Builder
	b.WriteString("DROP TABLE IF EXISTS re_v;\nCREATE TABLE re_v (id bigint, f double precision, r real, n numeric(38,16), i integer, af double precision[]);\n")
	for k, v := range reValues {
		f, r, n, af := "NULL", "NULL", "NULL", "NULL"
		if v != "NULL" {
			f = "'" + v + "'::float8"
			af = "ARRAY['" + v + "'::float8, 1.5]"
			if reReal(v) {
				r = "'" + v + "'::real"
			}
			if reFinite(v) {
				n = v
			}
		}
		fmt.Fprintf(&b, "INSERT INTO re_v VALUES (%d, %s, %s, %s, %d, %s);\n", k+1, f, r, n, k+1, af)
	}
	return b.String()
}

type reCell struct{ name, sql string }

// reOperand is one operand kind: its expression and the FROM clause it reads
// (empty for a constant), given the value's row id and spelling.
type reOperand struct {
	key  string
	ok   func(v string) bool
	expr func(id int, v string) (x, from string)
}

func reOperands() []reOperand {
	all := func(string) bool { return true }
	notNull := func(v string) bool { return v != "NULL" }
	where := func(id int) string { return fmt.Sprintf(" FROM re_v WHERE id = %d", id) }
	lit := func(typ string) func(int, string) (string, string) {
		return func(_ int, v string) (string, string) {
			if v == "NULL" {
				return "CAST(NULL AS " + typ + ")", ""
			}
			return "CAST('" + v + "' AS " + typ + ")", ""
		}
	}
	col := func(c string) func(int, string) (string, string) {
		return func(id int, _ string) (string, string) { return c, where(id) }
	}
	return []reOperand{
		{"f8col", all, col("f")},
		{"f4col", reReal, col("r")},
		{"numcol", func(v string) bool { return reFinite(v) || v == "NULL" }, col("n")},
		{"intcol", func(v string) bool { return v == "2.5" }, col("i")},
		{"f8lit", all, lit("DOUBLE PRECISION")},
		{"f4lit", reReal, lit("REAL")},
		{"numlit", reFinite, func(_ int, v string) (string, string) { return v, "" }},
		{"f8mul", all, col("(f * 1.0)")},
		{"f8add", all, col("(f + 0)")},
		{"f8sub", all, func(id int, _ string) (string, string) {
			return fmt.Sprintf("(SELECT f FROM re_v WHERE id = %d)", id), ""
		}},
		{"f8sum", all, col("sum(f)")},
		{"f8max", all, col("max(f)")},
		{"f8derived", all, func(id int, _ string) (string, string) {
			return "s.x", fmt.Sprintf(" FROM (SELECT DISTINCT f AS x FROM re_v WHERE id = %d) s", id)
		}},
		{"numderived", reFinite, func(id int, v string) (string, string) {
			return "s.x", fmt.Sprintf(" FROM (SELECT DISTINCT %s + t.id * 0 AS x FROM re_v t WHERE id = %d) s", v, id)
		}},
		{"f8cte", all, func(id int, _ string) (string, string) {
			return "c.x", fmt.Sprintf(" FROM (SELECT f AS x FROM re_v WHERE id = %d) c", id)
		}},
		{"numcte", reFinite, func(id int, v string) (string, string) {
			return "c.x", fmt.Sprintf(" FROM (SELECT %s + t.id * 0 AS x FROM re_v t WHERE id = %d) c", v, id)
		}},
		{"f8union", notNull, func(id int, _ string) (string, string) {
			return "u.x", fmt.Sprintf(" FROM (SELECT f AS x FROM re_v WHERE id = %d UNION ALL SELECT f FROM re_v WHERE id = %d) u", id, id)
		}},
		// A FLOAT-CARRIED numeric (ADR-0024 §2c): `5 / 2.0` is numeric to
		// PostgreSQL and a double here, so once a previous operator
		// materializes it only the plan knows its type.
		{"numdivderived", reHalf, func(id int, v string) (string, string) {
			return "s.x", fmt.Sprintf(" FROM (SELECT DISTINCT %s / 2.0 + t.id * 0 AS x FROM re_v t WHERE id = %d) s", reTwice(v), id)
		}},
		{"numdivcte", reHalf, func(id int, v string) (string, string) {
			return "c.x", fmt.Sprintf(" FROM (SELECT %s / 2.0 + t.id * 0 AS x FROM re_v t WHERE id = %d) c", reTwice(v), id)
		}},
		{"numdivunion", reHalf, func(_ int, v string) (string, string) {
			return "u.x", fmt.Sprintf(" FROM (SELECT %s / 2.0 AS x UNION ALL SELECT %s / 2.0) u", reTwice(v), reTwice(v))
		}},
		{"numdivmax", reHalf, func(id int, v string) (string, string) {
			return fmt.Sprintf("max(%s / 2.0 + id * 0)", reTwice(v)), fmt.Sprintf(" FROM re_v WHERE id = %d", id)
		}},
		{"numagg", func(v string) bool { return v == "2.5" }, func(_ int, _ string) (string, string) {
			return "avg(i)", " FROM re_v WHERE id IN (2, 3)" // avg(2, 3) = 2.5, numeric
		}},
	}
}

func reCells() []reCell {
	var cells []reCell
	for k, v := range reValues {
		id := k + 1
		for _, o := range reOperands() {
			if !o.ok(v) {
				continue
			}
			x, from := o.expr(id, v)
			key := o.key + "/" + v + "/"
			sites := []struct{ name, sel string }{
				{"round", "round(" + x + ")"},
				{"smallint", "CAST(" + x + " AS SMALLINT)"},
				{"integer", "CAST(" + x + " AS INTEGER)"},
				{"bigint", "CAST(" + x + " AS BIGINT)"},
				{"colon", x + "::bigint"},
			}
			if o.key != "numagg" && o.key != "f8sum" && o.key != "f8max" && o.key != "numdivmax" {
				sites = append(sites,
					struct{ name, sel string }{"arr_bigint", "CAST(ARRAY[" + x + "] AS BIGINT[])"},
					struct{ name, sel string }{"arr_integer", "CAST(ARRAY[" + x + "] AS INTEGER[])"})
			}
			for _, s := range sites {
				cells = append(cells, reCell{key + s.name, "SELECT " + s.sel + " AS a" + from})
			}
			// The consumers of a rounded value, over the operand kinds whose
			// rule comes from a column: WHERE, GROUP BY, an aggregate's
			// argument, a window's PARTITION BY.
			switch o.key {
			case "f8col", "f4col", "numcol", "f8derived", "numderived", "f8cte", "numcte", "numdivderived", "numdivcte":
			default:
				continue
			}
			r := "round(" + x + ")"
			conj := " WHERE "
			if strings.HasPrefix(from, " FROM re_v WHERE") {
				conj = " AND "
			}
			cells = append(cells,
				reCell{key + "where", "SELECT count(*) AS a" + from + conj + r + " < " + x},
				reCell{key + "group", "SELECT " + r + " AS a, count(*) AS b" + from + " GROUP BY " + r},
				reCell{key + "agg", "SELECT sum(" + r + ") AS a" + from},
				reCell{key + "window", "SELECT " + r + " AS a, count(*) OVER (PARTITION BY " + r + ") AS b" + from},
			)
		}
		// The float8[] COLUMN of #1542, {v, 1.5}.
		cells = append(cells,
			reCell{"af/" + v + "/arr_bigint", fmt.Sprintf("SELECT CAST(af AS BIGINT[]) AS a FROM re_v WHERE id = %d", id)},
			reCell{"af/" + v + "/arr_integer", fmt.Sprintf("SELECT af::integer[] AS a FROM re_v WHERE id = %d", id)},
		)
	}
	// The literal spellings of #1542.
	cells = append(cells,
		reCell{"issue1542/arrlit_bigint", "SELECT CAST(ARRAY[2.5::float8, 1.5::float8] AS BIGINT[]) AS a"},
		reCell{"issue1542/arrlit_integer", "SELECT CAST(ARRAY[2.5::float8] AS INTEGER[]) AS a"},
		reCell{"issue1542/arrcast_chain", "SELECT ARRAY[2.5, 1.5]::float8[]::bigint[] AS a"},
		reCell{"issue1542/numeric_array", "SELECT CAST(ARRAY[2.5, 1.5] AS BIGINT[]) AS a"},
		reCell{"issue1542/text_array", "SELECT CAST('{2.5,1.5}'::float8[] AS BIGINT[]) AS a"},
		reCell{"issue1542/real_array", "SELECT CAST(ARRAY[2.5::real, 3.5::real] AS INTEGER[]) AS a"},
		reCell{"issue1542/mixed_array", "SELECT CAST(ARRAY[2.5::float8, 3.5] AS BIGINT[]) AS a"},
		// A quoted literal is `unknown`, and round(unknown) resolves to the
		// float8 overload (float8 is the numeric category's preferred type).
		reCell{"issue381/quoted_literal", "SELECT round('2.5') AS a, round('0.5') AS b, round('-2.5') AS c"},
		// The issue's own table: 2.5, 0.5, -2.5 in a DOUBLE PRECISION column.
		reCell{"issue381/column", "SELECT round(f) AS a FROM re_v WHERE id IN (1, 3, 7) ORDER BY id"},
	)
	return cells
}

// reRender is PostgreSQL's text of a result value: a double's shortest
// round-trip digits (extra_float_digits = 1), Infinity, an array's braces.
func reRender(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case float64:
		return reFloat(t, 64)
	case float32:
		return reFloat(float64(t), 32)
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = reRender(e)
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return fmt.Sprint(v)
}

func reFloat(f float64, bits int) string {
	switch {
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}

func reRenderRows(rows [][]any) string {
	if len(rows) == 0 {
		return "(0 rows)"
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		f := make([]string, len(r))
		for j, v := range r {
			f[j] = reRender(v)
		}
		out[i] = strings.Join(f, ",")
	}
	return strings.Join(out, "; ")
}

func reRunSingle(ctx context.Context, db *wadjet.DB, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	rows := make([][]any, len(res.Rows))
	for i := range res.Rows {
		rows[i] = res.Cells(i)
	}
	return reRenderRows(rows)
}

func reRunDAG(ctx context.Context, c *Coordinator, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	if res.Error != "" {
		return "ERR (uncoded) " + res.Error
	}
	var rows [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(ctx)
			if berr != nil {
				return tbErr(berr)
			}
			if bb == nil {
				break
			}
			rows = append(rows, bb.ToRowValues()...)
		}
	} else {
		rr, rerr := res.Rows()
		if rerr != nil {
			return tbErr(rerr)
		}
		cols := res.OutputSchema()
		for _, r := range rr {
			row := make([]any, len(cols))
			for j, col := range cols {
				row[j] = r[col.Name]
			}
			rows = append(rows, row)
		}
	}
	return reRenderRows(rows)
}

// TestArcREGenerate writes the cells (name<TAB>statement) and the PostgreSQL
// fixture beside them, for the oracle run (RE_GEN=<path>).
func TestArcREGenerate(t *testing.T) {
	path := os.Getenv("RE_GEN")
	if path == "" {
		t.Skip("RE_GEN unset")
	}
	var b strings.Builder
	for _, c := range reCells() {
		fmt.Fprintf(&b, "%s\t%s\n", c.name, c.sql)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fixture.sql", []byte(rePGFixture()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func rePGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_re_float_round_pg17.tsv")
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

func reStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tb := range reTables() {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tb.name, err)
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: len(tb.rows) + 1, RowGroupSize: 4})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatalf("ingest %s: %v", tb.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tb.name, err)
		}
	}
	return db
}

// reStillDiverges names the cells of the table whose answer is NOT this
// arc's rule and is wrong at the base as well, on the arms named ("all" or
// "dag", the three stage-DAG arms), each with its mechanism (recorded in
// re_landing_notes.md as a filing candidate). The gate asserts they still
// disagree with PostgreSQL on those arms and agree on the others, so the
// entry is deleted — the proof — when one starts agreeing.
func reStillDiverges(name string) (arms, why string) {
	switch name {
	// GROUP BY publishes the key -0 as 0 (#1489), and sum() over a lone -0
	// answers 0 where PostgreSQL's float8pl keeps -0: round(-0.5) is -0 here
	// as there, and the consumer loses the sign.
	case "f8col/-0.5/group", "f4col/-0.5/group", "f8derived/-0.5/group", "f8cte/-0.5/group":
		return "all", "#1489"
	case "f8col/-0.5/agg", "f4col/-0.5/agg", "f8derived/-0.5/agg", "f8cte/-0.5/agg":
		return "all", "sum(-0)"
	// A numeric literal past a double's digits loses them inside ARRAY[…]:
	// the element is carried as the float64 4503599627370496 before any cast.
	case "numlit/4503599627370496.5/arr_bigint":
		return "all", "wide literal element"
	}
	switch {
	// GROUP BY / PARTITION BY round(s.x) over a derived table's
	// `<v> + t.id * 0` (a DECIMAL at run time) is declared FLOAT64, and the
	// DECIMAL text it computes is refused by the #361 store guard on the
	// single-process arms (NULL on the DAG arms; a failed window stage).
	case strings.HasPrefix(name, "numderived/") && (strings.HasSuffix(name, "/group") || strings.HasSuffix(name, "/window")):
		return "all", "derived DECIMAL key declared FLOAT64"
	// GROUP BY round(s.x) over a DISTINCT derived table's float-carried
	// numeric reads the key NULL on the stage DAG (the shape of NX-C2).
	case strings.HasPrefix(name, "numdivderived/") && strings.HasSuffix(name, "/group"):
		return "dag", "DAG derived group key reads NULL"
	}
	return "", ""
}

func TestArcREFloatRoundsByDeclaredTypeOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	answers := rePGAnswers(t)
	cells := reCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table (RE_GEN)", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	single := reStandalone(t, ctx, 0)
	spilled := reStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, reTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	arms := []struct {
		name string
		run  func(string) string
	}{
		{"single", func(s string) string { return reRunSingle(ctx, single, s) }},
		{"spilled512k", func(s string) string { return reRunSingle(ctx, spilled, s) }},
		{"dag", func(s string) string { return reRunDAG(ctx, coord, s) }},
		{"dag-shuffled", func(s string) string { return reRunDAG(ctx, coordB, s) }},
		{"dag-morsel4", func(s string) string { return reRunDAG(ctx, coordM, s) }},
	}
	var n, ties int
	for _, c := range cells {
		want := answers[c.name]
		if strings.Contains(c.name, ".5/") {
			ties++
		}
		n++
		t.Run(c.name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got[i] = arm.run(c.sql)
				}()
			}
			wg.Wait()
			pinned, _ := reStillDiverges(c.name)
			for i, arm := range arms {
				known := pinned == "all" || pinned == "dag" && strings.HasPrefix(arm.name, "dag")
				switch {
				case known && got[i] == want:
					t.Errorf("%s: %s now answers PostgreSQL's %s: delete its reStillDiverges case", arm.name, c.sql, want)
				case !known && got[i] != want:
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", arm.name, c.sql, got[i], want)
				}
			}
		})
	}
	// The table is not vacuous: it is the size the generator builds, and
	// most of it is a half-way value.
	if n < 1200 || ties < 700 {
		t.Fatalf("the table shrank: %d cells, %d over a half-way value", n, ties)
	}
}
