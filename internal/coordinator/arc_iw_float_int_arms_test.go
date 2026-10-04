// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
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

// A FLOAT BECOMES AN INTEGER THROUGH ONE RANGE CHECK (#1484), on five arms.
//
// PostgreSQL 17.11 converts a float8 or float4 to an integer by rint (half to
// even) and then refuses a rounded value outside the destination's range —
// NaN and the infinities included — with 22003, and the explicit CAST and the
// assignment of INSERT / UPDATE / MERGE answer alike on every cell measured.
// At 93e4804e the assignment's copy of the bound compared the rounded double
// against 9.223372036854776e18 — which IS 2^63 — with `>`, so the double 2^63
// passed, converted to -9223372036854775808, and INSERT … SELECT, INSERT …
// VALUES, UPDATE SET and MERGE … UPDATE SET stored it; a REAL 2^63 and the
// REAL nearest 2^63 - 1024 (which is 2^63) did the same. The bound is now
// parquet.FloatToInt64, shared by the CAST kernel, the assignment, the INT64
// vector's float arm, the Go-box writer and the planner's statistic reader.
//
// The table is generated: source {a DOUBLE PRECISION column, a REAL column,
// a float8 literal, a float4 literal, a float8 expression} × destination
// {SMALLINT (cast only: there is no SMALLINT column), INTEGER, BIGINT} × door
// {CAST, ::, INSERT … SELECT, INSERT … VALUES, UPDATE SET, MERGE … UPDATE
// SET} × 24 values (2^63, the largest double below it, ±2^63 ± one step, the
// int4 and int2 edges at ±.5 / ±.4 / ±.6, NaN, ±Infinity, the half-way
// values, -0, 1e300). A write runs on the two single-process arms (the
// coordinator refuses a query-sourced write by name); each write cell reads
// the table back, so a refused statement must leave the rows it found — the
// INSERT stores nothing, the UPDATE and MERGE change nothing. Every want is
// PostgreSQL 17.11's answer to the same statement over the same rows
// (testdata/arc_iw_float_int_pg17.tsv, IW_GEN regenerates the statements).
// The PORT / PROTOCOL cells have no PostgreSQL spelling: their want is the
// type's documented range (docs/data-types.md) after the same rint.

var iwValues = []string{
	"9223372036854775808", "9223372036854774784", "-9223372036854775808", "-9223372036854777856",
	"2147483648", "2147483647.5", "2147483647.4", "-2147483648.5", "-2147483648.4",
	"32768", "32767.5", "32767.4", "-32768.5", "-32768.4",
	"NaN", "Infinity", "-Infinity", "0.5", "1.5", "2.5", "-0.5", "-1.5", "-0", "1e300",
}

type iwCell struct {
	name string
	// stmts run in order; the answer is the write's outcome (when the cell
	// writes) and the last statement's rows.
	stmts []string
	write bool // single-process arms only
	// at is the index of the statement under test; a failure of any other
	// statement is a broken fixture, never an answer.
	at   int
	want string
}

// iwFloat4 reports whether v has a REAL: PostgreSQL's float4in refuses
// 1e300 (22003), so no REAL cell is built over it.
func iwFloat4(v string) bool { return v != "1e300" }

func iwTables() []tmdTable {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
	}}
	rows := []map[string]any{{"id": int64(0), "f": float64(1), "r": float32(1)}}
	for i, v := range iwValues {
		f, _ := strconv.ParseFloat(v, 64)
		row := map[string]any{"id": int64(i + 1), "f": f, "r": nil}
		if iwFloat4(v) {
			r, _ := strconv.ParseFloat(v, 32)
			row["r"] = float32(r)
		}
		rows = append(rows, row)
	}
	return []tmdTable{{"iw_v", schema, rows}}
}

// iwPGFixture is the same table in PostgreSQL.
func iwPGFixture() string {
	var b strings.Builder
	b.WriteString("DROP TABLE IF EXISTS iw_v;\nCREATE TABLE iw_v (id bigint, f double precision, r real);\n")
	b.WriteString("INSERT INTO iw_v VALUES (0, 1, 1);\n")
	for i, v := range iwValues {
		r := "NULL"
		if iwFloat4(v) {
			r = "'" + v + "'::real"
		}
		fmt.Fprintf(&b, "INSERT INTO iw_v VALUES (%d, '%s'::float8, %s);\n", i+1, v, r)
	}
	return b.String()
}

func iwCells() []iwCell {
	var cells []iwCell
	n := 0
	for i, v := range iwValues {
		id := i + 1
		type src struct{ key, typ, col, lit string }
		srcs := []src{{"f8", "DOUBLE PRECISION", "f", "CAST('" + v + "' AS DOUBLE PRECISION)"}}
		if iwFloat4(v) {
			srcs = append(srcs, src{"f4", "REAL", "r", "CAST('" + v + "' AS REAL)"})
		}
		for _, s := range srcs {
			for _, tgt := range []string{"SMALLINT", "INTEGER", "BIGINT"} {
				k := s.key + "/" + strings.ToLower(tgt) + "/" + v + "/"
				where := fmt.Sprintf(" FROM iw_v WHERE id = %d", id)
				cells = append(cells,
					iwCell{name: k + "cast_col", stmts: []string{"SELECT CAST(" + s.col + " AS " + tgt + ")" + where}},
					iwCell{name: k + "colon_col", stmts: []string{"SELECT " + s.col + "::" + tgt + where}},
					iwCell{name: k + "cast_lit", stmts: []string{"SELECT CAST(" + s.lit + " AS " + tgt + ")"}},
					iwCell{name: k + "cast_expr", stmts: []string{"SELECT CAST(" + s.col + " * CAST(1 AS " + s.typ + ") AS " + tgt + ")" + where}},
				)
				if tgt == "SMALLINT" {
					continue // no SMALLINT column type: the write doors have no destination
				}
				n++
				tb := fmt.Sprintf("iw_w%d", n)
				// INSERT … SELECT over two rows, the 1 first: a refusal stores neither.
				cells = append(cells, iwCell{name: k + "insert_select", write: true, at: 1, stmts: []string{
					"CREATE TABLE " + tb + "a (k BIGINT, a " + tgt + ")",
					fmt.Sprintf("INSERT INTO %sa SELECT id, %s FROM iw_v WHERE id IN (0, %d)", tb, s.col, id),
					"SELECT k, a FROM " + tb + "a ORDER BY k",
				}})
				// INSERT … VALUES of the literal (a bound float parameter's
				// spelling, pgwire.floatParamLiteral) after a good row.
				cells = append(cells, iwCell{name: k + "insert_values", write: true, at: 1, stmts: []string{
					"CREATE TABLE " + tb + "b (k BIGINT, a " + tgt + ")",
					"INSERT INTO " + tb + "b VALUES (0, 1), (" + strconv.Itoa(id) + ", " + s.lit + ")",
					"SELECT k, a FROM " + tb + "b ORDER BY k",
				}})
				// UPDATE SET a = <float column> over two rows: a refusal changes neither.
				cells = append(cells, iwCell{name: k + "update", write: true, at: 2, stmts: []string{
					"CREATE TABLE " + tb + "c (k BIGINT, a " + tgt + ", x " + s.typ + ")",
					"INSERT INTO " + tb + "c VALUES (0, 7, 1), (" + strconv.Itoa(id) + ", 7, " + s.lit + ")",
					"UPDATE " + tb + "c SET a = x",
					"SELECT k, a FROM " + tb + "c ORDER BY k",
				}})
				if s.key == "f8" {
					cells = append(cells, iwCell{name: k + "merge_update", write: true, at: 2, stmts: []string{
						"CREATE TABLE " + tb + "d (k BIGINT, a " + tgt + ")",
						"INSERT INTO " + tb + "d VALUES (0, 7), (" + strconv.Itoa(id) + ", 7)",
						"MERGE INTO " + tb + "d USING iw_v ON " + tb + "d.k = iw_v.id WHEN MATCHED THEN UPDATE SET a = iw_v.f",
						"SELECT k, a FROM " + tb + "d ORDER BY k",
					}})
				}
			}
		}
	}
	// PORT and PROTOCOL: wadjet's own int4-backed types, no PostgreSQL
	// spelling. The want is the type's range after the same rint.
	for _, pc := range []struct{ typ, v, want string }{
		{"PORT", "65535.5", "ERR 22003"}, {"PORT", "65535.4", "65535"}, {"PORT", "-0.5", "0"},
		{"PORT", "-0.6", "ERR 22003"}, {"PORT", "NaN", "ERR 22003"}, {"PORT", "9223372036854775808", "ERR 22003"},
		{"PROTOCOL", "255.5", "ERR 22003"}, {"PROTOCOL", "254.5", "254"}, {"PROTOCOL", "2.5", "2"},
	} {
		lit := "CAST('" + pc.v + "' AS DOUBLE PRECISION)"
		k := "spec/" + strings.ToLower(pc.typ) + "/" + pc.v + "/"
		cells = append(cells, iwCell{name: k + "cast_lit", stmts: []string{"SELECT CAST(" + lit + " AS " + pc.typ + ")"}, want: pc.want})
		n++
		tb := fmt.Sprintf("iw_w%d", n)
		w := "0,1; 1," + pc.want
		if strings.HasPrefix(pc.want, "ERR") {
			w = pc.want + " => (0 rows)"
		}
		cells = append(cells, iwCell{name: k + "insert_values", write: true, at: 1, want: w, stmts: []string{
			"CREATE TABLE " + tb + " (k BIGINT, a " + pc.typ + ")",
			"INSERT INTO " + tb + " VALUES (0, 1), (1, " + lit + ")",
			"SELECT k, a FROM " + tb + " ORDER BY k",
		}})
	}
	return cells
}

// iwRunCell runs one cell's statements on one arm. A write cell answers
// "<rows>" when every statement succeeds, or "<the write's error> => <rows>"
// when one fails — the rows the final read finds after the refusal.
func iwRunCell(run func(string) string, c iwCell) string {
	var failed string
	for i, st := range c.stmts[:len(c.stmts)-1] {
		if r := run(st); strings.HasPrefix(r, "ERR") || strings.HasPrefix(r, "PANIC") {
			if i != c.at {
				return "SETUP " + st + ": " + r
			}
			failed = r
		}
	}
	last := run(c.stmts[len(c.stmts)-1])
	if failed != "" {
		return failed + " => " + last
	}
	return last
}

// TestArcIWGenerate writes the cells (name<TAB>statements joined by " ;; ")
// and the PostgreSQL fixture beside them, for the oracle run (IW_GEN=<path>).
func TestArcIWGenerate(t *testing.T) {
	path := os.Getenv("IW_GEN")
	if path == "" {
		t.Skip("IW_GEN unset")
	}
	var b strings.Builder
	for _, c := range iwCells() {
		if c.want != "" {
			continue // a spec cell: no PostgreSQL spelling
		}
		fmt.Fprintf(&b, "%s\t%s\n", c.name, strings.Join(c.stmts, " ;; "))
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fixture.sql", []byte(iwPGFixture()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func iwPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/arc_iw_float_int_pg17.tsv")
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

func iwStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tb := range iwTables() {
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

func TestArcIWFloatToIntegerOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	answers := iwPGAnswers(t)
	cells := iwCells()
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok && c.want == "" {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table (IW_GEN)", c.name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	single := iwStandalone(t, ctx, 0)
	spilled := iwStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, iwTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	arms := []struct {
		name string
		dag  bool
		run  func(string) string
	}{
		{"single", false, func(s string) string { return tbRunSingle(ctx, single, s) }},
		{"spilled512k", false, func(s string) string { return tbRunSingle(ctx, spilled, s) }},
		{"dag", true, func(s string) string { return tbRunDAG(ctx, coord, s) }},
		{"dag-shuffled", true, func(s string) string { return tbRunDAG(ctx, coordB, s) }},
		{"dag-morsel4", true, func(s string) string { return tbRunDAG(ctx, coordM, s) }},
	}
	var writes, reads int
	for _, c := range cells {
		want, why := answers[c.name], "PostgreSQL 17.11"
		if c.want != "" {
			want, why = c.want, "the type's documented range"
		}
		t.Run(c.name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				if c.write && arm.dag {
					continue
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					got[i] = iwRunCell(arm.run, c)
				}()
			}
			wg.Wait()
			for i, arm := range arms {
				if c.write && arm.dag {
					continue
				}
				if got[i] != want {
					t.Errorf("%s: %s\n  got  %s\n  want %s (%s)", arm.name, strings.Join(c.stmts, " ;; "), got[i], want, why)
				}
			}
		})
		if c.write {
			writes++
		} else {
			reads++
		}
	}
	// The table is not vacuous: the write doors and the five-arm reads are
	// both there, at the size the generator builds.
	if writes < 250 || reads < 500 {
		t.Fatalf("the table shrank: %d write cells, %d read cells", writes, reads)
	}
}
