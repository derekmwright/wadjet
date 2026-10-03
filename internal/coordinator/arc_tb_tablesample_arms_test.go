// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE TABLESAMPLE ARGUMENT IS PostgreSQL's REAL, RANGE-CHECKED (#1411), on
// five arms.
//
// PostgreSQL 17.11 reads the argument of BERNOULLI / SYSTEM as ANY constant
// expression coerced to real (float4: tsm_bernoulli / tsm_system declare a
// FLOAT4 parameter). The coercion happens when the statement is planned — a
// value float4 cannot hold (1e39, 1e400, a nonzero 1e-46) is 22003, text is
// 42804 — and the RANGE is checked when the scan begins: NULL, NaN, below 0
// or above 100 is 2202H. 0 samples no row and 100 every row. Because the
// range is a scan-time check, a scan that never begins (WHERE false, LIMIT
// 0) answers, and EXPLAIN shows the plan.
//
// At 6184761c the argument was a number token (or a float parameter's CAST
// spelling) read with strconv.ParseFloat and no check: BERNOULLI (0) and
// SYSTEM (0) answered every row (the sampler was only installed for a
// percentage above 0), every percentage over 100 — 101, 1e20,
// 99999999999999999999, CAST('1e400' AS DOUBLE PRECISION), a bare 1e400 —
// answered every row, and every other expression PostgreSQL takes (a
// negative literal, NULL, '50', 25 * 2, CAST(50 AS NUMERIC)) was 42601.
//
// On the three DAG arms no stage fragment carried the sampler at all: every
// file was read whole, so BERNOULLI (50) over 20 000 rows answered 20 000.
// A sampled scan now runs on the coordinator-local pipeline.
//
// The fixture is tb_p (3 rows), tb_e (no rows) and tb_big (20 000 rows).
// Every want is PostgreSQL 17.11 over the same rows (tb_author/pg/pg.txt). A
// cell PostgreSQL answers with a random count asserts the count's range
// (`RANGE lo hi`); 0 % and 100 % are exact. A cell pinned on every arm is a
// kept divergence named in docs/adr/0012-divergences/table-functions.md: the
// engine must answer the pin, and a pin that starts agreeing FAILS.
func TestArcTBTablesampleArgumentOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := tbArms(t, ctx)
	for _, c := range tbCells() {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				got := arm.run(c.sql)
				if !tbMatches(got, c.want) {
					why := ""
					if c.pinned {
						why = " (a pinned divergence: re-measure it)"
					}
					t.Errorf("%s: %s\n  got  %s\n  want %s%s", arm.name, c.sql, got, c.want, why)
				}
			}
		})
	}
	// The DAG arms answered through the coordinator-local pipeline: a stage
	// fragment carries no sampler, so a sampled scan is routed there
	// (dagplan.refuseTableLessSelect). At 6184761c every DAG arm read every
	// file whole — big/bernoulli_fifty answered 20000.
	for _, arm := range arms {
		if arm.coord != nil && arm.coord.TableLessLocalRoutes() == 0 {
			t.Errorf("%s: no sampled scan was routed to the local pipeline", arm.name)
		}
	}
}

type tbCell struct {
	name, sql, want string
	// pinned marks a kept divergence: want is the ENGINE's answer, and
	// PostgreSQL answers otherwise (the catalog row names both).
	pinned bool
}

// tbMatches compares an answer with a want: the exact rendering, or `RANGE
// lo hi` — one integer row within [lo, hi] — or PLAN, any answer that is not
// an error.
func tbMatches(got, want string) bool {
	switch {
	case want == "PLAN":
		return !strings.HasPrefix(got, "ERR ") && !strings.HasPrefix(got, "PANIC ")
	case strings.HasPrefix(want, "RANGE "):
		f := strings.Fields(want)
		lo, _ := strconv.Atoi(f[1])
		hi, _ := strconv.Atoi(f[2])
		n, err := strconv.Atoi(got)
		return err == nil && n >= lo && n <= hi
	}
	return got == want
}

func tbCells() []tbCell {
	var cells []tbCell
	add := func(name, sql, want string) { cells = append(cells, tbCell{name: name, sql: sql, want: want}) }
	pin := func(name, sql, want string) {
		cells = append(cells, tbCell{name: name, sql: sql, want: want, pinned: true})
	}
	count := func(method, arg string) string {
		return "SELECT count(*) FROM tb_p TABLESAMPLE " + method + " (" + arg + ")"
	}

	// The argument, BERNOULLI over tb_p (3 rows), COUNT(*).
	for _, a := range []struct{ name, arg, want string }{
		// exact: 0 % and 100 %, through every spelling of the number
		{"zero", "0", "0"},
		{"zero_point", "0.0", "0"},
		{"minus_zero", "-0.0", "0"},
		{"minus_zero_text", "'-0'", "0"},
		{"hundred", "100", "3"},
		{"hundred_point", "100.0", "3"},
		{"hundred_float8", "CAST('100' AS DOUBLE PRECISION)", "3"},
		// float4 rounds these to 100 (measured: PostgreSQL answers every row)
		{"hundred_e6_numeric", "100.000001", "3"},
		{"hundred_e7_numeric", "100.0000001", "3"},
		{"hundred_e6_text", "'100.000001'", "3"},
		{"hundred_e6_float8", "CAST('100.000001' AS DOUBLE PRECISION)", "3"},
		{"hundred_numeric_scale2", "CAST(100.00001 AS NUMERIC(10,2))", "3"},
		{"hundred_parenthesized", "((100))", "3"},
		// a sample: any count is PostgreSQL's
		{"fifty", "50", "RANGE 0 3"},
		{"fifty_real", "CAST('50' AS REAL)", "RANGE 0 3"},
		{"fifty_numeric", "CAST(50 AS NUMERIC)", "RANGE 0 3"},
		{"fifty_smallint", "CAST(50 AS SMALLINT)", "RANGE 0 3"},
		{"fifty_bigint", "CAST(50 AS BIGINT)", "RANGE 0 3"},
		{"fifty_text_literal", "'50'", "RANGE 0 3"},
		{"fifty_text_padded", "' 50 '", "RANGE 0 3"},
		{"fifty_product", "25 * 2", "RANGE 0 3"},
		{"fifty_double_negation", "-(-50)", "RANGE 0 3"},
		{"fifty_unary_plus", "+50", "RANGE 0 3"},
		{"fifty_abs", "abs(-50)", "RANGE 0 3"},
		{"fifty_case", "CASE WHEN true THEN 50 END", "RANGE 0 3"},
		{"denormal_1e45", "1e-45", "RANGE 0 3"},
		{"denormal_1e40_float8", "CAST('1e-40' AS DOUBLE PRECISION)", "RANGE 0 3"},
		// out of range: 2202H
		{"over_101", "101", "ERR 2202H"},
		{"over_100_00001", "100.00001", "ERR 2202H"},
		{"minus_one", "-1", "ERR 2202H"},
		{"minus_fifty_spaced", "- 50", "ERR 2202H"},
		{"minus_five_int", "CAST(-5 AS INTEGER)", "ERR 2202H"},
		{"e20", "1e20", "ERR 2202H"},
		{"twenty_nines", "99999999999999999999", "ERR 2202H"},
		{"int8_max", "9223372036854775807", "ERR 2202H"},
		{"real_max_ish", "3.4e38", "ERR 2202H"},
		{"e20_float8", "CAST('1e20' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"nan_float8", "CAST('NaN' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"inf_float8", "CAST('Infinity' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"minus_inf_float8", "CAST('-Infinity' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"inf_text", "'Infinity'", "ERR 2202H"},
		{"inf_text_short", "'inf'", "ERR 2202H"},
		{"nan_text", "'NaN'", "ERR 2202H"},
		{"null", "NULL", "ERR 2202H"},
		{"null_int", "CAST(NULL AS INTEGER)", "ERR 2202H"},
		{"null_numeric", "CAST(NULL AS NUMERIC)", "ERR 2202H"},
		{"null_float8", "CAST(NULL AS DOUBLE PRECISION)", "ERR 2202H"},
		// float4 cannot hold the value: 22003 at the coercion
		{"e400", "1e400", "ERR 22003"},
		{"e39", "1e39", "ERR 22003"},
		{"e_minus46", "1e-46", "ERR 22003"},
		{"e400_float8", "CAST('1e400' AS DOUBLE PRECISION)", "ERR 22003"},
		{"e39_float8", "CAST('1e39' AS DOUBLE PRECISION)", "ERR 22003"},
		{"e_minus46_float8", "CAST('1e-46' AS DOUBLE PRECISION)", "ERR 22003"},
		{"e39_real", "CAST('1e39' AS REAL)", "ERR 22003"},
		{"e400_text", "'1e400'", "ERR 22003"},
		{"e39_text", "'1e39'", "ERR 22003"},
		// the argument's own failure
		{"text_abc", "'abc'", "ERR 22P02"},
		{"text_empty", "''", "ERR 22P02"},
		{"div_zero", "1/0", "ERR 22012"},
		{"sqrt_negative", "sqrt(-1)", "ERR 2201F"},
		// not a real: 42804
		{"text_cast", "CAST('50' AS TEXT)", "ERR 42804"},
		{"text_concat", "'5' || '0'", "ERR 42804"},
		{"varchar_cast", "CAST('1' AS VARCHAR)", "ERR 42804"},
		{"bool", "true", "ERR 42804"},
		{"null_text", "CAST(NULL AS TEXT)", "ERR 42804"},
		{"null_bool", "CAST(NULL AS BOOLEAN)", "ERR 42804"},
		{"date", "DATE '2024-01-01'", "ERR 42804"},
		// a column is not in scope there: 42703; an aggregate: 42803
		{"column", "id", "ERR 42703"},
		{"aggregate", "count(*)", "ERR 42803"},
	} {
		add("bernoulli/"+a.name, count("BERNOULLI", a.arg), a.want)
	}
	// A subquery is folded by PostgreSQL (an InitPlan) and answers; the
	// argument is evaluated here before any row exists, so it is refused.
	pin("bernoulli/subquery", count("BERNOULLI", "(SELECT 50)"), "ERR 0A000")

	// SYSTEM: the same argument rule.
	for _, a := range []struct{ name, arg, want string }{
		{"zero", "0", "0"},
		{"hundred", "100", "3"},
		{"over_101", "101", "ERR 2202H"},
		{"minus_one", "-1", "ERR 2202H"},
		{"null", "NULL", "ERR 2202H"},
		{"nan_float8", "CAST('NaN' AS DOUBLE PRECISION)", "ERR 2202H"},
		{"e400", "1e400", "ERR 22003"},
		{"text_cast", "CAST('50' AS TEXT)", "ERR 42804"},
	} {
		add("system/"+a.name, count("SYSTEM", a.arg), a.want)
	}

	// The table: no rows (the range is still checked — the scan begins) and
	// 20 000 rows (0 % none, 100 % all, 50 % a count within bounds).
	add("empty/bernoulli_101", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (101)", "ERR 2202H")
	add("empty/bernoulli_minus_one", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (-1)", "ERR 2202H")
	add("empty/bernoulli_null", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (NULL)", "ERR 2202H")
	add("empty/bernoulli_zero", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (0)", "0")
	add("empty/bernoulli_e400", "SELECT count(*) FROM tb_e TABLESAMPLE BERNOULLI (CAST('1e400' AS DOUBLE PRECISION))", "ERR 22003")
	add("empty/system_101", "SELECT count(*) FROM tb_e TABLESAMPLE SYSTEM (101)", "ERR 2202H")
	add("empty/system_zero", "SELECT count(*) FROM tb_e TABLESAMPLE SYSTEM (0)", "0")
	add("big/bernoulli_zero", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (0)", "0")
	add("big/bernoulli_hundred", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (100)", "20000")
	add("big/bernoulli_fifty", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (50)", "RANGE 9000 11000")
	add("big/bernoulli_101", "SELECT count(*) FROM tb_big TABLESAMPLE BERNOULLI (101)", "ERR 2202H")
	add("big/system_zero", "SELECT count(*) FROM tb_big TABLESAMPLE SYSTEM (0)", "0")
	add("big/system_hundred", "SELECT count(*) FROM tb_big TABLESAMPLE SYSTEM (100)", "20000")
	add("big/system_fifty", "SELECT count(*) FROM tb_big TABLESAMPLE SYSTEM (50)", "RANGE 0 20000")

	// The consumers above a sampled scan.
	add("projection/zero", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (0) ORDER BY id", "(0 rows)")
	add("projection/hundred", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (100) ORDER BY id", "1; 2; 3")
	add("projection/101", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) ORDER BY id", "ERR 2202H")
	add("projection/101_filtered", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE id > 5", "ERR 2202H")
	add("join/zero", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (0) JOIN tb_p q ON tb_p.id = q.id", "0")
	add("join/hundred", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (100) JOIN tb_p q ON tb_p.id = q.id", "3")
	add("join/101", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) JOIN tb_p q ON tb_p.id = q.id", "ERR 2202H")
	add("join/comma_zero", "SELECT count(*) FROM tb_p q, tb_p TABLESAMPLE BERNOULLI (0)", "0")
	add("cte/zero", "WITH c AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (0)) SELECT count(*) FROM c", "0")
	add("cte/101", "WITH c AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (101)) SELECT count(*) FROM c", "ERR 2202H")
	add("cte/e400", "WITH c AS (SELECT * FROM tb_p TABLESAMPLE BERNOULLI (1e400)) SELECT count(*) FROM c", "ERR 22003")
	add("subquery/exists_101", "SELECT 1 WHERE EXISTS (SELECT 1 FROM tb_p TABLESAMPLE BERNOULLI (101))", "ERR 2202H")
	add("subquery/scalar_101", "SELECT (SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101))", "ERR 2202H")
	add("union/101", "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (101) UNION ALL SELECT 1", "ERR 2202H")
	// The range is a scan-time check, and PostgreSQL never begins a scan
	// under WHERE false or LIMIT 0, so it answers zero rows. This engine's
	// pipeline pulls the scan's first batch in both shapes, so the check runs
	// (kept refusal: loud where PostgreSQL answers nothing).
	pin("never_scanned/where_false", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) WHERE false", "ERR 2202H")
	pin("never_scanned/limit_zero", "SELECT id FROM tb_p TABLESAMPLE BERNOULLI (101) LIMIT 0", "ERR 2202H")
	// EXPLAIN plans without scanning; the coercion's own failure is raised.
	add("explain/101", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (101)", "PLAN")
	add("explain/null", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (NULL)", "PLAN")
	add("explain/minus_one", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (-1)", "PLAN")
	add("explain/e400", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (1e400)", "ERR 22003")
	add("explain/div_zero", "EXPLAIN SELECT * FROM tb_p TABLESAMPLE BERNOULLI (1/0)", "ERR 22012")

	// REPEATABLE is not parsed: its refusal is kept (PostgreSQL answers
	// (1), (0), (-1); raises 22003 for (1e400) and 2202G for (NULL)).
	for _, s := range []string{"1", "0", "-1", "1e400", "NULL"} {
		pin("repeatable/"+s, "SELECT count(*) FROM tb_p TABLESAMPLE BERNOULLI (100) REPEATABLE ("+s+")", "ERR 42601")
	}
	return cells
}

func tbSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}}
}

func tbRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1), "v": float64(i+1) * 1.5}
	}
	return rows
}

func tbTables() []tmdTable {
	return []tmdTable{
		{"tb_p", tbSchema(), tbRows(3)},
		{"tb_e", tbSchema(), nil},
		{"tb_big", tbSchema(), tbRows(20000)},
	}
}

func tbStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tb := range tbTables() {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tb.name, err)
		}
		if len(tb.rows) == 0 {
			continue
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: len(tb.rows) + 1, RowGroupSize: 4096})
		if err := ing.Ingest(ctx, tb.rows); err != nil {
			t.Fatalf("ingest %s: %v", tb.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tb.name, err)
		}
	}
	return db
}

type tbArm struct {
	name  string
	run   func(sql string) string
	coord *Coordinator // nil for an embedded arm
}

func tbArms(t *testing.T, ctx context.Context) []tbArm {
	t.Helper()
	single := tbStandalone(t, ctx, 0)
	spilled := tbStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tbTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	return []tbArm{
		{"single", func(s string) string { return tbRunSingle(ctx, single, s) }, nil},
		{"spilled512k", func(s string) string { return tbRunSingle(ctx, spilled, s) }, nil},
		{"dag", func(s string) string { return tbRunDAG(ctx, coord, s) }, coord},
		{"dag-shuffled", func(s string) string { return tbRunDAG(ctx, coordB, s) }, coordB},
		{"dag-morsel4", func(s string) string { return tbRunDAG(ctx, coordM, s) }, coordM},
	}
}

func tbErr(err error) string {
	if s := sqlerr.StateOf(err); s != "" {
		return "ERR " + s
	}
	return "ERR (uncoded) " + err.Error()
}

func tbRender(cells [][]any) string {
	if len(cells) == 0 {
		return "(0 rows)"
	}
	rows := make([]string, len(cells))
	for i, r := range cells {
		f := make([]string, len(r))
		for j, v := range r {
			if v == nil {
				f[j] = "NULL"
			} else {
				f[j] = fmt.Sprint(v)
			}
		}
		rows[i] = strings.Join(f, ",")
	}
	return strings.Join(rows, "; ")
}

func tbRunSingle(ctx context.Context, db *wadjet.DB, sql string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("PANIC %v", r)
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	cells := make([][]any, len(res.Rows))
	for i := range res.Rows {
		cells[i] = res.Cells(i)
	}
	return tbRender(cells)
}

func tbRunDAG(ctx context.Context, c *Coordinator, sql string) (out string) {
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
	var cells [][]any
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
			cells = append(cells, bb.ToRowValues()...)
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return tbErr(rerr)
		}
		cols := res.OutputSchema()
		for _, r := range rows {
			row := make([]any, len(cols))
			for j, col := range cols {
				row[j] = r[col.Name]
			}
			cells = append(cells, row)
		}
	}
	return tbRender(cells)
}
