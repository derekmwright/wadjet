// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A COLUMN CREATED FROM AN UNCONSTRAINED NUMERIC (#1541, ADR-0024 §10):
// `CREATE TABLE un_t (… v NUMERIC, w NUMERIC …)` is DECIMAL(38,10) marked
// unconstrained, and every SELECT over it answers PostgreSQL 17.11's rows on
// the five arms — the single-process arms create the tables by DDL and
// INSERT, the three DAG arms read the same declaration (parquet.DeclaredColumn)
// from files. A bare copy of the column prints without trailing zeros
// (`1.25`, `1`) through every consumer that copies it; an expression over it
// prints by the one-scale rule (catalog numeric-decimal r18) and is kept
// below with its reason. At 8e681724 the column was DECIMAL(38,0): 1.25
// stored 1 and 0.755 stored 1, so every cell reading v answered other values.
func TestArcUNUnconstrainedColumnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over an unconstrained NUMERIC column")
	}
	cells := wdConsumerTable(t, "testdata/arc_un_unconstrained_cells.tsv")
	answers := map[string]string{}
	for _, a := range wdConsumerTable(t, "testdata/arc_un_unconstrained_pg17.tsv") {
		answers[a[0]] = a[1]
	}
	kept := unKeptCells()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := unArms(t, ctx)
	for _, c := range cells {
		name, sql := c[0], c[1]
		want, ok := answers[name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", name)
		}
		if k, ok := kept[name]; ok {
			want = k.want
		}
		t.Run(name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(sql)
				if rest, ok := strings.CutPrefix(want, "ERR "); ok {
					state, msg, _ := strings.Cut(rest, " ")
					if err == nil {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s %q", sql, arm.name, unRender(res), state, msg)
					} else if st := sqlerr.StateOf(err); st != state || !strings.Contains(err.Error(), msg) {
						t.Errorf("%s\n  arm  %s\n  got  %s %v\n  want %s %q", sql, arm.name, st, err, state, msg)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s\n  arm  %s\n  refused: %v\n  want %s", sql, arm.name, err, want)
					continue
				}
				if got := unRender(res); got != want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s", sql, arm.name, got, want)
				}
			}
		})
	}
}

// unKeptCells are the cells whose printed answer is not PostgreSQL's text,
// each for a recorded reason; the VALUES are PostgreSQL's unless the reason
// says otherwise.
func unKeptCells() map[string]wdKept {
	r18 := "an expression over the column prints at its declared one scale (numeric-decimal r18)"
	return map[string]wdKept{
		"union_n":  {"type=numeric rows=12 0.7500000000 | 0.7550000000 | 1.0000000000 | 1.2500000000 | 1.2500000000 | 10.0000000000 | 1234567890.0000000000 | 2.5000000000 | 2.5000000000 | 3.3300000000 | NULL | NULL", r18 + "; a set operation over a column of another declaration is not a bare copy"},
		"minmax":   {"type=numeric;numeric rows=1 0.7550000000,1234567890.0000000000", r18},
		"sum":      {"type=numeric rows=1 1234567895.5050000000", r18},
		"sum_g":    {"type=bigint;numeric rows=3 1,3.0050000000 | 2,1234567890.0000000000 | 3,2.5000000000", r18},
		"having":   {"type=bigint;numeric rows=3 1,3.0050000000 | 2,1234567890.0000000000 | 3,2.5000000000", r18},
		"avg":      {"type=numeric rows=1 3.46666666666000", r18 + "; AVG keeps min(s+4, 38) digits (ADR-0024 §2)"},
		"plus":     {"type=numeric rows=6 1,2.2500000000 | 2,1.7550000000 | 3,2.0000000000 | 4,NULL | 5,1234567891.0000000000 | 6,3.5000000000", r18},
		"vw":       {"type=numeric rows=5 1,2.50000000000000000000 | 2,0.37750000000000000000 | 3,NULL | 4,NULL | 6,3.75000000000000000000", r18},
		"x1_vv":    {"ERR 22003 numeric field overflow", "X1: v * v keeps scale 20 (ADR-0024 §3), so more than 18 integer digits is 22003 where PostgreSQL answers"},
		"x1_cast":  {"type=numeric rows=1 5,1524157875019052100.0000000000", r18 + "; the X1 workaround"},
		"x1_vb":    {"type=numeric rows=1 5,1524157875019052100.0000000000", r18},
		"coalesce": {"type=numeric rows=6 1,1.2500000000 | 2,0.7550000000 | 3,1.0000000000 | 4,3.3300000000 | 5,1234567890.0000000000 | 6,2.5000000000", r18},
		"case":     {"type=numeric rows=6 1,1.2500000000 | 2,0.7550000000 | 3,1.0000000000 | 4,3.3300000000 | 5,10.0000000000 | 6,0.7500000000", r18},
		"lag":      {"type=numeric rows=6 1,NULL | 2,1.2500000000 | 3,0.7550000000 | 4,1.0000000000 | 5,NULL | 6,1234567890.0000000000", r18},
		"win_sum":  {"type=numeric rows=6 1,3.0050000000 | 2,3.0050000000 | 3,3.0050000000 | 4,1234567890.0000000000 | 5,1234567890.0000000000 | 6,2.5000000000", r18},
		"scalar":   {"type=numeric rows=6 1,7.0000000000 | 2,7.0000000000 | 3,7.0000000000 | 4,7.0000000000 | 5,7.0000000000 | 6,7.0000000000", r18},
		"div":      {"type=numeric rows=2 1,0.4166666667 | 6,0.8333333333", r18 + "; a quotient keeps max(6, s1 + p2 + 1) digits capped at 38 (ADR-0024 §3)"},
	}
}

// unPGFixture is the fixture as PostgreSQL DDL + INSERT, and the single-process
// arms run it verbatim.
var unPGFixture = []string{
	"CREATE TABLE un_t (id BIGINT, g BIGINT, b BIGINT, v NUMERIC, w NUMERIC, n NUMERIC(10,2))",
	"INSERT INTO un_t VALUES (1,1,10,1.25,2,1.25),(2,1,20,0.755,0.5,2.50),(3,1,NULL,1,NULL,NULL),(4,2,40,NULL,3.3333333333,3.33),(5,2,1234567890,1234567890,10,10.00),(6,3,60,2.5,1.5,0.75)",
	"CREATE TABLE un_u (id BIGINT, v NUMERIC)",
	"INSERT INTO un_u VALUES (1,1.25),(2,7),(7,0.1)",
}

// unTables is the same fixture as the files the DAG arms read: each column's
// declaration is the DDL door's (parquet.DeclaredColumn), and each value is
// stored at that declaration's scale, rounded half away from zero.
func unTables(t *testing.T) []tmdTable {
	t.Helper()
	decl := func(name, typ string) parquet.Column {
		c, err := parquet.DeclaredColumn(name, typ, true)
		if err != nil {
			t.Fatalf("declare %s %s: %v", name, typ, err)
		}
		return c
	}
	mk := func(name string, cols []parquet.Column, rows [][]string) tmdTable {
		tb := tmdTable{name: name, schema: parquet.Schema{Columns: cols}}
		for _, r := range rows {
			row := map[string]any{}
			for i, c := range cols {
				if r[i] == "NULL" {
					row[c.Name] = nil
					continue
				}
				if c.Type == parquet.TypeDecimal {
					row[c.Name] = dtpDecimal128(unUnscaled(t, r[i], c.Scale))
					continue
				}
				n, ok := new(big.Int).SetString(r[i], 10)
				if !ok {
					t.Fatalf("fixture value %q", r[i])
				}
				row[c.Name] = n.Int64()
			}
			tb.rows = append(tb.rows, row)
		}
		return tb
	}
	tc := []parquet.Column{decl("id", "BIGINT"), decl("g", "BIGINT"), decl("b", "BIGINT"),
		decl("v", "NUMERIC"), decl("w", "NUMERIC"), decl("n", "NUMERIC(10,2)")}
	uc := []parquet.Column{decl("id", "BIGINT"), decl("v", "NUMERIC")}
	return []tmdTable{
		mk("un_t", tc, [][]string{{"1", "1", "10", "1.25", "2", "1.25"}, {"2", "1", "20", "0.755", "0.5", "2.50"},
			{"3", "1", "NULL", "1", "NULL", "NULL"}, {"4", "2", "40", "NULL", "3.3333333333", "3.33"},
			{"5", "2", "1234567890", "1234567890", "10", "10.00"}, {"6", "3", "60", "2.5", "1.5", "0.75"}}),
		mk("un_u", uc, [][]string{{"1", "1.25"}, {"2", "7"}, {"7", "0.1"}}),
	}
}

// unUnscaled is s × 10^scale rounded half away from zero.
func unUnscaled(t *testing.T, s string, scale int) *big.Int {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatalf("fixture value %q", s)
	}
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
	num, den := new(big.Int).Abs(r.Num()), r.Denom()
	q, m := new(big.Int).QuoRem(num, den, new(big.Int))
	if new(big.Int).Mul(m, big.NewInt(2)).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if r.Sign() < 0 {
		q.Neg(q)
	}
	return q
}

func unArms(t *testing.T, ctx context.Context) []wdArm {
	t.Helper()
	tables := unTables(t)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tables)
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	single := func(budget int64) *wadjet.DB {
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
		for _, q := range unPGFixture {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		return db
	}
	runSingle := func(db *wadjet.DB) func(string) (wdResult, error) {
		return func(sql string) (res wdResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := db.Query(ctx, sql)
			if qerr != nil {
				return wdResult{}, qerr
			}
			res.schema = out.OutputSchema
			for i := range out.Rows {
				res.rows = append(res.rows, out.Cells(i))
			}
			return res, nil
		}
	}
	runDAG := func(c *Coordinator) func(string) (wdResult, error) {
		return func(sql string) (res wdResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := c.ExecuteSQL(ctx, sql)
			if qerr != nil {
				return wdResult{}, qerr
			}
			if out.Error != "" {
				return wdResult{}, fmt.Errorf("%s", out.Error)
			}
			res.schema = append([]parquet.Column(nil), out.OutputSchema()...)
			st := out.Stream()
			if st == nil {
				rows, rerr := out.Rows()
				if rerr != nil {
					return wdResult{}, rerr
				}
				for _, r := range rows {
					cells := make([]any, len(out.Columns))
					for j, col := range out.Columns {
						cells[j] = r[col]
					}
					res.rows = append(res.rows, cells)
				}
				return res, nil
			}
			defer st.Close()
			for {
				bb, berr := st.Next(ctx)
				if berr != nil {
					return wdResult{}, berr
				}
				if bb == nil {
					break
				}
				res.rows = append(res.rows, bb.ToRowValues()...)
			}
			return res, nil
		}
	}
	dag := stand(nil)
	shuffled := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	morsel := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	return []wdArm{
		{"single", runSingle(single(0)), nil},
		{"spilled512k", runSingle(single(512 * 1024)), nil},
		{"dag", runDAG(dag), dag},
		{"dag-shuffled", runDAG(shuffled), shuffled},
		{"dag-morsel4", runDAG(morsel), morsel},
	}
}

// unRender is wdRender with the doors' printer applied: a DAG arm's stream
// carries the stored scale, and the door that prints it (pgwire's routed
// path) drops the trailing zeros of a column its schema marks unconstrained.
// The marker is read by name so this file compiles against a tree that has
// none (the at-base run).
func unRender(res wdResult) string {
	var types []string
	for i, c := range res.schema {
		if i == 0 && len(res.schema) > 1 && c.Name == "id" {
			continue
		}
		types = append(types, wdPGType(c))
	}
	rows := make([]string, 0, len(res.rows))
	for _, r := range res.rows {
		cells := make([]string, len(r))
		for j, v := range r {
			var c parquet.Column
			if j < len(res.schema) {
				c = res.schema[j]
			}
			cells[j] = wdFmt(c, v)
			if s, ok := v.(string); ok && c.Type == parquet.TypeDecimal && unMarked(c) {
				cells[j] = unTrim(s)
			}
		}
		rows = append(rows, strings.Join(cells, ","))
	}
	sort.Strings(rows)
	return fmt.Sprintf("type=%s rows=%d %s", strings.Join(types, ";"), len(rows), strings.Join(rows, " | "))
}

func unMarked(c parquet.Column) bool {
	f := reflect.ValueOf(c).FieldByName("Unconstrained")
	return f.IsValid() && f.Bool()
}

func unTrim(s string) string {
	dot := strings.IndexByte(s, '.')
	if dot < 0 {
		return s
	}
	end := len(s)
	for end > dot+1 && s[end-1] == '0' {
		end--
	}
	if end == dot+1 {
		end = dot
	}
	return s[:end]
}
