// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// AN UNCORRELATED SUBQUERY'S FAILURE IS RAISED WHEN THE SUBQUERY IS
// EVALUATED, NOT WHEN IT IS PLANNED (#1411 measured case B1), on seven arms.
//
// PostgreSQL 17.11 runs an uncorrelated sublink as an InitPlan on its first
// reference: `CASE WHEN id > 5 THEN EXISTS (… TABLESAMPLE BERNOULLI (101))
// ELSE true END` over ids 1–3 answers 3 (no row reaches the arm) and over
// ids 1–7 raises 2202H (rows 6 and 7 do). A top-level conjunct that reads no
// row is a one-time filter, evaluated before any row: `WHERE EXISTS (…101)`
// raises 2202H over an empty table too.
//
// The stage DAG runs every uncorrelated EXISTS and every scalar subquery it
// cannot defer to a producer stage on the coordinator at plan time, to splice
// its answer in as a constant. At 1edb55e2 a coded failure of that run became
// the statement's answer whatever arm the subquery sat in — CASE WHEN false,
// CASE WHEN id > 5, CASE WHEN true THEN true ELSE …, a HAVING CASE, `x OR
// (NULL AND …)` — on the four DAG arms and the asynchronous door, sampled or
// not (an unsampled 22012 too). Now the failure stands where the answer
// would have (expr.DeferredError, spelled `__deferred_error(…)` in the stage
// text) and raises when a row evaluates it; a conjunct that reads no row is
// evaluated once at plan time, as PostgreSQL's one-time filter is.
//
// The cells are positions × kinds × outcomes × tables, every want PostgreSQL
// 17.11's (tb_author/r4/pg.tsv). Where an arm answers otherwise for a reason
// that is not this rule, the arm's answer is pinned by NAME (tbdExpect): a
// pin that starts agreeing with PostgreSQL FAILS, which is the signal to
// re-measure it and delete the pin.
//
//   - #1190 — a relation with no files has no distributed scan stage: on the
//     four DAG arms a statement over tb_e that reaches a scan answers that
//     uncoded refusal.
//   - unresolved scalar — the DAG's resolution resolves only EXISTS under a
//     boolean connective or CASE; a scalar subquery there ships unresolved
//     and the task refuses it uncoded (base-identical).
//   - ADR-0012 other r21 — over an empty input the embedded engine (and the
//     asynchronous door's task, for the subqueries it evaluates itself)
//     evaluates a subquery per row, so a one-time-filter failure PostgreSQL
//     raises is never reached and the statement answers.
//   - ADR-0012 other r22 — a subquery whose PLANNING fails (an argument real
//     cannot hold, 22003) raises when it is evaluated here, so in an arm no
//     row reaches the statement answers where PostgreSQL, which plans every
//     subquery it does not fold away, raises.
//   - the asynchronous door reports a failure that happens while a task runs
//     as text with no SQLSTATE, and cannot lower a SELECT-list subquery
//     (base-identical door behaviour).
func TestArcTBDeferredSubqueryFailureOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: four DAG arms stand up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := tbdArms(t, ctx)
	cells := tbdCells()
	if len(cells) != len(tbdPostgres) {
		t.Fatalf("%d cells, %d PostgreSQL answers", len(cells), len(tbdPostgres))
	}
	for _, c := range cells {
		pg, ok := tbdPostgres[c.name]
		if !ok {
			t.Fatalf("%s: no PostgreSQL answer measured", c.name)
		}
		for _, arm := range arms {
			want, why := tbdExpect(c, pg, arm.name)
			got := arm.run(c.sql)
			if !tbdMatches(got, want) {
				t.Errorf("%s %s: %s\n  got  %s\n  want %s%s", c.name, arm.name, c.sql, got, want, why)
			}
		}
	}
}

type tbdCell struct {
	name, sql            string
	pos, kind, out, tabl string
}

// The subquery bodies, per outcome: an EXISTS body, a scalar body the stage
// planner cannot prove one row (so it runs it at plan time rather than as a
// producer stage).
var tbdBodies = []struct{ out, exists, scalar string }{
	{"ok", "SELECT 1 FROM tb_big", "SELECT id FROM tb_big ORDER BY id LIMIT 1"},
	{"2202H", "SELECT 1 FROM tb_big TABLESAMPLE BERNOULLI (101)",
		"SELECT id FROM tb_big TABLESAMPLE BERNOULLI (101) ORDER BY id LIMIT 1"},
	{"22003", "SELECT 1 FROM tb_big TABLESAMPLE BERNOULLI (1e400)", ""},
	{"22012", "SELECT 1 FROM tb_p WHERE 1/(id-id) > 0", "SELECT id FROM tb_p WHERE 1/(id-id) > 0 ORDER BY id LIMIT 1"},
	{"22P02", "SELECT 1 FROM tb_p WHERE CAST('x' || CAST(id AS TEXT) AS INT) > 0",
		"SELECT id FROM tb_p WHERE CAST('x' || CAST(id AS TEXT) AS INT) > 0 ORDER BY id LIMIT 1"},
}

var tbdPositions = []struct{ name, tmpl string }{
	{"top", "SELECT count(*) FROM {T} o WHERE {L}"},
	{"and_col", "SELECT count(*) FROM {T} o WHERE o.id > 1 AND {L}"},
	{"or_col", "SELECT count(*) FROM {T} o WHERE o.id = 1 OR {L}"},
	{"or_null_and", "SELECT count(*) FROM {T} o WHERE o.id = 1 OR (NULL AND {L})"},
	{"not", "SELECT count(*) FROM {T} o WHERE NOT ({L})"},
	{"case_false", "SELECT count(*) FROM {T} o WHERE CASE WHEN false THEN {L} ELSE true END"},
	{"case_col", "SELECT count(*) FROM {T} o WHERE CASE WHEN o.id > 5 THEN {L} ELSE true END"},
	{"case_cond", "SELECT count(*) FROM {T} o WHERE CASE WHEN {L} THEN true ELSE true END"},
	{"case_true_else", "SELECT count(*) FROM {T} o WHERE CASE WHEN true THEN true ELSE {L} END"},
	{"cmp", "SELECT count(*) FROM {T} o WHERE ({L}) = true"},
	{"having", "SELECT o.id % 2, count(*) FROM {T} o GROUP BY 1 HAVING {L} ORDER BY 1"},
	{"having_case", "SELECT o.id % 2, count(*) FROM {T} o GROUP BY 1 HAVING CASE WHEN false THEN {L} ELSE true END ORDER BY 1"},
	{"join_on", "SELECT count(*) FROM {T} o JOIN tb_k k ON o.id = k.id AND {L}"},
	{"select_case_col", "SELECT o.id, CASE WHEN o.id > 5 THEN {L} ELSE false END FROM {T} o ORDER BY 1"},
	{"in_list", "SELECT count(*) FROM {T} o WHERE o.id IN (1, {S})"},
}

var tbdTableKeys = []struct{ key, name string }{{"p", "tb_p"}, {"7", "tb_7"}, {"e", "tb_e"}}

func tbdCells() []tbdCell {
	var cells []tbdCell
	for _, b := range tbdBodies {
		leaves := []struct{ kind, leaf string }{
			{"E", "EXISTS (" + b.exists + ")"},
			{"NE", "NOT EXISTS (" + b.exists + ")"},
		}
		if b.scalar != "" {
			leaves = append(leaves, struct{ kind, leaf string }{"SL", "(" + b.scalar + ") >= 0"})
		}
		for _, l := range leaves {
			if b.out == "22003" && l.kind != "E" {
				continue
			}
			for _, p := range tbdPositions {
				if p.name == "in_list" && l.kind != "SL" {
					continue
				}
				for _, tb := range tbdTableKeys {
					sql := strings.NewReplacer("{T}", tb.name, "{L}", l.leaf, "{S}", "("+b.scalar+")").Replace(p.tmpl)
					cells = append(cells, tbdCell{
						name: p.name + "/" + l.kind + "/" + b.out + "/" + tb.key, sql: sql,
						pos: p.name, kind: l.kind, out: b.out, tabl: tb.key,
					})
				}
			}
		}
	}
	return cells
}

// The positions where the stage planner leaves a scalar subquery unresolved
// (the unresolved-scalar pin), and the positions PostgreSQL evaluates as a one-time filter (a
// conjunct that reads no row).
var (
	tbdUnresolvedScalar = map[string]bool{"or_col": true, "or_null_and": true, "not": true,
		"case_false": true, "case_col": true, "case_cond": true, "case_true_else": true, "having_case": true}
	tbdOneTimeFilter = map[string]bool{"top": true, "and_col": true, "not": true, "case_cond": true,
		"cmp": true, "having": true, "join_on": true}
)

// tbdR22 is ADR-0012 other r22: the engine's answer for a subquery whose
// planning fails in an arm no row reaches, per arm ("-" = PostgreSQL's).
var tbdR22 = map[string][7]string{
	"or_col/E/22003/e":   {"0", "0", "UNCODED", "UNCODED", "UNCODED", "UNCODED", "0"},
	"case_col/E/22003/p": {"3", "3", "3", "3", "3", "3", "3"},
	"case_col/E/22003/e": {"0", "0", "UNCODED", "UNCODED", "UNCODED", "UNCODED", "0"},
	"select_case_col/E/22003/p": {"1,false; 2,false; 3,false", "1,false; 2,false; 3,false",
		"1,false; 2,false; 3,false", "1,false; 2,false; 3,false", "1,false; 2,false; 3,false",
		"1,false; 2,false; 3,false", "-"},
	"select_case_col/E/22003/e": {"(0 rows)", "(0 rows)", "(0 rows)", "(0 rows)", "(0 rows)", "(0 rows)", "-"},
}

var tbdArmNames = [7]string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "dag-fastpath64k", "async"}

// tbdExpect is the answer an arm must give: PostgreSQL's, or a named pin.
func tbdExpect(c tbdCell, pg, arm string) (want, why string) {
	idx := 0
	for i, a := range tbdArmNames {
		if a == arm {
			idx = i
		}
	}
	dag := strings.HasPrefix(arm, "dag")
	embedded := arm == "single" || arm == "spilled512k"
	pgErr := strings.HasPrefix(pg, "ERR ")
	empty := "0"
	if strings.HasPrefix(c.pos, "having") || strings.HasPrefix(c.pos, "select") {
		empty = "(0 rows)"
	}
	if r, ok := tbdR22[c.name]; ok && r[idx] != "-" {
		return r[idx], " (pinned: ADR-0012 other r22 — re-measure it)"
	}
	switch {
	case arm == "async" && c.pos == "select_case_col":
		return "UNCODED", " (pinned: the asynchronous door cannot lower a SELECT-list subquery)"
	case dag && c.kind == "SL" && tbdUnresolvedScalar[c.pos]:
		return "UNCODED", " (pinned: a scalar subquery under a connective ships unresolved, an open distributed defect)"
	case dag && c.tabl == "e" && !pgErr && c.pos != "select_case_col":
		return "UNCODED", " (pinned: #1190 — a relation with no files has no distributed scan stage)"
	case c.tabl == "e" && pgErr && ((embedded && tbdOneTimeFilter[c.pos]) ||
		(arm == "async" && c.kind == "SL" && tbdUnresolvedScalar[c.pos])):
		return empty, " (pinned: ADR-0012 other r21 — evaluated per row, nothing reaches the subquery)"
	case arm == "async" && pgErr:
		return "ERROR", " (the asynchronous door reports a task's failure without a SQLSTATE)"
	}
	return pg, ""
}

// tbdMatches: UNCODED = an error with no SQLSTATE, ERROR = any error, else
// the exact rendering.
func tbdMatches(got, want string) bool {
	switch want {
	case "UNCODED":
		return strings.HasPrefix(got, "ERR (uncoded)")
	case "ERROR":
		return strings.HasPrefix(got, "ERR ")
	}
	return got == want
}

type tbdArm struct {
	name string
	run  func(sql string) string
}

func tbdSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeFloat64},
	}}
}

func tbdRows(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1), "v": float64(i+1) * 1.5}
	}
	return rows
}

var tbdTables = []struct {
	name   string
	rows   int
	chunks int
}{{"tb_p", 3, 4}, {"tb_7", 7, 4}, {"tb_e", 0, 4}, {"tb_big", 20000, 4}, {"tb_k", 1000, 4}}

func tbdWrite(t *testing.T, ctx context.Context, infra tmdInfraT) {
	t.Helper()
	for _, tbl := range tbdTables {
		if err := infra.cat.CreateTable(ctx, tbl.name, tbdSchema(), nil); err != nil {
			t.Fatal(err)
		}
		rows := tbdRows(tbl.rows)
		if len(rows) == 0 {
			continue
		}
		per := (len(rows) + tbl.chunks - 1) / tbl.chunks
		var entries []catalog.FileEntry
		for c := 0; c < tbl.chunks; c++ {
			lo, hi := c*per, min(c*per+per, len(rows))
			if lo >= hi {
				break
			}
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, tbdSchema(), parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := pw.WriteRows(rows[lo:hi]); err != nil {
				t.Fatal(err)
			}
			if err := pw.Close(); err != nil {
				t.Fatal(err)
			}
			path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tbl.name, c)
			if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
				t.Fatal(err)
			}
			entries = append(entries, catalog.FileEntry{Path: path, SizeBytes: int64(buf.Len()), NumRows: int64(hi - lo), CreatedAt: time.Now()})
		}
		if err := infra.cat.AddFiles(ctx, tbl.name, map[string]string{}, "tables/"+tbl.name+"/", entries); err != nil {
			t.Fatal(err)
		}
	}
}

func tbdStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
	if budget > 0 {
		cfg.MemoryBudget = budget
		cfg.SpillDir = t.TempDir()
	}
	db, err := wadjet.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tbdTables {
		if err := db.CreateTable(ctx, tbl.name, tbdSchema(), nil); err != nil {
			t.Fatal(err)
		}
		if tbl.rows == 0 {
			continue
		}
		ing := db.NewIngester(tbl.name, tbdSchema(), nil, ingest.Config{MaxBufferRows: tbl.rows + 1, RowGroupSize: 4096})
		if err := ing.Ingest(ctx, tbdRows(tbl.rows)); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func tbdRender(cells [][]any) string {
	if len(cells) == 0 {
		return "(0 rows)"
	}
	if len(cells) > 8 {
		return fmt.Sprintf("(%d rows)", len(cells))
	}
	rows := make([]string, len(cells))
	for i, r := range cells {
		f := make([]string, len(r))
		for j, v := range r {
			f[j] = fmt.Sprint(v)
		}
		rows[i] = strings.Join(f, ",")
	}
	return strings.Join(rows, "; ")
}

func tbdSingle(ctx context.Context, db *wadjet.DB, sql string) string {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	cells := make([][]any, len(res.Rows))
	for i := range res.Rows {
		cells[i] = res.Cells(i)
	}
	return tbdRender(cells)
}

func tbdCollect(ctx context.Context, res *SQLResult) ([][]any, error) {
	var cells [][]any
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, err := st.Next(ctx)
			if err != nil {
				return nil, err
			}
			if bb == nil {
				return cells, nil
			}
			cells = append(cells, bb.ToRowValues()...)
		}
	}
	rows, err := res.Rows()
	if err != nil {
		return nil, err
	}
	cols := res.OutputSchema()
	for _, r := range rows {
		row := make([]any, len(cols))
		for j, col := range cols {
			row[j] = r[col.Name]
		}
		cells = append(cells, row)
	}
	return cells, nil
}

func tbdDAG(ctx context.Context, c *Coordinator, sql string) string {
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	if res.Error != "" {
		return "ERR (uncoded) " + res.Error
	}
	cells, err := tbdCollect(ctx, res)
	if err != nil {
		return tbErr(err)
	}
	return tbdRender(cells)
}

// tbdAsync is the asynchronous submit door: SubmitSQL, then the tracker,
// then GetQueryResults — what internal/server's /v1/queries routes call.
func tbdAsync(ctx context.Context, c *Coordinator, sql string) string {
	qid, _, err := c.SubmitSQL(ctx, sql)
	if err != nil {
		return tbErr(err)
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		info := c.tracker.Get(qid)
		if info != nil && (info.State == QueryStateCompleted || info.State == QueryStateFailed) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	res, err := c.GetQueryResults(ctx, qid)
	if err != nil {
		return tbErr(err)
	}
	if res.Error != "" {
		return "ERR (uncoded) " + res.Error
	}
	cells, err := tbdCollect(ctx, res)
	if err != nil {
		return tbErr(err)
	}
	return tbdRender(cells)
}

func tbdArms(t *testing.T, ctx context.Context) []tbdArm {
	t.Helper()
	single := tbdStandalone(t, ctx, 0)
	spilled := tbdStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tbdWrite(t, ctx, infra)
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	coordS := stand(nil, func(c *Config) { c.LocalFastPathBytes = 64 << 10 })
	return []tbdArm{
		{"single", func(s string) string { return tbdSingle(ctx, single, s) }},
		{"spilled512k", func(s string) string { return tbdSingle(ctx, spilled, s) }},
		{"dag", func(s string) string { return tbdDAG(ctx, coord, s) }},
		{"dag-shuffled", func(s string) string { return tbdDAG(ctx, coordB, s) }},
		{"dag-morsel4", func(s string) string { return tbdDAG(ctx, coordM, s) }},
		{"dag-fastpath64k", func(s string) string { return tbdDAG(ctx, coordS, s) }},
		{"async", func(s string) string { return tbdAsync(ctx, coord, s) }},
	}
}

// tbdPostgres is PostgreSQL 17.11 over the same rows (tb_author/r4/pg.tsv).
var tbdPostgres = map[string]string{
	"and_col/E/22003/7":          "ERR 22003",
	"and_col/E/22003/e":          "ERR 22003",
	"and_col/E/22003/p":          "ERR 22003",
	"and_col/E/22012/7":          "ERR 22012",
	"and_col/E/22012/e":          "ERR 22012",
	"and_col/E/22012/p":          "ERR 22012",
	"and_col/E/2202H/7":          "ERR 2202H",
	"and_col/E/2202H/e":          "ERR 2202H",
	"and_col/E/2202H/p":          "ERR 2202H",
	"and_col/E/22P02/7":          "ERR 22P02",
	"and_col/E/22P02/e":          "ERR 22P02",
	"and_col/E/22P02/p":          "ERR 22P02",
	"and_col/E/ok/7":             "6",
	"and_col/E/ok/e":             "0",
	"and_col/E/ok/p":             "2",
	"and_col/NE/22012/7":         "ERR 22012",
	"and_col/NE/22012/e":         "ERR 22012",
	"and_col/NE/22012/p":         "ERR 22012",
	"and_col/NE/2202H/7":         "ERR 2202H",
	"and_col/NE/2202H/e":         "ERR 2202H",
	"and_col/NE/2202H/p":         "ERR 2202H",
	"and_col/NE/22P02/7":         "ERR 22P02",
	"and_col/NE/22P02/e":         "ERR 22P02",
	"and_col/NE/22P02/p":         "ERR 22P02",
	"and_col/NE/ok/7":            "0",
	"and_col/NE/ok/e":            "0",
	"and_col/NE/ok/p":            "0",
	"and_col/SL/22012/7":         "ERR 22012",
	"and_col/SL/22012/e":         "ERR 22012",
	"and_col/SL/22012/p":         "ERR 22012",
	"and_col/SL/2202H/7":         "ERR 2202H",
	"and_col/SL/2202H/e":         "ERR 2202H",
	"and_col/SL/2202H/p":         "ERR 2202H",
	"and_col/SL/22P02/7":         "ERR 22P02",
	"and_col/SL/22P02/e":         "ERR 22P02",
	"and_col/SL/22P02/p":         "ERR 22P02",
	"and_col/SL/ok/7":            "6",
	"and_col/SL/ok/e":            "0",
	"and_col/SL/ok/p":            "2",
	"case_col/E/22003/7":         "ERR 22003",
	"case_col/E/22003/e":         "ERR 22003",
	"case_col/E/22003/p":         "ERR 22003",
	"case_col/E/22012/7":         "ERR 22012",
	"case_col/E/22012/e":         "0",
	"case_col/E/22012/p":         "3",
	"case_col/E/2202H/7":         "ERR 2202H",
	"case_col/E/2202H/e":         "0",
	"case_col/E/2202H/p":         "3",
	"case_col/E/22P02/7":         "ERR 22P02",
	"case_col/E/22P02/e":         "0",
	"case_col/E/22P02/p":         "3",
	"case_col/E/ok/7":            "7",
	"case_col/E/ok/e":            "0",
	"case_col/E/ok/p":            "3",
	"case_col/NE/22012/7":        "ERR 22012",
	"case_col/NE/22012/e":        "0",
	"case_col/NE/22012/p":        "3",
	"case_col/NE/2202H/7":        "ERR 2202H",
	"case_col/NE/2202H/e":        "0",
	"case_col/NE/2202H/p":        "3",
	"case_col/NE/22P02/7":        "ERR 22P02",
	"case_col/NE/22P02/e":        "0",
	"case_col/NE/22P02/p":        "3",
	"case_col/NE/ok/7":           "5",
	"case_col/NE/ok/e":           "0",
	"case_col/NE/ok/p":           "3",
	"case_col/SL/22012/7":        "ERR 22012",
	"case_col/SL/22012/e":        "0",
	"case_col/SL/22012/p":        "3",
	"case_col/SL/2202H/7":        "ERR 2202H",
	"case_col/SL/2202H/e":        "0",
	"case_col/SL/2202H/p":        "3",
	"case_col/SL/22P02/7":        "ERR 22P02",
	"case_col/SL/22P02/e":        "0",
	"case_col/SL/22P02/p":        "3",
	"case_col/SL/ok/7":           "7",
	"case_col/SL/ok/e":           "0",
	"case_col/SL/ok/p":           "3",
	"case_cond/E/22003/7":        "ERR 22003",
	"case_cond/E/22003/e":        "ERR 22003",
	"case_cond/E/22003/p":        "ERR 22003",
	"case_cond/E/22012/7":        "ERR 22012",
	"case_cond/E/22012/e":        "ERR 22012",
	"case_cond/E/22012/p":        "ERR 22012",
	"case_cond/E/2202H/7":        "ERR 2202H",
	"case_cond/E/2202H/e":        "ERR 2202H",
	"case_cond/E/2202H/p":        "ERR 2202H",
	"case_cond/E/22P02/7":        "ERR 22P02",
	"case_cond/E/22P02/e":        "ERR 22P02",
	"case_cond/E/22P02/p":        "ERR 22P02",
	"case_cond/E/ok/7":           "7",
	"case_cond/E/ok/e":           "0",
	"case_cond/E/ok/p":           "3",
	"case_cond/NE/22012/7":       "ERR 22012",
	"case_cond/NE/22012/e":       "ERR 22012",
	"case_cond/NE/22012/p":       "ERR 22012",
	"case_cond/NE/2202H/7":       "ERR 2202H",
	"case_cond/NE/2202H/e":       "ERR 2202H",
	"case_cond/NE/2202H/p":       "ERR 2202H",
	"case_cond/NE/22P02/7":       "ERR 22P02",
	"case_cond/NE/22P02/e":       "ERR 22P02",
	"case_cond/NE/22P02/p":       "ERR 22P02",
	"case_cond/NE/ok/7":          "7",
	"case_cond/NE/ok/e":          "0",
	"case_cond/NE/ok/p":          "3",
	"case_cond/SL/22012/7":       "ERR 22012",
	"case_cond/SL/22012/e":       "ERR 22012",
	"case_cond/SL/22012/p":       "ERR 22012",
	"case_cond/SL/2202H/7":       "ERR 2202H",
	"case_cond/SL/2202H/e":       "ERR 2202H",
	"case_cond/SL/2202H/p":       "ERR 2202H",
	"case_cond/SL/22P02/7":       "ERR 22P02",
	"case_cond/SL/22P02/e":       "ERR 22P02",
	"case_cond/SL/22P02/p":       "ERR 22P02",
	"case_cond/SL/ok/7":          "7",
	"case_cond/SL/ok/e":          "0",
	"case_cond/SL/ok/p":          "3",
	"case_false/E/22003/7":       "7",
	"case_false/E/22003/e":       "0",
	"case_false/E/22003/p":       "3",
	"case_false/E/22012/7":       "7",
	"case_false/E/22012/e":       "0",
	"case_false/E/22012/p":       "3",
	"case_false/E/2202H/7":       "7",
	"case_false/E/2202H/e":       "0",
	"case_false/E/2202H/p":       "3",
	"case_false/E/22P02/7":       "7",
	"case_false/E/22P02/e":       "0",
	"case_false/E/22P02/p":       "3",
	"case_false/E/ok/7":          "7",
	"case_false/E/ok/e":          "0",
	"case_false/E/ok/p":          "3",
	"case_false/NE/22012/7":      "7",
	"case_false/NE/22012/e":      "0",
	"case_false/NE/22012/p":      "3",
	"case_false/NE/2202H/7":      "7",
	"case_false/NE/2202H/e":      "0",
	"case_false/NE/2202H/p":      "3",
	"case_false/NE/22P02/7":      "7",
	"case_false/NE/22P02/e":      "0",
	"case_false/NE/22P02/p":      "3",
	"case_false/NE/ok/7":         "7",
	"case_false/NE/ok/e":         "0",
	"case_false/NE/ok/p":         "3",
	"case_false/SL/22012/7":      "7",
	"case_false/SL/22012/e":      "0",
	"case_false/SL/22012/p":      "3",
	"case_false/SL/2202H/7":      "7",
	"case_false/SL/2202H/e":      "0",
	"case_false/SL/2202H/p":      "3",
	"case_false/SL/22P02/7":      "7",
	"case_false/SL/22P02/e":      "0",
	"case_false/SL/22P02/p":      "3",
	"case_false/SL/ok/7":         "7",
	"case_false/SL/ok/e":         "0",
	"case_false/SL/ok/p":         "3",
	"case_true_else/E/22003/7":   "7",
	"case_true_else/E/22003/e":   "0",
	"case_true_else/E/22003/p":   "3",
	"case_true_else/E/22012/7":   "7",
	"case_true_else/E/22012/e":   "0",
	"case_true_else/E/22012/p":   "3",
	"case_true_else/E/2202H/7":   "7",
	"case_true_else/E/2202H/e":   "0",
	"case_true_else/E/2202H/p":   "3",
	"case_true_else/E/22P02/7":   "7",
	"case_true_else/E/22P02/e":   "0",
	"case_true_else/E/22P02/p":   "3",
	"case_true_else/E/ok/7":      "7",
	"case_true_else/E/ok/e":      "0",
	"case_true_else/E/ok/p":      "3",
	"case_true_else/NE/22012/7":  "7",
	"case_true_else/NE/22012/e":  "0",
	"case_true_else/NE/22012/p":  "3",
	"case_true_else/NE/2202H/7":  "7",
	"case_true_else/NE/2202H/e":  "0",
	"case_true_else/NE/2202H/p":  "3",
	"case_true_else/NE/22P02/7":  "7",
	"case_true_else/NE/22P02/e":  "0",
	"case_true_else/NE/22P02/p":  "3",
	"case_true_else/NE/ok/7":     "7",
	"case_true_else/NE/ok/e":     "0",
	"case_true_else/NE/ok/p":     "3",
	"case_true_else/SL/22012/7":  "7",
	"case_true_else/SL/22012/e":  "0",
	"case_true_else/SL/22012/p":  "3",
	"case_true_else/SL/2202H/7":  "7",
	"case_true_else/SL/2202H/e":  "0",
	"case_true_else/SL/2202H/p":  "3",
	"case_true_else/SL/22P02/7":  "7",
	"case_true_else/SL/22P02/e":  "0",
	"case_true_else/SL/22P02/p":  "3",
	"case_true_else/SL/ok/7":     "7",
	"case_true_else/SL/ok/e":     "0",
	"case_true_else/SL/ok/p":     "3",
	"cmp/E/22003/7":              "ERR 22003",
	"cmp/E/22003/e":              "ERR 22003",
	"cmp/E/22003/p":              "ERR 22003",
	"cmp/E/22012/7":              "ERR 22012",
	"cmp/E/22012/e":              "ERR 22012",
	"cmp/E/22012/p":              "ERR 22012",
	"cmp/E/2202H/7":              "ERR 2202H",
	"cmp/E/2202H/e":              "ERR 2202H",
	"cmp/E/2202H/p":              "ERR 2202H",
	"cmp/E/22P02/7":              "ERR 22P02",
	"cmp/E/22P02/e":              "ERR 22P02",
	"cmp/E/22P02/p":              "ERR 22P02",
	"cmp/E/ok/7":                 "7",
	"cmp/E/ok/e":                 "0",
	"cmp/E/ok/p":                 "3",
	"cmp/NE/22012/7":             "ERR 22012",
	"cmp/NE/22012/e":             "ERR 22012",
	"cmp/NE/22012/p":             "ERR 22012",
	"cmp/NE/2202H/7":             "ERR 2202H",
	"cmp/NE/2202H/e":             "ERR 2202H",
	"cmp/NE/2202H/p":             "ERR 2202H",
	"cmp/NE/22P02/7":             "ERR 22P02",
	"cmp/NE/22P02/e":             "ERR 22P02",
	"cmp/NE/22P02/p":             "ERR 22P02",
	"cmp/NE/ok/7":                "0",
	"cmp/NE/ok/e":                "0",
	"cmp/NE/ok/p":                "0",
	"cmp/SL/22012/7":             "ERR 22012",
	"cmp/SL/22012/e":             "ERR 22012",
	"cmp/SL/22012/p":             "ERR 22012",
	"cmp/SL/2202H/7":             "ERR 2202H",
	"cmp/SL/2202H/e":             "ERR 2202H",
	"cmp/SL/2202H/p":             "ERR 2202H",
	"cmp/SL/22P02/7":             "ERR 22P02",
	"cmp/SL/22P02/e":             "ERR 22P02",
	"cmp/SL/22P02/p":             "ERR 22P02",
	"cmp/SL/ok/7":                "7",
	"cmp/SL/ok/e":                "0",
	"cmp/SL/ok/p":                "3",
	"having/E/22003/7":           "ERR 22003",
	"having/E/22003/e":           "ERR 22003",
	"having/E/22003/p":           "ERR 22003",
	"having/E/22012/7":           "ERR 22012",
	"having/E/22012/e":           "ERR 22012",
	"having/E/22012/p":           "ERR 22012",
	"having/E/2202H/7":           "ERR 2202H",
	"having/E/2202H/e":           "ERR 2202H",
	"having/E/2202H/p":           "ERR 2202H",
	"having/E/22P02/7":           "ERR 22P02",
	"having/E/22P02/e":           "ERR 22P02",
	"having/E/22P02/p":           "ERR 22P02",
	"having/E/ok/7":              "0,3; 1,4",
	"having/E/ok/e":              "(0 rows)",
	"having/E/ok/p":              "0,1; 1,2",
	"having/NE/22012/7":          "ERR 22012",
	"having/NE/22012/e":          "ERR 22012",
	"having/NE/22012/p":          "ERR 22012",
	"having/NE/2202H/7":          "ERR 2202H",
	"having/NE/2202H/e":          "ERR 2202H",
	"having/NE/2202H/p":          "ERR 2202H",
	"having/NE/22P02/7":          "ERR 22P02",
	"having/NE/22P02/e":          "ERR 22P02",
	"having/NE/22P02/p":          "ERR 22P02",
	"having/NE/ok/7":             "(0 rows)",
	"having/NE/ok/e":             "(0 rows)",
	"having/NE/ok/p":             "(0 rows)",
	"having/SL/22012/7":          "ERR 22012",
	"having/SL/22012/e":          "ERR 22012",
	"having/SL/22012/p":          "ERR 22012",
	"having/SL/2202H/7":          "ERR 2202H",
	"having/SL/2202H/e":          "ERR 2202H",
	"having/SL/2202H/p":          "ERR 2202H",
	"having/SL/22P02/7":          "ERR 22P02",
	"having/SL/22P02/e":          "ERR 22P02",
	"having/SL/22P02/p":          "ERR 22P02",
	"having/SL/ok/7":             "0,3; 1,4",
	"having/SL/ok/e":             "(0 rows)",
	"having/SL/ok/p":             "0,1; 1,2",
	"having_case/E/22003/7":      "0,3; 1,4",
	"having_case/E/22003/e":      "(0 rows)",
	"having_case/E/22003/p":      "0,1; 1,2",
	"having_case/E/22012/7":      "0,3; 1,4",
	"having_case/E/22012/e":      "(0 rows)",
	"having_case/E/22012/p":      "0,1; 1,2",
	"having_case/E/2202H/7":      "0,3; 1,4",
	"having_case/E/2202H/e":      "(0 rows)",
	"having_case/E/2202H/p":      "0,1; 1,2",
	"having_case/E/22P02/7":      "0,3; 1,4",
	"having_case/E/22P02/e":      "(0 rows)",
	"having_case/E/22P02/p":      "0,1; 1,2",
	"having_case/E/ok/7":         "0,3; 1,4",
	"having_case/E/ok/e":         "(0 rows)",
	"having_case/E/ok/p":         "0,1; 1,2",
	"having_case/NE/22012/7":     "0,3; 1,4",
	"having_case/NE/22012/e":     "(0 rows)",
	"having_case/NE/22012/p":     "0,1; 1,2",
	"having_case/NE/2202H/7":     "0,3; 1,4",
	"having_case/NE/2202H/e":     "(0 rows)",
	"having_case/NE/2202H/p":     "0,1; 1,2",
	"having_case/NE/22P02/7":     "0,3; 1,4",
	"having_case/NE/22P02/e":     "(0 rows)",
	"having_case/NE/22P02/p":     "0,1; 1,2",
	"having_case/NE/ok/7":        "0,3; 1,4",
	"having_case/NE/ok/e":        "(0 rows)",
	"having_case/NE/ok/p":        "0,1; 1,2",
	"having_case/SL/22012/7":     "0,3; 1,4",
	"having_case/SL/22012/e":     "(0 rows)",
	"having_case/SL/22012/p":     "0,1; 1,2",
	"having_case/SL/2202H/7":     "0,3; 1,4",
	"having_case/SL/2202H/e":     "(0 rows)",
	"having_case/SL/2202H/p":     "0,1; 1,2",
	"having_case/SL/22P02/7":     "0,3; 1,4",
	"having_case/SL/22P02/e":     "(0 rows)",
	"having_case/SL/22P02/p":     "0,1; 1,2",
	"having_case/SL/ok/7":        "0,3; 1,4",
	"having_case/SL/ok/e":        "(0 rows)",
	"having_case/SL/ok/p":        "0,1; 1,2",
	"in_list/SL/22012/7":         "ERR 22012",
	"in_list/SL/22012/e":         "0",
	"in_list/SL/22012/p":         "ERR 22012",
	"in_list/SL/2202H/7":         "ERR 2202H",
	"in_list/SL/2202H/e":         "0",
	"in_list/SL/2202H/p":         "ERR 2202H",
	"in_list/SL/22P02/7":         "ERR 22P02",
	"in_list/SL/22P02/e":         "0",
	"in_list/SL/22P02/p":         "ERR 22P02",
	"in_list/SL/ok/7":            "1",
	"in_list/SL/ok/e":            "0",
	"in_list/SL/ok/p":            "1",
	"join_on/E/22003/7":          "ERR 22003",
	"join_on/E/22003/e":          "ERR 22003",
	"join_on/E/22003/p":          "ERR 22003",
	"join_on/E/22012/7":          "ERR 22012",
	"join_on/E/22012/e":          "ERR 22012",
	"join_on/E/22012/p":          "ERR 22012",
	"join_on/E/2202H/7":          "ERR 2202H",
	"join_on/E/2202H/e":          "ERR 2202H",
	"join_on/E/2202H/p":          "ERR 2202H",
	"join_on/E/22P02/7":          "ERR 22P02",
	"join_on/E/22P02/e":          "ERR 22P02",
	"join_on/E/22P02/p":          "ERR 22P02",
	"join_on/E/ok/7":             "7",
	"join_on/E/ok/e":             "0",
	"join_on/E/ok/p":             "3",
	"join_on/NE/22012/7":         "ERR 22012",
	"join_on/NE/22012/e":         "ERR 22012",
	"join_on/NE/22012/p":         "ERR 22012",
	"join_on/NE/2202H/7":         "ERR 2202H",
	"join_on/NE/2202H/e":         "ERR 2202H",
	"join_on/NE/2202H/p":         "ERR 2202H",
	"join_on/NE/22P02/7":         "ERR 22P02",
	"join_on/NE/22P02/e":         "ERR 22P02",
	"join_on/NE/22P02/p":         "ERR 22P02",
	"join_on/NE/ok/7":            "0",
	"join_on/NE/ok/e":            "0",
	"join_on/NE/ok/p":            "0",
	"join_on/SL/22012/7":         "ERR 22012",
	"join_on/SL/22012/e":         "ERR 22012",
	"join_on/SL/22012/p":         "ERR 22012",
	"join_on/SL/2202H/7":         "ERR 2202H",
	"join_on/SL/2202H/e":         "ERR 2202H",
	"join_on/SL/2202H/p":         "ERR 2202H",
	"join_on/SL/22P02/7":         "ERR 22P02",
	"join_on/SL/22P02/e":         "ERR 22P02",
	"join_on/SL/22P02/p":         "ERR 22P02",
	"join_on/SL/ok/7":            "7",
	"join_on/SL/ok/e":            "0",
	"join_on/SL/ok/p":            "3",
	"not/E/22003/7":              "ERR 22003",
	"not/E/22003/e":              "ERR 22003",
	"not/E/22003/p":              "ERR 22003",
	"not/E/22012/7":              "ERR 22012",
	"not/E/22012/e":              "ERR 22012",
	"not/E/22012/p":              "ERR 22012",
	"not/E/2202H/7":              "ERR 2202H",
	"not/E/2202H/e":              "ERR 2202H",
	"not/E/2202H/p":              "ERR 2202H",
	"not/E/22P02/7":              "ERR 22P02",
	"not/E/22P02/e":              "ERR 22P02",
	"not/E/22P02/p":              "ERR 22P02",
	"not/E/ok/7":                 "0",
	"not/E/ok/e":                 "0",
	"not/E/ok/p":                 "0",
	"not/NE/22012/7":             "ERR 22012",
	"not/NE/22012/e":             "ERR 22012",
	"not/NE/22012/p":             "ERR 22012",
	"not/NE/2202H/7":             "ERR 2202H",
	"not/NE/2202H/e":             "ERR 2202H",
	"not/NE/2202H/p":             "ERR 2202H",
	"not/NE/22P02/7":             "ERR 22P02",
	"not/NE/22P02/e":             "ERR 22P02",
	"not/NE/22P02/p":             "ERR 22P02",
	"not/NE/ok/7":                "7",
	"not/NE/ok/e":                "0",
	"not/NE/ok/p":                "3",
	"not/SL/22012/7":             "ERR 22012",
	"not/SL/22012/e":             "ERR 22012",
	"not/SL/22012/p":             "ERR 22012",
	"not/SL/2202H/7":             "ERR 2202H",
	"not/SL/2202H/e":             "ERR 2202H",
	"not/SL/2202H/p":             "ERR 2202H",
	"not/SL/22P02/7":             "ERR 22P02",
	"not/SL/22P02/e":             "ERR 22P02",
	"not/SL/22P02/p":             "ERR 22P02",
	"not/SL/ok/7":                "0",
	"not/SL/ok/e":                "0",
	"not/SL/ok/p":                "0",
	"or_col/E/22003/7":           "ERR 22003",
	"or_col/E/22003/e":           "ERR 22003",
	"or_col/E/22003/p":           "ERR 22003",
	"or_col/E/22012/7":           "ERR 22012",
	"or_col/E/22012/e":           "0",
	"or_col/E/22012/p":           "ERR 22012",
	"or_col/E/2202H/7":           "ERR 2202H",
	"or_col/E/2202H/e":           "0",
	"or_col/E/2202H/p":           "ERR 2202H",
	"or_col/E/22P02/7":           "ERR 22P02",
	"or_col/E/22P02/e":           "0",
	"or_col/E/22P02/p":           "ERR 22P02",
	"or_col/E/ok/7":              "7",
	"or_col/E/ok/e":              "0",
	"or_col/E/ok/p":              "3",
	"or_col/NE/22012/7":          "ERR 22012",
	"or_col/NE/22012/e":          "0",
	"or_col/NE/22012/p":          "ERR 22012",
	"or_col/NE/2202H/7":          "ERR 2202H",
	"or_col/NE/2202H/e":          "0",
	"or_col/NE/2202H/p":          "ERR 2202H",
	"or_col/NE/22P02/7":          "ERR 22P02",
	"or_col/NE/22P02/e":          "0",
	"or_col/NE/22P02/p":          "ERR 22P02",
	"or_col/NE/ok/7":             "1",
	"or_col/NE/ok/e":             "0",
	"or_col/NE/ok/p":             "1",
	"or_col/SL/22012/7":          "ERR 22012",
	"or_col/SL/22012/e":          "0",
	"or_col/SL/22012/p":          "ERR 22012",
	"or_col/SL/2202H/7":          "ERR 2202H",
	"or_col/SL/2202H/e":          "0",
	"or_col/SL/2202H/p":          "ERR 2202H",
	"or_col/SL/22P02/7":          "ERR 22P02",
	"or_col/SL/22P02/e":          "0",
	"or_col/SL/22P02/p":          "ERR 22P02",
	"or_col/SL/ok/7":             "7",
	"or_col/SL/ok/e":             "0",
	"or_col/SL/ok/p":             "3",
	"or_null_and/E/22003/7":      "1",
	"or_null_and/E/22003/e":      "0",
	"or_null_and/E/22003/p":      "1",
	"or_null_and/E/22012/7":      "1",
	"or_null_and/E/22012/e":      "0",
	"or_null_and/E/22012/p":      "1",
	"or_null_and/E/2202H/7":      "1",
	"or_null_and/E/2202H/e":      "0",
	"or_null_and/E/2202H/p":      "1",
	"or_null_and/E/22P02/7":      "1",
	"or_null_and/E/22P02/e":      "0",
	"or_null_and/E/22P02/p":      "1",
	"or_null_and/E/ok/7":         "1",
	"or_null_and/E/ok/e":         "0",
	"or_null_and/E/ok/p":         "1",
	"or_null_and/NE/22012/7":     "1",
	"or_null_and/NE/22012/e":     "0",
	"or_null_and/NE/22012/p":     "1",
	"or_null_and/NE/2202H/7":     "1",
	"or_null_and/NE/2202H/e":     "0",
	"or_null_and/NE/2202H/p":     "1",
	"or_null_and/NE/22P02/7":     "1",
	"or_null_and/NE/22P02/e":     "0",
	"or_null_and/NE/22P02/p":     "1",
	"or_null_and/NE/ok/7":        "1",
	"or_null_and/NE/ok/e":        "0",
	"or_null_and/NE/ok/p":        "1",
	"or_null_and/SL/22012/7":     "1",
	"or_null_and/SL/22012/e":     "0",
	"or_null_and/SL/22012/p":     "1",
	"or_null_and/SL/2202H/7":     "1",
	"or_null_and/SL/2202H/e":     "0",
	"or_null_and/SL/2202H/p":     "1",
	"or_null_and/SL/22P02/7":     "1",
	"or_null_and/SL/22P02/e":     "0",
	"or_null_and/SL/22P02/p":     "1",
	"or_null_and/SL/ok/7":        "1",
	"or_null_and/SL/ok/e":        "0",
	"or_null_and/SL/ok/p":        "1",
	"select_case_col/E/22003/7":  "ERR 22003",
	"select_case_col/E/22003/e":  "ERR 22003",
	"select_case_col/E/22003/p":  "ERR 22003",
	"select_case_col/E/22012/7":  "ERR 22012",
	"select_case_col/E/22012/e":  "(0 rows)",
	"select_case_col/E/22012/p":  "1,false; 2,false; 3,false",
	"select_case_col/E/2202H/7":  "ERR 2202H",
	"select_case_col/E/2202H/e":  "(0 rows)",
	"select_case_col/E/2202H/p":  "1,false; 2,false; 3,false",
	"select_case_col/E/22P02/7":  "ERR 22P02",
	"select_case_col/E/22P02/e":  "(0 rows)",
	"select_case_col/E/22P02/p":  "1,false; 2,false; 3,false",
	"select_case_col/E/ok/7":     "1,false; 2,false; 3,false; 4,false; 5,false; 6,true; 7,true",
	"select_case_col/E/ok/e":     "(0 rows)",
	"select_case_col/E/ok/p":     "1,false; 2,false; 3,false",
	"select_case_col/NE/22012/7": "ERR 22012",
	"select_case_col/NE/22012/e": "(0 rows)",
	"select_case_col/NE/22012/p": "1,false; 2,false; 3,false",
	"select_case_col/NE/2202H/7": "ERR 2202H",
	"select_case_col/NE/2202H/e": "(0 rows)",
	"select_case_col/NE/2202H/p": "1,false; 2,false; 3,false",
	"select_case_col/NE/22P02/7": "ERR 22P02",
	"select_case_col/NE/22P02/e": "(0 rows)",
	"select_case_col/NE/22P02/p": "1,false; 2,false; 3,false",
	"select_case_col/NE/ok/7":    "1,false; 2,false; 3,false; 4,false; 5,false; 6,false; 7,false",
	"select_case_col/NE/ok/e":    "(0 rows)",
	"select_case_col/NE/ok/p":    "1,false; 2,false; 3,false",
	"select_case_col/SL/22012/7": "ERR 22012",
	"select_case_col/SL/22012/e": "(0 rows)",
	"select_case_col/SL/22012/p": "1,false; 2,false; 3,false",
	"select_case_col/SL/2202H/7": "ERR 2202H",
	"select_case_col/SL/2202H/e": "(0 rows)",
	"select_case_col/SL/2202H/p": "1,false; 2,false; 3,false",
	"select_case_col/SL/22P02/7": "ERR 22P02",
	"select_case_col/SL/22P02/e": "(0 rows)",
	"select_case_col/SL/22P02/p": "1,false; 2,false; 3,false",
	"select_case_col/SL/ok/7":    "1,false; 2,false; 3,false; 4,false; 5,false; 6,true; 7,true",
	"select_case_col/SL/ok/e":    "(0 rows)",
	"select_case_col/SL/ok/p":    "1,false; 2,false; 3,false",
	"top/E/22003/7":              "ERR 22003",
	"top/E/22003/e":              "ERR 22003",
	"top/E/22003/p":              "ERR 22003",
	"top/E/22012/7":              "ERR 22012",
	"top/E/22012/e":              "ERR 22012",
	"top/E/22012/p":              "ERR 22012",
	"top/E/2202H/7":              "ERR 2202H",
	"top/E/2202H/e":              "ERR 2202H",
	"top/E/2202H/p":              "ERR 2202H",
	"top/E/22P02/7":              "ERR 22P02",
	"top/E/22P02/e":              "ERR 22P02",
	"top/E/22P02/p":              "ERR 22P02",
	"top/E/ok/7":                 "7",
	"top/E/ok/e":                 "0",
	"top/E/ok/p":                 "3",
	"top/NE/22012/7":             "ERR 22012",
	"top/NE/22012/e":             "ERR 22012",
	"top/NE/22012/p":             "ERR 22012",
	"top/NE/2202H/7":             "ERR 2202H",
	"top/NE/2202H/e":             "ERR 2202H",
	"top/NE/2202H/p":             "ERR 2202H",
	"top/NE/22P02/7":             "ERR 22P02",
	"top/NE/22P02/e":             "ERR 22P02",
	"top/NE/22P02/p":             "ERR 22P02",
	"top/NE/ok/7":                "0",
	"top/NE/ok/e":                "0",
	"top/NE/ok/p":                "0",
	"top/SL/22012/7":             "ERR 22012",
	"top/SL/22012/e":             "ERR 22012",
	"top/SL/22012/p":             "ERR 22012",
	"top/SL/2202H/7":             "ERR 2202H",
	"top/SL/2202H/e":             "ERR 2202H",
	"top/SL/2202H/p":             "ERR 2202H",
	"top/SL/22P02/7":             "ERR 22P02",
	"top/SL/22P02/e":             "ERR 22P02",
	"top/SL/22P02/p":             "ERR 22P02",
	"top/SL/ok/7":                "7",
	"top/SL/ok/e":                "0",
	"top/SL/ok/p":                "3",
}
