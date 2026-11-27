// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The compact semi/anti builds under a budget, on every arm, against
// PostgreSQL 17.11 (#1706, line 1706p). cb_s is a 200,000-row build side
// with 2,500 distinct keys, NULL keys and NULL values and one to three
// distinct values per key; cb_p probes it with NULL keys and values. At a
// small budget the planner's 200,000-row estimate makes the distinct-pair
// (NE) build decline to the generic filtered build, which spills; the
// key-only builds still fit. The cells are the subquery predicates the two
// compact builds serve, with and without a `<>` residual, plus a generic
// residual control and an empty and an all-NULL build side.

const (
	cbBuildRows = 200_000
	cbProbeRows = 3_000
	cbKeys      = 2_500
)

func cbaTables() []tmdTable {
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64, Nullable: true} }
	nullable := func(v int64, null bool) any {
		if null {
			return nil
		}
		return v
	}
	s := tmdTable{name: "cb_s", schema: parquet.Schema{Columns: []parquet.Column{i64("sid"), i64("id"), i64("v")}}}
	for i := 0; i < cbBuildRows; i++ {
		s.rows = append(s.rows, map[string]any{"sid": int64(i),
			"id": nullable(int64(i%cbKeys), i%97 == 0), "v": nullable(int64(i%3), i%89 == 0)})
	}
	p := tmdTable{name: "cb_p", schema: parquet.Schema{Columns: []parquet.Column{i64("rid"), i64("id"), i64("v")}}}
	for i := 0; i < cbProbeRows; i++ {
		// ids run past the build's keys, so some probe rows match nothing.
		p.rows = append(p.rows, map[string]any{"rid": int64(i),
			"id": nullable(int64(i), i%101 == 0), "v": nullable(int64(i%4), i%53 == 0)})
	}
	e := tmdTable{name: "cb_e", schema: parquet.Schema{Columns: []parquet.Column{i64("sid"), i64("id"), i64("v")}}}
	n := tmdTable{name: "cb_n", schema: parquet.Schema{Columns: []parquet.Column{i64("sid"), i64("id"), i64("v")}}}
	for i := 0; i < 2000; i++ {
		n.rows = append(n.rows, map[string]any{"sid": int64(i), "id": nil, "v": int64(i % 3)})
	}
	return []tmdTable{s, p, e, n}
}

func cbaPGFixture() string {
	var b strings.Builder
	lit := func(v any) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprint(v)
	}
	for _, t := range cbaTables() {
		cols := []string{}
		for _, c := range t.schema.Columns {
			cols = append(cols, c.Name+" int8")
		}
		fmt.Fprintf(&b, "DROP TABLE IF EXISTS %s; CREATE TABLE %s (%s);\n", t.name, t.name, strings.Join(cols, ", "))
		for i := 0; i < len(t.rows); i += 1000 {
			fmt.Fprintf(&b, "INSERT INTO %s VALUES ", t.name)
			for j := i; j < min(i+1000, len(t.rows)); j++ {
				if j > i {
					b.WriteString(",")
				}
				vals := []string{}
				for _, c := range t.schema.Columns {
					vals = append(vals, lit(t.rows[j][c.Name]))
				}
				b.WriteString("(" + strings.Join(vals, ",") + ")")
			}
			b.WriteString(";\n")
		}
	}
	return b.String()
}

type cbaCell struct{ name, sql string }

func cbaCells() []cbaCell {
	const sel = "SELECT count(*) AS c, COALESCE(sum(p.rid), -1) AS s FROM cb_p p WHERE "
	preds := []struct{ name, where string }{
		{"in", "p.id IN (SELECT s.id FROM {S} s)"},
		{"notIn", "p.id NOT IN (SELECT s.id FROM {S} s)"},
		{"exists", "EXISTS (SELECT 1 FROM {S} s WHERE s.id = p.id)"},
		{"notExists", "NOT EXISTS (SELECT 1 FROM {S} s WHERE s.id = p.id)"},
		{"existsNE", "EXISTS (SELECT 1 FROM {S} s WHERE s.id = p.id AND s.v <> p.v)"},
		{"notExistsNE", "NOT EXISTS (SELECT 1 FROM {S} s WHERE s.id = p.id AND s.v <> p.v)"},
		{"existsNEflip", "EXISTS (SELECT 1 FROM {S} s WHERE s.id = p.id AND p.v <> s.v)"},
		{"inNE", "p.id IN (SELECT s.id FROM {S} s WHERE s.v <> p.v)"},
		{"notInNE", "p.id NOT IN (SELECT s.id FROM {S} s WHERE s.v <> p.v)"},
		{"existsGT", "EXISTS (SELECT 1 FROM {S} s WHERE s.id = p.id AND s.v > p.v)"},
		{"notExistsGT", "NOT EXISTS (SELECT 1 FROM {S} s WHERE s.id = p.id AND s.v > p.v)"},
		{"inFiltered", "p.id IN (SELECT s.id FROM {S} s WHERE s.v = 1)"},
		{"notInFiltered", "p.id NOT IN (SELECT s.id FROM {S} s WHERE s.v = 1)"},
	}
	var out []cbaCell
	for _, side := range []string{"cb_s", "cb_e", "cb_n"} {
		for _, p := range preds {
			if side == "cb_s" && (p.name == "notInNE" || p.name == "inNE") {
				// A correlated [NOT] IN with a residual is re-run per outer
				// row (3,000 x 200,000 rows): 16 minutes for the seven arms,
				// and it reaches no compact build. The empty and all-NULL
				// sides keep it.
				continue
			}
			out = append(out, cbaCell{side + "/" + p.name, sel + strings.ReplaceAll(p.where, "{S}", side)})
		}
	}
	return out
}

// TestCompactBuildBudgetGenerate dumps the cells (CBA_GEN) and the PostgreSQL
// fixture (CBA_PGDDL) for the oracle run; skipped otherwise.
func TestCompactBuildBudgetGenerate(t *testing.T) {
	cells, ddl := os.Getenv("CBA_GEN"), os.Getenv("CBA_PGDDL")
	if cells == "" && ddl == "" {
		t.Skip("CBA_GEN / CBA_PGDDL unset")
	}
	if cells != "" {
		var b strings.Builder
		for _, c := range cbaCells() {
			b.WriteString(c.name + "\t" + c.sql + "\n")
		}
		if err := os.WriteFile(cells, []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if ddl != "" {
		if err := os.WriteFile(ddl, []byte(cbaPGFixture()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func cbaPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	f, err := os.Open("testdata/compact_build_budget_pg17.tsv")
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
	return out
}

func cbaStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range cbaTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: 50_000, RowGroupSize: 16_384})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

func cbaRender(res *oracle.Result) string {
	if res == nil || len(res.Rows) != 1 {
		return fmt.Sprintf("<%v>", res)
	}
	return fmt.Sprintf("%v|%v", res.Rows[0]["c"], res.Rows[0]["s"])
}

// COMPACT SEMI/ANTI BUILDS ANSWER POSTGRESQL'S ROWS UNDER A BUDGET ON EVERY
// ARM. The embedded engine unbudgeted (the NE and key-only builds), at 4 MiB
// (the NE build declines to the spilling generic build; key-only fits) and at
// 512 KiB, the stage DAG unbudgeted, shuffled, and with a 4 MiB worker pool,
// and the coordinator's local fast path. Any arm answering other than
// PostgreSQL 17.11 fails. A refusal fails too unless it is a compact build's
// honest budget refusal (it cannot spill and its bytes do not fit): those are
// the budget transitions 1706p makes on purpose, and each is logged by cell
// and arm.
func TestCompactBuildBudgetEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: seven arms over a 200,000-row build")
	}
	answers := cbaPGAnswers(t)
	cells := cbaCells()
	if len(answers) != len(cells) {
		t.Fatalf("%d cells, %d PostgreSQL answers: re-measure", len(cells), len(answers))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	t.Cleanup(cancel)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, cbaTables())
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	runSingle := func(db *wadjet.DB) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, db, sql) }
	}
	runDAG := func(c *Coordinator) func(string) (*oracle.Result, error) {
		return func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, c, sql) }
	}
	arms := []brArm{
		{"single", runSingle(cbaStandalone(t, ctx, 0))},
		{"single-4MiB", runSingle(cbaStandalone(t, ctx, 4<<20))},
		{"spilled512k", runSingle(cbaStandalone(t, ctx, 512<<10))},
		{"dag", runDAG(stand(nil))},
		{"dag-shuffled", runDAG(stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-pool4MiB", runDAG(stand(func(w *worker.Config) { w.SharedPoolBudget = 4 << 20 }))},
		{"fastpath", runDAG(stand(nil, func(c *Config) { c.LocalFastPathBytes = 64 << 20 }))},
	}
	dump := os.Getenv("CBA_DUMP")
	var dumpB strings.Builder
	declinedBefore := exec.CompactNEDeclined.Load()
	for _, c := range cells {
		want := answers[c.name]
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(c.sql)
				got, verdict := cbaRender(res), "right"
				switch {
				case err != nil:
					got, verdict = err.Error(), "refused"
				case got != want:
					verdict = "wrong"
				}
				fmt.Fprintf(&dumpB, "%s\t%s\t%s\t%s\n", c.name, arm.name, verdict, got)
				if verdict == "refused" && strings.Contains(got, "this build cannot spill") &&
					strings.Contains(got, "memory budget exceeded") {
					t.Logf("TRANSITION %s on %s: honest compact-build refusal: %s", c.name, arm.name, got)
					continue
				}
				if verdict == "refused" && strings.HasPrefix(c.name, "cb_e/") && strings.HasPrefix(arm.name, "dag") &&
					strings.Contains(got, "has no dependencies and no ScanFiles") {
					// A table with no rows has no files, and this harness's
					// stage DAG refuses to scan it; outside the compact builds
					// (base 085c26e5 refuses the same cells the same way).
					continue
				}
				if verdict != "right" {
					t.Errorf("%s\n  arm %s: %s: %s\n  want %s (PostgreSQL 17.11)", c.sql, arm.name, verdict, got, want)
				}
			}
		})
	}
	t.Logf("NE builds declined to the spilling build during the matrix: %d", exec.CompactNEDeclined.Load()-declinedBefore)
	if dump != "" {
		if err := os.WriteFile(dump, []byte(dumpB.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
