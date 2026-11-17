// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE GROUPING-SET DRAIN TABLE ON ALL FIVE ARMS (#1700). The embedded table is
// wadjet's TestGroupingSetDrain (../../wadjet/testdata/grouping_set_drain_pg17.tsv:
// grouping construct x WHERE x aggregate x key spelling over two fixtures,
// every want PostgreSQL 17.11's). Here the same statements run on the
// single-process engine, with every aggregate forced to drain, and on the
// stage DAG three ways — plus the morsel DAG arm with the drain forced, since
// the workers' aggregates merge clone partial states the same way the
// embedded engine's do. GSD_MEASURE=1 logs every cell instead of asserting.

func gsdTables() []tmdTable {
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		{Name: "a", Type: parquet.TypeInt32, Nullable: true},
		{Name: "b", Type: parquet.TypeInt32, Nullable: true},
	}}
	var tiny, big []map[string]any
	for _, r := range [][3]int32{{1, 10, 100}, {1, 10, 200}, {1, 20, 100}, {2, 30, 300}} {
		tiny = append(tiny, map[string]any{"k": r[0], "a": r[1], "b": r[2]})
	}
	for i := int32(0); i < 6000; i++ {
		big = append(big, map[string]any{"k": i%3 + 1, "a": (i%5 + 1) * 10, "b": (i%7 + 1) * 100})
	}
	return []tmdTable{{"gsd_i", sch, tiny}, {"gsd_b", sch, big}}
}

// gsdCoordRender is wadjet's gsdRender: sorted rows joined by " | ", NULL
// spelled out, (none) for no rows.
func gsdCoordRender(rows [][]any) string {
	if len(rows) == 0 {
		return "(none)"
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		parts := make([]string, len(r))
		for j, v := range r {
			if v == nil {
				parts[j] = "NULL"
				continue
			}
			parts[j] = fmt.Sprint(v)
		}
		out[i] = strings.Join(parts, ",")
	}
	sort.Strings(out)
	return strings.Join(out, " | ")
}

type gsdCoordCell struct{ name, sql, pg, pin string }

func gsdCoordCells(t *testing.T) []gsdCoordCell {
	t.Helper()
	f, err := os.Open("../../wadjet/testdata/grouping_set_drain_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []gsdCoordCell
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.Split(line, "\t")
		if len(p) != 4 {
			t.Fatalf("malformed line %q", line)
		}
		out = append(out, gsdCoordCell{p[0], p[1], p[2], p[3]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGroupingSetDrainEveryArm(t *testing.T) {
	// GSD_ARMS=1 runs it under -short too, for a host whose rules keep this
	// package to -short.
	if testing.Short() && os.Getenv("GSD_ARMS") == "" {
		t.Skip("-short: five arms over the grouping-set drain table (GSD_ARMS=1 runs it)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	cells := gsdCoordCells(t)

	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 1 << 20, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range gsdTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatal(err)
		}
		rg := 512
		if tbl.name == "gsd_i" {
			rg = 2 // so a filter can empty the warmup row group
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: rg})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	single := func(sql string) (out string) {
		defer func() {
			if r := recover(); r != nil {
				out = fmt.Sprintf("PANIC %v", r)
			}
		}()
		res, err := db.Query(ctx, sql)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err)
		}
		var rows [][]any
		for i := range res.Rows {
			rows = append(rows, res.Cells(i))
		}
		return gsdCoordRender(rows)
	}
	forced := func(run func(string) string) func(string) string {
		return func(sql string) string {
			defer exec.ForceAggDrainEvery(exec.ForceAggDrainEvery(1))
			return run(sql)
		}
	}
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, gsdTables())
		if wcfg != nil {
			return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
		}
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	dag := func(c *Coordinator) func(string) string {
		return func(sql string) string {
			got, err := ssRunDAG(ctx, c, sql, false)
			if err != nil {
				return "ERR " + sqlerr.StateOf(err)
			}
			_, rest, _ := strings.Cut(got, "} rows=")
			n, rows, _ := strings.Cut(rest, " ")
			if n == "0" {
				return "(none)"
			}
			// ssRender spells booleans and NULL as gsRender does; the rows
			// are sorted the same way.
			return rows
		}
	}
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	arms := []struct {
		name string
		run  func(string) string
	}{
		{"single", single},
		{"spilled", forced(single)},
		{"dag", dag(stand(nil))},
		{"dag-shuffled", dag(stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 }))},
		{"dag-morsel4", dag(coordM)},
		{"dag-morsel4-forced", forced(dag(coordM))},
	}
	measure := os.Getenv("GSD_MEASURE") != ""
	before := exec.ForcedAggDrains.Load()
	for _, c := range cells {
		for _, arm := range arms {
			got := arm.run(c.sql)
			if measure {
				status := "RIGHT"
				if got != c.pg {
					status = "DIFF"
				}
				t.Logf("CELL\t%s\t%s\t%s\tgot=%s\tpg=%s", status, c.name, arm.name, got, c.pg)
				continue
			}
			want := c.pg
			if c.pin != "" {
				want = c.pin
			}
			if got != want {
				t.Errorf("%s [%s]: %s\n got  %s\n want %s (PostgreSQL 17.11 answers %s)", c.name, arm.name, c.sql, got, want, c.pg)
			}
		}
	}
	if exec.ForcedAggDrains.Load() == before {
		t.Error("no aggregate drain was forced: the spilled arms did not exercise the drain")
	}
}
