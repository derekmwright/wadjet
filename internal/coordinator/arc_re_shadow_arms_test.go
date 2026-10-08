// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"math"
	"os"
	"regexp"
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

// A ROUNDING site (round, the integer CAST and `::`, an
// array element cast) over a column whose NAME is shared by another column
// of the query — a derived table that computes `f` over a table that has an
// `f`, a UNION ALL arm, a self-join, a join of two relations each with `f`, a
// CTE read twice, an aggregate / window / DISTINCT / LIMIT block's `f`, a
// rename through two derived levels — on five arms against PostgreSQL 17.11.
//
// The derived value (`i / 2.0 + 1`) differs from the stored `f` on every row,
// and half its rows are half-way values whose numeric and float8 rules
// differ, so a site that reads the wrong COLUMN and a site that reads the
// right column under the wrong RULE both fail here. 89cea148 failed 70 of the
// 152 cells on the stage-DAG arms (gate_shadow_at_base_FAILS.log).

var re2ShadowVals = []string{"0.5", "1.5", "2.5", "3.5", "-0.5", "-1.5", "-2.5", "2.4999999999999996", "2.5000000000000004", "NaN", "Infinity", "-Infinity", "NULL"}

func re2ShadowTables() []tmdTable {
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
	for k, v := range re2ShadowVals {
		row := map[string]any{"id": int64(k + 1), "f": nil, "r": nil, "n": nil, "i": nil, "af": nil}
		if v != "NULL" {
			f, _ := strconv.ParseFloat(v, 64)
			row["f"] = f
			row["r"] = float32(f)
			row["af"] = []any{f, 1.5}
			if !math.IsNaN(f) && !math.IsInf(f, 0) {
				row["n"] = v
				if k < 7 {
					row["i"] = int32(f * 2)
				}
			}
		}
		rows = append(rows, row)
	}
	big := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
	}}
	var brows []map[string]any
	for k := 0; k < 5000; k++ {
		brows = append(brows, map[string]any{"id": int64(k + 1), "f": float64(k%4) + 0.5, "i": int32(2*(k%4) + 1)})
	}
	return []tmdTable{{"rr_a", schema, rows}, {"rr_big", big, brows}}
}

func re2ShadowStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tb := range re2ShadowTables() {
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

// re2ShadowTSV reads name<TAB>value lines, skipping comments.
func re2ShadowTSV(t *testing.T, path string) ([]string, map[string]string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var names []string
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		n, v, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("%s: malformed line %q", path, line)
		}
		names = append(names, n)
		out[n] = v
	}
	return names, out
}

var (
	re2NegZero  = regexp.MustCompile(`(^|[^\d.])-0($|[^\d.])`)
	re2TrailDot = regexp.MustCompile(`(\d)\.0($|[,;}\s])`)
)

// re2ShadowNorm compares answers up to the sign of a zero (#1489's, not this
// seam's) and a DECIMAL's printed `.0`.
func re2ShadowNorm(s string) string {
	s = strings.TrimSpace(s)
	for i := 0; i < 2; i++ {
		s = re2NegZero.ReplaceAllString(s, "${1}0${2}")
		s = re2TrailDot.ReplaceAllString(s, "${1}${2}")
	}
	return s
}

// re2ShadowStillDiverges names the cells whose stage-DAG answer is still not
// PostgreSQL's, each with its mechanism; the gate asserts they still disagree
// there, so the entry is deleted — the proof — when one starts agreeing.
func re2ShadowStillDiverges(name string) string {
	return ""
}

func TestArcREShadowedColumnRoundsByItsOwnTypeOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("five-arm table")
	}
	names, cells := re2ShadowTSV(t, "testdata/arc_re_shadow_cells.tsv")
	_, answers := re2ShadowTSV(t, "testdata/arc_re_shadow_pg17.tsv")
	for _, n := range names {
		if _, ok := answers[n]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer", n)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	single := re2ShadowStandalone(t, ctx, 0)
	spilled := re2ShadowStandalone(t, ctx, 512*1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, re2ShadowTables())
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
	for _, name := range names {
		sql, want := cells[name], answers[name]
		t.Run(name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got[i] = arm.run(sql)
				}()
			}
			wg.Wait()
			pinned := re2ShadowStillDiverges(name) != ""
			for i, arm := range arms {
				known := pinned && strings.HasPrefix(arm.name, "dag")
				agrees := re2ShadowNorm(got[i]) == re2ShadowNorm(want)
				switch {
				case known && agrees:
					t.Errorf("%s: %s now answers PostgreSQL's %s: delete its re2ShadowStillDiverges case", arm.name, sql, want)
				case !known && !agrees:
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", arm.name, sql, got[i], want)
				}
			}
		})
	}
	if len(names) < 150 {
		t.Fatalf("the table shrank: %d cells", len(names))
	}
}
