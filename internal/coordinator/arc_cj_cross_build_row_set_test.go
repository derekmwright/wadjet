// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"

	"testing"
)

// THE ARC CJ FIXTURE — a filter over a build side that spans more than one
// BATCH, on five arms. The LAYOUT is the dimension #1189 turns on: a build
// side arrives as one batch per ROW GROUP, and the defect needs two of them.
//
// Seven builds differing only in layout: cj_b1 (1 row, one file — the control
// that was already right at 1c2b4d25); cj_b3f (9 rows, THREE files) beside
// cj_brg (the same 9 rows in ONE file at rg 3), so a cell that answers
// differently between the two names the FILE boundary rather than the batch
// boundary; and cj_b2047 / cj_b2048 / cj_b2049 / cj_b4097 at rg 2048 — one,
// one, two and three batches, the batch boundary from below, at and above.
//
// The nine-row builds carry one row of each disposition a predicate can meet:
// f is TRUE at bid 1,3,4,6,7,9 and FALSE at 2,5,8; n is NULL at bid 4 and 8,
// so `n > k` is UNKNOWN and REJECTS there; s is nine distinct strings, for an
// IN list and a DISTINCT.
//
// Every `want` below is PostgreSQL 17.11's own answer over exactly these rows,
// transcribed from a postgres:17-alpine container standing alone (--locale=C,
// text columns COLLATE "C", since wadjet compares strings by bytes):
// cj_author/pg/. At 1c2b4d25 this gate FAILS, cj_author/gate_cj_table_at_base_FAILS.log.

func cjSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "bid", Type: parquet.TypeInt64},
		{Name: "f", Type: parquet.TypeBool, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func cjProbeSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "pid", Type: parquet.TypeInt64},
		{Name: "ptag", Type: parquet.TypeString, Nullable: true},
	}}
}

var cjSmallS = []string{"alpha", "beta", "gamma", "delta", "eps", "zeta", "eta", "theta", "iota"}

// cjSmallRows is the nine-row build relation. The rule, not a literal table,
// so the PostgreSQL loader in cj_author/pg/cells.py can state the same one.
func cjSmallRows(n int) []map[string]any {
	out := make([]map[string]any, 0, n)
	for i := 1; i <= n; i++ {
		var nv any
		if i%4 != 0 {
			nv = int64(i)
		}
		out = append(out, map[string]any{
			"bid": int64(i), "f": i%3 != 2, "s": cjSmallS[i-1], "n": nv,
		})
	}
	return out
}

// cjBigRows is the same shape at a size that crosses batch.DefaultBatchSize.
func cjBigRows(n int) []map[string]any {
	out := make([]map[string]any, 0, n)
	for i := 1; i <= n; i++ {
		var nv any
		if i%4 != 0 {
			nv = int64(i)
		}
		s := "beta"
		if i%5 == 0 {
			s = "alpha"
		}
		out = append(out, map[string]any{
			"bid": int64(i), "f": i%3 != 2, "s": s, "n": nv,
		})
	}
	return out
}

// cjLayout is one relation plus the storage layout the single-process arms
// give it. The DAG arms take tmdWriteTableList's own four-chunk split: they
// are the REFERENCE here, not the subject — they answered PostgreSQL's rows
// at 1c2b4d25 already, because a stage boundary materialises the filtered
// side before the join ever sees it.
type cjLayout struct {
	name   string
	schema parquet.Schema
	rows   []map[string]any
	files  int // how many files the rows are split across
	rg     int // parquet row-group size within a file
}

func cjLayouts() []cjLayout {
	return []cjLayout{
		{"cj_p", cjProbeSchema(), []map[string]any{
			{"pid": int64(1), "ptag": "p1"},
			{"pid": int64(2), "ptag": "p2"},
			{"pid": int64(3), "ptag": "p3"},
		}, 1, 4},
		{"cj_b1", cjSchema(), cjSmallRows(1), 1, 4},
		{"cj_b3f", cjSchema(), cjSmallRows(9), 3, 8},
		{"cj_brg", cjSchema(), cjSmallRows(9), 1, 3},
		{"cj_b2047", cjSchema(), cjBigRows(2047), 1, 2048},
		{"cj_b2048", cjSchema(), cjBigRows(2048), 1, 2048},
		{"cj_b2049", cjSchema(), cjBigRows(2049), 1, 2048},
		{"cj_b4097", cjSchema(), cjBigRows(4097), 1, 2048},
	}
}

func cjTmdTables() []tmdTable {
	ls := cjLayouts()
	out := make([]tmdTable, 0, len(ls))
	for _, l := range ls {
		out = append(out, tmdTable{l.name, l.schema, l.rows})
	}
	return out
}

// cjStandalone is one embedded engine over the CJ fixture alone, at the given
// memory budget (0 = none), with each relation written in ITS layout.
func cjStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, l := range cjLayouts() {
		if err := db.CreateTable(ctx, l.name, l.schema, nil); err != nil {
			t.Fatalf("create %s: %v", l.name, err)
		}
		if len(l.rows) == 0 {
			continue
		}
		per := (len(l.rows) + l.files - 1) / l.files
		for f := 0; f < l.files; f++ {
			lo, hi := f*per, min(f*per+per, len(l.rows))
			if lo >= hi {
				break
			}
			// One ingester per FILE: MaxBufferRows above the chunk keeps the
			// chunk in one file, and FlushAll closes it before the next.
			ing := db.NewIngester(l.name, l.schema, nil, ingest.Config{
				MaxBufferRows: hi - lo + 1, RowGroupSize: l.rg,
			})
			if err := ing.Ingest(ctx, l.rows[lo:hi]); err != nil {
				t.Fatalf("ingest %s: %v", l.name, err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatalf("flush %s: %v", l.name, err)
			}
		}
	}
	return db
}

// cjArms is the five arms over the CJ fixture alone. It stands on its own
// fixture rather than the shared type-matrix corpus for the reason JR's does:
// a budgeted arm over that corpus already sits near its 512 KiB, and eight
// more relations there is eight relations' worth of headroom taken from every
// other budgeted gate in this package.
func cjArms(t *testing.T, ctx context.Context) []c1Arm {
	t.Helper()
	single := cjStandalone(t, ctx, 0)
	spilled := cjStandalone(t, ctx, 512*1024)
	stand := func(opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, cjTmdTables())
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand()
	coordB := stand(func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infraM, nil, cjTmdTables())
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })
	return []c1Arm{
		{"single", func(s string) (string, error) { return f1RenderSingle(ctx, single, s) }, nil},
		{"spilled512k", func(s string) (string, error) { return f1RenderSingle(ctx, spilled, s) }, nil},
		{"dag", func(s string) (string, error) { return f1RenderDAG(ctx, coord, s) }, coord},
		{"dag-shuffled", func(s string) (string, error) { return f1RenderDAG(ctx, coordB, s) }, coordB},
		{"dag-morsel4", func(s string) (string, error) { return f1RenderDAG(ctx, coordM, s) }, coordM},
	}
}
