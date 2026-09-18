// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE ARC JR FIXTURE — an outer join's ON residual, on five arms.
//
// Three relations of one shape, so every join kind can be spelled over the
// same two sides and the empty-side cells differ only in which table the
// alias names. The ROWS are chosen so that one query exercises every match
// disposition at once, rather than needing a fixture per disposition:
//
//	l.id 1, 2   duplicate key 1 on the probe, against 101/102 on the build:
//	            one probe row's chain can be PARTIALLY accepted.
//	l.id 3      key 2, one candidate (103) — accepted or rejected whole.
//	l.id 4      key 3, NO candidate at all, and a NULL `n`.
//	l.id 5      NULL key: never matches, so it is padded on every LEFT/FULL.
//	l.id 6      NULL `s`: a residual over it is UNKNOWN, which REJECTS, and
//	            the probe row is then padded — not dropped.
//	r.id 105    key 5, no probe partner: the RIGHT/FULL unmatched flush.
//	r.id 106    NULL key and NULL `s`.
//
// jr_e is empty, and is the empty BUILD side and the empty PROBE side of the
// same query text.
//
// PostgreSQL 17.11's answers for every cell were taken from a
// postgres:17-alpine container standing alone (`--locale=C`, text columns
// `COLLATE "C"`, since wadjet compares strings by bytes), loaded with exactly
// these rows; the commands and answers are in the arc's pg_answers.tsv.

const (
	jrProbeTable = "jr_l"
	jrBuildTable = "jr_r"
	jrEmptyTable = "jr_e"
)

func jrSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func jrRow(id int64, k any, s any, n any) map[string]any {
	return map[string]any{"id": id, "k": k, "s": s, "n": n}
}

func jrProbeData() []map[string]any {
	return []map[string]any{
		jrRow(1, int64(1), "alpha", int64(10)),
		jrRow(2, int64(1), "beta", int64(20)),
		jrRow(3, int64(2), "gamma", int64(30)),
		jrRow(4, int64(3), "delta", nil),
		jrRow(5, nil, "eps", int64(50)),
		jrRow(6, int64(4), nil, int64(60)),
	}
}

func jrBuildData() []map[string]any {
	return []map[string]any{
		jrRow(101, int64(1), "alpha", int64(10)),
		jrRow(102, int64(1), "ALPHA", int64(15)),
		jrRow(103, int64(2), "gamma2", int64(30)),
		jrRow(104, int64(4), "zeta", int64(61)),
		jrRow(105, int64(5), "omega", int64(70)),
		jrRow(106, nil, nil, nil),
	}
}

// jrTables is this arc's fixture, and it rides its OWN arms rather than the
// shared type-matrix corpus. That is a measurement, not a preference: a
// budgeted arm over the shared corpus already sits near its 512 KiB (the
// outstanding forced bytes of its scans; see n1SpillBudget's note), so three
// tables added there are three tables' worth of headroom taken away from every
// other budgeted gate in this package. Measured — with these three in
// tmdTables the nested-grouped-LATERAL cells of TestN1A* refused
// `memory budget exceeded` in 2 of 2 full-package runs, a different cell each
// time, and passed alone; with them out, the same package run is green.
func jrTables() []tmdTable {
	return []tmdTable{
		{jrProbeTable, jrSchema(), jrProbeData()},
		{jrBuildTable, jrSchema(), jrBuildData()},
		{jrEmptyTable, jrSchema(), nil},
	}
}

// jrStandalone is one embedded engine over the JR fixture alone, at the given
// memory budget (0 = none).
func jrStandalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
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
	for _, tbl := range jrTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		if len(tbl.rows) == 0 {
			continue
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// jrArms is c1Arms' five arms over the JR fixture alone.
func jrArms(t *testing.T, ctx context.Context) []c1Arm {
	t.Helper()
	single := jrStandalone(t, ctx, 0)
	spilled := jrStandalone(t, ctx, 512*1024)
	stand := func(opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, jrTables())
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	coord := stand()
	coordB := stand(func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infraM, nil, jrTables())
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
