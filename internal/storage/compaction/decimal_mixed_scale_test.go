package compaction

import (
	"context"
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Compaction is the one job in this system that is read → write over a whole
// table and DELETES its inputs, so a read→write asymmetry here is silent data
// loss (ADR-0018 §4). #707 is such an asymmetry: two files declaring one
// catalog DECIMAL column at DIFFERENT scales carry the right NUMBER under two
// different halves of one declaration, and a compactor that reads each file's
// carrier verbatim and writes it under the catalog's scale multiplies one of
// them by a power of ten — permanently, with the evidence deleted.
//
// The compactor needs no rule of its own for this: it already reads through
// ReadRowGroupAs with the TABLE's schema and writes under the TABLE's schema,
// so the reader's reconciliation (parquet.DecimalRescale) is what makes the
// output right. This gate is what says so, and it is what fails if the reader's
// half is reverted.
//
// PostgreSQL 17.11 is the authority: a numeric(15,2) column holding 12.75 twice
// sums to 25.50 and groups into ONE group. Both files below mean 12.75.
func TestCompactionReconcilesMixedDeclaredScales(t *testing.T) {
	ctx := context.Background()
	cat, store := setupTestCatalog(t)
	const table = "decmixedscale"

	tableSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "a", Type: parquet.TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
	}}
	fileAt := func(scale int) parquet.Schema {
		return parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "a", Type: parquet.TypeDecimal, Precision: 15, Scale: scale, Nullable: true},
		}}
	}
	if err := cat.CreateTable(ctx, table, tableSchema, nil); err != nil {
		t.Fatal(err)
	}
	// Both files mean 12.75. File 0 declares the catalog's scale; file 1
	// declares scale 4 and stores the same number as 127500. A third row is
	// NULL, because a rescale must not invent a value for one.
	type spec struct {
		schema parquet.Schema
		rows   []map[string]any
	}
	specs := []spec{
		{fileAt(2), []map[string]any{
			{"id": int64(1), "a": parquet.Decimal128{Lo: 1275}},
			{"id": int64(2), "a": nil},
		}},
		{fileAt(4), []map[string]any{
			{"id": int64(3), "a": parquet.Decimal128{Lo: 127500}},
			{"id": int64(4), "a": parquet.Decimal128{Lo: 127550}}, // 12.7550 -> 12.76
		}},
	}
	for i, s := range specs {
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, i)
		size := writeTestFile(t, store, "test-bucket", path, s.schema, s.rows)
		if err := cat.AddFiles(ctx, table, nil, "", []catalog.FileEntry{
			{Path: path, SizeBytes: size, NumRows: int64(len(s.rows)), CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Idempotence, three passes: compaction REPLACES its inputs, so the
	// property to gate is that the second and third passes are the identity
	// on what the first produced (ADR-0018 §4).
	cfg := DefaultConfig()
	cfg.MinFiles = 2
	want := map[int64]string{1: "1275", 3: "1275", 4: "1276"}
	for pass := 1; pass <= 3; pass++ {
		if _, err := New(cat, nil, cfg).CompactTable(ctx, table); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		rows := readTableRows(t, cat, store, table, tableSchema, false)
		sort.Slice(rows, func(i, j int) bool { return idOf(rows[i]) < idOf(rows[j]) })
		if len(rows) != 4 {
			t.Fatalf("pass %d: %d rows, want 4", pass, len(rows))
		}
		for _, r := range rows {
			id := idOf(r)
			if r["a"] == nil {
				if id != 2 {
					t.Errorf("pass %d: row %d lost its value", pass, id)
				}
				continue
			}
			got := decCarrier(t, r["a"])
			if got != want[id] {
				t.Errorf("pass %d: row %d carrier = %s, want %s at the catalog's scale 2 — "+
					"a file declaring another scale was rewritten under the catalog's "+
					"without moving its carrier (#707)", pass, id, got, want[id])
			}
		}
		// After the first pass every file is the compactor's own, written at
		// the catalog's scale, so passes 2 and 3 have nothing to reconcile —
		// which is the other half of the property.
		if pass == 1 {
			assertEveryFileDeclaresScale(t, cat, store, table, 2)
		}
	}
}

// decCarrier renders either DECIMAL box the row path produces: an int64 to 18
// declared digits, a Decimal128 beyond (#419).
func decCarrier(t *testing.T, v any) string {
	t.Helper()
	switch tv := v.(type) {
	case int64:
		return parquet.Decimal128From(tv).String()
	case parquet.Decimal128:
		return tv.String()
	}
	t.Fatalf("column a is %#v, want a DECIMAL box", v)
	return ""
}

// assertEveryFileDeclaresScale is the WRITE half: the compactor's output must
// declare the catalog's (p, s), or the next reader has the same problem with
// the inputs already deleted.
func assertEveryFileDeclaresScale(t *testing.T, cat *catalog.Catalog, store objstore.Store,
	table string, want int,
) {
	t.Helper()
	ctx := context.Background()
	m, err := cat.GetManifest(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			rc, _, err := store.Get(ctx, "test-bucket", f.Path)
			if err != nil {
				t.Fatalf("%s: %v", f.Path, err)
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatalf("%s: %v", f.Path, err)
			}
			fr, ferr := parquet.OpenFileReaderFromBytes(data)
			if ferr != nil {
				t.Fatalf("%s: %v", f.Path, ferr)
			}
			for _, c := range fr.Schema().Columns {
				if c.Name == "a" && c.Scale != want {
					t.Errorf("%s declares column a at scale %d, want the catalog's %d",
						f.Path, c.Scale, want)
				}
			}
		}
	}
}
