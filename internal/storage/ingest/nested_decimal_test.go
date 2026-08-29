package ingest

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A value no leaf can hold must be refused at the INGEST boundary, at every
// depth — not at the flush that eventually writes it.
//
// checkType's container arm validated nested DATEs only, so a DECIMAL inside a
// ROW, an ARRAY or a MAP was accepted here and refused by the writer's leaf at
// FlushAll. The flush is per BUFFER: a row that fails there takes every
// already-accepted row in the same partition with it, and reports against the
// partition rather than against the statement that carried the bad value
// (#647 review). The prior good row surviving is the assertion.
func TestIngestRefusesANestedDecimalWithNoValue(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "r", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		}},
		{Name: "a", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true}},
		{Name: "m", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
			}}},
	}}

	good := func(id int64) map[string]any {
		return map[string]any{
			"id": id,
			"r":  map[string]any{"d": "1.25"},
			"a":  []any{"2.50", "3.75"},
			"m":  map[string]any{"k": "4.00"},
		}
	}

	for _, tc := range []struct {
		name  string
		row   map[string]any
		state string
	}{
		{name: "ROW field names no number", state: "22P02",
			row: map[string]any{"id": int64(9), "r": map[string]any{"d": "abc"}}},
		{name: "ROW field past the precision", state: "22003",
			row: map[string]any{"id": int64(9), "r": map[string]any{"d": "99999999999999999999.99"}}},
		{name: "ARRAY element names no number", state: "22P02",
			row: map[string]any{"id": int64(9), "a": []any{"1.00", "abc"}}},
		{name: "ARRAY element past the precision", state: "22003",
			row: map[string]any{"id": int64(9), "a": []any{"1e40"}}},
		{name: "MAP value names no number", state: "22P02",
			row: map[string]any{"id": int64(9), "m": map[string]any{"k": "abc"}}},
		{name: "MAP value is NaN", state: "22003",
			row: map[string]any{"id": int64(9), "m": map[string]any{"k": "NaN"}}},
		// The MAP's STORAGE shape — the []any of {key,value} entry maps
		// batch.Vector.GetValue hands back, which every row that passed
		// through RowAt/ToRows carries. decomposeMap accepts it, so the
		// boundary check must read it too: it asserted map[string]any only,
		// so a bad value in this shape was admitted here and killed the whole
		// buffer at the flush (#647 re-review).
		{name: "MAP storage shape names no number", state: "22P02",
			row: map[string]any{"id": int64(9), "m": []any{
				map[string]any{"key": "k", "value": "abc"},
			}}},
		{name: "MAP storage shape past the precision", state: "22003",
			row: map[string]any{"id": int64(9), "m": []any{
				map[string]any{"key": "ok", "value": "1.00"},
				map[string]any{"key": "k", "value": "99999999999999999999.99"},
			}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := objstore.NewMemStore()
			cat := catalog.NewWithStore(store, testBucket)
			if err := cat.Init(ctx); err != nil {
				t.Fatal(err)
			}
			if err := cat.CreateTable(ctx, "nest", schema, nil); err != nil {
				t.Fatal(err)
			}
			ing := New(cat, "nest", schema, nil, DefaultConfig())

			// A good row is accepted and buffered first: it is what the bad
			// row used to take down with it.
			if err := ing.Ingest(ctx, []map[string]any{good(1)}); err != nil {
				t.Fatalf("the good row was refused: %v", err)
			}
			err := ing.Ingest(ctx, []map[string]any{tc.row})
			if err == nil {
				t.Fatalf("a nested DECIMAL with no value was accepted at the ingest boundary")
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Fatalf("SQLSTATE %q, want %q (err: %v)", got, tc.state, err)
			}
			// The buffered good row still flushes.
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatalf("FlushAll after the refused row: %v — the good row went down with it", err)
			}
			manifest, err := cat.GetManifest(ctx, "nest")
			if err != nil {
				t.Fatal(err)
			}
			var rows int64
			for _, p := range manifest.Partitions {
				for _, f := range p.Files {
					rows += f.NumRows
				}
			}
			if rows != 1 {
				t.Fatalf("%d rows landed, want the 1 good row", rows)
			}
		})
	}
}
