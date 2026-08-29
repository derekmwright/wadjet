package server

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"testing"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The server's own UPDATE executor has the same shape as the embedded API's,
// and had the same defect: delete markers committed before the replacement
// rows were ingested, so an UPDATE the ingester refused DELETED the rows it
// declined to change (#647 review). This is wadjet.TestFailedUpdateLeaves
// TheRowSetUnchanged through executeDMLUpdate.
func TestServerFailedUpdateLeavesTheRowSetUnchanged(t *testing.T) {
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	rows := func() []map[string]any {
		return []map[string]any{
			{"id": int64(1), "d": "1.50"},
			{"id": int64(2), "d": "2.50"},
			{"id": int64(3), "d": "3.50"},
		}
	}

	for _, tc := range []struct {
		name  string
		set   string
		state string
	}{
		{name: "past the declared precision", set: "d = 99999999999999999999.99", state: "22003"},
		{name: "text naming no number", set: "d = 'abc'", state: "22P02"},
		{name: "NaN", set: "d = 'NaN'", state: "22003"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cat, filePath := nestServerDMLSetup(t, "srv_dec_upd", schema, rows())

			before := serverDecimalRowSet(t, cat, "srv_dec_upd", filePath, schema)
			if len(before) != 3 {
				t.Fatalf("seeded %d rows, want 3", len(before))
			}
			info := parseDMLOrFatal(t, "UPDATE srv_dec_upd SET "+tc.set+" WHERE id = 1").Update
			if _, err := executeDMLUpdate(ctx, cat, info); err == nil {
				t.Fatalf("UPDATE SET %s succeeded; want a refusal", tc.set)
			} else if got := sqlerr.StateOf(err); got != tc.state {
				t.Fatalf("SQLSTATE %q, want %q (err: %v)", got, tc.state, err)
			}
			after := serverDecimalRowSet(t, cat, "srv_dec_upd", filePath, schema)
			if len(after) != len(before) {
				t.Fatalf("%d rows after a REFUSED UPDATE, want %d — it deleted what it would not "+
					"replace\n  before: %v\n  after:  %v", len(after), len(before), before, after)
			}
			for i := range before {
				if after[i] != before[i] {
					t.Fatalf("row %d is %q after a REFUSED UPDATE, want %q", i, after[i], before[i])
				}
			}
		})
	}
}

func serverDecimalRowSet(t *testing.T, cat *catalog.Catalog, table, originalFile string, schema parquet.Schema) []string {
	t.Helper()
	all := nestServerAllRowsAfterUpdate(t, cat, table, originalFile, schema)
	out := make([]string, 0, len(all))
	for _, r := range all {
		out = append(out, fmt.Sprintf("id=%v d=%v", r["id"], r["d"]))
	}
	sort.Strings(out)
	return out
}

// A DECIMAL's precision and scale live in the type TEXT. The HTTP and gRPC
// CREATE TABLE paths read only the TypeID, so `DECIMAL(9,2)` produced a
// Precision 0, Scale 0 column over either — 12.34 stored as 12, 9999999.999
// stored with no error at all, and DECIMAL(50,2) accepted (#647 review). All
// three DDL doors go through parquet.DeclaredColumn now, and this asserts they
// cannot drift apart again.
func TestEveryDDLDoorReadsDecimalParameters(t *testing.T) {
	defs := []plansql.ColumnDef{
		{Name: "id", Type: "INT64", Nullable: false},
		{Name: "d", Type: "DECIMAL(9,2)", Nullable: true},
		{Name: "w", Type: "NUMERIC(38,10)", Nullable: true},
		{Name: "bare", Type: "DECIMAL", Nullable: true},
	}
	want := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
		{Name: "w", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "bare", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}

	// The HTTP door.
	got, err := columnDefsToSchema(defs)
	if err != nil {
		t.Fatalf("columnDefsToSchema: %v", err)
	}
	for i, w := range want {
		if !sameDeclaredColumn(got.Columns[i], w) {
			t.Errorf("HTTP CREATE TABLE column %q = %s, want %s", w.Name,
				declaredColumnString(got.Columns[i]), declaredColumnString(w))
		}
	}

	// The gRPC door, through the same declaration.
	for i, d := range defs {
		col, err := parquet.DeclaredColumn(d.Name, d.Type, d.Nullable)
		if err != nil {
			t.Fatalf("DeclaredColumn(%q): %v", d.Type, err)
		}
		if !sameDeclaredColumn(col, want[i]) {
			t.Errorf("gRPC CREATE TABLE column %q = %s, want %s", d.Name,
				declaredColumnString(col), declaredColumnString(want[i]))
		}
	}

	// And a declaration with no carrier is refused at every door, not
	// silently narrowed to the unconstrained sentinel.
	bad := []plansql.ColumnDef{{Name: "d", Type: "DECIMAL(50,2)", Nullable: true}}
	if _, err := columnDefsToSchema(bad); err == nil {
		t.Error("HTTP CREATE TABLE accepted DECIMAL(50,2)")
	} else if got := sqlerr.StateOf(err); got != "22023" {
		t.Errorf("HTTP SQLSTATE %q, want 22023 (err: %v)", got, err)
	}
	if _, err := parquet.DeclaredColumn("d", "DECIMAL(50,2)", true); err == nil {
		t.Error("gRPC CREATE TABLE accepted DECIMAL(50,2)")
	}
}

// The gRPC handler end to end, so the wiring is asserted and not only the
// helper it calls.
func TestGRPCCreateTableCarriesDecimalParameters(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("init catalog: %v", err)
	}
	g := NewGRPCServer(GRPCConfig{Catalog: cat}, slog.Default())

	_, err := g.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name: "g_dec",
		Columns: []*wadjetv1.ColumnDef{
			{Name: "id", Type: "INT64"},
			{Name: "d", Type: "DECIMAL(9,2)", Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	meta, err := cat.GetTable(ctx, "g_dec")
	if err != nil {
		t.Fatal(err)
	}
	d := meta.Schema.Columns[1]
	if d.Precision != 9 || d.Scale != 2 {
		t.Fatalf("gRPC CREATE TABLE stored DECIMAL(%d,%d), want DECIMAL(9,2) — 12.34 would store as 12",
			d.Precision, d.Scale)
	}
	if _, err := g.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name:    "g_wide",
		Columns: []*wadjetv1.ColumnDef{{Name: "d", Type: "DECIMAL(50,2)", Nullable: true}},
	}); err == nil {
		t.Error("gRPC CreateTable accepted DECIMAL(50,2)")
	}
}

// parquet.Column holds a slice, so the four fields a declaration decides are
// compared by name.
func sameDeclaredColumn(a, b parquet.Column) bool {
	return a.Name == b.Name && a.Type == b.Type &&
		a.Precision == b.Precision && a.Scale == b.Scale && a.Nullable == b.Nullable
}

func declaredColumnString(c parquet.Column) string {
	return fmt.Sprintf("%s %v(%d,%d) nullable=%v", c.Name, c.Type, c.Precision, c.Scale, c.Nullable)
}
