// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ftOpen is one embedded engine over an in-memory store.
func ftOpen(t *testing.T) (*DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, ctx
}

// ftOne runs a one-cell SELECT and returns the cell and its declared type.
func ftOne(t *testing.T, ctx context.Context, db *DB, sql string) (any, parquet.TypeID) {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 || len(res.ColumnMetas) != 1 {
		t.Fatalf("%s: %d rows, %d columns, want one cell", sql, len(res.Rows), len(res.ColumnMetas))
	}
	return res.Cells(0)[0], res.ColumnMetas[0].TypeID
}

// THE ISSUE'S OWN CELLS, ON THE EMBEDDED ENGINE (#1464). PostgreSQL 17.11:
// `CREATE TABLE z (dv FLOAT)` is double precision and stores 674999997;
// `dv FLOAT8` and `dv DOUBLE PRECISION` are accepted (format_type double
// precision), `dv REAL` / `dv FLOAT4` / `dv FLOAT(24)` are real and store
// 6.75e+08. At v0.25.3 the FLOAT column stored 675000000 (float4), and FLOAT8,
// FLOAT4 and REAL were 42704 and DOUBLE PRECISION 42601 (#1405).
func TestArcFTFloatColumnStoresDoublePrecision(t *testing.T) {
	db, ctx := ftOpen(t)
	for _, sql := range []string{
		"CREATE TABLE ft_src (id BIGINT, b BIGINT, n DECIMAL(10,2))",
		"INSERT INTO ft_src VALUES (1, 9, 7.50)",
	} {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	for i, tc := range []struct {
		decl string
		want parquet.TypeID
	}{
		{"FLOAT", parquet.TypeFloat64},
		{"float", parquet.TypeFloat64},
		{"FLOAT8", parquet.TypeFloat64},
		{"DOUBLE PRECISION", parquet.TypeFloat64},
		{"FLOAT(53)", parquet.TypeFloat64},
		{"REAL", parquet.TypeFloat32},
		{"FLOAT4", parquet.TypeFloat32},
		{"FLOAT(24)", parquet.TypeFloat32},
	} {
		t.Run(tc.decl, func(t *testing.T) {
			name := fmt.Sprintf("z%d", i)
			if _, err := db.Query(ctx, "CREATE TABLE "+name+" (dv "+tc.decl+")"); err != nil {
				t.Fatalf("CREATE TABLE … (dv %s): %v — PostgreSQL 17.11 accepts it", tc.decl, err)
			}
			if _, err := db.Query(ctx, "INSERT INTO "+name+" SELECT t.b * 10000000 * t.n - 3 FROM ft_src t WHERE t.id = 1"); err != nil {
				t.Fatalf("INSERT: %v", err)
			}
			got, typ := ftOne(t, ctx, db, "SELECT dv FROM "+name)
			if typ != tc.want {
				t.Errorf("dv %s declares %s, want %s (PostgreSQL 17.11)", tc.decl, typ, tc.want)
			}
			switch tc.want {
			case parquet.TypeFloat64:
				if f, ok := got.(float64); !ok || f != 674999997 {
					t.Errorf("dv %s stored %v (%T), want 674999997 (PostgreSQL 17.11)", tc.decl, got, got)
				}
			case parquet.TypeFloat32:
				if f, ok := got.(float32); !ok || f != 675000000 {
					t.Errorf("dv %s stored %v (%T), want float4 6.75e+08 (PostgreSQL 17.11)", tc.decl, got, got)
				}
			}
			meta, err := db.Catalog().GetTable(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if c := meta.Schema.Columns[0]; c.Type != tc.want {
				t.Errorf("the catalog records dv %s as %s, want %s", tc.decl, c.Type, tc.want)
			}
		})
	}
	// The two FLOAT(p) ends PostgreSQL refuses, with its SQLSTATE.
	for _, decl := range []string{"FLOAT(0)", "FLOAT(54)"} {
		_, err := db.Query(ctx, "CREATE TABLE z_bad (dv "+decl+")")
		if st := sqlerr.StateOf(err); st != "22023" {
			t.Errorf("dv %s: %v (%s), want 22023 (PostgreSQL 17.11)", decl, err, st)
		}
	}
}

// A COLUMN CREATED FLOAT BEFORE THIS CHANGE IS STILL FLOAT4 (#1464's upgrade
// note). The catalog records a column's TypeID — `{"type":3}` for float4,
// the bytes a v0.25.3 `CREATE TABLE … (c FLOAT)` wrote (measured on a
// v0.25.3 data directory) — never the spelling that declared it, so changing
// what FLOAT means cannot reinterpret stored data: the column keeps reading,
// storing and declaring real.
func TestArcFTAColumnDeclaredFloatBeforeTheChangeStaysReal(t *testing.T) {
	db, ctx := ftOpen(t)
	var legacy parquet.Schema
	if err := json.Unmarshal([]byte(`{"columns":[{"name":"c","type":3,"nullable":true}]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "ft_legacy", legacy, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, "INSERT INTO ft_legacy VALUES (674999997)"); err != nil {
		t.Fatal(err)
	}
	got, typ := ftOne(t, ctx, db, "SELECT c FROM ft_legacy")
	if typ != parquet.TypeFloat32 {
		t.Errorf("the legacy column declares %s, want FLOAT32 (real)", typ)
	}
	if f, ok := got.(float32); !ok || f != 675000000 {
		t.Errorf("the legacy column stored %v (%T), want float4 6.75e+08", got, got)
	}
	if _, err := db.Query(ctx, "INSERT INTO ft_legacy VALUES (1e39)"); sqlerr.StateOf(err) != "22003" {
		t.Errorf("1e39 into the legacy real column: %v, want 22003 (float4's range)", err)
	}

	// And a column declared FLOAT now is recorded as float8, type 4.
	if _, err := db.Query(ctx, "CREATE TABLE ft_new (c FLOAT)"); err != nil {
		t.Fatal(err)
	}
	meta, err := db.Catalog().GetTable(ctx, "ft_new")
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(meta.Schema)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `{"name":"c","type":4,`) {
		t.Errorf("a column declared FLOAT is recorded as %s, want type 4 (FLOAT64)", b)
	}
}

// A CONTAINER'S ELEMENT TYPE IS READ BY THE SAME TABLE (#1464): ARRAY, ROW
// and MAP element spellings resolve through the one float table, so an
// `ARRAY(FLOAT)` element is double precision exactly as a FLOAT column is
// (PostgreSQL 17.11: `float[]` is double precision[]). At v0.25.3 the FLOAT
// element was float4 and REAL / FLOAT8 / DOUBLE PRECISION elements refused
// the CREATE TABLE.
func TestArcFTContainerElementFloatNames(t *testing.T) {
	db, ctx := ftOpen(t)
	if _, err := db.Query(ctx, "CREATE TABLE ft_box (a ARRAY(FLOAT), b ARRAY(DOUBLE PRECISION), r ROW(x FLOAT, y REAL), m MAP(STRING, FLOAT8))"); err != nil {
		t.Fatalf("CREATE TABLE with float element spellings: %v", err)
	}
	meta, err := db.Catalog().GetTable(ctx, "ft_box")
	if err != nil {
		t.Fatal(err)
	}
	cols := meta.Schema.Columns
	got := []parquet.TypeID{
		cols[0].ElementType.Type, cols[1].ElementType.Type,
		cols[2].Fields[0].Type, cols[2].Fields[1].Type,
		cols[3].ElementType.Fields[1].Type,
	}
	want := []parquet.TypeID{parquet.TypeFloat64, parquet.TypeFloat64, parquet.TypeFloat64, parquet.TypeFloat32, parquet.TypeFloat64}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("element %d is %s, want %s", i, got[i], want[i])
		}
	}
}
