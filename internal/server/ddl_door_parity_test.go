package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/derekmwright/wadjet/wadjet"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ddlDoorMatrix is the full 22-type matrix as DDL DECLARATIONS, with every
// parameterized type carrying parameters and the nested ones carrying
// parameterized children.
//
// The declarations, not the programmatic parquet.Schema the type-matrix
// fixture uses, are the point. `parquet.DeclaredColumn` — the one place a
// declaration becomes a Column since #647 — read DECIMAL's (p, s) and nothing
// else, so `VECTOR(384)` created a `Dimension: 0` column that no INSERT could
// ever write (it failed at flush with an internal error and no SQLSTATE), and
// ARRAY's element, ROW's fields and MAP's key/value were dropped at all three
// doors. Every existing gate declared its containers PROGRAMMATICALLY, which
// is exactly why none of them could see it (#675).
func ddlDoorMatrix() ([]plansql.ColumnDef, []parquet.Column) {
	str := func(name string, t parquet.TypeID) parquet.Column {
		return parquet.Column{Name: name, Type: t, Nullable: true}
	}
	dec := func(name string, p, s int) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeDecimal, Nullable: true, Precision: p, Scale: s}
	}
	elem := func(c parquet.Column) *parquet.Column { return &c }

	defs := []plansql.ColumnDef{
		{Name: "c_bool", Type: "BOOL", Nullable: true},
		{Name: "c_int32", Type: "INT32", Nullable: true},
		{Name: "c_int64", Type: "INT64", Nullable: false},
		{Name: "c_float32", Type: "FLOAT32", Nullable: true},
		{Name: "c_float64", Type: "FLOAT64", Nullable: true},
		{Name: "c_string", Type: "STRING", Nullable: true},
		{Name: "c_bytes", Type: "BYTES", Nullable: true},
		{Name: "c_timestamp", Type: "TIMESTAMP", Nullable: true},
		{Name: "c_ipv4", Type: "IPV4", Nullable: true},
		{Name: "c_ipv6", Type: "IPV6", Nullable: true},
		{Name: "c_cidr", Type: "CIDR", Nullable: true},
		{Name: "c_mac", Type: "MAC", Nullable: true},
		{Name: "c_port", Type: "PORT", Nullable: true},
		{Name: "c_protocol", Type: "PROTOCOL", Nullable: true},
		{Name: "c_duration", Type: "DURATION", Nullable: true},
		{Name: "c_uuid", Type: "UUID", Nullable: true},
		{Name: "c_date", Type: "DATE", Nullable: true},
		// The parameterized five, plus the parameters nested inside the
		// containers — the half no door read.
		{Name: "c_decimal", Type: "DECIMAL(9,2)", Nullable: true},
		{Name: "c_decimal_bare", Type: "DECIMAL", Nullable: true},
		{Name: "c_decimal_wide", Type: "NUMERIC(38,10)", Nullable: true},
		{Name: "c_vector", Type: "VECTOR(384)", Nullable: true},
		{Name: "c_array", Type: "ARRAY(STRING)", Nullable: true},
		{Name: "c_array_decimal", Type: "ARRAY(DECIMAL(9,2))", Nullable: true},
		{Name: "c_array_nested", Type: "ARRAY(ARRAY(INT64))", Nullable: true},
		{Name: "c_row", Type: "ROW(a INT64, d DECIMAL(9,2))", Nullable: true},
		{Name: "c_row_nested", Type: "ROW(a INT64, r ROW(v VECTOR(4)))", Nullable: true},
		{Name: "c_map", Type: "MAP(STRING, DECIMAL(9,2))", Nullable: true},
		{Name: "c_map_array", Type: "MAP(STRING, ARRAY(INT64))", Nullable: true},
	}

	mapCol := func(name string, val parquet.Column) parquet.Column {
		key := str("key", parquet.TypeString)
		val.Name = "value"
		return parquet.Column{Name: name, Type: parquet.TypeMap, Nullable: true,
			ElementType: elem(parquet.Column{Name: "entry", Type: parquet.TypeRow,
				Fields: []parquet.Column{key, val}})}
	}

	want := []parquet.Column{
		{Name: "c_bool", Type: parquet.TypeBool, Nullable: true},
		{Name: "c_int32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c_int64", Type: parquet.TypeInt64, Nullable: false},
		{Name: "c_float32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "c_float64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "c_string", Type: parquet.TypeString, Nullable: true},
		{Name: "c_bytes", Type: parquet.TypeBytes, Nullable: true},
		{Name: "c_timestamp", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_port", Type: parquet.TypePort, Nullable: true},
		{Name: "c_protocol", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "c_duration", Type: parquet.TypeDuration, Nullable: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "c_date", Type: parquet.TypeDate, Nullable: true},
		dec("c_decimal", 9, 2),
		dec("c_decimal_bare", 38, 0),
		dec("c_decimal_wide", 38, 10),
		{Name: "c_vector", Type: parquet.TypeVector, Nullable: true, Dimension: 384},
		{Name: "c_array", Type: parquet.TypeArray, Nullable: true,
			ElementType: elem(str("element", parquet.TypeString))},
		{Name: "c_array_decimal", Type: parquet.TypeArray, Nullable: true,
			ElementType: elem(dec("element", 9, 2))},
		{Name: "c_array_nested", Type: parquet.TypeArray, Nullable: true,
			ElementType: elem(parquet.Column{Name: "element", Type: parquet.TypeArray, Nullable: true,
				ElementType: elem(str("element", parquet.TypeInt64))})},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true,
			Fields: []parquet.Column{str("a", parquet.TypeInt64), dec("d", 9, 2)}},
		{Name: "c_row_nested", Type: parquet.TypeRow, Nullable: true,
			Fields: []parquet.Column{str("a", parquet.TypeInt64),
				{Name: "r", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
					{Name: "v", Type: parquet.TypeVector, Nullable: true, Dimension: 4}}}}},
		mapCol("c_map", dec("value", 9, 2)),
		mapCol("c_map_array", parquet.Column{Name: "value", Type: parquet.TypeArray, Nullable: true,
			ElementType: elem(str("element", parquet.TypeInt64))}),
	}
	return defs, want
}

// Every DDL door must produce the SAME Column for the same declaration, down
// to the last nested parameter. Three copies of "ParseTypeID and fill in the
// name" existed before #647 and only one read the DECIMAL parameters; after
// #647 there was one function and it read only DECIMAL's. This asserts the
// whole declaration, byte for byte (reflect.DeepEqual through
// sameDeclaredColumn), at all three doors.
func TestEveryDDLDoorResolvesEveryParameterizedType(t *testing.T) {
	defs, want := ddlDoorMatrix()

	// The HTTP door, through the REST handler.
	srv, cat := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()
	body := map[string]any{"name": "doors_http", "columns": httpColumnDefs(defs)}
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	respBody := new(bytes.Buffer)
	respBody.ReadFrom(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("HTTP CREATE TABLE: %d %s", resp.StatusCode, respBody.String())
	}
	httpMeta, err := cat.GetTable(context.Background(), "doors_http")
	if err != nil {
		t.Fatal(err)
	}
	compareDoorColumns(t, "HTTP", httpMeta.Schema.Columns, want)

	// The gRPC door, through the handler.
	ctx := context.Background()
	gcat := catalog.NewWithStore(objstore.NewMemStore(), "test-bucket")
	if err := gcat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	g := NewGRPCServer(GRPCConfig{Catalog: gcat}, slog.Default())
	cols := make([]*wadjetv1.ColumnDef, len(defs))
	for i, d := range defs {
		cols[i] = &wadjetv1.ColumnDef{Name: d.Name, Type: d.Type, Nullable: d.Nullable}
	}
	if _, err := g.CreateTable(ctx, &wadjetv1.CreateTableRequest{Name: "doors_grpc", Columns: cols}); err != nil {
		t.Fatalf("gRPC CreateTable: %v", err)
	}
	gMeta, err := gcat.GetTable(ctx, "doors_grpc")
	if err != nil {
		t.Fatal(err)
	}
	compareDoorColumns(t, "gRPC", gMeta.Schema.Columns, want)

	// The embedded door, through SQL. This is also the assertion that the SQL
	// GRAMMAR can spell these declarations at all: before #675 it accepted one
	// number and optionally a second inside a type's parentheses, so ARRAY,
	// ROW and MAP were syntax errors and the schema-side defect was
	// unreachable from here.
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query(ctx, ddlCreateSQL("doors_sql", defs)); err != nil {
		t.Fatalf("embedded CREATE TABLE: %v", err)
	}
	eMeta, err := db.Catalog().GetTable(ctx, "doors_sql")
	if err != nil {
		t.Fatal(err)
	}
	compareDoorColumns(t, "embedded SQL", eMeta.Schema.Columns, want)
}

func compareDoorColumns(t *testing.T, door string, got, want []parquet.Column) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s door produced %d columns, want %d", door, len(got), len(want))
	}
	for i := range want {
		if !sameDeclaredColumn(got[i], want[i]) {
			t.Errorf("%s door column %q:\n  got  %s\n  want %s", door, want[i].Name,
				declaredColumnDeep(got[i]), declaredColumnDeep(want[i]))
		}
	}
}

// declaredColumnDeep renders the WHOLE declaration, nested parameters
// included. declaredColumnString stops at "elem=true", which is precisely the
// resolution at which #675 was invisible.
func declaredColumnDeep(c parquet.Column) string {
	s := fmt.Sprintf("%s %v", c.Name, c.Type)
	switch {
	case c.Type == parquet.TypeDecimal:
		s += fmt.Sprintf("(%d,%d)", c.Precision, c.Scale)
	case c.Type == parquet.TypeVector:
		s += fmt.Sprintf("(%d)", c.Dimension)
	}
	if c.ElementType != nil {
		s += "<" + declaredColumnDeep(*c.ElementType) + ">"
	}
	if len(c.Fields) > 0 {
		s += "{"
		for i, f := range c.Fields {
			if i > 0 {
				s += ", "
			}
			s += declaredColumnDeep(f)
		}
		s += "}"
	}
	if !c.Nullable {
		s += " NOT NULL"
	}
	return s
}

func httpColumnDefs(defs []plansql.ColumnDef) []map[string]any {
	out := make([]map[string]any, len(defs))
	for i, d := range defs {
		nullable := d.Nullable
		out[i] = map[string]any{"name": d.Name, "type": d.Type, "nullable": &nullable}
	}
	return out
}

func ddlCreateSQL(table string, defs []plansql.ColumnDef) string {
	s := "CREATE TABLE " + table + " ("
	for i, d := range defs {
		if i > 0 {
			s += ", "
		}
		s += d.Name + " " + d.Type
		if !d.Nullable {
			s += " NOT NULL"
		}
	}
	return s + ")"
}

// The migration story for the tables the pre-#647 HTTP and gRPC doors created,
// pinned in both directions (ADR-0024 item 8).
//
// Those doors persisted `DECIMAL(9,2)` as `Precision: 0, Scale: 0`, and
// ALTER TABLE has no ALTER COLUMN TYPE to migrate them with. Such a column is
// read as the BARE DECIMAL — `DECIMAL(38,0)` — which is what
// ParseDecimalParams already means by a bare declaration and what
// decimalEffectivePrecision already writes into the leaf annotation. It is a
// read-side default, deliberately NOT a refusal: the stored unscaled integers
// are exactly what DECIMAL(38,0) means, so there is no unrepresentable value
// to fail the write on, and refusing would take every such table offline
// including the ones whose columns really were declared bare.
func TestLegacyPrecisionZeroDecimalReadsAsBareDecimal(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// A manifest exactly as a pre-#647 HTTP/gRPC door wrote one.
	legacy := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Nullable: true}, // Precision 0, Scale 0
	}}
	if err := cat.CreateTable(ctx, "legacy_dec", legacy, nil); err != nil {
		t.Fatal(err)
	}

	// A bare `DECIMAL` declared TODAY produces (38, 0), so the two are the
	// same column and nothing in a manifest can tell them apart. That is the
	// premise the read-side default rests on; if it stopped holding, the
	// default would be inventing a rule rather than stating one.
	bare, err := parquet.DeclaredColumn("d", "DECIMAL", true)
	if err != nil {
		t.Fatal(err)
	}
	if bare.Precision != 38 || bare.Scale != 0 {
		t.Fatalf("a bare DECIMAL declares (%d,%d), want (38,0) — ADR-0024 item 8's premise no longer holds",
			bare.Precision, bare.Scale)
	}

	// Values are accepted and read back at scale 0 — PostgreSQL's numeric(38,0)
	// behaviour, not a refusal and not scale-2 storage. The stored box is the
	// UNSCALED integer at the column's scale (ADR-0018 §4), which at scale 0 is
	// the number itself.
	for id, tc := range []struct {
		literal string
		want    int64
	}{
		{"12", 12},
		{"12.34", 12}, // rounds to zero fractional digits
		{"12.5", 13},  // half away from zero, as PostgreSQL assigns
		{"-12.5", -13},
	} {
		info := parseDMLOrFatal(t, fmt.Sprintf("INSERT INTO legacy_dec (id, d) VALUES (%d, %s)", id, tc.literal))
		if _, err := executeDMLInsert(ctx, cat, info.Insert); err != nil {
			t.Fatalf("INSERT %s into a Precision-0 DECIMAL was refused: %v", tc.literal, err)
		}
		want := map[int64]int64{int64(id): tc.want}
		for _, row := range legacyDecimalRows(t, cat, legacy) {
			gotID := row["id"].(int64)
			if w, ok := want[gotID]; ok {
				if got := legacyDecimalInt(t, row["d"]); got != w {
					t.Errorf("INSERT %s stored %d, want %d — a Precision-0 DECIMAL is DECIMAL(38,0)",
						tc.literal, got, w)
				}
			}
		}
	}

	// And a value past the bare declaration's own range is still refused, so
	// the default is a DECLARATION, not an absence of one.
	info := parseDMLOrFatal(t,
		"INSERT INTO legacy_dec (id, d) VALUES (2, 999999999999999999999999999999999999999)")
	if _, err := executeDMLInsert(ctx, cat, info.Insert); err == nil {
		t.Error("a 39-digit value was accepted into a Precision-0 DECIMAL; the bare declaration is (38,0)")
	}
}

// legacyDecimalRows reads every row of legacy_dec through the DML file reader —
// the same path the UPDATE/DELETE match scans use.
func legacyDecimalRows(t *testing.T, cat *catalog.Catalog, schema parquet.Schema) []map[string]any {
	t.Helper()
	ctx := context.Background()
	manifest, err := cat.GetManifest(ctx, "legacy_dec")
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			b, err := readDMLFile(ctx, cat, f.Path, schema.Columns)
			if err != nil {
				t.Fatal(err)
			}
			if b == nil {
				continue
			}
			for i := 0; i < b.Len; i++ {
				out = append(out, b.RowAt(i))
			}
		}
	}
	return out
}

// legacyDecimalInt reads whatever box a scale-0 DECIMAL comes back in as the
// integer it spells.
func legacyDecimalInt(t *testing.T, v any) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case string:
		var n int64
		if _, err := fmt.Sscanf(x, "%d", &n); err != nil {
			t.Fatalf("DECIMAL came back as %q, which is not an integer at scale 0", x)
		}
		return n
	default:
		t.Fatalf("DECIMAL came back boxed as %T (%v)", v, v)
		return 0
	}
}
