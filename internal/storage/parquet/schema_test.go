package parquet

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

func TestTypeIDString(t *testing.T) {
	tests := []struct {
		id   TypeID
		want string
	}{
		{TypeBool, "BOOL"},
		{TypeInt32, "INT32"},
		{TypeInt64, "INT64"},
		{TypeFloat32, "FLOAT32"},
		{TypeFloat64, "FLOAT64"},
		{TypeString, "STRING"},
		{TypeBytes, "BYTES"},
		{TypeTimestamp, "TIMESTAMP"},
		{TypeIPv4, "IPV4"},
		{TypeIPv6, "IPV6"},
		{TypeCIDR, "CIDR"},
		{TypeMAC, "MAC"},
		{TypePort, "PORT"},
		{TypeProtocol, "PROTOCOL"},
		{TypeDuration, "DURATION"},
		{TypeUUID, "UUID"},
		{TypeDate, "DATE"},
		{TypeDecimal, "DECIMAL"},
		{TypeArray, "ARRAY"},
		{TypeRow, "ROW"},
		{TypeMap, "MAP"},
		{TypeID(999), "UNKNOWN(999)"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.id.String()
			if got != tt.want {
				t.Errorf("TypeID(%d).String() = %q, want %q", int(tt.id), got, tt.want)
			}
		})
	}
}

func TestParseTypeID_AllTypes(t *testing.T) {
	tests := []struct {
		input string
		want  TypeID
		err   bool
	}{
		// Booleans
		{"BOOL", TypeBool, false},
		{"BOOLEAN", TypeBool, false},
		{"bool", TypeBool, false},
		// Integers
		{"INT32", TypeInt32, false},
		{"INT", TypeInt32, false},
		{"INTEGER", TypeInt32, false},
		{"INT64", TypeInt64, false},
		{"BIGINT", TypeInt64, false},
		{"LONG", TypeInt64, false},
		// Floats
		{"FLOAT32", TypeFloat32, false},
		{"FLOAT", TypeFloat32, false},
		{"FLOAT64", TypeFloat64, false},
		{"DOUBLE", TypeFloat64, false},
		// Strings
		{"STRING", TypeString, false},
		{"VARCHAR", TypeString, false},
		{"TEXT", TypeString, false},
		// Bytes
		{"BYTES", TypeBytes, false},
		{"BINARY", TypeBytes, false},
		{"VARBINARY", TypeBytes, false},
		// Timestamp
		{"TIMESTAMP", TypeTimestamp, false},
		{"DATETIME", TypeTimestamp, false},
		// Parameterized types (prefix match)
		{"DECIMAL(10,2)", TypeDecimal, false},
		{"NUMERIC(38,6)", TypeDecimal, false},
		{"ARRAY(STRING)", TypeArray, false},
		{"ROW(name STRING)", TypeRow, false},
		{"STRUCT(x INT)", TypeRow, false},
		{"MAP(STRING, INT)", TypeMap, false},
		// A PARAMETERIZED string spelling: what a PostgreSQL user writes and
		// what a migration tool emits. `VARCHAR(255)` used to fail the whole
		// CREATE TABLE with "unknown type", because the switch below reads the
		// WHOLE name and the parameter made it match nothing (#838). The
		// length is not stored — one unparameterized TypeString is all there
		// is — so an INSERT past n is accepted where PostgreSQL raises 22001,
		// a superset ADR-0012 records.
		{"VARCHAR(255)", TypeString, false},
		{"varchar(4)", TypeString, false},
		{"CHAR(4)", TypeString, false},
		{"CHARACTER(4)", TypeString, false},
		{"CHARACTER VARYING(10)", TypeString, false},
		{"NVARCHAR(8)", TypeString, false},
		{"VARCHAR(10485760)", TypeString, false}, // PostgreSQL's exact maximum
		// The modifier is VALIDATED, with the same rule the CAST door reads.
		// The first pass matched only the NAME, so DDL created a table for
		// every one of these while `CAST(x AS VARCHAR(0))` raised 22023 — one
		// type name, two dispositions across two doors.
		{"VARCHAR(0)", 0, true},        // 22023 must be at least 1
		{"CHAR(0)", 0, true},           // 22023
		{"VARCHAR(-1)", 0, true},       // 42601 syntax error at or near "-"
		{"VARCHAR(abc)", 0, true},      // 42601 syntax error at or near "abc"
		{"VARCHAR(10485761)", 0, true}, // 22023 cannot exceed 10485760
		{"TEXT(8)", 0, true},           // 42601 — PostgreSQL allows no modifier on text
		{"CHARACTER VARYING(0)", 0, true},
		// FLOAT(n) resolves by WIDTH, which is PostgreSQL's rule: float(1..24)
		// is real and float(25..53) is double precision (pg_typeof, measured
		// live). It failed the whole CREATE TABLE before, like VARCHAR(4).
		{"FLOAT(1)", TypeFloat32, false},
		{"FLOAT(24)", TypeFloat32, false},
		{"float(25)", TypeFloat64, false},
		{"FLOAT(53)", TypeFloat64, false},
		// The two ends PostgreSQL refuses with 22023.
		{"FLOAT(0)", 0, true},
		{"FLOAT(54)", 0, true},
		// The NAME is matched exactly, not by prefix, so a longer name that
		// merely starts the same way is still unknown.
		{"VARCHARX(1)", 0, true},
		{"FLOATX(1)", 0, true},
		// Unknown
		{"BANANA", 0, true},
		{"", 0, true},
		// With whitespace
		{"  INT64  ", TypeInt64, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseTypeID(tt.input)
			if tt.err {
				if err == nil {
					t.Fatalf("ParseTypeID(%q): expected error, got %v", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTypeID(%q): unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("ParseTypeID(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// TestStringTypeLengthIsPostgresRule is the ONE reading of a parameterized
// string type name, which the DDL door and the CAST door both take.
//
// The first pass of #838 had two: `expr.parseStringDest` refused `VARCHAR(0)`
// with 22023 and `ParseTypeID` matched only the NAME, so `CREATE TABLE t (a
// VARCHAR(0))` CREATED the table — one type name, two dispositions across two
// doors, which is the defect class this arc exists to close, introduced by it.
// It also said `character` where PostgreSQL says `char`, and passed
// `VARCHAR(abc)` and `TEXT(5)` through in silence.
//
// Every code and message below is postgres:17.11's own, measured.
func TestStringTypeLengthIsPostgresRule(t *testing.T) {
	for _, c := range []struct {
		in         string
		want       int
		state, msg string
	}{
		{in: "VARCHAR(255)", want: 255},
		{in: "char(4)", want: 4},
		{in: "CHARACTER VARYING(10)", want: 10},
		{in: "VARCHAR(10485760)", want: 10485760},
		{in: "VARCHAR(0)", state: "22023", msg: "length for type varchar must be at least 1"},
		{in: "CHAR(0)", state: "22023", msg: "length for type char must be at least 1"},
		{in: "CHARACTER(0)", state: "22023", msg: "length for type char must be at least 1"},
		{in: "NCHAR(0)", state: "22023", msg: "length for type char must be at least 1"},
		{in: "VARCHAR(10485761)", state: "22023",
			msg: "length for type varchar cannot exceed 10485760"},
		{in: "CHAR(100000000)", state: "22023",
			msg: "length for type char cannot exceed 10485760"},
		{in: "VARCHAR(abc)", state: "42601", msg: `syntax error at or near "abc"`},
		{in: "VARCHAR(-1)", state: "42601", msg: `syntax error at or near "-"`},
		{in: "TEXT(5)", state: "42601", msg: `type modifier is not allowed for type "text"`},
	} {
		t.Run(c.in, func(t *testing.T) {
			n, err, ok := StringTypeLength(c.in)
			if !ok {
				t.Fatalf("StringTypeLength(%q) declined; it is a parameterized string type", c.in)
			}
			if c.state == "" {
				if err != nil {
					t.Fatalf("StringTypeLength(%q): %v, want %d", c.in, err, c.want)
				}
				if n != c.want {
					t.Errorf("StringTypeLength(%q) = %d, want %d", c.in, n, c.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("StringTypeLength(%q) = %d, want [%s] %s (live PostgreSQL 17.11)",
					c.in, n, c.state, c.msg)
			}
			if got := sqlerr.StateOf(err); got != c.state || err.Error() != c.msg {
				t.Errorf("StringTypeLength(%q) = [%s] %v, want [%s] %s (live PostgreSQL 17.11)",
					c.in, got, err, c.state, c.msg)
			}
		})
	}
	// The names that carry NO modifier decline, so the caller's own rules
	// stand and an unparameterized VARCHAR is unchanged.
	for _, in := range []string{"VARCHAR", "TEXT", "STRING", "INT64", "DECIMAL(9,2)"} {
		if _, _, ok := StringTypeLength(in); ok {
			t.Errorf("StringTypeLength(%q) claimed the name; it carries no string modifier", in)
		}
	}
}

func TestParseDecimalParams(t *testing.T) {
	tests := []struct {
		input     string
		wantPrec  int
		wantScale int
	}{
		{"DECIMAL", 38, 0},
		{"DECIMAL(10,2)", 10, 2},
		{"DECIMAL(18)", 18, 0},
		{"NUMERIC(5,3)", 5, 3},
		{"DECIMAL(38,18)", 38, 18},
		{"DECIMAL(38,38)", 38, 38},
		{"DECIMAL(1,0)", 1, 0},
		// No closing paren
		{"DECIMAL(10", 38, 0},
		// Empty params
		{"DECIMAL()", 38, 0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			p, s, err := ParseDecimalParams(tt.input)
			if err != nil {
				t.Fatalf("ParseDecimalParams(%q): %v", tt.input, err)
			}
			if p != tt.wantPrec {
				t.Errorf("precision = %d, want %d", p, tt.wantPrec)
			}
			if s != tt.wantScale {
				t.Errorf("scale = %d, want %d", s, tt.wantScale)
			}
		})
	}
}

// A declaration this carrier cannot honour is REFUSED, at DDL, with 22023.
//
// Every one of these used to be accepted: DECIMAL(50,2) produced a column
// whose 16-byte FIXED_LEN_BYTE_ARRAY leaf was annotated DECIMAL(50,s) — a
// combination the Apache implementation refuses to open and no value can
// satisfy — and DECIMAL(0,0) produced the "unconstrained" sentinel from an
// EXPLICIT declaration (R8/#647).
//
// DECIMAL(50,2) is legal PostgreSQL (its numeric goes to 1000 digits), so
// this is the documented divergence of ADR-0024 item 1. A negative scale and
// a scale past the precision are legal PostgreSQL too, and have no parquet
// DECIMAL annotation at all.
func TestParseDecimalParamsRefusesADeclarationWithNoCarrier(t *testing.T) {
	for _, input := range []string{
		"DECIMAL(50,2)",
		"DECIMAL(39,0)",
		"NUMERIC(1000,2)",
		"DECIMAL(0,0)",
		"DECIMAL(-1,0)",
		"DECIMAL(9,-2)",
		"DECIMAL(9,10)",
		"DECIMAL(abc,2)",
		"DECIMAL(9,abc)",
		"DECIMAL(9,2,1)",
	} {
		t.Run(input, func(t *testing.T) {
			if _, _, err := ParseDecimalParams(input); err == nil {
				t.Fatalf("ParseDecimalParams(%q) was accepted", input)
			} else if got := sqlerr.StateOf(err); got != "22023" {
				t.Fatalf("SQLSTATE %q, want 22023 (err: %v)", got, err)
			}
			// ResolveColumn is the DDL door and must carry the refusal out.
			if _, err := ResolveColumn("d", input); err == nil {
				t.Fatalf("ResolveColumn(%q) was accepted", input)
			}
		})
	}
}

func TestResolveColumn_SimpleTypes(t *testing.T) {
	tests := []struct {
		typeStr string
		want    TypeID
	}{
		{"INT64", TypeInt64},
		{"STRING", TypeString},
		{"BOOL", TypeBool},
		{"FLOAT64", TypeFloat64},
		{"DATE", TypeDate},
	}
	for _, tt := range tests {
		t.Run(tt.typeStr, func(t *testing.T) {
			col, err := ResolveColumn("test", tt.typeStr)
			if err != nil {
				t.Fatalf("ResolveColumn(%q): %v", tt.typeStr, err)
			}
			if col.Type != tt.want {
				t.Fatalf("Type = %v, want %v", col.Type, tt.want)
			}
			if col.Name != "test" {
				t.Fatalf("Name = %q, want %q", col.Name, "test")
			}
			if !col.Nullable {
				t.Fatal("expected Nullable to be true")
			}
		})
	}
}

func TestResolveColumn_Array(t *testing.T) {
	col, err := ResolveColumn("tags", "ARRAY(STRING)")
	if err != nil {
		t.Fatalf("ResolveColumn: %v", err)
	}
	if col.Type != TypeArray {
		t.Fatalf("Type = %v, want TypeArray", col.Type)
	}
	if col.ElementType == nil {
		t.Fatal("expected non-nil ElementType")
	}
	if col.ElementType.Type != TypeString {
		t.Fatalf("ElementType.Type = %v, want TypeString", col.ElementType.Type)
	}
}

func TestResolveColumn_NestedArray(t *testing.T) {
	col, err := ResolveColumn("matrix", "ARRAY(ARRAY(INT64))")
	if err != nil {
		t.Fatalf("ResolveColumn: %v", err)
	}
	if col.Type != TypeArray {
		t.Fatalf("Type = %v, want TypeArray", col.Type)
	}
	if col.ElementType == nil || col.ElementType.Type != TypeArray {
		t.Fatal("expected ARRAY element type")
	}
	if col.ElementType.ElementType == nil || col.ElementType.ElementType.Type != TypeInt64 {
		t.Fatal("expected INT64 inner element type")
	}
}

func TestResolveColumn_Map(t *testing.T) {
	col, err := ResolveColumn("meta", "MAP(STRING, INT64)")
	if err != nil {
		t.Fatalf("ResolveColumn: %v", err)
	}
	if col.Type != TypeMap {
		t.Fatalf("Type = %v, want TypeMap", col.Type)
	}
	if col.ElementType == nil {
		t.Fatal("expected non-nil ElementType")
	}
	if len(col.ElementType.Fields) != 2 {
		t.Fatalf("expected 2 fields in entry, got %d", len(col.ElementType.Fields))
	}
	if col.ElementType.Fields[0].Type != TypeString {
		t.Fatalf("key type = %v, want TypeString", col.ElementType.Fields[0].Type)
	}
	if col.ElementType.Fields[1].Type != TypeInt64 {
		t.Fatalf("value type = %v, want TypeInt64", col.ElementType.Fields[1].Type)
	}
}

func TestResolveColumn_MapWrongArity(t *testing.T) {
	_, err := ResolveColumn("bad", "MAP(STRING)")
	if err == nil {
		t.Fatal("expected error for MAP with 1 param")
	}

	_, err = ResolveColumn("bad", "MAP(STRING, INT64, BOOL)")
	if err == nil {
		t.Fatal("expected error for MAP with 3 params")
	}
}

// The field NAMES come back lower-cased, like every other column name in the
// system. They used to come back upper-cased — ResolveColumn sliced its inner
// text out of an upper-cased copy of the whole type string — which was
// invisible while the function had no non-test caller, and became a
// cross-door schema divergence the moment DeclaredColumn started calling it
// (#675). The declaration here is deliberately written in CAPITALS so the
// lower-casing is what is being asserted.
func TestResolveColumn_Row(t *testing.T) {
	col, err := ResolveColumn("addr", "ROW(CITY STRING, ZIP STRING)")
	if err != nil {
		t.Fatalf("ResolveColumn: %v", err)
	}
	if col.Type != TypeRow {
		t.Fatalf("Type = %v, want TypeRow", col.Type)
	}
	if len(col.Fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(col.Fields))
	}
	if col.Fields[0].Name != "city" {
		t.Fatalf("field[0].Name = %q, want %q", col.Fields[0].Name, "city")
	}
	if col.Fields[1].Name != "zip" {
		t.Fatalf("field[1].Name = %q, want %q", col.Fields[1].Name, "zip")
	}
}

func TestResolveColumn_Struct(t *testing.T) {
	col, err := ResolveColumn("data", "STRUCT(X INT64, Y FLOAT64)")
	if err != nil {
		t.Fatalf("ResolveColumn: %v", err)
	}
	if col.Type != TypeRow {
		t.Fatalf("STRUCT should map to TypeRow, got %v", col.Type)
	}
}

func TestResolveColumn_Decimal(t *testing.T) {
	col, err := ResolveColumn("price", "DECIMAL(10,2)")
	if err != nil {
		t.Fatalf("ResolveColumn: %v", err)
	}
	if col.Type != TypeDecimal {
		t.Fatalf("Type = %v, want TypeDecimal", col.Type)
	}
	if col.Precision != 10 {
		t.Fatalf("Precision = %d, want 10", col.Precision)
	}
	if col.Scale != 2 {
		t.Fatalf("Scale = %d, want 2", col.Scale)
	}
}

func TestResolveColumn_SimpleDecimal(t *testing.T) {
	// Simple "DECIMAL" without params
	col, err := ResolveColumn("amount", "DECIMAL")
	if err != nil {
		t.Fatalf("ResolveColumn: %v", err)
	}
	if col.Type != TypeDecimal {
		t.Fatalf("Type = %v, want TypeDecimal", col.Type)
	}
	if col.Precision != 38 {
		t.Fatalf("Precision = %d, want 38", col.Precision)
	}
	if col.Scale != 0 {
		t.Fatalf("Scale = %d, want 0", col.Scale)
	}
}

func TestResolveColumn_Errors(t *testing.T) {
	// Unmatched paren
	_, err := ResolveColumn("bad", "ARRAY(STRING")
	if err == nil {
		t.Fatal("expected error for unmatched paren")
	}

	// Unknown type
	_, err = ResolveColumn("bad", "BANANA")
	if err == nil {
		t.Fatal("expected error for unknown type")
	}

	// Invalid ARRAY element
	_, err = ResolveColumn("bad", "ARRAY(BANANA)")
	if err == nil {
		t.Fatal("expected error for invalid ARRAY element type")
	}

	// Invalid MAP key
	_, err = ResolveColumn("bad", "MAP(BANANA, INT64)")
	if err == nil {
		t.Fatal("expected error for invalid MAP key type")
	}

	// Invalid MAP value
	_, err = ResolveColumn("bad", "MAP(STRING, BANANA)")
	if err == nil {
		t.Fatal("expected error for invalid MAP value type")
	}

	// Invalid ROW field (no type)
	_, err = ResolveColumn("bad", "ROW(fieldonly)")
	if err == nil {
		t.Fatal("expected error for ROW field without type")
	}

	// Invalid ROW field type
	_, err = ResolveColumn("bad", "ROW(name BANANA)")
	if err == nil {
		t.Fatal("expected error for ROW field with invalid type")
	}
}

func TestSplitTopLevel(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"STRING, INT64", []string{"STRING", "INT64"}},
		{"ARRAY(STRING), INT64", []string{"ARRAY(STRING)", "INT64"}},
		{"STRING", []string{"STRING"}},
		{"MAP(STRING, INT64), ARRAY(ROW(X INT64, Y INT64))", []string{"MAP(STRING, INT64)", "ARRAY(ROW(X INT64, Y INT64))"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := splitTopLevel(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d; got %v", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSchemaColumnIndex(t *testing.T) {
	s := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat64},
		},
	}

	if idx := s.ColumnIndex("id"); idx != 0 {
		t.Fatalf("ColumnIndex(id) = %d, want 0", idx)
	}
	if idx := s.ColumnIndex("name"); idx != 1 {
		t.Fatalf("ColumnIndex(name) = %d, want 1", idx)
	}
	if idx := s.ColumnIndex("score"); idx != 2 {
		t.Fatalf("ColumnIndex(score) = %d, want 2", idx)
	}
	if idx := s.ColumnIndex("nonexistent"); idx != -1 {
		t.Fatalf("ColumnIndex(nonexistent) = %d, want -1", idx)
	}
}

func TestSchemaHasColumn(t *testing.T) {
	s := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
		},
	}

	if !s.HasColumn("id") {
		t.Fatal("HasColumn(id) should be true")
	}
	if s.HasColumn("nonexistent") {
		t.Fatal("HasColumn(nonexistent) should be false")
	}
}

func TestSchemaHasNestedColumns(t *testing.T) {
	// No nested columns
	flat := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
		},
	}
	if flat.HasNestedColumns() {
		t.Fatal("flat schema should not have nested columns")
	}

	// ARRAY
	withArray := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "tags", Type: TypeArray},
		},
	}
	if !withArray.HasNestedColumns() {
		t.Fatal("schema with ARRAY should have nested columns")
	}

	// ROW
	withRow := Schema{
		Columns: []Column{
			{Name: "addr", Type: TypeRow},
		},
	}
	if !withRow.HasNestedColumns() {
		t.Fatal("schema with ROW should have nested columns")
	}

	// MAP
	withMap := Schema{
		Columns: []Column{
			{Name: "meta", Type: TypeMap},
		},
	}
	if !withMap.HasNestedColumns() {
		t.Fatal("schema with MAP should have nested columns")
	}
}

func TestSchemaFlatColumns(t *testing.T) {
	s := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "tags", Type: TypeArray, ElementType: &Column{
				Name: "element", Type: TypeString,
			}},
			{Name: "addr", Type: TypeRow, Fields: []Column{
				{Name: "city", Type: TypeString},
				{Name: "zip", Type: TypeString},
			}},
			{Name: "meta", Type: TypeMap, ElementType: &Column{
				Name: "entry", Type: TypeRow, Fields: []Column{
					{Name: "key", Type: TypeString},
					{Name: "value", Type: TypeInt64},
				},
			}},
		},
	}

	flat := s.FlatColumns()
	// id + element (from tags array) + city + zip (from row) + key + value (from map entry) = 6
	if len(flat) != 6 {
		names := make([]string, len(flat))
		for i, c := range flat {
			names[i] = c.Name
		}
		t.Fatalf("expected 6 flat columns, got %d: %v", len(flat), names)
	}
}

func TestSchemaColumnNames(t *testing.T) {
	s := Schema{
		Columns: []Column{
			{Name: "a"},
			{Name: "b"},
			{Name: "c"},
		},
	}
	names := s.ColumnNames()
	if len(names) != 3 || names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Fatalf("ColumnNames = %v, want [a b c]", names)
	}
}

func TestSchemaColumnNames_Empty(t *testing.T) {
	s := Schema{}
	names := s.ColumnNames()
	if len(names) != 0 {
		t.Fatalf("expected empty names, got %v", names)
	}
}
