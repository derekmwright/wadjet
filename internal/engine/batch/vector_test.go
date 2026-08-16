package batch

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestNewVectorTypes(t *testing.T) {
	types := []struct {
		typ    TypeID
		name   string
	}{
		{TypeBool, "bool"},
		{TypeInt32, "int32"},
		{TypeInt64, "int64"},
		{TypeFloat32, "float32"},
		{TypeFloat64, "float64"},
		{TypeString, "string"},
		{TypeBytes, "bytes"},
		{TypeTimestamp, "timestamp"},
		{TypeIPv4, "ipv4"},
		{TypeIPv6, "ipv6"},
		{TypeCIDR, "cidr"},
		{TypeMAC, "mac"},
		{TypePort, "port"},
		{TypeProtocol, "protocol"},
		{TypeDuration, "duration"},
		{TypeUUID, "uuid"},
		{TypeDate, "date"},
		{TypeDecimal, "decimal"},
		{TypeArray, "array"},
		{TypeMap, "map"},
		{TypeRow, "row"},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			v := NewVector(tt.typ, 4)
			if v.Type != tt.typ {
				t.Fatalf("expected type %v, got %v", tt.typ, v.Type)
			}
			if v.Len != 4 {
				t.Fatalf("expected len 4, got %d", v.Len)
			}
		})
	}
}

func TestTypedAccessors(t *testing.T) {
	t.Run("GetInt64", func(t *testing.T) {
		v := NewVector(TypeInt64, 3)
		v.Int64Data[0] = 42
		v.Int64Data[1] = -10
		v.Nulls.SetNull(2)

		val, ok := v.GetInt64(0)
		if !ok || val != 42 {
			t.Fatalf("expected (42, true), got (%d, %v)", val, ok)
		}
		val, ok = v.GetInt64(1)
		if !ok || val != -10 {
			t.Fatalf("expected (-10, true), got (%d, %v)", val, ok)
		}
		val, ok = v.GetInt64(2)
		if ok || val != 0 {
			t.Fatalf("expected (0, false) for null, got (%d, %v)", val, ok)
		}
	})

	t.Run("GetFloat64", func(t *testing.T) {
		v := NewVector(TypeFloat64, 3)
		v.Float64Data[0] = 3.14
		v.Nulls.SetNull(1)
		v.Float64Data[2] = -2.5

		val, ok := v.GetFloat64(0)
		if !ok || val != 3.14 {
			t.Fatalf("expected (3.14, true), got (%f, %v)", val, ok)
		}
		_, ok = v.GetFloat64(1)
		if ok {
			t.Fatal("expected false for null")
		}
	})

	t.Run("GetInt32", func(t *testing.T) {
		v := NewVector(TypeInt32, 2)
		v.Int32Data[0] = 100
		v.Nulls.SetNull(1)

		val, ok := v.GetInt32(0)
		if !ok || val != 100 {
			t.Fatalf("expected (100, true), got (%d, %v)", val, ok)
		}
		_, ok = v.GetInt32(1)
		if ok {
			t.Fatal("expected false for null")
		}
	})

	t.Run("GetFloat32", func(t *testing.T) {
		v := NewVector(TypeFloat32, 2)
		v.Float32Data[0] = 1.5
		v.Nulls.SetNull(1)

		val, ok := v.GetFloat32(0)
		if !ok || val != 1.5 {
			t.Fatalf("expected (1.5, true), got (%f, %v)", val, ok)
		}
		_, ok = v.GetFloat32(1)
		if ok {
			t.Fatal("expected false for null")
		}
	})

	t.Run("GetBool", func(t *testing.T) {
		v := NewVector(TypeBool, 3)
		v.BoolData[0] = true
		v.BoolData[1] = false
		v.Nulls.SetNull(2)

		val, ok := v.GetBool(0)
		if !ok || !val {
			t.Fatalf("expected (true, true), got (%v, %v)", val, ok)
		}
		val, ok = v.GetBool(1)
		if !ok || val {
			t.Fatalf("expected (false, true), got (%v, %v)", val, ok)
		}
		_, ok = v.GetBool(2)
		if ok {
			t.Fatal("expected false for null")
		}
	})

	t.Run("GetString", func(t *testing.T) {
		v := NewVector(TypeString, 3)
		v.BytesData.Set(0, []byte("hello"))
		v.BytesData.Set(1, nil)
		v.Nulls.SetNull(1)
		v.BytesData.Set(2, []byte("world"))

		val, ok := v.GetString(0)
		if !ok || val != "hello" {
			t.Fatalf("expected (hello, true), got (%q, %v)", val, ok)
		}
		_, ok = v.GetString(1)
		if ok {
			t.Fatal("expected false for null")
		}
		val, ok = v.GetString(2)
		if !ok || val != "world" {
			t.Fatalf("expected (world, true), got (%q, %v)", val, ok)
		}
	})
}

func TestGetNumericFloat64AllTypes(t *testing.T) {
	t.Run("Int32", func(t *testing.T) {
		v := NewVector(TypeInt32, 1)
		v.Int32Data[0] = 42
		f, ok := v.GetNumericFloat64(0)
		if !ok || f != 42.0 {
			t.Fatalf("expected (42.0, true), got (%f, %v)", f, ok)
		}
	})

	t.Run("Float32", func(t *testing.T) {
		v := NewVector(TypeFloat32, 1)
		v.Float32Data[0] = 3.5
		f, ok := v.GetNumericFloat64(0)
		if !ok || f != 3.5 {
			t.Fatalf("expected (3.5, true), got (%f, %v)", f, ok)
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		v := NewVector(TypeString, 1)
		v.BytesData.Set(0, []byte("hello"))
		_, ok := v.GetNumericFloat64(0)
		if ok {
			t.Fatal("expected false for string type")
		}
	})
}

func TestVectorString(t *testing.T) {
	v := NewVector(TypeInt64, 5)
	v.Nulls.SetNull(2)
	s := v.String()
	if s == "" {
		t.Fatal("expected non-empty string representation")
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		expect string
	}{
		{"nil", nil, "NULL"},
		{"string", "hello", "hello"},
		{"int64", int64(42), "42"},
		{"float64", float64(3.14), "3.14"},
		{"array_empty", []any{}, "[]"},
		{"array_ints", []any{int64(1), int64(2)}, "[1, 2]"},
		{"array_strings", []any{"a", "b"}, "['a', 'b']"},
		{"array_with_null", []any{"a", nil}, "['a', NULL]"},
		{"map", map[string]any{"key": "val"}, "{key: 'val'}"},
		{"map_with_null", map[string]any{"key": nil}, "{key: NULL}"},
		{"map_with_int", map[string]any{"n": int64(5)}, "{n: 5}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatValue(tt.input)
			if got != tt.expect {
				t.Fatalf("FormatValue(%v) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

func TestFormatUUIDAndParseUUID(t *testing.T) {
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	v := NewVector(TypeUUID, 1)
	v.SetValue(0, uuid)

	got := v.GetValue(0)
	if got != uuid {
		t.Fatalf("UUID roundtrip: expected %q, got %q", uuid, got)
	}
}

func TestParseUUIDInvalid(t *testing.T) {
	// Too short
	result := parseUUID("abc")
	if result != nil {
		t.Fatalf("expected nil for short UUID, got %v", result)
	}

	// Invalid hex
	result = parseUUID("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")
	if result != nil {
		t.Fatalf("expected nil for invalid hex, got %v", result)
	}

	// No dashes
	result = parseUUID("550e8400e29b41d4a716446655440000")
	if result == nil {
		t.Fatal("expected non-nil for dash-free UUID")
	}
}

func TestGetValueUUIDShort(t *testing.T) {
	v := NewVector(TypeUUID, 1)
	// Set a short byte slice directly (not 16 bytes)
	v.BytesData.Set(0, []byte("short"))

	got := v.GetValue(0)
	if got != "" {
		t.Fatalf("expected empty string for short UUID, got %q", got)
	}
}

func TestGetValueIPv6Short(t *testing.T) {
	v := NewVector(TypeIPv6, 1)
	// Set a short byte slice (not 16 bytes)
	v.BytesData.Set(0, []byte("short"))

	got := v.GetValue(0)
	if got != "" {
		t.Fatalf("expected empty string for short IPv6, got %q", got)
	}
}

func TestFormatDateAndParseDateString(t *testing.T) {
	t.Run("formatDate", func(t *testing.T) {
		// Epoch = 1970-01-01
		if got := FormatDate(0); got != "1970-01-01" {
			t.Fatalf("expected 1970-01-01, got %q", got)
		}
		// Day 365 = 1971-01-01
		if got := FormatDate(365); got != "1971-01-01" {
			t.Fatalf("expected 1971-01-01, got %q", got)
		}
	})

	t.Run("parseDateString", func(t *testing.T) {
		days := parseDateString("1970-01-01")
		if days != 0 {
			t.Fatalf("expected 0 days, got %d", days)
		}
		// Invalid date
		days = parseDateString("not-a-date")
		if days != 0 {
			t.Fatalf("expected 0 for invalid date, got %d", days)
		}
	})

	t.Run("roundtrip", func(t *testing.T) {
		v := NewVector(TypeDate, 1)
		v.SetValue(0, "2024-03-15")
		got := v.GetValue(0)
		if got != "2024-03-15" {
			t.Fatalf("expected 2024-03-15, got %q", got)
		}
	})
}

func TestSetValueDate(t *testing.T) {
	v := NewVector(TypeDate, 4)
	v.SetValue(0, int32(100))
	v.SetValue(1, int(200))
	v.SetValue(2, int64(300))
	v.SetValue(3, "2020-01-01")

	if v.Int32Data[0] != 100 {
		t.Fatalf("expected 100, got %d", v.Int32Data[0])
	}
	if v.Int32Data[1] != 200 {
		t.Fatalf("expected 200, got %d", v.Int32Data[1])
	}
	if v.Int32Data[2] != 300 {
		t.Fatalf("expected 300, got %d", v.Int32Data[2])
	}
}

func TestSetValueDecimalVariousInputs(t *testing.T) {
	v := NewVectorWithScale(TypeDecimal, 5, 2)
	v.SetValue(0, int64(100))
	v.SetValue(1, int(200))
	v.SetValue(2, int32(300))
	v.SetValue(3, float32(4.5))
	v.SetValue(4, Int128From(500))

	if v.DecimalData.Data[0].ToInt64() != 100 {
		t.Fatalf("expected 100, got %d", v.DecimalData.Data[0].ToInt64())
	}
	if v.DecimalData.Data[1].ToInt64() != 200 {
		t.Fatalf("expected 200, got %d", v.DecimalData.Data[1].ToInt64())
	}
}

func TestSetValueStringCoercion(t *testing.T) {
	v := NewVector(TypeString, 2)
	// Non-string value should be coerced
	v.SetValue(0, int64(42))
	v.SetValue(1, true)

	if got := v.BytesData.StringValue(0); got != "42" {
		t.Fatalf("expected '42', got %q", got)
	}
}

func TestSetValueIPv4Edge(t *testing.T) {
	v := NewVector(TypeIPv4, 3)
	// Invalid IP
	v.SetValue(0, "not-an-ip")
	// Int32 input
	v.SetValue(1, int32(100))
	// Unknown type input
	v.SetValue(2, true) // bool is not handled

	if v.Int64Data[1] != 100 {
		t.Fatalf("expected 100, got %d", v.Int64Data[1])
	}
}

func TestSetValueIPv6NonString(t *testing.T) {
	v := NewVector(TypeIPv6, 2)
	// Non-string should set nil bytes
	v.SetValue(0, int64(42))

	// Invalid IP string
	v.SetValue(1, "not-an-ip")
}

func TestSetValueCIDRNonString(t *testing.T) {
	v := NewVector(TypeCIDR, 1)
	v.SetValue(0, int64(42))
	// non-string goes to the nil path
}

func TestSetValueMACEdge(t *testing.T) {
	v := NewVector(TypeMAC, 2)
	// Invalid MAC
	v.SetValue(0, "not-a-mac")
	// Unknown type
	v.SetValue(1, true)
}

func TestSetValueDuration(t *testing.T) {
	v := NewVector(TypeDuration, 4)
	v.SetValue(0, int64(100))
	v.SetValue(1, int(200))
	v.SetValue(2, int32(300))
	v.SetValue(3, float64(400.0))

	if v.Int64Data[0] != 100 {
		t.Fatalf("expected 100, got %d", v.Int64Data[0])
	}
	if v.Int64Data[3] != 400 {
		t.Fatalf("expected 400, got %d", v.Int64Data[3])
	}
}

func TestSetValuePortAndProtocol(t *testing.T) {
	v := NewVector(TypePort, 4)
	v.SetValue(0, int32(80))
	v.SetValue(1, int(443))
	v.SetValue(2, int64(8080))
	v.SetValue(3, float64(22.0))

	if v.Int32Data[0] != 80 {
		t.Fatalf("expected 80, got %d", v.Int32Data[0])
	}
	if v.Int32Data[3] != 22 {
		t.Fatalf("expected 22, got %d", v.Int32Data[3])
	}
}

func TestSetValueUUIDBytes(t *testing.T) {
	v := NewVector(TypeUUID, 2)
	raw := make([]byte, 16)
	for i := range raw {
		raw[i] = byte(i)
	}
	v.SetValue(0, raw)
	// Non-string, non-bytes
	v.SetValue(1, int64(42))
}

func TestSetValueArrayMap(t *testing.T) {
	t.Run("NilChild", func(t *testing.T) {
		v := NewVector(TypeArray, 1)
		v.Child = nil
		v.SetValue(0, []any{int64(1)})
		// Should not panic, just return
	})

	t.Run("MapOfStrings", func(t *testing.T) {
		v := NewVector(TypeArray, 1)
		v.Child = NewVector(TypeString, 0)
		v.Child.BytesData = NewBytesColumn(0)
		v.SetValue(0, []map[string]any{{"element": "hello"}, {"element": "world"}})

		got := v.GetValue(0).([]any)
		if len(got) != 2 {
			t.Fatalf("expected 2 elements, got %d", len(got))
		}
	})

	t.Run("UnsupportedInput", func(t *testing.T) {
		v := NewVector(TypeArray, 1)
		v.Child = NewVector(TypeInt64, 0)
		v.SetValue(0, "not-an-array")
		// Should not panic
	})
}

func TestSetValueRowEdge(t *testing.T) {
	t.Run("NonMapInput", func(t *testing.T) {
		v := NewRowVector(1, []string{"name"}, []TypeID{TypeString})
		v.SetValue(0, "not-a-map")
		// Should not panic
	})

	t.Run("NilChildren", func(t *testing.T) {
		v := NewVector(TypeRow, 1)
		v.Children = nil
		v.SetValue(0, map[string]any{"key": "val"})
		// Should not panic
	})
}

func TestGetValueRowNilChildren(t *testing.T) {
	v := NewVector(TypeRow, 1)
	v.Children = nil
	got := v.GetValue(0)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestGetValueArrayNilChild(t *testing.T) {
	v := NewVector(TypeArray, 1)
	v.Child = nil
	got := v.GetValue(0)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestNewRowVector(t *testing.T) {
	v := NewRowVector(3, []string{"name", "age"}, []TypeID{TypeString, TypeInt64})
	if v.Type != TypeRow {
		t.Fatalf("expected TypeRow, got %v", v.Type)
	}
	if len(v.FieldNames) != 2 {
		t.Fatalf("expected 2 field names, got %d", len(v.FieldNames))
	}
	if len(v.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(v.Children))
	}
	if v.Children[0].Type != TypeString {
		t.Fatalf("expected child 0 to be string, got %v", v.Children[0].Type)
	}
	if v.Children[1].Len != 3 {
		t.Fatalf("expected child len 3, got %d", v.Children[1].Len)
	}
}

func TestNewMapVector(t *testing.T) {
	v := NewMapVector(2, TypeString, TypeInt64)
	if v.Type != TypeMap {
		t.Fatalf("expected TypeMap, got %v", v.Type)
	}
	if v.Child == nil {
		t.Fatal("expected non-nil child")
	}
	if v.Child.Type != TypeRow {
		t.Fatalf("expected child type Row, got %v", v.Child.Type)
	}
	if len(v.Child.FieldNames) != 2 {
		t.Fatalf("expected 2 field names, got %d", len(v.Child.FieldNames))
	}
}

func TestAppendToVector(t *testing.T) {
	types := []struct {
		typ  TypeID
		name string
		val  any
	}{
		{TypeBool, "bool", true},
		{TypeInt32, "int32", int32(42)},
		{TypeInt64, "int64", int64(100)},
		{TypeFloat32, "float32", float32(1.5)},
		{TypeFloat64, "float64", float64(3.14)},
		{TypeString, "string", "hello"},
		{TypeDecimal, "decimal", Int128From(123)},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			v := NewVector(tt.typ, 0)
			if tt.typ == TypeString {
				v.BytesData = NewBytesColumn(0)
			}
			appendToVector(v, tt.val)
			appendToVector(v, nil) // null
			if v.Len != 2 {
				t.Fatalf("expected len 2, got %d", v.Len)
			}
			if v.Nulls.IsNullFast(0) {
				t.Fatal("expected first value non-null")
			}
			if !v.Nulls.IsNullFast(1) {
				t.Fatal("expected second value null")
			}
		})
	}

	t.Run("array", func(t *testing.T) {
		v := NewVector(TypeArray, 0)
		v.Offsets = make([]int32, 1) // need initial offset
		appendToVector(v, nil)
		if v.Len != 1 {
			t.Fatalf("expected len 1, got %d", v.Len)
		}
	})

	t.Run("row", func(t *testing.T) {
		v := NewRowVector(0, []string{"a"}, []TypeID{TypeInt64})
		v.Children[0].Int64Data = nil
		appendToVector(v, nil)
		if v.Len != 1 {
			t.Fatalf("expected len 1, got %d", v.Len)
		}
	})
}

func TestColumnIndex(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString},
	}
	b := NewRecordBatch(schema, 1)

	if idx := b.ColumnIndex("id"); idx != 0 {
		t.Fatalf("expected 0, got %d", idx)
	}
	if idx := b.ColumnIndex("name"); idx != 1 {
		t.Fatalf("expected 1, got %d", idx)
	}
	if idx := b.ColumnIndex("missing"); idx != -1 {
		t.Fatalf("expected -1, got %d", idx)
	}
}

func TestColumnByNameNotFound(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	b := NewRecordBatch(schema, 1)
	if v := b.ColumnByName("missing"); v != nil {
		t.Fatal("expected nil for missing column")
	}
}

func TestReleaseWithoutPool(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	b := NewRecordBatch(schema, 1)
	b.Release() // should not panic (pool is nil)
}

func TestCompact(t *testing.T) {
	t.Run("NoSel", func(t *testing.T) {
		schema := []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
		}
		b := FromRows(schema, []map[string]any{
			{"id": int64(1)},
			{"id": int64(2)},
		})
		got := b.Compact()
		if got != b {
			t.Fatal("expected same batch when no selection vector")
		}
	})

	t.Run("WithSel", func(t *testing.T) {
		schema := []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
			{Name: "score", Type: parquet.TypeFloat64},
			{Name: "active", Type: parquet.TypeBool},
			{Name: "age", Type: parquet.TypeInt32},
			{Name: "rate", Type: parquet.TypeFloat32},
		}
		b := FromRows(schema, []map[string]any{
			{"id": int64(1), "name": "alice", "score": 90.0, "active": true, "age": int32(30), "rate": float32(1.5)},
			{"id": int64(2), "name": "bob", "score": 80.0, "active": false, "age": int32(25), "rate": float32(2.0)},
			{"id": int64(3), "name": "carol", "score": 95.0, "active": true, "age": int32(35), "rate": float32(1.0)},
		})
		b.Sel = []uint32{0, 2}
		out := b.Compact()
		if out.Len != 2 {
			t.Fatalf("expected 2 rows, got %d", out.Len)
		}
		rows := out.ToRows()
		if rows[0]["id"].(int64) != 1 {
			t.Fatalf("expected id=1, got %v", rows[0]["id"])
		}
		if rows[1]["name"].(string) != "carol" {
			t.Fatalf("expected carol, got %v", rows[1]["name"])
		}
	})

	t.Run("WithNull", func(t *testing.T) {
		schema := []parquet.Column{
			{Name: "val", Type: parquet.TypeInt64},
		}
		b := FromRows(schema, []map[string]any{
			{"val": nil},
			{"val": int64(42)},
		})
		b.Sel = []uint32{0, 1}
		out := b.Compact()
		rows := out.ToRows()
		if rows[0]["val"] != nil {
			t.Fatalf("expected nil, got %v", rows[0]["val"])
		}
		if rows[1]["val"].(int64) != 42 {
			t.Fatalf("expected 42, got %v", rows[1]["val"])
		}
	})

	t.Run("WithDecimal", func(t *testing.T) {
		schema := []parquet.Column{
			{Name: "val", Type: parquet.TypeDecimal},
		}
		b := NewRecordBatch(schema, 2)
		b.Columns[0].DecimalData.Data[0] = Int128From(100)
		b.Columns[0].DecimalData.Data[1] = Int128From(200)
		b.Sel = []uint32{1}
		out := b.Compact()
		if out.Len != 1 {
			t.Fatalf("expected 1 row, got %d", out.Len)
		}
	})
}

func TestInt128IsZero(t *testing.T) {
	zero := Int128{}
	if !zero.IsZero() {
		t.Fatal("expected IsZero true for zero value")
	}
	nonZero := Int128From(1)
	if nonZero.IsZero() {
		t.Fatal("expected IsZero false for non-zero")
	}
}

func TestInt128NegZeroCarry(t *testing.T) {
	// Neg of zero should be zero
	zero := Int128{}
	neg := zero.Neg()
	// lo = ^0 + 1 = 0 (wraps), hi = ^0 + 1 = 0
	if neg.Hi != 0 || neg.Lo != 0 {
		t.Fatalf("Neg(0) = {%d, %d}, expected {0, 0}", neg.Hi, neg.Lo)
	}
}

func TestBitmapNewBitmapAllNull(t *testing.T) {
	bm := NewBitmapAllNull(10)
	if bm.NullCount() != 10 {
		t.Fatalf("expected 10 nulls, got %d", bm.NullCount())
	}
	for i := 0; i < 10; i++ {
		if !bm.IsNull(i) {
			t.Fatalf("expected bit %d to be null", i)
		}
	}
}

func TestBitmapBoundsChecking(t *testing.T) {
	bm := NewBitmap(10)
	// Out of bounds should return true (null)
	if !bm.IsNull(-1) {
		t.Fatal("expected true for negative index")
	}
	if !bm.IsNull(10) {
		t.Fatal("expected true for out of bounds index")
	}
	// SetNull out of bounds should not panic
	bm.SetNull(-1)
	bm.SetNull(10)
	// SetValid out of bounds should not panic
	bm.SetValid(-1)
	bm.SetValid(10)
}

func TestBitmapGrowCapacity(t *testing.T) {
	bm := NewBitmap(60)
	bm.SetNull(5)

	// Grow within same word capacity
	bm = bm.Grow(62)
	if bm.Len() != 62 {
		t.Fatalf("expected len 62, got %d", bm.Len())
	}
	if !bm.IsNull(5) {
		t.Fatal("expected bit 5 still null")
	}
	if bm.IsNull(61) {
		t.Fatal("expected new bit 61 valid")
	}

	// Grow to smaller (no-op)
	bm2 := bm.Grow(10)
	if bm2.Len() != 62 {
		t.Fatalf("expected len 62 (no change), got %d", bm2.Len())
	}
}

func TestNewVectorFromColumn(t *testing.T) {
	col := parquet.Column{
		Name: "nested_array",
		Type: parquet.TypeArray,
		ElementType: &parquet.Column{
			Name: "elem",
			Type: parquet.TypeInt64,
		},
	}
	v := newVectorFromColumn(col, 3)
	if v.Type != TypeArray {
		t.Fatalf("expected TypeArray, got %v", v.Type)
	}
	if v.Child == nil {
		t.Fatal("expected non-nil child")
	}
	if v.Child.Type != TypeInt64 {
		t.Fatalf("expected child type Int64, got %v", v.Child.Type)
	}
}

func TestNewVectorFromColumnRow(t *testing.T) {
	col := parquet.Column{
		Name: "person",
		Type: parquet.TypeRow,
		Fields: []parquet.Column{
			{Name: "name", Type: parquet.TypeString},
			{Name: "age", Type: parquet.TypeInt64},
		},
	}
	v := newVectorFromColumn(col, 2)
	if v.Type != TypeRow {
		t.Fatalf("expected TypeRow, got %v", v.Type)
	}
	if len(v.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(v.Children))
	}
	if v.FieldNames[0] != "name" {
		t.Fatalf("expected field 0 name 'name', got %q", v.FieldNames[0])
	}
}

func TestResetStringVector(t *testing.T) {
	schema := []parquet.Column{
		{Name: "name", Type: parquet.TypeString},
	}
	b := FromRows(schema, []map[string]any{
		{"name": "hello"},
		{"name": "world"},
	})
	b.Reset(3)
	if b.Len != 3 {
		t.Fatalf("expected len 3, got %d", b.Len)
	}
	if b.Sel != nil {
		t.Fatal("expected nil sel after reset")
	}
}

func TestPoolOverflow(t *testing.T) {
	schema := []parquet.Column{
		{Name: "x", Type: parquet.TypeInt64},
	}
	pool := NewBatchPool(schema, 10)

	// Put more batches than maxPoolPerClass
	batches := make([]*RecordBatch, 20)
	for i := range batches {
		batches[i] = pool.Get()
	}
	for _, b := range batches {
		pool.Put(b)
	}
	// Should not panic, excess batches are silently dropped
}

func TestSetValueFloat64Coercions(t *testing.T) {
	v := NewVector(TypeFloat64, 4)
	v.SetValue(0, float64(1.0))
	v.SetValue(1, float32(2.0))
	v.SetValue(2, int64(3))
	v.SetValue(3, int(4))

	if v.Float64Data[0] != 1.0 {
		t.Fatalf("expected 1.0, got %f", v.Float64Data[0])
	}
	if v.Float64Data[1] != 2.0 {
		t.Fatalf("expected 2.0, got %f", v.Float64Data[1])
	}
	if v.Float64Data[2] != 3.0 {
		t.Fatalf("expected 3.0, got %f", v.Float64Data[2])
	}
	if v.Float64Data[3] != 4.0 {
		t.Fatalf("expected 4.0, got %f", v.Float64Data[3])
	}
}

func TestSetValueFloat32Coercions(t *testing.T) {
	v := NewVector(TypeFloat32, 2)
	v.SetValue(0, float32(1.0))
	v.SetValue(1, float64(2.0))

	if v.Float32Data[0] != 1.0 {
		t.Fatalf("expected 1.0, got %f", v.Float32Data[0])
	}
	if v.Float32Data[1] != 2.0 {
		t.Fatalf("expected 2.0, got %f", v.Float32Data[1])
	}
}

func TestSetValueInt32Coercions(t *testing.T) {
	v := NewVector(TypeInt32, 4)
	v.SetValue(0, int32(1))
	v.SetValue(1, int(2))
	v.SetValue(2, int64(3))
	v.SetValue(3, float64(4.0))

	for i, expected := range []int32{1, 2, 3, 4} {
		if v.Int32Data[i] != expected {
			t.Fatalf("index %d: expected %d, got %d", i, expected, v.Int32Data[i])
		}
	}
}

func TestSetValueInt64Coercions(t *testing.T) {
	v := NewVector(TypeInt64, 4)
	v.SetValue(0, int64(1))
	v.SetValue(1, int(2))
	v.SetValue(2, int32(3))
	v.SetValue(3, float64(4.0))

	for i, expected := range []int64{1, 2, 3, 4} {
		if v.Int64Data[i] != expected {
			t.Fatalf("index %d: expected %d, got %d", i, expected, v.Int64Data[i])
		}
	}
}
