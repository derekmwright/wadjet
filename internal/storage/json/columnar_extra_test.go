package json

import (
	"fmt"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestColumnarReaderSkipValue(t *testing.T) {
	// Schema inference samples up to 100 rows (defaultSampleSize=100).
	// To exercise skipValue, new columns must appear AFTER the sample window.
	// Generate 101 rows with column "a", then row 102 introduces unknown columns.
	var sb strings.Builder
	for i := 0; i < 101; i++ {
		sb.WriteString(fmt.Sprintf(`{"a":%d}`, i))
		sb.WriteByte('\n')
	}
	// Row 102: extra columns of various types that should all be skipped
	sb.WriteString(`{"a":999,"extra_str":"hello","extra_num":42,"extra_bool":true,"extra_null":null,"extra_obj":{"x":1},"extra_arr":[1,2]}`)
	sb.WriteByte('\n')

	r, err := NewColumnarReader([]byte(sb.String()))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if len(schema) != 1 || schema[0].Name != "a" {
		t.Fatalf("expected schema with just 'a', got %v", schema)
	}
	// Consume all batches
	total := 0
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		total += b.Len
	}
	if total != 102 {
		t.Fatalf("expected 102 rows, got %d", total)
	}
}

func TestColumnarReaderSkipValueEscapedStrings(t *testing.T) {
	// Exercise skipValue with escaped strings in both objects and arrays
	var sb strings.Builder
	for i := 0; i < 101; i++ {
		sb.WriteString(fmt.Sprintf(`{"a":%d}`, i))
		sb.WriteByte('\n')
	}
	// Row with extra object containing escaped string
	sb.WriteString(`{"a":999,"extra_obj":{"key":"val\"ue"},"extra_arr":["str\"with\\escape"]}`)
	sb.WriteByte('\n')

	r, err := NewColumnarReader([]byte(sb.String()))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		total += b.Len
	}
	if total != 102 {
		t.Fatalf("expected 102 rows, got %d", total)
	}
}

func TestColumnarReaderBoolToInt(t *testing.T) {
	// First row: int, second row: bool -> schema promotes to Int64
	// writeBoolTrue/writeBoolFalse with TypeInt64 target
	data := `{"v":42}
{"v":true}
{"v":false}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeInt64 {
		t.Fatalf("expected TypeInt64, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 3 {
		t.Fatal("expected 3 rows")
	}
	// true -> 1, false -> 0
	if b.Columns[0].Int64Data[1] != 1 {
		t.Errorf("expected true -> 1, got %d", b.Columns[0].Int64Data[1])
	}
	if b.Columns[0].Int64Data[2] != 0 {
		t.Errorf("expected false -> 0, got %d", b.Columns[0].Int64Data[2])
	}
}

func TestColumnarReaderBoolToFloat(t *testing.T) {
	// First row: float, second: bool -> promotes to Float64
	data := `{"v":3.14}
{"v":true}
{"v":false}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeFloat64 {
		t.Fatalf("expected TypeFloat64, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Columns[0].Float64Data[1] != 1 {
		t.Errorf("expected true -> 1.0, got %f", b.Columns[0].Float64Data[1])
	}
	if b.Columns[0].Float64Data[2] != 0 {
		t.Errorf("expected false -> 0.0, got %f", b.Columns[0].Float64Data[2])
	}
}

func TestColumnarReaderBoolToString(t *testing.T) {
	// First row: string, second: bool -> promotes to String
	data := `{"v":"hello"}
{"v":true}
{"v":false}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeString {
		t.Fatalf("expected TypeString, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Columns[0].BytesData.StringValue(1) != "true" {
		t.Errorf("expected 'true', got %q", b.Columns[0].BytesData.StringValue(1))
	}
	if b.Columns[0].BytesData.StringValue(2) != "false" {
		t.Errorf("expected 'false', got %q", b.Columns[0].BytesData.StringValue(2))
	}
}

func TestColumnarReaderNumToString(t *testing.T) {
	// When schema inferred as string but value is number
	data := `{"v":"hello"}
{"v":42}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeString {
		t.Fatalf("expected TypeString, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	// Number should be written as string bytes to string column
	val := b.Columns[0].BytesData.StringValue(1)
	if val != "42" {
		t.Errorf("expected '42', got %q", val)
	}
}

func TestColumnarReaderFloatNumber(t *testing.T) {
	data := `{"x":1.5,"y":2.5}
{"x":3.0,"y":4.0}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Columns[0].Float64Data[0] != 1.5 {
		t.Errorf("expected 1.5, got %f", b.Columns[0].Float64Data[0])
	}
}

func TestColumnarReaderNestedObjectScalarSchema(t *testing.T) {
	// When schema says scalar but JSON has nested object -> store as JSON string
	data := `{"v":"plain"}
{"v":{"nested":true}}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
}

func TestColumnarReaderMapType(t *testing.T) {
	// Test MAP type nested value inference
	data := `{"data":{"key1":"val1","key2":"val2"}}
{"data":{"key3":"val3"}}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeRow {
		t.Fatalf("expected TypeRow for nested object, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
}

func TestColumnarReaderNestedArrayOfInts(t *testing.T) {
	data := `{"nums":[1,2,3]}
{"nums":[4,5]}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeArray {
		t.Fatalf("expected TypeArray, got %v", schema[0].Type)
	}
	if schema[0].ElementType == nil || schema[0].ElementType.Type != parquet.TypeInt64 {
		t.Fatalf("expected ARRAY(INT64)")
	}
}

func TestColumnarReaderEmptyJSONArray(t *testing.T) {
	data := `[]`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Schema()) != 0 {
		t.Errorf("expected empty schema for empty array")
	}
}

func TestColumnarReaderWhitespaceOnly(t *testing.T) {
	data := `   `
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Schema()) != 0 {
		t.Errorf("expected empty schema for whitespace")
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b != nil {
		t.Error("expected nil batch")
	}
}

func TestColumnarReaderTimestampFormats(t *testing.T) {
	data := `{"ts":"2024-01-15T10:30:00"}
{"ts":"2024-01-16 11:00:00"}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeTimestamp {
		t.Errorf("expected TypeTimestamp, got %v", schema[0].Type)
	}
}

func TestColumnarReaderDateOnlyTimestamp(t *testing.T) {
	data := `{"d":"2024-01-15"}
{"d":"2024-06-30"}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeTimestamp {
		t.Errorf("expected TypeTimestamp for date-only, got %v", schema[0].Type)
	}
}

func TestColumnarReaderIPv4Column(t *testing.T) {
	// When IP-like strings are detected, schema should infer TypeIPv4
	data := `{"ip":"192.168.1.1"}
{"ip":"10.0.0.1"}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeIPv4 {
		t.Fatalf("expected TypeIPv4, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
	// The IP should be stored as int64 from 4 bytes big-endian
	if b.Columns[0].Int64Data[0] == 0 && b.Columns[0].Int64Data[1] == 0 {
		t.Error("expected non-zero IP data")
	}
}

func TestColumnarReaderEscapedStringValues(t *testing.T) {
	// Exercise the hasEscape path in writeStringValue
	data := `{"msg":"hello"}
{"msg":"world \"quoted\""}
{"msg":"path\\to\\file"}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 3 {
		t.Fatal("expected 3 rows")
	}
	// First row is plain string
	if b.Columns[0].BytesData.StringValue(0) != "hello" {
		t.Errorf("row 0: expected 'hello', got %q", b.Columns[0].BytesData.StringValue(0))
	}
	// Second row has escape sequences that should be unescaped
	val := b.Columns[0].BytesData.StringValue(1)
	if val != `world "quoted"` {
		t.Errorf("row 1: expected unescaped string, got %q", val)
	}
}

func TestColumnarReaderNullColumn(t *testing.T) {
	// Test null handling across various types
	data := `{"a":1,"b":"hello","c":true}
{"a":null,"b":null,"c":null}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
	// Row 0 should be valid, row 1 should be null for all columns
	for i := 0; i < len(b.Columns); i++ {
		if b.Columns[i].Nulls.IsNull(0) {
			t.Errorf("col %d row 0: expected valid", i)
		}
		if !b.Columns[i].Nulls.IsNull(1) {
			t.Errorf("col %d row 1: expected null", i)
		}
	}
}

func TestColumnarReaderMissingColumns(t *testing.T) {
	// Test that missing columns in a row are set to null
	data := `{"a":1,"b":"hello"}
{"a":2}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	// Column "b" should be null in row 1
	if !b.Columns[1].Nulls.IsNull(1) {
		t.Error("expected null for missing column 'b' in row 1")
	}
}

func TestColumnarReaderJSONArray(t *testing.T) {
	// Test JSON array format (not JSONL)
	data := `[{"a":1},{"a":2},{"a":3}]`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", b.Len)
	}
}

func TestColumnarReaderBoolColumn(t *testing.T) {
	// Direct bool columns (schema is TypeBool)
	data := `{"flag":true}
{"flag":false}
{"flag":true}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeBool {
		t.Fatalf("expected TypeBool, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !b.Columns[0].BoolData[0] {
		t.Error("row 0: expected true")
	}
	if b.Columns[0].BoolData[1] {
		t.Error("row 1: expected false")
	}
}

func TestColumnarReaderNestedArrayParsed(t *testing.T) {
	// Test nested array values are properly decoded
	data := `{"items":[1,2,3]}
{"items":[4]}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
	// The nested array values should be stored as decoded values
	val := b.Columns[0].GetValue(0)
	if val == nil {
		t.Error("expected non-nil array value")
	}
}

func TestColumnarReaderNestedObjectParsed(t *testing.T) {
	// Nested object detected during schema inference should be decoded
	data := `{"obj":{"x":1,"y":2}}
{"obj":{"x":3,"y":4}}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
}

func TestColumnarReaderMultiBatch(t *testing.T) {
	// Generate enough rows to span multiple batches (defaultBatchSize=2048)
	var sb strings.Builder
	for i := 0; i < 2050; i++ {
		sb.WriteString(fmt.Sprintf(`{"id":%d}`, i))
		sb.WriteByte('\n')
	}
	r, err := NewColumnarReader([]byte(sb.String()))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	batchCount := 0
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		total += b.Len
		batchCount++
	}
	if total != 2050 {
		t.Fatalf("expected 2050 rows, got %d", total)
	}
	if batchCount < 2 {
		t.Fatalf("expected at least 2 batches, got %d", batchCount)
	}
}
