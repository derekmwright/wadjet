package json

import (
	"bytes"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestInvalidJSON(t *testing.T) {
	_, err := NewReaderFromBytes([]byte("{invalid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestInvalidJSONArray(t *testing.T) {
	_, err := NewReaderFromBytes([]byte("[{\"a\":1}, invalid]"))
	if err == nil {
		t.Error("expected error for invalid JSON array element")
	}
}

func TestInvalidJSONL(t *testing.T) {
	_, err := NewReaderFromBytes([]byte("{\"a\":1}\n{invalid"))
	if err == nil {
		t.Error("expected error for invalid JSONL")
	}
}

func TestDetectTypeDefault(t *testing.T) {
	// Unknown type should default to TypeString
	typ := detectType([]int{1, 2, 3})
	if typ != parquet.TypeString {
		t.Errorf("expected TypeString for unknown type, got %v", typ)
	}
}

func TestDetectTypeBool(t *testing.T) {
	if detectType(true) != parquet.TypeBool {
		t.Error("expected TypeBool for true")
	}
	if detectType(false) != parquet.TypeBool {
		t.Error("expected TypeBool for false")
	}
}

func TestDetectTypeInt64(t *testing.T) {
	if detectType(int64(42)) != parquet.TypeInt64 {
		t.Error("expected TypeInt64")
	}
}

func TestDetectTypeFloat64(t *testing.T) {
	if detectType(float64(3.14)) != parquet.TypeFloat64 {
		t.Error("expected TypeFloat64")
	}
}

func TestDetectStringTypeIPv4(t *testing.T) {
	if detectStringType("192.168.1.1") != parquet.TypeIPv4 {
		t.Error("expected TypeIPv4 for valid IP")
	}
}

func TestDetectStringTypeNotIPv4(t *testing.T) {
	// Looks like IP but invalid
	if detectStringType("999.999.999.999") != parquet.TypeString {
		t.Error("expected TypeString for invalid IP")
	}
}

func TestDetectStringTypeTimestampFormats(t *testing.T) {
	tests := []struct {
		input string
		want  parquet.TypeID
	}{
		{"2024-01-15T10:30:00Z", parquet.TypeTimestamp},
		{"2024-01-15T10:30:00.123456789Z", parquet.TypeTimestamp},
		{"2024-01-15T10:30:00", parquet.TypeTimestamp},
		{"2024-01-15 10:30:00", parquet.TypeTimestamp},
		{"2024-01-15", parquet.TypeTimestamp},
		{"not a timestamp", parquet.TypeString},
	}
	for _, tt := range tests {
		got := detectStringType(tt.input)
		if got != tt.want {
			t.Errorf("detectStringType(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestPromoteTypeSame(t *testing.T) {
	if promoteType(parquet.TypeInt64, parquet.TypeInt64) != parquet.TypeInt64 {
		t.Error("same types should stay the same")
	}
}

func TestPromoteTypeNumeric(t *testing.T) {
	// int64 + float64 -> float64
	if promoteType(parquet.TypeInt64, parquet.TypeFloat64) != parquet.TypeFloat64 {
		t.Error("int64 + float64 should promote to float64")
	}
	// float64 + int64 -> float64
	if promoteType(parquet.TypeFloat64, parquet.TypeInt64) != parquet.TypeFloat64 {
		t.Error("float64 + int64 should promote to float64")
	}
	// int32 + int64 -> int64
	if promoteType(parquet.TypeInt32, parquet.TypeInt64) != parquet.TypeInt64 {
		t.Error("int32 + int64 should promote to int64")
	}
	// int32 + int32 -> int32
	if promoteType(parquet.TypeInt32, parquet.TypeInt32) != parquet.TypeInt32 {
		t.Error("int32 + int32 should stay int32")
	}
}

func TestPromoteTypeFloatFloat(t *testing.T) {
	// float32 + int64 -> float64 (via isFloat path)
	if promoteType(parquet.TypeFloat32, parquet.TypeInt64) != parquet.TypeFloat64 {
		t.Error("float32 + int64 should promote to float64")
	}
	// int64 + float32 -> float64
	if promoteType(parquet.TypeInt64, parquet.TypeFloat32) != parquet.TypeFloat64 {
		t.Error("int64 + float32 should promote to float64")
	}
}

func TestPromoteTypeBoolNumeric(t *testing.T) {
	if promoteType(parquet.TypeBool, parquet.TypeInt64) != parquet.TypeInt64 {
		t.Error("bool + int64 should promote to int64")
	}
	if promoteType(parquet.TypeBool, parquet.TypeFloat64) != parquet.TypeFloat64 {
		t.Error("bool + float64 should promote to float64")
	}
	if promoteType(parquet.TypeInt64, parquet.TypeBool) != parquet.TypeInt64 {
		t.Error("int64 + bool should promote to int64")
	}
}

func TestPromoteTypeStringLikeDisagree(t *testing.T) {
	// IPv4 + Timestamp -> String (string-like disagree)
	if promoteType(parquet.TypeIPv4, parquet.TypeTimestamp) != parquet.TypeString {
		t.Error("IPv4 + Timestamp should fall back to String")
	}
	// IPv4 + String -> String
	if promoteType(parquet.TypeIPv4, parquet.TypeString) != parquet.TypeString {
		t.Error("IPv4 + String should fall back to String")
	}
}

func TestPromoteTypeMixed(t *testing.T) {
	// bool + string -> string (anything else)
	if promoteType(parquet.TypeBool, parquet.TypeString) != parquet.TypeString {
		t.Error("bool + string should fall back to string")
	}
}

func TestIsFloat(t *testing.T) {
	if !isFloat(parquet.TypeFloat32) {
		t.Error("Float32 should be float")
	}
	if !isFloat(parquet.TypeFloat64) {
		t.Error("Float64 should be float")
	}
	if isFloat(parquet.TypeInt64) {
		t.Error("Int64 should not be float")
	}
}

func TestIsStringLike(t *testing.T) {
	if !isStringLike(parquet.TypeString) {
		t.Error("String should be string-like")
	}
	if !isStringLike(parquet.TypeIPv4) {
		t.Error("IPv4 should be string-like")
	}
	if !isStringLike(parquet.TypeTimestamp) {
		t.Error("Timestamp should be string-like")
	}
	if isStringLike(parquet.TypeInt64) {
		t.Error("Int64 should not be string-like")
	}
}

func TestCoerceValueNil(t *testing.T) {
	if coerceValue(nil, parquet.TypeFloat64) != nil {
		t.Error("nil should stay nil")
	}
}

func TestCoerceValueToFloat64(t *testing.T) {
	// int64 -> float64
	result := coerceValue(int64(42), parquet.TypeFloat64)
	if result != float64(42) {
		t.Errorf("expected 42.0, got %v", result)
	}
	// float64 -> float64 (identity)
	result2 := coerceValue(float64(3.14), parquet.TypeFloat64)
	if result2 != 3.14 {
		t.Errorf("expected 3.14, got %v", result2)
	}
	// bool true -> float64(1)
	result3 := coerceValue(true, parquet.TypeFloat64)
	if result3 != float64(1) {
		t.Errorf("expected 1.0, got %v", result3)
	}
	// bool false -> float64(0)
	result4 := coerceValue(false, parquet.TypeFloat64)
	if result4 != float64(0) {
		t.Errorf("expected 0.0, got %v", result4)
	}
	// string -> pass through (non-numeric string for float64 target)
	result5 := coerceValue("hello", parquet.TypeFloat64)
	if result5 != "hello" {
		t.Errorf("expected hello, got %v", result5)
	}
}

func TestCoerceValueToInt64(t *testing.T) {
	// int64 -> int64 (identity)
	result := coerceValue(int64(42), parquet.TypeInt64)
	if result != int64(42) {
		t.Errorf("expected 42, got %v", result)
	}
	// float64 whole number -> int64
	result2 := coerceValue(float64(100.0), parquet.TypeInt64)
	if result2 != int64(100) {
		t.Errorf("expected 100, got %v", result2)
	}
	// float64 with fraction -> float64 (no truncation)
	result3 := coerceValue(float64(3.14), parquet.TypeInt64)
	if result3 != float64(3.14) {
		t.Errorf("expected 3.14 (no truncation), got %v", result3)
	}
	// bool true -> int64(1)
	result4 := coerceValue(true, parquet.TypeInt64)
	if result4 != int64(1) {
		t.Errorf("expected 1, got %v", result4)
	}
	// bool false -> int64(0)
	result5 := coerceValue(false, parquet.TypeInt64)
	if result5 != int64(0) {
		t.Errorf("expected 0, got %v", result5)
	}
}

func TestCoerceValueToString(t *testing.T) {
	// string -> string
	result := coerceValue("hello", parquet.TypeString)
	if result != "hello" {
		t.Errorf("expected hello, got %v", result)
	}
	// int64 -> string
	result2 := coerceValue(int64(42), parquet.TypeString)
	if result2 != "42" {
		t.Errorf("expected '42', got %v", result2)
	}
	// bool -> string
	result3 := coerceValue(true, parquet.TypeString)
	if result3 != "true" {
		t.Errorf("expected 'true', got %v", result3)
	}
}

func TestCoerceValuePassthrough(t *testing.T) {
	// Unknown target type -> pass through
	result := coerceValue(int64(42), parquet.TypeBool)
	if result != int64(42) {
		t.Errorf("expected passthrough, got %v", result)
	}
}

func TestNewReaderFromStreamError(t *testing.T) {
	// Test with reader that returns an error
	errReader := &errorReader{}
	_, err := NewReaderFromStream(errReader)
	if err == nil {
		t.Error("expected error from failing reader")
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, bytes.ErrTooLarge
}

func TestNewReaderFromBytesWithCoercionMixedTypes(t *testing.T) {
	// First row int, second row float -> coerce all to float64
	input := `{"value":42}
{"value":3.14}
`
	r, err := NewReaderFromBytesWithCoercion([]byte(input))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeFloat64 {
		t.Errorf("expected Float64 after coercion, got %v", schema[0].Type)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
}

func TestConvertNumbersScientific(t *testing.T) {
	// Scientific notation should become float64
	input := `{"x":1e10}`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeFloat64 {
		t.Errorf("expected Float64 for scientific notation, got %v", schema[0].Type)
	}
}

func TestConvertNumbersDecimal(t *testing.T) {
	input := `{"x":3.14}`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeFloat64 {
		t.Errorf("expected Float64 for decimal, got %v", schema[0].Type)
	}
}

func TestInferSchemaEmpty(t *testing.T) {
	schema := inferSchema(nil, 100)
	if schema != nil {
		t.Error("expected nil schema for nil rows")
	}
}

func TestInferSchemaSampleSizeLargerThanRows(t *testing.T) {
	rows := []map[string]any{
		{"a": int64(1)},
		{"a": int64(2)},
	}
	schema := inferSchema(rows, 1000)
	if len(schema) != 1 {
		t.Fatalf("expected 1 column, got %d", len(schema))
	}
	if schema[0].Type != parquet.TypeInt64 {
		t.Errorf("expected Int64, got %v", schema[0].Type)
	}
}

func TestColumnarReaderFromStream(t *testing.T) {
	input := `{"a":1,"b":"hello"}
{"a":2,"b":"world"}
`
	r, err := NewColumnarReaderFromStream(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if len(schema) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(schema))
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
}

func TestColumnarReaderFromStreamError(t *testing.T) {
	_, err := NewColumnarReaderFromStream(&errorReader{})
	if err == nil {
		t.Error("expected error from failing reader")
	}
}

func TestColumnarReaderEmptyArray(t *testing.T) {
	r, err := NewColumnarReader([]byte("[]"))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Schema()) != 0 {
		t.Errorf("expected empty schema, got %d columns", len(r.Schema()))
	}
}

func TestColumnarReaderEscapedStrings(t *testing.T) {
	data := `{"msg":"hello \"world\""}
{"msg":"tab\there"}
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
	// Check that escaped strings are properly unescaped
	msg := b.Columns[0].BytesData.StringValue(0)
	if msg != `hello "world"` {
		t.Errorf("expected unescaped string, got %q", msg)
	}
}

func TestColumnarReaderTimestamp(t *testing.T) {
	data := `[{"ts":"2024-01-15T10:30:00Z"},{"ts":"2024-01-16T11:00:00Z"}]`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeTimestamp {
		t.Errorf("expected TypeTimestamp, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
	// Timestamp should be stored as int64 (unix microseconds)
	if b.Columns[0].Int64Data[0] == 0 {
		t.Error("expected non-zero timestamp value")
	}
}

func TestColumnarReaderBoolValues(t *testing.T) {
	data := `{"active":true,"deleted":false}
{"active":false,"deleted":true}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Columns[0].BoolData[0] != true {
		t.Error("expected active[0] = true")
	}
	if b.Columns[0].BoolData[1] != false {
		t.Error("expected active[1] = false")
	}
	if b.Columns[1].BoolData[0] != false {
		t.Error("expected deleted[0] = false")
	}
	if b.Columns[1].BoolData[1] != true {
		t.Error("expected deleted[1] = true")
	}
}

func TestColumnarReaderExtraColumns(t *testing.T) {
	// Row with extra columns not in schema (from first row inference)
	data := `{"a":1}
{"a":2,"b":"extra"}
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

func TestColumnarReaderIPv4(t *testing.T) {
	data := `{"ip":"10.0.0.1"}
{"ip":"192.168.1.1"}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeIPv4 {
		t.Errorf("expected TypeIPv4, got %v", schema[0].Type)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 2 {
		t.Fatal("expected 2 rows")
	}
}
