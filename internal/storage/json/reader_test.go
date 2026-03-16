package json

import (
	"bytes"
	"strings"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestJSONLParsing(t *testing.T) {
	input := `{"name":"alice","age":30,"active":true}
{"name":"bob","age":25,"active":false}
{"name":"carol","age":40,"active":true}
`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(schema))
	}

	// Check column types.
	colMap := make(map[string]parquet.TypeID)
	for _, c := range schema {
		colMap[c.Name] = c.Type
	}
	if colMap["name"] != parquet.TypeString {
		t.Errorf("expected name=STRING, got %v", colMap["name"])
	}
	if colMap["age"] != parquet.TypeInt64 {
		t.Errorf("expected age=INT64, got %v", colMap["age"])
	}
	if colMap["active"] != parquet.TypeBool {
		t.Errorf("expected active=BOOL, got %v", colMap["active"])
	}

	// Verify batch contents.
	b, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b == nil {
		t.Fatal("expected a batch, got nil")
	}
	if b.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", b.Len)
	}

	// Second call should return nil (exhausted).
	b2, err := r.Next()
	if err != nil {
		t.Fatalf("Next (second call): %v", err)
	}
	if b2 != nil {
		t.Fatal("expected nil batch on exhaustion")
	}
}

func TestJSONArrayParsing(t *testing.T) {
	input := `[
		{"host":"10.0.0.1","port":443},
		{"host":"10.0.0.2","port":80},
		{"host":"10.0.0.3","port":8080}
	]`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(schema))
	}

	colMap := make(map[string]parquet.TypeID)
	for _, c := range schema {
		colMap[c.Name] = c.Type
	}
	if colMap["host"] != parquet.TypeIPv4 {
		t.Errorf("expected host=IPV4, got %v", colMap["host"])
	}
	if colMap["port"] != parquet.TypeInt64 {
		t.Errorf("expected port=INT64, got %v", colMap["port"])
	}

	b, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b == nil || b.Len != 3 {
		t.Fatalf("expected batch with 3 rows, got %v", b)
	}
}

func TestSchemaInferenceMixedTypes(t *testing.T) {
	// First row has int, second has float -> should promote to float64.
	input := `{"value":42}
{"value":3.14}
`
	r, err := NewReaderFromBytesWithCoercion([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytesWithCoercion: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 1 {
		t.Fatalf("expected 1 column, got %d", len(schema))
	}
	if schema[0].Type != parquet.TypeFloat64 {
		t.Errorf("expected value=FLOAT64, got %v", schema[0].Type)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b == nil || b.Len != 2 {
		t.Fatalf("expected batch with 2 rows, got %v", b)
	}
}

func TestSchemaInferenceTimestamp(t *testing.T) {
	input := `[{"ts":"2024-01-15T10:30:00Z"},{"ts":"2024-01-16T11:00:00Z"}]`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 1 {
		t.Fatalf("expected 1 column, got %d", len(schema))
	}
	if schema[0].Type != parquet.TypeTimestamp {
		t.Errorf("expected ts=TIMESTAMP, got %v", schema[0].Type)
	}
}

func TestSchemaInferenceNullable(t *testing.T) {
	// Null values should not affect type inference, column stays nullable.
	input := `{"x":1,"y":null}
{"x":null,"y":"hello"}
`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(schema))
	}
	colMap := make(map[string]parquet.TypeID)
	for _, c := range schema {
		colMap[c.Name] = c.Type
		if !c.Nullable {
			t.Errorf("expected column %q to be nullable", c.Name)
		}
	}
	if colMap["x"] != parquet.TypeInt64 {
		t.Errorf("expected x=INT64, got %v", colMap["x"])
	}
	if colMap["y"] != parquet.TypeString {
		t.Errorf("expected y=STRING, got %v", colMap["y"])
	}
}

func TestSchemaInferenceAllNull(t *testing.T) {
	// Column with all nulls should default to STRING.
	input := `{"a":null}
{"a":null}
`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 1 {
		t.Fatalf("expected 1 column, got %d", len(schema))
	}
	if schema[0].Type != parquet.TypeString {
		t.Errorf("expected a=STRING for all-null column, got %v", schema[0].Type)
	}
}

func TestEmptyInput(t *testing.T) {
	for _, input := range []string{"", "   ", "\n\n"} {
		r, err := NewReaderFromBytes([]byte(input))
		if err != nil {
			t.Fatalf("NewReaderFromBytes(%q): %v", input, err)
		}
		if len(r.Schema()) != 0 {
			t.Errorf("expected empty schema for %q, got %d columns", input, len(r.Schema()))
		}
		b, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b != nil {
			t.Errorf("expected nil batch for empty input %q", input)
		}
	}
}

func TestEmptyArray(t *testing.T) {
	r, err := NewReaderFromBytes([]byte("[]"))
	if err != nil {
		t.Fatalf("NewReaderFromBytes([]): %v", err)
	}
	if len(r.Schema()) != 0 {
		t.Errorf("expected empty schema for [], got %d columns", len(r.Schema()))
	}
	b, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b != nil {
		t.Error("expected nil batch for empty array")
	}
}

func TestBatchIteration(t *testing.T) {
	// Build input with more rows than defaultBatchSize to test multi-batch.
	var sb strings.Builder
	n := defaultBatchSize + 500
	for i := 0; i < n; i++ {
		sb.WriteString(`{"i":`)
		sb.WriteString(strings.Repeat("", 0)) // no-op
		// Write the integer directly.
		sb.WriteString(intToStr(i))
		sb.WriteString("}\n")
	}
	r, err := NewReaderFromBytes([]byte(sb.String()))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}

	totalRows := 0
	batchCount := 0
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		totalRows += b.Len
		batchCount++
	}
	if totalRows != n {
		t.Errorf("expected %d total rows, got %d", n, totalRows)
	}
	if batchCount != 2 {
		t.Errorf("expected 2 batches, got %d", batchCount)
	}
}

func TestNewReaderFromStream(t *testing.T) {
	input := `{"key":"value","num":42}
{"key":"other","num":99}
`
	r, err := NewReaderFromStream(bytes.NewReader([]byte(input)))
	if err != nil {
		t.Fatalf("NewReaderFromStream: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(schema))
	}
	b, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b == nil || b.Len != 2 {
		t.Fatalf("expected batch with 2 rows, got %v", b)
	}
}

func TestPromoteTypeIntBool(t *testing.T) {
	// Bool + int64 should promote to int64.
	input := `{"v":true}
{"v":7}
`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	schema := r.Schema()
	if len(schema) != 1 {
		t.Fatalf("expected 1 column, got %d", len(schema))
	}
	if schema[0].Type != parquet.TypeInt64 {
		t.Errorf("expected v=INT64 after bool+int promotion, got %v", schema[0].Type)
	}
}

func TestPromoteTypeMismatchToString(t *testing.T) {
	// Completely incompatible types should fall back to string.
	input := `{"v":42}
{"v":"hello"}
`
	r, err := NewReaderFromBytes([]byte(input))
	if err != nil {
		t.Fatalf("NewReaderFromBytes: %v", err)
	}
	schema := r.Schema()
	if schema[0].Type != parquet.TypeString {
		t.Errorf("expected v=STRING for int+string mismatch, got %v", schema[0].Type)
	}
}

// intToStr is a simple int-to-string helper to avoid importing strconv in test.
func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
