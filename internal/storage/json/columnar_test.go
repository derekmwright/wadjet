package json

import (
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestColumnarReader_JSONL(t *testing.T) {
	data := `{"name":"alice","age":30,"active":true}
{"name":"bob","age":25,"active":false}
{"name":"carol","age":35,"active":true}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Schema()) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(r.Schema()))
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected batch")
	}
	if b.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", b.Len)
	}

	// Verify typed data access (no ToRows needed)
	nameVec := b.Columns[0]
	if nameVec.BytesData.StringValue(0) != "alice" {
		t.Errorf("expected alice, got %s", nameVec.BytesData.StringValue(0))
	}
	if nameVec.BytesData.StringValue(1) != "bob" {
		t.Errorf("expected bob, got %s", nameVec.BytesData.StringValue(1))
	}

	ageVec := b.Columns[1]
	if ageVec.Int64Data[0] != 30 {
		t.Errorf("expected 30, got %d", ageVec.Int64Data[0])
	}
	if ageVec.Int64Data[1] != 25 {
		t.Errorf("expected 25, got %d", ageVec.Int64Data[1])
	}

	activeVec := b.Columns[2]
	if activeVec.BoolData[0] != true {
		t.Error("expected active=true for alice")
	}
	if activeVec.BoolData[1] != false {
		t.Error("expected active=false for bob")
	}

	// Next should be nil
	b2, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b2 != nil {
		t.Error("expected nil after exhaustion")
	}
}

func TestColumnarReader_JSONArray(t *testing.T) {
	data := `[{"id":1,"value":10.5},{"id":2,"value":20.3}]`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", b.Len)
	}

	idVec := b.Columns[0]
	if idVec.Int64Data[0] != 1 || idVec.Int64Data[1] != 2 {
		t.Errorf("id values: got %d, %d", idVec.Int64Data[0], idVec.Int64Data[1])
	}

	valVec := b.Columns[1]
	if valVec.Float64Data[0] != 10.5 || valVec.Float64Data[1] != 20.3 {
		t.Errorf("value data: got %f, %f", valVec.Float64Data[0], valVec.Float64Data[1])
	}
}

func TestColumnarReader_NullValues(t *testing.T) {
	data := `{"a":1,"b":"hello"}
{"a":null,"b":"world"}
{"a":3,"b":null}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}

	aVec := b.Columns[0]
	if aVec.Nulls.IsNull(0) {
		t.Error("a[0] should not be null")
	}
	if !aVec.Nulls.IsNull(1) {
		t.Error("a[1] should be null")
	}
	if aVec.Int64Data[0] != 1 {
		t.Errorf("a[0]: expected 1, got %d", aVec.Int64Data[0])
	}

	bVec := b.Columns[1]
	if bVec.BytesData.StringValue(0) != "hello" {
		t.Errorf("b[0]: expected hello, got %s", bVec.BytesData.StringValue(0))
	}
	if !bVec.Nulls.IsNull(2) {
		t.Error("b[2] should be null")
	}
}

func TestColumnarReader_MissingColumns(t *testing.T) {
	// Row 2 is missing "b"
	data := `{"a":1,"b":"x"}
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

	bVec := b.Columns[1]
	if bVec.BytesData.StringValue(0) != "x" {
		t.Errorf("b[0]: expected x, got %s", bVec.BytesData.StringValue(0))
	}
	if !bVec.Nulls.IsNull(1) {
		t.Error("b[1] should be null (missing key)")
	}
}

func TestColumnarReader_MultiBatch(t *testing.T) {
	// Generate more than defaultBatchSize rows
	var sb strings.Builder
	n := 2048 + 500 // 1 full batch + partial
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"id":%d,"val":%d.5}`, i, i)
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

	if total != n {
		t.Errorf("expected %d total rows, got %d", n, total)
	}
	if batchCount != 2 {
		t.Errorf("expected 2 batches, got %d", batchCount)
	}
}

func TestColumnarReader_Empty(t *testing.T) {
	r, err := NewColumnarReader([]byte(""))
	if err != nil {
		t.Fatal(err)
	}
	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b != nil {
		t.Error("expected nil for empty input")
	}
}

func TestColumnarReader_SchemaInference(t *testing.T) {
	data := `{"ip":"192.168.1.1","count":42,"ratio":3.14}
{"ip":"10.0.0.1","count":7,"ratio":2.71}
`
	r, err := NewColumnarReader([]byte(data))
	if err != nil {
		t.Fatal(err)
	}

	schema := r.Schema()
	if schema[0].Type != parquet.TypeIPv4 {
		t.Errorf("expected TypeIPv4 for ip, got %v", schema[0].Type)
	}
	if schema[1].Type != parquet.TypeInt64 {
		t.Errorf("expected TypeInt64 for count, got %v", schema[1].Type)
	}
	if schema[2].Type != parquet.TypeFloat64 {
		t.Errorf("expected TypeFloat64 for ratio, got %v", schema[2].Type)
	}
}

// Benchmarks comparing row-oriented Reader vs ColumnarReader

func benchData(n int) []byte {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"id":%d,"name":"user_%d","score":%d.%d,"active":%t}`, i, i, i*10, i%10, i%2 == 0)
		sb.WriteByte('\n')
	}
	return []byte(sb.String())
}

func BenchmarkReader_RowOriented(b *testing.B) {
	data := benchData(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, err := NewReaderFromBytes(data)
		if err != nil {
			b.Fatal(err)
		}
		for {
			batch, err := r.Next()
			if err != nil {
				b.Fatal(err)
			}
			if batch == nil {
				break
			}
		}
	}
}

func BenchmarkReader_Columnar(b *testing.B) {
	data := benchData(10000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, err := NewColumnarReader(data)
		if err != nil {
			b.Fatal(err)
		}
		for {
			batch, err := r.Next()
			if err != nil {
				b.Fatal(err)
			}
			if batch == nil {
				break
			}
		}
	}
}
