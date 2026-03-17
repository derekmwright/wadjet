package csv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestReader_Basic(t *testing.T) {
	data := `name,age,score
alice,30,95.5
bob,25,87.2
carol,35,92.1
`
	r, err := NewReader([]byte(data), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	schema := r.Schema()
	if len(schema) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(schema))
	}
	if schema[0].Name != "name" || schema[0].Type != parquet.TypeString {
		t.Errorf("col0: got %s/%v", schema[0].Name, schema[0].Type)
	}
	if schema[1].Name != "age" || schema[1].Type != parquet.TypeInt64 {
		t.Errorf("col1: got %s/%v", schema[1].Name, schema[1].Type)
	}
	if schema[2].Name != "score" || schema[2].Type != parquet.TypeFloat64 {
		t.Errorf("col2: got %s/%v", schema[2].Name, schema[2].Type)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 3 {
		t.Fatalf("expected 3 rows, got %v", b)
	}

	// Check typed data
	if b.Columns[0].BytesData.StringValue(0) != "alice" {
		t.Errorf("expected alice, got %s", b.Columns[0].BytesData.StringValue(0))
	}
	if b.Columns[1].Int64Data[0] != 30 {
		t.Errorf("expected 30, got %d", b.Columns[1].Int64Data[0])
	}
	if b.Columns[2].Float64Data[1] != 87.2 {
		t.Errorf("expected 87.2, got %f", b.Columns[2].Float64Data[1])
	}

	// Exhaustion
	b2, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b2 != nil {
		t.Error("expected nil after exhaustion")
	}
}

func TestReader_NoHeader(t *testing.T) {
	data := `alice,30
bob,25
`
	cfg := ReaderConfig{Delimiter: ',', HasHeader: false}
	r, err := NewReader([]byte(data), cfg)
	if err != nil {
		t.Fatal(err)
	}

	schema := r.Schema()
	if schema[0].Name != "col0" || schema[1].Name != "col1" {
		t.Errorf("expected col0/col1, got %s/%s", schema[0].Name, schema[1].Name)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", b.Len)
	}
}

func TestReader_TSV(t *testing.T) {
	data := "name\tage\nalice\t30\nbob\t25\n"
	cfg := ReaderConfig{Delimiter: '\t', HasHeader: true}
	r, err := NewReader([]byte(data), cfg)
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
	if b.Columns[0].BytesData.StringValue(0) != "alice" {
		t.Errorf("expected alice, got %s", b.Columns[0].BytesData.StringValue(0))
	}
}

func TestReader_TypeInference(t *testing.T) {
	data := `ip,count,ratio,active
192.168.1.1,42,3.14,true
10.0.0.1,7,2.71,false
`
	r, err := NewReader([]byte(data), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	schema := r.Schema()
	if schema[0].Type != parquet.TypeIPv4 {
		t.Errorf("expected TypeIPv4, got %v", schema[0].Type)
	}
	if schema[1].Type != parquet.TypeInt64 {
		t.Errorf("expected TypeInt64, got %v", schema[1].Type)
	}
	if schema[2].Type != parquet.TypeFloat64 {
		t.Errorf("expected TypeFloat64, got %v", schema[2].Type)
	}
	if schema[3].Type != parquet.TypeBool {
		t.Errorf("expected TypeBool, got %v", schema[3].Type)
	}
}

func TestReader_MissingValues(t *testing.T) {
	data := `a,b
1,hello
,world
3,
`
	r, err := NewReader([]byte(data), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}

	// Row 1 (index 1): a is empty → null
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("a[1] should be null")
	}
	// Row 2 (index 2): b is empty → null
	if !b.Columns[1].Nulls.IsNull(2) {
		t.Error("b[2] should be null")
	}
}

func TestReader_Empty(t *testing.T) {
	r, err := NewReader([]byte(""), DefaultConfig())
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

func TestReader_Pipe(t *testing.T) {
	data := `name|age
alice|30
bob|25
`
	cfg := ReaderConfig{Delimiter: '|', HasHeader: true}
	r, err := NewReader([]byte(data), cfg)
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
	if b.Columns[1].Int64Data[0] != 30 {
		t.Errorf("expected 30, got %d", b.Columns[1].Int64Data[0])
	}
}

// --- Streaming reader tests ---

func TestStreamReader_Basic(t *testing.T) {
	data := "name,age,score\nalice,30,95.5\nbob,25,87.2\ncarol,35,92.1\n"
	r, err := NewStreamReader(strings.NewReader(data), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	schema := r.Schema()
	if len(schema) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(schema))
	}
	if schema[0].Name != "name" || schema[0].Type != parquet.TypeString {
		t.Errorf("col0: got %s/%v", schema[0].Name, schema[0].Type)
	}
	if schema[1].Name != "age" || schema[1].Type != parquet.TypeInt64 {
		t.Errorf("col1: got %s/%v", schema[1].Name, schema[1].Type)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Len != 3 {
		t.Fatalf("expected 3 rows, got %v", b)
	}
	if b.Columns[0].BytesData.StringValue(0) != "alice" {
		t.Errorf("expected alice, got %s", b.Columns[0].BytesData.StringValue(0))
	}

	// Exhaustion
	b2, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b2 != nil {
		t.Error("expected nil after exhaustion")
	}
}

func TestStreamReader_NoHeader(t *testing.T) {
	data := "alice,30\nbob,25\n"
	cfg := ReaderConfig{Delimiter: ',', HasHeader: false}
	r, err := NewStreamReader(strings.NewReader(data), cfg)
	if err != nil {
		t.Fatal(err)
	}

	schema := r.Schema()
	if schema[0].Name != "col0" || schema[1].Name != "col1" {
		t.Errorf("expected col0/col1, got %s/%s", schema[0].Name, schema[1].Name)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", b.Len)
	}
}

func TestStreamReader_LargerThanBatch(t *testing.T) {
	// Generate more rows than the default batch size (2048) to test multi-batch streaming
	var sb strings.Builder
	sb.WriteString("id,value\n")
	const numRows = 3000
	for i := 0; i < numRows; i++ {
		fmt.Fprintf(&sb, "%d,%d\n", i, i*10)
	}

	r, err := NewStreamReader(strings.NewReader(sb.String()), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	totalRows := 0
	batches := 0
	for {
		b, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		totalRows += b.Len
		batches++
	}

	if totalRows != numRows {
		t.Errorf("expected %d total rows, got %d", numRows, totalRows)
	}
	if batches < 2 {
		t.Errorf("expected at least 2 batches for %d rows, got %d", numRows, batches)
	}
}

func TestStreamReader_TSV(t *testing.T) {
	data := "name\tage\nalice\t30\nbob\t25\n"
	cfg := ReaderConfig{Delimiter: '\t', HasHeader: true}
	r, err := NewStreamReader(strings.NewReader(data), cfg)
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
	if b.Columns[0].BytesData.StringValue(0) != "alice" {
		t.Errorf("expected alice, got %s", b.Columns[0].BytesData.StringValue(0))
	}
}

func TestStreamReader_Empty(t *testing.T) {
	r, err := NewStreamReader(strings.NewReader(""), DefaultConfig())
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

func TestStreamReader_HeaderOnly(t *testing.T) {
	r, err := NewStreamReader(strings.NewReader("name,age\n"), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	if len(schema) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(schema))
	}
	if schema[0].Name != "name" {
		t.Errorf("expected name, got %s", schema[0].Name)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b != nil {
		t.Error("expected nil for header-only input")
	}
}

func TestStreamReader_MissingValues(t *testing.T) {
	data := "a,b\n1,hello\n,world\n3,\n"
	r, err := NewStreamReader(strings.NewReader(data), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	b, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if b.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", b.Len)
	}
	if !b.Columns[0].Nulls.IsNull(1) {
		t.Error("a[1] should be null")
	}
	if !b.Columns[1].Nulls.IsNull(2) {
		t.Error("b[2] should be null")
	}
}

func TestStreamReader_ConsistentWithByteReader(t *testing.T) {
	// Verify both readers produce identical results for the same data
	data := "ip,count,active\n192.168.1.1,42,true\n10.0.0.1,7,false\n"

	byteReader, err := NewReader([]byte(data), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	streamReader, err := NewStreamReader(strings.NewReader(data), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	// Compare schemas
	bs := byteReader.Schema()
	ss := streamReader.Schema()
	if len(bs) != len(ss) {
		t.Fatalf("schema length mismatch: byte=%d stream=%d", len(bs), len(ss))
	}
	for i := range bs {
		if bs[i].Name != ss[i].Name || bs[i].Type != ss[i].Type {
			t.Errorf("schema[%d] mismatch: byte=%s/%v stream=%s/%v",
				i, bs[i].Name, bs[i].Type, ss[i].Name, ss[i].Type)
		}
	}

	// Compare batches
	bb, err := byteReader.Next()
	if err != nil {
		t.Fatal(err)
	}
	sb, err := streamReader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if bb.Len != sb.Len {
		t.Errorf("batch length mismatch: byte=%d stream=%d", bb.Len, sb.Len)
	}
}
