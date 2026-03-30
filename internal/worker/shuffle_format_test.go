package worker

import (
	"bytes"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestShuffleFormatRoundTrip(t *testing.T) {
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
		{Name: "price", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "count", Type: parquet.TypeInt32, Nullable: true},
	}

	// Create a batch with mixed data including nulls
	b := batch.NewRecordBatch(schema, 5)
	b.Columns[0].Int64Data[0] = 100
	b.Columns[0].Int64Data[1] = 200
	b.Columns[0].Int64Data[2] = 300
	b.Columns[0].Int64Data[3] = 400
	b.Columns[0].Int64Data[4] = 500

	b.Columns[1].BytesData.Set(0, []byte("alice"))
	b.Columns[1].BytesData.Set(1, []byte("bob"))
	b.Columns[1].BytesData.Set(2, []byte(""))
	b.Columns[1].Nulls.SetNull(2) // null string
	b.Columns[1].BytesData.Set(3, []byte("dave"))
	b.Columns[1].BytesData.Set(4, []byte("eve"))

	b.Columns[2].Float64Data[0] = 1.5
	b.Columns[2].Float64Data[1] = 2.5
	b.Columns[2].Float64Data[2] = 3.5
	b.Columns[2].Float64Data[3] = 4.5
	b.Columns[2].Nulls.SetNull(3)
	b.Columns[2].Float64Data[4] = 5.5

	b.Columns[3].Int32Data[0] = 10
	b.Columns[3].Int32Data[1] = 20
	b.Columns[3].Int32Data[2] = 30
	b.Columns[3].Int32Data[3] = 40
	b.Columns[3].Int32Data[4] = 50

	// Write with selection vector (rows 0, 2, 4)
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}

	sel := []uint32{0, 2, 4}
	if err := sw.writeChunk(b.Columns, sel, len(sel)); err != nil {
		t.Fatal(err)
	}

	// Write another chunk with all rows
	if err := sw.writeChunk(b.Columns, nil, 5); err != nil {
		t.Fatal(err)
	}

	// Patch chunk count
	data := buf.Bytes()
	data[4] = byte(sw.numChunks)

	// Read back
	batches, err := shuffleReadBatches(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}

	// Check first batch (selected rows 0, 2, 4)
	b1 := batches[0]
	if b1.Len != 3 {
		t.Fatalf("batch 1: expected 3 rows, got %d", b1.Len)
	}
	if b1.Columns[0].Int64Data[0] != 100 {
		t.Errorf("batch 1 row 0 id: got %d, want 100", b1.Columns[0].Int64Data[0])
	}
	if b1.Columns[0].Int64Data[1] != 300 {
		t.Errorf("batch 1 row 1 id: got %d, want 300", b1.Columns[0].Int64Data[1])
	}
	if b1.Columns[0].Int64Data[2] != 500 {
		t.Errorf("batch 1 row 2 id: got %d, want 500", b1.Columns[0].Int64Data[2])
	}

	// Check string column round-trip
	if string(b1.Columns[1].BytesData.Value(0)) != "alice" {
		t.Errorf("batch 1 row 0 name: got %q, want alice", b1.Columns[1].BytesData.Value(0))
	}
	// Row 2 was null
	if !b1.Columns[1].Nulls.IsNull(1) {
		t.Error("batch 1 row 1 name should be null")
	}
	if string(b1.Columns[1].BytesData.Value(2)) != "eve" {
		t.Errorf("batch 1 row 2 name: got %q, want eve", b1.Columns[1].BytesData.Value(2))
	}

	// Check float64 column
	if b1.Columns[2].Float64Data[0] != 1.5 {
		t.Errorf("batch 1 row 0 price: got %f, want 1.5", b1.Columns[2].Float64Data[0])
	}

	// Check second batch (all 5 rows)
	b2 := batches[1]
	if b2.Len != 5 {
		t.Fatalf("batch 2: expected 5 rows, got %d", b2.Len)
	}
	if b2.Columns[0].Int64Data[3] != 400 {
		t.Errorf("batch 2 row 3 id: got %d, want 400", b2.Columns[0].Int64Data[3])
	}
	// Row 3 price should be null
	if !b2.Columns[2].Nulls.IsNull(3) {
		t.Error("batch 2 row 3 price should be null")
	}
}

func TestShuffleFormatDetection(t *testing.T) {
	// Binary shuffle format
	shuffleData := []byte{'W', 'S', 'H', 'F', 0, 0, 0, 0}
	if !isShuffleFormat(shuffleData) {
		t.Error("should detect shuffle format")
	}

	// Parquet format (starts with PAR1)
	parquetData := []byte{'P', 'A', 'R', '1', 0, 0, 0, 0}
	if isShuffleFormat(parquetData) {
		t.Error("should not detect parquet as shuffle format")
	}

	// Too short
	if isShuffleFormat([]byte{1, 2}) {
		t.Error("should not detect short data as shuffle format")
	}
}

func BenchmarkShuffleWriteVsParquet(b *testing.B) {
	schema := []parquet.Column{
		{Name: "key", Type: parquet.TypeInt64, Nullable: false},
		{Name: "value", Type: parquet.TypeFloat64, Nullable: false},
		{Name: "label", Type: parquet.TypeString, Nullable: false},
	}

	// Create a batch with 2048 rows
	rb := batch.NewRecordBatch(schema, 2048)
	for i := 0; i < 2048; i++ {
		rb.Columns[0].Int64Data[i] = int64(i)
		rb.Columns[1].Float64Data[i] = float64(i) * 1.5
		rb.Columns[2].BytesData.Set(i, []byte("test-value-string"))
	}

	b.Run("binary_shuffle", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			sw := newShuffleWriter(&buf, schema)
			sw.writeHeader()
			sw.writeChunk(rb.Columns, nil, 2048)
		}
	})

}
