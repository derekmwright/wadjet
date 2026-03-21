package physical

import (
	"bytes"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestReadRowGroupInto_ParallelColumns verifies that readRowGroupInto produces
// correct results when reading multiple columns in parallel. It writes a
// multi-column Parquet file, reads it back via readRowGroupInto (which uses
// parallel goroutines for 2+ columns), and compares against expected values.
func TestReadRowGroupInto_ParallelColumns(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
			{Name: "score", Type: parquet.TypeFloat64},
			{Name: "active", Type: parquet.TypeBool},
		},
	}

	// Write test data into an in-memory Parquet file
	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5, "active": true},
		{"id": int64(2), "name": "bob", "score": 87.3, "active": false},
		{"id": int64(3), "name": "charlie", "score": 72.1, "active": true},
		{"id": int64(4), "name": "diana", "score": 99.9, "active": false},
		{"id": int64(5), "name": "eve", "score": 60.0, "active": true},
	}

	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("creating parquet writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	// Read it back
	data := buf.Bytes()
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("creating parquet reader: %v", err)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("expected at least 1 row group")
	}

	// Read all columns via readRowGroupInto (parallel path: 4 columns)
	rg := rgs[0]
	numRows := int(rg.NumRows())
	b := batch.NewRecordBatch(schema.Columns, numRows)
	readRowGroupInto(pqFile, rg, b, schema.Columns, nil)

	if b.Len != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), b.Len)
	}

	// Verify int64 column (id)
	for i, want := range []int64{1, 2, 3, 4, 5} {
		got := b.Columns[0].Int64Data[i]
		if got != want {
			t.Errorf("id[%d] = %d, want %d", i, got, want)
		}
	}

	// Verify string column (name)
	for i, want := range []string{"alice", "bob", "charlie", "diana", "eve"} {
		got := b.Columns[1].BytesData.StringValue(i)
		if got != want {
			t.Errorf("name[%d] = %q, want %q", i, got, want)
		}
	}

	// Verify float64 column (score)
	for i, want := range []float64{95.5, 87.3, 72.1, 99.9, 60.0} {
		got := b.Columns[2].Float64Data[i]
		if got != want {
			t.Errorf("score[%d] = %f, want %f", i, got, want)
		}
	}

	// Verify bool column (active)
	for i, want := range []bool{true, false, true, false, true} {
		got := b.Columns[3].BoolData[i]
		if got != want {
			t.Errorf("active[%d] = %v, want %v", i, got, want)
		}
	}
}

// TestReadRowGroupInto_SingleColumn verifies the sequential path (1 column)
// produces correct results.
func TestReadRowGroupInto_SingleColumn(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "value", Type: parquet.TypeInt64},
		},
	}

	rows := []map[string]any{
		{"value": int64(10)},
		{"value": int64(20)},
		{"value": int64(30)},
	}

	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("creating parquet writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	data := buf.Bytes()
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("creating parquet reader: %v", err)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("expected at least 1 row group")
	}

	rg := rgs[0]
	numRows := int(rg.NumRows())
	b := batch.NewRecordBatch(schema.Columns, numRows)
	readRowGroupInto(pqFile, rg, b, schema.Columns, nil)

	if b.Len != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), b.Len)
	}

	for i, want := range []int64{10, 20, 30} {
		got := b.Columns[0].Int64Data[i]
		if got != want {
			t.Errorf("value[%d] = %d, want %d", i, got, want)
		}
	}
}

// TestReadRowGroupInto_WithProjection verifies that column projection works
// correctly with parallel reads (only a subset of columns are read).
func TestReadRowGroupInto_WithProjection(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
			{Name: "score", Type: parquet.TypeFloat64},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5},
		{"id": int64(2), "name": "bob", "score": 87.3},
	}

	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("creating parquet writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	data := buf.Bytes()
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("creating parquet reader: %v", err)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("expected at least 1 row group")
	}

	// Read only id and score (2 columns — triggers parallel path)
	projected := []string{"id", "score"}
	readSchema := buildReadSchema(schema.Columns, projected)
	rg := rgs[0]
	numRows := int(rg.NumRows())
	b := batch.NewRecordBatch(readSchema, numRows)
	readRowGroupInto(pqFile, rg, b, schema.Columns, projected)

	if b.Len != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), b.Len)
	}

	// Verify projected columns
	for i, want := range []int64{1, 2} {
		got := b.Columns[0].Int64Data[i]
		if got != want {
			t.Errorf("id[%d] = %d, want %d", i, got, want)
		}
	}
	for i, want := range []float64{95.5, 87.3} {
		got := b.Columns[1].Float64Data[i]
		if got != want {
			t.Errorf("score[%d] = %f, want %f", i, got, want)
		}
	}
}

// TestReadRowGroupInto_NullableColumns verifies parallel reads handle nullable
// columns correctly (definition levels with actual nulls).
func TestReadRowGroupInto_NullableColumns(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "label", Type: parquet.TypeString, Nullable: true},
			{Name: "amount", Type: parquet.TypeFloat64, Nullable: true},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "label": "a", "amount": 10.0},
		{"id": int64(2), "label": nil, "amount": 20.0},
		{"id": int64(3), "label": "c", "amount": nil},
		{"id": int64(4), "label": nil, "amount": nil},
	}

	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("creating parquet writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	data := buf.Bytes()
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("creating parquet reader: %v", err)
	}

	pqFile := reader.File()
	rgs := pqFile.RowGroups()
	if len(rgs) == 0 {
		t.Fatal("expected at least 1 row group")
	}

	rg := rgs[0]
	numRows := int(rg.NumRows())
	b := batch.NewRecordBatch(schema.Columns, numRows)
	readRowGroupInto(pqFile, rg, b, schema.Columns, nil)

	if b.Len != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), b.Len)
	}

	// Verify id (non-nullable)
	for i, want := range []int64{1, 2, 3, 4} {
		got := b.Columns[0].Int64Data[i]
		if got != want {
			t.Errorf("id[%d] = %d, want %d", i, got, want)
		}
	}

	// Verify label (nullable string): rows 0,2 have values; rows 1,3 are null
	labelVec := b.Columns[1]
	if labelVec.BytesData.StringValue(0) != "a" {
		t.Errorf("label[0] = %q, want %q", labelVec.BytesData.StringValue(0), "a")
	}
	if !labelVec.Nulls.IsNull(1) {
		t.Error("label[1] should be null")
	}
	if labelVec.BytesData.StringValue(2) != "c" {
		t.Errorf("label[2] = %q, want %q", labelVec.BytesData.StringValue(2), "c")
	}
	if !labelVec.Nulls.IsNull(3) {
		t.Error("label[3] should be null")
	}

	// Verify amount (nullable float64): rows 0,1 have values; rows 2,3 are null
	amountVec := b.Columns[2]
	if amountVec.Float64Data[0] != 10.0 {
		t.Errorf("amount[0] = %f, want 10.0", amountVec.Float64Data[0])
	}
	if amountVec.Float64Data[1] != 20.0 {
		t.Errorf("amount[1] = %f, want 20.0", amountVec.Float64Data[1])
	}
	if !amountVec.Nulls.IsNull(2) {
		t.Error("amount[2] should be null")
	}
	if !amountVec.Nulls.IsNull(3) {
		t.Error("amount[3] should be null")
	}
}

// TestReadRowGroupDirect_ParallelConsistency verifies that readRowGroupDirect
// (which calls readRowGroupInto) produces the same result across multiple
// invocations, confirming no race conditions in the parallel column reads.
func TestReadRowGroupDirect_ParallelConsistency(t *testing.T) {
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64},
			{Name: "b", Type: parquet.TypeString},
			{Name: "c", Type: parquet.TypeFloat64},
			{Name: "d", Type: parquet.TypeInt32},
		},
	}

	const numRows = 100
	rows := make([]map[string]any, numRows)
	for i := range rows {
		rows[i] = map[string]any{
			"a": int64(i),
			"b": string(rune('A' + (i % 26))),
			"c": float64(i) * 1.5,
			"d": int32(i * 10),
		}
	}

	var buf bytes.Buffer
	cfg := parquet.DefaultWriterConfig()
	cfg.Compression = parquet.CompressionNone
	w, err := parquet.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("creating parquet writer: %v", err)
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	data := buf.Bytes()

	// Read the same row group 10 times and verify consistency
	for iter := 0; iter < 10; iter++ {
		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("iter %d: creating parquet reader: %v", iter, err)
		}

		pqFile := reader.File()
		rg := pqFile.RowGroups()[0]
		b := readRowGroupDirect(pqFile, rg, schema.Columns, nil)

		if b == nil || b.Len != numRows {
			t.Fatalf("iter %d: expected %d rows, got %v", iter, numRows, b)
		}

		for i := 0; i < numRows; i++ {
			if b.Columns[0].Int64Data[i] != int64(i) {
				t.Errorf("iter %d: a[%d] = %d, want %d", iter, i, b.Columns[0].Int64Data[i], i)
			}
			want := string(rune('A' + (i % 26)))
			if b.Columns[1].BytesData.StringValue(i) != want {
				t.Errorf("iter %d: b[%d] = %q, want %q", iter, i, b.Columns[1].BytesData.StringValue(i), want)
			}
			if b.Columns[2].Float64Data[i] != float64(i)*1.5 {
				t.Errorf("iter %d: c[%d] = %f, want %f", iter, i, b.Columns[2].Float64Data[i], float64(i)*1.5)
			}
			if b.Columns[3].Int32Data[i] != int32(i*10) {
				t.Errorf("iter %d: d[%d] = %d, want %d", iter, i, b.Columns[3].Int32Data[i], i*10)
			}
		}
	}
}
