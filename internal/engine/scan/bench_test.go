package scan

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// generateParquetData writes n rows to a parquet buffer and returns the bytes.
func generateParquetData(n int, nullable bool) []byte {
	schema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "name", Type: pqt.TypeString, Nullable: nullable},
			{Name: "amount", Type: pqt.TypeFloat64, Nullable: nullable},
			{Name: "category", Type: pqt.TypeString},
		},
	}

	cats := []string{"purchase", "refund", "view", "click", "impression"}
	rows := make([]map[string]any, n)
	for i := range rows {
		row := map[string]any{
			"id":       int64(i),
			"category": cats[i%len(cats)],
		}
		if nullable && i%10 == 0 {
			row["name"] = nil
			row["amount"] = nil
		} else {
			row["name"] = fmt.Sprintf("user_%06d", i)
			row["amount"] = float64(i) * 1.5
		}
		rows[i] = row
	}

	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		panic(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		panic(err)
	}
	if err := pw.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// BenchmarkReadRowOriented benchmarks the old path: ReadRows + batch.FromRows
func BenchmarkReadRowOriented(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		data := generateParquetData(n, false)
		schema := []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "name", Type: pqt.TypeString},
			{Name: "amount", Type: pqt.TypeFloat64},
			{Name: "category", Type: pqt.TypeString},
		}

		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					b.Fatal(err)
				}
				rows, err := reader.ReadRows(nil)
				if err != nil {
					b.Fatal(err)
				}
				rb := batch.FromRows(schema, rows)
				if rb.Len != n {
					b.Fatalf("expected %d rows, got %d", n, rb.Len)
				}
			}
		})
	}
}

// BenchmarkReadColumnar benchmarks the native columnar path: ReadRowGroupNative directly
func BenchmarkReadColumnar(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		data := generateParquetData(n, false)
		schema := []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "name", Type: pqt.TypeString},
			{Name: "amount", Type: pqt.TypeFloat64},
			{Name: "category", Type: pqt.TypeString},
		}

		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					b.Fatal(err)
				}
				fr := reader.FileReader()
				var total int
				for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
					rb, err := ReadRowGroupNative(fr, rgIdx, schema, nil)
					if err != nil {
						b.Fatal(err)
					}
					if rb != nil {
						total += rb.Len
					}
				}
				if total != n {
					b.Fatalf("expected %d rows, got %d", n, total)
				}
			}
		})
	}
}

// BenchmarkReadColumnarNullable benchmarks columnar read with nullable columns.
func BenchmarkReadColumnarNullable(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		data := generateParquetData(n, true)
		schema := []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "name", Type: pqt.TypeString, Nullable: true},
			{Name: "amount", Type: pqt.TypeFloat64, Nullable: true},
			{Name: "category", Type: pqt.TypeString},
		}

		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					b.Fatal(err)
				}
				fr := reader.FileReader()
				var total int
				for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
					rb, err := ReadRowGroupNative(fr, rgIdx, schema, nil)
					if err != nil {
						b.Fatal(err)
					}
					if rb != nil {
						total += rb.Len
					}
				}
				if total != n {
					b.Fatalf("expected %d rows, got %d", n, total)
				}
			}
		})
	}
}

// BenchmarkReadProjection benchmarks columnar read with column projection (2 of 4 cols).
func BenchmarkReadProjection(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		data := generateParquetData(n, false)

		b.Run(fmt.Sprintf("rows=%d/row_oriented", n), func(b *testing.B) {
			schema := []pqt.Column{
				{Name: "id", Type: pqt.TypeInt64},
				{Name: "amount", Type: pqt.TypeFloat64},
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					b.Fatal(err)
				}
				rows, err := reader.ReadRows([]string{"id", "amount"})
				if err != nil {
					b.Fatal(err)
				}
				rb := batch.FromRows(schema, rows)
				_ = rb
			}
		})

		b.Run(fmt.Sprintf("rows=%d/columnar", n), func(b *testing.B) {
			schema := []pqt.Column{
				{Name: "id", Type: pqt.TypeInt64},
				{Name: "amount", Type: pqt.TypeFloat64},
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					b.Fatal(err)
				}
				fr := reader.FileReader()
				for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
					rb, err := ReadRowGroupNative(fr, rgIdx, schema, nil)
					if err != nil {
						b.Fatal(err)
					}
					_ = rb
				}
			}
		})
	}
}

// TestColumnarReadCorrectness verifies that the columnar reader produces the
// same results as the row-oriented reader.
func TestColumnarReadCorrectness(t *testing.T) {
	schema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "name", Type: pqt.TypeString, Nullable: true},
		{Name: "amount", Type: pqt.TypeFloat64, Nullable: true},
		{Name: "category", Type: pqt.TypeString},
	}
	data := generateParquetData(100, true)

	// Row-oriented path
	reader1, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader1.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	rowBatch := batch.FromRows(schema, rows)

	// Columnar path (native)
	reader2, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	fr := reader2.FileReader()
	if fr.NumRowGroups() == 0 {
		t.Fatal("no row groups")
	}

	var colBatches []*batch.RecordBatch
	for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
		rb, err := ReadRowGroupNative(fr, rgIdx, schema, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rb != nil {
			colBatches = append(colBatches, rb)
		}
	}

	// Merge columnar batches
	totalRows := 0
	for _, b := range colBatches {
		totalRows += b.Len
	}
	if totalRows != rowBatch.Len {
		t.Fatalf("row count mismatch: row-oriented=%d, columnar=%d", rowBatch.Len, totalRows)
	}

	// Compare values
	colBatch := colBatches[0]
	if len(colBatches) > 1 {
		colBatch = batch.NewRecordBatch(schema, totalRows)
		offset := 0
		for _, b := range colBatches {
			for j := range schema {
				copyVectorRange(colBatch.Columns[j], offset, b.Columns[j], 0, b.Len)
			}
			offset += b.Len
		}
	}

	for row := 0; row < totalRows; row++ {
		for col := range schema {
			rv := rowBatch.Columns[col].GetValue(row)
			cv := colBatch.Columns[col].GetValue(row)
			if fmt.Sprint(rv) != fmt.Sprint(cv) {
				t.Fatalf("mismatch at row %d, col %q: row-oriented=%v, columnar=%v",
					row, schema[col].Name, rv, cv)
			}
		}
	}
}

// TestColumnarReadWithProjection verifies column projection works correctly.
func TestColumnarReadWithProjection(t *testing.T) {
	data := generateParquetData(50, false)
	projectedSchema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "amount", Type: pqt.TypeFloat64},
	}

	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	fr := reader.FileReader()

	for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
		rb, err := ReadRowGroupNative(fr, rgIdx, projectedSchema, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rb == nil {
			t.Fatal("expected non-nil batch")
		}
		if len(rb.Columns) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(rb.Columns))
		}

		// Verify id values
		for i := 0; i < rb.Len; i++ {
			id := rb.Columns[0].Int64Data[i]
			if id != int64(i) {
				t.Fatalf("row %d: expected id=%d, got %d", i, i, id)
			}
			amount := rb.Columns[1].Float64Data[i]
			expected := float64(i) * 1.5
			if amount != expected {
				t.Fatalf("row %d: expected amount=%f, got %f", i, expected, amount)
			}
		}
	}
}

// TestColumnarReadMissingColumn verifies that missing columns produce null vectors.
func TestColumnarReadMissingColumn(t *testing.T) {
	data := generateParquetData(20, false)
	// Request a column that doesn't exist in the parquet file
	schemaWithExtra := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "nonexistent", Type: pqt.TypeString, Nullable: true},
	}

	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	fr := reader.FileReader()

	for rgIdx := 0; rgIdx < fr.NumRowGroups(); rgIdx++ {
		rb, err := ReadRowGroupNative(fr, rgIdx, schemaWithExtra, nil)
		if err != nil {
			t.Fatal(err)
		}
		if rb == nil {
			t.Fatal("expected non-nil batch")
		}

		// nonexistent column should be all nulls
		for i := 0; i < rb.Len; i++ {
			if !rb.Columns[1].Nulls.IsNull(i) {
				t.Fatalf("row %d: expected null for nonexistent column", i)
			}
		}
	}
}

func BenchmarkFindParquetColumn(b *testing.B) {
	// Simulate a wide table schema
	cols := make([][]string, 50)
	for i := range cols {
		cols[i] = []string{fmt.Sprintf("col_%03d", i)}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = findParquetColumn(cols, "col_025")
		_ = findParquetColumn(cols, "col_049")
		_ = findParquetColumn(cols, "nonexistent")
	}
}
