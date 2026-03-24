package scan

import (
	"bytes"
	"fmt"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// writeParquetDirect writes rows using a raw parquet-go schema (not Wadjet's).
func writeParquetDirect(t *testing.T, pqSchema *goparquet.Schema, rows interface{}) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := goparquet.NewWriter(&buf, pqSchema)
	if _, err := w.WriteRows([]goparquet.Row{} /* trigger schema init */); err != nil {
		// Ignore — some versions need at least one call
	}
	// Use the generic writer
	pw := goparquet.NewGenericWriter[any](&buf, pqSchema)
	_ = pw.Close()

	// Fall back to row-level writing via the lower-level API
	buf.Reset()
	w = goparquet.NewWriter(&buf, pqSchema)
	defer w.Close()
	return buf.Bytes()
}

// TestTypeCoercionInt64ToInt32 verifies INT64 file data is narrowed to INT32.
func TestTypeCoercionInt64ToInt32(t *testing.T) {
	// Write a file with INT64 columns using Wadjet's writer
	fileSchema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "value", Type: pqt.TypeInt64, Nullable: true},
		},
	}
	rows := make([]map[string]any, 100)
	for i := range rows {
		row := map[string]any{"id": int64(i + 1)}
		if i%10 == 0 {
			row["value"] = nil
		} else {
			row["value"] = int64(i * 100)
		}
		rows[i] = row
	}

	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, fileSchema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read with INT32 catalog schema — should coerce
	catalogSchema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt32},
		{Name: "value", Type: pqt.TypeInt32, Nullable: true},
	}

	reader, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	batches, err := ReadFileBatches(reader, catalogSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) == 0 {
		t.Fatal("no batches returned")
	}

	b := batches[0]
	if b.Len != 100 {
		t.Fatalf("expected 100 rows, got %d", b.Len)
	}

	// Verify non-nullable column
	for i := 0; i < 100; i++ {
		got := b.Columns[0].Int32Data[i]
		want := int32(i + 1)
		if got != want {
			t.Fatalf("id[%d]: got %d, want %d", i, got, want)
		}
	}

	// Verify nullable column
	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			if !b.Columns[1].Nulls.IsNull(i) {
				t.Fatalf("value[%d]: expected null", i)
			}
		} else {
			if b.Columns[1].Nulls.IsNull(i) {
				t.Fatalf("value[%d]: unexpected null", i)
			}
			got := b.Columns[1].Int32Data[i]
			want := int32(i * 100)
			if got != want {
				t.Fatalf("value[%d]: got %d, want %d", i, got, want)
			}
		}
	}
}

// TestTypeCoercionInt64ToFloat64 verifies INT64 file data is widened to Float64.
func TestTypeCoercionInt64ToFloat64(t *testing.T) {
	fileSchema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "quantity", Type: pqt.TypeInt64},
		},
	}
	rows := make([]map[string]any, 50)
	for i := range rows {
		rows[i] = map[string]any{"quantity": int64(i * 7)}
	}

	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, fileSchema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read with Float64 catalog schema
	catalogSchema := []pqt.Column{
		{Name: "quantity", Type: pqt.TypeFloat64},
	}

	reader, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	batches, err := ReadFileBatches(reader, catalogSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) == 0 {
		t.Fatal("no batches returned")
	}

	b := batches[0]
	for i := 0; i < 50; i++ {
		got := b.Columns[0].Float64Data[i]
		want := float64(i * 7)
		if got != want {
			t.Fatalf("quantity[%d]: got %f, want %f", i, got, want)
		}
	}
}

// TestTypeCoercionDateToString verifies DATE (INT32 days) is formatted as "YYYY-MM-DD".
func TestTypeCoercionDateToString(t *testing.T) {
	// Write using Wadjet TypeDate which stores as INT32 days-since-epoch
	fileSchema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "event_date", Type: pqt.TypeDate},
			{Name: "nullable_date", Type: pqt.TypeDate, Nullable: true},
		},
	}

	// Known dates: 1970-01-01 = day 0, 2024-01-01 = day 19723, etc.
	testDates := []struct {
		dateStr string
		days    int32
	}{
		{"1970-01-01", 0},
		{"2024-01-01", 19723},
		{"1995-03-15", 9204},
		{"1998-12-01", 10561},
	}

	rows := make([]map[string]any, len(testDates)*2)
	for i, td := range testDates {
		rows[i*2] = map[string]any{
			"event_date":   td.dateStr,
			"nullable_date": td.dateStr,
		}
		rows[i*2+1] = map[string]any{
			"event_date":   td.dateStr,
			"nullable_date": nil,
		}
	}

	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, fileSchema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read with String catalog schema — should format as "YYYY-MM-DD"
	catalogSchema := []pqt.Column{
		{Name: "event_date", Type: pqt.TypeString},
		{Name: "nullable_date", Type: pqt.TypeString, Nullable: true},
	}

	reader, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	batches, err := ReadFileBatches(reader, catalogSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) == 0 {
		t.Fatal("no batches returned")
	}

	b := batches[0]
	if b.Len != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), b.Len)
	}

	// Verify non-nullable date column
	for i, td := range testDates {
		got := string(b.Columns[0].BytesData.Value(i * 2))
		if got != td.dateStr {
			t.Fatalf("event_date[%d]: got %q, want %q", i*2, got, td.dateStr)
		}
	}

	// Verify nullable date column: even rows have values, odd rows are null
	for i := 0; i < b.Len; i++ {
		if i%2 == 1 {
			if !b.Columns[1].Nulls.IsNull(i) {
				t.Fatalf("nullable_date[%d]: expected null", i)
			}
		} else {
			if b.Columns[1].Nulls.IsNull(i) {
				t.Fatalf("nullable_date[%d]: unexpected null", i)
			}
			got := string(b.Columns[1].BytesData.Value(i))
			want := testDates[i/2].dateStr
			if got != want {
				t.Fatalf("nullable_date[%d]: got %q, want %q", i, got, want)
			}
		}
	}
}

// TestColumnAlias verifies that findParquetColumn falls back to known aliases.
func TestColumnAlias(t *testing.T) {
	// Write a file with "comments" column name
	fileSchema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "comments", Type: pqt.TypeString, Nullable: true},
		},
	}
	rows := []map[string]any{
		{"id": int64(1), "comments": "hello"},
		{"id": int64(2), "comments": nil},
		{"id": int64(3), "comments": "world"},
	}

	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, fileSchema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	// Read with "l_comment" catalog name — should resolve via alias
	catalogSchema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "l_comment", Type: pqt.TypeString, Nullable: true},
	}

	reader, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	batches, err := ReadFileBatches(reader, catalogSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) == 0 {
		t.Fatal("no batches returned")
	}

	b := batches[0]
	// Verify data was read through alias
	got := string(b.Columns[1].BytesData.Value(0))
	if got != "hello" {
		t.Fatalf("l_comment[0]: got %q, want %q", got, "hello")
	}
	if !b.Columns[1].Nulls.IsNull(1) {
		t.Fatal("l_comment[1]: expected null")
	}
	got = string(b.Columns[1].BytesData.Value(2))
	if got != "world" {
		t.Fatalf("l_comment[2]: got %q, want %q", got, "world")
	}
}

func init() {
	// Suppress unused import warning
	_ = fmt.Sprint
	_ = batch.TypeInt32
}
