package scan

import (
	"bytes"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestRowColumnarRead verifies that TypeRow columns are read via the direct
// columnar path instead of falling back to row-based reading.
func TestRowColumnarRead(t *testing.T) {
	// Use parquet-go to write a file with a nested struct column, since our
	// native writer does not support TypeRow yet.
	type Person struct {
		Name string `parquet:"name"`
		Age  int64  `parquet:"age"`
	}
	type Record struct {
		ID     int64   `parquet:"id"`
		Person *Person `parquet:"person,optional"`
	}

	rows := []Record{
		{ID: 1, Person: &Person{Name: "alice", Age: 30}},
		{ID: 2, Person: &Person{Name: "bob", Age: 25}},
		{ID: 3, Person: &Person{Name: "carol", Age: 35}},
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	schema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "person", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
				{Name: "name", Type: pqt.TypeString},
				{Name: "age", Type: pqt.TypeInt64},
			}},
		},
	}

	// Read back via columnar reader
	reader, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	// Confirm the schema has TypeRow (not flattened to leaves)
	if HasUnsupportedColumnarTypes(schema.Columns) {
		t.Fatal("TypeRow should not trigger unsupported type fallback")
	}

	batches, err := ReadFileBatches(reader, schema.Columns, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}

	b := batches[0]
	if b.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", b.Len)
	}

	// Verify id column
	for i, want := range []int64{1, 2, 3} {
		got := b.Columns[0].Int64Data[i]
		if got != want {
			t.Errorf("row %d id: got %d, want %d", i, got, want)
		}
	}

	// Verify person ROW column has Children
	personVec := b.Columns[1]
	if personVec.Type != batch.TypeRow {
		t.Fatalf("person column type: got %v, want TypeRow", personVec.Type)
	}
	if len(personVec.Children) != 2 {
		t.Fatalf("person children: got %d, want 2", len(personVec.Children))
	}

	// Verify person.name
	nameVec := personVec.Children[0]
	for i, want := range []string{"alice", "bob", "carol"} {
		got := nameVec.BytesData.StringValue(i)
		if got != want {
			t.Errorf("row %d person.name: got %q, want %q", i, got, want)
		}
	}

	// Verify person.age
	ageVec := personVec.Children[1]
	for i, want := range []int64{30, 25, 35} {
		got := ageVec.Int64Data[i]
		if got != want {
			t.Errorf("row %d person.age: got %d, want %d", i, got, want)
		}
	}
}

// TestRowColumnarReadWithProjection verifies that column projection works
// when a ROW column is in the schema.
func TestRowColumnarReadWithProjection(t *testing.T) {
	// Use parquet-go to write a file with nested struct + extra column.
	type Info struct {
		Label string  `parquet:"label"`
		Score float64 `parquet:"score"`
	}
	type Record struct {
		ID    int64  `parquet:"id"`
		Info  *Info  `parquet:"info,optional"`
		Extra string `parquet:"extra"`
	}

	rows := []Record{
		{ID: 1, Info: &Info{Label: "a", Score: 1.5}, Extra: "x"},
		{ID: 2, Info: &Info{Label: "b", Score: 2.5}, Extra: "y"},
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := pqt.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}

	// Project only id and info (skip extra)
	projected := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "info", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "label", Type: pqt.TypeString},
			{Name: "score", Type: pqt.TypeFloat64},
		}},
	}

	batches, err := ReadFileBatches(reader, projected, []string{"id", "info"})
	if err != nil {
		t.Fatal(err)
	}

	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}

	b := batches[0]
	if b.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", b.Len)
	}

	// Verify ROW children are intact
	infoVec := b.Columns[1]
	if len(infoVec.Children) != 2 {
		t.Fatalf("info children: got %d, want 2", len(infoVec.Children))
	}

	if infoVec.Children[0].BytesData.StringValue(0) != "a" {
		t.Errorf("info.label[0]: got %q, want %q", infoVec.Children[0].BytesData.StringValue(0), "a")
	}
	if infoVec.Children[1].Float64Data[1] != 2.5 {
		t.Errorf("info.score[1]: got %f, want 2.5", infoVec.Children[1].Float64Data[1])
	}
}
