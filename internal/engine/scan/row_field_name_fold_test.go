package scan

import (
	"bytes"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A ROW field resolves by the package's FoldName rule, not byte-exactly (#904).
//
// `leafByPath` was the one byte-exact lookup in a package whose stated rule is
// FoldName — parquet.LeafIndex is a struct precisely so that a caller cannot
// index it unfolded (`schema_tree.go`: "the folding IS the lookup rule, and a
// site that forgets it reads the column as all-NULL rather than failing"), and
// this second map bypassed it. `leaf.Path` carries the FILE's capitalization
// and `col.Name`/`field.Name` the CATALOG's, so a file written by an external
// writer with `Name` inside a ROW, read through a table that declares `name`,
// MISSED — and the miss takes the all-NULL arm, which is #448 one level down:
// the field is read away rather than reported.
//
// The file is written with the capitalized field names and read back with the
// lower-cased schema, which is exactly the shape a PyArrow- or Bento-written
// file registered through a CREATE TABLE produces.
func TestNativeRowFieldResolvesByFoldedName(t *testing.T) {
	fileSchema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "R", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "Name", Type: pqt.TypeString, Nullable: true},
			{Name: "Count", Type: pqt.TypeInt64, Nullable: true},
		}},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, fileSchema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows([]map[string]any{
		{"id": int64(1), "R": map[string]any{"Name": "alice", "Count": int64(10)}},
		{"id": int64(2), "R": map[string]any{"Name": "bob", "Count": int64(20)}},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	// The READ schema is the catalog's, lower-cased the way #731's lexer
	// folds an unquoted identifier.
	readSchema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "name", Type: pqt.TypeString, Nullable: true},
			{Name: "count", Type: pqt.TypeInt64, Nullable: true},
		}},
	}
	batches, err := ReadFileBatchesNative(r.FileReader(), readSchema, nil)
	if err != nil {
		t.Fatalf("ReadFileBatchesNative: %v", err)
	}
	wantName := []any{"alice", "bob"}
	wantCount := []any{int64(10), int64(20)}
	row := 0
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			if row >= len(wantName) {
				t.Fatalf("more rows than written")
			}
			if got := b.Columns[1].Children[0].GetValue(i); got != wantName[row] {
				t.Errorf("row %d: r.name = %#v, want %#v — a ROW field whose file "+
					"spelling differs only in CASE was read away as NULL",
					row, got, wantName[row])
			}
			if got := b.Columns[1].Children[1].GetValue(i); got != wantCount[row] {
				t.Errorf("row %d: r.count = %#v, want %#v", row, got, wantCount[row])
			}
			row++
		}
	}
	if row != len(wantName) {
		t.Fatalf("read %d rows, want %d", row, len(wantName))
	}
}

// The BOUNDARY of that fold, from the other side: a field the file genuinely
// does not carry still reads as NULL rather than raising, because that is
// schema evolution — a field added to the table after the file was written —
// and it is what the top-level column arm does for the same reason. Folding
// removes the case MISS; it does not turn absence into an error.
func TestNativeRowFieldAbsentFromTheFileIsStillNull(t *testing.T) {
	fileSchema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "name", Type: pqt.TypeString, Nullable: true},
		}},
	}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, fileSchema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := w.WriteRows([]map[string]any{
		{"id": int64(1), "r": map[string]any{"name": "alice"}},
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	r, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	readSchema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "r", Type: pqt.TypeRow, Nullable: true, Fields: []pqt.Column{
			{Name: "name", Type: pqt.TypeString, Nullable: true},
			{Name: "added_later", Type: pqt.TypeInt64, Nullable: true},
		}},
	}
	batches, err := ReadFileBatchesNative(r.FileReader(), readSchema, nil)
	if err != nil {
		t.Fatalf("ReadFileBatchesNative: %v", err)
	}
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			if got := b.Columns[1].Children[0].GetValue(i); got != any("alice") {
				t.Errorf("r.name = %#v, want alice", got)
			}
			if !b.Columns[1].Children[1].Nulls.IsNull(i) {
				t.Errorf("r.added_later is not NULL; the file carries no such field")
			}
		}
	}
}
