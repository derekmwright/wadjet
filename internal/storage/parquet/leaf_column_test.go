package parquet

import (
	"bytes"
	"testing"
)

// TestLeafColumnMatchesTheOverlaidSchema pins the second half of the fix.
//
// Restoring the schema is not enough on its own: readLeafColumn asked
// nodeToColumn for the leaf's Column, which is the BARE parquet inference and
// not the file's own declared schema, so the nested decode kept taking the
// STRING arm even after Schema() started saying IPV6. FileReader.LeafColumn
// is the one answer both now come from, and this holds them together.
func TestLeafColumnMatchesTheOverlaidSchema(t *testing.T) {
	schema := ndsSchema(TypeUUID)
	rows := []map[string]any{{"id": int64(0), "flat": "00000000-0000-4000-8000-000000000009",
		"rw": map[string]any{"v": "00000000-0000-4000-8000-000000000009"}}}
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	fr, err := OpenFileReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	// Schema().FlatColumns() walks the recovered Column tree depth-first,
	// which is the order BuildSchemaTree numbers the leaves in, so entry i
	// of one is leaf i of the other.
	fileSchema := fr.Schema()
	flat := fileSchema.FlatColumns()
	if len(flat) != len(fr.Leaves()) {
		t.Fatalf("the schema flattens to %d leaves and the tree has %d", len(flat), len(fr.Leaves()))
	}
	for i := range flat {
		got := fr.LeafColumn(i)
		if got.Name != flat[i].Name || got.Type != flat[i].Type {
			t.Errorf("leaf %d (%s): LeafColumn says %s %s, Schema() says %s %s",
				i, pathKey(fr.Leaves()[i].Path), got.Name, got.Type, flat[i].Name, flat[i].Type)
		}
	}
	// The narrow claim spelled out: the ROW field's leaf is UUID, not the
	// STRING nodeToColumn infers for an unannotated BYTE_ARRAY.
	seen := false
	for i, leaf := range fr.Leaves() {
		if leaf.Name != "v" {
			continue
		}
		seen = true
		if got := fr.LeafColumn(i).Type; got != TypeUUID {
			t.Errorf("LeafColumn(%d) for %s is %s, want UUID", i, pathKey(leaf.Path), got)
		}
		if bare := nodeToColumn(leaf).Type; bare != TypeString {
			t.Errorf("the bare inference for %s is %s; this test is asserting the "+
				"wrong thing if it is no longer STRING", pathKey(leaf.Path), bare)
		}
	}
	if !seen {
		t.Fatal("no ROW-field leaf named \"v\" in the tree")
	}
	// An out-of-range index is the zero Column, not a panic.
	if c := fr.LeafColumn(len(fr.Leaves())); c.Name != "" {
		t.Errorf("out-of-range LeafColumn returned %#v", c)
	}
	if c := fr.LeafColumn(-1); c.Name != "" {
		t.Errorf("negative LeafColumn returned %#v", c)
	}
}
