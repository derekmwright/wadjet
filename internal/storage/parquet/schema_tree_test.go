package parquet

import (
	"bytes"
	"testing"

	goparquet "github.com/parquet-go/parquet-go"
)

// TestBuildSchemaTreeFlat tests schema tree construction for a flat schema.
func TestBuildSchemaTreeFlat(t *testing.T) {
	// Build schema elements: root group "schema" with 3 leaves.
	int64Type := PhysicalInt64
	baType := PhysicalByteArray
	doubleType := PhysicalDouble

	elements := []SchemaElement{
		{Name: "schema", NumChildren: 3, RepetitionType: FieldRequired},
		{Name: "id", Type: &int64Type, RepetitionType: FieldRequired},
		{Name: "name", Type: &baType, RepetitionType: FieldOptional},
		{Name: "val", Type: &doubleType, RepetitionType: FieldRequired},
	}

	root, leaves := BuildSchemaTree(elements)
	if root == nil {
		t.Fatal("root is nil")
	}
	if root.Name != "schema" {
		t.Errorf("root name = %q, want %q", root.Name, "schema")
	}
	if len(root.Children) != 3 {
		t.Fatalf("root children = %d, want 3", len(root.Children))
	}
	if len(leaves) != 3 {
		t.Fatalf("leaves = %d, want 3", len(leaves))
	}

	// Verify leaf properties.
	id := leaves[0]
	if id.Name != "id" || id.LeafIndex != 0 {
		t.Errorf("leaf[0]: name=%q index=%d, want name=id index=0", id.Name, id.LeafIndex)
	}
	if id.MaxDefLevel != 0 {
		t.Errorf("id.MaxDefLevel = %d, want 0 (required)", id.MaxDefLevel)
	}

	name := leaves[1]
	if name.Name != "name" || name.LeafIndex != 1 {
		t.Errorf("leaf[1]: name=%q index=%d", name.Name, name.LeafIndex)
	}
	if name.MaxDefLevel != 1 {
		t.Errorf("name.MaxDefLevel = %d, want 1 (optional)", name.MaxDefLevel)
	}

	val := leaves[2]
	if val.Name != "val" || val.LeafIndex != 2 {
		t.Errorf("leaf[2]: name=%q index=%d", val.Name, val.LeafIndex)
	}

	// Test lookup functions.
	found := FindLeafByName(root, "name")
	if found == nil || found.Name != "name" {
		t.Error("FindLeafByName(name) failed")
	}
	found = FindLeafByIndex(root, 2)
	if found == nil || found.Name != "val" {
		t.Error("FindLeafByIndex(2) failed")
	}
	if SchemaLeafCount(root) != 3 {
		t.Errorf("SchemaLeafCount = %d, want 3", SchemaLeafCount(root))
	}
}

// TestBuildSchemaTreeNested tests schema with nested group (LIST/MAP pattern).
func TestBuildSchemaTreeNested(t *testing.T) {
	// Schema: root -> id (INT64), tags (group: list -> element: BYTE_ARRAY)
	int64Type := PhysicalInt64
	baType := PhysicalByteArray

	elements := []SchemaElement{
		{Name: "schema", NumChildren: 2, RepetitionType: FieldRequired},
		{Name: "id", Type: &int64Type, RepetitionType: FieldRequired},
		{Name: "tags", NumChildren: 1, RepetitionType: FieldOptional},
		{Name: "list", NumChildren: 1, RepetitionType: FieldRepeated},
		{Name: "element", Type: &baType, RepetitionType: FieldOptional},
	}

	_, leaves := BuildSchemaTree(elements)
	if len(leaves) != 2 {
		t.Fatalf("leaves = %d, want 2", len(leaves))
	}

	// id: required -> def=0, rep=0
	if leaves[0].MaxDefLevel != 0 || leaves[0].MaxRepLevel != 0 {
		t.Errorf("id: def=%d rep=%d, want def=0 rep=0",
			leaves[0].MaxDefLevel, leaves[0].MaxRepLevel)
	}

	// element: optional(tags) -> repeated(list) -> optional(element)
	// def = 0 + 1(tags optional) + 1(list repeated) + 1(element optional) = 3
	// rep = 0 + 0 + 1(list repeated) + 0 = 1
	elem := leaves[1]
	if elem.MaxDefLevel != 3 {
		t.Errorf("element.MaxDefLevel = %d, want 3", elem.MaxDefLevel)
	}
	if elem.MaxRepLevel != 1 {
		t.Errorf("element.MaxRepLevel = %d, want 1", elem.MaxRepLevel)
	}
	if len(elem.Path) != 3 {
		t.Errorf("element.Path = %v, want 3 components", elem.Path)
	}
}

// TestBuildSchemaTreeFromRealFile reads schema from a parquet-go file
// and verifies our tree matches parquet-go's interpretation.
func TestBuildSchemaTreeFromRealFile(t *testing.T) {
	type Record struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	var buf bytes.Buffer
	writer := goparquet.NewGenericWriter[Record](&buf)
	if _, err := writer.Write([]Record{{1, "alice"}, {2, "bob"}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	fileData := buf.Bytes()
	r := bytes.NewReader(fileData)

	md, err := ReadFileMetaData(r, int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}

	schemaRoot, leaves := BuildSchemaTree(md.Schema)
	if schemaRoot == nil {
		t.Fatal("root is nil")
	}

	// Compare with parquet-go's schema.
	pqFile, err := goparquet.OpenFile(r, int64(len(fileData)))
	if err != nil {
		t.Fatal(err)
	}
	pqCols := pqFile.Schema().Columns()

	if len(leaves) != len(pqCols) {
		t.Fatalf("leaf count: ours=%d, parquet-go=%d", len(leaves), len(pqCols))
	}

	for i, leaf := range leaves {
		pqPath := pqCols[i]
		pqName := pqPath[len(pqPath)-1]
		if leaf.Name != pqName {
			t.Errorf("leaf[%d] name: ours=%q, parquet-go=%q", i, leaf.Name, pqName)
		}
	}

	// Verify definition levels match parquet-go.
	pqRoot := pqFile.Root()
	for i, leaf := range leaves {
		pqCol := findPQColumnByIndex(pqRoot, i)
		if pqCol == nil {
			continue
		}
		pqMaxDef := pqCol.MaxDefinitionLevel()
		if leaf.MaxDefLevel != pqMaxDef {
			t.Errorf("leaf[%d] %q maxDefLevel: ours=%d, parquet-go=%d",
				i, leaf.Name, leaf.MaxDefLevel, pqMaxDef)
		}
	}
}

// findPQColumnByIndex finds a parquet-go column by leaf index.
func findPQColumnByIndex(root *goparquet.Column, idx int) *goparquet.Column {
	if root.Leaf() {
		if root.Index() == idx {
			return root
		}
		return nil
	}
	for _, child := range root.Columns() {
		if found := findPQColumnByIndex(child, idx); found != nil {
			return found
		}
	}
	return nil
}
