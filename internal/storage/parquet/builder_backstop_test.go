package parquet

import (
	"strings"
	"testing"
)

// Round-1 P1: the schema-element builders are a BACKSTOP, not a second
// spelling of the validator's opinion.
//
// ADR-0018 §14 claims "if the validator and the builder ever disagree, the file
// is not written at all". At 0a3da4ff that was true only for the two narrowing
// casts: buildMapSchemaElements returned nil for a malformed MAP after emitting
// a key_value group promising two children and giving none, and
// buildArraySchemaElements substituted a STRING element for a missing
// ElementType. BuildSchemaTree reads the first back as a group borrowing the
// next two TOP-LEVEL columns — the #970 corruption — so with the constructor's
// validation removed the builder still finalized a 424-byte unreadable file.
//
// The builders are driven DIRECTLY here, which is the only way to reach them
// with a shape ValidateWriteSchema refuses: through either constructor the
// validator answers first, and that is the point of a backstop.
func TestTheSchemaElementBuildersRefuseWhatTheValidatorRefuses(t *testing.T) {
	for _, tc := range writeSchemaRefusalCases {
		schema := Schema{Columns: []Column{tc.column, {Name: "after", Type: TypeInt64, Nullable: true}}}
		if err := ValidateWriteSchema(schema); err == nil {
			t.Fatalf("%s: the corpus entry is not actually refused by the validator", tc.name)
		}
		elements, err := buildSchemaElements(schema)
		if err == nil {
			t.Errorf("%s: buildSchemaElements emitted %d elements for a schema the validator refuses; "+
				"a truncated or substituted tree is exactly what #970 finalized (round-1 P1)",
				tc.name, len(elements))
			continue
		}
		if !strings.Contains(err.Error(), tc.column.Name) {
			t.Errorf("%s: the builder's refusal %q does not name the column %q",
				tc.name, err, tc.column.Name)
		}
		if elements != nil {
			t.Errorf("%s: the builder returned %d elements beside its error", tc.name, len(elements))
		}
	}
}

// The other direction: every shape the validator ACCEPTS, the builders build.
// A backstop that refused more than the validator would refuse files the writer
// is supposed to produce.
func TestTheSchemaElementBuildersBuildWhatTheValidatorAccepts(t *testing.T) {
	entry := Column{Name: "key_value", Type: TypeRow, Fields: []Column{
		{Name: "key", Type: TypeString},
		{Name: "value", Type: TypeInt64, Nullable: true},
	}}
	elem := Column{Name: "element", Type: TypeInt64, Nullable: true}
	nestedElem := Column{Name: "element", Type: TypeRow, Nullable: true, Fields: []Column{
		{Name: "f", Type: TypeVector, Dimension: 2, Nullable: true},
	}}
	schema := Schema{Columns: []Column{
		{Name: "i", Type: TypeInt64, Nullable: true},
		{Name: "d", Type: TypeDecimal, Precision: 38, Scale: 38, Nullable: true},
		{Name: "d0", Type: TypeDecimal, Scale: 2, Nullable: true},
		{Name: "v", Type: TypeVector, Dimension: MaxVectorDimension, Nullable: true},
		{Name: "arr", Type: TypeArray, Nullable: true, ElementType: &elem},
		{Name: "arow", Type: TypeArray, Nullable: true, ElementType: &nestedElem},
		{Name: "m", Type: TypeMap, Nullable: true, ElementType: &entry},
		{Name: "r", Type: TypeRow, Nullable: true, Fields: []Column{{Name: "f", Type: TypeString}}},
	}}
	if err := ValidateWriteSchema(schema); err != nil {
		t.Fatalf("the validator refuses the well-formed fixture: %v", err)
	}
	elements, err := buildSchemaElements(schema)
	if err != nil {
		t.Fatalf("the builder refuses a schema the validator accepts: %v", err)
	}
	root, leaves := BuildSchemaTree(elements)
	if root == nil {
		t.Fatal("no schema root")
	}
	if len(root.Children) != len(schema.Columns) {
		t.Fatalf("the tree has %d top-level columns, want %d", len(root.Children), len(schema.Columns))
	}
	if len(leaves) == 0 {
		t.Fatal("no leaves")
	}
}
