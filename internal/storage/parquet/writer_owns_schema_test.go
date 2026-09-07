package parquet

import (
	"bytes"
	"reflect"
	"testing"
)

// #973: the writer owns its schema.
//
// Every case builds the SAME file twice — once against a schema nobody
// touches, once against one the caller mutates the instant the constructor
// returns — and asserts the two files are byte-identical and read back the
// same. Measured at f415faba, with WriteRows and Close both returning nil:
//
//	top-level rename      -> the row reader cannot OPEN the file
//	                         ("carries path [a] but schema leaf 0 is [b]")
//	nested ROW rename     -> the same one level down ([r a] vs [r b])
//	INT64 -> FLOAT64      -> opens, then ReadRows fails; pyarrow refuses it
//	ElementType retype    -> pyarrow OPENS it as list<element: string> and
//	                         reinterprets the INT64 bytes as strings
//
// Both exported constructors are driven, because NewWriter's schema is the
// native writer's (a second copy would be a second thing to keep in step).
func TestTheWriterOwnsItsSchema(t *testing.T) {
	cases := []struct {
		name   string
		schema func() Schema
		mutate func(*Schema)
		rows   []map[string]any
	}{
		{
			name: "top-level column name",
			schema: func() Schema {
				return Schema{Columns: []Column{{Name: "a", Type: TypeInt64, Nullable: true}}}
			},
			mutate: func(s *Schema) { s.Columns[0].Name = "b" },
			rows:   []map[string]any{{"a": int64(7)}},
		},
		{
			name: "top-level column type",
			schema: func() Schema {
				return Schema{Columns: []Column{{Name: "a", Type: TypeInt64, Nullable: true}}}
			},
			mutate: func(s *Schema) { s.Columns[0].Type = TypeFloat64 },
			rows:   []map[string]any{{"a": int64(7)}},
		},
		{
			name: "nested ROW field name",
			schema: func() Schema {
				return Schema{Columns: []Column{{Name: "r", Type: TypeRow, Nullable: true, Fields: []Column{
					{Name: "a", Type: TypeInt64, Nullable: true},
					{Name: "b", Type: TypeString, Nullable: true},
				}}}}
			},
			mutate: func(s *Schema) { s.Columns[0].Fields[0].Name = "zzz" },
			rows:   []map[string]any{{"r": map[string]any{"a": int64(7), "b": "x"}}},
		},
		{
			name: "nested ROW field type",
			schema: func() Schema {
				return Schema{Columns: []Column{{Name: "r", Type: TypeRow, Nullable: true, Fields: []Column{
					{Name: "a", Type: TypeInt64, Nullable: true},
				}}}}
			},
			mutate: func(s *Schema) { s.Columns[0].Fields[0].Type = TypeString },
			rows:   []map[string]any{{"r": map[string]any{"a": int64(7)}}},
		},
		{
			name: "ARRAY ElementType through the pointer",
			schema: func() Schema {
				elem := Column{Name: "element", Type: TypeInt64, Nullable: true}
				return Schema{Columns: []Column{{Name: "arr", Type: TypeArray, Nullable: true, ElementType: &elem}}}
			},
			mutate: func(s *Schema) { s.Columns[0].ElementType.Type = TypeString },
			rows:   []map[string]any{{"arr": []any{int64(1), int64(2)}}},
		},
		{
			name: "MAP entry ROW field name",
			schema: func() Schema {
				entry := Column{Name: "key_value", Type: TypeRow, Fields: []Column{
					{Name: "key", Type: TypeString},
					{Name: "value", Type: TypeInt64, Nullable: true},
				}}
				return Schema{Columns: []Column{{Name: "m", Type: TypeMap, Nullable: true, ElementType: &entry}}}
			},
			mutate: func(s *Schema) { s.Columns[0].ElementType.Fields[1].Type = TypeString },
			rows:   []map[string]any{{"m": map[string]any{"k": int64(3)}}},
		},
		{
			name: "the Columns slice itself",
			schema: func() Schema {
				return Schema{Columns: []Column{
					{Name: "a", Type: TypeInt64, Nullable: true},
					{Name: "b", Type: TypeInt64, Nullable: true},
				}}
			},
			mutate: func(s *Schema) { s.Columns[0], s.Columns[1] = s.Columns[1], s.Columns[0] },
			rows:   []map[string]any{{"a": int64(1), "b": int64(2)}},
		},
	}

	doors := []struct {
		name  string
		build func(t *testing.T, out *bytes.Buffer, s Schema) (write func([]map[string]any) error, close func() error)
	}{
		{"Writer", func(t *testing.T, out *bytes.Buffer, s Schema) (func([]map[string]any) error, func() error) {
			w, err := NewWriter(out, s, DefaultWriterConfig())
			if err != nil {
				t.Fatal(err)
			}
			return w.WriteRows, w.Close
		}},
		{"NativeWriter", func(t *testing.T, out *bytes.Buffer, s Schema) (func([]map[string]any) error, func() error) {
			w := NewNativeWriter(out, s, DefaultWriterConfig())
			return w.WriteMapRows, w.Close
		}},
	}

	for _, c := range cases {
		for _, door := range doors {
			t.Run(c.name+"/"+door.name, func(t *testing.T) {
				// The reference run: nobody touches the schema.
				var refBuf bytes.Buffer
				refSchema := c.schema()
				write, closeIt := door.build(t, &refBuf, refSchema)
				if err := write(cloneRows(c.rows)); err != nil {
					t.Fatalf("reference write: %v", err)
				}
				if err := closeIt(); err != nil {
					t.Fatalf("reference Close: %v", err)
				}

				// The subject run: the caller amends its schema the instant
				// the constructor returns.
				var gotBuf bytes.Buffer
				subject := c.schema()
				write, closeIt = door.build(t, &gotBuf, subject)
				c.mutate(&subject)
				if err := write(cloneRows(c.rows)); err != nil {
					t.Fatalf("write after the caller mutated its schema: %v", err)
				}
				if err := closeIt(); err != nil {
					t.Fatalf("Close after the caller mutated its schema: %v", err)
				}

				if !bytes.Equal(gotBuf.Bytes(), refBuf.Bytes()) {
					t.Fatalf("mutating the caller's schema changed the file: %d bytes vs %d — the writer "+
						"is still reading the caller's memory (#973)", gotBuf.Len(), refBuf.Len())
				}

				// And the file is the one the construction-time schema
				// describes: readable here, readable there, same values.
				r, err := NewReaderFromBytes(gotBuf.Bytes())
				if err != nil {
					t.Fatalf("reopening the file: %v", err)
				}
				got, err := r.ReadRows(nil)
				if err != nil {
					t.Fatalf("ReadRows: %v", err)
				}
				refReader, err := NewReaderFromBytes(refBuf.Bytes())
				if err != nil {
					t.Fatalf("reopening the reference file: %v", err)
				}
				want, err := refReader.ReadRows(nil)
				if err != nil {
					t.Fatalf("reference ReadRows: %v", err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("values differ after the mutation:\n got %v\nwant %v", got, want)
				}
				mustPyArrowRead(t, gotBuf.Bytes(), len(c.rows), "the file written beside a mutated schema")
			})
		}
	}
}

// cloneRows gives each run its own maps: Writer.WriteRows rewrites the caller's
// values in place for the network and temporal types.
func cloneRows(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(r))
		for k, v := range r {
			m[k] = v
		}
		out[i] = m
	}
	return out
}

// Column.Clone shares nothing with its receiver — the property the writer's
// ownership rests on, asserted directly so a new pointer field in Column that
// Clone forgets is caught here rather than as a corrupt file.
func TestColumnCloneSharesNothing(t *testing.T) {
	elem := Column{Name: "element", Type: TypeRow, Fields: []Column{
		{Name: "deep", Type: TypeInt64},
	}}
	orig := Schema{Columns: []Column{
		{Name: "arr", Type: TypeArray, Nullable: true, ElementType: &elem},
		{Name: "r", Type: TypeRow, Fields: []Column{{Name: "f", Type: TypeString}}},
		{Name: "flat", Type: TypeInt64},
	}}
	clone := orig.Clone()
	if !reflect.DeepEqual(clone, orig) {
		t.Fatalf("the clone is not equal to the original:\n got %+v\nwant %+v", clone, orig)
	}

	// Every mutable reach of the original, mutated.
	orig.Columns[0].Name = "mutated"
	orig.Columns[0].ElementType.Name = "mutated"
	orig.Columns[0].ElementType.Fields[0].Name = "mutated"
	orig.Columns[1].Fields[0].Name = "mutated"
	orig.Columns[2].Type = TypeString

	if clone.Columns[0].Name != "arr" ||
		clone.Columns[0].ElementType.Name != "element" ||
		clone.Columns[0].ElementType.Fields[0].Name != "deep" ||
		clone.Columns[1].Fields[0].Name != "f" ||
		clone.Columns[2].Type != TypeInt64 {
		t.Fatalf("the clone followed the original's mutations: %+v", clone.Columns)
	}
	if clone.Columns[0].ElementType == orig.Columns[0].ElementType {
		t.Fatal("the clone shares the ElementType pointer")
	}
	if len(clone.Columns) > 0 && &clone.Columns[0] == &orig.Columns[0] {
		t.Fatal("the clone shares the Columns backing array")
	}

	// A nil Columns clones to an empty schema, not a panic.
	if got := (Schema{}).Clone(); len(got.Columns) != 0 {
		t.Fatalf("cloning an empty schema gave %+v", got)
	}
}
