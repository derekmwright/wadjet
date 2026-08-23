package parquet

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// declaredSchemaFixture is one column of every FLAT type the native writer
// accepts, including the nine parquet cannot annotate.
func declaredSchemaFixture() Schema {
	return Schema{
		Columns: []Column{
			{Name: "c_bool", Type: TypeBool},
			{Name: "c_i32", Type: TypeInt32},
			{Name: "c_i64", Type: TypeInt64},
			{Name: "c_f32", Type: TypeFloat32},
			{Name: "c_f64", Type: TypeFloat64},
			{Name: "c_str", Type: TypeString, Nullable: true},
			{Name: "c_bytes", Type: TypeBytes, Nullable: true},
			{Name: "c_ts", Type: TypeTimestamp},
			{Name: "c_ipv4", Type: TypeIPv4},
			{Name: "c_ipv6", Type: TypeIPv6, Nullable: true},
			{Name: "c_cidr", Type: TypeCIDR, Nullable: true},
			{Name: "c_mac", Type: TypeMAC},
			{Name: "c_port", Type: TypePort},
			{Name: "c_proto", Type: TypeProtocol},
			{Name: "c_dur", Type: TypeDuration},
			{Name: "c_uuid", Type: TypeUUID, Nullable: true},
			{Name: "c_date", Type: TypeDate},
			{Name: "c_dec", Type: TypeDecimal, Precision: 18, Scale: 4},
		},
	}
}

func writeDeclaredSchemaFixture(tb testing.TB, schema Schema, rows []map[string]any) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w := NewNativeWriter(&buf, schema, WriterConfig{RowGroupSize: 100, Compression: CompressionSnappy})
	if err := w.WriteMapRows(rows); err != nil {
		tb.Fatalf("WriteMapRows: %v", err)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// TestDeclaredSchemaRoundTrip is the regression test for #396.
//
// Nine of the 22 types have no parquet annotation buildLeafSchemaElement can
// write — IPv4, IPv6, MAC, UUID, Bytes, Port, Protocol, Duration, and the
// STRING/BYTE_ARRAY pair generally — so TypeIDFromSchemaNode read them back
// as INT64 or STRING. Every reader that takes its column types from the
// CATALOG was unaffected; the ones that take them from the FILE (the DAG
// worker's parquet scan among them) rendered 10.0.0.5 as 167772165.
//
// The footer now carries the declared schema and Reader.Schema() restores
// it. The physical layout is unchanged, which the sibling tests below pin.
func TestDeclaredSchemaRoundTrip(t *testing.T) {
	schema := declaredSchemaFixture()
	rows := []map[string]any{{
		"c_bool": true, "c_i32": int32(7), "c_i64": int64(9),
		"c_f32": float32(1.5), "c_f64": 2.5,
		"c_str": "s", "c_bytes": []byte{1, 2, 3}, "c_ts": int64(1700000000000),
		"c_ipv4": "10.0.0.5", "c_ipv6": "2001:db8::5", "c_cidr": "10.0.0.0/8",
		"c_mac": "aa:bb:cc:00:00:05", "c_port": int32(443), "c_proto": int32(6),
		"c_dur": int64(1500), "c_uuid": "00000000-0000-4000-8000-000000000005",
		"c_date": "2021-03-04", "c_dec": "12.3456",
	}}

	fr, err := OpenFileReaderFromBytes(writeDeclaredSchemaFixture(t, schema, rows))
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}
	got := fr.Schema()
	if len(got.Columns) != len(schema.Columns) {
		t.Fatalf("column count: got %d want %d", len(got.Columns), len(schema.Columns))
	}
	for i, want := range schema.Columns {
		g := got.Columns[i]
		if g.Name != want.Name {
			t.Fatalf("column %d: name %q, want %q", i, g.Name, want.Name)
		}
		if g.Type != want.Type {
			t.Errorf("column %q: read back as %s, want %s", want.Name, g.Type, want.Type)
		}
		if want.Type == TypeDecimal && (g.Precision != want.Precision || g.Scale != want.Scale) {
			t.Errorf("column %q: precision/scale %d/%d, want %d/%d",
				want.Name, g.Precision, g.Scale, want.Precision, want.Scale)
		}
	}
}

// TestDeclaredSchemaKeyIsPresent pins that the blob is actually written, and
// that it is a side channel: an unknown KeyValueMetadata key, nothing more.
func TestDeclaredSchemaKeyIsPresent(t *testing.T) {
	schema := declaredSchemaFixture()
	data := writeDeclaredSchemaFixture(t, schema, nil)
	meta, err := ReadFileMetaData(newBytesReaderAt(data), int64(len(data)))
	if err != nil {
		t.Fatalf("ReadFileMetaData: %v", err)
	}
	var raw string
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key == DeclaredSchemaKey {
			raw = kv.Value
		}
	}
	if raw == "" {
		t.Fatal("footer carries no declared schema")
	}
	var decoded Schema
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("declared schema is not valid JSON: %v", err)
	}
	if len(decoded.Columns) != len(schema.Columns) {
		t.Fatalf("declared schema has %d columns, want %d", len(decoded.Columns), len(schema.Columns))
	}
	// The parquet schema tree must be unchanged: a foreign reader that
	// ignores the key still sees the same physical columns it saw before.
	for i, se := range buildSchemaElements(schema) {
		if i >= len(meta.Schema) {
			t.Fatalf("footer schema is shorter than the built one at %d", i)
		}
		if meta.Schema[i].Name != se.Name {
			t.Errorf("schema element %d: name %q, want %q", i, meta.Schema[i].Name, se.Name)
		}
	}
}

// --- The blob as UNTRUSTED INPUT ---
//
// The declared-schema blob is bytes in a file. A reader that lets it choose
// how pages are interpreted has handed the file the power to make the engine
// misread its own data: relabel DECIMAL(18,4) as DECIMAL(12,1) and every
// value is 1000× off; call a TIMESTAMP an IPv4 and an instant renders as a
// dotted quad; call a UTF-8 STRING an IPv6 and every row renders as the empty
// string. The overlay's answer is that an ANNOTATED leaf is immune — the blob
// reaches only the eight types parquet cannot annotate, which is the entire
// reason it exists.

// overlayFixture is a schema with one leaf of each class the overlay has to
// tell apart: an annotated DECIMAL, an annotated TIMESTAMP, an annotated
// STRING, an UNANNOTATED INT64 (the legitimate overlay target) and an
// annotated VECTOR whose dimension drives an allocation.
func overlayFixture() Schema {
	return Schema{
		Columns: []Column{
			{Name: "c_dec", Type: TypeDecimal, Precision: 18, Scale: 4},
			{Name: "c_ts", Type: TypeTimestamp},
			{Name: "c_str", Type: TypeString, Nullable: true},
			{Name: "c_ipv4", Type: TypeIPv4},
			{Name: "c_vec", Type: TypeVector, Dimension: 4},
			{Name: "c_cidr", Type: TypeCIDR, Nullable: true},
		},
	}
}

// overlayTree returns the inferred schema and the top-level schema nodes of a
// REAL file written from the fixture — the same tree a FileReader hands the
// overlay, not a hand-built approximation.
func overlayTree(tb testing.TB) (Schema, []*SchemaNode) {
	tb.Helper()
	data := writeDeclaredSchemaFixture(tb, overlayFixture(), nil)
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		tb.Fatalf("OpenFileReaderFromBytes: %v", err)
	}
	return schemaFromTree(fr.schemaRoot, fr.leaves), fr.schemaRoot.Children
}

// cloneColumns copies the column slice: the overlay writes through pointers
// into it, so every case needs its own.
func cloneColumns(s Schema) Schema {
	return Schema{Columns: append([]Column(nil), s.Columns...)}
}

func declaredBlob(tb testing.TB, s Schema) []KeyValue {
	tb.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		tb.Fatalf("marshal declared schema: %v", err)
	}
	return []KeyValue{{Key: "wadjet.version", Value: "0.1.0"}, {Key: DeclaredSchemaKey, Value: string(raw)}}
}

// colState is the whole per-column outcome the overlay can influence.
type colState struct {
	typ         TypeID
	prec, scale int
	dim         int
	elem        bool
	fields      int
}

func statesOf(s Schema) []colState {
	out := make([]colState, len(s.Columns))
	for i, c := range s.Columns {
		out[i] = colState{c.Type, c.Precision, c.Scale, c.Dimension, c.ElementType != nil, len(c.Fields)}
	}
	return out
}

// TestOverlayDeclaredSchemaAdversarialBlobs is the security regression for
// #396's read side: every blob below is inert except the honest one, and the
// only column any of them can move is the unannotated INT64 leaf.
func TestOverlayDeclaredSchemaAdversarialBlobs(t *testing.T) {
	inferred, nodes := overlayTree(t)

	// What the tree alone says, before any blob.
	inferredState := []colState{
		{typ: TypeDecimal, prec: 18, scale: 4},
		{typ: TypeTimestamp},
		{typ: TypeString},
		{typ: TypeInt64}, // c_ipv4 has no annotation: this is the #396 loss
		{typ: TypeVector, dim: 4},
		{typ: TypeString}, // c_cidr is annotated UTF8, like every STRING
	}
	// What the HONEST blob is allowed to change: the unannotated INT64 leaf,
	// and the NAME of the UTF8 leaf the writer stamped for a CIDR column.
	honestState := append([]colState(nil), inferredState...)
	honestState[3] = colState{typ: TypeIPv4}
	honestState[5] = colState{typ: TypeCIDR}
	// A blob that lines up with the file but lies about ONE column loses that
	// column only: the other honest restorations still happen. Used by the
	// per-column refusal cases below, all of which mutate c_ipv4.
	perColumnRefusal := append([]colState(nil), honestState...)
	perColumnRefusal[3] = colState{typ: TypeInt64}

	mutate := func(f func(s *Schema)) []KeyValue {
		s := cloneColumns(overlayFixture())
		f(&s)
		return declaredBlob(t, s)
	}

	cases := []struct {
		name string
		kv   []KeyValue
		want []colState
	}{
		{
			name: "the honest blob restores only the unannotated leaf",
			kv:   declaredBlob(t, overlayFixture()),
			want: honestState,
		},
		{
			name: "no blob keeps the inferred schema",
			kv:   []KeyValue{{Key: "wadjet.version", Value: "0.1.0"}},
			want: inferredState,
		},
		{
			name: "an annotated DECIMAL cannot be rescaled",
			kv: mutate(func(s *Schema) {
				s.Columns[0].Precision, s.Columns[0].Scale = 12, 1
			}),
			want: honestState,
		},
		{
			name: "an annotated TIMESTAMP cannot be retyped as IPv4",
			kv:   mutate(func(s *Schema) { s.Columns[1].Type = TypeIPv4 }),
			want: honestState,
		},
		{
			name: "an annotated STRING cannot be retyped as IPv6",
			kv:   mutate(func(s *Schema) { s.Columns[2].Type = TypeIPv6 }),
			want: honestState,
		},
		{
			name: "an annotated STRING cannot be retyped as UUID",
			kv:   mutate(func(s *Schema) { s.Columns[2].Type = TypeUUID }),
			want: honestState,
		},
		{
			name: "an annotated STRING cannot be retyped as BYTES",
			kv:   mutate(func(s *Schema) { s.Columns[2].Type = TypeBytes }),
			want: honestState,
		},
		{
			name: "an annotated STRING CAN carry back the name CIDR",
			// The one relabel a UTF8 leaf accepts, and the reason the
			// exception exists at all: same BYTE_ARRAY storage, same text
			// out of Vector.GetValue. A blob that calls a plain STRING
			// column CIDR therefore changes the type NAME and nothing a
			// value passes through — which is why it is allowed where
			// STRING to IPv6 (a 16-byte contract) is not.
			kv: mutate(func(s *Schema) { s.Columns[2].Type = TypeCIDR }),
			want: func() []colState {
				w := append([]colState(nil), honestState...)
				w[2] = colState{typ: TypeCIDR}
				return w
			}(),
		},
		{
			name: "an annotated TIMESTAMP cannot be relabelled CIDR either",
			kv:   mutate(func(s *Schema) { s.Columns[1].Type = TypeCIDR }),
			want: honestState,
		},
		{
			name: "an annotated VECTOR cannot be given a fabricated dimension",
			kv:   mutate(func(s *Schema) { s.Columns[4].Dimension = 1 << 30 }),
			want: honestState,
		},
		{
			name: "a dimension on an overlay target is not copied",
			kv:   mutate(func(s *Schema) { s.Columns[3].Dimension = 1 << 30 }),
			want: honestState,
		},
		{
			name: "a precision and scale on an overlay target are not copied",
			kv: mutate(func(s *Schema) {
				s.Columns[3].Precision, s.Columns[3].Scale = 12, 1
			}),
			want: honestState,
		},
		{
			name: "DECLARED DECIMAL over an unannotated INT64 leaf is refused",
			kv: mutate(func(s *Schema) {
				s.Columns[3].Type = TypeDecimal
				s.Columns[3].Precision, s.Columns[3].Scale = 12, 1
			}),
			want: perColumnRefusal,
		},
		{
			name: "a negative type id is refused",
			kv:   mutate(func(s *Schema) { s.Columns[3].Type = TypeID(-3) }),
			want: perColumnRefusal,
		},
		{
			name: "a type id past the last type is refused",
			kv:   mutate(func(s *Schema) { s.Columns[3].Type = TypeID(9999) }),
			want: perColumnRefusal,
		},
		{
			name: "a declared type stored differently is refused",
			// IPv6 is BYTE_ARRAY; the c_ipv4 leaf is INT64.
			kv:   mutate(func(s *Schema) { s.Columns[3].Type = TypeIPv6 }),
			want: perColumnRefusal,
		},
		{
			name: "reordered names void the whole blob",
			kv: mutate(func(s *Schema) {
				s.Columns[2], s.Columns[3] = s.Columns[3], s.Columns[2]
			}),
			want: inferredState,
		},
		{
			name: "a short column list voids the whole blob",
			kv:   mutate(func(s *Schema) { s.Columns = s.Columns[:4] }),
			want: inferredState,
		},
		{
			name: "an extra column voids the whole blob",
			kv: mutate(func(s *Schema) {
				s.Columns = append(s.Columns, Column{Name: "c_extra", Type: TypeIPv4})
			}),
			want: inferredState,
		},
		{
			name: "truncated JSON is inert",
			kv:   []KeyValue{{Key: DeclaredSchemaKey, Value: `{"columns":[{"name":"c_dec"`}},
			want: inferredState,
		},
		{
			name: "text that is not JSON is inert",
			kv:   []KeyValue{{Key: DeclaredSchemaKey, Value: "not json at all"}},
			want: inferredState,
		},
		{
			name: "an empty value is inert",
			kv:   []KeyValue{{Key: DeclaredSchemaKey, Value: ""}},
			want: inferredState,
		},
		{
			name: "JSON of another shape is inert",
			kv:   []KeyValue{{Key: DeclaredSchemaKey, Value: `[1,2,3]`}},
			want: inferredState,
		},
		{
			name: "deeply nested JSON is inert and does not panic",
			kv: []KeyValue{{Key: DeclaredSchemaKey, Value: `{"columns":[{"name":"c_dec","element_type":` +
				strings.Repeat(`{"name":"x","element_type":`, 2000) + `null` + strings.Repeat(`}`, 2000) + `}]}`}},
			want: inferredState,
		},
	}

	// An otherwise-honest blob padded past the size cap.
	{
		raw, err := json.Marshal(overlayFixture())
		if err != nil {
			t.Fatal(err)
		}
		padded := string(raw) + strings.Repeat(" ", maxDeclaredSchemaBytes)
		cases = append(cases, struct {
			name string
			kv   []KeyValue
			want []colState
		}{
			name: "an oversized blob is not even decoded",
			kv:   []KeyValue{{Key: DeclaredSchemaKey, Value: padded}},
			want: inferredState,
		})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := overlayDeclaredSchema(cloneColumns(inferred), nodes, tc.kv)
			gotStates := statesOf(got)
			if len(gotStates) != len(tc.want) {
				t.Fatalf("column count %d, want %d", len(gotStates), len(tc.want))
			}
			for i := range tc.want {
				if gotStates[i] != tc.want[i] {
					t.Errorf("column %q: got %+v, want %+v",
						got.Columns[i].Name, gotStates[i], tc.want[i])
				}
			}
			// Names and nullability are never the blob's to change.
			for i := range got.Columns {
				if got.Columns[i].Name != inferred.Columns[i].Name ||
					got.Columns[i].Nullable != inferred.Columns[i].Nullable {
					t.Errorf("column %d: name/nullability moved: %q/%v want %q/%v", i,
						got.Columns[i].Name, got.Columns[i].Nullable,
						inferred.Columns[i].Name, inferred.Columns[i].Nullable)
				}
			}
		})
	}
}

// TestOverlayRestoresTheEightInexpressibleTypes pins the other half of the
// contract: the tightened rule still restores every type it exists for.
func TestOverlayRestoresTheEightInexpressibleTypes(t *testing.T) {
	want := []struct {
		name string
		typ  TypeID
	}{
		{"c_ipv4", TypeIPv4},
		{"c_ipv6", TypeIPv6},
		{"c_mac", TypeMAC},
		{"c_uuid", TypeUUID},
		{"c_bytes", TypeBytes},
		{"c_port", TypePort},
		{"c_proto", TypeProtocol},
		{"c_dur", TypeDuration},
	}
	schema := Schema{}
	for _, w := range want {
		schema.Columns = append(schema.Columns, Column{Name: w.name, Type: w.typ})
	}
	fr, err := OpenFileReaderFromBytes(writeDeclaredSchemaFixture(t, schema, nil))
	if err != nil {
		t.Fatalf("OpenFileReaderFromBytes: %v", err)
	}
	got := fr.Schema()
	for i, w := range want {
		if got.Columns[i].Type != w.typ {
			t.Errorf("column %q: read back as %s, want %s", w.name, got.Columns[i].Type, w.typ)
		}
		if len(declaredOverlayTypes) != len(want) {
			t.Fatalf("declaredOverlayTypes has %d entries, this test pins %d — "+
				"a type was added to or removed from the overlay's reach without updating the gate",
				len(declaredOverlayTypes), len(want))
		}
		// CIDR is the ninth restorable type and rides the UTF8 exception,
		// not this map; if that ever changes the gate should say so.
		if len(declaredOverlayUTF8Types) != 1 || !declaredOverlayUTF8Types[TypeCIDR] {
			t.Fatalf("declaredOverlayUTF8Types is %v, want exactly {CIDR}", declaredOverlayUTF8Types)
		}
		if !declaredOverlayTypes[w.typ] {
			t.Errorf("%s is not in declaredOverlayTypes", w.typ)
		}
	}
}

// FuzzOverlayDeclaredSchema drives the overlay with arbitrary footer bytes
// and asserts the invariants that make the blob safe to read at all: the
// column list keeps its shape, names, nullability, precision, scale and
// dimension are never taken from the blob, and the only type a column can end
// up with is either the one the TREE inferred or one of the eight
// inexpressible types with matching storage on an unannotated leaf.
func FuzzOverlayDeclaredSchema(f *testing.F) {
	inferred, nodes := overlayTree(f)

	honest, err := json.Marshal(overlayFixture())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(string(honest))
	f.Add("")
	f.Add("{")
	f.Add("null")
	f.Add("[1,2,3]")
	f.Add(`{"columns":[]}`)
	f.Add(`{"columns":[{"name":"c_dec","type":8},{"name":"c_ts","type":8},{"name":"c_str","type":9},{"name":"c_ipv4","type":8},{"name":"c_vec","type":21,"dimension":1073741824}]}`)
	f.Add(`{"columns":[{"name":"c_dec","type":-3},{"name":"c_ts","type":-3},{"name":"c_str","type":-3},{"name":"c_ipv4","type":-3},{"name":"c_vec","type":-3}]}`)
	f.Add(`{"columns":[{"name":"c_dec","type":17,"precision":12,"scale":1},{"name":"c_ts","type":17},{"name":"c_str","type":17},{"name":"c_ipv4","type":17},{"name":"c_vec","type":17}]}`)
	f.Add(`{"columns":[{"name":"c_vec","type":21,"dimension":2147483647},{"name":"c_ts"},{"name":"c_str"},{"name":"c_ipv4"},{"name":"c_dec"}]}`)
	f.Add(`{"columns":[{"name":"c_dec","element_type":{"name":"x","type":8}},{"name":"c_ts"},{"name":"c_str"},{"name":"c_ipv4","fields":[{"name":"y","type":8}]},{"name":"c_vec"}]}`)

	f.Fuzz(func(t *testing.T, raw string) {
		got := overlayDeclaredSchema(cloneColumns(inferred), nodes,
			[]KeyValue{{Key: DeclaredSchemaKey, Value: raw}})

		if len(got.Columns) != len(inferred.Columns) {
			t.Fatalf("column count moved: %d, want %d", len(got.Columns), len(inferred.Columns))
		}
		for i := range got.Columns {
			g, in, n := got.Columns[i], inferred.Columns[i], nodes[i]
			if g.Name != in.Name || g.Nullable != in.Nullable {
				t.Fatalf("column %d: name/nullability moved: %q/%v want %q/%v",
					i, g.Name, g.Nullable, in.Name, in.Nullable)
			}
			if g.Precision != in.Precision || g.Scale != in.Scale || g.Dimension != in.Dimension {
				t.Fatalf("column %q: precision/scale/dimension came from the blob: %d/%d/%d want %d/%d/%d",
					g.Name, g.Precision, g.Scale, g.Dimension, in.Precision, in.Scale, in.Dimension)
			}
			if (g.ElementType != nil) != (in.ElementType != nil) || len(g.Fields) != len(in.Fields) {
				t.Fatalf("column %q: nested structure came from the blob", g.Name)
			}
			if g.Type == in.Type {
				continue
			}
			annotated := n.LogicalType != nil || n.ConvertedType != nil
			switch {
			case !annotated && declaredOverlayTypes[g.Type]:
				// The eight types parquet cannot annotate, on a leaf the
				// file said nothing about.
			case annotated && leafIsUTF8String(n) && declaredOverlayUTF8Types[g.Type]:
				// The single exception: CIDR over UTF8 text.
			case annotated:
				t.Fatalf("column %q: an ANNOTATED leaf was retyped to %s", g.Name, g.Type)
			default:
				t.Fatalf("column %q: type became %s, which the overlay may never install", g.Name, g.Type)
			}
			if wadjetTypeToPhysical(g.Type) != *n.Type {
				t.Fatalf("column %q: type %s does not match the leaf's physical type", g.Name, g.Type)
			}
		}
	})
}
