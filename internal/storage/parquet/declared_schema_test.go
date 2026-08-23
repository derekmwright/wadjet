package parquet

import (
	"bytes"
	"encoding/json"
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

// TestOverlayDeclaredSchemaRejectsMismatch pins the integrity checks: a blob
// that does not line up with the file is inert, not a source of misread
// pages.
func TestOverlayDeclaredSchemaRejectsMismatch(t *testing.T) {
	inferred := Schema{Columns: []Column{
		{Name: "a", Type: TypeInt64},
		{Name: "b", Type: TypeString},
	}}
	blob := func(s Schema) []KeyValue {
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		return []KeyValue{{Key: "wadjet.version", Value: "0.1.0"}, {Key: DeclaredSchemaKey, Value: string(raw)}}
	}

	cases := []struct {
		name string
		kv   []KeyValue
		want []TypeID
	}{
		{
			name: "no blob keeps the inferred schema",
			kv:   []KeyValue{{Key: "wadjet.version", Value: "0.1.0"}},
			want: []TypeID{TypeInt64, TypeString},
		},
		{
			name: "unparseable blob keeps the inferred schema",
			kv:   []KeyValue{{Key: DeclaredSchemaKey, Value: "{not json"}},
			want: []TypeID{TypeInt64, TypeString},
		},
		{
			name: "column count mismatch keeps the inferred schema",
			kv:   blob(Schema{Columns: []Column{{Name: "a", Type: TypeIPv4}}}),
			want: []TypeID{TypeInt64, TypeString},
		},
		{
			name: "name mismatch keeps the inferred schema",
			kv: blob(Schema{Columns: []Column{
				{Name: "z", Type: TypeIPv4}, {Name: "b", Type: TypeUUID},
			}}),
			want: []TypeID{TypeInt64, TypeString},
		},
		{
			name: "a declared type stored differently is ignored per column",
			kv: blob(Schema{Columns: []Column{
				{Name: "a", Type: TypeUUID}, {Name: "b", Type: TypeUUID},
			}}),
			// a: UUID is BYTE_ARRAY, the file column is INT64 → ignored.
			// b: UUID is BYTE_ARRAY like STRING → restored.
			want: []TypeID{TypeInt64, TypeUUID},
		},
		{
			name: "a matching blob is applied",
			kv: blob(Schema{Columns: []Column{
				{Name: "a", Type: TypeIPv4}, {Name: "b", Type: TypeIPv6},
			}}),
			want: []TypeID{TypeIPv4, TypeIPv6},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := Schema{Columns: append([]Column(nil), inferred.Columns...)}
			got := overlayDeclaredSchema(in, tc.kv)
			for i, want := range tc.want {
				if got.Columns[i].Type != want {
					t.Errorf("column %q: got %s, want %s", got.Columns[i].Name, got.Columns[i].Type, want)
				}
			}
		})
	}
}
