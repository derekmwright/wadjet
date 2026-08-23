package parquet

import (
	"bytes"
	"reflect"
	"testing"
)

// MAP write/read round trips (#393).
//
// The only MAP coverage this package had was TestRoundTripMap, which asserts
// a row COUNT and never looks at a value — and it happened to declare its
// value field non-nullable, which is the one shape the writer got right.
// Everything else was broken and invisible:
//
//   - a nullable value column double-counted its definition level (once in
//     flattenColumn's MAP arm, once from its own Nullable), so every value
//     was written one level above the file's own maxDefLevel and read back
//     as NULL;
//   - an entry whose VALUE is NULL was written at the "value present" level,
//     which told the reader to consume a value that was never encoded — the
//     level and value streams slid apart and the decoder ran off the page
//     (a slice-bounds panic, on whatever goroutine happened to be reading);
//   - an EMPTY map came back as a NULL map.
//
// The table below is one case per shape, and the assertions are on values.

func mapTestSchema(valueNullable bool) Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "m", Type: TypeMap, Nullable: true, ElementType: &Column{
			Name: "entry", Type: TypeRow, Fields: []Column{
				{Name: "key", Type: TypeString},
				{Name: "value", Type: TypeInt64, Nullable: valueNullable},
			},
		}},
	}}
}

func mapWriteRead(t *testing.T, schema Schema, rows []map[string]any) []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	raw := buf.Bytes()
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, wrote %d", len(got), len(rows))
	}
	return got
}

func TestMapRoundTripEveryShape(t *testing.T) {
	for _, valueNullable := range []bool{true, false} {
		name := "value_required"
		if valueNullable {
			name = "value_nullable"
		}
		t.Run(name, func(t *testing.T) {
			rows := []map[string]any{
				{"id": int64(0), "m": map[string]any{"a": int64(10)}},
				{"id": int64(1), "m": map[string]any{"a": int64(11), "b": int64(21), "c": int64(31)}},
				{"id": int64(2), "m": nil},
				{"id": int64(3), "m": map[string]any{}},
				{"id": int64(4), "m": map[string]any{"z": int64(-1)}},
				{"id": int64(5), "m": map[string]any{"": int64(0)}}, // zero-length key
			}
			if valueNullable {
				rows = append(rows, map[string]any{"id": int64(6), "m": map[string]any{"n": nil}})
			}
			got := mapWriteRead(t, mapTestSchema(valueNullable), rows)
			for i, want := range rows {
				gotID, _ := got[i]["id"].(int64)
				if gotID != want["id"].(int64) {
					t.Fatalf("row %d: id = %v, want %v", i, got[i]["id"], want["id"])
				}
				if want["m"] == nil {
					if v, ok := got[i]["m"]; ok && v != nil {
						t.Errorf("row %d: NULL map read back as %#v", i, v)
					}
					continue
				}
				wantMap := want["m"].(map[string]any)
				gotMap, ok := got[i]["m"].(map[string]any)
				if !ok {
					t.Errorf("row %d: map read back as %#v (%T), want %#v", i, got[i]["m"], got[i]["m"], wantMap)
					continue
				}
				if !reflect.DeepEqual(gotMap, wantMap) {
					t.Errorf("row %d: map = %#v, want %#v", i, gotMap, wantMap)
				}
			}
		})
	}
}

// TestMapEmptyIsNotNull pins the distinction on its own: an empty map and a
// NULL map are different values and reading one as the other is a wrong
// answer, not a formatting detail.
func TestMapEmptyIsNotNull(t *testing.T) {
	got := mapWriteRead(t, mapTestSchema(true), []map[string]any{
		{"id": int64(0), "m": map[string]any{}},
		{"id": int64(1), "m": nil},
	})
	m, ok := got[0]["m"].(map[string]any)
	if !ok || m == nil {
		t.Fatalf("empty map read back as %#v, want an empty map", got[0]["m"])
	}
	if len(m) != 0 {
		t.Fatalf("empty map read back with %d entries", len(m))
	}
	if v, ok := got[1]["m"]; ok && v != nil {
		t.Fatalf("NULL map read back as %#v", v)
	}
}

// TestMapLevelsMatchTheFooter is the direct form of the writer defect: the
// definition levels the writer stamps on a MAP's leaves must be the levels
// the file's own schema declares. When they disagree the reader either drops
// every value (writer high) or invents one (writer low), and both failures
// are silent until a page runs short.
func TestMapLevelsMatchTheFooter(t *testing.T) {
	schema := mapTestSchema(true)
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows([]map[string]any{
		{"id": int64(0), "m": map[string]any{"a": int64(1)}},
		{"id": int64(1), "m": map[string]any{"b": int64(2), "c": int64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	raw := buf.Bytes()
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	for i, leaf := range fr.Leaves() {
		lcd, err := readLeafColumn(fr, 0, i)
		if err != nil {
			t.Fatalf("leaf %v: %v", leaf.Path, err)
		}
		for j, def := range lcd.defLevels {
			if def > lcd.maxDef {
				t.Fatalf("leaf %v entry %d written at def %d, above the file's maxDef %d",
					leaf.Path, j, def, lcd.maxDef)
			}
		}
		// Every present entry must have produced a value. A key or value
		// count short of the level count is the desynchronisation that took
		// the decoder out of bounds.
		present := 0
		for _, def := range lcd.defLevels {
			if def == lcd.maxDef {
				present++
			}
		}
		if present != len(lcd.values) {
			t.Fatalf("leaf %v: %d entries at maxDef but %d values decoded",
				leaf.Path, present, len(lcd.values))
		}
	}
}

// TestReadRowsAsUsesCallerTypes covers the other half of the row reader's
// type fidelity: a parquet file cannot name IPv4/IPv6/MAC/PORT/PROTOCOL/
// DURATION/BYTES/UUID, so decoded by the footer alone an IPv6 came back as a
// 16-byte Go string that Vector.SetValue then stored as NULL. VECTOR is
// annotated but had no arm in either unpack function, so it read as nil.
func TestReadRowsAsUsesCallerTypes(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "c_ipv6", Type: TypeIPv6, Nullable: true},
		{Name: "c_uuid", Type: TypeUUID, Nullable: true},
		{Name: "c_ipv4", Type: TypeIPv4, Nullable: true},
		{Name: "c_mac", Type: TypeMAC, Nullable: true},
		{Name: "c_vec", Type: TypeVector, Nullable: true, Dimension: 4},
	}}
	rows := []map[string]any{{
		"id": int64(0), "c_ipv6": "2001:db8::7", "c_uuid": "00000000-0000-4000-8000-000000000007",
		"c_ipv4": "10.0.0.7", "c_mac": "aa:bb:cc:00:00:07", "c_vec": []float32{1, 2, 3, 4},
	}}
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
	raw := buf.Bytes()
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadRowsAs(schema.Columns, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
	// The bytes-backed types decode to their RAW storage form, which is what
	// the typed vector stores; the display string is GetValue's job. What
	// matters here is the shape and the length: a 16-byte []byte for IPv6
	// and UUID, an int64 for IPv4/MAC, a []float32 for VECTOR.
	if b, ok := got[0]["c_ipv6"].([]byte); !ok || len(b) != 16 {
		t.Errorf("c_ipv6 = %#v (%T), want a 16-byte slice", got[0]["c_ipv6"], got[0]["c_ipv6"])
	}
	if b, ok := got[0]["c_uuid"].([]byte); !ok || len(b) != 16 {
		t.Errorf("c_uuid = %#v (%T), want a 16-byte slice", got[0]["c_uuid"], got[0]["c_uuid"])
	}
	if _, ok := got[0]["c_ipv4"].(int64); !ok {
		t.Errorf("c_ipv4 = %#v (%T), want int64", got[0]["c_ipv4"], got[0]["c_ipv4"])
	}
	if _, ok := got[0]["c_mac"].(int64); !ok {
		t.Errorf("c_mac = %#v (%T), want int64", got[0]["c_mac"], got[0]["c_mac"])
	}
	if v, ok := got[0]["c_vec"].([]float32); !ok || !reflect.DeepEqual(v, []float32{1, 2, 3, 4}) {
		t.Errorf("c_vec = %#v (%T), want []float32{1,2,3,4}", got[0]["c_vec"], got[0]["c_vec"])
	}
}
