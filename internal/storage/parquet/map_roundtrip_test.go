package parquet

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
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

// --- coverage beyond the one-value-type table above ---

func mapSchemaWithValue(valueType TypeID, mapNullable, valueNullable bool) Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "m", Type: TypeMap, Nullable: mapNullable, ElementType: &Column{
			Name: "entry", Type: TypeRow, Fields: []Column{
				{Name: "key", Type: TypeString},
				{Name: "value", Type: valueType, Nullable: valueNullable},
			},
		}},
	}}
}

// TestMapRoundTripValueTypes: the shape table above only ever carried INT64
// values, so the four physical widths a MAP value can be written at were
// covered by exactly one of them. Each of these writes the same five shapes.
func TestMapRoundTripValueTypes(t *testing.T) {
	cases := []struct {
		name   string
		typ    TypeID
		values []any // one per entry key a, b, c
	}{
		{"int64", TypeInt64, []any{int64(1), int64(-2), int64(1 << 40)}},
		{"string", TypeString, []any{"", "two", "\u00fc\u00f1\u00ee\u00e7\u00f8d\u00e9"}},
		{"float64", TypeFloat64, []any{0.0, -1.5, 3.25}},
		{"bool", TypeBool, []any{true, false, true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := []map[string]any{
				{"id": int64(0), "m": map[string]any{"a": tc.values[0]}},
				{"id": int64(1), "m": map[string]any{"a": tc.values[0], "b": tc.values[1], "c": tc.values[2]}},
				{"id": int64(2), "m": nil},
				{"id": int64(3), "m": map[string]any{}},
				{"id": int64(4), "m": map[string]any{"n": nil}},
				{"id": int64(5), "m": map[string]any{"": tc.values[0]}},
				// Keys are UTF-8 byte strings, and the key column carries
				// them as bytes: a multi-byte key must not be split, and an
				// empty key must not be confused with an absent one.
				{"id": int64(6), "m": map[string]any{
					"\u65e5\u672c\u8a9e":       tc.values[0],
					"\u043a\u043b\u044e\u0447": tc.values[1],
					"emoji \U0001f5dd":         tc.values[2],
					"":                         tc.values[0],
				}},
			}
			got := mapWriteRead(t, mapSchemaWithValue(tc.typ, true, true), rows)
			for i, want := range rows {
				if want["m"] == nil {
					if v, ok := got[i]["m"]; ok && v != nil {
						t.Errorf("row %d: NULL map read back as %#v", i, v)
					}
					continue
				}
				if !reflect.DeepEqual(got[i]["m"], want["m"]) {
					t.Errorf("row %d: map = %#v, want %#v", i, got[i]["m"], want["m"])
				}
			}
		})
	}
}

// TestMapNonNullable: a required MAP has no encoding for NULL — level 0 on
// its key leaf already means "map present, no entries". So the EMPTY map
// must survive the round trip, and a NULL must be refused at the writer
// rather than written as something the file cannot say.
func TestMapNonNullable(t *testing.T) {
	schema := mapSchemaWithValue(TypeInt64, false, true)

	got := mapWriteRead(t, schema, []map[string]any{
		{"id": int64(0), "m": map[string]any{"a": int64(1)}},
		{"id": int64(1), "m": map[string]any{}},
		{"id": int64(2), "m": map[string]any{"b": int64(2), "c": int64(3)}},
	})
	if !reflect.DeepEqual(got[0]["m"], map[string]any{"a": int64(1)}) {
		t.Errorf("row 0: %#v", got[0]["m"])
	}
	m, ok := got[1]["m"].(map[string]any)
	if !ok || len(m) != 0 {
		t.Errorf("row 1: empty map in a required column read back as %#v", got[1]["m"])
	}
	if !reflect.DeepEqual(got[2]["m"], map[string]any{"b": int64(2), "c": int64(3)}) {
		t.Errorf("row 2: %#v", got[2]["m"])
	}

	// The NULL the column cannot hold.
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	err = pw.WriteRows([]map[string]any{{"id": int64(0), "m": nil}})
	if err == nil {
		t.Fatal("writing a NULL into a non-nullable MAP succeeded, want an error")
	}
	if !strings.Contains(err.Error(), `"m"`) {
		t.Errorf("error %q does not name the column", err)
	}
	// The writer stays failed: a half-decomposed row cannot be repaired by
	// a later one, and a file closed over it would be silently short.
	if err := pw.Close(); err == nil {
		t.Error("Close after a failed write succeeded, want the latched error")
	}
}

// TestMapLargeAndMultiRowGroup: one map big enough to cross a page boundary,
// and a file whose maps straddle row groups. Row-group boundaries are where
// the writer resets its level buffers, and the map assembler walks a whole
// row group at a time.
func TestMapLargeAndMultiRowGroup(t *testing.T) {
	t.Run("large_map", func(t *testing.T) {
		big := make(map[string]any, 10000)
		for i := 0; i < 10000; i++ {
			big[fmt.Sprintf("k%05d", i)] = int64(i)
		}
		got := mapWriteRead(t, mapTestSchema(true), []map[string]any{
			{"id": int64(0), "m": big},
			{"id": int64(1), "m": map[string]any{"tail": int64(-1)}},
		})
		gotMap, ok := got[0]["m"].(map[string]any)
		if !ok {
			t.Fatalf("10k-entry map read back as %#v", got[0]["m"])
		}
		if !reflect.DeepEqual(gotMap, big) {
			t.Fatalf("10k-entry map: %d entries read back, want %d", len(gotMap), len(big))
		}
		if !reflect.DeepEqual(got[1]["m"], map[string]any{"tail": int64(-1)}) {
			t.Errorf("row after the large map: %#v", got[1]["m"])
		}
	})

	t.Run("multi_row_group", func(t *testing.T) {
		rows := []map[string]any{
			{"id": int64(0), "m": map[string]any{"a": int64(0)}},
			{"id": int64(1), "m": nil},
			{"id": int64(2), "m": map[string]any{}},
			{"id": int64(3), "m": map[string]any{"b": int64(3), "c": int64(33)}},
			{"id": int64(4), "m": map[string]any{"d": nil}},
		}
		var buf bytes.Buffer
		cfg := DefaultWriterConfig()
		cfg.RowGroupSize = 2
		pw, err := NewWriter(&buf, mapTestSchema(true), cfg)
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
		if n := r.NumRowGroups(); n < 3 {
			t.Fatalf("row groups = %d, want the maps split across at least 3", n)
		}
		got, err := r.ReadRows(nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(rows) {
			t.Fatalf("read %d rows, wrote %d", len(got), len(rows))
		}
		for i, want := range rows {
			if want["m"] == nil {
				if v, ok := got[i]["m"]; ok && v != nil {
					t.Errorf("row %d: NULL map read back as %#v", i, v)
				}
				continue
			}
			if !reflect.DeepEqual(got[i]["m"], want["m"]) {
				t.Errorf("row %d: map = %#v, want %#v", i, got[i]["m"], want["m"])
			}
		}
	})
}

// TestMapWriteIsDeterministic: decomposeMap used to range over the Go map,
// whose iteration order is randomized, so the same rows produced different
// bytes on every write. A file that is not a function of its input cannot be
// compared, hashed or pinned by a golden — including by the layout test
// above, which is only meaningful because this one holds.
func TestMapWriteIsDeterministic(t *testing.T) {
	rows := []map[string]any{
		{"id": int64(0), "m": map[string]any{"delta": int64(4), "alpha": int64(1), "charlie": int64(3), "bravo": int64(2)}},
		{"id": int64(1), "m": map[string]any{"z": nil, "y": int64(25), "x": int64(24)}},
	}
	write := func() []byte {
		var buf bytes.Buffer
		pw, err := NewWriter(&buf, mapTestSchema(true), DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(rows); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	first := write()
	for i := 0; i < 8; i++ {
		if next := write(); !bytes.Equal(first, next) {
			t.Fatalf("write %d produced different bytes (%d vs %d) for identical input", i+2, len(first), len(next))
		}
	}
}

// goldenSingleEntryMap is a one-row file holding {"a": 10} in a nullable
// MAP<STRING, INT64>, captured before the writer changes in the commit that
// made MAP writes deterministic.
//
// It pins the PHYSICAL LAYOUT of the shape that already read back correctly.
// Ordering is the one thing that commit deliberately changed about a MAP's
// bytes, and a single-entry map has only one order — so for this file the
// bytes must be identical, and any other difference is an accident.
//
// The footer's KeyValueMetadata carries one more entry than that commit
// left it with: "wadjet.schema", the declared-schema JSON blob every
// NativeWriter file now stamps into the footer so a reader can restore leaf
// type identity for the types parquet's own schema cannot express (IPv4,
// IPv6, MAC, UUID, Bytes, Port, Protocol, Duration). That is an additive,
// intentional footer change, not a layout regression — everything before
// and after the new key, including the encoded column data, is unchanged.
const goldenSingleEntryMap = "504152311500151015142c15021500150615061c36002808070000000000000018080700" +
	"000000000000000000081c07000000000000001500152215262c15021500150615061c36" +
	"002801611801610000001140020000000200020000000202010000006115001528152c2c" +
	"15021500150615061c360028080a0000000000000018080a00000000000000000000144c" +
	"0200000002000200000002030a000000000000001502196c3500180d7761646a65745f73" +
	"6368656d61150400150425001802696425244cac134011000000350218016d150215024c" +
	"2c000000350418096b65795f76616c75651504150400150c250018036b657925004c1c00" +
	"000015042502180576616c756525244cac1340110000001602191c193c26081c15041925" +
	"00061918026964150216021662166626083c360028080700000000000000180807000000" +
	"00000000000000266e1c150c192500061938016d096b65795f76616c7565036b65791502" +
	"16021658165c266e3c360028016118016100000026ca011c1504192500061938016d096b" +
	"65795f76616c75650576616c756515021602167a167e26ca013c360028080a0000000000" +
	"000018080a0000000000000000000016b402160200192c180e7761646a65742e76657273" +
	"696f6e1805302e312e3000180d7761646a65742e736368656d6118f5017b22636f6c756d" +
	"6e73223a5b7b226e616d65223a226964222c2274797065223a322c226e756c6c61626c65" +
	"223a66616c73657d2c7b226e616d65223a226d222c2274797065223a32302c226e756c6c" +
	"61626c65223a747275652c22656c656d656e745f74797065223a7b226e616d65223a2265" +
	"6e747279222c2274797065223a31392c226e756c6c61626c65223a66616c73652c226669" +
	"656c6473223a5b7b226e616d65223a226b6579222c2274797065223a352c226e756c6c61" +
	"626c65223a66616c73657d2c7b226e616d65223a2276616c7565222c2274797065223a32" +
	"2c226e756c6c61626c65223a747275657d5d7d7d5d7d0018167761646a657420286e6174" +
	"6976652077726974657229005c02000050415231"

func TestMapLayoutUnchanged(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, mapTestSchema(true), DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows([]map[string]any{{"id": int64(7), "m": map[string]any{"a": int64(10)}}}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	want, err := hex.DecodeString(goldenSingleEntryMap)
	if err != nil {
		t.Fatal(err)
	}
	// The footer's created_by carries the WRITER'S VERSION since #456, so it is
	// per-build by design: a golden that pinned it would fail on every release
	// and pass only on the machine that recorded it. Both sides are normalized
	// to one constant, which leaves this test measuring what it says it
	// measures — the MAP's encoded column data and its schema layout.
	got := withCreatedBy(t, buf.Bytes(), goldenCreatedBy)
	want = withCreatedBy(t, want, goldenCreatedBy)
	if !bytes.Equal(got, want) {
		t.Fatalf("single-entry MAP layout changed: %d bytes now, %d before\n now: %s\nwas: %s",
			len(got), len(want), hex.EncodeToString(got), hex.EncodeToString(want))
	}
}

// goldenCreatedBy is the constant every golden-bytes comparison here
// normalizes to. Its value does not matter; that it is FIXED does.
const goldenCreatedBy = "wadjet (golden)"

// withCreatedBy rewrites a parquet file's footer with a fixed created_by,
// through the same footer decode/re-encode StripDeclaredSchema uses. It is the
// tool a byte-for-byte fixture needs now that created_by identifies the build
// that wrote the file (#456).
func withCreatedBy(t *testing.T, data []byte, createdBy string) []byte {
	t.Helper()
	const magic = "PAR1"
	const trailer = 4 + len(magic)
	if len(data) < trailer+len(magic) || !bytes.Equal(data[len(data)-len(magic):], []byte(magic)) {
		t.Fatalf("not a parquet file (%d bytes)", len(data))
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-trailer : len(data)-len(magic)]))
	start := len(data) - trailer - footerLen
	if footerLen <= 0 || start < len(magic) {
		t.Fatalf("footer length %d does not fit a %d-byte file", footerLen, len(data))
	}
	meta, err := DecodeFileMetaData(data[start : len(data)-trailer])
	if err != nil {
		t.Fatalf("decoding footer: %v", err)
	}
	meta.CreatedBy = createdBy
	out := append([]byte{}, data[:start]...)
	footer := EncodeFileMetaData(meta)
	out = append(out, footer...)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(footer)))
	out = append(out, lenBuf[:]...)
	return append(out, magic...)
}

// TestMapAcceptsStorageShapeEntries pins the round trip UPDATE and MERGE
// depend on. batch.Vector.GetValue's TypeMap arm — and therefore
// RecordBatch.RowAt/ToRows — boxes a MAP column as its STORAGE shape: []any
// of {"key": k, "value": v} entry maps, key-sorted, NOT the native
// map[string]any WriteRows otherwise requires (batch.mapEntryRows builds
// that same shape from a native map on the way in). Any row boxed by
// RowAt/ToRows before being handed back to WriteRows — UPDATE's and MERGE's
// re-ingest of a matched row — carried its MAP columns in that shape, and
// decomposeMap's plain map[string]any type assertion failed on it: the value
// silently wrote as NULL, with no error to say a column went missing.
//
// Found chasing #448/#449's DML regression tests, once ReadFileColumnar
// could finally reach a table with a MAP column on the UPDATE path (it had
// always errored earlier, so this shape was never exercised before).
func TestMapAcceptsStorageShapeEntries(t *testing.T) {
	schema := mapTestSchema(true)
	nativeRows := []map[string]any{
		{"id": int64(0), "m": map[string]any{"a": int64(10), "b": int64(20)}},
		{"id": int64(1), "m": map[string]any{}},
		{"id": int64(2), "m": nil},
		{"id": int64(3), "m": map[string]any{"z": nil}},
	}
	// storageShapeRows carries the exact same values as nativeRows, but each
	// MAP value in the []any-of-entries shape GetValue/RowAt/ToRows produce.
	storageShapeRows := []map[string]any{
		{"id": int64(0), "m": []any{
			map[string]any{"key": "a", "value": int64(10)},
			map[string]any{"key": "b", "value": int64(20)},
		}},
		{"id": int64(1), "m": []any{}},
		{"id": int64(2), "m": nil},
		{"id": int64(3), "m": []any{
			map[string]any{"key": "z", "value": nil},
		}},
	}

	native := mapWriteRead(t, schema, nativeRows)
	viaEntries := mapWriteRead(t, schema, storageShapeRows)
	for i := range nativeRows {
		if !reflect.DeepEqual(native[i]["m"], viaEntries[i]["m"]) {
			t.Errorf("row %d: native-shape input read back %#v, storage-shape input read back %#v — must be identical",
				i, native[i]["m"], viaEntries[i]["m"])
		}
	}

	want := []any{
		map[string]any{"a": int64(10), "b": int64(20)},
		map[string]any{},
		nil,
		map[string]any{"z": nil},
	}
	for i, w := range want {
		if w == nil {
			if v, ok := viaEntries[i]["m"]; ok && v != nil {
				t.Errorf("row %d: NULL map read back as %#v", i, v)
			}
			continue
		}
		if !reflect.DeepEqual(viaEntries[i]["m"], w) {
			t.Errorf("row %d: map = %#v, want %#v", i, viaEntries[i]["m"], w)
		}
	}
}

// TestMapLevelSitesAgree pins the asymmetry that #393 left behind. THREE
// places decide the definition levels of a MAP's leaves:
//
//   - flattenColumn, which sets each leaf buffer's maxDefLevel;
//   - buildMapSchemaElements, which writes the repetition types into the
//     footer the reader derives its maxDef from;
//   - decomposeMap, which stamps the level on each entry as it is written.
//
// All three force the key required and the value optional, because the MAP
// schema fixes those regardless of what the caller declared. Two of them did
// so already; decomposeMap used the declaration as-is, so a MAP whose value
// is itself a ROW declared non-nullable had that ROW's fields written one
// level below what the footer describes — the level that says the whole
// value is ABSENT, not that one field is null.
func TestMapLevelSitesAgree(t *testing.T) {
	schema := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "m", Type: TypeMap, Nullable: true, ElementType: &Column{
			Name: "entry", Type: TypeRow, Fields: []Column{
				{Name: "key", Type: TypeString},
				// Declared NON-nullable on purpose: the MAP schema says
				// otherwise and all three sites have to say the same thing.
				{Name: "value", Type: TypeRow, Nullable: false, Fields: []Column{
					{Name: "a", Type: TypeInt64, Nullable: true},
				}},
			},
		}},
	}}

	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, schema, DefaultWriterConfig())

	// Site 1: the leaf buffers.
	writerMaxDef := make(map[string]int32, len(nw.leafBufs))
	for _, lb := range nw.leafBufs {
		writerMaxDef[pathKey(lb.path)] = lb.maxDefLevel
	}

	if err := nw.WriteMapRows([]map[string]any{
		{"id": int64(0), "m": map[string]any{"k": map[string]any{"a": int64(1)}}},
		{"id": int64(1), "m": map[string]any{"k": map[string]any{"a": nil}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := nw.Close(); err != nil {
		t.Fatal(err)
	}

	raw := buf.Bytes()
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	fr := r.FileReader()
	for i, leaf := range fr.Leaves() {
		key := pathKey(leaf.Path)
		// Site 2: the footer.
		if wmd, ok := writerMaxDef[key]; !ok {
			t.Errorf("leaf %s is in the footer but has no leaf buffer", key)
		} else if wmd != int32(leaf.MaxDefLevel) {
			t.Errorf("leaf %s: writer stamps up to def %d, footer declares maxDef %d",
				key, wmd, leaf.MaxDefLevel)
		}

		// Site 3: the levels actually written.
		lcd, err := readLeafColumn(fr, 0, i)
		if err != nil {
			t.Fatalf("leaf %s: %v", key, err)
		}
		present := 0
		for j, def := range lcd.defLevels {
			if def > lcd.maxDef {
				t.Fatalf("leaf %s entry %d written at def %d, above the footer's maxDef %d",
					key, j, def, lcd.maxDef)
			}
			if def == lcd.maxDef {
				present++
			}
		}
		if present != len(lcd.values) {
			t.Fatalf("leaf %s: %d entries at maxDef but %d values decoded", key, present, len(lcd.values))
		}

		// The NULL field of row 1's ROW value: "value present, field a
		// null" is exactly one level below the leaf's maximum. A level of
		// maxDef-2 would be the claim that the whole ROW is absent.
		if key == "m.key_value.value.a" {
			if len(lcd.defLevels) != 2 {
				t.Fatalf("leaf %s: %d entries, want 2", key, len(lcd.defLevels))
			}
			if lcd.defLevels[1] != lcd.maxDef-1 {
				t.Errorf("leaf %s: a NULL field inside a present ROW value written at def %d, want %d (maxDef %d)",
					key, lcd.defLevels[1], lcd.maxDef-1, lcd.maxDef)
			}
		}
	}

	// Reading the row back must not desynchronise or panic. Assembling a
	// nested MAP value is #409 and out of scope here; not crashing is not.
	if _, err := r.ReadRows(nil); err != nil {
		t.Fatalf("ReadRows over a MAP with a ROW value: %v", err)
	}
}
