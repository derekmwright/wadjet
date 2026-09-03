package parquet

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

// #589: the footer's declared-schema overlay used to stop at the top level.
//
// Parquet cannot annotate eight of wadjet's types — IPv4, IPv6, MAC, UUID,
// BYTES, PORT, PROTOCOL, DURATION — and spells the ninth, CIDR, as plain UTF8
// text, so all nine survive a round trip only because the writer stamps the
// declared schema into the footer under DeclaredSchemaKey and the reader
// overlays it back over the bare parquet inference.
//
// The overlay walked the top-level columns and stopped. Its own comment said
// containers "round trip through parquet's own annotations", which is true of
// the LIST/MAP/STRUCT structure and false of the leaf TYPES beneath it: those
// are the same nine, one level down, with the same absent annotations. So a
// nested IPv6 recovered as TypeString, decodePresentValues took its
// string arm, and sixteen intact bytes arrived boxed as a Go string.
// batch.Vector.SetValue read that as text, net.ParseIP refused it, and the
// value read back as the EMPTY STRING — a wrong answer, silently, on data
// that was never damaged on disk.
//
// The invariant these tests hold is the one the issue is written around: THE
// SAME VALUE, written to a top-level column and to a leaf inside a container,
// READS BACK THE SAME. That is asserted rather than a hardcoded Go box, so it
// stays true if a type's boxing ever changes.

func ndsLeaf(name string, t TypeID) Column {
	return Column{Name: name, Type: t, Nullable: true}
}

func ndsArr(name string, elem Column) Column {
	return Column{Name: name, Type: TypeArray, Nullable: true, ElementType: &elem}
}

func ndsMap(name string, val Column) Column {
	val.Name = "value"
	return Column{Name: name, Type: TypeMap, Nullable: true,
		ElementType: &Column{Name: "entry", Type: TypeRow, Fields: []Column{
			{Name: "key", Type: TypeString}, val,
		}}}
}

func ndsRow(name string, fields ...Column) Column {
	return Column{Name: name, Type: TypeRow, Nullable: true, Fields: fields}
}

// ndsDeclaredTypes is the nine the blob exists for, with a value each.
var ndsDeclaredTypes = []struct {
	name string
	typ  TypeID
	val  any
	alt  any
}{
	{"ipv4", TypeIPv4, "9.0.0.1", "9.0.0.2"},
	{"ipv6", TypeIPv6, "2001:db8::9", "2001:db8::a"},
	{"mac", TypeMAC, "aa:bb:cc:dd:ee:09", "aa:bb:cc:dd:ee:0a"},
	{"uuid", TypeUUID, "00000000-0000-4000-8000-000000000009", "00000000-0000-4000-8000-00000000000a"},
	{"bytes", TypeBytes, []byte{0xde, 0xad, 0xbe, 0xef}, []byte{0x00}},
	{"port", TypePort, int32(443), int32(8080)},
	{"proto", TypeProtocol, int32(6), int32(17)},
	{"dur", TypeDuration, int64(1_500_000), int64(-42)},
	{"cidr", TypeCIDR, "9.0.0.0/8", "10.0.0.1/8"},
}

// ndsSchema puts one leaf of type typ at the top level and in every container
// shape: a ROW field, a list element, a map value, and then two and three
// containers deep in each combination the writer can build.
func ndsSchema(typ TypeID) Schema {
	leaf := func(name string) Column { return ndsLeaf(name, typ) }
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		leaf("flat"),
		ndsRow("rw", leaf("v")),
		ndsArr("ar", leaf("element")),
		ndsMap("mp", leaf("value")),
		ndsArr("arow", ndsRow("element", leaf("v"))),
		ndsRow("rarr", ndsArr("l", leaf("element"))),
		ndsMap("mrow", ndsRow("value", leaf("v"))),
		ndsRow("rrow", ndsRow("mid", leaf("v"))),
		ndsRow("deep", ndsArr("l", ndsRow("element", leaf("v")))),
	}}
}

// ndsLeafPaths walks a schema depth-first — the order BuildSchemaTree numbers
// the leaves in — and returns each leaf's dotted path, so an assertion can
// name the exact depth that lost a type and can line a Column up with its
// tree node by position.
func ndsLeafPaths(s Schema) []string {
	var out []string
	var walk func(prefix string, c Column)
	walk = func(prefix string, c Column) {
		name := c.Name
		if prefix != "" {
			name = prefix + "." + c.Name
		}
		switch c.Type {
		case TypeArray, TypeMap:
			if c.ElementType != nil {
				walk(name, *c.ElementType)
			}
		case TypeRow:
			for _, f := range c.Fields {
				walk(name, f)
			}
		default:
			out = append(out, name)
		}
	}
	for _, c := range s.Columns {
		walk("", c)
	}
	return out
}

// ndsLeafTypes is ndsLeafPaths keyed by path.
func ndsLeafTypes(s Schema) map[string]TypeID {
	paths, flat := ndsLeafPaths(s), s.FlatColumns()
	out := make(map[string]TypeID, len(paths))
	for i, p := range paths {
		if i < len(flat) {
			out[p] = flat[i].Type
		}
	}
	return out
}

func ndsWriteRead(t *testing.T, schema Schema, rows []map[string]any) (Schema, []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatalf("WriteRows: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	raw := buf.Bytes()
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, wrote %d", len(got), len(rows))
	}
	return r.Schema(), got
}

func TestDeclaredTypesSurviveEveryContainerShape(t *testing.T) {
	for _, d := range ndsDeclaredTypes {
		t.Run(d.name, func(t *testing.T) {
			schema := ndsSchema(d.typ)
			v, w := d.val, d.alt
			rows := []map[string]any{
				{ // every position populated, lists holding more than one
					"id": int64(0), "flat": v,
					"rw":   map[string]any{"v": v},
					"ar":   []any{v, w},
					"mp":   map[string]any{"k": v, "k2": w},
					"arow": []any{map[string]any{"v": v}, map[string]any{"v": w}},
					"rarr": map[string]any{"l": []any{v, w}},
					"mrow": map[string]any{"k": map[string]any{"v": v}},
					"rrow": map[string]any{"mid": map[string]any{"v": v}},
					"deep": map[string]any{"l": []any{map[string]any{"v": v}}},
				},
				{ // a NULL leaf inside a present container
					"id": int64(1), "flat": nil,
					"rw":   map[string]any{"v": nil},
					"ar":   []any{nil},
					"mp":   map[string]any{"k": nil},
					"arow": []any{map[string]any{"v": nil}},
					"rarr": map[string]any{"l": []any{nil}},
					"mrow": map[string]any{"k": map[string]any{"v": nil}},
					"rrow": map[string]any{"mid": map[string]any{"v": nil}},
					"deep": map[string]any{"l": []any{map[string]any{"v": nil}}},
				},
				{ // empty containers
					"id": int64(2), "flat": v,
					"rw":   map[string]any{"v": v},
					"ar":   []any{},
					"mp":   map[string]any{},
					"arow": []any{},
					"rarr": map[string]any{"l": []any{}},
					"mrow": map[string]any{},
					"rrow": map[string]any{"mid": map[string]any{"v": v}},
					"deep": map[string]any{"l": []any{}},
				},
				{"id": int64(3)}, // every container NULL
			}

			recovered, got := ndsWriteRead(t, schema, rows)

			// (1) the recovered SCHEMA names the declared type at every depth
			for path, ty := range ndsLeafTypes(recovered) {
				if path == "id" || isNdsKey(path) {
					continue // the MAP keys are declared STRING and stay STRING
				}
				if ty != d.typ {
					t.Errorf("recovered schema: leaf %s is %s, want %s", path, ty, d.typ)
				}
			}

			// (2) the same value reads back the same everywhere it sits.
			// The flat column is the control: it is the position the overlay
			// always covered, so anything that disagrees with it is the
			// container losing the value, not the value being odd.
			flat := got[0]["flat"]
			if flat == nil {
				t.Fatalf("the control itself is NULL: a top-level %s did not round trip", d.typ)
			}
			r0 := got[0]
			eq := func(where string, have any) {
				t.Helper()
				if !reflect.DeepEqual(have, flat) {
					t.Errorf("%s read back %#v, the same value in a top-level column reads back %#v",
						where, have, flat)
				}
			}
			eq("rw.v", ndsField(r0["rw"], "v"))
			eq("ar[0]", ndsElem(r0["ar"], 0))
			eq("mp[k]", ndsKey(r0["mp"], "k"))
			eq("arow[0].v", ndsField(ndsElem(r0["arow"], 0), "v"))
			eq("rarr.l[0]", ndsElem(ndsField(r0["rarr"], "l"), 0))
			eq("mrow[k].v", ndsField(ndsKey(r0["mrow"], "k"), "v"))
			eq("rrow.mid.v", ndsField(ndsField(r0["rrow"], "mid"), "v"))
			eq("deep.l[0].v", ndsField(ndsElem(ndsField(r0["deep"], "l"), 0), "v"))

			// (3) the second element of a multi-valued container is its own
			// value, not a repeat of the first: a decoder that reads the
			// right type but the wrong offsets passes (2) and fails here.
			alt := ndsElem(r0["ar"], 1)
			if reflect.DeepEqual(alt, flat) {
				t.Errorf("ar[1] read back the same value as ar[0] (%#v); the two were written different", alt)
			}

			// (4) NULL and empty containers stay NULL and empty rather than
			// becoming a zero value the retype could have invented.
			if v := ndsField(got[1]["rw"], "v"); v != nil {
				t.Errorf("row 1 rw.v: wrote NULL, read %#v", v)
			}
			if a, ok := got[2]["ar"].([]any); !ok || len(a) != 0 {
				t.Errorf("row 2 ar: wrote an empty list, read %#v", got[2]["ar"])
			}
			for _, name := range []string{"rw", "ar", "mp", "arow", "rarr", "mrow", "rrow", "deep"} {
				if v, ok := got[3][name]; ok && v != nil {
					t.Errorf("row 3 %s: wrote NULL, read %#v", name, v)
				}
			}
		})
	}
}

// isNdsKey reports whether a leaf path is one of the MAP key leaves, which
// are declared STRING and must stay STRING.
func isNdsKey(path string) bool {
	return len(path) > 4 && path[len(path)-4:] == ".key"
}

func ndsField(v any, name string) any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return m[name]
}

func ndsKey(v any, key string) any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	return m[key]
}

func ndsElem(v any, i int) any {
	a, ok := v.([]any)
	if !ok || i >= len(a) {
		return nil
	}
	return a[i]
}

// TestNestedDeclaredTypesNeedTheFooterBlob is the other direction: strip the
// blob and the nested leaves go back to their bare parquet inference, exactly
// as they do at the top level. It is the premise of the legacy-file gate in
// internal/coordinator asserted here at the unit, so a change to the overlay
// that started INVENTING nested types (rather than restoring declared ones)
// fails in this package rather than three layers up.
//
// Its claim NARROWED with #608, and the narrowing is the point: what a
// blob-stripped file cannot do is describe itself. Reading it against the
// CATALOG now does recover those types at every depth — see
// TestCatalogRepairsNestedTypesInAPreV0180File — which is why every read
// below passes a nil schema. The two tests are the two halves of one rule:
// the READER alone cannot recover a lost type, and the catalog can.
func TestNestedDeclaredTypesNeedTheFooterBlob(t *testing.T) {
	schema := ndsSchema(TypeIPv6)
	rows := []map[string]any{{
		"id": int64(0), "flat": "2001:db8::9",
		"rw":   map[string]any{"v": "2001:db8::9"},
		"ar":   []any{"2001:db8::9"},
		"mp":   map[string]any{"k": "2001:db8::9"},
		"arow": []any{map[string]any{"v": "2001:db8::9"}},
		"rarr": map[string]any{"l": []any{"2001:db8::9"}},
		"mrow": map[string]any{"k": map[string]any{"v": "2001:db8::9"}},
		"rrow": map[string]any{"mid": map[string]any{"v": "2001:db8::9"}},
		"deep": map[string]any{"l": []any{map[string]any{"v": "2001:db8::9"}}},
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
	stripped, err := StripDeclaredSchema(buf.Bytes())
	if err != nil {
		t.Fatalf("StripDeclaredSchema: %v", err)
	}
	r, err := NewReader(bytes.NewReader(stripped), int64(len(stripped)))
	if err != nil {
		t.Fatal(err)
	}
	for path, ty := range ndsLeafTypes(r.Schema()) {
		if path == "id" || isNdsKey(path) {
			continue
		}
		if ty != TypeString {
			t.Errorf("without the blob, leaf %s recovered as %s; a 16-byte BYTE_ARRAY "+
				"the file says nothing about is a STRING, at every depth", path, ty)
		}
	}
	got, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	if s, ok := got[0]["flat"].(string); !ok || len(s) != 16 {
		t.Errorf("without the blob the top-level leaf should be the raw 16 bytes as a string, got %#v", got[0]["flat"])
	}
	if s, ok := ndsField(got[0]["rw"], "v").(string); !ok || len(s) != 16 {
		t.Errorf("without the blob a nested leaf should read exactly like the top-level one, got %#v",
			ndsField(got[0]["rw"], "v"))
	}
}

// --- the overlay itself, with a hand-built tree and a hand-built blob ------

// ndsTree builds the schema tree a file with this schema would have, so a
// test can hand overlayDeclaredSchema a tree and a blob that DISAGREE — the
// case a real writer never produces and a corrupt or hostile footer does.
func ndsTree(t *testing.T, schema Schema) (*SchemaNode, []*SchemaNode) {
	t.Helper()
	return BuildSchemaTree(buildSchemaElements(schema))
}

func ndsBlob(t *testing.T, declared Schema) []KeyValue {
	t.Helper()
	raw, err := json.Marshal(declared)
	if err != nil {
		t.Fatal(err)
	}
	return []KeyValue{{Key: DeclaredSchemaKey, Value: string(raw)}}
}

// ndsOverlay runs the overlay the way readerSchema does: the tree is built
// from fileSchema, the blob carries declared.
func ndsOverlay(t *testing.T, fileSchema, declared Schema) Schema {
	t.Helper()
	root, leaves := ndsTree(t, fileSchema)
	return overlayDeclaredSchema(schemaFromTree(root, leaves), root.Children, ndsBlob(t, declared))
}

func TestOverlayDeclaredSchemaNestedEdgeCases(t *testing.T) {
	ipv6 := func(name string) Column { return ndsLeaf(name, TypeIPv6) }
	str := func(name string) Column { return ndsLeaf(name, TypeString) }

	// The file's own shape for every case below: one ROW of two fields, one
	// ARRAY, one MAP.
	file := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		ndsRow("rw", ipv6("a"), str("b")),
		ndsArr("ar", ipv6("element")),
		ndsMap("mp", ipv6("value")),
	}}

	// A leaf's declared type, or -1 when the path is not a leaf of s.
	leafType := func(s Schema, path string) TypeID {
		if ty, ok := ndsLeafTypes(s)[path]; ok {
			return ty
		}
		return TypeID(-1)
	}

	t.Run("the ordinary case is restored at every depth", func(t *testing.T) {
		got := ndsOverlay(t, file, file)
		for _, path := range []string{"rw.a", "ar.element", "mp.entry.value"} {
			if ty := leafType(got, path); ty != TypeIPv6 {
				t.Errorf("%s: got %s, want IPV6", path, ty)
			}
		}
		if ty := leafType(got, "rw.b"); ty != TypeString {
			t.Errorf("rw.b: got %s, want STRING — the blob declared STRING and STRING is what the tree said", ty)
		}
	})

	t.Run("an empty declared schema changes nothing", func(t *testing.T) {
		// Condition 2 already refuses a blob with a different column count,
		// and an EMPTY one is that case at its limit.
		got := ndsOverlay(t, file, Schema{})
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING — an empty blob may not retype anything", ty)
		}
	})

	t.Run("a declared field the file does not have leaves the subtree alone", func(t *testing.T) {
		declared := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			ndsRow("rw", ipv6("a"), str("b"), ipv6("c")), // "c" is not in the file
			ndsArr("ar", ipv6("element")),
			ndsMap("mp", ipv6("value")),
		}}
		got := ndsOverlay(t, file, declared)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING — the blob describes a ROW this file does not have, "+
				"so the whole subtree keeps the tree's answer", ty)
		}
		if ty := leafType(got, "ar.element"); ty != TypeIPv6 {
			t.Errorf("ar.element: got %s, want IPV6 — one bad subtree may not disarm the others", ty)
		}
	})

	t.Run("a file field the declaration does not have leaves the subtree alone", func(t *testing.T) {
		bigger := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			ndsRow("rw", ipv6("a"), str("b"), ipv6("c")),
			ndsArr("ar", ipv6("element")),
			ndsMap("mp", ipv6("value")),
		}}
		got := ndsOverlay(t, bigger, file) // tree has three ROW fields, blob has two
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING", ty)
		}
		if ty := leafType(got, "rw.c"); ty != TypeString {
			t.Errorf("rw.c: got %s, want STRING", ty)
		}
	})

	t.Run("a field name that differs only in case does not match", func(t *testing.T) {
		declared := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			ndsRow("rw", ipv6("A"), str("b")),
			ndsArr("ar", ipv6("element")),
			ndsMap("mp", ipv6("value")),
		}}
		got := ndsOverlay(t, file, declared)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING — matching is exact, as it is for top-level columns", ty)
		}
	})

	t.Run("a container the blob calls something else is left alone", func(t *testing.T) {
		declared := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			ndsArr("rw", ipv6("element")), // the file's "rw" is a ROW
			ndsArr("ar", ipv6("element")),
			ndsMap("mp", ipv6("value")),
		}}
		got := ndsOverlay(t, file, declared)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING", ty)
		}
	})

	t.Run("a nested type outside the overlay set is refused", func(t *testing.T) {
		// TIMESTAMP is annotated in the file and DECIMAL carries its own
		// precision, so neither is the blob's to install — one level down
		// exactly as at the top (condition 4).
		declared := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			ndsRow("rw", ndsLeaf("a", TypeTimestamp), str("b")),
			ndsArr("ar", ndsLeaf("element", TypeDecimal)),
			ndsMap("mp", ipv6("value")),
		}}
		got := ndsOverlay(t, file, declared)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING — an unannotated BYTE_ARRAY is not a TIMESTAMP "+
				"because a footer says so", ty)
		}
		if ty := leafType(got, "ar.element"); ty != TypeString {
			t.Errorf("ar.element: got %s, want STRING", ty)
		}
	})

	t.Run("a nested declared type whose storage does not match is refused", func(t *testing.T) {
		// Condition 5: IPv4 is INT64 storage and the file's leaf here is a
		// BYTE_ARRAY, so the physical types disagree and the blob is inert.
		declared := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			ndsRow("rw", ndsLeaf("a", TypeIPv4), str("b")),
			ndsArr("ar", ipv6("element")),
			ndsMap("mp", ipv6("value")),
		}}
		got := ndsOverlay(t, file, declared)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING", ty)
		}
	})

	t.Run("a repeated field name matches nothing", func(t *testing.T) {
		dup := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			ndsRow("rw", ipv6("a"), ipv6("a")),
			ndsArr("ar", ipv6("element")),
			ndsMap("mp", ipv6("value")),
		}}
		// Same shape both sides, so the count and the positional names line
		// up; what must not happen is the reader picking one of the two.
		got := ndsOverlay(t, dup, dup)
		for _, path := range []string{"rw.a"} {
			if ty := leafType(got, path); ty != TypeIPv6 {
				t.Errorf("%s: got %s — positional alignment holds even with a duplicate name", path, ty)
			}
		}
	})

	t.Run("a MAP key is retyped like any other leaf", func(t *testing.T) {
		keyed := Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "mp", Type: TypeMap, Nullable: true, ElementType: &Column{
				Name: "entry", Type: TypeRow, Fields: []Column{
					{Name: "key", Type: TypeUUID}, {Name: "value", Type: TypeInt64, Nullable: true},
				}}},
		}}
		got := ndsOverlay(t, keyed, keyed)
		if ty := leafType(got, "mp.entry.key"); ty != TypeUUID {
			t.Errorf("mp.entry.key: got %s, want UUID — the key is a leaf and the blob covers it", ty)
		}
	})

	t.Run("no blob at all changes nothing", func(t *testing.T) {
		root, leaves := ndsTree(t, file)
		got := overlayDeclaredSchema(schemaFromTree(root, leaves), root.Children, nil)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING", ty)
		}
	})

	t.Run("a blob that is not JSON changes nothing", func(t *testing.T) {
		root, leaves := ndsTree(t, file)
		kv := []KeyValue{{Key: DeclaredSchemaKey, Value: "{not json"}}
		got := overlayDeclaredSchema(schemaFromTree(root, leaves), root.Children, kv)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING", ty)
		}
	})

	t.Run("an oversize blob changes nothing", func(t *testing.T) {
		root, leaves := ndsTree(t, file)
		raw, err := json.Marshal(file)
		if err != nil {
			t.Fatal(err)
		}
		pad := make([]byte, maxDeclaredSchemaBytes+1)
		for i := range pad {
			pad[i] = ' '
		}
		kv := []KeyValue{{Key: DeclaredSchemaKey, Value: string(raw) + string(pad)}}
		got := overlayDeclaredSchema(schemaFromTree(root, leaves), root.Children, kv)
		if ty := leafType(got, "rw.a"); ty != TypeString {
			t.Errorf("rw.a: got %s, want STRING", ty)
		}
	})
}

// FuzzOverlayDeclaredSchema fuzzes the footer blob against a FIXED nested
// tree. The blob is attacker-controlled bytes and the overlay now walks it to
// arbitrary depth, so the properties it must hold are worth stating as
// invariants rather than as a list of cases:
//
//  1. it never panics, whatever the blob says;
//  2. it never changes the SHAPE — the set of leaf paths after the overlay is
//     the set before it, so no container gains, loses or renames a leaf;
//  3. it never changes a leaf's STORAGE — a type it installs has the same
//     physical parquet type the leaf already had, which is what keeps a
//     hostile blob from steering the page decoders;
//  4. it only ever installs a type from the overlay set — the eight parquet
//     cannot annotate, plus CIDR over UTF8.
//
// Together those bound a hostile footer to relabelling a column as another
// type with identical storage, at any depth, which is the same bound the
// top-level rule always had.
func FuzzOverlayDeclaredSchema(f *testing.F) {
	file := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		ndsRow("rw", ndsLeaf("a", TypeIPv6), ndsLeaf("b", TypeString),
			ndsRow("mid", ndsLeaf("v", TypeUUID))),
		ndsArr("ar", ndsLeaf("element", TypeIPv6)),
		ndsMap("mp", ndsLeaf("value", TypeBytes)),
		ndsArr("arow", ndsRow("element", ndsLeaf("v", TypeIPv4))),
		ndsLeaf("dec", TypeDecimal),
		{Name: "vec", Type: TypeVector, Nullable: true, Dimension: 4},
	}}
	root, leaves := BuildSchemaTree(buildSchemaElements(file))
	base := schemaFromTree(root, leaves)
	// FlatColumns and BuildSchemaTree walk the same structure in the same
	// order, so leaf i of the tree is path i of the schema.
	basePaths := ndsLeafPaths(base)
	if len(basePaths) != len(leaves) {
		f.Fatalf("the fixture schema flattens to %d leaves and its tree has %d", len(basePaths), len(leaves))
	}
	basePhys := make(map[string]PhysicalType, len(leaves))
	for i, leaf := range leaves {
		basePhys[basePaths[i]] = *leaf.Type
	}
	baseTypes := ndsLeafTypes(base)

	seed := func(s Schema) {
		raw, err := json.Marshal(s)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(raw)
	}
	seed(file)
	seed(Schema{})
	seed(Schema{Columns: []Column{{Name: "id", Type: TypeIPv6}}})
	f.Add([]byte(`{"columns":[{"name":"id","type":2},{"name":"rw","type":19,"fields":[]}]}`))
	f.Add([]byte(`{"columns":null}`))
	f.Add([]byte("not json"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, blob []byte) {
		got := overlayDeclaredSchema(schemaFromTree(root, leaves), root.Children,
			[]KeyValue{{Key: DeclaredSchemaKey, Value: string(blob)}})
		gotTypes := ndsLeafTypes(got)
		if len(gotTypes) != len(baseTypes) {
			t.Fatalf("the overlay changed the leaf set: %d leaves, was %d", len(gotTypes), len(baseTypes))
		}
		for path, was := range baseTypes {
			now, ok := gotTypes[path]
			if !ok {
				t.Fatalf("the overlay lost leaf %s", path)
			}
			if now == was {
				continue
			}
			if !declaredOverlayTypes[now] && !declaredOverlayUTF8Types[now] {
				t.Fatalf("leaf %s became %s, which is not a type the blob may install", path, now)
			}
			if p, ok := basePhys[path]; ok && wadjetTypeToPhysical(now) != p {
				t.Fatalf("leaf %s became %s, whose storage is %v, but the file stores %v",
					path, now, wadjetTypeToPhysical(now), p)
			}
		}
	})
}
