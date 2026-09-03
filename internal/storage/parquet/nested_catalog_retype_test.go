package parquet

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

// #608: a file written BEFORE the wadjet.schema footer key existed
// (pre-v0.18.0, #396) has no blob, so the CATALOG is the only place its types
// survive — and retypeFromCatalog was top-level-only by construction, so a
// nested IPv6 or UUID in such a file still read back as "".
//
// #589 taught the FILE-side overlay to recurse through ROW / ARRAY / MAP to
// any depth. This is the catalog-side half, and until it landed the two
// disagreed about what a container can carry (ADR-0018 §8's closing note).
//
// The assertion is EQUALITY with the same file read WITH its blob, over the
// whole nine-shape nesting matrix ndsSchema builds (flat, ROW, ARRAY, MAP,
// ARRAY-of-ROW, ROW-of-ARRAY, MAP-of-ROW, ROW-of-ROW, and a ROW-of-ARRAY-of-ROW
// three deep). Comparing against a hand-written expectation would let a
// half-repaired read pass on the shapes the expectation happened to name;
// comparing against the blob's own answer cannot.
func TestCatalogRepairsNestedTypesInAPreV0180File(t *testing.T) {
	for _, typ := range []TypeID{TypeIPv6, TypeUUID, TypeIPv4, TypeMAC, TypeBytes} {
		t.Run(typ.String(), func(t *testing.T) {
			schema := ndsSchema(typ)
			rows := ncrRows(typ)

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
			withBlob := buf.Bytes()
			stripped, err := StripDeclaredSchema(withBlob)
			if err != nil {
				t.Fatalf("building the pre-v0.18.0 fixture: %v", err)
			}

			wantRows := ncrRead(t, withBlob, nil)       // the file describes itself
			got := ncrRead(t, stripped, schema.Columns) // only the catalog can

			if !reflect.DeepEqual(got, wantRows) {
				t.Errorf("a pre-v0.18.0 file read against the catalog does not match the same "+
					"file read with its footer blob (#608)\n got: %#v\nwant: %#v", got, wantRows)
			}
			// The types each LEAF decodes as, which is the discriminating
			// assertion for every type in the matrix: the row path boxes an
			// IPv4 and a MAC in the same Go type an unannotated INT32/INT64
			// leaf produces, so for those two the VALUES above cannot tell a
			// repair from a no-op — the leaf TYPE always can.
			blobFR := ncrFileReader(t, withBlob)
			strippedFR := ncrFileReader(t, stripped)
			want := ncrLeafTypes(blobFR, nil)                    // the file describes itself
			repaired := ncrLeafTypes(strippedFR, schema.Columns) // only the catalog can
			bare := ncrLeafTypes(strippedFR, nil)                // neither

			if !reflect.DeepEqual(repaired, want) {
				t.Errorf("leaf types after the catalog repair do not match the blob's (#608)\n"+
					" got: %v\nwant: %v", repaired, want)
			}
			if reflect.DeepEqual(bare, want) {
				t.Errorf("the blob-stripped file already recovered these types with NO "+
					"catalog (%v) — the fixture cannot tell a repair from a no-op", bare)
			}
		})
	}
}

// ncrFileReader opens raw parquet bytes as a FileReader.
func ncrFileReader(t *testing.T, data []byte) *FileReader {
	t.Helper()
	fr, err := OpenFileReaderFromBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	return fr
}

// ncrLeafTypes is the type each CONTAINER-NESTED leaf decodes as, keyed by its
// dotted path, as the nested read actually resolves it.
//
// Top-level leaves are excluded because they are not this function's business
// and never were: retypeFromCatalog repairs them (and reports drift by name),
// and the flat arm of the read decodes them through readColumnToAny, which
// never consults these columns. Including them would assert that the nested
// resolver duplicates the top-level one, which is exactly what
// leafColumnsFromCatalog declines to do — the two use different rules, and
// applying the weaker one to a top-level column would be a regression.
func ncrLeafTypes(fr *FileReader, catalog []Column) map[string]TypeID {
	cols := leafColumnsFromCatalog(fr, catalog)
	out := make(map[string]TypeID, len(cols))
	for i, leaf := range fr.Leaves() {
		if len(leaf.Path) < 2 {
			continue
		}
		out[strings.Join(leaf.Path, ".")] = cols[i].Type
	}
	return out
}

// ncrRead reads every row through the row path, with catalog as the declared
// schema (nil for "the file's own").
func ncrRead(t *testing.T, data []byte, catalog []Column) []map[string]any {
	t.Helper()
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadRowsAs(catalog, nil)
	if err != nil {
		t.Fatalf("ReadRowsAs: %v", err)
	}
	return rows
}

// ncrRows fills every shape of ndsSchema with a value of typ, plus the NULL
// and empty-container rows the nesting matrix needs — a repair must not
// invent a value for one.
func ncrRows(typ TypeID) []map[string]any {
	v := ncrValue(typ)
	alt := ncrAltValue(typ)
	return []map[string]any{{
		"id": int64(0), "flat": v,
		"rw":   map[string]any{"v": v},
		"ar":   []any{v, alt},
		"mp":   map[string]any{"k": v},
		"arow": []any{map[string]any{"v": v}},
		"rarr": map[string]any{"l": []any{v}},
		"mrow": map[string]any{"k": map[string]any{"v": v}},
		"rrow": map[string]any{"mid": map[string]any{"v": v}},
		"deep": map[string]any{"l": []any{map[string]any{"v": v}}},
	}, {
		"id": int64(1), "flat": v,
		"rw":   map[string]any{"v": nil},
		"ar":   []any{},
		"mp":   map[string]any{"k": v},
		"arow": []any{map[string]any{"v": v}},
		"rarr": map[string]any{"l": []any{v}},
		"mrow": map[string]any{"k": map[string]any{"v": v}},
		"rrow": map[string]any{"mid": map[string]any{"v": v}},
		"deep": map[string]any{"l": []any{map[string]any{"v": v}}},
	}}
}

func ncrValue(typ TypeID) string {
	switch typ {
	case TypeIPv6:
		return "2001:db8::9"
	case TypeIPv4:
		return "10.0.0.9"
	case TypeUUID:
		return "0192a5c0-1111-7000-8000-000000000009"
	case TypeMAC:
		return "00:11:22:33:44:09"
	}
	return "\x00\x01\x02\x03"
}

func ncrAltValue(typ TypeID) string {
	switch typ {
	case TypeIPv6:
		return "2001:db8::10"
	case TypeIPv4:
		return "10.0.0.10"
	case TypeUUID:
		return "0192a5c0-1111-7000-8000-000000000010"
	case TypeMAC:
		return "00:11:22:33:44:10"
	}
	return "\x04\x05\x06\x07"
}

// TestCatalogNestedRetypeDeclinesWhatItCannotCarry attempts the boundary from
// the other side (protocol rules 10 and 11). The repair substitutes a type
// only where the file's leaf says nothing about one and the storage matches;
// every other pairing keeps the file's answer, which is the rule the FILE-side
// overlay already applies one level up.
func TestCatalogNestedRetypeDeclinesWhatItCannotCarry(t *testing.T) {
	// The file ANNOTATED this leaf as INT64. A catalog claiming IPv6 for it is
	// not a lost annotation, it is drift, and honouring it would decode eight
	// bytes as a sixteen-byte address.
	file := Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64},
		ndsRow("rw", Column{Name: "v", Type: TypeInt64, Nullable: true}),
	}}
	catalog := []Column{
		{Name: "id", Type: TypeInt64},
		ndsRow("rw", Column{Name: "v", Type: TypeIPv6, Nullable: true}),
	}
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, file, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows([]map[string]any{
		{"id": int64(1), "rw": map[string]any{"v": int64(7)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	stripped, err := StripDeclaredSchema(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	rows := ncrRead(t, stripped, catalog)
	if got := ndsField(rows[0]["rw"], "v"); got != int64(7) {
		t.Errorf("rw.v = %#v, want the file's own int64(7): a catalog type over an "+
			"ANNOTATED leaf of different storage is drift, not a lost annotation", got)
	}
}
