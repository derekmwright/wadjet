package parquet

import (
	"bytes"
	"strings"
	"testing"
)

// ReadRowsAs decodes a column as the type the CALLER names, because a
// parquet file cannot annotate eight of this engine's types. That
// substitution is only sound while the two types are carried by the same
// physical bytes. When they are not, the typed accessors in values.go were
// asked for N elements of a width the page does not hold — Values.Int64()
// over an INT32 page returned an unsafe.Slice twice as long as its backing
// array, so a query answered with megabytes of whatever the allocator had
// put next to the page buffer.
//
// The tests below are the two orderings of that mismatch plus a wide probe,
// and they assert an error: a silent skip would answer the query from the
// file's own type instead, which is a different answer than the caller asked
// for, given without saying so.

func retypeTestFile(t *testing.T, schema Schema, rows []map[string]any) *Reader {
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
	return r
}

func TestReadRowsAsRefusesWidthMismatch(t *testing.T) {
	tests := []struct {
		name     string
		fileType TypeID
		value    any
		want     TypeID // what the "catalog" claims
	}{
		// R1: the reviewer's shape — an INT32 column read as INT64 asks for
		// 8 bytes per value out of a page holding 4.
		{"int64_over_int32", TypeInt32, int64(7), TypeInt64},
		// The drift the other way round (INT32 over a file INT64) is NOT
		// here: it is one of the three pairings the native scan converts,
		// so the row path converts it too. See
		// TestReadRowsAsCoercesTheSamePairsTheScanDoes.
		{"float64_over_float32", TypeFloat32, 1.5, TypeFloat64},
		{"float32_over_float64", TypeFloat64, 1.5, TypeFloat32},
		{"int64_over_string", TypeString, "seven", TypeInt64},
		{"string_over_int64", TypeInt64, int64(7), TypeString},
		{"bool_over_int64", TypeInt64, int64(7), TypeBool},
		{"int64_over_float64", TypeFloat64, 1.5, TypeInt64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fileSchema := Schema{Columns: []Column{{Name: "c", Type: tc.fileType, Nullable: true}}}
			rows := make([]map[string]any, 512)
			for i := range rows {
				rows[i] = map[string]any{"c": tc.value}
			}
			r := retypeTestFile(t, fileSchema, rows)

			// The file reads correctly on its own terms.
			if _, err := r.ReadRows(nil); err != nil {
				t.Fatalf("ReadRows on the file's own types: %v", err)
			}

			got, err := r.ReadRowsAs([]Column{{Name: "c", Type: tc.want, Nullable: true}}, nil)
			if err == nil {
				t.Fatalf("ReadRowsAs(%s over %s) succeeded, want an error; read %d rows",
					tc.want, tc.fileType, len(got))
			}
			if got != nil {
				t.Errorf("ReadRowsAs returned %d rows alongside its error", len(got))
			}
			for _, want := range []string{`"c"`, tc.want.String(), tc.fileType.String()} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %s", err, want)
				}
			}
		})
	}
}

// TestReadRowsAsWideProbe is the crash probe: enough rows that an INT64 cast
// over an INT32 page would run well past the end of the page buffer. It must
// come back as an error, not a read of adjacent heap and not a fault.
func TestReadRowsAsWideProbe(t *testing.T) {
	const n = 200_000
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"c": int64(i % 1000)}
	}
	r := retypeTestFile(t, Schema{Columns: []Column{{Name: "c", Type: TypeInt32}}}, rows)
	if _, err := r.ReadRowsAs([]Column{{Name: "c", Type: TypeInt64}}, nil); err == nil {
		t.Fatal("reading a 200k-row INT32 column as INT64 succeeded, want an error")
	}
}

// TestRetypeFromCatalogAcceptsTheEightLossyTypes pins the substitution this
// mechanism exists for: the eight types buildLeafSchemaElement writes with no
// logical annotation, which the footer therefore recovers as a plain
// INT32/INT64/BYTE_ARRAY column. Every one of them has the file's own
// physical type, so every one of them still retypes.
func TestRetypeFromCatalogAcceptsTheEightLossyTypes(t *testing.T) {
	cases := []struct {
		want      TypeID // catalog
		recovered TypeID // what the footer alone gives back
	}{
		{TypeIPv4, TypeInt64},
		{TypeMAC, TypeInt64},
		{TypeDuration, TypeInt64},
		{TypePort, TypeInt32},
		{TypeProtocol, TypeInt32},
		{TypeIPv6, TypeString},
		{TypeBytes, TypeString},
		{TypeUUID, TypeString},
	}
	for _, tc := range cases {
		t.Run(tc.want.String(), func(t *testing.T) {
			out, err := retypeFromCatalog(
				[]Column{{Name: "c", Type: tc.recovered}},
				[]Column{{Name: "c", Type: tc.want}},
				nil,
			)
			if err != nil {
				t.Fatalf("retype %s over %s: %v", tc.want, tc.recovered, err)
			}
			if out[0].Type != tc.want {
				t.Fatalf("column type = %s, want %s", out[0].Type, tc.want)
			}
		})
	}
}

// TestRetypeFromCatalogLeavesNestedAlone: a nested column's read plan comes
// from the file's shape, so substituting a catalog Column there would look up
// leaves that do not exist. Nested pairings are skipped, not rejected.
func TestRetypeFromCatalogLeavesNestedAlone(t *testing.T) {
	out, err := retypeFromCatalog(
		[]Column{{Name: "m", Type: TypeMap}, {Name: "a", Type: TypeArray}},
		[]Column{{Name: "m", Type: TypeString}, {Name: "a", Type: TypeRow}},
		nil,
	)
	if err != nil {
		t.Fatalf("nested columns: %v", err)
	}
	if out[0].Type != TypeMap || out[1].Type != TypeArray {
		t.Fatalf("nested columns were retyped: %v", out)
	}
}

// TestVectorDimensionMismatchIsAnError covers the other silent substitution:
// the element count of a VECTOR comes from the file's fixed byte width, so a
// VECTOR(8) declared over a file storing four floats used to read back as a
// four-element vector. A shorter vector is a different point, not a truncated
// one, and every distance function downstream would answer over the wrong
// number of dimensions.
func TestVectorDimensionMismatchIsAnError(t *testing.T) {
	fileSchema := Schema{Columns: []Column{{Name: "v", Type: TypeVector, Dimension: 4, Nullable: true}}}
	r := retypeTestFile(t, fileSchema, []map[string]any{
		{"v": []float32{1, 2, 3, 4}},
	})

	// Same dimension: fine.
	if _, err := r.ReadRowsAs(fileSchema.Columns, nil); err != nil {
		t.Fatalf("matching dimension: %v", err)
	}

	// The file cannot be re-annotated, so drive the width check the way a
	// drifted catalog would: eight dimensions declared over four stored.
	got, err := readColumnToAny(r.FileReader(), 0, 0, 1, Column{Name: "v", Type: TypeVector, Dimension: 8})
	if err == nil {
		t.Fatalf("VECTOR(8) over a 4-float file succeeded: %#v", got)
	}
	if !strings.Contains(err.Error(), "8") || !strings.Contains(err.Error(), "4") {
		t.Errorf("error %q does not name both dimensions", err)
	}
}

// TestReadRowsAsCoercesTheSamePairsTheScanDoes: three catalog/file pairings
// are not drift but schema evolution the engine already converts on the
// native scan path (copyNativeCoercedDirect). Which path a query takes is
// decided by the SHAPE of the table's schema — one ARRAY/MAP column sends
// every query on that table to the row reader (#393) — so a pairing that
// converts on one path and errors on the other is a divergence waiting for
// a schema change to expose it. parquet.CoercibleTo is the one set both
// paths gate on; this pins the row half of it.
func TestReadRowsAsCoercesTheSamePairsTheScanDoes(t *testing.T) {
	cases := []struct {
		name     string
		fileType TypeID
		value    any
		want     TypeID
		expect   any
	}{
		{"int32_over_int64", TypeInt64, int64(7), TypeInt32, int64(7)},
		{"float64_over_int64", TypeInt64, int64(7), TypeFloat64, float64(7)},
		{"string_over_date", TypeDate, "2021-03-04", TypeString, "2021-03-04"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !CoercibleTo(tc.fileType, tc.want) {
				t.Fatalf("CoercibleTo(%s, %s) = false", tc.fileType, tc.want)
			}
			fileSchema := Schema{Columns: []Column{{Name: "c", Type: tc.fileType, Nullable: true}}}
			r := retypeTestFile(t, fileSchema, []map[string]any{{"c": tc.value}, {"c": nil}})

			rows, err := r.ReadRowsAs([]Column{{Name: "c", Type: tc.want, Nullable: true}}, nil)
			if err != nil {
				t.Fatalf("ReadRowsAs(%s over %s): %v", tc.want, tc.fileType, err)
			}
			if len(rows) != 2 {
				t.Fatalf("read %d rows, want 2", len(rows))
			}
			if got := rows[0]["c"]; got != tc.expect {
				t.Errorf("value = %#v, want %#v", got, tc.expect)
			}
			if _, ok := rows[1]["c"]; ok {
				t.Errorf("the NULL row came back as %#v", rows[1]["c"])
			}
		})
	}
}

// TestPhysicalReadableAsUsesTheFileLeafNotTheWriterMapping is finding 3: the
// guard compared wadjetTypeToPhysical(catalog) against
// wadjetTypeToPhysical(recovered) — OUR WRITER's mapping on both sides. Our
// writer stores DECIMAL as INT64, so a catalog INT64 (or TIMESTAMP, IPv4,
// MAC, DURATION) over a file DECIMAL compared INT64 to INT64 and passed,
// whatever the file actually holds. pyarrow stores DECIMAL(9,2) as INT32.
func TestPhysicalReadableAsUsesTheFileLeafNotTheWriterMapping(t *testing.T) {
	int32Leaf := PhysicalInt32
	leaves := []*SchemaNode{{
		Name: "d", Type: &int32Leaf, LeafIndex: 0,
		LogicalType: &LogicalType{Type: LogicalDecimal, Precision: 9, Scale: 2},
	}}
	for _, want := range []TypeID{TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration} {
		t.Run(want.String(), func(t *testing.T) {
			// The writer mapping agrees with itself and would let this
			// through; the file leaf does not.
			if wadjetTypeToPhysical(want) != wadjetTypeToPhysical(TypeDecimal) {
				t.Fatalf("precondition: the writer mapping already separates %s from DECIMAL", want)
			}
			_, err := retypeFromCatalog(
				[]Column{{Name: "d", Type: TypeDecimal, Precision: 9, Scale: 2}},
				[]Column{{Name: "d", Type: want}},
				leaves,
			)
			if err == nil {
				t.Fatalf("catalog %s over an INT32-backed DECIMAL leaf was accepted", want)
			}
			if !strings.Contains(err.Error(), "INT32") {
				t.Errorf("error %q does not name the file's physical type", err)
			}
		})
	}

	// A DECIMAL leaf that really is INT64 still retypes to the eight lossy
	// types, which is what the mechanism is for.
	int64Leaf := PhysicalInt64
	ok := []*SchemaNode{{Name: "d", Type: &int64Leaf, LeafIndex: 0}}
	if _, err := retypeFromCatalog(
		[]Column{{Name: "d", Type: TypeInt64}},
		[]Column{{Name: "d", Type: TypeIPv4}},
		ok,
	); err != nil {
		t.Errorf("IPv4 over an INT64 leaf: %v", err)
	}
}

// TestRetypeChecksFixedLenByteArrayWidth: a UUID is sixteen bytes by
// definition. A FIXED_LEN_BYTE_ARRAY leaf of another width is not a
// truncated UUID, it is a different value.
func TestRetypeChecksFixedLenByteArrayWidth(t *testing.T) {
	flba := PhysicalFixedLenByteArray
	leaves := []*SchemaNode{{Name: "u", Type: &flba, TypeLength: 8, LeafIndex: 0}}
	if _, err := retypeFromCatalog(
		[]Column{{Name: "u", Type: TypeString}},
		[]Column{{Name: "u", Type: TypeUUID}},
		leaves,
	); err == nil {
		t.Fatal("UUID declared over an 8-byte FIXED_LEN_BYTE_ARRAY leaf was accepted")
	}
	leaves[0].TypeLength = 16
	if _, err := retypeFromCatalog(
		[]Column{{Name: "u", Type: TypeString}},
		[]Column{{Name: "u", Type: TypeUUID}},
		leaves,
	); err != nil {
		t.Fatalf("UUID over a 16-byte leaf: %v", err)
	}
}
