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
		// The two LOSSLESS WIDENINGS that used to be here — INT64 over a
		// file INT32 and FLOAT64 over a file FLOAT32 — moved to
		// TestReadRowsAsCoercesTheSamePairsTheScanDoes when #440 admitted
		// them. What made them dangerous was the unsafe cast (Values.Int64()
		// over an INT32 page returns a slice twice as long as its backing
		// array), not the pairing; converting per value has no such
		// question, and refusing them stopped a drifted partition compacting
		// at all. TestReadRowsAsWideProbe still runs the crash shape.
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

// TestReadRowsAsWideProbe is the crash probe: enough rows that an INT64 CAST
// over an INT32 page would run well past the end of the page buffer.
//
// Since #440 the pairing is admitted, so the assertion is stronger than an
// error — every one of the 200k values must be the value that was written.
// A cast over the page would answer with adjacent heap, and that is precisely
// what this many rows makes visible: the wrong answer is not a fault, it is
// numbers.
func TestReadRowsAsWideProbe(t *testing.T) {
	const n = 200_000
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"c": int64(i % 1000)}
	}
	r := retypeTestFile(t, Schema{Columns: []Column{{Name: "c", Type: TypeInt32}}}, rows)
	got, err := r.ReadRowsAs([]Column{{Name: "c", Type: TypeInt64}}, nil)
	if err != nil {
		t.Fatalf("reading a 200k-row INT32 column as INT64: %v", err)
	}
	if len(got) != n {
		t.Fatalf("read %d rows, want %d", len(got), n)
	}
	for i, row := range got {
		if want := int64(i % 1000); row["c"] != want {
			t.Fatalf("row %d: c = %#v, want %d — the widening read the wrong bytes", i, row["c"], want)
		}
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
		// The lossless widenings (#440). The row path boxes an INT32 column
		// as int64 and a FLOAT32 column as float64 already, so the values
		// come back unchanged — which is the point: no conversion can lose
		// anything here.
		{"int64_over_int32", TypeInt32, int64(7), TypeInt64, int64(7)},
		{"int64_over_int32_min", TypeInt32, int64(-2147483648), TypeInt64, int64(-2147483648)},
		{"float64_over_float32", TypeFloat32, 1.5, TypeFloat64, 1.5},
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

// TestReadRowsAsRefusesInt32AsDate is the regression pin for #439:
// CoercibleTo used to admit ANY plain INT32 leaf — not only one the file
// annotated as DATE — into a catalog STRING column, and coerceDecoded
// rendered it with FormatDateDays. A file column holding ordinary integers
// (12345) then read back as an ISO date ("2003-10-20") under a STRING
// catalog. Only a leaf the file itself ANNOTATED as DATE carries evidence its
// values are day counts; a bare INT32 does not, and it must refuse by name —
// not fabricate a date, and not (the pre-#439-fix baseline this repo also
// tried) read back "".
func TestReadRowsAsRefusesInt32AsDate(t *testing.T) {
	fileSchema := Schema{Columns: []Column{{Name: "c", Type: TypeInt32, Nullable: true}}}
	r := retypeTestFile(t, fileSchema, []map[string]any{{"c": int64(12345)}, {"c": nil}})

	// The file reads correctly on its own terms.
	rows, err := r.ReadRows(nil)
	if err != nil || rows[0]["c"] != int64(12345) {
		t.Fatalf("ReadRows on the file's own types: %v %#v", err, rows)
	}

	got, err := r.ReadRowsAs([]Column{{Name: "c", Type: TypeString, Nullable: true}}, nil)
	if err == nil {
		t.Fatalf("a bare INT32 column read as STRING succeeded: %#v "+
			"— want an error, not a fabricated date or an empty string", got)
	}
	if got != nil {
		t.Errorf("ReadRowsAs returned %d rows alongside its error", len(got))
	}
	for _, want := range []string{`"c"`, "STRING", "INT32"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}

	// DATE, the one annotation that DOES carry that evidence, still coerces.
	if !CoercibleTo(TypeDate, TypeString) {
		t.Error("CoercibleTo(DATE, STRING) = false — the annotated pairing must survive this fix")
	}
	if CoercibleTo(TypeInt32, TypeString) {
		t.Error("CoercibleTo(INT32, STRING) = true — #439's coercion is back")
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

// flatTypes is every non-nested TypeID. The retype relation is defined over
// all of them, so the tests below enumerate rather than sample: the defect
// this file now guards against was that a WIDER admission rule quietly let in
// eleven pairings nobody listed, and no sampled test would have named them.
var flatTypes = []TypeID{
	TypeBool, TypeInt32, TypeInt64, TypeFloat32, TypeFloat64,
	TypeString, TypeBytes, TypeTimestamp, TypeIPv4, TypeIPv6,
	TypeCIDR, TypeMAC, TypePort, TypeProtocol, TypeDuration,
	TypeUUID, TypeDate, TypeDecimal, TypeVector,
}

// expectedStorageClass is written out by hand, deliberately: asserting
// StorageClassOf against itself would pin nothing. This is the table the
// engine's vector layout actually implies — which typed array a decoded value
// lands in — and a change to either side has to be made in both places.
func expectedStorageClass(t *testing.T, id TypeID) TypeID {
	t.Helper()
	switch id {
	case TypeInt64, TypeTimestamp, TypeIPv4, TypeMAC, TypeDuration:
		return TypeInt64 // Int64Data
	case TypeInt32, TypePort, TypeProtocol, TypeDate:
		return TypeInt32 // Int32Data
	case TypeFloat64:
		return TypeFloat64
	case TypeFloat32:
		return TypeFloat32
	case TypeBool:
		return TypeBool
	case TypeString, TypeBytes, TypeIPv6, TypeCIDR, TypeUUID:
		return TypeString // BytesData
	case TypeDecimal:
		return TypeDecimal // DecimalData ([]Int128)
	case TypeVector:
		return TypeVector // Float32Data, VectorDim wide
	}
	t.Fatalf("no storage class recorded for %s", id)
	return id
}

func TestStorageClassOfHasNoCatchAllBucket(t *testing.T) {
	for _, id := range flatTypes {
		if got, want := StorageClassOf(id), expectedStorageClass(t, id); got != want {
			t.Errorf("StorageClassOf(%s) = %s, want %s", id, got, want)
		}
	}
	// The three that matter most: DECIMAL and VECTOR must NOT share the
	// bytes bucket, which is precisely the lumping that let a STRING page be
	// copied into a DECIMAL vector.
	for _, id := range []TypeID{TypeDecimal, TypeVector} {
		if StorageClassOf(id) == StorageClassOf(TypeString) {
			t.Errorf("%s shares a storage class with STRING", id)
		}
	}
}

// leafFor writes a one-column file of the given type with our own writer and
// hands back the leaf the reader recovers, plus the type it recovers it as.
// Using a REAL file is the point: the annotations the writer emits (or
// declines to emit) are what the admission rule reads.
func leafFor(t *testing.T, id TypeID) (*SchemaNode, TypeID) {
	t.Helper()
	col := Column{Name: "c", Type: id, Nullable: true}
	switch id {
	case TypeDecimal:
		col.Precision, col.Scale = 18, 2
	case TypeVector:
		col.Dimension = 4
	}
	r := retypeTestFile(t, Schema{Columns: []Column{col}}, []map[string]any{{"c": nil}})
	leaves := r.FileReader().Leaves()
	if len(leaves) != 1 {
		t.Fatalf("%s: file has %d leaves, want 1", id, len(leaves))
	}
	return leaves[0], TypeIDFromSchemaNode(leaves[0])
}

// TestRetypeAdmissionMatrix walks every (file, catalog) pairing over the flat
// types and asserts the admission rule holds exactly: a substitution is
// accepted only when the two types land in the same vector arrays, or when
// the pairing is one of the three CoercibleTo conversions.
//
// The regression it exists for: admission was asked of the leaf's PHYSICAL
// type alone, and parquet stores DECIMAL in four different physicals, so
// "can DECIMAL be read from this physical?" answered yes for essentially
// every leaf in the file. Eleven catalog-DECIMAL pairings the previous rule
// refused were let back in, and a STRING column read as DECIMAL(18,2) came
// back as integers made of its letters.
func TestRetypeAdmissionMatrix(t *testing.T) {
	type leafInfo struct {
		node      *SchemaNode
		recovered TypeID
	}
	info := make(map[TypeID]leafInfo, len(flatTypes))
	for _, ft := range flatTypes {
		node, rec := leafFor(t, ft)
		info[ft] = leafInfo{node, rec}
	}

	for _, ft := range flatTypes {
		li := info[ft]
		for _, want := range flatTypes {
			if li.recovered == want {
				continue // nothing to substitute
			}
			wantCol := Column{Name: "c", Type: want}
			if want == TypeVector {
				wantCol.Dimension = 4
			}
			admit := expectedStorageClass(t, li.recovered) == expectedStorageClass(t, want) ||
				CoercibleTo(li.recovered, want)
			// UUID and IPv6 are sixteen bytes; over a narrower
			// FIXED_LEN_BYTE_ARRAY leaf they are refused on width even
			// though the class matches.
			if admit && li.node.Type != nil && *li.node.Type == PhysicalFixedLenByteArray {
				if w := fixedByteWidth(wantCol); w > 0 && int(li.node.TypeLength) != w {
					admit = false
				}
			}
			_, err := retypeFromCatalog(
				[]Column{{Name: "c", Type: li.recovered}},
				[]Column{wantCol},
				[]*SchemaNode{li.node},
			)
			if admit && err != nil {
				t.Errorf("file %s (recovered %s) → catalog %s: refused (%v), want accepted",
					ft, li.recovered, want, err)
			}
			if !admit && err == nil {
				t.Errorf("file %s (recovered %s) → catalog %s: accepted, want refused",
					ft, li.recovered, want)
			}
		}
	}
}

// TestRetypeRefusesEveryCatalogDecimal names the widened class outright: a
// DECIMAL is what the file's ANNOTATION says it is. Over any leaf the
// annotations recover as something else, the catalog cannot make it one —
// same width or not, the values mean something different.
func TestRetypeRefusesEveryCatalogDecimal(t *testing.T) {
	for _, ft := range flatTypes {
		node, recovered := leafFor(t, ft)
		if recovered == TypeDecimal {
			continue // a real decimal leaf; nothing to substitute
		}
		t.Run(ft.String(), func(t *testing.T) {
			_, err := retypeFromCatalog(
				[]Column{{Name: "c", Type: recovered}},
				[]Column{{Name: "c", Type: TypeDecimal, Precision: 18, Scale: 2}},
				[]*SchemaNode{node},
			)
			if err == nil {
				t.Fatalf("catalog DECIMAL over a %s leaf (recovered %s) was accepted", ft, recovered)
			}
			if !strings.Contains(err.Error(), "DECIMAL") {
				t.Errorf("error %q does not name the declared type", err)
			}
		})
	}
}

// TestReadStringColumnAsDecimalIsAnError is the end-to-end shape of the same
// defect. ("hello","world","x") read as DECIMAL(18,2) came back as
// [448378203247 512970878052 120] — the letters of each string reinterpreted
// as a big-endian integer. No fault, no warning, just different numbers.
func TestReadStringColumnAsDecimalIsAnError(t *testing.T) {
	r := retypeTestFile(t,
		Schema{Columns: []Column{{Name: "s", Type: TypeString, Nullable: true}}},
		[]map[string]any{{"s": "hello"}, {"s": "world"}, {"s": "x"}})

	// The file reads correctly on its own terms.
	rows, err := r.ReadRows(nil)
	if err != nil || len(rows) != 3 || rows[0]["s"] != "hello" {
		t.Fatalf("ReadRows on the file's own types: %v %#v", err, rows)
	}

	got, err := r.ReadRowsAs([]Column{{Name: "s", Type: TypeDecimal, Precision: 18, Scale: 2, Nullable: true}}, nil)
	if err == nil {
		t.Fatalf("a STRING column read as DECIMAL(18,2) succeeded: %#v", got)
	}
	for _, want := range []string{`"s"`, "DECIMAL", "STRING"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// TestRetypeChecksByteArrayWidthPerValue: fixedByteWidth only ran when the
// leaf was FIXED_LEN_BYTE_ARRAY, so a catalog UUID over an ordinary
// BYTE_ARRAY column skipped the width check entirely and handed back
// "UUID"s of whatever length the strings happened to be. The footer cannot
// answer the question for a BYTE_ARRAY leaf — entries are individually sized
// — so the check is per value, at decode.
func TestRetypeChecksByteArrayWidthPerValue(t *testing.T) {
	r := retypeTestFile(t,
		Schema{Columns: []Column{{Name: "u", Type: TypeString, Nullable: true}}},
		[]map[string]any{{"u": "not-sixteen-bytes-at-all"}})

	got, err := r.ReadRowsAs([]Column{{Name: "u", Type: TypeUUID, Nullable: true}}, nil)
	if err == nil {
		t.Fatalf("a 24-byte value read as a UUID succeeded: %#v", got)
	}
	if !strings.Contains(err.Error(), "16") {
		t.Errorf("error %q does not name the required width", err)
	}

	// Sixteen bytes is a UUID, and still reads.
	r16 := retypeTestFile(t,
		Schema{Columns: []Column{{Name: "u", Type: TypeString, Nullable: true}}},
		[]map[string]any{{"u": "0123456789abcdef"}})
	if _, err := r16.ReadRowsAs([]Column{{Name: "u", Type: TypeUUID, Nullable: true}}, nil); err != nil {
		t.Fatalf("a 16-byte value read as a UUID: %v", err)
	}
}
