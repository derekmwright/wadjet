package scan

import (
	"bytes"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// flatTypes is every non-nested TypeID: the axes of the (catalog, file)
// matrix the native decoder has to survive.
var flatTypes = []pqt.TypeID{
	pqt.TypeBool, pqt.TypeInt32, pqt.TypeInt64, pqt.TypeFloat32, pqt.TypeFloat64,
	pqt.TypeString, pqt.TypeBytes, pqt.TypeTimestamp, pqt.TypeIPv4, pqt.TypeIPv6,
	pqt.TypeCIDR, pqt.TypeMAC, pqt.TypePort, pqt.TypeProtocol, pqt.TypeDuration,
	pqt.TypeUUID, pqt.TypeDate, pqt.TypeDecimal, pqt.TypeVector,
}

// sampleFor is one writable value per flat type, in the shape the writer's
// converters accept.
func sampleFor(id pqt.TypeID) any {
	switch id {
	case pqt.TypeBool:
		return true
	case pqt.TypeInt32, pqt.TypePort, pqt.TypeProtocol:
		return int64(7)
	case pqt.TypeInt64, pqt.TypeTimestamp, pqt.TypeDuration:
		return int64(1_600_000_000_000)
	case pqt.TypeFloat32, pqt.TypeFloat64:
		return 1.5
	case pqt.TypeString, pqt.TypeCIDR:
		return "10.0.0.0/8"
	case pqt.TypeBytes:
		return []byte("0123456789abcdef")
	case pqt.TypeIPv4:
		return "10.1.2.3"
	case pqt.TypeIPv6:
		return "2001:db8::1"
	case pqt.TypeMAC:
		return "00:11:22:33:44:55"
	case pqt.TypeUUID:
		return "550e8400-e29b-41d4-a716-446655440000"
	case pqt.TypeDate:
		return "2021-03-04"
	case pqt.TypeDecimal:
		return 12.34
	case pqt.TypeVector:
		return []float32{1, 2, 3, 4}
	}
	return nil
}

func colFor(id pqt.TypeID) pqt.Column {
	c := pqt.Column{Name: "c", Type: id, Nullable: true}
	switch id {
	case pqt.TypeDecimal:
		c.Precision, c.Scale = 18, 2
	case pqt.TypeVector:
		c.Dimension = 4
	}
	return c
}

func writeOneColumnFile(t *testing.T, id pqt.TypeID) *pqt.FileReader {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{colFor(id)}}
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		t.Fatalf("%s writer: %v", id, err)
	}
	if err := w.WriteRows([]map[string]any{
		{"c": sampleFor(id)}, {"c": nil}, {"c": sampleFor(id)},
	}); err != nil {
		t.Fatalf("%s write: %v", id, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("%s close: %v", id, err)
	}
	raw := buf.Bytes()
	fr, err := pqt.OpenFileReaderFromBytes(raw)
	if err != nil {
		t.Fatalf("%s file reader: %v", id, err)
	}
	return fr
}

// TestNativeTypePairMatrix drives every (catalog, file) pairing over the flat
// types through the native columnar decoder. The property is total: each cell
// either decodes, or comes back as an error naming the column. Not a panic.
//
// Sixteen of the 361 cells used to panic. storageClass had a `default:` arm
// that put DECIMAL, VECTOR and the five bytes-backed types in one class, so
// those pairings "matched", coerce stayed false, and copyNativeDataDirect
// switched on the FILE's type while writing into a vector allocated for the
// CATALOG's: a catalog STRING over a file DECIMAL indexed DecimalData on a
// vector that had only ever allocated a bytes arena. The panic surfaces
// inside the per-column errgroup, so in a worker process it is the worker,
// and the test binary here dies the same way — which is the point of running
// the whole matrix with no recover in sight.
func TestNativeTypePairMatrix(t *testing.T) {
	readers := make(map[pqt.TypeID]*pqt.FileReader, len(flatTypes))
	recovered := make(map[pqt.TypeID]pqt.TypeID, len(flatTypes))
	for _, ft := range flatTypes {
		fr := writeOneColumnFile(t, ft)
		readers[ft] = fr
		recovered[ft] = pqt.TypeIDFromSchemaNode(fr.Leaves()[0])
	}

	for _, ft := range flatTypes {
		fr, rec := readers[ft], recovered[ft]
		for _, ct := range flatTypes {
			ft, ct, rec, fr := ft, ct, rec, fr
			t.Run(ft.String()+"_file/"+ct.String()+"_catalog", func(t *testing.T) {
				want := pqt.StorageClassOf(rec) == pqt.StorageClassOf(ct) ||
					pqt.CoercibleTo(rec, ct)
				b, err := ReadRowGroupNative(fr, 0, []pqt.Column{colFor(ct)}, nil)
				switch {
				case want && err != nil:
					t.Fatalf("file %s (recovered %s) as catalog %s: %v", ft, rec, ct, err)
				case want && b == nil:
					t.Fatalf("file %s as catalog %s: no batch and no error", ft, ct)
				case !want && err == nil:
					t.Fatalf("file %s (recovered %s) decoded as catalog %s with no error", ft, rec, ct)
				}
				if err == nil {
					// Touching every value is what turns a bad copy into an
					// observable failure rather than a quiet one.
					for i := 0; i < b.Len; i++ {
						_ = b.Columns[0].GetValue(i)
					}
				}
			})
		}
	}
}

// TestNativeTypePairMatrixCoversTheKnownPanics pins the specific cells the
// bucketed storageClass got wrong, so a future rule that happens to be total
// but wrong in these three shapes still fails by name.
func TestNativeTypePairMatrixCoversTheKnownPanics(t *testing.T) {
	cases := []struct{ file, catalog pqt.TypeID }{
		{pqt.TypeDecimal, pqt.TypeString}, // slice bounds in the bytes copy
		{pqt.TypeString, pqt.TypeDecimal}, // BulkSet out of range
		{pqt.TypeString, pqt.TypeVector},  // Float32Data indexed by byte offsets
		{pqt.TypeVector, pqt.TypeString},
		{pqt.TypeDecimal, pqt.TypeVector},
		{pqt.TypeVector, pqt.TypeDecimal},
	}
	for _, tc := range cases {
		t.Run(tc.file.String()+"_as_"+tc.catalog.String(), func(t *testing.T) {
			fr := writeOneColumnFile(t, tc.file)
			b, err := ReadRowGroupNative(fr, 0, []pqt.Column{colFor(tc.catalog)}, nil)
			if err == nil {
				t.Fatalf("decoded a %s column as %s: %v", tc.file, tc.catalog, b)
			}
			if b != nil {
				t.Errorf("a batch came back alongside the error")
			}
			if !contains(err.Error(), tc.catalog.String()) {
				t.Errorf("error %q does not name the declared type", err)
			}
		})
	}
}

func contains(s, sub string) bool { return len(sub) == 0 || bytes.Contains([]byte(s), []byte(sub)) }

// TestStorageClassStaysExact guards the relation the matrix rests on: the
// three classes that must never merge again.
func TestStorageClassStaysExact(t *testing.T) {
	bytesBacked := []pqt.TypeID{pqt.TypeString, pqt.TypeBytes, pqt.TypeIPv6, pqt.TypeCIDR, pqt.TypeUUID}
	for _, b := range bytesBacked {
		if storageClass(b) != storageClass(pqt.TypeString) {
			t.Errorf("%s left the bytes storage class", b)
		}
		for _, other := range []pqt.TypeID{pqt.TypeDecimal, pqt.TypeVector} {
			if storageClass(b) == storageClass(other) {
				t.Errorf("%s and %s share a storage class", b, other)
			}
		}
	}
	if storageClass(pqt.TypeDecimal) == storageClass(pqt.TypeVector) {
		t.Error("DECIMAL and VECTOR share a storage class")
	}
	// And the vectors really do differ: a Decimal vector has no bytes arena
	// and a bytes vector has no Int128 array, which is why the copy paths
	// cannot be allowed to disagree about which one they are writing into.
	if len(batch.NewVector(pqt.TypeDecimal, 4).BytesData.Offsets) != 0 {
		t.Error("a DECIMAL vector allocated a bytes arena")
	}
	if len(batch.NewVector(pqt.TypeString, 4).DecimalData.Data) != 0 {
		t.Error("a STRING vector allocated an Int128 array")
	}
}

// TestShortPlainByteArrayPageIsAnError: the PLAIN byte-array fallback walks
// four-byte length prefixes, and both of its bounds tests used to `break`.
// A page that ends mid-value therefore left every remaining row holding
// whatever the pooled vector had in it — poison bytes, or a previous row
// group's strings — and returned nil. The rows after a truncated page are
// not empty, they are unknown.
func TestShortPlainByteArrayPageIsAnError(t *testing.T) {
	// Two values declared: "abc" complete, then a prefix claiming 9 bytes
	// with only two present.
	body := []byte{3, 0, 0, 0, 'a', 'b', 'c', 9, 0, 0, 0, 'x', 'y'}
	cases := []struct {
		name string
		body []byte
		n    int
	}{
		{"value runs off the end", body, 2},
		{"page ends mid-prefix", body[:len(body)-2-2], 2},
		{"no prefix at all for the second value", body[:7], 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := pqt.RawValues(pqt.PhysicalByteArray, tc.body, tc.n)

			vec := batch.NewVector(pqt.TypeString, tc.n)
			if err := copyNativeDataDirect(vec, 0, data, tc.n, pqt.TypeString); err == nil {
				t.Error("the all-present copy accepted a truncated PLAIN page")
			}

			nvec := batch.NewVector(pqt.TypeString, tc.n)
			defLevels := make([]int32, tc.n)
			for i := range defLevels {
				defLevels[i] = 1
			}
			if err := copyNativeDataScatter(nvec, 0, data, defLevels, 1, tc.n, pqt.TypeString); err == nil {
				t.Error("the nullable copy accepted a truncated PLAIN page")
			}
		})
	}

	// A complete page still copies, so the refusal is of the truncation and
	// not of the encoding.
	full := []byte{3, 0, 0, 0, 'a', 'b', 'c', 2, 0, 0, 0, 'x', 'y'}
	vec := batch.NewVector(pqt.TypeString, 2)
	if err := copyNativeDataDirect(vec, 0, pqt.RawValues(pqt.PhysicalByteArray, full, 2), 2, pqt.TypeString); err != nil {
		t.Fatalf("a complete PLAIN page: %v", err)
	}
	if got := vec.BytesData.StringValue(0); got != "abc" {
		t.Errorf("value 0 = %q, want \"abc\"", got)
	}
	if got := vec.BytesData.StringValue(1); got != "xy" {
		t.Errorf("value 1 = %q, want \"xy\"", got)
	}
}

// TestAllThreePlainWalksRefuseATruncatedPage: the PLAIN length-prefixed
// layout is walked in three places — the full decoder (copyNativeDataDirect /
// copyNativeDataScatter), the selection-aware decoder (selCopyRawLengths) and
// the lengths-only decoder (lengthsFromRawPrefixes, for shape-only columns).
// Two of them used to `break` on a page that ends mid-prefix, with a comment
// saying the full decoder tolerated truncation and stopped early. That
// stopped being true when the full decoder was fixed and the comment stayed,
// so the same page was unreadable through one path and read as a column of
// empty strings through the other two.
//
// The body is the reviewer's: one complete value "abc", then a length prefix
// with two of its four bytes present.
func TestAllThreePlainWalksRefuseATruncatedPage(t *testing.T) {
	bodies := map[string][]byte{
		"page ends mid-prefix":     {3, 0, 0, 0, 'a', 'b', 'c', 9, 0},
		"value runs off the end":   {3, 0, 0, 0, 'a', 'b', 'c', 9, 0, 0, 0, 'x', 'y'},
		"no prefix for the second": {3, 0, 0, 0, 'a', 'b', 'c'},
	}
	const n = 2
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			defLevels := []int32{1, 1}

			vec := batch.NewVector(pqt.TypeString, n)
			if err := copyNativeDataDirect(vec, 0, pqt.RawValues(pqt.PhysicalByteArray, body, n),
				n, pqt.TypeString); err == nil {
				t.Error("the full decoder accepted a truncated PLAIN page")
			}

			svec := batch.NewVector(pqt.TypeString, n)
			if err := selCopyRawLengths(svec, []uint32{0, 1}, 0, n, defLevels, 1, true, body); err == nil {
				t.Error("the selection decoder accepted a truncated PLAIN page")
			}

			lvec := batch.NewVector(pqt.TypeString, n)
			if err := lengthsFromRawPrefixes(lvec, 0, n, defLevels, 1, true, body); err == nil {
				t.Error("the lengths decoder accepted a truncated PLAIN page")
			}

			// And with no selection at all, where the walk still has to
			// parse every prefix to find the next value.
			nvec := batch.NewVector(pqt.TypeString, n)
			if err := selCopyRawLengths(nvec, nil, 0, n, defLevels, 1, true, body); err == nil {
				t.Error("the selection decoder accepted a truncated page with nothing selected")
			}
		})
	}

	// A complete page still reads through all three, so what is refused is
	// the truncation and not the encoding.
	full := []byte{3, 0, 0, 0, 'a', 'b', 'c', 2, 0, 0, 0, 'x', 'y'}
	defLevels := []int32{1, 1}
	vec := batch.NewVector(pqt.TypeString, n)
	if err := copyNativeDataDirect(vec, 0, pqt.RawValues(pqt.PhysicalByteArray, full, n), n, pqt.TypeString); err != nil {
		t.Fatalf("full decoder on a complete page: %v", err)
	}
	svec := batch.NewVector(pqt.TypeString, n)
	if err := selCopyRawLengths(svec, []uint32{0, 1}, 0, n, defLevels, 1, true, full); err != nil {
		t.Fatalf("selection decoder on a complete page: %v", err)
	}
	lvec := batch.NewVector(pqt.TypeString, n)
	if err := lengthsFromRawPrefixes(lvec, 0, n, defLevels, 1, true, full); err != nil {
		t.Fatalf("lengths decoder on a complete page: %v", err)
	}
	for i, want := range []string{"abc", "xy"} {
		if got := vec.BytesData.StringValue(i); got != want {
			t.Errorf("full decoder value %d = %q, want %q", i, got, want)
		}
		if got := svec.BytesData.StringValue(i); got != want {
			t.Errorf("selection decoder value %d = %q, want %q", i, got, want)
		}
		if got := lvec.BytesData.LengthAt(i); got != len(want) {
			t.Errorf("lengths decoder value %d has length %d, want %d", i, got, len(want))
		}
	}
}

// TestZeroDimensionVectorColumnIsAnError: a VECTOR(N) column with N <= 0 is
// an invalid catalog entry. Both copy arms used to `break` on it, which
// returns nil having written nothing into a POOLED vector — so the column
// came back holding whatever the previous row group had left there.
func TestZeroDimensionVectorColumnIsAnError(t *testing.T) {
	body := make([]byte, 16) // four float32s, enough for a dim-4 value
	for _, tc := range []struct {
		name string
		run  func(vec *batch.Vector) error
	}{
		{"all present", func(vec *batch.Vector) error {
			return copyNativeDataDirect(vec, 0, pqt.RawValues(pqt.PhysicalFixedLenByteArray, body, 1),
				1, pqt.TypeVector)
		}},
		{"nullable scatter", func(vec *batch.Vector) error {
			return copyNativeDataScatter(vec, 0, pqt.RawValues(pqt.PhysicalFixedLenByteArray, body, 1),
				[]int32{1}, 1, 1, pqt.TypeVector)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := batch.NewVectorVector(1, 0) // dimension 0
			err := tc.run(vec)
			if err == nil {
				t.Fatal("a VECTOR column with dimension 0 decoded without error")
			}
			if !contains(err.Error(), "VECTOR") || !contains(err.Error(), "dimension") {
				t.Errorf("error %q does not name the column's dimension", err)
			}
		})
	}

	// A real dimension still decodes.
	vec := batch.NewVectorVector(1, 4)
	if err := copyNativeDataDirect(vec, 0, pqt.RawValues(pqt.PhysicalFixedLenByteArray, body, 1),
		1, pqt.TypeVector); err != nil {
		t.Fatalf("a dimension-4 VECTOR: %v", err)
	}
}
