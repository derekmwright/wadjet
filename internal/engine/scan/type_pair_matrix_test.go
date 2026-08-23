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
