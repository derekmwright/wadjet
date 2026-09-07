package parquet

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// FuzzWriteSchemaShape holds the writer to the arc's whole property over
// arbitrary SCHEMA declarations, which is the input class the six refusals in
// this arc are about: a writer that cannot write a file exactly refuses, and it
// never finalizes a file a reader cannot read.
//
// The other fuzz targets in this package feed the DECODERS untrusted bytes.
// This one feeds the WRITER an untrusted declaration — a Column built in Go,
// which is exactly what bypassed ParseDecimalParams (#969) and
// ValidateWriteSchema (#970) — and asserts the disposition is one of two
// things and never a third:
//
//   - refused, with nothing written; or
//   - finalized, and reopened by this package.
//
// A file that Close returns nil for and NewReaderFromBytes cannot open is the
// failure every issue in this arc reported.
func FuzzWriteSchemaShape(f *testing.F) {
	// One seed per refusal measured at f415faba, plus the legal boundaries
	// next to them, so the corpus keeps exercising both sides.
	seeds := []struct {
		container byte
		leaf      TypeID
		flags     byte
		prec      int32
		scale     int32
		dim       int64
	}{
		{container: 0, leaf: TypeDecimal, flags: 3, prec: 9, scale: -1},   // #969 negative scale
		{container: 0, leaf: TypeDecimal, flags: 3, prec: 4, scale: 9},    // #969 scale > precision
		{container: 0, leaf: TypeDecimal, flags: 3, prec: 0, scale: 40},   // #969 scale > the declared 38
		{container: 0, leaf: TypeDecimal, flags: 3, prec: 50, scale: 2},   // #969 precision > 38
		{container: 0, leaf: TypeDecimal, flags: 3, prec: -3, scale: 2},   // #969 negative precision
		{container: 0, leaf: TypeDecimal, flags: 3, prec: 38, scale: 38},  // legal: scale == precision
		{container: 0, leaf: TypeDecimal, flags: 3, prec: 0, scale: 2},    // legal: the unset sentinel
		{container: 0, leaf: TypeVector, flags: 3, dim: 0},                // #971 no dimension
		{container: 0, leaf: TypeVector, flags: 3, dim: -1},               // #971 negative
		{container: 0, leaf: TypeVector, flags: 3, dim: 536870912},        // #971 MaxInt32/4 + 1
		{container: 0, leaf: TypeVector, flags: 3, dim: 2147483647},       // #971 MaxInt32
		{container: 0, leaf: TypeVector, flags: 3, dim: 1 << 61},          // round-1 B1: wraps int64
		{container: 0, leaf: TypeVector, flags: 3, dim: 1 << 62},          // round-1 B1: wraps to 0
		{container: 0, leaf: TypeVector, flags: 3, dim: (1 << 62) + 1},    // round-1 B1: wraps to 4
		{container: 0, leaf: TypeVector, flags: 3, dim: math.MaxInt64},    // round-1 B1: wraps to -4
		{container: 0, leaf: TypeDecimal, flags: 3, prec: math.MaxInt32},  // round-1 P2: past an int8
		{container: 0, leaf: TypeDecimal, flags: 3, scale: math.MaxInt32}, // round-1 P2: past an int8
		{container: 0, leaf: TypeVector, flags: 3, dim: 536870911},        // legal: the widest width
		{container: 0, leaf: TypeVector, flags: 3, dim: 3},                // legal
		{container: 1, leaf: TypeVector, flags: 3, dim: 536870912},        // #971 inside an ARRAY
		{container: 2, leaf: TypeDecimal, flags: 3, prec: 9, scale: -1},   // #969 inside a ROW
		{container: 3, leaf: TypeDecimal, flags: 3, prec: 50, scale: 2},   // #969 as a MAP value
		{container: 4, leaf: TypeInt64, flags: 3},                         // #970 the malformed MAP
		{container: 0, leaf: TypeInt64, flags: 3},                         // the ordinary case
		{container: 3, leaf: TypeString, flags: 3},                        // a well-formed MAP
	}
	for _, s := range seeds {
		f.Add(fuzzSchemaBytes(s.container, s.leaf, s.flags, s.prec, s.scale, s.dim))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		schema, ok := schemaFromFuzzBytes(data)
		if !ok {
			return
		}
		for _, native := range []bool{false, true} {
			var buf bytes.Buffer
			var write func([]map[string]any) error
			var closeIt func() error
			if native {
				w := NewNativeWriter(&buf, schema, DefaultWriterConfig())
				write, closeIt = w.WriteMapRows, w.Close
			} else {
				w, err := NewWriter(&buf, schema, DefaultWriterConfig())
				if err != nil {
					if buf.Len() != 0 {
						t.Fatalf("NewWriter refused the schema after writing %d bytes: %v", buf.Len(), err)
					}
					continue
				}
				write, closeIt = w.WriteRows, w.Close
			}
			// One all-NULL row: enough to make the writer emit a row group
			// and a chunk per leaf, without depending on a value box. A
			// value-level refusal (a REQUIRED leaf, say) is a legitimate
			// answer and simply ends this case.
			if err := write([]map[string]any{{}}); err != nil {
				continue
			}
			if err := closeIt(); err != nil {
				continue
			}
			r, err := NewReaderFromBytes(buf.Bytes())
			if err != nil {
				t.Fatalf("the writer finalized a %d-byte file it cannot reopen: %v\nschema: %+v",
					buf.Len(), err, schema.Columns)
			}
			if _, err := r.ReadRows(nil); err != nil {
				t.Fatalf("the writer finalized a file whose rows it cannot read: %v\nschema: %+v",
					err, schema.Columns)
			}
			// A FIXED_LEN_BYTE_ARRAY leaf carries no per-value length: the
			// chunk is one run of bytes cut every type_length bytes. A width
			// of zero or less is a file no reader can cut at all — pyarrow
			// says "Invalid FIXED_LEN_BYTE_ARRAY length: 0" — and it is what a
			// narrowed VECTOR dimension produces while Close returns nil
			// (round-1 B1). Asserted here because it is the one property that
			// catches a wrapped width without calling out to pyarrow per
			// execution.
			for i, leaf := range r.FileReader().leaves {
				if leaf.Type != nil && *leaf.Type == PhysicalFixedLenByteArray && leaf.TypeLength <= 0 {
					t.Fatalf("the writer finalized a file whose leaf %d (%s) declares a %d-byte "+
						"fixed width\nschema: %+v", i, leaf.Name, leaf.TypeLength, schema.Columns)
				}
			}
		}
	})
}

// fuzzSchemaBytes is the seed encoder; schemaFromFuzzBytes is its inverse and
// the decoder the fuzzer's own mutations go through.
//
// The field widths are the FULL Go widths, not the byte-sized ones the first
// cut used. That encoding bounded Dimension to an int32 and Precision/Scale to
// an int8 — exactly the three fields this arc's refusals are about — so the
// generator could not reach the overflow class at all and 5.5M executions said
// nothing about it: round-1 B1 (a VECTOR dimension of 2^62 wrapping the
// FIXED_LEN_BYTE_ARRAY type_length) lay outside its range by construction.
func fuzzSchemaBytes(container byte, leaf TypeID, flags byte, prec, scale int32, dim int64) []byte {
	b := make([]byte, 19)
	b[0] = container
	b[1] = byte(leaf)
	b[2] = flags
	binary.BigEndian.PutUint32(b[3:], uint32(prec))
	binary.BigEndian.PutUint32(b[7:], uint32(scale))
	binary.BigEndian.PutUint64(b[11:], uint64(dim))
	return b
}

func schemaFromFuzzBytes(b []byte) (Schema, bool) {
	if len(b) < 19 {
		return Schema{}, false
	}
	leaf := Column{
		Name:      "leaf",
		Type:      TypeID(int(b[1]) % (int(TypeVector) + 1)),
		Nullable:  b[2]&2 != 0,
		Precision: int(int32(binary.BigEndian.Uint32(b[3:7]))),
		Scale:     int(int32(binary.BigEndian.Uint32(b[7:11]))),
		Dimension: int(int64(binary.BigEndian.Uint64(b[11:19]))),
	}
	// A container leaf would need its own subtree; keep the generated leaf
	// primitive and let the container arms below supply the nesting.
	switch leaf.Type {
	case TypeArray, TypeRow, TypeMap:
		leaf.Type = TypeInt64
	}
	outerNullable := b[2]&1 != 0

	var col Column
	switch b[0] % 5 {
	case 0:
		col = leaf
		col.Name = "c"
		col.Nullable = outerNullable
	case 1:
		elem := leaf
		elem.Name = "element"
		col = Column{Name: "c", Type: TypeArray, Nullable: outerNullable, ElementType: &elem}
	case 2:
		col = Column{Name: "c", Type: TypeRow, Nullable: outerNullable, Fields: []Column{leaf}}
	case 3:
		val := leaf
		val.Name = "value"
		col = Column{Name: "c", Type: TypeMap, Nullable: outerNullable, ElementType: &Column{
			Name: "key_value", Type: TypeRow, Fields: []Column{
				{Name: "key", Type: TypeString},
				val,
			},
		}}
	case 4:
		// The malformed MAP spelling of #970: key and value on Fields, no
		// ElementType. It must be refused, never written.
		val := leaf
		val.Name = "value"
		col = Column{Name: "c", Type: TypeMap, Nullable: outerNullable, Fields: []Column{
			{Name: "key", Type: TypeString},
			val,
		}}
	}
	// A second, ordinary column: the malformed-MAP corruption was that the
	// columns AFTER the bad one are misplaced, which needs one to exist.
	return Schema{Columns: []Column{col, {Name: "after", Type: TypeInt64, Nullable: true}}}, true
}
