package parquet

import (
	"bytes"
	"fmt"
	"testing"
)

// #885: EVERY numeric box ingest.checkType admits, into EVERY numeric leaf
// that admits it, round-trips as its own value. The four cells the issue
// measured stored 0 with no error from WriteRows, Close or the read:
//
//	INT32   <- int8(42)     0
//	INT64   <- uint32(42)   0
//	FLOAT32 <- int64(42)    0
//	FLOAT64 <- int32(42)    0
//
// and they were four of a much larger set, because each converter named three
// or four boxes out of the nine or ten its own boundary admits.
func TestEveryAcceptedNumericBoxRoundTripsAsItsOwnValue(t *testing.T) {
	// The accept-sets are ingest.checkType's. The writer is asked for their
	// UNION, since a caller of the exported writer is not obliged to come
	// through ingest at all.
	boxes := []any{
		int(42), int8(42), int16(42), int32(42), int64(42),
		uint(42), uint8(42), uint16(42), uint32(42), uint64(42),
		float32(42), float64(42),
	}
	for _, colType := range []TypeID{
		TypeInt32, TypePort, TypeProtocol, TypeDate,
		TypeInt64, TypeFloat32, TypeFloat64,
	} {
		col := Column{Name: "c", Type: colType, Nullable: true}
		for _, box := range boxes {
			t.Run(colType.String()+"/"+fmt.Sprintf("%T", box), func(t *testing.T) {
				got := roundTripOneValue(t, col, box)
				if !isFortyTwo(got) {
					t.Fatalf("%s column, %v (%T) written and read back as %v (%T), want 42",
						colType, box, box, got, got)
				}
			})
		}
	}
}

func isFortyTwo(v any) bool {
	switch g := v.(type) {
	case int32:
		return g == 42
	case int64:
		return g == 42
	case int:
		return g == 42
	case float32:
		return g == 42
	case float64:
		return g == 42
	}
	return false
}

// A BOOL leaf takes a bool. Its predecessor answered every other box with
// FALSE, a stored value nobody wrote.
func TestABoolLeafRefusesABoxThatIsNotABool(t *testing.T) {
	col := Column{Name: "c", Type: TypeBool, Nullable: true}
	for _, box := range []any{int64(1), "true", 1.0, []byte{1}} {
		if err := writeOneValue(t, col, box); err == nil {
			t.Errorf("writing %v (%T) into a BOOL column succeeded; it is not a bool", box, box)
		}
	}
	for _, want := range []bool{true, false} {
		if got := roundTripOneValue(t, col, want); got != want {
			t.Errorf("BOOL round trip of %v gave %v", want, got)
		}
	}
}

// A BYTE_ARRAY leaf's predecessor answered a box it did not name with a nil
// slice, which the leaf appends as a zero-length value: an empty string or an
// all-zero address, stored with no error.
func TestAByteArrayLeafRefusesABoxItCannotRender(t *testing.T) {
	for _, tc := range []struct {
		colType TypeID
		box     any
	}{
		{TypeString, int64(7)},
		{TypeBytes, 1.5},
		{TypeIPv6, int64(7)},
		{TypeUUID, []byte{1, 2, 3}},     // a three-byte value in a sixteen-byte column
		{TypeIPv6, []byte{10, 0, 0, 1}}, // a v4 address in a v6 column
	} {
		col := Column{Name: "c", Type: tc.colType, Nullable: true}
		if err := writeOneValue(t, col, tc.box); err == nil {
			t.Errorf("writing %v (%T) into a %s column succeeded; it has no value there",
				tc.box, tc.box, tc.colType)
		}
	}
}

// #890: the row writer used to narrow a PORT / PROTOCOL box to an int32 in
// prepareRows, with a bare Go conversion, ON THE CALLER'S MAP, so
// int64(4294967297) was 1 before the native writer saw it and the leaf's own
// range check had nothing left to refuse. The value must be refused, and the
// caller's map must come back unchanged.
func TestTheRowWriterDoesNotNarrowBeforeItChecks(t *testing.T) {
	for _, colType := range []TypeID{TypeInt32, TypePort, TypeProtocol} {
		schema := Schema{Columns: []Column{{Name: "x", Type: colType, Nullable: true}}}
		rows := []map[string]any{{"x": int64(4294967297)}}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, schema, DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		werr := w.WriteRows(rows)
		if werr == nil {
			werr = w.Close()
		}
		if werr == nil {
			r, rerr := NewReaderFromBytes(buf.Bytes())
			if rerr != nil {
				t.Fatal(rerr)
			}
			got, rerr := r.ReadRows(nil)
			t.Fatalf("%s: writing int64(4294967297) succeeded and read back as %v (%v); "+
				"no int32 holds that number", colType, got, rerr)
		}
		if got := rows[0]["x"]; got != int64(4294967297) {
			t.Errorf("%s: the caller's row was mutated to %v (%T) before the value was checked",
				colType, got, got)
		}
	}
}
