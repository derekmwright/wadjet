package parquet

import (
	"bytes"
	"math"
	"net"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// int32LeafTypes is every column type this writer puts in an INT32 leaf. The
// list is asked of wadjetTypeToPhysical rather than written out, so a
// twenty-third type that lands in an INT32 leaf joins the gate by existing.
func int32LeafTypes(t *testing.T) []TypeID {
	t.Helper()
	var out []TypeID
	for _, id := range []TypeID{
		TypeBool, TypeInt32, TypeInt64, TypeFloat32, TypeFloat64, TypeString,
		TypeBytes, TypeTimestamp, TypeIPv4, TypeIPv6, TypeCIDR, TypeMAC,
		TypePort, TypeProtocol, TypeDuration, TypeUUID, TypeDate, TypeDecimal,
		TypeArray, TypeRow, TypeMap, TypeVector,
	} {
		if wadjetTypeToPhysical(id) == PhysicalInt32 {
			out = append(out, id)
		}
	}
	if len(out) == 0 {
		t.Fatal("no type maps to an INT32 leaf — this gate is asserting nothing")
	}
	return out
}

// A value an INT32 leaf cannot hold FAILS THE WRITE. It is never wrapped, never
// truncated and never replaced by a zero.
//
// Every cell here was measured storing a wrong number before int32LeafValue
// existed: int64(3000000000) round-tripped as -1294967296, math.NaN() as
// -2147483648, 2.5 as 2, and int8/int16/uint8/uint16/uint32 — five boxes
// ingest.checkType ADMITS for these columns — as 0 (alerts #36/#37).
func TestAnInt32LeafRefusesEveryValueItCannotHold(t *testing.T) {
	refused := []struct {
		name string
		box  any
	}{
		{"MaxInt32+1", int64(math.MaxInt32) + 1},
		{"MinInt32-1", int64(math.MinInt32) - 1},
		{"int MaxInt32+1", int(math.MaxInt32) + 1},
		{"1e10", float64(1e10)},
		{"-1e10", float64(-1e10)},
		{"NaN", math.NaN()},
		{"+Inf", math.Inf(1)},
		{"-Inf", math.Inf(-1)},
		{"2.5", float64(2.5)},
		{"float32 2.5", float32(2.5)},
		{"MaxUint64", uint64(math.MaxUint64)},
		{"MaxInt64", int64(math.MaxInt64)},
	}
	accepted := []struct {
		name string
		box  any
		want int32
	}{
		{"MaxInt32", int64(math.MaxInt32), math.MaxInt32},
		{"MinInt32", int64(math.MinInt32), math.MinInt32},
		{"int32 MaxInt32", int32(math.MaxInt32), math.MaxInt32},
		{"zero", int64(0), 0},
		{"int8", int8(5), 5},
		{"int16", int16(-7), -7},
		{"uint8", uint8(9), 9},
		{"uint16", uint16(11), 11},
		{"uint32", uint32(13), 13},
		{"uint", uint(15), 15},
		{"whole float", float64(17), 17},
		{"float at MaxInt32", float64(math.MaxInt32), math.MaxInt32},
	}

	for _, colType := range int32LeafTypes(t) {
		col := Column{Name: "c", Type: colType, Nullable: true}
		for _, tc := range refused {
			t.Run(colType.String()+"/refused/"+tc.name, func(t *testing.T) {
				if err := writeOneValue(t, col, tc.box); err == nil {
					t.Fatalf("writing %v (%T) into a %s column succeeded; "+
						"it has no INT32 and must fail the write", tc.box, tc.box, colType)
				} else if s := sqlerr.StateOf(err); s != "22003" {
					t.Fatalf("writing %v into a %s column: SQLSTATE %q, want 22003 "+
						"(PostgreSQL's numeric_value_out_of_range): %v", tc.box, colType, s, err)
				}
			})
		}
		for _, tc := range accepted {
			t.Run(colType.String()+"/stored/"+tc.name, func(t *testing.T) {
				got := roundTripOneValue(t, col, tc.box)
				if !sameInt32Reading(got, tc.want) {
					t.Fatalf("writing %v (%T) into a %s column read back as %v (%T), want %d",
						tc.box, tc.box, colType, got, got, tc.want)
				}
			})
		}
	}
}

// The INT64 leaf is the same seam one physical type over: uint64 and uint8
// stored 0 there, and a NaN or a 1e30 stored math.MinInt64.
func TestAnInt64LeafRefusesEveryValueItCannotHold(t *testing.T) {
	col := Column{Name: "c", Type: TypeInt64, Nullable: true}
	for _, box := range []any{math.NaN(), math.Inf(1), math.Inf(-1), float64(1e30), float64(2.5), uint64(math.MaxUint64)} {
		if err := writeOneValue(t, col, box); err == nil {
			t.Errorf("writing %v (%T) into an INT64 column succeeded; it has no int64", box, box)
		} else if s := sqlerr.StateOf(err); s != "22003" {
			t.Errorf("writing %v into an INT64 column: SQLSTATE %q, want 22003: %v", box, s, err)
		}
	}
	for _, tc := range []struct {
		box  any
		want int64
	}{
		{uint64(5), 5}, {uint8(6), 6}, {uint16(7), 7}, {uint32(8), 8}, {uint(9), 9},
		{int8(10), 10}, {int16(11), 11}, {int64(math.MaxInt64), math.MaxInt64},
		{int64(math.MinInt64), math.MinInt64}, {float64(1e15), 1e15},
	} {
		got := roundTripOneValue(t, col, tc.box)
		if n, ok := got.(int64); !ok || n != tc.want {
			t.Errorf("writing %v (%T) into an INT64 column read back as %v (%T), want %d",
				tc.box, tc.box, got, got, tc.want)
		}
	}
}

// A network address handed over in its BINARY form is the address, not a zero.
func TestAnIPv4LeafStoresABinaryAddress(t *testing.T) {
	col := Column{Name: "c", Type: TypeIPv4, Nullable: true}
	for _, box := range []any{net.ParseIP("10.0.0.5").To4(), net.ParseIP("10.0.0.5"), []byte{10, 0, 0, 5}} {
		// The reader hands an IPV4 column back in its INT64 storage form:
		// 10.0.0.5 is 0x0A000005. Before networkBytesToInt64 all three boxes
		// stored 0.
		got := roundTripOneValue(t, col, box)
		if n, ok := got.(int64); !ok || n != 0x0A000005 {
			t.Errorf("writing %v (%T) into an IPV4 column read back as %v (%T), want %d (10.0.0.5)",
				box, box, got, got, 0x0A000005)
		}
	}
	if err := writeOneValue(t, col, []byte{1, 2, 3}); err == nil {
		t.Error("writing a three-byte value into an IPV4 column succeeded; it names no address")
	}
}

// writeOneValue writes a single row through the native writer and reports the
// error it latched, if any.
func writeOneValue(t *testing.T, col Column, box any) error {
	t.Helper()
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
	if err := nw.WriteMapRows([]map[string]any{{col.Name: box}}); err != nil {
		return err
	}
	return nw.Close()
}

func roundTripOneValue(t *testing.T, col Column, box any) any {
	t.Helper()
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
	if err := nw.WriteMapRows([]map[string]any{{col.Name: box}}); err != nil {
		t.Fatalf("write %v (%T) into a %s column: %v", box, box, col.Type, err)
	}
	if err := nw.Close(); err != nil {
		t.Fatalf("close after %v (%T) into a %s column: %v", box, box, col.Type, err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("read %d rows, want 1", len(rows))
	}
	return rows[0][col.Name]
}

// sameInt32Reading compares against the reader's box for the column: an int32
// for all four INT32-leaf types (reader.go maps TypeInt32/Port/Protocol/Date to
// the same reading).
func sameInt32Reading(got any, want int32) bool {
	switch g := got.(type) {
	case int32:
		return g == want
	case int64:
		return g == int64(want)
	case int:
		return int32(g) == want
	}
	return false
}

// FuzzInt32LeafValue drives the box → INT32-leaf coercion over arbitrary
// values of every box class the boundaries admit, and asserts the ONE
// invariant that matters for a safety-critical writer: the resolver either
// refuses, or hands back a value that is EQUAL to the input. It never wraps,
// truncates or substitutes.
func FuzzInt32LeafValue(f *testing.F) {
	f.Add(0, int64(0), 0.0)
	f.Add(1, int64(math.MaxInt32)+1, 1e10)
	f.Add(3, int64(math.MinInt32)-1, math.NaN())
	f.Add(5, int64(-1), 2.5)
	f.Add(7, int64(math.MaxInt64), math.Inf(1))

	f.Fuzz(func(t *testing.T, class int, n int64, x float64) {
		boxes := []any{
			int(n), int8(n), int16(n), int32(n), n,
			uint8(n), uint16(n), uint32(n), uint(n), uint64(n),
			x, float32(x),
		}
		box := boxes[((class%len(boxes))+len(boxes))%len(boxes)]
		for _, colType := range []TypeID{TypeInt32, TypePort, TypeProtocol, TypeDate} {
			got, err := int32LeafValue(colType, box)
			if err != nil {
				if s := sqlerr.StateOf(err); s != "22003" && s != "42804" {
					t.Fatalf("%s / %v (%T): SQLSTATE %q, want 22003 or 42804", colType, box, box, s)
				}
				continue
			}
			// Accepted: the stored int32 must be the SAME NUMBER as the box.
			switch b := box.(type) {
			case float64:
				if float64(got) != b {
					t.Fatalf("%s: float64(%v) accepted and stored as %d — a different number", colType, b, got)
				}
			case float32:
				if float32(got) != b {
					t.Fatalf("%s: float32(%v) accepted and stored as %d — a different number", colType, b, got)
				}
			default:
				wide, isInt, werr := integerBoxValue(colType, box)
				if !isInt || werr != nil {
					t.Fatalf("%s: %v (%T) accepted by int32LeafValue but not an integer box", colType, box, box)
				}
				if int64(got) != wide {
					t.Fatalf("%s: %v (%T) accepted and stored as %d — a different number", colType, box, box, got)
				}
			}
		}
	})
}
