package parquet

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// #887: a value missing from a REQUIRED column FAILS THE WRITE.
//
// A definition level says how many of a leaf's optional ancestors are present,
// and a required leaf's absence level IS its present level — so appending one
// for a nil advanced the count with nothing behind it and shifted every later
// value in that column. Measured at de5bc970, with WriteRows, Close and the
// read all returning nil:
//
//	required BOOL,  rows [nil, true]   read back [true, false]
//	required INT64, rows [nil, 42]     a file the decoder cannot finish
//	required ARRAY, row  nil           read back as an empty array
//
// The refusal is asked of EVERY type, because the level arithmetic is the
// same for all of them and the boolean case only looked different because bit
// padding hid it.
func TestARequiredColumnRefusesAMissingValue(t *testing.T) {
	for _, col := range requiredPresenceMatrix() {
		t.Run(col.Name, func(t *testing.T) {
			// An explicit nil.
			assertNotNullViolation(t, col, []map[string]any{{col.Name: nil}, {col.Name: presentValue(col)}})
			// An ABSENT key is the same absence by another spelling.
			assertNotNullViolation(t, col, []map[string]any{{}, {col.Name: presentValue(col)}})
			// And the present value alone must still write and read back.
			var buf bytes.Buffer
			nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
			if err := nw.WriteMapRows([]map[string]any{{col.Name: presentValue(col)}}); err != nil {
				t.Fatalf("a present value was refused: %v", err)
			}
			if err := nw.Close(); err != nil {
				t.Fatalf("a present value was refused at Close: %v", err)
			}
		})
	}
}

// An empty network literal is an absence by another name, and a required
// column cannot express it either.
func TestARequiredNetworkColumnRefusesAnEmptyLiteral(t *testing.T) {
	for _, colType := range []TypeID{TypeIPv4, TypeIPv6, TypeMAC, TypeUUID} {
		col := Column{Name: "c", Type: colType, Nullable: false}
		assertNotNullViolation(t, col, []map[string]any{{"c": ""}})
	}
}

// A required child of a PRESENT optional struct is refused; the same child
// under an ABSENT optional struct is not, because it is not there to be
// missing. That distinction is the whole reason the check reads the LEVEL and
// not the column's Nullable flag.
func TestARequiredFieldIsOnlyRequiredWhereItsParentIsPresent(t *testing.T) {
	col := Column{Name: "r", Type: TypeRow, Nullable: true, Fields: []Column{
		{Name: "a", Type: TypeInt64, Nullable: false},
		{Name: "b", Type: TypeInt64, Nullable: true},
	}}
	// Parent present, required child missing: refused.
	assertNotNullViolation(t, col, []map[string]any{{"r": map[string]any{"b": int64(1)}}})
	// Parent absent: legal, and the file reads back.
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
	if err := nw.WriteMapRows([]map[string]any{{"r": nil}, {"r": map[string]any{"a": int64(7), "b": int64(8)}}}); err != nil {
		t.Fatalf("an absent optional struct was refused: %v", err)
	}
	if err := nw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	rows, err := r.ReadRows(nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("read %d rows, want 2", len(rows))
	}
	if rows[0]["r"] != nil {
		t.Errorf("the absent struct read back as %v, want NULL", rows[0]["r"])
	}
	got, ok := rows[1]["r"].(map[string]any)
	if !ok || got["a"] != int64(7) || got["b"] != int64(8) {
		t.Errorf("the present struct read back as %v, want {a:7 b:8}", rows[1]["r"])
	}
}

// A required CONTAINER cannot be NULL either — the level for "absent" is the
// level that already means "present and empty", so the reader is right to read
// {} or [] back and the writer was what was wrong.
func TestARequiredContainerRefusesANullValue(t *testing.T) {
	for _, col := range []Column{
		{Name: "c", Type: TypeArray, Nullable: false,
			ElementType: &Column{Name: "element", Type: TypeInt64, Nullable: true}},
		{Name: "c", Type: TypeRow, Nullable: false,
			Fields: []Column{{Name: "a", Type: TypeInt64, Nullable: true}}},
		{Name: "c", Type: TypeMap, Nullable: false,
			ElementType: &Column{Name: "entry", Type: TypeRow, Fields: []Column{
				{Name: "key", Type: TypeString},
				{Name: "value", Type: TypeInt64, Nullable: true},
			}}},
	} {
		t.Run(col.Type.String(), func(t *testing.T) {
			var buf bytes.Buffer
			nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
			err := nw.WriteMapRows([]map[string]any{{"c": nil}})
			if err == nil {
				err = nw.Close()
			}
			if err == nil {
				t.Fatalf("a NULL %s was written into a NOT NULL column; the file cannot say so",
					col.Type)
			}
		})
	}
}

// requiredPresenceMatrix is one NOT NULL column per type this writer stores in
// a leaf, so the refusal is asserted for all of them and not only for the
// three the issue happened to measure.
func requiredPresenceMatrix() []Column {
	cols := []Column{
		{Name: "c_bool", Type: TypeBool},
		{Name: "c_int32", Type: TypeInt32},
		{Name: "c_int64", Type: TypeInt64},
		{Name: "c_float32", Type: TypeFloat32},
		{Name: "c_float64", Type: TypeFloat64},
		{Name: "c_string", Type: TypeString},
		{Name: "c_bytes", Type: TypeBytes},
		{Name: "c_timestamp", Type: TypeTimestamp},
		{Name: "c_ipv4", Type: TypeIPv4},
		{Name: "c_ipv6", Type: TypeIPv6},
		{Name: "c_cidr", Type: TypeCIDR},
		{Name: "c_mac", Type: TypeMAC},
		{Name: "c_port", Type: TypePort},
		{Name: "c_protocol", Type: TypeProtocol},
		{Name: "c_duration", Type: TypeDuration},
		{Name: "c_uuid", Type: TypeUUID},
		{Name: "c_date", Type: TypeDate},
		{Name: "c_decimal", Type: TypeDecimal, Precision: 9, Scale: 2},
		{Name: "c_decimal_wide", Type: TypeDecimal, Precision: 38, Scale: 10},
		{Name: "c_vector", Type: TypeVector, Dimension: 4},
	}
	for i := range cols {
		cols[i].Nullable = false
	}
	return cols
}

func presentValue(col Column) any {
	switch col.Type {
	case TypeBool:
		return true
	case TypeInt32, TypePort, TypeProtocol:
		return int32(7)
	case TypeInt64, TypeTimestamp, TypeDuration:
		return int64(7)
	case TypeFloat32:
		return float32(7)
	case TypeFloat64:
		return float64(7)
	case TypeString:
		return "x"
	case TypeBytes:
		return []byte{1, 2}
	case TypeIPv4:
		return "10.0.0.1"
	case TypeIPv6:
		return "2001:db8::1"
	case TypeCIDR:
		return "10.0.0.0/8"
	case TypeMAC:
		return "00:11:22:33:44:55"
	case TypeUUID:
		return "00000000-0000-4000-8000-000000000001"
	case TypeDate:
		return "2026-09-05"
	case TypeDecimal:
		return "1.25"
	case TypeVector:
		return []float32{1, 2, 3, 4}
	}
	return nil
}

func assertNotNullViolation(t *testing.T, col Column, rows []map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	nw := NewNativeWriter(&buf, Schema{Columns: []Column{col}}, DefaultWriterConfig())
	err := nw.WriteMapRows(rows)
	if err == nil {
		err = nw.Close()
	}
	if err == nil {
		r, rerr := NewReaderFromBytes(buf.Bytes())
		got := "a file that could not be opened"
		if rerr == nil {
			if back, rerr2 := r.ReadRows(nil); rerr2 == nil {
				got = "a file reading back as " + sprintRows(back)
			}
		}
		t.Fatalf("a missing value in NOT NULL column %q (%s) was accepted and produced %s",
			col.Name, col.Type, got)
	}
	if s := sqlerr.StateOf(err); s != "23502" {
		t.Errorf("NOT NULL column %q (%s): SQLSTATE %q, want 23502 (not_null_violation): %v",
			col.Name, col.Type, s, err)
	}
}

func sprintRows(rows []map[string]any) string {
	return fmt.Sprintf("%v", rows)
}
