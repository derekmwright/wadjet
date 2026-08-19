package pgwire

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// dateMetas is a one-column DATE result description.
func dateMetas() []wadjet.ColumnMeta {
	return []wadjet.ColumnMeta{{Name: "d", TypeName: "DATE", TypeID: parquet.TypeDate}}
}

// TestDateBinaryFormat pins binary `date` as PostgreSQL's 4-byte day count
// from 2000-01-01. The engine boxes a date as its rendered text, and the
// binary encoder dispatched on the Go type, so those bytes went out verbatim
// under the OID 1082 the RowDescription had already declared — a client
// reading a day count got the ASCII of "1996-03-13" instead.
func TestDateBinaryFormat(t *testing.T) {
	cases := []struct {
		name string
		date string
		want int32 // days since 2000-01-01
	}{
		{"pg epoch", "2000-01-01", 0},
		{"day after pg epoch", "2000-01-02", 1},
		{"before pg epoch", "1996-03-13", -1389},
		{"unix epoch", "1970-01-01", -10957},
		{"leap day", "2024-02-29", 8825},
	}
	for _, tc := range cases {
		rc := &recordConn{}
		c := &pgConn{conn: rc}
		rows := []map[string]any{{"d": tc.date}}
		if _, err := c.sendResultRows(context.Background(), []string{"d"}, nil, rows, []int16{1}, dateMetas()); err != nil {
			t.Fatalf("%s: sendResultRows: %v", tc.name, err)
		}
		fields := dataRowFields(t, rc.buf.Bytes())
		if len(fields[0]) != 4 {
			t.Fatalf("%s: binary date is %d bytes, want 4 (RowDescription declared size 4)",
				tc.name, len(fields[0]))
		}
		if got := int32(binary.BigEndian.Uint32(fields[0])); got != tc.want {
			t.Errorf("%s: binary date = %d days since 2000-01-01, want %d", tc.name, got, tc.want)
		}
	}
}

// TestDateBinaryUnparseableIsNull covers the only honest answer for a value
// that is not a date: the field is a fixed 4 bytes, so absence is the only
// way to say so.
func TestDateBinaryUnparseableIsNull(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}
	rows := []map[string]any{{"d": "not a date"}}
	if _, err := c.sendResultRows(context.Background(), []string{"d"}, nil, rows, []int16{1}, dateMetas()); err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	if f := dataRowFields(t, rc.buf.Bytes())[0]; f != nil {
		t.Errorf("unparseable date encoded as %q, want NULL", f)
	}
}

// TestDateTextFormatUnchanged guards the common path: a text-format date is
// the rendered date, exactly as before.
func TestDateTextFormatUnchanged(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}
	rows := []map[string]any{{"d": "1996-03-13"}}
	if _, err := c.sendResultRows(context.Background(), []string{"d"}, nil, rows, []int16{0}, dateMetas()); err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	if got := string(dataRowFields(t, rc.buf.Bytes())[0]); got != "1996-03-13" {
		t.Errorf("text date = %q, want %q", got, "1996-03-13")
	}
}
