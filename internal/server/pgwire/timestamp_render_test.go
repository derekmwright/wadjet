package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// tsMetas is a one-column TIMESTAMP result description.
func tsMetas() []wadjet.ColumnMeta {
	return []wadjet.ColumnMeta{{Name: "ts", TypeName: "TIMESTAMP", TypeID: parquet.TypeTimestamp}}
}

// dataRowFields splits the first DataRow ('D') message in wire into its
// per-column payloads. A -1 length (SQL NULL) yields a nil entry.
func dataRowFields(tb testing.TB, wire []byte) [][]byte {
	tb.Helper()
	for i := 0; i+5 <= len(wire); {
		msgType := wire[i]
		length := int(binary.BigEndian.Uint32(wire[i+1 : i+5]))
		body := wire[i+5 : i+1+length]
		if msgType != 'D' {
			i += 1 + length
			continue
		}
		n := int(binary.BigEndian.Uint16(body[0:2]))
		fields := make([][]byte, 0, n)
		off := 2
		for f := 0; f < n; f++ {
			flen := int(int32(binary.BigEndian.Uint32(body[off : off+4])))
			off += 4
			if flen < 0 {
				fields = append(fields, nil)
				continue
			}
			fields = append(fields, body[off:off+flen])
			off += flen
		}
		return fields
	}
	tb.Fatalf("no DataRow in wire: %q", wire)
	return nil
}

// TestTimestampTextFormat is the regression for issue #321 defect 1: the
// RowDescription declares OID 1114 (`timestamp`), so the text a client reads
// has to be an instant. It used to be the raw epoch integer, which psql
// echoes verbatim and a typed client cannot parse at all.
func TestTimestampTextFormat(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}

	rows := []map[string]any{{"ts": int64(826727136000)}}
	sent, err := c.sendResultRows(context.Background(), []string{"ts"}, nil, rows, nil, tsMetas())
	if err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	if sent != 1 {
		t.Fatalf("sent = %d, want 1", sent)
	}

	fields := dataRowFields(t, rc.buf.Bytes())
	if len(fields) != 1 {
		t.Fatalf("DataRow has %d fields, want 1", len(fields))
	}
	if got, want := string(fields[0]), "1996-03-13 14:25:36"; got != want {
		t.Errorf("timestamp text = %q, want %q", got, want)
	}
}

// TestTimestampTextFormatWithoutMetas: introspection answers carry no typed
// metadata, and the RowDescription then declares text — so the value must be
// left exactly as the engine boxed it rather than guessed at.
func TestTimestampTextFormatWithoutMetas(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}

	rows := []map[string]any{{"ts": int64(826727136000)}}
	if _, err := c.sendResultRows(context.Background(), []string{"ts"}, nil, rows, nil, nil); err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	fields := dataRowFields(t, rc.buf.Bytes())
	if got, want := string(fields[0]), "826727136000"; got != want {
		t.Errorf("untyped column text = %q, want %q", got, want)
	}
}

// TestTimestampBinaryFormat: binary `timestamp` is int64 MICROSECONDS since
// 2000-01-01, not the engine's milliseconds since 1970. The old code emitted
// the raw int64, which kept the declared 8-byte width — so the client parsed
// it without complaint and landed tens of thousands of years off.
func TestTimestampBinaryFormat(t *testing.T) {
	cases := []struct {
		name string
		ms   int64
		want int64 // microseconds since 2000-01-01
	}{
		{"pg epoch", 946684800000, 0},
		{"issue 321 value", 826727136000, (826727136000 - 946684800000) * 1000},
		{"unix epoch", 0, -946684800 * 1_000_000},
		{"sub-second", 1755000000123, (1755000000123 - 946684800000) * 1000},
	}
	for _, tc := range cases {
		rc := &recordConn{}
		c := &pgConn{conn: rc}
		rows := []map[string]any{{"ts": tc.ms}}
		if _, err := c.sendResultRows(context.Background(), []string{"ts"}, nil, rows, []int16{1}, tsMetas()); err != nil {
			t.Fatalf("%s: sendResultRows: %v", tc.name, err)
		}
		fields := dataRowFields(t, rc.buf.Bytes())
		if len(fields[0]) != 8 {
			t.Fatalf("%s: binary timestamp is %d bytes, want 8 (RowDescription declared size 8)",
				tc.name, len(fields[0]))
		}
		if got := int64(binary.BigEndian.Uint64(fields[0])); got != tc.want {
			t.Errorf("%s: binary timestamp = %d us since 2000, want %d", tc.name, got, tc.want)
		}
	}
}

// TestTimestampBinaryOutOfRange: the field is fixed at 8 bytes, so a value
// whose microsecond form would overflow int64 has no in-band way to say
// "out of range". Sending NULL is the only alternative to a silently wrapped
// date, which is the failure this change exists to remove.
func TestTimestampBinaryOutOfRange(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}
	rows := []map[string]any{{"ts": int64(1) << 62}}
	if _, err := c.sendResultRows(context.Background(), []string{"ts"}, nil, rows, []int16{1}, tsMetas()); err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	if f := dataRowFields(t, rc.buf.Bytes()); f[0] != nil {
		t.Errorf("out-of-range timestamp encoded as %v, want NULL", f[0])
	}
}

// TestTimestampColumnsMask covers the column/meta alignment, including the
// reordered case the name lookup exists for.
func TestTimestampColumnsMask(t *testing.T) {
	metas := []wadjet.ColumnMeta{
		{Name: "a", TypeID: parquet.TypeInt64},
		{Name: "ts", TypeID: parquet.TypeTimestamp},
		{Name: "s", TypeID: parquet.TypeString},
	}

	if got := sendColumnTypes([]string{"a", "s"}, nil); got != nil {
		t.Errorf("no metas: types = %v, want nil", got)
	}
	if got := sendColumnTypes([]string{"a", "s"}, metas[:1]); got != nil {
		t.Errorf("no converted column: types = %v, want nil", got)
	}

	got := sendColumnTypes([]string{"a", "ts", "s"}, metas)
	want := []parquet.TypeID{colTypeNone, parquet.TypeTimestamp, colTypeNone}
	if len(got) != len(want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("types = %v, want %v", got, want)
		}
	}

	// Columns in a different order than the metas: resolved by name.
	got = sendColumnTypes([]string{"s", "ts"}, metas)
	if len(got) != 2 || got[0] != colTypeNone || got[1] != parquet.TypeTimestamp {
		t.Errorf("reordered types = %v, want [none timestamp]", got)
	}

	// A column with no meta at all stays untyped rather than borrowing the
	// meta that happens to sit at its index.
	got = sendColumnTypes([]string{"unknown", "ts"}, metas)
	if len(got) != 2 || got[0] != colTypeNone || got[1] != parquet.TypeTimestamp {
		t.Errorf("unmatched-column types = %v, want [none timestamp]", got)
	}

	// A DATE column resolves too — it is the other type the send path
	// converts, and the reason the mask became a type list.
	got = sendColumnTypes([]string{"d", "a"}, []wadjet.ColumnMeta{
		{Name: "d", TypeID: parquet.TypeDate}, {Name: "a", TypeID: parquet.TypeInt64}})
	if len(got) != 2 || got[0] != parquet.TypeDate || got[1] != colTypeNone {
		t.Errorf("date types = %v, want [date none]", got)
	}
}

// TestTimestampTextMatchesDeclaredOID ties the two halves together: whatever
// the send path writes must be readable as the type the RowDescription
// declared. This is the invariant that was broken.
func TestTimestampTextMatchesDeclaredOID(t *testing.T) {
	if oid := pgTypeOID("TIMESTAMP"); oid != 1114 {
		t.Fatalf("TIMESTAMP OID = %d, want 1114", oid)
	}
	if sz := pgTypeSize(1114); sz != 8 {
		t.Fatalf("timestamp binary size = %d, want 8", sz)
	}

	rc := &recordConn{}
	c := &pgConn{conn: rc}
	c.sendTypedRowDescription(tsMetas())
	if !bytes.Contains(rc.buf.Bytes(), []byte{0, 0, 4, 90}) { // 1114 big-endian
		t.Error("RowDescription does not declare OID 1114 for a TIMESTAMP column")
	}
}
