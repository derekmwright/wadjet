package pgwire

// Regression tests for #363: a CAST to a temporal type must declare the
// temporal wire type, not the type its row-boxed value happens to look like.
// CAST(x AS date) boxed as its rendered text was declared OID 25 (text,
// size -1) instead of 1082 (date, size 4); CAST(x AS timestamp) boxed as
// epoch milliseconds was declared OID 20 and SENT the raw integer.

import (
	"context"
	"testing"
)

// wireField runs sql through pgconn under a text result format and returns
// the single field's declared OID, size, and first-row text value.
func wireField(t *testing.T, addr, sql string) (oid uint32, size int16, value string) {
	t.Helper()
	conn := connectPgconn(t, addr)
	res := conn.ExecParams(context.Background(), sql, nil, nil, nil, []int16{0}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams(%s): %v", sql, res.Err)
	}
	if len(res.FieldDescriptions) != 1 {
		t.Fatalf("%s: got %d fields, want 1", sql, len(res.FieldDescriptions))
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) != 1 {
		t.Fatalf("%s: no rows", sql)
	}
	f := res.FieldDescriptions[0]
	return f.DataTypeOID, f.DataTypeSize, string(res.Rows[0][0])
}

func TestCastDateDeclaresDateOID(t *testing.T) {
	_, srv := setupRealDB(t)
	oid, size, val := wireField(t, srv.Addr(), `SELECT CAST('1996-01-10' AS date) AS d`)
	if oid != 1082 {
		t.Errorf("CAST(x AS date) declared OID %d, want 1082 (date)", oid)
	}
	if size != 4 {
		t.Errorf("CAST(x AS date) declared size %d, want 4", size)
	}
	if val != "1996-01-10" {
		t.Errorf("CAST(x AS date) sent %q, want 1996-01-10", val)
	}
}

func TestCastTimestampDeclaresTimestampOIDAndRenders(t *testing.T) {
	_, srv := setupRealDB(t)
	oid, size, val := wireField(t, srv.Addr(), `SELECT CAST('1996-01-10 12:34:56' AS timestamp) AS ts`)
	if oid != 1114 {
		t.Errorf("CAST(x AS timestamp) declared OID %d, want 1114 (timestamp)", oid)
	}
	if size != 8 {
		t.Errorf("CAST(x AS timestamp) declared size %d, want 8", size)
	}
	// The epoch-millisecond integer 821277296000 is the boxed form; the wire
	// must carry the rendered timestamp.
	if val != "1996-01-10 12:34:56" {
		t.Errorf("CAST(x AS timestamp) sent %q, want 1996-01-10 12:34:56", val)
	}
}
