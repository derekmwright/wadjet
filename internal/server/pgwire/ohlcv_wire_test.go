package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE BAR ON THE WIRE (#965).
//
// `ohlcv(...)` is the first aggregate this engine has whose result is a ROW it
// CONSTRUCTS rather than one it read out of a column, so the whole
// declaration chain — the planner's field list, the operator's output vector,
// the coordinator's OutputSchema, pgwire's nested-schema map — is exercised
// for the first time by a value no catalog describes. A break anywhere in it
// is silent on the wire: the DataRow is well formed and the field count is
// right, and only the ORDER and the digits say the declaration was lost.
//
// What PostgreSQL 17.11 does with a composite, measured 2026-09-08:
//
//	SELECT ROW(1::int4, 2.5::float8, 'x'::text)  ->  (1,2.5,x)
//	\gdesc                                       ->  record, OID 2249
//	ROW(1.0::numeric(18,4), NULL, 3)::text       ->  (1.0000,,3)
//	ROW('a,b', 'has "quote"', '')::text          ->  ("a,b","has ""quote""","")
//
// A NULL field is an EMPTY slot, never the word NULL. This engine's
// `formatPgComposite` already renders exactly that; what this gate holds is
// that the bar reaches it with its DECLARATION, in the declared order.
//
// The OID is the recorded divergence: a ROW column declares 25 (text) here
// where PostgreSQL declares `record` 2249. That is the whole TYPE's
// declaration and predates the bar — #992 scopes it as its own decision — so
// the gate asserts 25 rather than pretending otherwise, and a change to 2249
// has to move this line deliberately.
func TestPGWireRendersTheBarAsAPostgresComposite(t *testing.T) {
	db := ohlcvWireDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("test", "wadjet")
	client.writeMsg('Q', append([]byte(
		`SELECT bar FROM (SELECT ohlcv(ts, px, vol) AS bar FROM owt) t`), 0))

	var row []byte
	var desc []byte
	for {
		typ, payload, err := client.readMsg()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		switch typ {
		case 'T':
			desc = append([]byte(nil), payload...)
		case 'D':
			row = append([]byte(nil), payload...)
		case 'E':
			t.Fatalf("refused: %s", client.parseError(payload))
		}
		if typ == 'Z' {
			break
		}
	}
	client.terminate()

	// The VALUE. open/high/low/close come from a DECIMAL(9,2) column so they
	// carry their scale; volume is SUM(int4) = bigint; vwap is AVG's
	// DECIMAL(38,6). Declared field order is
	// (open, high, low, close, volume, vwap), which is NOT alphabetical —
	// a declaration-less render would sort the keys and put close first.
	const want = "(10.00,21.00,7.00,21.00,24,14.583333)"
	if !bytes.Contains(row, []byte(want)) {
		t.Fatalf("DataRow %q does not carry %s.\n"+
			"Alphabetical order — what a render with no declaration produces — would be\n"+
			"  (close,high,low,open,volume,vwap) = (21.00,21.00,7.00,10.00,24,14.583333)\n"+
			"which has the SAME field count and the same six numbers.", row, want)
	}

	// The DECLARATION beside it.
	if len(desc) < 6 {
		t.Fatal("no RowDescription")
	}
	if n := binary.BigEndian.Uint16(desc[0:2]); n != 1 {
		t.Fatalf("RowDescription has %d fields, want 1", n)
	}
	// name (NUL-terminated) then tableOID(4) attnum(2) typeOID(4).
	nul := bytes.IndexByte(desc[2:], 0)
	if nul < 0 {
		t.Fatal("malformed RowDescription")
	}
	p := 2 + nul + 1
	if string(desc[2:2+nul]) != "bar" {
		t.Errorf("column named %q, want bar", desc[2:2+nul])
	}
	oid := binary.BigEndian.Uint32(desc[p+6 : p+10])
	if oid != 25 {
		t.Errorf("bar declares OID %d, want 25 (text).\n"+
			"PostgreSQL declares `record` = 2249 for an anonymous composite. That is the "+
			"whole ROW TYPE's declaration here, not this function's, and it is recorded in "+
			"ADR-0012's divergence list (see also #992 for ARRAY). Moving it to 2249 is a "+
			"deliberate change to every ROW column and this line moves with it.", oid)
	}
}

// A NULL bar renders as a NULL FIELD — a -1 length in the DataRow — not as a
// composite of empty slots. An aggregate over no rows is NULL on the server
// too, and `(NULL::record).f` is NULL rather than an error (both measured).
func TestPGWireRendersANullBarAsNull(t *testing.T) {
	db := ohlcvWireDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("test", "wadjet")
	client.writeMsg('Q', append([]byte(
		`SELECT bar FROM (SELECT ohlcv(ts, px, vol) AS bar FROM owt WHERE id < 0) t`), 0))

	var row []byte
	for {
		typ, payload, err := client.readMsg()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		switch typ {
		case 'D':
			row = append([]byte(nil), payload...)
		case 'E':
			t.Fatalf("refused: %s", client.parseError(payload))
		}
		if typ == 'Z' {
			break
		}
	}
	client.terminate()

	if len(row) < 6 {
		t.Fatalf("DataRow too short: %q", row)
	}
	if n := binary.BigEndian.Uint16(row[0:2]); n != 1 {
		t.Fatalf("DataRow has %d fields, want 1", n)
	}
	if l := int32(binary.BigEndian.Uint32(row[2:6])); l != -1 {
		t.Errorf("the bar's field length is %d, want -1 (SQL NULL). A composite of "+
			"empty slots — `(,,,,,)` — is a bar over no rows pretending to be a bar.", l)
	}
}

func ohlcvWireDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "px", Type: parquet.TypeDecimal, Precision: 9, Scale: 2},
		{Name: "vol", Type: parquet.TypeInt32},
	}}
	if err := db.Catalog().CreateTable(ctx, "owt", schema, nil); err != nil {
		t.Fatal(err)
	}
	ms := func(s string) int64 {
		v, err := time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			t.Fatal(err)
		}
		return v.UTC().UnixMilli()
	}
	rows := []map[string]any{
		{"id": int32(1), "ts": ms("2020-09-13 12:00:05"), "px": "10.00", "vol": int32(3)},
		{"id": int32(2), "ts": ms("2020-09-13 12:00:15"), "px": "14.00", "vol": int32(1)},
		{"id": int32(3), "ts": ms("2020-09-13 12:00:25"), "px": "7.00", "vol": int32(5)},
		{"id": int32(4), "ts": ms("2020-09-13 12:00:35"), "px": "11.00", "vol": int32(2)},
		{"id": int32(5), "ts": ms("2020-09-13 12:01:05"), "px": "20.00", "vol": int32(4)},
		{"id": int32(6), "ts": ms("2020-09-13 12:01:05"), "px": "18.00", "vol": int32(6)},
		{"id": int32(7), "ts": ms("2020-09-13 12:01:45"), "px": "19.00", "vol": int32(1)},
		{"id": int32(8), "ts": ms("2020-09-13 12:01:45"), "px": "21.00", "vol": int32(2)},
	}
	ing := db.NewIngester("owt", schema, nil, ingest.Config{MaxBufferRows: 32, RowGroupSize: 32})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}
