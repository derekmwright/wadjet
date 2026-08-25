package pgwire

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// This file gates #570: a BYTES column is PostgreSQL's `bytea` on the wire.
//
// What it was. pgTypeOID had no BYTES case, so the column was declared OID
// 25 (text); formatPgValueTyped had no []byte case, so the TEXT body was
// Go's %v ("[255 254 0 65]"); and appendBinaryValue had the same gap, so the
// BINARY body was those same debug characters under a fixed declaration. A
// bytea Bind PARAMETER was rendered as the SPELLING of its bytes
// ('\x6869'), so `WHERE b = $1` compared a ten-character string against a
// two-byte column and matched nothing.
//
// What PostgreSQL does, verified against live postgres:17-alpine:
//
//	bytea_output          hex (the default)
//	text format           \x then LOWERCASE hex; the empty value is `\x`
//	binary format         the bytes themselves, nothing else
//	bytea::text           the same \x hex the text format carries
//	b LIKE '%A%'          BYTEWISE — matches the 0x41 BYTE, not a hex digit
//
// The last line is why CAST and LIKE deliberately disagree for this one
// type; ADR-0012 item 11 records it.
//
// The hazard the hex form removes: for 0xff 0xfe 0x00 0x41 the raw bytes are
// invalid UTF-8 and hold an embedded NUL, which no PostgreSQL text-format
// field can carry and which libpq's PQgetvalue (a NUL-terminated char*)
// truncates at. pgx read four bytes and psql read two, from one query.

// byteaProbe are the values every case below is asserted over: the empty
// value, an all-zero one, the invalid-UTF-8-with-embedded-NUL one from the
// issue, ASCII text, and a NULL.
var byteaProbe = []struct {
	k       int64
	b       []byte
	null    bool
	wantHex string // the text-format body PostgreSQL sends
}{
	{k: 0, b: []byte{}, wantHex: `\x`},
	{k: 1, b: []byte{0, 0, 0, 0}, wantHex: `\x00000000`},
	{k: 2, b: []byte{0xff, 0xfe, 0x00, 0x41}, wantHex: `\xfffe0041`},
	{k: 3, b: []byte("hi"), wantHex: `\x6869`},
	{k: 4, null: true},
}

func setupByteaDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("wadjet.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeBytes, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "bytea_probe", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	rows := make([]map[string]any, 0, len(byteaProbe))
	for _, p := range byteaProbe {
		r := map[string]any{"k": p.k}
		if !p.null {
			r["b"] = p.b
		}
		rows = append(rows, r)
	}
	ing := db.NewIngester("bytea_probe", schema, nil, ingest.Config{MaxBufferRows: 64, RowGroupSize: 64})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

func byteaDSN(srv *Server) string {
	return fmt.Sprintf("postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", srv.Addr())
}

// TestByteaColumnDeclaresBytea covers the RowDescription a client reads its
// decoder off: the OID, the declared size, and the type modifier, through
// Describe (the promise made before any row moves) rather than off a
// DataRow.
func TestByteaColumnDeclaresBytea(t *testing.T) {
	srv := setupByteaDB(t)
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, byteaDSN(srv))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())

	desc, err := conn.Prepare(ctx, "", `SELECT k, b FROM bytea_probe ORDER BY k`, nil)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if len(desc.Fields) != 2 {
		t.Fatalf("RowDescription has %d fields, want 2", len(desc.Fields))
	}
	f := desc.Fields[1]
	if f.DataTypeOID != 17 {
		t.Errorf("column b declared OID %d, want 17 (bytea). Under 25 (text) a client "+
			"decodes the cell as a string and a strlen-based one truncates at the first NUL", f.DataTypeOID)
	}
	if f.DataTypeSize != -1 {
		t.Errorf("column b declared size %d, want -1 (bytea is variable length)", f.DataTypeSize)
	}
	if f.TypeModifier != -1 {
		t.Errorf("column b declared typmod %d, want -1 (bytea takes no modifier)", f.TypeModifier)
	}
}

// TestByteaTextAndBinaryFormats is the body half: the same rows read once
// under each result format code, against what PostgreSQL sends.
//
// The binary arm is the one the OID change makes load-bearing. Under OID 25
// the %v fallback was at least self-consistent — the binary form of a text
// column IS its bytes — and under OID 17 it ships debug characters a typed
// client decodes as the value.
func TestByteaTextAndBinaryFormats(t *testing.T) {
	srv := setupByteaDB(t)
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, byteaDSN(srv))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())

	const sql = `SELECT k, b FROM bytea_probe ORDER BY k`
	for _, tc := range []struct {
		name  string
		codes []int16
		want  func(p int) []byte
	}{
		{"text", []int16{0, 0}, func(p int) []byte { return []byte(byteaProbe[p].wantHex) }},
		{"binary", []int16{1, 1}, func(p int) []byte { return byteaProbe[p].b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := conn.ExecParams(ctx, sql, nil, nil, nil, tc.codes).Read()
			if res.Err != nil {
				t.Fatalf("exec: %v", res.Err)
			}
			if len(res.Rows) != len(byteaProbe) {
				t.Fatalf("%d rows, want %d", len(res.Rows), len(byteaProbe))
			}
			for i, p := range byteaProbe {
				cell := res.Rows[i][1]
				// NULL is a NEGATIVE length on the wire, which pgconn hands
				// back as a nil slice; the empty value is a ZERO length, a
				// non-nil empty one. Conflating them is how a NULLed column
				// reads as an empty value.
				if p.null {
					if cell != nil {
						t.Errorf("row %d: NULL came back as %q, want a negative length", i, cell)
					}
					continue
				}
				if cell == nil {
					t.Errorf("row %d: a non-NULL value came back as NULL", i)
					continue
				}
				if want := tc.want(i); !bytes.Equal(cell, want) {
					t.Errorf("row %d (k=%d): %s body %q (hex %s), want %q (hex %s)",
						i, p.k, tc.name, cell, hex.EncodeToString(cell), want, hex.EncodeToString(want))
				}
			}
		})
	}
}

// TestByteaRoundTripsThroughPgx is the client-facing smoke: pgx picks its
// decoder from the declared OID, so this is what a Go, JDBC or psql user
// actually gets. It scans into []byte (pgx's mapping for bytea; scanning a
// text column into one would hand back the RENDERED bytes instead) and
// compares against the value that went in.
func TestByteaRoundTripsThroughPgx(t *testing.T) {
	srv := setupByteaDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, byteaDSN(srv))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(ctx, `SELECT k, b FROM bytea_probe ORDER BY k`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var k int64
		var b []byte
		if err := rows.Scan(&k, &b); err != nil {
			t.Fatalf("scan row %d: %v", i, err)
		}
		if i >= len(byteaProbe) {
			t.Fatalf("more rows than the fixture holds")
		}
		p := byteaProbe[i]
		switch {
		case p.null && b != nil:
			t.Errorf("row %d: NULL scanned as %#v", i, b)
		case !p.null && !bytes.Equal(b, p.b):
			t.Errorf("row %d (k=%d): scanned %#v, want %#v", i, k, b, p.b)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if i != len(byteaProbe) {
		t.Fatalf("%d rows, want %d", i, len(byteaProbe))
	}
}

// TestByteaParameterBindsToTheValuesBytes covers the way IN. A bytea
// parameter denotes BYTES, so the literal Bind writes has to carry the
// value's bytes and not their spelling — in either wire format, and in
// either of PostgreSQL's two text spellings for the type.
func TestByteaParameterBindsToTheValuesBytes(t *testing.T) {
	srv := setupByteaDB(t)
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, byteaDSN(srv))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())

	for _, tc := range []struct {
		name    string
		raw     []byte
		binary  bool
		wantKey string
	}{
		{name: "binary", raw: []byte("hi"), binary: true, wantKey: "3"},
		{name: "binary_high_bytes", raw: []byte{0xff, 0xfe, 0x00, 0x41}, binary: true, wantKey: "2"},
		{name: "text_hex", raw: []byte(`\x6869`), wantKey: "3"},
		{name: "text_hex_high_bytes", raw: []byte(`\xfffe0041`), wantKey: "2"},
		// byteain's other accepted spelling: printable bytes stand for
		// themselves and \ooo is one octal byte.
		{name: "text_escape", raw: []byte(`hi`), wantKey: "3"},
		{name: "text_escape_octal", raw: []byte(`\377\376\000A`), wantKey: "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			format := int16(0)
			if tc.binary {
				format = 1
			}
			res := conn.ExecParams(ctx, `SELECT k FROM bytea_probe WHERE b = $1`,
				[][]byte{tc.raw}, []uint32{17}, []int16{format}, nil).Read()
			if res.Err != nil {
				t.Fatalf("exec: %v", res.Err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1 — a bytea parameter that binds to the SPELLING of "+
					"its bytes matches nothing against a BYTES column", len(res.Rows))
			}
			if got := string(res.Rows[0][0]); got != tc.wantKey {
				t.Errorf("matched k=%s, want k=%s", got, tc.wantKey)
			}
		})
	}
}

// TestCastByteaToTextIsHexOnTheWire pins the second half of #570 at the
// wire: `CAST(b AS STRING)` is PostgreSQL's `bytea::text`, so the value is
// the same \x hex the bytea column itself carries, declared as text (OID
// 25) because that is what a cast to text produces on both engines.
func TestCastByteaToTextIsHexOnTheWire(t *testing.T) {
	srv := setupByteaDB(t)
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, byteaDSN(srv))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(context.Background())

	const sql = `SELECT k, CAST(b AS STRING) AS s FROM bytea_probe ORDER BY k`
	desc, err := conn.Prepare(ctx, "", sql, nil)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if got := desc.Fields[1].DataTypeOID; got != 25 {
		t.Errorf("CAST(b AS STRING) declared OID %d, want 25 (text)", got)
	}

	res := conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
	if res.Err != nil {
		t.Fatalf("exec: %v", res.Err)
	}
	for i, p := range byteaProbe {
		cell := res.Rows[i][1]
		if p.null {
			if cell != nil {
				t.Errorf("row %d: CAST of NULL came back as %q", i, cell)
			}
			continue
		}
		if string(cell) != p.wantHex {
			t.Errorf("row %d (k=%d): CAST(b AS STRING) = %q, want %q. The raw bytes were the "+
				"previous answer and carry an embedded NUL that libpq truncates at",
				i, p.k, cell, p.wantHex)
		}
	}
}

// TestDecodeByteaText covers byteain's two spellings and the malformed ones,
// which are an ERROR rather than a silent fallback to the raw characters:
// binding the SPELLING of a value the client meant as bytes is the defect
// this decoder exists to prevent, so guessing is the wrong failure mode.
func TestDecodeByteaText(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    []byte
		wantErr bool
	}{
		{in: `\x`, want: []byte{}},
		{in: `\x6869`, want: []byte("hi")},
		{in: `\xFFFE0041`, want: []byte{0xff, 0xfe, 0x00, 0x41}},
		{in: ``, want: []byte{}},
		{in: `hi`, want: []byte("hi")},
		{in: `\377\376\000A`, want: []byte{0xff, 0xfe, 0x00, 0x41}},
		{in: `a\\b`, want: []byte(`a\b`)},
		{in: `\x686`, wantErr: true}, // odd hex digit count
		{in: `\xzz`, wantErr: true},  // not hex
		{in: `a\b`, wantErr: true},   // a backslash that introduces nothing legal
		{in: `\77`, wantErr: true},   // an octal escape needs three digits
		{in: `\400`, wantErr: true},  // past one byte
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := decodeByteaText(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("decodeByteaText(%q) = %#v, want an error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeByteaText(%q): %v", tc.in, err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("decodeByteaText(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}
