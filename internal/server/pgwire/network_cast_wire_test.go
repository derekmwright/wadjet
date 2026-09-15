package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// The WIRE half of #1092 and #986: what a cast to a network type DECLARES and
// what it RENDERS, which a value oracle cannot see.
//
// Before the fix `SELECT CAST('abc' AS IPV4)` answered "abc" under OID 25 —
// right-looking in psql, a Go string in pgx, and text PostgreSQL's inet
// refuses outright — and `CAST('udp' AS PROTOCOL)` was an integer-syntax
// error. The OIDs themselves are unchanged and that is the point of asserting
// them: the four address types have declared `text` (25) since #834's wire
// table, so the cast now produces a PARSED value under the OID its own COLUMN
// declares, rather than an unparsed one under the OID a STRING column declares
// — the same number for two different reasons until now.
func TestANetworkCastOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	for _, c := range []struct {
		sql  string
		oid  uint32
		text string
	}{
		{`SELECT CAST('010.1.2.3' AS IPV4) AS v`, 25, "10.1.2.3"},
		{`SELECT CAST('2001:DB8::1' AS IPV6) AS v`, 25, "2001:db8::1"},
		{`SELECT CAST('192.168/16' AS CIDR) AS v`, 25, "192.168/16"},
		{`SELECT CAST('aabbcc:ddeeff' AS MACADDR) AS v`, 25, "aa:bb:cc:dd:ee:ff"},
		// uuid keeps OID 2950, the OID a UUID column declares (#839).
		{`SELECT CAST('{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}' AS UUID) AS v`, 2950,
			"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"},
		// PROTOCOL reads its own text form and still declares int4, the OID a
		// PROTOCOL column declares (#834).
		{`SELECT CAST('udp' AS PROTOCOL) AS v`, 23, "17"},
		{`SELECT CAST('TCP' AS PROTOCOL) AS v`, 23, "6"},
		// The edges of each type's own range still answer, under the OID the
		// column declares.
		{`SELECT CAST(65535 AS PORT) AS v`, 23, "65535"},
		{`SELECT CAST(0 AS PORT) AS v`, 23, "0"},
		{`SELECT CAST(255 AS PROTOCOL) AS v`, 23, "255"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			res := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("ExecParams: %v", res.Err)
			}
			if got := res.FieldDescriptions[0].DataTypeOID; got != c.oid {
				t.Errorf("declared OID %d, want %d", got, c.oid)
			}
			if len(res.Rows) != 1 || string(res.Rows[0][0]) != c.text {
				t.Errorf("rendered %q, want %q", res.Rows, c.text)
			}
		})
	}

	// The refusals carry PostgreSQL's class on the wire, not the blanket
	// 42000 an unclassified error becomes.
	for _, c := range []struct {
		sql  string
		code string
	}{
		{`SELECT CAST('abc' AS IPV4) AS v`, "22P02"},
		{`SELECT CAST('aabb:ccdd:eeff' AS MACADDR) AS v`, "22P02"},
		{`SELECT CAST('a-0eebc999c0b4ef8bb6d6bb9bd380a11' AS UUID) AS v`, "22P02"},
		{`SELECT CAST('nosuchproto' AS PROTOCOL) AS v`, "22P02"},
		// A value ENTERING PORT or PROTOCOL is held to the TYPE's range, and
		// the refusal crosses the wire as 22003 rather than the blanket class
		// (Derek, 2026-09-15). `CAST('0x1bb' AS PORT)` is 22P02 one line up in
		// the value table: the type's TEXT form is decimal, not int4's.
		{`SELECT CAST(65536 AS PORT) AS v`, "22003"},
		{`SELECT CAST('65536' AS PORT) AS v`, "22003"},
		{`SELECT CAST(-1 AS PORT) AS v`, "22003"},
		{`SELECT CAST(256 AS PROTOCOL) AS v`, "22003"},
		// PostgreSQL-valid text naming a NETWORK, which a bare-address column
		// has no room for: a feature limit, not a syntax error.
		{`SELECT CAST('10/8' AS IPV4) AS v`, "0A000"},
	} {
		t.Run("refusal/"+c.sql, func(t *testing.T) {
			res := conn.ExecParams(ctx, c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err == nil {
				t.Fatalf("answered %v; PostgreSQL 17.11 refuses this text", res.Rows)
			}
			var pgErr *pgconn.PgError
			if !asPgError(res.Err, &pgErr) {
				t.Fatalf("error %v is not a *pgconn.PgError — the refusal crossed the "+
					"wire without a class", res.Err)
			}
			if pgErr.Code != c.code {
				t.Errorf("SQLSTATE %q, want %q (%v)", pgErr.Code, c.code, res.Err)
			}
		})
	}
}
