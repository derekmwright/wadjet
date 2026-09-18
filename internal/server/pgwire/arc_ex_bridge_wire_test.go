// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"strings"
	"testing"
)

// #583's bridge, ON THE WIRE. A value oracle cannot see a right value under a
// wrong OID — a bytea under text's 25 is the embedded-NUL hazard #570 removed
// for the column itself — so the four functions this arc added declare what
// PostgreSQL declares for them, measured with pg_typeof on 17.11:
//
//	encode(bytea, format)   text    (OID 25)
//	decode(text, format)    bytea   (OID 17)
//	get_byte(bytea, int)    integer
//	set_byte(bytea, int, i) bytea   (OID 17)
func TestTheByteaBridgeDeclaresItsTypesOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	for _, c := range []struct {
		name, sql string
		wantOID   uint32
		wantVal   string
	}{
		{"encode_hex", `SELECT ENCODE(TO_UTF8('hi'), 'hex') AS v`, 25, "6869"},
		{"encode_base64", `SELECT ENCODE(TO_UTF8('hi'), 'base64') AS v`, 25, "aGk="},
		{"decode_hex", `SELECT DECODE('6869', 'hex') AS v`, 17, `\x6869`},
		{"decode_base64", `SELECT DECODE('aGk=', 'base64') AS v`, 17, `\x6869`},
		{"get_byte", `SELECT GET_BYTE(TO_UTF8('hi'), 0) AS v`, 20, "104"},
		{"set_byte", `SELECT SET_BYTE(TO_UTF8('hi'), 0, 65) AS v`, 17, `\x4169`},
	} {
		t.Run(c.name, func(t *testing.T) {
			oid, _, val := wireField(t, srv.Addr(), c.sql)
			if oid != c.wantOID {
				t.Errorf("%s declared OID %d, want %d — a bytea under text's 25 is what a "+
					"strlen-based client truncates at the first NUL (#570)", c.sql, oid, c.wantOID)
			}
			if val != c.wantVal {
				t.Errorf("%s sent %q, want %q", c.sql, val, c.wantVal)
			}
		})
	}
}

// The refusals this arc added reach the wire with PostgreSQL's own class, not
// the blanket 42000 and not XX000. A client branches on this code.
func TestTheCallResolutionRefusalsCarryTheirSQLStateOnTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	for _, c := range []struct{ name, sql, state, msg string }{
		{"wrong_arity", `SELECT UPPER('a','b') AS v`, "42883",
			"function upper(unknown, unknown) does not exist"},
		{"numeric_literal_in_a_text_position", `SELECT UPPER(1) AS v`, "42883",
			"function upper(bigint) does not exist"},
		// TO_UTF8 is the one builtin declared RetBytes, so it is how a query
		// produces a BYTES value without a column. `CAST(x AS BYTES)` is
		// deliberately NOT used here: that destination is a pass-through the
		// cast does not implement and physical.inferCastType declares STRING
		// for it, so the argument really is text at that point — a residue
		// recorded with this arc rather than a hole in the refusal.
		{"text_function_over_bytes", `SELECT UPPER(TO_UTF8('hi')) AS v`, "42883",
			"function upper(bytea) does not exist"},
		{"quoted_fraction_to_integer", `SELECT CAST('2.5' AS INTEGER) AS v`, "22P02",
			`invalid input syntax for type integer: "2.5"`},
		{"unrecognized_encoding", `SELECT ENCODE(TO_UTF8('hi'), 'zzz') AS v`, "22023",
			`unrecognized encoding: "zzz"`},
		{"byte_index_out_of_range", `SELECT GET_BYTE(TO_UTF8('hi'), 5) AS v`, "2202E",
			"index 5 out of valid range, 0..1"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), c.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err == nil {
				t.Fatalf("ANSWERED %d rows; PostgreSQL 17.11 refuses this with %s\n  SQL: %s",
					len(res.Rows), c.state, c.sql)
			}
			if got := pgErrCode(res.Err); got != c.state {
				t.Errorf("SQLSTATE %s, want %s\n  err: %v\n  SQL: %s", got, c.state, res.Err, c.sql)
			}
			if !strings.Contains(res.Err.Error(), c.msg) {
				t.Errorf("%q does not carry %q\n  SQL: %s", res.Err, c.msg, c.sql)
			}
		})
	}
}
