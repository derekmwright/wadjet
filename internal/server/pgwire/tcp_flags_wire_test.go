package pgwire

// WHAT THE WIRE CARRIES FOR THE TCP FLAG FAMILY (#966).
//
// A value oracle cannot see a right value under a wrong OID, which is why this
// exists beside the value gates. Four declarations and their renderings:
//
//	tcp_flags_has_all/any/none  BOOL, OID 16, size 1, rendered t/f
//	tcp_flag_mask               int4, OID 23, size 4
//	tcp_flags_text              text, OID 25
//	tcp_flags                   text, OID 25 — an ARRAY column declares OID 25
//	                            here (#992, recorded in ADR-0012's list), and a
//	                            top-level projection of a container-returning
//	                            function is TEXT before that even applies (see
//	                            wadjet.TestATopLevelTCPFlagsProjectionIsTextToday)
//
// The values are PostgreSQL 17.11's, measured for the same three `visits`
// values (100, 42, 200) with the RFC 9293 bit table joined in SQL:
//
//	100 -> RST|URG|ECE     (100 & 18) = 18  -> f
//	 42 -> SYN|PSH|URG     ( 42 & 18) <> 0  -> t
//	200 -> PSH|ECE|CWR     (200 & 18) = 0   -> t

import (
	"context"
	"testing"
)

func TestPGWireDeclaresTheTCPFlagFamily(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	res := conn.ExecParams(context.Background(),
		`SELECT tcp_flags_has_all(visits, 'SYN', 'ACK') AS b,
		        tcp_flag_mask('SYN', 'ACK') AS m,
		        tcp_flags_text(visits) AS s,
		        tcp_flags(visits) AS a
		 FROM users ORDER BY id`, nil, nil, nil, []int16{0}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	for _, want := range []struct {
		col  int
		name string
		oid  uint32
		size int16
	}{
		{0, "tcp_flags_has_all", 16, 1}, // bool
		{1, "tcp_flag_mask", 23, 4},     // int4
		{2, "tcp_flags_text", 25, -1},   // text
		// An ARRAY declares OID 25 on this wire (#992). It is recorded in
		// ADR-0012's divergence list rather than pinned per function; when
		// that changes, this line changes with every other ARRAY column.
		{3, "tcp_flags", 25, -1},
	} {
		f := res.FieldDescriptions[want.col]
		if f.DataTypeOID != want.oid {
			t.Errorf("%s declared OID %d, want %d", want.name, f.DataTypeOID, want.oid)
		}
		if f.DataTypeSize != want.size {
			t.Errorf("%s declared size %d, want %d", want.name, f.DataTypeSize, want.size)
		}
	}

	// The rendered cells, in id order: users holds visits 100, 42, 200.
	wantRows := [][]string{
		{"f", "18", "RST|URG|ECE", "[RST URG ECE]"},
		{"f", "18", "SYN|PSH|URG", "[SYN PSH URG]"},
		{"f", "18", "PSH|ECE|CWR", "[PSH ECE CWR]"},
	}
	if len(res.Rows) != len(wantRows) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(wantRows))
	}
	for i, wr := range wantRows {
		for j, w := range wr {
			if got := string(res.Rows[i][j]); got != w {
				t.Errorf("row %d column %d rendered %q, want %q", i, j, got, w)
			}
		}
	}
}

// The BOOLEAN half in the BINARY format, which is the half a text comparison
// cannot see: one byte, 0 or 1.
func TestPGWireRendersAFlagPredicateAsABinaryBool(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	res := conn.ExecParams(context.Background(),
		`SELECT tcp_flags_has_any(visits, 'SYN', 'ACK') AS b FROM users ORDER BY id`,
		nil, nil, nil, []int16{1}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	// PostgreSQL: (100 & 18) <> 0 -> f, (42 & 18) <> 0 -> t, (200 & 18) <> 0 -> f.
	want := []byte{0, 1, 0}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
	}
	for i, w := range want {
		got := res.Rows[i][0]
		if len(got) != 1 || got[0] != w {
			t.Errorf("row %d binary bool = %v, want [%d]", i, got, w)
		}
	}
}

// THE BITWISE FAMILY ON THE WIRE (#966 round 2).
//
// Every function that answers an integer must DECLARE one, or the value is
// coerced through a double on the way out and the client is handed a right
// number under a wrong OID — or a wrong number. `visits` holds 100, 42, 200;
// the wide cells use a literal because `users` has no BIGINT past 2^53.
//
// PostgreSQL 17.11: `pg_typeof(2::int8 >> 1)` is bigint, `to_hex(bigint)` is
// text, `bit_count(bit)` is bigint.
func TestPGWireDeclaresTheBitwiseFamily(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	res := conn.ExecParams(context.Background(),
		`SELECT BITWISE_AND(visits, 18) AS a,
		        BITWISE_LEFT_SHIFT(4611686018427387922, 1) AS l,
		        BITWISE_RIGHT_SHIFT(4611686018427387922, 0) AS r,
		        BITWISE_ARITHMETIC_SHIFT_RIGHT(4611686018427387922, 1) AS ar,
		        BIT_COUNT(4611686018427387922) AS bc,
		        FROM_HEX('4000000000000012') AS fh,
		        FROM_BASE('4000000000000012', 16) AS fb,
		        TO_HEX(4611686018427387922) AS th
		 FROM users WHERE id = 1`, nil, nil, nil, []int16{0}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	for _, want := range []struct {
		col  int
		name string
		oid  uint32
	}{
		{0, "BITWISE_AND", 20}, {1, "BITWISE_LEFT_SHIFT", 20},
		{2, "BITWISE_RIGHT_SHIFT", 20}, {3, "BITWISE_ARITHMETIC_SHIFT_RIGHT", 20},
		{4, "BIT_COUNT", 20}, {5, "FROM_HEX", 20}, {6, "FROM_BASE", 20},
		{7, "TO_HEX", 25},
	} {
		if got := res.FieldDescriptions[want.col].DataTypeOID; got != want.oid {
			t.Errorf("%s declared OID %d, want %d", want.name, got, want.oid)
		}
	}
	// The values, which is what the declaration protects: every one of these
	// was wrong at round 1 because the argument went through a double.
	wantRow := []string{
		"0",                    // 100 & 18
		"-9223372036854775772", // (2^62|18) << 1
		"4611686018427387922",  // >> 0 is the identity
		"2305843009213693961",  // PostgreSQL's arithmetic >> 1
		"3",                    // popcount of 2^62|18
		"4611686018427387922",  // from_hex
		"4611686018427387922",  // from_base
		"4000000000000012",     // to_hex
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	for i, w := range wantRow {
		if got := string(res.Rows[0][i]); got != w {
			t.Errorf("column %d rendered %q, PostgreSQL 17.11 answers %q", i, got, w)
		}
	}
}

// An unrecognized flag name reaches the client as an ErrorResponse carrying
// SQLSTATE 22023 and the offending name — not as a larger row set, and not as
// a connection that hangs.
func TestPGWireRefusesAnUnknownFlagName(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	res := conn.ExecParams(context.Background(),
		`SELECT tcp_flags_has_all(visits, 'SYN', 'ACKK') AS b FROM users`,
		nil, nil, nil, []int16{0}).Read()
	if res.Err == nil {
		t.Fatal("the wire answered where 22023 is due")
	}
	msg := res.Err.Error()
	if !contains(msg, "22023") && !contains(msg, "SQLSTATE 22023") {
		t.Errorf("error %q does not carry SQLSTATE 22023", msg)
	}
	if !contains(msg, "ACKK") {
		t.Errorf("error %q does not name the unrecognized flag", msg)
	}
}

// SUM OVER AN INTEGER-DECLARED FUNCTION DECLARES NUMERIC (#966 round 2 B1).
//
// This is the half no value oracle can see. PostgreSQL's `f & k` over a BIGINT
// operand is bigint and `SUM(bigint)` is NUMERIC — OID 1700 — so a wide sum
// does not overflow; over an int4 operand `f & k` is int4 and its SUM is
// bigint. Every bitwise result is declared int8 here (the value-preserving
// widening already in ADR-0012's list), so BOTH spellings' SUM is numeric,
// and the second cell below is where that divergence is visible: the VALUE is
// 2 on both engines, and only the OID differs.
//
// Before the fix the aggregate-width walk did not follow an ordinary function,
// so SUM took the BIGINT accumulator: the first cell answered 22003 where
// PostgreSQL answers 9223372036854775846, and the second carried a right value
// under OID 20.
//
// The LENGTH cell is the control from the other side — an INT32-declared
// function keeps the bigint accumulator, exactly as PostgreSQL's
// `SUM(length(text))` is bigint.
func TestPGWireDeclaresSumOverAnIntegerFunction(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql string
		oid       uint32
		want      string
	}{
		{"wide_or", `SELECT SUM(BITWISE_OR(4611686018427387922, 1)) AS v FROM users WHERE id < 3`,
			1700, "9223372036854775846"},
		{"narrow_and", `SELECT SUM(BITWISE_AND(visits, 18)) AS v FROM users`, 1700, "2"},
		{"windowed_or", `SELECT SUM(BITWISE_OR(4611686018427387922, 1)) OVER () AS v
		                 FROM users WHERE id = 1`, 1700, "4611686018427387923"},
		{"bit_count", `SELECT SUM(BIT_COUNT(4611686018427387922)) AS v FROM users WHERE id = 1`,
			1700, "3"},
		{"length_control", `SELECT SUM(LENGTH(name)) AS v FROM users WHERE id = 1`, 20, "5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("ExecParams: %v", res.Err)
			}
			if got := res.FieldDescriptions[0].DataTypeOID; got != tc.oid {
				t.Errorf("declared OID %d, want %d\n  SQL: %s", got, tc.oid, tc.sql)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(res.Rows))
			}
			if got := string(res.Rows[0][0]); got != tc.want {
				t.Errorf("rendered %q, want %q\n  SQL: %s", got, tc.want, tc.sql)
			}
		})
	}
}
