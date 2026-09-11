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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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

// SUM OVER A FUNCTION DECLARES WHAT POSTGRESQL DECLARES (#966 rounds 2-3).
//
// This is the half no value oracle can see: a right value under a wrong OID.
// PostgreSQL's rule for an ACCUMULATING aggregate is by the operand's WIDTH —
// `sum(int4)` is bigint (OID 20) and `sum(int8)` is numeric (OID 1700) — so
// the question each cell asks is what width PostgreSQL declares for the
// FUNCTION inside the SUM. Both sides of that rule are pinned here, because a
// fix for one side is how the other side broke twice:
//
//   - round 2 B1: the walk followed no ordinary function at all, so
//     `SUM(BITWISE_OR(wide, 1))` took the BIGINT accumulator and answered
//     22003 where PostgreSQL answers 9223372036854775846.
//   - round 3 B1: the walk then read the function's Ret DECLARATION, which is
//     the CARRIER this engine stores a result in and not PostgreSQL's result
//     type. Every integer here computes in an int64, so `regexp_count` —
//     `integer` in PostgreSQL — declares RetInt64 exactly as `bit_count` —
//     `bigint` in PostgreSQL — does, and `SUM(REGEXP_COUNT(…))` declared
//     numeric where PostgreSQL declares bigint.
//
// The width is `expr.PGIntegerResultWidth`'s now: PostgreSQL's measured type
// where PostgreSQL has the function, the domain's own width where it does not,
// and the OPERANDS' width for the bitwise family, which is arithmetic for this
// purpose. Measured on 17.11 for every cell below:
//
//	sum(f8 & 18)                  numeric   sum(f4 & 3)          bigint
//	sum(bit_count(bytea))         numeric   sum(length(text))    bigint
//	sum(regexp_count('abab','a')) bigint    sum(masklen(cidr))   bigint
//	sum(octet_length('abc'))      bigint
//
// `users.visits` is INT64 and `users.id` is INT32, which is what makes the two
// bitwise cells a pair rather than a repetition.
func TestPGWireDeclaresSumOverAnIntegerFunction(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql string
		oid       uint32
		want      string
	}{
		// ---- the int8 side: numeric, as PostgreSQL's sum(bigint) is.
		{"wide_literal_or", `SELECT SUM(BITWISE_OR(4611686018427387922, 1)) AS v FROM users WHERE id < 3`,
			1700, "9223372036854775846"},
		{"wide_column_and", `SELECT SUM(BITWISE_AND(visits, 18)) AS v FROM users`, 1700, "2"},
		{"windowed_wide_or", `SELECT SUM(BITWISE_OR(4611686018427387922, 1)) OVER () AS v
		                 FROM users WHERE id = 1`, 1700, "4611686018427387923"},
		// BIT_COUNT is PostgreSQL's own function and it declares BIGINT, even
		// though a 64-bit word's population count is 0..64. PostgreSQL
		// decides, so its SUM is numeric.
		{"bit_count", `SELECT SUM(BIT_COUNT(4611686018427387922)) AS v FROM users WHERE id = 1`,
			1700, "3"},

		// ---- the int4 side: bigint, as PostgreSQL's sum(integer) is. These
		// five are round 3's regression: all of them declared 1700 when the
		// walk read the carrier.
		{"narrow_column_and", `SELECT SUM(BITWISE_AND(id, 3)) AS v FROM users`, 20, "6"},
		{"windowed_narrow_and", `SELECT SUM(BITWISE_AND(id, 3)) OVER () AS v FROM users WHERE id = 1`,
			20, "1"},
		{"regexp_count", `SELECT SUM(REGEXP_COUNT(name, 'a')) AS v FROM users WHERE id < 3`, 20, "1"},
		{"windowed_regexp_count", `SELECT SUM(REGEXP_COUNT(name, 'a')) OVER () AS v
		                 FROM users WHERE id = 1`, 20, "1"},
		{"prefix_length", `SELECT SUM(PREFIX_LENGTH('10.0.0.0/24')) AS v FROM users WHERE id < 3`,
			20, "48"},
		{"payload_length", `SELECT SUM(PAYLOAD_LENGTH('abc')) AS v FROM users WHERE id < 3`,
			20, "6"},
		{"length_control", `SELECT SUM(LENGTH(name)) AS v FROM users WHERE id = 1`, 20, "5"},

		// ---- AVG is numeric on BOTH sides in PostgreSQL, so it is the
		// control that says a cell above moved because of the WIDTH rule and
		// not because the aggregate's whole typing moved. The OID is the
		// claim: the DIGITS are this engine's fixed +4 scale, PostgreSQL
		// renders 0.66666666666666666667 and 0.50000000000000000000, and
		// that scale is ADR-0024 item 2's recorded divergence rather than
		// anything this arc decides.
		{"avg_wide_is_numeric", `SELECT AVG(BITWISE_AND(visits, 18)) AS v FROM users`,
			1700, "0.6667"},
		{"avg_narrow_is_numeric", `SELECT AVG(REGEXP_COUNT(name, 'a')) AS v FROM users WHERE id < 3`,
			1700, "0.5000"},
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

// WHAT A MATERIALIZED INTEGER COLUMN DECLARES (#1018 round 5, B1).
//
// PostgreSQL's integer WIDTH is a property of a column's DECLARATION, and it
// survives materialization: `SELECT SUM(v) FROM (SELECT id & 3 AS v FROM t) s`
// is bigint there, exactly as the direct `SUM(id & 3)` is. Here every integer
// expression materializes as an INT64 carrier (ADR-0024's recorded widening),
// so a reader that had only the carrier declared numeric for the derived
// spelling and bigint for the direct one — the same number in two boxes,
// depending only on whether the CALL was still visible in the AST.
//
// Every OID below is live PostgreSQL 17.11's, measured over the same three
// rows (id 1..3 integer, visits 100/42/200 bigint, name alice/bob/carol). The
// AVG cells are the control that says a cell moved because of the WIDTH rule
// and not because the aggregate's whole typing moved: AVG is numeric on both
// sides in PostgreSQL. Their DIGITS are this engine's fixed +4 scale, which is
// ADR-0024 item 2's recorded divergence and not this gate's claim.
func TestPGWireDeclaresSumOverAMaterializedIntegerColumn(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql string
		oid       uint32
		want      string
	}{
		// ---- one derived level, each class.
		{"derived_narrow_and", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s`,
			20, "6"},
		{"derived_wide_and", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(visits,18) AS v FROM users) s`,
			1700, "2"},
		{"derived_regexp_count", `SELECT SUM(v) AS v FROM (SELECT REGEXP_COUNT(name,'a') AS v FROM users) s`,
			20, "2"},
		{"derived_payload_length", `SELECT SUM(v) AS v FROM (SELECT PAYLOAD_LENGTH(name) AS v FROM users) s`,
			20, "13"},
		// An int8-RESULT function through the same shape stays numeric:
		// PostgreSQL declares bit_count BIGINT and its SUM is numeric.
		{"derived_bit_count", `SELECT SUM(v) AS v FROM (SELECT BIT_COUNT(visits) AS v FROM users) s`,
			1700, "9"},

		// ---- two levels: a CTE over a derived table, and a derived table
		// over a derived table. The width has to ride EVERY boundary, not the
		// first one.
		{"cte_over_derived_narrow",
			`WITH c AS (SELECT v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s) SELECT SUM(v) AS v FROM c`,
			20, "6"},
		{"cte_over_derived_wide",
			`WITH c AS (SELECT v FROM (SELECT BITWISE_AND(visits,18) AS v FROM users) s) SELECT SUM(v) AS v FROM c`,
			1700, "2"},
		{"two_derived_levels",
			`SELECT SUM(v) AS v FROM (SELECT v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s1) s2`,
			20, "6"},

		// ---- a SET OPERATION resolves to the COMMON type of its arms, which
		// for two integers is the wider one (both measured on PostgreSQL).
		{"union_all_narrow",
			`SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users
			  UNION ALL SELECT BITWISE_AND(id,7) AS v FROM users) s`, 20, "12"},
		{"union_all_mixed_widths",
			`SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users
			  UNION ALL SELECT BITWISE_AND(visits,18) AS v FROM users) s`, 1700, "8"},

		// ---- the other producers of a materialized integer column.
		{"derived_arith", `SELECT SUM(v) AS v FROM (SELECT id*2 AS v FROM users) s`, 20, "12"},
		{"derived_cast_to_bigint", `SELECT SUM(v) AS v FROM (SELECT CAST(id AS BIGINT) AS v FROM users) s`,
			1700, "6"},
		{"derived_bare_int4_column", `SELECT SUM(v) AS v FROM (SELECT id AS v FROM users) s`, 20, "6"},
		{"derived_bare_int8_column", `SELECT SUM(v) AS v FROM (SELECT visits AS v FROM users) s`,
			1700, "342"},
		{"derived_group_key",
			`SELECT SUM(k) AS v FROM (SELECT BITWISE_AND(id,3) AS k, COUNT(*) AS c
			  FROM users GROUP BY BITWISE_AND(id,3)) s`, 20, "6"},
		// MIN hands back a value the column HELD, so it keeps the column's
		// width; SUM ANSWERS in bigint, so a SUM of a SUM is numeric.
		{"derived_min_keeps_the_width",
			`SELECT SUM(m) AS v FROM (SELECT MIN(v) AS m FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s1) s2`,
			20, "1"},
		{"derived_sum_of_a_sum_is_numeric",
			`SELECT SUM(s1) AS v FROM (SELECT SUM(v) AS s1 FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s0) s`,
			1700, "6"},

		// ---- the WINDOW slot reads the same declaration as the grouped one.
		{"windowed_derived_narrow_and",
			`SELECT SUM(v) OVER () AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s LIMIT 1`, 20, "6"},
		{"windowed_derived_wide_and",
			`SELECT SUM(v) OVER () AS v FROM (SELECT BITWISE_AND(visits,18) AS v FROM users) s LIMIT 1`,
			1700, "2"},
		{"windowed_derived_regexp_count",
			`SELECT SUM(v) OVER () AS v FROM (SELECT REGEXP_COUNT(name,'a') AS v FROM users) s LIMIT 1`,
			20, "2"},

		// ---- MIN/MAX OVER A COMPUTED ARGUMENT keeps the ARGUMENT's width
		// too, not the carrier's (#1018 round 5 review, P1). These declared
		// 1700 — `aggArgIntWidth` declined anything but a bare column and the
		// caller then recorded the INT64 CARRIER, so a MIN of an int4
		// expression positively claimed int8. PostgreSQL 17.11, measured:
		// `min(id & 3)` / `max(id & 3)` / `min(regexp_count(name,'a'))` are
		// `integer`, grouped and `OVER ()`, and their SUM is `bigint`;
		// `min(visits & 18)` is `bigint` and its SUM `numeric`.
		{"min_over_a_computed_narrow_argument",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(id,3)) AS m FROM users) s`, 20, "1"},
		{"min_over_a_computed_narrow_argument_grouped",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(id,3)) AS m FROM users GROUP BY id) s`,
			20, "6"},
		{"max_over_a_computed_narrow_argument_grouped",
			`SELECT SUM(m) AS v FROM (SELECT MAX(BITWISE_AND(id,3)) AS m FROM users GROUP BY id) s`,
			20, "6"},
		{"min_over_a_computed_int4_result_function",
			`SELECT SUM(m) AS v FROM (SELECT MIN(REGEXP_COUNT(name,'a')) AS m FROM users GROUP BY id) s`,
			20, "2"},
		{"min_over_computed_arithmetic",
			`SELECT SUM(m) AS v FROM (SELECT MIN(id*2) AS m FROM users) s`, 20, "2"},
		{"min_over_a_computed_case",
			`SELECT SUM(m) AS v FROM (SELECT MIN(CASE WHEN id>1 THEN id ELSE 0 END) AS m FROM users) s`,
			20, "0"},
		// The int8 side, and the CAST, are the boundary: the walk must not
		// narrow what PostgreSQL keeps wide.
		{"min_over_a_computed_wide_argument_is_numeric",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(visits,18)) AS m FROM users GROUP BY id) s`,
			1700, "2"},
		{"min_over_a_cast_to_bigint_is_numeric",
			`SELECT SUM(m) AS v FROM (SELECT MIN(CAST(id AS BIGINT)) AS m FROM users) s`, 1700, "1"},
		// And the WINDOW spelling of the same rule, which must agree with the
		// grouped one.
		{"windowed_min_over_a_computed_narrow_argument",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(id,3)) OVER () AS m FROM users) s`, 20, "3"},
		{"windowed_min_over_a_computed_wide_argument",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(visits,18)) OVER () AS m FROM users) s`,
			1700, "0"},

		// ---- AVG is numeric on BOTH sides in PostgreSQL: the control.
		{"avg_derived_narrow_is_numeric",
			`SELECT AVG(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s`, 1700, "2.0000"},
		{"windowed_avg_derived_narrow_is_numeric",
			`SELECT AVG(v) OVER () AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s LIMIT 1`,
			1700, "2.0000"},
	} {
		// Both wire FORMATS. A value oracle cannot see a right value under a
		// wrong OID, and a text-only gate cannot see a binary renderer that
		// disagrees with the declaration it was handed.
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/format=%d", tc.name, format), func(t *testing.T) {
				res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil,
					[]int16{format}).Read()
				if res.Err != nil {
					t.Fatalf("ExecParams: %v\n  SQL: %s", res.Err, tc.sql)
				}
				if got := res.FieldDescriptions[0].DataTypeOID; got != tc.oid {
					t.Errorf("declared OID %d, PostgreSQL declares %d\n  SQL: %s",
						got, tc.oid, tc.sql)
				}
				if len(res.Rows) != 1 {
					t.Fatalf("got %d rows, want 1\n  SQL: %s", len(res.Rows), tc.sql)
				}
				if format == 0 {
					if got := string(res.Rows[0][0]); got != tc.want {
						t.Errorf("rendered %q, want %q\n  SQL: %s", got, tc.want, tc.sql)
					}
				}
			})
		}
	}
}

// AN INVALID LITERAL FLAG NAME IS REFUSED WITH NO ROWS AT ALL (#1018 round 5,
// B2).
//
// A flag NAME is a MASK OPERAND, and its spelling is a property of the QUERY
// rather than of the data. PostgreSQL settles it for the arithmetic this
// family is named for: `SELECT 'x'::int FROM (VALUES (1)) t WHERE false` is
// 22P02 there, because the coercion happens at parse analysis and does not
// wait for rows. Here the fold was per row, so an empty input answered zero
// rows and no error while the same typo over a reached row was 22023 —
// whether a typo is an error depended on the data.
//
// The wire arm is the one that sees this: a value oracle has no row to
// compare, and the SQLSTATE is the whole answer.
func TestPGWireRefusesAnInvalidFlagNameWithNoRows(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql, msg string
	}{
		{"has_all", `SELECT tcp_flags_has_all(visits,'BOGUS') AS v FROM users WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"has_any", `SELECT tcp_flags_has_any(visits,'BOGUS') AS v FROM users WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"has_none", `SELECT tcp_flags_has_none(visits,'BOGUS') AS v FROM users WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"has_tcp_flag", `SELECT has_tcp_flag(visits,'BOGUS') AS v FROM users WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"tcp_flag_mask", `SELECT tcp_flag_mask('BOGUS') AS v FROM users WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"tcp_flags_from_string", `SELECT tcp_flags_from_string('SYN,BOGUS') AS v FROM users WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"in_a_predicate", `SELECT COUNT(*) AS n FROM users WHERE tcp_flags_has_all(visits,'BOGUS') AND id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"empty_list", `SELECT tcp_flags_has_any(visits) AS v FROM users WHERE id < 0`,
			"tcp_flags_has_any requires at least one TCP flag name"},
		{"empty_element", `SELECT tcp_flags_from_string('SYN,') AS v FROM users WHERE id < 0`,
			`empty TCP flag name at position 2`},
		// The reached-row shape is the SAME refusal, so the two layers say
		// one thing.
		{"the_reached_row_shape", `SELECT tcp_flags_has_all(visits,'BOGUS') AS v FROM users`,
			`TCP flag name "BOGUS" not recognized`},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/format=%d", tc.name, format), func(t *testing.T) {
				res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil,
					[]int16{format}).Read()
				if res.Err == nil {
					t.Fatalf("ANSWERED %d rows; 22023 is due\n  SQL: %s", len(res.Rows), tc.sql)
				}
				if got := pgErrCode(res.Err); got != "22023" {
					t.Errorf("SQLSTATE %s, want 22023\n  err: %v", got, res.Err)
				}
				if !strings.Contains(res.Err.Error(), tc.msg) {
					t.Errorf("%q does not carry %q", res.Err, tc.msg)
				}
			})
		}
	}

	// AND THE OTHER SIDE. A name the family DOES know, and a name supplied by
	// a COLUMN rather than written as a constant, still answer over an empty
	// input: a plan-time refusal that fired on either would be the false
	// positive the binder's standing contract forbids, and a non-constant
	// name is not knowable before rows at all.
	for _, tc := range []struct{ name, sql string }{
		{"valid_names_answer", `SELECT tcp_flags_has_all(visits,'SYN','ACK') AS v FROM users WHERE id < 0`},
		{"a_column_name_stays_per_row", `SELECT tcp_flags_has_all(visits, name) AS v FROM users WHERE id < 0`},
		{"a_null_name_is_a_null_mask", `SELECT tcp_flags_has_all(visits, NULL) AS v FROM users WHERE id < 0`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("refused a query it must answer: %v\n  SQL: %s", res.Err, tc.sql)
			}
			if len(res.Rows) != 0 {
				t.Errorf("got %d rows, want 0", len(res.Rows))
			}
		})
	}
}

// pgErrCode is the SQLSTATE a pgconn error carries.
func pgErrCode(err error) string {
	var pge *pgconn.PgError
	if errors.As(err, &pge) {
		return pge.Code
	}
	return ""
}

// AN INVALID LITERAL FLAG NAME IS REFUSED IN EVERY EXPRESSION POSITION, AND ON
// BOTH PLANNING PATHS (#1018 round 6, B1).
//
// Round 5 folded the constant name at COMPILATION, which is where the
// single-process path compiles the whole expression tree while it plans. On the
// stage DAG a stage's fragment compiles its own expressions WHEN A TASK RUNS,
// so a position whose stage receives no rows was never folded at all: HAVING,
// an ORDER BY key, a set-operation arm and a projection above a GROUP BY raised
// 22023 in one process and answered zero rows on three DAG arms, while a
// SELECT-list projection and a WHERE predicate — the two the coordinator folds
// on both paths — agreed. Whether a typo was an error depended on the data AND
// on the plan shape.
//
// The refusal now happens at the BINDER (physical.refuseUnknownFlagNames),
// which Plan and PlanDistributed both reach before any stage exists. The
// arm-by-arm half of this gate is the five-arm census in
// internal/coordinator; this one is the WIRE half, both formats, because the
// SQLSTATE is the whole answer when there is no row to compare.
//
// Each position is asserted over an EMPTY input and over a NON-EMPTY one: the
// pair is the claim, since "the refusal appears the moment a row reaches the
// stage" is exactly the defect.
func TestPGWireRefusesAnInvalidFlagNameInEveryExpressionPosition(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	const bogus = `TCP flag name "BOGUS" not recognized`
	for _, tc := range []struct{ name, empty, nonEmpty string }{
		{"having",
			`SELECT id AS n FROM users WHERE id < 0 GROUP BY id HAVING COUNT(*) > 0 AND tcp_flags_has_all(MIN(visits),'BOGUS')`,
			`SELECT id AS n FROM users GROUP BY id HAVING COUNT(*) > 0 AND tcp_flags_has_all(MIN(visits),'BOGUS')`},
		{"order_by",
			`SELECT id AS n FROM users WHERE id < 0 ORDER BY tcp_flag_mask('BOGUS')`,
			`SELECT id AS n FROM users ORDER BY tcp_flag_mask('BOGUS')`},
		{"union_all_arm",
			`SELECT tcp_flag_mask('SYN') AS n FROM users WHERE id < 0 UNION ALL SELECT tcp_flag_mask('BOGUS') AS n FROM users WHERE id < 0`,
			`SELECT tcp_flag_mask('SYN') AS n FROM users UNION ALL SELECT tcp_flag_mask('BOGUS') AS n FROM users`},
		{"projection_above_group_by",
			`SELECT id AS g, tcp_flag_mask('BOGUS') AS n FROM users WHERE id < 0 GROUP BY id`,
			`SELECT id AS g, tcp_flag_mask('BOGUS') AS n FROM users GROUP BY id`},
		{"exists_subquery",
			`SELECT COUNT(*) AS n FROM users t WHERE EXISTS (SELECT 1 FROM users u WHERE u.id < 0 AND tcp_flags_has_all(u.visits,'BOGUS'))`,
			`SELECT COUNT(*) AS n FROM users t WHERE EXISTS (SELECT 1 FROM users u WHERE tcp_flags_has_all(u.visits,'BOGUS'))`},
		{"in_subquery",
			`SELECT COUNT(*) AS n FROM users t WHERE t.id IN (SELECT u.id FROM users u WHERE u.id < 0 AND tcp_flags_has_all(u.visits,'BOGUS'))`,
			`SELECT COUNT(*) AS n FROM users t WHERE t.id IN (SELECT u.id FROM users u WHERE tcp_flags_has_all(u.visits,'BOGUS'))`},
		{"scalar_subquery",
			`SELECT (SELECT MAX(tcp_flag_mask('BOGUS')) FROM users u2 WHERE u2.id < 0) AS v FROM users WHERE id < 0`,
			`SELECT (SELECT MAX(tcp_flag_mask('BOGUS')) FROM users u2) AS v FROM users`},
		{"window_argument",
			`SELECT SUM(tcp_flag_mask('BOGUS')) OVER () AS w FROM users WHERE id < 0`,
			`SELECT SUM(tcp_flag_mask('BOGUS')) OVER () AS w FROM users`},
		{"window_partition_by",
			`SELECT COUNT(*) OVER (PARTITION BY tcp_flag_mask('BOGUS')) AS w FROM users WHERE id < 0`,
			`SELECT COUNT(*) OVER (PARTITION BY tcp_flag_mask('BOGUS')) AS w FROM users`},
		{"window_order_by",
			`SELECT RANK() OVER (ORDER BY tcp_flag_mask('BOGUS')) AS w FROM users WHERE id < 0`,
			`SELECT RANK() OVER (ORDER BY tcp_flag_mask('BOGUS')) AS w FROM users`},
		{"case_arm_never_taken",
			`SELECT COUNT(*) AS n FROM users WHERE id < 0 AND CASE WHEN 1 = 0 THEN tcp_flags_has_all(visits,'BOGUS') ELSE TRUE END`,
			`SELECT COUNT(*) AS n FROM users WHERE CASE WHEN 1 = 0 THEN tcp_flags_has_all(visits,'BOGUS') ELSE TRUE END`},
		{"join_on_condition",
			`SELECT COUNT(*) AS n FROM users a JOIN users b ON a.id = b.id AND tcp_flags_has_all(b.visits,'BOGUS') WHERE a.id < 0`,
			`SELECT COUNT(*) AS n FROM users a JOIN users b ON a.id = b.id AND tcp_flags_has_all(b.visits,'BOGUS')`},
		{"derived_table_body",
			`SELECT COUNT(*) AS n FROM (SELECT tcp_flag_mask('BOGUS') AS m FROM users WHERE id < 0) s`,
			`SELECT COUNT(*) AS n FROM (SELECT tcp_flag_mask('BOGUS') AS m FROM users) s`},
		{"cte_body",
			`WITH c AS (SELECT tcp_flag_mask('BOGUS') AS m FROM users WHERE id < 0) SELECT COUNT(*) AS n FROM c`,
			`WITH c AS (SELECT tcp_flag_mask('BOGUS') AS m FROM users) SELECT COUNT(*) AS n FROM c`},
		// The DML door is ADR-0031's: a DML predicate is not planned at all, so
		// the binder never sees it and the COMPILE-time fold is what answers.
		// It is here because "one refusal, whatever the door" is the claim.
		{"delete_predicate",
			`DELETE FROM users WHERE id < 0 AND tcp_flags_has_all(visits,'BOGUS')`,
			`DELETE FROM users WHERE tcp_flags_has_all(visits,'BOGUS')`},
		{"update_predicate",
			`UPDATE users SET visits = 1 WHERE id < 0 AND tcp_flags_has_all(visits,'BOGUS')`,
			`UPDATE users SET visits = 1 WHERE tcp_flags_has_all(visits,'BOGUS')`},
	} {
		for _, arm := range []struct {
			label string
			sql   string
		}{{"empty_input", tc.empty}, {"non_empty_input", tc.nonEmpty}} {
			for _, format := range []int16{0, 1} {
				t.Run(fmt.Sprintf("%s/%s/format=%d", tc.name, arm.label, format), func(t *testing.T) {
					res := conn.ExecParams(context.Background(), arm.sql, nil, nil, nil,
						[]int16{format}).Read()
					if res.Err == nil {
						t.Fatalf("ANSWERED %d rows; 22023 naming BOGUS is due\n  SQL: %s",
							len(res.Rows), arm.sql)
					}
					if got := pgErrCode(res.Err); got != "22023" {
						t.Errorf("SQLSTATE %s, want 22023\n  err: %v\n  SQL: %s", got, res.Err, arm.sql)
					}
					if !strings.Contains(res.Err.Error(), bogus) {
						t.Errorf("%q does not name the flag\n  SQL: %s", res.Err, arm.sql)
					}
				})
			}
		}
	}

	// THE BOUNDARY FROM THE OTHER SIDE. Every one of these is the same
	// position with something the fold must NOT refuse, over the same empty
	// input: a name the family knows, a name supplied by a COLUMN or by a
	// call (not knowable before rows — the per-row refusal stands), a NULL
	// name (a NULL mask operand, not a misspelling), and `NS`, which this
	// family accepts as a spelling of AE.
	for _, tc := range []struct {
		name, sql string
		rows      int // an ungrouped COUNT over an empty input is ONE row
	}{
		{"having_valid", `SELECT id AS n FROM users WHERE id < 0 GROUP BY id HAVING COUNT(*) > 0 AND tcp_flags_has_all(MIN(visits),'SYN')`, 0},
		{"order_by_valid", `SELECT id AS n FROM users WHERE id < 0 ORDER BY tcp_flag_mask('SYN')`, 0},
		{"union_all_arm_valid", `SELECT tcp_flag_mask('SYN') AS n FROM users WHERE id < 0 UNION ALL SELECT tcp_flag_mask('ACK') AS n FROM users WHERE id < 0`, 0},
		{"projection_above_group_by_valid", `SELECT id AS g, tcp_flag_mask('SYN') AS n FROM users WHERE id < 0 GROUP BY id`, 0},
		{"window_argument_valid", `SELECT SUM(tcp_flag_mask('SYN')) OVER () AS w FROM users WHERE id < 0`, 0},
		{"derived_table_body_valid", `SELECT COUNT(*) AS n FROM (SELECT tcp_flag_mask('SYN') AS m FROM users WHERE id < 0) s`, 1},
		{"cte_body_valid", `WITH c AS (SELECT tcp_flag_mask('SYN') AS m FROM users WHERE id < 0) SELECT COUNT(*) AS n FROM c`, 1},
		{"exists_subquery_valid", `SELECT COUNT(*) AS n FROM users t WHERE EXISTS (SELECT 1 FROM users u WHERE u.id < 0 AND tcp_flags_has_all(u.visits,'SYN'))`, 1},
		{"a_column_name_in_having_stays_per_row", `SELECT id AS n FROM users WHERE id < 0 GROUP BY id, name HAVING tcp_flags_has_all(MIN(visits), name)`, 0},
		{"a_computed_name_is_not_a_constant", `SELECT tcp_flags_has_all(visits, UPPER('bogus')) AS v FROM users WHERE id < 0`, 0},
		{"a_null_name_in_an_order_by", `SELECT id AS n FROM users WHERE id < 0 ORDER BY tcp_flags_has_all(visits, NULL)`, 0},
		{"ns_is_a_spelling_of_ae", `SELECT id AS g, tcp_flag_mask('NS') AS n FROM users WHERE id < 0 GROUP BY id`, 0},
	} {
		t.Run("control/"+tc.name, func(t *testing.T) {
			res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("refused a query it must answer: %v\n  SQL: %s", res.Err, tc.sql)
			}
			if len(res.Rows) != tc.rows {
				t.Errorf("got %d rows, want %d\n  SQL: %s", len(res.Rows), tc.rows, tc.sql)
			}
		})
	}
}

// A SCALAR SUBQUERY'S COLUMN DECLARES WHAT THE SUBQUERY DECLARES, AND THAT
// SURVIVES MATERIALIZATION (#1018 round 5 review, P2).
//
// A subquery is a whole second query whose type lives in the CATALOG, so only
// a Planner can answer it — and the declaration walks are free functions over
// the logical tree that hold none. `colDecls.subqueryDecl` was nil in every one
// of them and the only caller that passed a resolver was `declaredOutputSchema`
// at the OUTPUT projection, so a scalar-subquery column MATERIALIZED one level
// down (a derived table, a CTE, a set-operation arm) was declared STRING and
// every reader above it fell to float8: all six shapes below declared OID 701
// in BOTH wire formats, with the binary rendering confirming a real float8 on
// the wire. A float64 accumulator over a wide bigint drops digits past 2^53,
// which is the class ADR-0024 exists to prevent.
//
// Every OID and value is live PostgreSQL 17.11's over the same three rows.
func TestPGWireDeclaresSumOverAScalarSubqueryColumn(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql string
		oid       uint32
		want      string
	}{
		{"derived_scalar_subquery_narrow",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT BITWISE_AND(id,3) FROM users u2 WHERE u2.id=1) AS v FROM users) s`,
			20, "3"},
		{"derived_scalar_subquery_wide",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT BITWISE_AND(visits,18) FROM users u2 WHERE u2.id=1) AS v FROM users) s`,
			1700, "0"},
		{"derived_scalar_subquery_bare_int4_column",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT id FROM users u2 WHERE u2.id=1) AS v FROM users) s`,
			20, "3"},
		{"derived_scalar_subquery_bare_int8_column",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT visits FROM users u2 WHERE u2.id=1) AS v FROM users) s`,
			1700, "300"},
		{"derived_scalar_subquery_count",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT COUNT(*) FROM users u2) AS v FROM users) s`,
			1700, "9"},
		{"cte_over_a_scalar_subquery",
			`WITH c AS (SELECT (SELECT BITWISE_AND(id,3) FROM users u2 WHERE u2.id=1) AS v FROM users)
			 SELECT SUM(v) AS v FROM c`, 20, "3"},
		// NOT HERE, and recorded rather than quietly dropped: a
		// set-operation ARM holding a scalar subquery is still declared TEXT
		// beside a bigint arm, so `… (SELECT (SELECT id & 3 …) AS v FROM t
		// UNION ALL SELECT id & 3 FROM t)` is refused 42804 where PostgreSQL
		// answers bigint 9. That arm's declaration comes from a walk this
		// stamp does not reach (setOpArmSchemas), and the disposition is a
		// LOUD refusal, not a wrong value — pre-existing, and the same before
		// this round. See a2_landing_notes_r3.md round 6 §5.
		{"min_over_a_scalar_subquery_column",
			`SELECT SUM(m) AS v FROM (SELECT MIN(v) AS m FROM (SELECT (SELECT BITWISE_AND(id,3) FROM users u2 WHERE u2.id=1) AS v FROM users) s0) s`,
			20, "1"},
		{"windowed_sum_over_a_scalar_subquery_column",
			`SELECT SUM(v) OVER () AS v FROM (SELECT (SELECT BITWISE_AND(id,3) FROM users u2 WHERE u2.id=1) AS v FROM users) s LIMIT 1`,
			20, "3"},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/format=%d", tc.name, format), func(t *testing.T) {
				res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil,
					[]int16{format}).Read()
				if res.Err != nil {
					t.Fatalf("ExecParams: %v\n  SQL: %s", res.Err, tc.sql)
				}
				if got := res.FieldDescriptions[0].DataTypeOID; got != tc.oid {
					t.Errorf("declared OID %d, PostgreSQL declares %d\n  SQL: %s",
						got, tc.oid, tc.sql)
				}
				if len(res.Rows) != 1 {
					t.Fatalf("got %d rows, want 1\n  SQL: %s", len(res.Rows), tc.sql)
				}
				if format == 0 {
					if got := string(res.Rows[0][0]); got != tc.want {
						t.Errorf("rendered %q, want %q\n  SQL: %s", got, tc.want, tc.sql)
					}
				}
			})
		}
	}
}
