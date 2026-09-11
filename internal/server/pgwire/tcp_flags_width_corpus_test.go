package pgwire

import (
	"context"
	"fmt"
	"testing"
)

// THE DECLARED WIDTH ACROSS EVERY MATERIALIZATION, AND ACROSS THE FUNCTION
// FAMILIES (#1018 round 5 review, promoted round 6).
//
// These two tables are the adversarial reviewer's own corpus — 38 + 21 shapes
// it chose independently of the arc's gates, every `oid` and `want` measured
// live on PostgreSQL 17.11 over the same three rows. They are kept because an
// arc's own cells and the cells that FOUND its two residuals are different
// evidence: A15 (MIN over a computed argument) and A29 (a scalar subquery)
// were the only two disagreements in 41 shapes, and both are round 6's fixes.
// Every cell runs in BOTH wire formats — a value oracle cannot see a right
// value under a wrong OID, and a text-only gate cannot see a binary renderer
// that disagrees with the declaration it was handed.

// The width through a derived table, two levels, a CTE, a set operation, a
// CAST, a JOIN, DISTINCT, ORDER BY … LIMIT, a GROUP BY, CASE, COALESCE, ABS,
// GREATEST, a window slot, a LATERAL, VALUES and a scalar subquery.
func TestPGWireDeclaresPostgresWidthAcrossEveryMaterialization(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql string
		oid       uint32 // live PostgreSQL 17.11
		want      string // live PostgreSQL 17.11 value (text)
	}{
		{"A01_derived_narrow_and", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s`, 20, "6"},
		{"A02_derived_wide_and", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(visits,18) AS v FROM users) s`, 1700, "2"},
		{"A03_derived_regexp_count", `SELECT SUM(v) AS v FROM (SELECT REGEXP_COUNT(name,'a') AS v FROM users) s`, 20, "2"},
		{"A04_derived_int8_fn_bitcount", `SELECT SUM(v) AS v FROM (SELECT BIT_COUNT(visits) AS v FROM users) s`, 1700, ""},
		{"A05_cte_over_derived_narrow", `WITH c AS (SELECT v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s0) SELECT SUM(v) AS v FROM c`, 20, "6"},
		{"A06_two_derived_levels", `SELECT SUM(v) AS v FROM (SELECT v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s1) s2`, 20, "6"},
		{"A07_union_all_two_narrow", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users UNION ALL SELECT BITWISE_AND(id,7) AS v FROM users) s`, 20, "12"},
		{"A08_union_all_mixed", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users UNION ALL SELECT BITWISE_AND(visits,18) AS v FROM users) s`, 1700, "8"},
		{"A09_cast_to_bigint", `SELECT SUM(v) AS v FROM (SELECT CAST(BITWISE_AND(id,3) AS BIGINT) AS v FROM users) s`, 1700, "6"},
		{"A10_cast_to_int", `SELECT SUM(v) AS v FROM (SELECT CAST(BITWISE_AND(visits,18) AS INT) AS v FROM users) s`, 20, "2"},
		{"A11_arith_composite", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) + 1 AS v FROM users) s`, 20, "9"},
		{"A12_literal_direct", `SELECT SUM(3) AS v FROM users`, 20, "9"},
		{"A13_literal_derived", `SELECT SUM(v) AS v FROM (SELECT 3 AS v FROM users) s`, 20, "9"},
		{"A14_agg_output_reused_count", `SELECT SUM(c) AS v FROM (SELECT COUNT(*) AS c FROM users GROUP BY id) s`, 1700, "3"},
		{"A15_min_over_derived_grouped", `SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(id,3)) AS m FROM users GROUP BY id) s`, 20, "6"},
		{"A16_distinct_over_derived", `SELECT SUM(v) AS v FROM (SELECT DISTINCT BITWISE_AND(id,3) AS v FROM users) s`, 20, "6"},
		{"A17_orderby_limit_derived", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users ORDER BY 1 LIMIT 3) s`, 20, "6"},
		{"A18_join_over_derived", `SELECT SUM(s.v) AS v FROM (SELECT BITWISE_AND(id,3) AS v, id AS sid FROM users) s JOIN users u ON s.sid = u.id`, 20, "6"},
		{"A19_case_narrow", `SELECT SUM(v) AS v FROM (SELECT CASE WHEN id>1 THEN BITWISE_AND(id,3) ELSE 0 END AS v FROM users) s`, 20, "5"},
		{"A20_case_mixed", `SELECT SUM(v) AS v FROM (SELECT CASE WHEN id>1 THEN BITWISE_AND(id,3) ELSE BITWISE_AND(visits,18) END AS v FROM users) s`, 1700, "5"},
		{"A21_coalesce_narrow", `SELECT SUM(v) AS v FROM (SELECT COALESCE(BITWISE_AND(id,3), 0) AS v FROM users) s`, 20, "6"},
		{"A22_abs_narrow", `SELECT SUM(v) AS v FROM (SELECT ABS(BITWISE_AND(id,3)) AS v FROM users) s`, 20, "6"},
		{"A23_abs_wide", `SELECT SUM(v) AS v FROM (SELECT ABS(BITWISE_AND(visits,18)) AS v FROM users) s`, 1700, "2"},
		{"A24_mixed_int4_int8literal", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,4611686018427387904) AS v FROM users) s`, 1700, "0"},
		{"A25_mixed_int4col_int8col", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,visits) AS v FROM users) s`, 1700, "2"},
		{"A26_direct_narrow", `SELECT SUM(BITWISE_AND(id,3)) AS v FROM users`, 20, "6"},
		{"A27_window_sum_over_derived", `SELECT SUM(v) OVER () AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s LIMIT 1`, 20, "6"},
		{"A28_outer_sum_of_window", `SELECT SUM(w) AS v FROM (SELECT SUM(v) OVER () AS w FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s) q`, 1700, "18"},
		{"A29_scalar_subquery_derived", `SELECT SUM(v) AS v FROM (SELECT (SELECT BITWISE_AND(id,3) FROM users u2 WHERE u2.id=1) AS v FROM users) s`, 20, "3"},
		{"A32_derived_group_key", `SELECT SUM(k) AS v FROM (SELECT BITWISE_AND(id,3) AS k, COUNT(*) AS c FROM users GROUP BY BITWISE_AND(id,3)) s`, 20, "6"},
		{"A33_shift_narrow", `SELECT SUM(v) AS v FROM (SELECT BITWISE_LEFT_SHIFT(id,1) AS v FROM users) s`, 20, "12"},
		{"A34_not_narrow", `SELECT SUM(v) AS v FROM (SELECT BITWISE_NOT(id) AS v FROM users) s`, 20, "-9"},
		{"A35_xor_narrow", `SELECT SUM(v) AS v FROM (SELECT BITWISE_XOR(id,3) AS v FROM users) s`, 20, "3"},
		{"A36_payload_length_derived", `SELECT SUM(v) AS v FROM (SELECT PAYLOAD_LENGTH(name) AS v FROM users) s`, 20, "13"},
		{"A37_prefix_length_derived", `SELECT SUM(v) AS v FROM (SELECT PREFIX_LENGTH('10.0.0.0/24') AS v FROM users) s`, 20, "72"},
		{"A39_union_narrow_and_literal", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users UNION ALL SELECT 5) s`, 20, "11"},
		{"A40_greatest_mixed", `SELECT SUM(v) AS v FROM (SELECT GREATEST(BITWISE_AND(id,3), BITWISE_AND(visits,18)) AS v FROM users) s`, 1700, "6"},
		// filter over a derived table (a NodeFilter passthrough).
		{"A41_filter_over_derived", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v FROM users) s WHERE v >= 0`, 20, "6"},
		// the width under a HAVING/GROUP BY above the derived table.
		{"A42_grouped_over_derived", `SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(id,3) AS v, id AS g FROM users) s GROUP BY g ORDER BY 1 LIMIT 1`, 20, "1"},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/format=%d", tc.name, format), func(t *testing.T) {
				res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{format}).Read()
				if res.Err != nil {
					t.Errorf("CELL %s fmt=%d ERROR %v\n  SQL: %s", tc.name, format, res.Err, tc.sql)
					return
				}
				got := res.FieldDescriptions[0].DataTypeOID
				rendered := ""
				if len(res.Rows) > 0 && res.Rows[0][0] != nil {
					rendered = string(res.Rows[0][0])
				}
				if format == 0 {
					t.Logf("CELL %s fmt=%d oid=%d want=%d value=%q pgvalue=%q", tc.name, format, got, tc.oid, rendered, tc.want)
				} else {
					t.Logf("CELL %s fmt=%d oid=%d want=%d", tc.name, format, got, tc.oid)
				}
				if got != tc.oid {
					t.Errorf("OID MISMATCH %s fmt=%d: declared %d, PostgreSQL declares %d\n  SQL: %s", tc.name, format, got, tc.oid, tc.sql)
				}
				if format == 0 && tc.want != "" && rendered != tc.want {
					t.Errorf("VALUE MISMATCH %s: rendered %q, PostgreSQL %q\n  SQL: %s", tc.name, rendered, tc.want, tc.sql)
				}
			})
		}
	}
}

// The width table across string, network, temporal, array-adjacent, bitwise
// and flag families, GROUPED and WINDOWED. The rows marked DOMAIN have no
// PostgreSQL equivalent and carry this engine's own reading.
func TestPGWireDeclaresTheIntegerWidthTableAcrossFunctionFamilies(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	for _, tc := range []struct {
		name, sql string
		oid       uint32
		want      string
	}{
		{"G01_length", `SELECT SUM(LENGTH(name)) AS v FROM users`, 20, "13"},
		{"G02_strpos", `SELECT SUM(STRPOS(name,'a')) AS v FROM users`, 20, "3"},
		{"G03_codepoint", `SELECT SUM(CODEPOINT(name)) AS v FROM users`, 20, "294"},
		{"G04_char_length", `SELECT SUM(CHAR_LENGTH(name)) AS v FROM users`, 20, "13"},
		{"G05_prefix_length", `SELECT SUM(PREFIX_LENGTH('10.0.0.0/24')) AS v FROM users`, 20, "72"},
		{"G08_width_bucket", `SELECT SUM(WIDTH_BUCKET(5.0,0.0,10.0,4)) AS v FROM users`, 20, "9"},
		{"G09_bit_count", `SELECT SUM(BIT_COUNT(258)) AS v FROM users`, 1700, "6"},
		{"G10_payload_length", `SELECT SUM(PAYLOAD_LENGTH(name)) AS v FROM users`, 20, "13"},
		// DOMAIN readings (no PostgreSQL equivalent): the table's own rows.
		{"G12_tcp_flag_mask", `SELECT SUM(TCP_FLAG_MASK('SYN','ACK')) AS v FROM users`, 20, "54"},
		{"G13_tcp_flags_from_string", `SELECT SUM(TCP_FLAGS_FROM_STRING('SYN,ACK')) AS v FROM users`, 20, "54"},
		{"G14_from_hex", `SELECT SUM(FROM_HEX('12')) AS v FROM users`, 1700, "54"},
		{"G15_hosts_in_cidr", `SELECT SUM(HOSTS_IN_CIDR('10.0.0.0/24')) AS v FROM users`, 1700, "762"},
		{"G16_ip_ttl", `SELECT SUM(IP_TTL('10.0.0.1')) AS v FROM users`, 20, ""},
		// WINDOWED twins.
		{"W01_length", `SELECT SUM(LENGTH(name)) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 20, "5"},
		{"W02_prefix_length", `SELECT SUM(PREFIX_LENGTH('10.0.0.0/24')) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 20, "24"},
		{"W03_bit_count", `SELECT SUM(BIT_COUNT(258)) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 1700, "2"},
		{"W04_codepoint", `SELECT SUM(CODEPOINT(name)) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 20, "97"},
		{"W05_tcp_flag_mask", `SELECT SUM(TCP_FLAG_MASK('SYN','ACK')) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 20, "18"},
		{"W06_from_hex", `SELECT SUM(FROM_HEX('12')) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 1700, "18"},
		{"W07_width_bucket", `SELECT SUM(WIDTH_BUCKET(5.0,0.0,10.0,4)) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 20, "3"},
		{"W08_strpos", `SELECT SUM(STRPOS(name,'a')) OVER (PARTITION BY id) AS v FROM users WHERE id=1`, 20, "1"},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/format=%d", tc.name, format), func(t *testing.T) {
				res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{format}).Read()
				if res.Err != nil {
					t.Errorf("CELL %s fmt=%d ERROR %v", tc.name, format, res.Err)
					return
				}
				got := res.FieldDescriptions[0].DataTypeOID
				r := ""
				if len(res.Rows) > 0 && res.Rows[0][0] != nil {
					r = string(res.Rows[0][0])
				}
				t.Logf("CELL %s fmt=%d oid=%d want=%d value=%q pg=%q", tc.name, format, got, tc.oid, r, tc.want)
				if got != tc.oid {
					t.Errorf("OID MISMATCH %s fmt=%d: declared %d, want %d", tc.name, format, got, tc.oid)
				}
				if format == 0 && tc.want != "" && r != tc.want {
					t.Errorf("VALUE MISMATCH %s: %q, want %q", tc.name, r, tc.want)
				}
			})
		}
	}
}
