package wadjet

import (
	"context"
	"testing"
)

// TestGenericStringFuncsRenderNetworkColumnArgumentsAsText is the regression
// guard for #500: internal/engine/expr's stringInputFuncs registry
// (length/concat/upper/lower/starts_with/ends_with/||/...) is a SEPARATE
// argument-rendering registry from networkTextFuncs, the one #484 fixed, so a
// TypeIPv4/TypeMAC column argument to any of these still hit ColRef.Eval's
// raw-encoded-int64 fast path and read the DIGIT STRING of that number
// instead of the dotted-quad/colon-hex address text CAST and every
// networkTextFuncs argument already render.
//
// Every case here runs against a 2-row batch (openNetTypeMatrix,
// network_typed_column_args_test.go) so the vectorized EvalVec path is
// exercised too, not just the per-row fast path a 1-row batch can hide
// behind.
func TestGenericStringFuncsRenderNetworkColumnArgumentsAsText(t *testing.T) {
	db := openNetTypeMatrix(t)
	ctx := context.Background()

	cases := []struct {
		name string
		sql  string
		want any
	}{
		// --- the two types that box as a raw int64 (the reported bug) ---
		{"length(ipv4)", `SELECT length(c_ipv4) AS v FROM net_matrix WHERE id = 1`, int32(len("10.1.2.3"))},
		{"concat(ipv4, '!')", `SELECT concat(c_ipv4, '!') AS v FROM net_matrix WHERE id = 1`, "10.1.2.3!"},
		{"upper(mac)", `SELECT upper(c_mac) AS v FROM net_matrix WHERE id = 1`, "AA:BB:CC:DD:EE:FF"},
		{"lower(mac)", `SELECT lower(c_mac) AS v FROM net_matrix WHERE id = 1`, "aa:bb:cc:dd:ee:ff"},
		{"starts_with(ipv4, '10.')", `SELECT starts_with(c_ipv4, '10.') AS v FROM net_matrix WHERE id = 1`, true},
		{"ends_with(ipv4, '.3')", `SELECT ends_with(c_ipv4, '.3') AS v FROM net_matrix WHERE id = 1`, true},
		{"contains(mac, ':bb:')", `SELECT contains(c_mac, ':bb:') AS v FROM net_matrix WHERE id = 1`, true},
		{"substr(ipv4, 1, 2)", `SELECT substr(c_ipv4, 1, 2) AS v FROM net_matrix WHERE id = 1`, "10"},
		{"|| operator (ipv4)", `SELECT c_ipv4 || '/32' AS v FROM net_matrix WHERE id = 1`, "10.1.2.3/32"},
		{"cast_string(mac)", `SELECT cast_string(c_mac) AS v FROM net_matrix WHERE id = 1`, "aa:bb:cc:dd:ee:ff"},

		// --- already-correct types, pinned so a future change can't break
		// them while "fixing" IPv4/MAC (same regression-guard shape as
		// network_typed_column_args_test.go) ---
		{"length(ipv6) (guard)", `SELECT length(c_ipv6) AS v FROM net_matrix WHERE id = 1`, int32(len("2001:db8::1"))},
		{"length(cidr) (guard)", `SELECT length(c_cidr) AS v FROM net_matrix WHERE id = 1`, int32(len("192.168.1.0/24"))},
		{"length(port) (guard)", `SELECT length(c_port) AS v FROM net_matrix WHERE id = 1`, int32(len("443"))},
		{"length(proto) (guard)", `SELECT length(c_proto) AS v FROM net_matrix WHERE id = 1`, int32(len("6"))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query %q failed: %v", tc.sql, err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("query %q: expected 1 row, got %d", tc.sql, len(res.Rows))
			}
			got := res.Rows[0]["v"]
			if got != tc.want {
				t.Errorf("%s = %#v, want %#v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestGenericStringFuncsRenderNetworkColumnArgumentsAsTextVectorized forces
// FuncCall.EvalVec's batch entry (rather than any single-row fast path) by
// running the same shape over every row of the fixture, checking the vector
// kernel's guard for TypeIPv4/TypeMAC (added alongside TypeDate's) actually
// falls back to the per-row (now fixed) path instead of reading raw
// Int64Data as if it were a bytes arena.
func TestGenericStringFuncsRenderNetworkColumnArgumentsAsTextVectorized(t *testing.T) {
	db := openNetTypeMatrix(t)
	ctx := context.Background()

	res, err := db.Query(ctx, `SELECT id, length(c_ipv4) AS l, upper(c_mac) AS u FROM net_matrix ORDER BY id`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	want := []struct {
		l int32
		u string
	}{
		{int32(len("10.1.2.3")), "AA:BB:CC:DD:EE:FF"},
		{int32(len("192.168.1.1")), "11:22:33:44:55:66"},
	}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
	}
	for i, r := range res.Rows {
		if r["l"] != want[i].l {
			t.Errorf("row %d: length(c_ipv4) = %#v, want %#v", i, r["l"], want[i].l)
		}
		if r["u"] != want[i].u {
			t.Errorf("row %d: upper(c_mac) = %#v, want %#v", i, r["u"], want[i].u)
		}
	}
}
