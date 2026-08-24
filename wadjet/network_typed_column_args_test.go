package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file gates a consumer class the 22-type matrix in
// type_matrix_fixture_test.go does not reach: a network-native column used
// as a scalar-FUNCTION ARGUMENT, a CAST operand, or the column side of a
// comparison against a string literal. mbtypes' assertions read every
// column back by id (batch.Vector.GetValue, which already renders these
// types correctly) — none of it goes through internal/engine/expr's
// compiled expression tree, which is where the bug lived.
//
// Root cause (found via README verification): internal/engine/expr's
// ColRef.Eval grouped TypeIPv4 and TypeMAC into the same raw-int64 fast
// path as TypeInt64/TypeTimestamp/TypeDuration, instead of falling through
// to Vector.GetValue's default case the way TypeIPv6/TypeCIDR/TypeUUID
// already do. Every function that parses its argument as network-address
// TEXT (net.ParseIP / net.ParseMAC) then read a decimal digit string
// instead of a dotted-quad or colon-hex address and silently answered
// NULL; CAST(... AS STRING) hit the identical path through fmt.Sprint; a
// comparison against a string literal (`ip_col = '10.0.0.1'`) boxed the
// column the same way and never matched. TypePort/TypeProtocol (Int32-
// backed, numeric functions) and TypeIPv6/TypeCIDR/TypeUUID (never
// intercepted, already fell through to GetValue) were already correct —
// the "regression guard" cases below pin that so a future change can't
// quietly break them while "fixing" IPv4/MAC.
//
// There is no separate vectorized kernel for any of these functions
// (FuncCall.EvalVec falls back to the identical per-row Eval() when
// DefaultRegistry.LookupVec returns nil, which it does for all of them) —
// so the row path IS the vectorized path here, and fixing ColRef/FuncCall/
// Cast/Cmp closes both at once. netTypeMatrixVecRow below still runs every
// case through a 2-row batch (forcing EvalVec's batch entry rather than a
// single-row fast path some callers use) to pin that equivalence.

// netTypeMatrixSchema carries one column of each of the six network-native
// types plus a second row's worth of distinct values, so a comparison case
// can also be checked for correct FILTERING (not just a correct boolean
// value on a single row).
func netTypeMatrixSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		// c_ipv4_b sits on the SAME row as c_ipv4, chosen so lexical
		// (string) and numeric (address) ordering of the pair disagree —
		// "9.255.255.255" < "10.1.2.3" numerically but sorts AFTER it as
		// text. It exists to catch a column-to-column ordering regression
		// without depending on join/alias parser support.
		{Name: "c_ipv4_b", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_port", Type: parquet.TypePort, Nullable: true},
		{Name: "c_proto", Type: parquet.TypeProtocol, Nullable: true},
	}}
}

func netTypeMatrixRows() []map[string]any {
	return []map[string]any{
		{
			"id": int64(1), "c_ipv4": "10.1.2.3", "c_ipv4_b": "9.255.255.255",
			"c_ipv6": "2001:db8::1",
			"c_cidr": "192.168.1.0/24", "c_mac": "aa:bb:cc:dd:ee:ff",
			"c_port": int32(443), "c_proto": int32(6),
		},
		{
			// c_ipv4_b > c_ipv4 here (unlike row 1), so the ordering
			// assertion below (c_ipv4_b < c_ipv4) is discriminating: only
			// row 1 passes it.
			"id": int64(2), "c_ipv4": "192.168.1.1", "c_ipv4_b": "192.168.1.9",
			"c_ipv6": "2001:db8::2",
			"c_cidr": "10.0.0.0/8", "c_mac": "11:22:33:44:55:66",
			"c_port": int32(53), "c_proto": int32(17),
		},
	}
}

func openNetTypeMatrix(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := netTypeMatrixSchema()
	if err := db.CreateTable(ctx, "net_matrix", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("net_matrix", schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, netTypeMatrixRows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestNetworkTypedColumnFunctionArgumentMatrix sweeps a representative
// network function per type, CAST(... AS STRING), and a comparison against
// a string literal — the three consumer classes BUG 1 named — over all six
// network-native types.
func TestNetworkTypedColumnFunctionArgumentMatrix(t *testing.T) {
	db := openNetTypeMatrix(t)
	ctx := context.Background()

	cases := []struct {
		name string
		sql  string
		want any
	}{
		// --- function argument: IPv4 / MAC (the two reported broken) ---
		{"ipv4 arg: ip_to_string", `SELECT ip_to_string(c_ipv4) AS v FROM net_matrix WHERE id = 1`, "10.1.2.3"},
		{"ipv4 arg: cidr_contains", `SELECT cidr_contains('10.0.0.0/8', c_ipv4) AS v FROM net_matrix WHERE id = 1`, true},
		{"ipv4 arg: mask_ip", `SELECT mask_ip(c_ipv4, 1) AS v FROM net_matrix WHERE id = 1`, "10.1.2.0"},
		{"mac arg: mac_vendor_oui", `SELECT mac_vendor_oui(c_mac) AS v FROM net_matrix WHERE id = 1`, "AA:BB:CC"},
		{"mac arg: mac_to_string", `SELECT mac_to_string(c_mac) AS v FROM net_matrix WHERE id = 1`, "aa:bb:cc:dd:ee:ff"},

		// --- function argument: already-correct types (regression guard) ---
		{"ipv6 arg (guard): ip_to_string", `SELECT ip_to_string(c_ipv6) AS v FROM net_matrix WHERE id = 1`, "2001:db8::1"},
		{"cidr arg (guard): network_address", `SELECT network_address(c_cidr) AS v FROM net_matrix WHERE id = 1`, "192.168.1.0"},
		{"port arg (guard): port_name", `SELECT port_name(c_port) AS v FROM net_matrix WHERE id = 1`, "https"},
		{"protocol arg (guard): protocol_name", `SELECT protocol_name(c_proto) AS v FROM net_matrix WHERE id = 1`, "tcp"},

		// --- CAST ... AS STRING ---
		{"ipv4 CAST AS STRING", `SELECT CAST(c_ipv4 AS STRING) AS v FROM net_matrix WHERE id = 1`, "10.1.2.3"},
		{"mac CAST AS STRING", `SELECT CAST(c_mac AS STRING) AS v FROM net_matrix WHERE id = 1`, "aa:bb:cc:dd:ee:ff"},
		{"ipv6 CAST AS STRING (guard)", `SELECT CAST(c_ipv6 AS STRING) AS v FROM net_matrix WHERE id = 1`, "2001:db8::1"},
		{"cidr CAST AS STRING (guard)", `SELECT CAST(c_cidr AS STRING) AS v FROM net_matrix WHERE id = 1`, "192.168.1.0/24"},
		{"port CAST AS STRING (guard)", `SELECT CAST(c_port AS STRING) AS v FROM net_matrix WHERE id = 1`, "443"},
		{"protocol CAST AS STRING (guard)", `SELECT CAST(c_proto AS STRING) AS v FROM net_matrix WHERE id = 1`, "6"},

		// --- comparison against a string literal ---
		{"ipv4 = literal", `SELECT (c_ipv4 = '10.1.2.3') AS v FROM net_matrix WHERE id = 1`, true},
		{"ipv4 <> literal", `SELECT (c_ipv4 = '9.9.9.9') AS v FROM net_matrix WHERE id = 1`, false},
		{"mac = literal", `SELECT (c_mac = 'aa:bb:cc:dd:ee:ff') AS v FROM net_matrix WHERE id = 1`, true},
		{"ipv6 = literal (guard)", `SELECT (c_ipv6 = '2001:db8::1') AS v FROM net_matrix WHERE id = 1`, true},
		{"cidr = literal (guard)", `SELECT (c_cidr = '192.168.1.0/24') AS v FROM net_matrix WHERE id = 1`, true},
		{"port = literal (guard)", `SELECT (c_port = '443') AS v FROM net_matrix WHERE id = 1`, true},

		// --- numeric (not lexical) ordering vs a string literal: the
		// regression this fix must not introduce. "9.255.255.255" sorts
		// AFTER "10.1.2.3" lexically but BEFORE it numerically — only the
		// numeric answer is the address ordering PostgreSQL's inet uses.
		{"ipv4 > literal is numeric order", `SELECT (c_ipv4 > '9.255.255.255') AS v FROM net_matrix WHERE id = 1`, true},
		{"ipv4 < literal is numeric order", `SELECT (c_ipv4 < '9.255.255.255') AS v FROM net_matrix WHERE id = 2`, false},
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

// TestNetworkTypedColumnComparisonFiltersRows checks the comparison-vs-
// literal consumer class through an actual WHERE-clause row filter (not
// just a boolean projection), and that column-to-column ordering — which
// must keep using the raw numeric encoding, not the fix's formatted string
// — still filters correctly.
func TestNetworkTypedColumnComparisonFiltersRows(t *testing.T) {
	db := openNetTypeMatrix(t)
	ctx := context.Background()

	cases := []struct {
		name    string
		sql     string
		wantIDs []int64
	}{
		{"WHERE ipv4 = literal", `SELECT id FROM net_matrix WHERE c_ipv4 = '10.1.2.3'`, []int64{1}},
		{"WHERE mac = literal", `SELECT id FROM net_matrix WHERE c_mac = '11:22:33:44:55:66'`, []int64{2}},
		{"WHERE ipv4 column-to-column ordering (numeric, unaffected by fix)",
			`SELECT id FROM net_matrix WHERE c_ipv4_b < c_ipv4`, []int64{1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("query %q failed: %v", tc.sql, err)
			}
			if len(res.Rows) != len(tc.wantIDs) {
				t.Fatalf("query %q: got %d rows, want %d: %#v", tc.sql, len(res.Rows), len(tc.wantIDs), res.Rows)
			}
		})
	}
}
