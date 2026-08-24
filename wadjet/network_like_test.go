package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file gates #497: `WHERE <ipv4/mac/port/protocol column> LIKE '...'`
// PANICKED — not a recoverable query error, a process killer, since it is
// not the one deliberate FatalEvalPanic shape the pipeline drivers convert
// back into an error (exec.recoverFatalEval) and so re-raises untouched all
// the way up through Pipeline.Run/ChainDriver.Push/DB.Query. IPv6/UUID
// LIKE did not crash but silently matched nothing for every pattern.
//
// Root cause: ResolveLikeFilterKernel (internal/engine/exec/kernel/
// compare.go) unconditionally called vec.BytesData.UnsafeStringValue —
// TypeIPv4/TypeMAC/TypePort/TypeProtocol store their data in
// Int64Data/Int32Data (BytesData is empty, hence the index-out-of-range
// panic), and TypeIPv6/TypeUUID DO store into BytesData but as their RAW
// 16-byte binary form, not the human-readable text a LIKE pattern is
// written against.
//
// Decision: wadjet renders every network-native type (and UUID) as text for
// CAST AS STRING and scalar function arguments (#484); LIKE follows the
// same convention (render-to-text, then match) rather than refusing the way
// PostgreSQL does for inet/cidr/macaddr (verified live: `'10.0.0.1'::inet
// LIKE '10.%'` raises "operator does not exist: inet ~~ unknown" — recorded
// as a deliberate divergence near ADR-0012 items 8/9, not an oversight:
// PostgreSQL has no equivalent of wadjet's "these types are text
// everywhere" contract to agree or disagree with here). TypeCIDR already
// worked correctly by construction (stored as plain text) and is included
// below as a regression guard.

func netLikeSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_port", Type: parquet.TypePort, Nullable: true},
		{Name: "c_proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
	}}
}

func openNetLikeFixture(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := netLikeSchema()
	if err := db.CreateTable(ctx, "net_like", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("net_like", schema, nil, ingest.DefaultConfig())
	rows := []map[string]any{
		{
			"id": int64(1), "c_ipv4": "10.1.2.3", "c_ipv6": "2001:db8::1",
			"c_cidr": "192.168.1.0/24", "c_mac": "aa:bb:cc:dd:ee:ff",
			"c_port": int32(443), "c_proto": int32(6),
			"c_uuid": "550e8400-e29b-41d4-a716-446655440000",
		},
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestLikeOnNetworkTypesAndUUID sweeps a matching and a non-matching LIKE
// pattern over all six network-native types plus UUID. Every case must
// answer WITHOUT panicking or crashing the process — the safety half of
// #497 that holds regardless of the render-vs-refuse decision — and, under
// the render-to-text decision, must answer the way matching against the
// column's own CAST-AS-STRING text would.
func TestLikeOnNetworkTypesAndUUID(t *testing.T) {
	db := openNetLikeFixture(t)
	ctx := context.Background()

	cases := []struct {
		name string
		col  string
		like string
		want int64
	}{
		{"ipv4 prefix match", "c_ipv4", "'10.%'", 1},
		{"ipv4 prefix no match", "c_ipv4", "'9.%'", 0},
		{"ipv6 prefix match", "c_ipv6", "'2001:db8%'", 1},
		{"ipv6 prefix no match", "c_ipv6", "'::1%'", 0},
		{"cidr prefix match (guard)", "c_cidr", "'192.168.%'", 1},
		{"cidr prefix no match (guard)", "c_cidr", "'10.%'", 0},
		{"mac prefix match", "c_mac", "'aa:bb:%'", 1},
		{"mac prefix no match", "c_mac", "'11:22:%'", 0},
		{"port exact-ish match", "c_port", "'44%'", 1},
		{"port no match", "c_port", "'80%'", 0},
		{"protocol match", "c_proto", "'6'", 1},
		{"protocol no match", "c_proto", "'17'", 0},
		{"uuid prefix match", "c_uuid", "'550e8400%'", 1},
		{"uuid prefix no match", "c_uuid", "'ffffffff%'", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql := "SELECT COUNT(*) AS n FROM net_like WHERE " + tc.col + " LIKE " + tc.like
			res, err := tmRun(ctx, db, sql)
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			got := res.Rows[0]["n"]
			gotN, ok := got.(int64)
			if !ok {
				t.Fatalf("%s: n = %#v, not int64", sql, got)
			}
			if gotN != tc.want {
				t.Errorf("%s: n = %d, want %d", sql, gotN, tc.want)
			}
		})
	}
}

// TestNotLikeOnNetworkTypes is the negated form: NOT LIKE must not panic
// either, and must complement the un-negated case's answer over one row.
func TestNotLikeOnNetworkTypes(t *testing.T) {
	db := openNetLikeFixture(t)
	ctx := context.Background()

	for _, tc := range []struct {
		col  string
		like string
	}{
		{"c_ipv4", "'10.%'"},
		{"c_ipv6", "'2001:db8%'"},
		{"c_mac", "'aa:bb:%'"},
		{"c_port", "'44%'"},
		{"c_proto", "'6'"},
		{"c_uuid", "'550e8400%'"},
	} {
		t.Run(tc.col, func(t *testing.T) {
			sql := "SELECT COUNT(*) AS n FROM net_like WHERE " + tc.col + " NOT LIKE " + tc.like
			res, err := tmRun(ctx, db, sql)
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			if got := res.Rows[0]["n"].(int64); got != 0 {
				t.Errorf("%s: n = %d, want 0 (the one row matches the positive pattern)", sql, got)
			}
		})
	}
}
