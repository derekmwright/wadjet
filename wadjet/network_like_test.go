package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"

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

// TestLikeAnswersTheSameAtBothSites sweeps EVERY flat type in the type matrix
// through LIKE at both sites — the scan's vectorized kernel
// (`kernel.ResolveLikeFilterKernel`, reached from a WHERE clause) and the
// row-at-a-time evaluator (`expr.Like`, reached from a SELECT list) — and
// requires them to select the same rows.
//
// #497 gave the kernel a per-type row-to-text function and left the row path
// with the four types ColRef.Eval boxes differently from Vector.GetValue. Two
// of them were still diverging when that fix shipped, and neither was visible
// to any existing gate: `c_date LIKE '20%'` matched 4949 rows through the scan
// and 83 through a projection (the epoch DAY, not the date), and `c_f32 LIKE
// '%1%'` differed by 237 rows (a float64-widened rendering of a float32).
//
// The sweep is per TYPE rather than per known-bad type on purpose: an
// enumerated list of "types that box differently" is the same shape of gap
// #497 was filed for, and this is what makes the list checkable rather than
// believed.
func TestLikeAnswersTheSameAtBothSites(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)

	// Patterns that bite in different places: a contains, an anchored prefix
	// and an anchored suffix. A rendering difference usually shows in only one.
	patterns := []string{"%1%", "2%", "%0", "%-%"}

	for _, c := range typematrix.Columns() {
		if !c.Flat {
			// A container has no `~~` operator in PostgreSQL, and #522 made
			// wadjet refuse the same way at both sites (SQLSTATE 42883)
			// instead of matching Go's own fmt.Sprint of the boxed value.
			tbl := c.TableOf()
			for _, pat := range patterns {
				t.Run(c.Name+"_"+pat, func(t *testing.T) {
					_, err := tmRun(ctx, db, fmt.Sprintf(
						"SELECT COUNT(*) AS n FROM %s WHERE %s LIKE '%s'", tbl, c.Name, pat))
					if err == nil {
						t.Errorf("WHERE %s LIKE '%s' answered instead of refusing", c.Name, pat)
					} else if !strings.Contains(err.Error(), "operator does not exist") {
						t.Errorf("WHERE form: unexpected error: %v", err)
					}
					_, err = tmRun(ctx, db, fmt.Sprintf(
						"SELECT %s LIKE '%s' AS m FROM %s", c.Name, pat, tbl))
					if err == nil {
						t.Errorf("SELECT %s LIKE '%s' answered instead of refusing", c.Name, pat)
					} else if !strings.Contains(err.Error(), "operator does not exist") {
						t.Errorf("projection form: unexpected error: %v", err)
					}
				})
			}
			continue
		}
		for _, pat := range patterns {
			t.Run(c.Name+"_"+pat, func(t *testing.T) {
				kern, err := tmRun(ctx, db, fmt.Sprintf(
					"SELECT COUNT(*) AS n FROM %s WHERE %s LIKE '%s'", typematrix.Table, c.Name, pat))
				if err != nil {
					t.Fatalf("WHERE form: %v", err)
				}
				want, ok := tmAsInt64(kern.Rows[0]["n"])
				if !ok {
					t.Fatalf("COUNT(*) came back as %#v", kern.Rows[0]["n"])
				}
				// The projection form, counted here rather than in SQL: an
				// outer WHERE over the computed column lets the planner push
				// the LIKE back down to the kernel, which would compare the
				// kernel against itself.
				proj, err := tmRun(ctx, db, fmt.Sprintf(
					"SELECT %s LIKE '%s' AS m FROM %s", c.Name, pat, typematrix.Table))
				if err != nil {
					t.Fatalf("projection form: %v", err)
				}
				var got int64
				for _, r := range proj.Rows {
					if b, ok := r["m"].(bool); ok && b {
						got++
					}
				}
				if got != want {
					t.Errorf("%s LIKE '%s': the scan kernel matched %d rows and the row evaluator %d — "+
						"the two sites render this column's value differently", c.Name, pat, want, got)
				}
			})
		}
	}
}
