package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file gates #520: ORDER BY, GROUP BY, DISTINCT / COUNT(DISTINCT),
// MIN/MAX and a hash join over a CIDR column all used to compare the
// column's raw stored TEXT, disagreeing with the inet order `=`/`<`/`>`
// already use since #492 (ADR-0012 item 10). PostgreSQL's inet calls
// '10.0.0.1' and '10.0.0.1/32' the SAME value ("a bare address is a /32
// host route"), and orders '9.0.0.0/8' below '10.0.0.0/8' even though the
// text sorts the other way — neither of which text-order key consumers
// answered.

func cidrKeyConsumerSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
	}}
}

// cidrKeyConsumerRows: id 1 and 2 hold the SAME address spelled two ways
// (a bare host route and its explicit /32); id 3 and 4 hold '9.0.0.0/8' and
// '10.0.0.0/8' — text order puts "9..." above "10...", inet order puts it
// below, because the common bits under the smaller mask decide first.
func cidrKeyConsumerRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "c_cidr": "10.0.0.1"},
		{"id": int64(2), "c_cidr": "10.0.0.1/32"},
		{"id": int64(3), "c_cidr": "9.0.0.0/8"},
		{"id": int64(4), "c_cidr": "10.0.0.0/8"},
	}
}

func openCidrKeyConsumerTable(t *testing.T, name string) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := cidrKeyConsumerSchema()
	if err := db.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester(name, schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, cidrKeyConsumerRows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestCidrGroupByAndDistinctUseInetEquality: '10.0.0.1' and '10.0.0.1/32'
// are one value in PostgreSQL's inet, so GROUP BY and COUNT(DISTINCT) must
// answer ONE group/value for the pair, not two.
func TestCidrGroupByAndDistinctUseInetEquality(t *testing.T) {
	db := openCidrKeyConsumerTable(t, "cidr_keys")
	ctx := context.Background()

	res, err := db.Query(ctx, `SELECT c_cidr, COUNT(*) AS n FROM cidr_keys GROUP BY c_cidr ORDER BY n DESC, c_cidr`)
	if err != nil {
		t.Fatalf("GROUP BY query failed: %v", err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("GROUP BY c_cidr produced %d groups, want 3 (the id=1/id=2 pair collapses to one): %#v", len(res.Rows), res.Rows)
	}
	if n := res.Rows[0]["n"]; n != int64(2) {
		t.Errorf("the collapsed group's count = %#v, want 2", n)
	}

	res, err = db.Query(ctx, `SELECT COUNT(DISTINCT c_cidr) AS n FROM cidr_keys`)
	if err != nil {
		t.Fatalf("COUNT(DISTINCT) query failed: %v", err)
	}
	if got := res.Rows[0]["n"]; got != int64(3) {
		t.Errorf("COUNT(DISTINCT c_cidr) = %#v, want 3", got)
	}
}

// TestCidrOrderByUsesInetOrder: text order puts "9.0.0.0/8" above
// "10.0.0.0/8"; inet order — which `WHERE c_cidr < '10.0.0.0/8'` already
// uses (#492) — puts it below, because the common bits under the smaller
// mask decide before the mask length does.
func TestCidrOrderByUsesInetOrder(t *testing.T) {
	db := openCidrKeyConsumerTable(t, "cidr_keys")
	ctx := context.Background()

	res, err := db.Query(ctx, `SELECT id FROM cidr_keys WHERE id IN (3, 4) ORDER BY c_cidr`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Rows))
	}
	if res.Rows[0]["id"] != int64(3) || res.Rows[1]["id"] != int64(4) {
		t.Errorf("ORDER BY c_cidr answered ids %v, %v; want 3 (9.0.0.0/8) then 4 (10.0.0.0/8) — inet order, not text order",
			res.Rows[0]["id"], res.Rows[1]["id"])
	}
}

// TestCidrMinMaxUseInetOrder: MIN/MAX must agree with the ORDER BY / WHERE
// comparison a query typically sits it next to.
func TestCidrMinMaxUseInetOrder(t *testing.T) {
	db := openCidrKeyConsumerTable(t, "cidr_keys")
	ctx := context.Background()

	res, err := db.Query(ctx, `SELECT MIN(c_cidr) AS mn, MAX(c_cidr) AS mx FROM cidr_keys WHERE id IN (3, 4)`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if res.Rows[0]["mn"] != "9.0.0.0/8" {
		t.Errorf("MIN(c_cidr) = %#v, want \"9.0.0.0/8\" (inet order, not text order)", res.Rows[0]["mn"])
	}
	if res.Rows[0]["mx"] != "10.0.0.0/8" {
		t.Errorf("MAX(c_cidr) = %#v, want \"10.0.0.0/8\"", res.Rows[0]["mx"])
	}
}

// TestCidrHashJoinUsesInetEquality: a hash join on a CIDR key must match
// '10.0.0.1' against '10.0.0.1/32' the same way `=` already does (#492),
// whether or not the join is small enough to broadcast.
func TestCidrHashJoinUsesInetEquality(t *testing.T) {
	db := openCidrKeyConsumerTable(t, "cidr_keys")
	ctx := context.Background()

	schema := cidrKeyConsumerSchema()
	if err := db.CreateTable(ctx, "cidr_keys_r", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("cidr_keys_r", schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(100), "c_cidr": "10.0.0.1/32"}, // matches id=1's "10.0.0.1" by inet equality
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, `SELECT l.id AS lid, r.id AS rid FROM cidr_keys l JOIN cidr_keys_r r ON l.c_cidr = r.c_cidr ORDER BY l.id`)
	if err != nil {
		t.Fatalf("join query failed: %v", err)
	}
	// Both id=1 ("10.0.0.1") and id=2 ("10.0.0.1/32") must match r.id=100.
	if len(res.Rows) != 2 {
		t.Fatalf("join produced %d rows, want 2 (ids 1 and 2 both equal r's 10.0.0.1/32 by inet equality): %#v", len(res.Rows), res.Rows)
	}
	for _, r := range res.Rows {
		if r["rid"] != int64(100) {
			t.Errorf("row %#v: rid should be 100", r)
		}
	}
}
