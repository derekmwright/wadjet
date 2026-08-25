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
//
// A count alone can pass by accident — two wrong keys can still add up to
// the right total, the same trap ADR-0012 calls out for the DECIMAL set-op
// key (#499's "the VALUES, not only their count: two wrong keys can
// cancel"). This asserts the emitted VALUES too: the collapsed group's own
// c_cidr comes back as "10.0.0.1" (the first row's own raw text — GROUP BY
// picks a representative the same way a value that keys equal but renders
// two ways always does, and this pins WHICH one so a future change to that
// choice is a visible diff, not silent), and the three DISTINCT values are
// exactly the three inet-distinct addresses, not e.g. two copies of one
// spelling with a real value dropped.
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
	if v := res.Rows[0]["c_cidr"]; v != "10.0.0.1" {
		t.Errorf("the collapsed group's emitted c_cidr = %#v, want \"10.0.0.1\"", v)
	}
	wantGroups := map[string]int64{"10.0.0.1": 2, "9.0.0.0/8": 1, "10.0.0.0/8": 1}
	gotGroups := map[string]int64{}
	for _, r := range res.Rows {
		gotGroups[r["c_cidr"].(string)] = r["n"].(int64)
	}
	if len(gotGroups) != len(wantGroups) {
		t.Fatalf("GROUP BY c_cidr emitted values %v, want %v", gotGroups, wantGroups)
	}
	for k, want := range wantGroups {
		if got := gotGroups[k]; got != want {
			t.Errorf("group %q count = %d, want %d (full result: %v)", k, got, want, gotGroups)
		}
	}

	res, err = db.Query(ctx, `SELECT COUNT(DISTINCT c_cidr) AS n FROM cidr_keys`)
	if err != nil {
		t.Fatalf("COUNT(DISTINCT) query failed: %v", err)
	}
	if got := res.Rows[0]["n"]; got != int64(3) {
		t.Errorf("COUNT(DISTINCT c_cidr) = %#v, want 3", got)
	}

	res, err = db.Query(ctx, `SELECT DISTINCT c_cidr FROM cidr_keys ORDER BY c_cidr`)
	if err != nil {
		t.Fatalf("DISTINCT query failed: %v", err)
	}
	wantDistinct := []string{"9.0.0.0/8", "10.0.0.0/8", "10.0.0.1"} // inet order (#520): 9.../8 sorts before 10.../8
	if len(res.Rows) != len(wantDistinct) {
		t.Fatalf("SELECT DISTINCT c_cidr = %#v, want %v", res.Rows, wantDistinct)
	}
	for i, want := range wantDistinct {
		if got := res.Rows[i]["c_cidr"]; got != want {
			t.Errorf("DISTINCT row %d = %#v, want %q", i, got, want)
		}
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

// The ROW-field-path fixture (#568). `rw` carries a CIDR field and an INT64
// field holding the same ordering information as the flat columns beside
// them, so one query can ask the same question three ways — of a flat
// column, of the whole ROW, and of a field PATH into it — and only the
// third answers differently.
func cidrRowFieldSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "c_flat", Type: parquet.TypeCIDR},
		{Name: "rw", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "c", Type: parquet.TypeCIDR},
			{Name: "n", Type: parquet.TypeInt64},
		}},
	}}
}

// The three addresses order 9 < 10 < 192 as inet and 10 < 192 < 9 as text,
// and the three integers order 9 < 10 < 192 numerically and 10 < 192 < 9 as
// text. One fixture, two types, the same inversion.
func cidrRowFieldRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "c_flat": "9.0.0.0/8", "rw": map[string]any{"c": "9.0.0.0/8", "n": int64(9)}},
		{"id": int64(2), "c_flat": "10.0.0.0/8", "rw": map[string]any{"c": "10.0.0.0/8", "n": int64(10)}},
		{"id": int64(3), "c_flat": "192.168.1.0/24", "rw": map[string]any{"c": "192.168.1.0/24", "n": int64(192)}},
	}
}

// TestRowFieldPathLosesTheFieldsDeclaredType pins #568: `ORDER BY rw.c` sorts
// a ROW's CIDR field by its stored TEXT — 9.0.0.0/8 LAST — while `ORDER BY rw`
// over the same values sorts the whole row by inet and puts it FIRST.
//
// The sort is not what is wrong. A ROW field path is DECLARED STRING all the
// way down: physical.colRefDeclaredType looks the field name up among the
// input's own columns, where `c` is not one — the declaration lives in `rw`'s
// parquet.Column.Fields, which that lookup never consults — so the projection
// keeps its TypeString default, exec.Project cannot correct it
// (resolvePlainColumn answers -1 for a field path), and
// kernel.ResolveSortCompare is then handed a STRING column and dispatches
// accordingly. The whole-ROW comparator keeps TypeRow with its Fields and
// reaches container_sort.go's TypeCIDR arm, which is why the two disagree.
//
// It is not a CIDR defect. `ORDER BY rw.n` sorts an INT64 field as TEXT by the
// same mechanism, and that is a wrong answer with no network type in it at
// all — which is why this is pinned as its own issue rather than folded into
// ADR-0012 item 10's residual list, where only the CIDR half would be visible.
//
// Both halves are load-bearing: the flat column and the whole ROW are gated
// CORRECT so the working paths cannot regress behind the pin, and the field
// paths are asserted to answer TEXT order, so the pin fails the moment either
// starts answering the value's own order. Delete it then and gate all four.
func TestRowFieldPathLosesTheFieldsDeclaredType(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := cidrRowFieldSchema()
	if err := db.CreateTable(ctx, "cidr_rowfield", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("cidr_rowfield", schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, cidrRowFieldRows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	ids := func(t *testing.T, sql string) []int64 {
		t.Helper()
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		out := make([]int64, 0, len(res.Rows))
		for _, r := range res.Rows {
			out = append(out, r["id"].(int64))
		}
		return out
	}
	eq := func(a, b []int64) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	// The two paths that already follow the value's own order, gated so they
	// cannot regress behind the pin below.
	for _, tc := range []struct{ name, sql string }{
		{"flat cidr column", "SELECT id FROM cidr_rowfield ORDER BY c_flat"},
		{"whole row", "SELECT id FROM cidr_rowfield ORDER BY rw"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ids(t, tc.sql); !eq(got, []int64{1, 2, 3}) {
				t.Errorf("%s\n  got  %v\n  want [1 2 3] (inet order: 9.0.0.0/8 first)", tc.sql, got)
			}
		})
	}

	// The field paths, pinned at TEXT order.
	for _, tc := range []struct {
		name, sql string
		why       string
	}{
		{"cidr field path", "SELECT id FROM cidr_rowfield ORDER BY rw.c",
			"inet order is [1 2 3]; text order puts 9.0.0.0/8 last"},
		{"int64 field path", "SELECT id FROM cidr_rowfield ORDER BY rw.n",
			"numeric order is [1 2 3]; text order sorts \"10\" and \"192\" above \"9\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ids(t, tc.sql)
			if eq(got, []int64{1, 2, 3}) {
				t.Errorf("%s now answers the value's own order — #568 is FIXED.\n"+
					"Delete this pin and gate every field path alongside the flat column.", tc.sql)
				return
			}
			if !eq(got, []int64{2, 3, 1}) {
				t.Errorf("#568's shape changed — re-read it before re-pinning\n  %s\n  got  %v\n  want [2 3 1] (today's TEXT order; %s)",
					tc.sql, got, tc.why)
				return
			}
			t.Logf("known divergence, NOT gated (#568): %s answers %v, the field's own order is [1 2 3] (%s)",
				tc.sql, got, tc.why)
		})
	}

	// The output TYPE moves with the order, and is the same cause seen from
	// the other side: a field path's projected column is declared STRING, so
	// an INT64 field comes back as text.
	t.Run("field path projects as text", func(t *testing.T) {
		res, err := db.Query(ctx, "SELECT rw.n AS n FROM cidr_rowfield ORDER BY id")
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(res.Rows) == 0 {
			t.Fatal("no rows")
		}
		switch v := res.Rows[0]["n"].(type) {
		case int64:
			t.Errorf("rw.n now comes back as int64(%d) — #568 is FIXED. Delete this pin.", v)
		case string:
			t.Logf("known divergence, NOT gated (#568): rw.n over an INT64 field comes back as string(%q)", v)
		default:
			t.Errorf("#568's shape changed — rw.n came back as %T(%v)", v, v)
		}
	})
}
