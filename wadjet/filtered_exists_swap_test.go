package wadjet

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestFilteredExistsLargeInnerKeepsFilter: a decorrelated EXISTS with a
// non-equality correlated condition becomes a semi join with a JoinFilter.
// This shape stacked THREE independent wrong-results bugs (all fixed
// 2026-07-06), each with a distinct failure signature on this data:
//
//   - count 0: dedupSemiAntiBuildSide wrapped the filtered build side in
//     Project(keys)→Distinct, dropping the filter's build column — the
//     probe-time SemiAntiFilter resolved nothing and rejected every row.
//   - count 19: extractCorrelatedCols normalized "i.i_date > o.o_date" to
//     probe-left form without flipping the operator ("o_date > i_date").
//   - count 20: with the inner table >3x the outer, the physical planner
//     swapped to RightSemiJoin, whose probe (markMatchedBuildEntries) marks
//     every key match and never evaluates SemiAntiFilter — the condition
//     silently vanished.
//
// Data: 20 orders (o_id=i, o_date=i), 100 items (5 per order, i_date in
// 0..4). EXISTS(i_date > o_date) holds only for orders with o_date < 4 →
// 4 rows; NOT EXISTS → 16.
func TestFilteredExistsLargeInnerKeepsFilter(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	ordersSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "o_id", Type: parquet.TypeInt64},
		{Name: "o_date", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "orders", ordersSchema, nil); err != nil {
		t.Fatal(err)
	}
	orderRows := make([]map[string]any, 20)
	for i := range orderRows {
		orderRows[i] = map[string]any{"o_id": int64(i), "o_date": int64(i)}
	}
	ing := db.NewIngester("orders", ordersSchema, nil, ingest.Config{MaxBufferRows: 10000})
	if err := ing.Ingest(ctx, orderRows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	itemsSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "i_oid", Type: parquet.TypeInt64},
		{Name: "i_date", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "items", itemsSchema, nil); err != nil {
		t.Fatal(err)
	}
	itemRows := make([]map[string]any, 100)
	for k := range itemRows {
		itemRows[k] = map[string]any{"i_oid": int64(k % 20), "i_date": int64(k / 20)}
	}
	ing = db.NewIngester("items", itemsSchema, nil, ingest.Config{MaxBufferRows: 10000})
	if err := ing.Ingest(ctx, itemRows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	res, err := db.Query(ctx, `SELECT COUNT(*) AS n FROM orders o
		WHERE EXISTS (SELECT 1 FROM items i WHERE i.i_oid = o.o_id AND i.i_date > o.o_date)`)
	if err != nil {
		t.Fatalf("filtered EXISTS query: %v", err)
	}
	if n := res.Rows[0]["n"]; n != int64(4) {
		t.Fatalf("filtered EXISTS count = %v, want 4 (0 = dedup dropped the filter column; 19 = operator not flipped; 20 = swap dropped the JoinFilter)", n)
	}

	// NOT EXISTS over the same shape — the anti-join complement must also
	// keep the filter through the swap decision.
	res, err = db.Query(ctx, `SELECT COUNT(*) AS n FROM orders o
		WHERE NOT EXISTS (SELECT 1 FROM items i WHERE i.i_oid = o.o_id AND i.i_date > o.o_date)`)
	if err != nil {
		t.Fatalf("filtered NOT EXISTS query: %v", err)
	}
	if n := res.Rows[0]["n"]; n != int64(16) {
		t.Fatalf("filtered NOT EXISTS count = %v, want 16 (20 = dedup dropped the filter column; 1 = operator not flipped; 0 = swap dropped the JoinFilter)", n)
	}

	// Outer-column-on-the-left spelling — the no-flip path — must agree.
	res, err = db.Query(ctx, `SELECT COUNT(*) AS n FROM orders o
		WHERE EXISTS (SELECT 1 FROM items i WHERE i.i_oid = o.o_id AND o.o_date < i.i_date)`)
	if err != nil {
		t.Fatalf("filtered EXISTS (outer-left) query: %v", err)
	}
	if n := res.Rows[0]["n"]; n != int64(4) {
		t.Fatalf("filtered EXISTS (outer-left) count = %v, want 4", n)
	}
}
