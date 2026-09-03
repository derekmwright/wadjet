package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The LATERAL fixture: an orders table and a line-items table, with one order
// that has NO items.
//
// It is the fixture `test/lateral_join_test.go` builds, under names that
// cannot collide with the TPC-H `orders`/`lineitem` any suite in this package
// might also load. It rides in tmdTables for the reason every other fixture
// there does: no type-matrix corpus entry names it, and the type matrix cannot
// stand in for it — nothing there is a correlated LATERAL, which is a JOIN the
// planner MANUFACTURES from a subquery rather than one the query wrote.
//
// The order with no items is the whole point. Three of #767's four shapes are
// about what happens to an outer row the inner side does not match, and a
// fixture where every outer row matches cannot see any of them.
const (
	latOrdTable  = "lat_ord"
	latItemTable = "lat_item"
)

func latOrdSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "customer", Type: parquet.TypeString},
		{Name: "total", Type: parquet.TypeFloat64},
	}}
}

func latOrdData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "customer": "Alice", "total": 150.0},
		{"id": int64(2), "customer": "Bob", "total": 200.0},
		{"id": int64(3), "customer": "Carol", "total": 0.0},
	}
}

func latItemSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "order_id", Type: parquet.TypeInt64},
		{Name: "product", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}}
}

func latItemData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "order_id": int64(1), "product": "Widget", "amount": 50.0},
		{"id": int64(2), "order_id": int64(1), "product": "Gadget", "amount": 100.0},
		{"id": int64(3), "order_id": int64(2), "product": "Widget", "amount": 75.0},
		{"id": int64(4), "order_id": int64(2), "product": "Doohickey", "amount": 125.0},
		// Order 3 (Carol) deliberately has none.
	}
}
