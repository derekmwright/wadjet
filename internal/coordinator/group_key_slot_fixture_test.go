package coordinator

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// The SLOT-COLLISION fixture: a stored table that already carries a column
// named exactly like the slot the planner materializes a derived GROUP BY key
// into.
//
// A stored column in the reserved namespace is never refused at READ — the
// reservation binds where a user MINTS a name, not where one is read back —
// so the planner's slot has to move past it. That leaves two ways to get the
// binding wrong, and this fixture exists to make both of them answer a wrong
// NUMBER rather than a plausible one:
//
//   - two derived keys can be minted into ONE slot when the allocator skips
//     names in scope but not slots it has already issued, and then the second
//     key silently carries the first one's value;
//   - an aggregate over the STORED column can be bound to the slot instead,
//     and then SUM(__gb_expr_0) answers the sum of a group key.
//
// g and h are chosen so `g + 1` and `h * 2` are DIFFERENT partitions of the
// rows — three values each, crossing into twelve non-empty groups — because
// two keys that happened to partition alike would give the same row count
// whichever slot each landed in.
const (
	collSlotTable = "collslot"
	collSlotCol   = "__gb_expr_0"
	// The SECOND slot name, stored as well. With only the first taken the
	// allocator advances exactly ONE step, so the loop body runs once and a
	// bounded scan is never exercised; with both taken the first key must
	// reach `__gb_expr_2` and the second `__gb_expr_3`.
	collSlotCol2 = "__gb_expr_1"
)

func collSlotSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
		{Name: "h", Type: parquet.TypeInt32, Nullable: true},
		// The stored column whose NAME is the planner's slot. Its values are
		// far from any group key's, so a binding that reads it where it meant
		// the key (or the other way) is a wrong number, not a coincidence.
		{Name: collSlotCol, Type: parquet.TypeInt64, Nullable: true},
		{Name: collSlotCol2, Type: parquet.TypeInt64, Nullable: true},
	}}
}

// collSlotData is 240 rows: g cycles 0..2, h cycles 0..3, and the two cycles
// are coprime with each other so all twelve (g, h) pairs occur.
func collSlotData() []map[string]any {
	rows := make([]map[string]any, 0, 240)
	for i := 0; i < 240; i++ {
		rows = append(rows, map[string]any{
			"id":         int64(i),
			"g":          int32(i % 3),
			"h":          int32(i % 4),
			collSlotCol:  int64(1000 + i),
			collSlotCol2: int64(2000 + i),
		})
	}
	return rows
}
