// SPDX-License-Identifier: MIT

package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// A join whose condition qualifies a column by a relation NEITHER side holds
// is refused, not matched against a sibling's column of the same name — the
// plan #1299's reorderer built (`t.order_id = o.id` on a join of s and t).
func TestAJoinKeyNamingARelationNeitherSideHoldsIsRefused(t *testing.T) {
	scan := func(table, alias string, cols ...string) *logical.Node {
		n := logical.NewScan(table, alias)
		n.ScanColumns = cols
		return n
	}
	stranded := logical.NewJoin(scan("lat_item", "s", "id", "order_id"),
		scan("lat_item", "t", "id", "order_id"), "inner", "t.order_id = o.id")
	err := refuseStrandedJoinQualifier(stranded)
	if err == nil || !strings.Contains(err.Error(), "o.id names a relation neither side of this join holds") {
		t.Fatalf("got %v, want the stranded-qualifier refusal", err)
	}
	held := logical.NewJoin(logical.NewJoin(scan("lat_ord", "o", "id"),
		scan("lat_item", "s", "id", "order_id"), "inner", "s.order_id = o.id"),
		scan("lat_item", "t", "id", "order_id"), "inner", "t.order_id = o.id")
	if err := refuseStrandedJoinQualifier(held); err != nil {
		t.Fatalf("a condition whose relations are both below the join was refused: %v", err)
	}
	// A ROW field path's "qualifier" is a COLUMN of a side (`c_row.b`).
	fieldPath := logical.NewJoin(scan("typemx_nested", "n", "id", "c_row"), scan("decpair", "d", "b"),
		"left", "c_row.b = d.b")
	if err := refuseStrandedJoinQualifier(fieldPath); err != nil {
		t.Fatalf("a ROW field path was refused as a stranded qualifier: %v", err)
	}
	// A qualifier is matched without case and by table name too.
	byTable := logical.NewJoin(scan("lat_ord", "", "id"), scan("lat_item", "i", "order_id"),
		"inner", "i.order_id = LAT_ORD.id")
	if err := refuseStrandedJoinQualifier(byTable); err != nil {
		t.Fatalf("a table-name qualifier was refused: %v", err)
	}
}
