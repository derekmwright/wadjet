package logical

import (
	"reflect"
	"sort"
	"testing"
)

func TestSanitizeScanNeeds(t *testing.T) {
	lineitem := &Node{
		Type:        NodeScan,
		TableName:   "lineitem",
		TableAlias:  "l1",
		ScanColumns: []string{"l_orderkey", "l_suppkey", "l_receiptdate", "l_commitdate", "l_quantity"},
	}
	noSchema := &Node{Type: NodeScan, TableName: "lineitem", TableAlias: "l1"}

	tests := []struct {
		name  string
		node  *Node
		needs []string
		want  []string
	}{
		{
			// The Q21 l1 shape (memo §2 A1): alias-qualified dupes resolve
			// to bare, the other side's join key drops.
			"alias dupes and other-table key",
			lineitem,
			[]string{"l1.l_receiptdate", "l_receiptdate", "l1.l_commitdate", "l_commitdate", "s_suppkey", "l_suppkey", "l_orderkey"},
			[]string{"l_commitdate", "l_orderkey", "l_receiptdate", "l_suppkey"},
		},
		{
			// The Q18 outer-leg shape: bare other-table join key drops.
			"bare other-table join key",
			lineitem,
			[]string{"l_quantity", "l_orderkey", "o_orderkey"},
			[]string{"l_orderkey", "l_quantity"},
		},
		{
			// Alias-qualified for a DIFFERENT alias drops even without schema.
			"foreign alias drops without schema",
			noSchema,
			[]string{"s.s_suppkey", "l1.l_orderkey", "l_quantity"},
			[]string{"l_orderkey", "l_quantity"},
		},
		{
			// Derived names are kept: the worker guard's full-width
			// fallback for unknown derivation inputs must stay reachable.
			"derived __ names kept",
			lineitem,
			[]string{"l_quantity", "__having_0"},
			[]string{"__having_0", "l_quantity"},
		},
		{
			// No schema → bare names cannot be judged; keep all.
			"no schema keeps bare names",
			noSchema,
			[]string{"l_quantity", "o_orderkey"},
			[]string{"l_quantity", "o_orderkey"},
		},
		{
			// "attrs.score" where attrs is a ROW-typed column of THIS scan
			// is a field path, not an alias qualifier — kept verbatim.
			// Dropping it broke every dotted Row access when sanitization
			// landed in #249 (filter column "attrs.score" does not exist).
			"row field path kept",
			&Node{
				Type:        NodeScan,
				TableName:   "events",
				ScanColumns: []string{"id", "attrs"},
			},
			[]string{"id", "attrs.score", "other.col"},
			[]string{"attrs.score", "id"},
		},
		{
			// Table name works as the alias when no explicit alias exists.
			"table name as alias",
			&Node{Type: NodeScan, TableName: "orders", ScanColumns: []string{"o_orderkey"}},
			[]string{"orders.o_orderkey"},
			[]string{"o_orderkey"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			needs := make(map[string]bool, len(tc.needs))
			for _, n := range tc.needs {
				needs[n] = true
			}
			got := sanitizeScanNeeds(tc.node, needs)
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sanitizeScanNeeds = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSanitizeScanNeedsKillSwitch(t *testing.T) {
	old := scanColSanitize
	scanColSanitize = false
	defer func() { scanColSanitize = old }()
	n := &Node{Type: NodeScan, TableName: "lineitem", ScanColumns: []string{"l_orderkey"}}
	got := sanitizeScanNeeds(n, map[string]bool{"s_suppkey": true, "l_orderkey": true})
	if len(got) != 2 {
		t.Fatalf("kill switch off should keep raw set, got %v", got)
	}
}
