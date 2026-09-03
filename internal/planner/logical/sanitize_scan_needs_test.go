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
			// is a field path, not an alias qualifier. What the scan must
			// read is the BASE column: the expression compiler resolves the
			// field out of it, and there is no such column as "attrs.score"
			// in any file.
			//
			// Both of the other two answers have shipped and both were
			// wrong. Dropping the reference entirely broke every dotted ROW
			// access when sanitization landed in #249 ("filter column
			// attrs.score does not exist"); keeping the DOTTED spelling —
			// the fix for that — left a name no file schema carries in the
			// stage's requested-column list, where it is intersected away
			// along with the parent, so the field came back NULL on the
			// stage DAG while the single-process path answered correctly
			// (#568).
			"row field path resolves to its base column",
			&Node{
				Type:        NodeScan,
				TableName:   "events",
				ScanColumns: []string{"id", "attrs"},
			},
			[]string{"id", "attrs.score", "other.col"},
			[]string{"attrs", "id"},
		},
		{
			// Table name works as the alias when no explicit alias exists.
			"table name as alias",
			&Node{Type: NodeScan, TableName: "orders", ScanColumns: []string{"o_orderkey"}},
			[]string{"orders.o_orderkey"},
			[]string{"o_orderkey"},
		},
		{
			// #776: a DERIVED TABLE'S ALIAS BECOMES THE SCAN'S ALIAS, so the
			// outer query's reference to that derived table's own SELECT-list
			// alias arrives here QUALIFIED and matching. `x.w` over
			// `(SELECT g*3 AS w FROM lineitem) x` is the Project's OUTPUT
			// name, not a column of lineitem, and keeping it put a name no
			// file has into the read set every emitted-set model reads.
			//
			// The bare spelling was already dropped by the branch below; only
			// the qualified one got through, which is why the CTE spelling of
			// the same query was REFUSED at plan time while the derived-table
			// spelling failed loud at dispatch.
			"own-alias qualified name the table does not have drops",
			lineitem,
			[]string{"l1.w", "l1.l_orderkey", "l_quantity"},
			[]string{"l_orderkey", "l_quantity"},
		},
		{
			// …and the same reference is KEPT when the schema is unknown:
			// full width is the safe failure mode, which is what every other
			// undecidable case here does.
			"own-alias qualified name kept without schema",
			noSchema,
			[]string{"l1.w", "l1.l_orderkey"},
			[]string{"l_orderkey", "w"},
		},
		{
			// A hidden slot arriving qualified keeps the derived-column
			// semantics the bare branch gives it: nothing reads it off a
			// file, and the worker's guard needs it to stay reachable.
			"own-alias qualified hidden slot kept",
			lineitem,
			[]string{"l1.__having_0", "l_quantity"},
			[]string{"__having_0", "l_quantity"},
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
