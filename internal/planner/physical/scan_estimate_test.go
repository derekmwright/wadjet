package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// setupTPCHCatalog registers every file as 10 MiB (plan_tpch_test.go), so
// expected estimates are fileCount × 10 MiB.
func TestEstimatePlanScanBytes(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	const mb10 = 10 * 1024 * 1024

	buildOptimized := func(sql string) *logical.Node {
		t.Helper()
		parsed, err := plansql.Parse(sql)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		info, err := plansql.ExtractSelect(parsed)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		plan, err := logical.BuildFromSelect(info)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		annotate := func(p *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, p) }
		annotate(plan)
		return logical.Optimize(plan, annotate)
	}

	cases := []struct {
		name  string
		sql   string
		bytes int64
		ok    bool
	}{
		{"single small table", "SELECT n_name FROM nation", 1 * mb10, true},
		{"large table", "SELECT l_orderkey FROM lineitem WHERE l_orderkey = 1", 600 * mb10, true},
		{"join sums all scans", "SELECT n_name, r_name FROM nation JOIN region ON n_regionkey = r_regionkey", 2 * mb10, true},
		{"aggregate", "SELECT COUNT(*) AS c FROM supplier", 1 * mb10, true},
		{"no table", "SELECT 1 AS x", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := buildOptimized(tc.sql)
			got, ok := NewPlanner(cat).EstimatePlanScanBytes(ctx, plan)
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v", ok, tc.ok)
			}
			if got != tc.bytes {
				t.Fatalf("bytes=%d want %d", got, tc.bytes)
			}
		})
	}

	// Unknown table (table functions, virtual sources) is unestimable.
	unknown := &logical.Node{Type: logical.NodeScan, TableName: "no_such_table"}
	if _, ok := NewPlanner(cat).EstimatePlanScanBytes(ctx, unknown); ok {
		t.Fatal("unknown table should be unestimable")
	}

	// A residual subquery expression hides scans from the walk.
	sub := &logical.Node{Type: logical.NodeFilter,
		Predicates: []logical.Predicate{{Raw: "l_quantity > (SELECT avg(l_quantity) FROM lineitem)"}},
		Children:   []*logical.Node{{Type: logical.NodeScan, TableName: "nation"}}}
	if _, ok := NewPlanner(cat).EstimatePlanScanBytes(ctx, sub); ok {
		t.Fatal("residual subquery predicate should be unestimable")
	}
}
