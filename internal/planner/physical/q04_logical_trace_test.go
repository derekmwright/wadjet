package physical

import (
	"context"
	"os"
	"testing"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// TestQ04LogicalAfterOptimize dumps the logical plan tree AFTER the
// optimizer ran. Used to verify whether costBasedJoinReorder is picking
// the correct build side for Q04's semi-join (orders ⨝ lineitem).
//
// Expected: build side = orders (1.5M filtered), probe side = lineitem
// (30M filtered). Hash-join convention is RIGHT=build, LEFT=probe.
//
// If the optimizer puts lineitem on the right side, that explains the
// Q04 build-side regression observed in the SF10 A/B annotation audit.
func TestQ04LogicalAfterOptimize(t *testing.T) {
	if os.Getenv("WADJET_Q04_LOGICAL") != "1" {
		t.Skip("set WADJET_Q04_LOGICAL=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	sql := tpch.TPCHQueries[4].SQL
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	plan, err := logical.BuildFromSelect(selInfo)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	planner := NewPlanner(cat)
	planner.AnnotateScanColumns(ctx, plan)

	t.Log("=== BEFORE optimize ===")
	t.Log(plan.PrettyPrint(0))

	scanAnnotator := func(p *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(context.Background(), p)
	}
	optimized := logical.Optimize(plan, scanAnnotator)
	t.Log("=== AFTER optimize ===")
	t.Log(optimized.PrettyPrint(0))
}
