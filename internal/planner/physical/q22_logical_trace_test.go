package physical

import (
	"context"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

func TestQ22LogicalAfterOptimize(t *testing.T) {
	if os.Getenv("WADJET_Q22_LOGICAL") != "1" {
		t.Skip("set WADJET_Q22_LOGICAL=1 to enable")
	}
	cat, ctx := setupTPCHCatalog(t)
	parsed, _ := plansql.Parse(tpch.TPCHQueries[22].SQL)
	selInfo, _ := plansql.ExtractSelect(parsed)
	plan, _ := logical.BuildFromSelect(selInfo)
	NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	optimized := logical.Optimize(plan, func(p *logical.Node) {
		NewPlanner(cat).AnnotateScanColumns(context.Background(), p)
	})
	t.Log(optimized.PrettyPrint(0))
}
