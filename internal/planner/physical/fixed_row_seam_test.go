package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle/rowdecl"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The security and aggregate-input consumers need their own metadata check:
// COUNT can observe a non-NULL parent without reading its child values.
func TestFixedRowDeclarationMaterializationConsumers(t *testing.T) {
	expr.RegisterFunc("a3b_seam_row", func(_ []any) any { return rowdecl.Value(1) }, expr.RetRow(rowdecl.Fields()))
	defer expr.DefaultRegistry.Unregister("a3b_seam_row")
	ast, err := plansql.ParseExpression("a3b_seam_row(id)")
	if err != nil {
		t.Fatal(err)
	}
	scan := &logical.Node{Type: logical.NodeScan, TableName: "t", ScanColTypes: map[string]parquet.TypeID{"id": parquet.TypeInt64}}
	barrier := &logical.Node{Type: logical.NodeProject, Projections: []logical.Projection{{Alias: "r", Expr: ast.String(), ASTExpr: ast}}}
	stages := []Stage{{ID: "scan-0", Type: StageScan, TableName: "t", Columns: []string{"id"}}}
	absorbSecurityBarrier(barrier, scan, &stages)
	if len(stages[0].SecurityProjectExprs) != 1 || len(stages[0].SecurityProjectExprs[0].Fields) != 5 {
		t.Fatalf("security declaration %+v", stages[0].SecurityProjectExprs)
	}
	cat, ctx := setupTPCHCatalog(t)
	found := false
	for _, s := range sqlToStages(t, cat, ctx, "SELECT COUNT(a3b_seam_row(n_nationkey)) AS n FROM nation", 3) {
		for _, a := range append(s.AggSpecs, s.FusedAggSpecs...) {
			if a.InputExpr != "" {
				found = true
				if a.InputType != parquet.TypeRow || len(a.InputFields) != 5 {
					t.Errorf("aggregate input %+v", a)
				}
			}
		}
	}
	if !found {
		t.Fatal("aggregate input projection not reached")
	}
}
