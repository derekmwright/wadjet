package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Fixed function fields are schema-free binding decisions: the registry
// declares their complete type, even when no row reaches the expression.
func refuseInvalidRowFields(node plansql.Node) error {
	var calls []*plansql.FuncCallNode
	walkExpr(node, nil, nil, &calls)
	for _, fc := range calls {
		if err := expr.RefuseInvalidFixedRowField(fc); err != nil {
			return err
		}
	}
	return nil
}
