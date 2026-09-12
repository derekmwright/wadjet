package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// RefuseInvalidFixedRowField validates a field read from a fixed function
// declaration before rows are evaluated. Unknown declarations retain the
// existing dynamic row_field disposition. Both the binder and compiler ask
// this rule, so a zero-row DAG cannot hide an invalid fixed field name.
func RefuseInvalidFixedRowField(n *plansql.FuncCallNode) error {
	// OutputLabel is the field name for parser-rewritten postfix notation.
	// Explicit row_field calls keep their existing dynamic semantics, including MAPs.
	if n.OutputLabel == "" || !strings.EqualFold(n.Name, "row_field") || len(n.Args) != 2 {
		return nil
	}
	parent, ok := n.Args[0].(*plansql.FuncCallNode)
	if !ok {
		return nil
	}
	field, ok := n.Args[1].(*plansql.Lit)
	if !ok || field.Kind != plansql.LitString {
		return nil
	}
	ret := DefaultRegistry.ReturnType(parent.Name)
	if ret.kind != retFixed {
		return nil
	}
	d, _ := ret.Resolve(len(parent.Args), nil)
	if d.ID != batch.TypeRow {
		return sqlerr.New("42809", "column notation .%s applied to %s, which is not a composite type", field.Value, parent.String())
	}
	if len(d.RowFields()) == 0 {
		return nil
	}
	if _, ok := d.Schema.Field(field.Value); !ok {
		return sqlerr.New("42703", "could not identify column %q in record data type", field.Value)
	}
	return nil
}
