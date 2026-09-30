// SPDX-License-Identifier: MIT

package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// columnValueName is the one column a column-typed cast's scope declares.
const columnValueName = "__column_value"

// columnValueRef answers a COLUMN-TYPED cast — a correlated re-run's outer
// value, or the value-free stand-in its declaration is made from
// (plansql.CastNode.Column) — as the column reference it stands for, with the
// one-column scope that declares it. A typing walk that meets one types that
// reference instead, so the outer value is typed exactly as the outer column
// is — `coalesce(o.i, x.v)` over an int4 `o.i` is integer, `x.m / o.i`
// divides at DECIMAL(10,0), `coalesce(o.a, x.a)` over an int4[] is int4[] —
// and never by a CAST expression's own rules (an integer CAST declares
// bigint and is not a fixed-point operand; ADR-0012 item 12). ok=false for
// any other cast.
func columnValueRef(n *plansql.CastNode) (*plansql.ColRef, ColDecls, bool) {
	if n == nil || !n.Column {
		return nil, ColDecls{}, false
	}
	col, ok := expr.ColumnOfCastName(n.TypeName)
	if !ok {
		return nil, ColDecls{}, false
	}
	col.Name = columnValueName
	d := ColDecls{
		Types: map[string]parquet.TypeID{columnValueName: col.Type},
		Elems: map[string]parquet.Column{columnValueName: col},
	}
	if col.Type == parquet.TypeDecimal {
		d.Dec = map[string]logical.DecimalMeta{columnValueName: {Precision: col.Precision, Scale: col.Scale}}
	}
	return &plansql.ColRef{Column: columnValueName}, d, true
}
