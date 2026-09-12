package exec

import (
	"context"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
)

// LateralOuterProject appends a table-less LATERAL body's SELECT list to the
// OUTER stream: one computed column per item, every input column passed
// through by POSITION.
//
// It is the runtime half of "a LATERAL body with no FROM clause is a
// projection over the outer row" (logical/lateral_dual_body.go, #1033). The
// body's items are expressions over the outer row, so the values are computed
// by the SAME compiled kernels every other computed column goes through — no
// per-type stamp, no text carrier.
//
// The passthrough list is resolved from the FIRST BATCH, exactly as
// LateralEmptyDefault resolves its own: what the outer subtree emits is a
// `SELECT *` question at plan time, and this operator must not narrow it.
//
// AN ITEM NEVER REPLACES AN OUTER COLUMN. A body may publish an item under a
// name the outer row already carries — `LATERAL (SELECT u.id AS customer)` —
// and those are TWO columns, as they are in PostgreSQL: `u.customer` is the
// outer one and `l.customer` is the item. Writing the item over the outer
// column instead answered `SELECT u.id, u.customer` as `1, 1`, which is a
// right answer turned wrong. The colliding item is emitted QUALIFIED by the
// lateral's alias, the spelling the join already uses for a build column that
// collides with a probe column, so both names resolve to their own value.
type LateralOuterProject struct {
	// Cols are the body's items in SELECT-list order: the output name and the
	// compiled expression over the outer row.
	Cols []LateralOuterColumn
	// Alias is the lateral's own alias, used to qualify an item whose name the
	// outer row already carries. Empty when the lateral has none, in which case
	// a colliding item keeps its bare name and is shadowed — there is no
	// spelling that could reach it.
	Alias string

	resolved bool
	inner    *Project
}

// LateralOuterColumn is one item of a table-less LATERAL body: the output
// name, the compiled expression, and the type the PLANNER declared for it —
// which is the type the output vector is allocated as, so it travels with the
// expression rather than being re-derived here.
type LateralOuterColumn struct {
	Name string
	Expr Expression
	Decl expr.DeclType
}

// NewLateralOuterProject returns the operator, or nil when this join has no
// table-less body to compute.
// projectColumn is this item as the projection operator's own column: the
// compiled expression under the planner's declared type, with a computed
// DECIMAL's (p,s) and a ROW's fields carried, because the output column exists
// in no input schema and the operator has nothing else to read them off
// (ADR-0024 item 2).
func (c LateralOuterColumn) projectColumn(name string) ProjectColumn {
	return ProjectColumn{
		Name:      name,
		Type:      c.Decl.ID,
		Expr:      c.Expr,
		Fields:    c.Decl.RowFields(),
		Precision: c.Decl.Precision,
		Scale:     c.Decl.Scale,
	}
}

func NewLateralOuterProject(alias string, cols []LateralOuterColumn) *LateralOuterProject {
	if len(cols) == 0 {
		return nil
	}
	return &LateralOuterProject{
		Alias: alias,
		Cols:  append([]LateralOuterColumn(nil), cols...),
	}
}

func (o *LateralOuterProject) Init(_ context.Context) error { return nil }

func (o *LateralOuterProject) Close() error { return nil }

func (o *LateralOuterProject) Execute(ctx context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	if in == nil {
		return in, nil
	}
	if !o.resolved {
		o.resolve(in)
	}
	if o.inner == nil {
		return in, nil
	}
	return o.inner.Execute(ctx, in)
}

// resolve builds the projection this operator IS: every input column copied by
// position, then the body's items, in SELECT-list order.
func (o *LateralOuterProject) resolve(in *batch.RecordBatch) {
	o.resolved = true
	outer := make(map[string]bool, len(in.Schema))
	projections := make([]ProjectColumn, 0, len(in.Schema)+len(o.Cols))
	for i, col := range in.Schema {
		outer[batch.FoldIdent(col.Name)] = true
		projections = append(projections, ProjectColumn{
			Name:         col.Name,
			Type:         col.Type,
			Fields:       col.Fields,
			SourceCol:    col.Name,
			DirectCopy:   col.Name,
			SourceIdx:    i,
			SourceIdxSet: true,
		})
	}
	for _, c := range o.Cols {
		name := c.Name
		if outer[batch.FoldIdent(name)] && o.Alias != "" {
			name = o.Alias + "." + name
		}
		outer[batch.FoldIdent(name)] = true
		projections = append(projections, c.projectColumn(name))
	}
	o.inner = &Project{Projections: projections}
}
