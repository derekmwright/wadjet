// SPDX-License-Identifier: MIT

package expr

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The DECLARED shape of an operand, for the sites that read a container's box
// (arc CW round 3, ADR-0045 §2): a CAST that renders or converts a container,
// and every comparator that orders two of them. The box alone cannot say what
// it holds — a TIMESTAMP element is an int64 of epoch milliseconds, a DATE
// element an int32 or int64 day count, a DECIMAL element its text — so these
// sites need the declaration, and the declaration is the one the planner's
// declared-output walk computes for the same expression (the one that types a
// projection's output vector), asked of the same AST against the input batch's
// executed columns.
//
// Round 2 re-derived it instead, from a narrow operand walk (a column's vector,
// a cast's element, a constructor of those): every element another expression
// built — COALESCE, CASE, GREATEST, a scalar subquery, a subscript — had no
// declaration and printed its box raw (`CAST(ARRAY[COALESCE(ts, …)] AS TEXT)`
// = `{1704070800000}`), while the PROJECTION of the same array, typed by the
// walk, was right. One walk now answers both.
//
// The walk lives in the planner, above this package; the planner registers it
// (SetShapeResolver) and every binary that compiles SQL links the planner.

// ShapeResolver answers the declared shape of node over the given input
// columns — nil when the walk declines — with sub resolving a scalar
// subquery's declared column where the compile knew how.
type ShapeResolver func(node plansql.Node, schema []parquet.Column, sub SubqueryDeclFunc) *parquet.Column

var shapeResolver atomic.Pointer[ShapeResolver]

// SetShapeResolver installs the planner's declared-output walk.
func SetShapeResolver(f ShapeResolver) { shapeResolver.Store(&f) }

// operandDecl is one operand's declaration source: its AST, resolved once
// against the first input batch that reaches it (an operator's input keeps
// one schema for the life of the compiled expression).
type operandDecl struct {
	node plansql.Node
	sub  SubqueryDeclFunc
	res  atomic.Pointer[resolvedDecl]
}

type resolvedDecl struct{ col *parquet.Column }

func newOperandDecl(node plansql.Node, ctx *compileContext) *operandDecl {
	if node == nil {
		return nil
	}
	d := &operandDecl{node: node}
	if ctx != nil {
		d.sub = ctx.subqueryDecl
	}
	return d
}

// shape is operand's declared shape against b. Without an AST (an expression
// built outside the compiler) it is the operand's own vector when the operand
// is a column reference, and nil otherwise — the renderer then degrades to the
// box, which is right for every element type whose box is its value.
func (d *operandDecl) shape(b *batch.RecordBatch, row int, operand Expr) *parquet.Column {
	if d != nil {
		r := d.res.Load()
		if r == nil {
			if f := shapeResolver.Load(); f != nil {
				r = &resolvedDecl{col: (*f)(d.node, batchSchema(b), d.sub)}
				if b != nil {
					d.res.Store(r)
				}
			}
		}
		if r != nil && r.col != nil {
			return r.col
		}
	}
	if cr, ok := operand.(*ColRef); ok && b != nil {
		cr.resolve(b)
		if v, _, ok := cr.valueVector(b, row); ok && v != nil {
			c := batch.VectorDecl("", v)
			return &c
		}
	}
	return nil
}

// batchSchema is b's columns as EXECUTED: each vector's own declaration (the
// element and fields a container vector was built with), with the schema's
// name and a DECIMAL's precision, which a vector does not carry.
func batchSchema(b *batch.RecordBatch) []parquet.Column {
	if b == nil {
		return nil
	}
	out := make([]parquet.Column, 0, len(b.Schema))
	for i, sc := range b.Schema {
		if i >= len(b.Columns) || b.Columns[i] == nil {
			out = append(out, sc)
			continue
		}
		c := batch.VectorDecl(sc.Name, b.Columns[i])
		if c.Type == sc.Type {
			c.Precision = sc.Precision
		}
		out = append(out, c)
	}
	return out
}
