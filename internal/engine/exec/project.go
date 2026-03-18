package exec

import (
	"context"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Expression computes a value for a row in a batch.
type Expression func(b *batch.RecordBatch, row int) any

// ColumnRef creates an expression that reads a column value.
func ColumnRef(name string) Expression {
	return func(b *batch.RecordBatch, row int) any {
		v := b.ColumnByName(name)
		if v == nil {
			return nil
		}
		return v.GetValue(row)
	}
}

// Literal creates an expression that returns a constant.
func Literal(val any) Expression {
	return func(_ *batch.RecordBatch, _ int) any {
		return val
	}
}

// Float64Expression evaluates to float64 without boxing.
type Float64Expression func(b *batch.RecordBatch, row int) (float64, bool)

// Int64Expression evaluates to int64 without boxing.
type Int64Expression func(b *batch.RecordBatch, row int) (int64, bool)

// ProjectColumn defines an output column of a projection.
type ProjectColumn struct {
	Name        string
	Type        parquet.TypeID
	Expr        Expression
	Float64Eval Float64Expression // optional typed path (avoids interface{} boxing)
	Int64Eval   Int64Expression   // optional typed path
	SourceCol   string            // source column name for type resolution on renames
}

// Project is a UnaryOperator that selects and computes columns.
type Project struct {
	Projections []ProjectColumn
}

func NewProject(projections []ProjectColumn) *Project {
	return &Project{Projections: projections}
}

func (p *Project) Init(_ context.Context) error { return nil }

func (p *Project) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	// Resolve output types from the input batch schema rather than using the
	// pre-declared type (which may be a placeholder). This ensures float columns
	// don't get written into string vectors, which would panic.
	schema := make([]parquet.Column, len(p.Projections))
	for i, proj := range p.Projections {
		typ := proj.Type
		// Try to resolve the actual type from input schema by matching column name
		if idx := in.ColumnIndex(proj.Name); idx >= 0 {
			typ = in.Schema[idx].Type
		} else if proj.SourceCol != "" {
			// Fallback: renamed column (e.g., l_suppkey AS supplier_no) —
			// look up the source column to preserve the original type.
			if idx := in.ColumnIndex(proj.SourceCol); idx >= 0 {
				typ = in.Schema[idx].Type
			}
		}
		schema[i] = parquet.Column{
			Name:     proj.Name,
			Type:     typ,
			Nullable: true,
		}
	}

	activeLen := in.ActiveLen()
	out := batch.NewRecordBatch(schema, activeLen)

	if in.Sel != nil {
		for outRow, idx := range in.Sel {
			for j, proj := range p.Projections {
				val := proj.Expr(in, int(idx))
				out.Columns[j].SetValue(outRow, val)
			}
		}
	} else {
		for i := 0; i < in.Len; i++ {
			for j, proj := range p.Projections {
				val := proj.Expr(in, i)
				out.Columns[j].SetValue(i, val)
			}
		}
	}

	return out, nil
}

func (p *Project) Close() error { return nil }

// Clone returns a new Project that shares the same (immutable) projections.
// Project has no per-instance scratch buffers, so sharing is safe.
func (p *Project) Clone() UnaryOperator {
	return &Project{Projections: p.Projections}
}

// ArithExpr creates an arithmetic expression between two expressions.
func ArithExpr(left, right Expression, op string) Expression {
	return func(b *batch.RecordBatch, row int) any {
		lv := left(b, row)
		rv := right(b, row)
		if lv == nil || rv == nil {
			return nil
		}

		lf := toFloat64(lv)
		rf := toFloat64(rv)

		switch op {
		case "+":
			return lf + rf
		case "-":
			return lf - rf
		case "*":
			return lf * rf
		case "/":
			if rf == 0 {
				return nil
			}
			return lf / rf
		default:
			return nil // unknown op → SQL NULL (validated at plan time)
		}
	}
}
