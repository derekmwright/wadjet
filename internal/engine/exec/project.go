package exec

import (
	"context"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Expression computes a value for a row in a batch.
type Expression func(b *batch.RecordBatch, row int) any

// ColumnRef creates an expression that reads a column value.
// The column index is resolved on first call and cached for subsequent rows.
func ColumnRef(name string) Expression {
	cachedIdx := -2 // -2 = unresolved
	return func(b *batch.RecordBatch, row int) any {
		if cachedIdx == -2 {
			cachedIdx = b.ColumnIndex(name)
		}
		if cachedIdx < 0 {
			return nil
		}
		return b.Columns[cachedIdx].GetValue(row)
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
	Projections  []ProjectColumn
	cachedSchema []parquet.Column   // reused across batches after first resolution
	outPool      *batch.BatchPool   // output batch pool — eliminates per-batch allocation
}

func NewProject(projections []ProjectColumn) *Project {
	return &Project{Projections: projections}
}

func (p *Project) Init(_ context.Context) error { return nil }

func (p *Project) Execute(_ context.Context, in *batch.RecordBatch) (*batch.RecordBatch, error) {
	schema := p.cachedSchema
	if schema == nil {
		// Resolve output types from the input batch schema rather than using the
		// pre-declared type (which may be a placeholder). This ensures float columns
		// don't get written into string vectors, which would panic.
		schema = make([]parquet.Column, len(p.Projections))
		for i, proj := range p.Projections {
			typ := proj.Type
			if idx := in.ColumnIndex(proj.Name); idx >= 0 {
				typ = in.Schema[idx].Type
			} else if proj.SourceCol != "" {
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
		p.cachedSchema = schema
	}

	activeLen := in.ActiveLen()
	// Use pooled output batch to avoid per-batch allocation. Downstream sinks
	// that store batches (Sort) call Detach() which unlinks from the pool,
	// while sinks that only read (Aggregate) allow proper pool recycling.
	if p.outPool == nil {
		p.outPool = batch.NewBatchPool(schema, batch.DefaultBatchSize)
	}
	out := p.outPool.GetForSize(activeLen)

	// Use typed evaluation paths per-column to avoid interface{} boxing.
	// This applies to both selection-vector and non-sel paths.
	if in.Sel != nil {
		for j, proj := range p.Projections {
			col := out.Columns[j]
			if proj.Float64Eval != nil {
				for outRow, idx := range in.Sel {
					v, ok := proj.Float64Eval(in, int(idx))
					if ok {
						col.Float64Data[outRow] = v
					} else {
						col.Nulls.SetNull(outRow)
					}
				}
			} else if proj.Int64Eval != nil {
				for outRow, idx := range in.Sel {
					v, ok := proj.Int64Eval(in, int(idx))
					if ok {
						col.Int64Data[outRow] = v
					} else {
						col.Nulls.SetNull(outRow)
					}
				}
			} else {
				for outRow, idx := range in.Sel {
					col.SetValue(outRow, proj.Expr(in, int(idx)))
				}
			}
		}
	} else {
		for j, proj := range p.Projections {
			col := out.Columns[j]
			if proj.Float64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := proj.Float64Eval(in, i)
					if ok {
						col.Float64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else if proj.Int64Eval != nil {
				for i := 0; i < in.Len; i++ {
					v, ok := proj.Int64Eval(in, i)
					if ok {
						col.Int64Data[i] = v
					} else {
						col.Nulls.SetNull(i)
					}
				}
			} else {
				for i := 0; i < in.Len; i++ {
					col.SetValue(i, proj.Expr(in, i))
				}
			}
		}
	}

	return out, nil
}

func (p *Project) Close() error { return nil }

// Clone returns a new Project that shares the same (immutable) projections.
// Each clone gets its own pool (created lazily on first Execute).
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
