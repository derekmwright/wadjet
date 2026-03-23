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

// VecFloat64Expression evaluates float64 for all rows at once.
type VecFloat64Expression func(b *batch.RecordBatch, dst []float64, n int) bool

// ProjectColumn defines an output column of a projection.
type ProjectColumn struct {
	Name           string
	Type           parquet.TypeID
	Expr           Expression
	Float64Eval    Float64Expression    // optional typed path (avoids interface{} boxing)
	Int64Eval      Int64Expression      // optional typed path
	VecFloat64Eval VecFloat64Expression // optional vectorized path (entire column at once)
	SourceCol      string               // source column name for type resolution on renames
	DirectCopy     string               // if set, bulk copy this input column (no per-row eval)
}

// Project is a UnaryOperator that selects and computes columns.
type Project struct {
	Projections    []ProjectColumn
	cachedSchema   []parquet.Column // reused across batches after first resolution
	outPool        *batch.BatchPool // output batch pool — eliminates per-batch allocation
	directSrcIdx   []int            // resolved source col indices for DirectCopy (-1 = per-row eval)
	directResolved bool
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

	// Resolve DirectCopy column indices on first batch.
	if !p.directResolved {
		p.directSrcIdx = make([]int, len(p.Projections))
		for j, proj := range p.Projections {
			p.directSrcIdx[j] = -1
			if proj.DirectCopy != "" {
				if idx := in.ColumnIndex(proj.DirectCopy); idx >= 0 {
					p.directSrcIdx[j] = idx
				}
			}
		}
		p.directResolved = true
	}

	// Use typed evaluation paths per-column to avoid interface{} boxing.
	// This applies to both selection-vector and non-sel paths.
	if in.Sel != nil {
		for j, proj := range p.Projections {
			col := out.Columns[j]
			if srcIdx := p.directSrcIdx[j]; srcIdx >= 0 {
				projectGatherColumn(col, in.Columns[srcIdx], in.Sel)
			} else if proj.Float64Eval != nil {
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
			if srcIdx := p.directSrcIdx[j]; srcIdx >= 0 {
				projectCopyColumn(col, in.Columns[srcIdx], in.Len)
			} else if proj.VecFloat64Eval != nil {
				proj.VecFloat64Eval(in, col.Float64Data, in.Len)
			} else if proj.Float64Eval != nil {
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

// projectCopyColumn bulk-copies n rows from src to dst using type-specific memcpy.
// Eliminates per-row function call overhead for pass-through column projections.
func projectCopyColumn(dst, src *batch.Vector, n int) {
	switch src.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		copy(dst.Int64Data[:n], src.Int64Data[:n])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		copy(dst.Int32Data[:n], src.Int32Data[:n])
	case batch.TypeFloat64:
		copy(dst.Float64Data[:n], src.Float64Data[:n])
	case batch.TypeFloat32:
		copy(dst.Float32Data[:n], src.Float32Data[:n])
	case batch.TypeBool:
		copy(dst.BoolData[:n], src.BoolData[:n])
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		dst.BytesData.BulkCopy(0, &src.BytesData, 0, n)
	case batch.TypeDecimal:
		copy(dst.DecimalData.Data[:n], src.DecimalData.Data[:n])
	default:
		// Fallback for nested/unknown types: per-row copy
		for i := 0; i < n; i++ {
			dst.SetValue(i, src.GetValue(i))
		}
	}
	dst.Nulls.CopyFrom(&src.Nulls, n)
}

// projectGatherColumn copies selected rows from src to contiguous dst positions.
// Used when the input has a selection vector.
func projectGatherColumn(dst, src *batch.Vector, sel []uint32) {
	switch src.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for i, idx := range sel {
			dst.Int64Data[i] = src.Int64Data[idx]
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for i, idx := range sel {
			dst.Int32Data[i] = src.Int32Data[idx]
		}
	case batch.TypeFloat64:
		for i, idx := range sel {
			dst.Float64Data[i] = src.Float64Data[idx]
		}
	case batch.TypeFloat32:
		for i, idx := range sel {
			dst.Float32Data[i] = src.Float32Data[idx]
		}
	case batch.TypeBool:
		for i, idx := range sel {
			dst.BoolData[i] = src.BoolData[idx]
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		for i, idx := range sel {
			dst.BytesData.Set(i, src.BytesData.Value(int(idx)))
		}
	case batch.TypeDecimal:
		for i, idx := range sel {
			dst.DecimalData.Data[i] = src.DecimalData.Data[idx]
		}
	default:
		for i, idx := range sel {
			dst.SetValue(i, src.GetValue(int(idx)))
		}
	}
	// Gather null bits
	srcWords := src.Nulls.Words()
	if src.Nulls.HasNulls() {
		for i, idx := range sel {
			if srcWords[idx/64]&(1<<(idx%64)) == 0 {
				dst.Nulls.SetNull(i)
			}
		}
	}
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
