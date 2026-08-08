package worker

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// computedColAppender evaluates one Task.ComputedCols expression per batch
// and appends the result as a boolean column to the shuffle payload
// (exchange subsumption dedup: a dropped filtered sibling exchange's filter
// rides the subsuming raw exchange as a flag).
type computedColAppender struct {
	name string
	ex   expr.Expr
	be   expr.BoolExpr // non-nil fast path
}

func newComputedColAppenders(specs []distributed.ComputedColSpec) ([]computedColAppender, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	out := make([]computedColAppender, 0, len(specs))
	for _, sp := range specs {
		node, err := plansql.ParseExpression(sp.Expr)
		if err != nil {
			return nil, fmt.Errorf("parse computed col %s (%q): %w", sp.Name, sp.Expr, err)
		}
		compiled, err := expr.Compile(node)
		if err != nil {
			return nil, fmt.Errorf("compile computed col %s (%q): %w", sp.Name, sp.Expr, err)
		}
		a := computedColAppender{name: sp.Name, ex: compiled}
		if be, ok := compiled.(expr.BoolExpr); ok {
			a.be = be
		}
		out = append(out, a)
	}
	return out, nil
}

// applyComputedCols returns a shallow-copied batch with each appender's
// boolean column appended. The input batch's vectors are shared, never
// mutated — source batches may be pooled or cache-shared, so the column
// append must not write through to them. Values are computed for every
// PHYSICAL row (shuffle sources are unfiltered scans; Sel is typically nil).
func applyComputedCols(appenders []computedColAppender, dropCols []string, b *batch.RecordBatch) *batch.RecordBatch {
	if len(appenders) == 0 {
		return b
	}
	drop := make(map[string]bool, len(dropCols))
	for _, c := range dropCols {
		drop[c] = true
	}
	cols := make([]*batch.Vector, 0, len(b.Columns)+len(appenders))
	schema := make([]pqt.Column, 0, len(b.Schema)+len(appenders))
	for i, sc := range b.Schema {
		if drop[sc.Name] {
			continue
		}
		cols = append(cols, b.Columns[i])
		schema = append(schema, sc)
	}
	for _, a := range appenders {
		v := batch.NewVector(batch.TypeBool, b.Len)
		if a.be != nil {
			for i := 0; i < b.Len; i++ {
				v.BoolData[i] = a.be.EvalBool(b, i)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				val, _ := a.ex.Eval(b, i).(bool)
				v.BoolData[i] = val
			}
		}
		cols = append(cols, v)
		schema = append(schema, pqt.Column{Name: a.name, Type: pqt.TypeBool})
	}
	return &batch.RecordBatch{Columns: cols, Schema: schema, Len: b.Len, Sel: b.Sel}
}

// filteredSource applies a chain of filter operators to every batch a
// wrapped Source yields — the build-input pre-filter for exchange
// subsumption (executor_fragment.go buildHJ). Batches whose rows are all
// filtered away are skipped.
type filteredSource struct {
	exec.Source
	ops []exec.UnaryOperator
}

func (f *filteredSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	for {
		b, err := f.Source.Next(ctx)
		if err != nil || b == nil {
			return b, err
		}
		for _, op := range f.ops {
			b, err = op.Execute(ctx, b)
			if err != nil {
				return nil, err
			}
			if b == nil {
				break
			}
		}
		if b != nil && b.ActiveLen() > 0 {
			return b, nil
		}
	}
}
