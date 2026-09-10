// This file holds typed evaluation of gathered expression columns.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// evalInt64Column materializes an INTEGER expression result. It is
// evalExprColumn's integer arm, kept beside evalDecimalColumn: the box an
// integer expression produces is an int64 and the vector that holds it without
// loss is an INT64 one.
//
// A box that is not an integer nulls the row rather than being coerced. That
// cannot happen for an expression Int64ResultOf accepted — it is integer
// arithmetic over integer leaves — and nulling is the reading that cannot turn
// a wrong type into a wrong number.
func evalInt64Column(e expr.Expr, b *batch.RecordBatch) *batch.Vector {
	v := batch.NewVector(parquet.TypeInt64, b.Len)
	if cap(v.Int64Data) < b.Len {
		v.Int64Data = make([]int64, b.Len)
	} else {
		v.Int64Data = v.Int64Data[:b.Len]
	}
	emit := func(row, dst int) {
		val := e.Eval(b, row)
		switch x := val.(type) {
		case int64:
			v.Int64Data[dst] = x
			v.Nulls.SetValid(dst)
		case int32:
			v.Int64Data[dst] = int64(x)
			v.Nulls.SetValid(dst)
		case int:
			v.Int64Data[dst] = int64(x)
			v.Nulls.SetValid(dst)
		default:
			v.Nulls.SetNull(dst)
		}
	}
	if b.Sel != nil {
		for i, row := range b.Sel {
			emit(int(row), i)
		}
		return v
	}
	for row := 0; row < b.Len; row++ {
		emit(row, row)
	}
	return v
}

// evalExprColumn builds a new Vector by evaluating e against each row of b.
// Used by applyOutputRenames to materialize wrapped-aggregate and
// wrapped-window columns at gather time. outType is decided once per query by
// newBatchRenamer so it is stable across batches: TypeBool for a boolean
// wrapper (BETWEEN/AND/OR/IN/comparison/IS/LIKE/ANY/ALL over a __agg_/__win_
// column), evaluated on the three-valued protocol so UNKNOWN becomes SQL NULL;
// TypeFloat64 for everything else, the dominant wrapped-aggregate case
// (SUM/N, AVG-like ratios). A non-numeric, non-boolean result (e.g. a CASE
// producing strings) still lands on the float64 path and is nulled — a
// pre-existing bound, but no longer a LEAK: the wrapper is recognized and
// projected, never passed through as internal columns.
func evalExprColumn(e expr.Expr, b *batch.RecordBatch, outType parquet.TypeID,
	decl parquet.Column) *batch.Vector {
	if outType == parquet.TypeBool {
		return evalBoolColumn(e, b)
	}
	// An EXACT DECIMAL result — every wrapped aggregate over a DECIMAL column:
	// `SUM(d) * 2`, `AVG(d) * 100`, `MIN(d) * 2`, and `SUM(d * 2)`, which the
	// gather rewrites to `__agg_0 * 2`. A DECIMAL boxes as its rendered TEXT,
	// which the float64 switch below has no arm for, so every one of them fell
	// to `default: SetNull` and the DAG returned NULL in every row where the
	// single-process path answered (#555 review, R1). The type comes from the
	// input SCHEMA, so it is the same for every batch of one query.
	if _, scale, ok := expr.DecimalResultOf(e, b); ok {
		return evalDecimalColumn(e, b, scale)
	}
	// An INTEGER result, for the same reason and by the same rule (#784).
	// `SELECT SUM(c_i32 * 2)` is rewritten to `SUM(c_i32) * 2` above the
	// aggregate, so this materialization is what declares the client's type:
	// PostgreSQL and the single-process path both say bigint, and a float64
	// vector here handed the same query a different wire OID depending on
	// which engine ran it.
	if expr.Int64ResultOf(e, b) {
		return evalInt64Column(e, b)
	}
	// Everything else the PLAN declared. The two arms above read the input
	// SCHEMA and are kept ahead of it because a schema is a stronger answer
	// than an inference; what this arm adds is every type neither of them
	// names — STRING above all, but also a DATE, a TIMESTAMP or a network
	// address a CASE or a CAST can produce — which fell to the float64 switch
	// below and were nulled row by row (#831, #645).
	//
	// The materialization is exec.Project's, value for value: a vector of the
	// declared type and SetValue per row. That is the point of using the
	// declaration rather than probing a box — the two engines then build the
	// same column from the same expression, and a declaration that is wrong is
	// wrong on both instead of silent on one.
	if outType != parquet.TypeFloat64 && outType != parquet.TypeBool {
		return evalDeclaredColumn(e, b, decl)
	}
	v := batch.NewVector(parquet.TypeFloat64, b.Len)
	if cap(v.Float64Data) < b.Len {
		v.Float64Data = make([]float64, b.Len)
	} else {
		v.Float64Data = v.Float64Data[:b.Len]
	}
	emit := func(row, dst int) {
		val := e.Eval(b, row)
		if val == nil {
			v.Nulls.SetNull(dst)
			return
		}
		switch x := val.(type) {
		case float64:
			v.Float64Data[dst] = x
			v.Nulls.SetValid(dst)
		case float32:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int64:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int32:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case int:
			v.Float64Data[dst] = float64(x)
			v.Nulls.SetValid(dst)
		case bool:
			if x {
				v.Float64Data[dst] = 1
			}
			v.Nulls.SetValid(dst)
		default:
			v.Nulls.SetNull(dst)
		}
	}
	if b.Sel != nil {
		for i, src := range b.Sel {
			emit(int(src), i)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			emit(i, i)
		}
	}
	return v
}

// evalDecimalColumn is evalExprColumn's DECIMAL arm: it materializes an exact
// fixed-point vector at the scale the expression's own type names, writing
// unscaled carriers with no box in between.
func evalDecimalColumn(e expr.Expr, b *batch.RecordBatch, scale int) *batch.Vector {
	v := batch.NewVectorWithScale(parquet.TypeDecimal, b.Len, scale)
	emit := func(row, dst int) {
		if expr.EvalDecimalInto(e, b, row, v, dst) {
			v.Nulls.SetValid(dst)
			return
		}
		v.Nulls.SetNull(dst)
	}
	if b.Sel != nil {
		for i, src := range b.Sel {
			emit(int(src), i)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			emit(i, i)
		}
	}
	return v
}

// exprDecPrecision is the DECLARED precision of an exact DECIMAL rename, for
// the gathered schema. The vector carries only the scale (batch.DecimalColumn
// is Data plus Scale), and the precision is what sizes a parquet leaf and what
// a client reads as the typmod.
func (br *batchRenamer) exprDecPrecision(i int, e expr.Expr, b *batch.RecordBatch) int {
	if p, _, ok := expr.DecimalResultOf(e, b); ok {
		return p
	}
	return 0
}

// evalBoolColumn is evalExprColumn's TypeBool arm: it materializes a real
// boolean vector from a boolean-typed compiled expression, evaluated on the
// three-valued protocol (UNKNOWN → SQL NULL) when the expression implements
// it, else through the boxed Eval path. The dst indexing matches
// evalExprColumn exactly — dense under a selection vector — so the produced
// column aligns with the batch's other columns.
func evalBoolColumn(e expr.Expr, b *batch.RecordBatch) *batch.Vector {
	v := batch.NewVector(parquet.TypeBool, b.Len)
	bn, hasBN := e.(expr.BoolNullExpr)
	emit := func(row, dst int) {
		if hasBN {
			val, null := bn.EvalBoolNull(b, row)
			if null {
				v.Nulls.SetNull(dst)
				return
			}
			v.BoolData[dst] = val
			v.Nulls.SetValid(dst)
			return
		}
		val := e.Eval(b, row)
		if val == nil {
			v.Nulls.SetNull(dst)
			return
		}
		if x, ok := val.(bool); ok {
			v.BoolData[dst] = x
			v.Nulls.SetValid(dst)
			return
		}
		v.Nulls.SetNull(dst)
	}
	if b.Sel != nil {
		for i, src := range b.Sel {
			emit(int(src), i)
		}
	} else {
		for i := 0; i < b.Len; i++ {
			emit(i, i)
		}
	}
	return v
}
