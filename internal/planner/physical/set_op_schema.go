package physical

import (
	"fmt"
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// unifySetOpSchemas is the result type of a set operation: the first arm's
// column NAMES — SQL says the result takes them — over the COMMON TYPE of the
// two arms per position.
//
// Two rungs of the numeric ladder are reconciled here, because both destroy
// VALUES rather than merely renaming a type:
//
//   - DECIMAL over DECIMAL. The rows reach `batch.FromRows` as their rendered
//     decimal TEXT, boxed at each arm's own scale, and FromRows re-reads that
//     text at the schema's scale — so handing it the first arm's scale
//     truncated the second arm's values: over `DECIMAL(9,2) UNION ALL
//     DECIMAL(18,4)`, 12.7501 came back as 12.75 and 12.7499 as 12.74, and
//     the UNION then counted 8 distinct values where PostgreSQL counts 9
//     (#532). numeric(9,2) ∪ numeric(18,4) is numeric(18,4) there, and
//     parsing the narrower arm's text at the wider scale is exact in both
//     directions.
//
//   - DECIMAL over INTEGER. `numeric ∪ bigint` is `numeric` in PostgreSQL, so
//     the integer arm widens INTO the DECIMAL at the DECIMAL's scale. Left
//     unreconciled this was the silent corruption of #547: the integer arm's
//     box is an int64, NOT text, so FromRows read it into the DECIMAL vector
//     as an UNSCALED carrier and divided every integer by 10^scale (1 came
//     back as 0.01); in the other arm order the DECIMAL arm's rendered text
//     was stored into an INT64 column and failed outright. The unified type is
//     the same the stage DAG resolves — `physical.setOpDecimalTarget`, the
//     ladder's DECIMAL rung — so the two paths agree on what the result type
//     IS, and `coerceSetOpArmRows` moves each integer box into that type
//     before the arms meet.
//
// Anything else — two different TypeIDs neither of which pairs a DECIMAL with
// an integer — is left alone: the stage DAG reconciles those by CASTING each
// arm's projection (`reconcileSetOpArmTypes`), a plan-time rewrite this
// runtime adapter is not the place for.
func unifySetOpSchemas(left, right []parquet.Column) []parquet.Column {
	if len(left) == 0 {
		return right
	}
	if len(right) != len(left) {
		return left
	}
	var out []parquet.Column
	set := func(i int, col parquet.Column) {
		if out == nil {
			out = append(out, left...)
		}
		out[i] = col
	}
	for i := range left {
		l, r := left[i], right[i]
		if l.Type == parquet.TypeDecimal && r.Type == parquet.TypeDecimal {
			if r.Scale <= l.Scale && r.Precision <= l.Precision {
				continue
			}
			col := l
			col.Scale = max(l.Scale, r.Scale)
			col.Precision = max(l.Precision, r.Precision)
			set(i, col)
			continue
		}
		// The DECIMAL/INTEGER rung (#547).
		if meta, ok := setOpDecimalUnify(l, r); ok {
			col := l // the result takes the FIRST arm's NAME
			col.Type = parquet.TypeDecimal
			col.Precision = meta.Precision
			col.Scale = meta.Scale
			col.Nullable = true
			set(i, col)
		}
	}
	if out == nil {
		return left
	}
	return out
}

// setOpDecimalUnify returns the DECIMAL(p,s) a DECIMAL column and an INTEGER
// column must share when they meet as the arms of a set operation, or ok=false
// for any other pair. It delegates the (p,s) rule to setOpDecimalTarget — the
// same function the stage DAG uses (physical.reconcileSetOpArmTypes) — so the
// single-process path and the DAG cannot drift on what the result type IS
// (ADR-0012 item 12).
func setOpDecimalUnify(l, r parquet.Column) (logical.DecimalMeta, bool) {
	lc, ok1 := setOpColTypeFromColumn(l)
	rc, ok2 := setOpColTypeFromColumn(r)
	if !ok1 || !ok2 {
		return logical.DecimalMeta{}, false
	}
	// Only the DECIMAL rung: one arm DECIMAL, the other an integer. Two
	// integers (INT32 ∪ INT64) widen to an integer, which FromRows handles
	// without moving a value, so they are deliberately left for the existing
	// path.
	if lc.typ != parquet.TypeDecimal && rc.typ != parquet.TypeDecimal {
		return logical.DecimalMeta{}, false
	}
	widened, ok := setOpWiden(lc.typ, rc.typ)
	if !ok || widened != parquet.TypeDecimal {
		return logical.DecimalMeta{}, false
	}
	return setOpDecimalTarget([]setOpColType{lc, rc})
}

// setOpColTypeFromColumn adapts a runtime parquet.Column into the setOpColType
// the ladder helpers take. It resolves only the numeric types the DECIMAL rung
// needs; everything else is ok=false, which unifySetOpSchemas reads as "leave
// this column alone".
func setOpColTypeFromColumn(c parquet.Column) (setOpColType, bool) {
	switch c.Type {
	case parquet.TypeDecimal:
		if c.Precision <= 0 {
			return setOpColType{}, false
		}
		return setOpColType{
			typ:      c.Type,
			known:    true,
			dec:      logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale},
			decKnown: true,
		}, true
	case parquet.TypeInt32, parquet.TypeInt64:
		return setOpColType{typ: c.Type, known: true}, true
	default:
		return setOpColType{}, false
	}
}

// coerceSetOpArmRows rewrites an arm's boxed rows so an INTEGER column a set
// operation widened to DECIMAL carries the value the unified DECIMAL column
// expects. The box for an INT32/INT64 column is the raw integer, which
// batch.FromRows would read into a DECIMAL vector as an UNSCALED carrier —
// dividing every integer by 10^scale (#547). Rewriting it to the integer's
// decimal TEXT routes it through the same exact text path a native DECIMAL box
// already takes (ParseDecimalString at FromRows, DecimalTextAt at the dedup
// key), so the integer arrives at its true value at the unified scale and keys
// the same as an equal DECIMAL value.
//
// The value at the unified scale is CHECKED against the same two bounds the
// stage DAG checks (exec.coerceDecimalVector): the Int128 carrier
// (Int128.MulPow10) and the DECLARED PRECISION (setOpDecimalFitsPrecision). A
// value that fails either is a "numeric field overflow" ERROR here, worded to
// match the DAG so both paths refuse the same input the same way — NOT a
// silently saturated Int128Max, which is what routing an out-of-range value
// through DecimalTextAt's comparison-oriented saturating parser would produce
// (a NEW silent wrong answer). Wadjet's finite DECIMAL carrier cannot hold
// PostgreSQL's unconstrained numeric here, so in the overflow band wadjet
// errors where PostgreSQL answers; that residual is the finite-DECIMAL limit
// tracked by #552 (and the constrained-typmod-on-the-wire difference by #542),
// deliberately not closed here.
//
// srcSchema is the arm's OWN schema; target is the unified result schema. They
// correspond by POSITION, and the arm's rows are still keyed by srcSchema's
// names (they have not been re-aligned to the result names yet), so the
// rewrite is applied before alignSetOpRows.
func coerceSetOpArmRows(rows []map[string]any, srcSchema, target []parquet.Column) ([]map[string]any, error) {
	if len(target) == 0 || len(srcSchema) != len(target) {
		return rows, nil
	}
	var cols []int
	for i := range srcSchema {
		if target[i].Type != parquet.TypeDecimal {
			continue
		}
		if srcSchema[i].Type == parquet.TypeInt32 || srcSchema[i].Type == parquet.TypeInt64 {
			cols = append(cols, i)
		}
	}
	if cols == nil {
		return rows, nil
	}
	for _, row := range rows {
		for _, i := range cols {
			name := srcSchema[i].Name
			v, ok := row[name]
			if !ok || v == nil {
				continue
			}
			text, err := setOpCheckedIntDecimalText(v, target[i].Name, target[i].Precision, target[i].Scale)
			if err != nil {
				return nil, err
			}
			row[name] = text
		}
	}
	return rows, nil
}

// setOpCheckedIntDecimalText renders an integer box as the decimal text a
// DECIMAL(precision, scale) column reads at its own scale, after checking the
// value FITS that type. Anything that is not an integer box is returned
// untouched — a DECIMAL box is already its rendered text.
//
// The check mirrors exec.coerceDecimalVector exactly: the unscaled value at
// the output scale is n * 10^scale, which must have an Int128 (the carrier
// bound) and a magnitude below 10^precision (the declared-type bound). Only
// once it passes is the plain integer text handed on — DecimalTextAt and
// ParseDecimalString read it at the unified scale without ever reaching their
// saturating arm, because it is known in range.
func setOpCheckedIntDecimalText(v any, name string, precision, scale int) (any, error) {
	var n int64
	switch iv := v.(type) {
	case int64:
		n = iv
	case int32:
		n = int64(iv)
	case int:
		n = int64(iv)
	default:
		return v, nil
	}
	unscaled := batch.Int128From(n)
	shifted, ok := unscaled.MulPow10(scale)
	if ok {
		ok = setOpDecimalFitsPrecision(shifted, precision)
	}
	if !ok {
		return nil, fmt.Errorf(
			"numeric field overflow: %s does not fit DECIMAL(%d,%d), the type this set operation's "+
				"arms agree on for column %q — a field with precision %d, scale %d holds an "+
				"absolute value below 10^%d",
			unscaled.FormatDecimal(0), precision, scale, name, precision, scale, precision-scale)
	}
	return strconv.FormatInt(n, 10), nil
}

// setOpDecimalFitsPrecision reports whether an unscaled value's MAGNITUDE is
// below 10^precision, the exclusive bound on a DECIMAL(precision, s) column. It
// is exec.decimalFitsPrecision / decimalPrecisionLimit rebuilt on this side of
// the package boundary so the single-process and stage-DAG overflow decisions
// cannot drift: a precision the Int128 carrier cannot even express (39+, which
// setOpDecimalTarget's cap makes unreachable) is treated as "no bound to
// check", exactly as the DAG treats it.
func setOpDecimalFitsPrecision(v batch.Int128, precision int) bool {
	if precision <= 0 || precision > batch.MaxDecimalPrecision {
		return true
	}
	limit, ok := batch.Int128From(1).MulPow10(precision)
	if !ok {
		return true
	}
	mag := v
	if mag.IsNegative() {
		mag = mag.Neg()
		if mag.IsNegative() {
			// -2^127 negates to itself; its magnitude has no Int128, so it is
			// certainly outside any precision this carrier can declare.
			return false
		}
	}
	return mag.Cmp(limit) < 0
}
