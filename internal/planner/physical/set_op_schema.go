// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setOpResolveUnknownLiteralArms assigns an untyped item (a quoted literal, a
// bare NULL) the other arm's type before
// unifySetOpSchemas, despite its pipeline vector, so its boxes are read in
// that type. Use the plan-time SetOpArmFacts so local and DAG select the same
// items. Leave positions where both arms are untyped alone: PostgreSQL
// resolves those to text, which they already declare.
func setOpResolveUnknownLiteralArms(left, right []parquet.Column,
	leftFacts, rightFacts SetOpArmFacts) ([]parquet.Column, []parquet.Column) {
	if len(left) == 0 || len(left) != len(right) {
		return left, right
	}
	// A mask is a list of POSITIONS, and a position is only an address while
	// the mask and the runtime schema are the same length.
	if (len(leftFacts) != 0 && len(leftFacts) != len(left)) ||
		(len(rightFacts) != 0 && len(rightFacts) != len(right)) {
		return left, right
	}
	var l2, r2 []parquet.Column
	for i := range left {
		lu, ru := leftFacts.at(i).untyped, rightFacts.at(i).untyped
		if lu == ru {
			continue
		}
		if lu {
			if l2 == nil {
				l2 = append(l2, left...)
			}
			l2[i].Type = right[i].Type
			l2[i].Fields = right[i].Fields
			l2[i].Precision, l2[i].Scale = right[i].Precision, right[i].Scale
			// A bare NULL is one of the two unknown spellings, so the column
			// this arm contributes is nullable whatever the other arm declares.
			l2[i].Nullable = true
			continue
		}
		if r2 == nil {
			r2 = append(r2, right...)
		}
		r2[i].Type = left[i].Type
		r2[i].Fields = left[i].Fields
		r2[i].Precision, r2[i].Scale = left[i].Precision, left[i].Scale
		r2[i].Nullable = true
	}
	if l2 != nil {
		left = l2
	}
	if r2 != nil {
		right = r2
	}
	return left, right
}

// unifySetOpSchemas is the single-process path's result schema: per position,
// the column setOpResultColumn computes from the two arms' runtime columns and
// facts (the one rule the stage planner and the declared output use too: the
// numeric ladder, DECIMAL (p,s) through batch.DecimalCommon, the mark), named
// after the first arm. A position that rule cannot resolve keeps the first
// arm's column as written; never guess a declaration that could move values
// (#555). See docs/internals/local-set-operation-common-schema.md.
func unifySetOpSchemas(left, right []parquet.Column, leftFacts, rightFacts SetOpArmFacts) []parquet.Column {
	if len(left) == 0 {
		return right
	}
	if len(right) != len(left) {
		return left
	}
	var out []parquet.Column
	for i := range left {
		l, r := left[i], right[i]
		want, _, err := setOpResultColumn(
			[]SetOpColType{setOpColTypeOfColumn(l), setOpColTypeOfColumn(r)},
			[]setOpArmFact{leftFacts.at(i), rightFacts.at(i)}, l.Name, "UNION")
		col, ok := setOpColumnFromResult(l, want, err)
		if !ok {
			continue
		}
		if out == nil {
			out = append(out, left...)
		}
		out[i] = col
	}
	if out == nil {
		return left
	}
	return out
}

// coerceSetOpArmRows converts boxes before dedup/FromRows: integers to DECIMAL
// text, DECIMAL text to floats (round the box to float32 for REAL), and integer/
// float boxes to the unified FLOAT32/FLOAT64/INT64 shape. Check DECIMAL range.
// Unrepresentable unified DECIMAL yields "numeric field overflow", SQLSTATE 22003,
// matching exec.coerceDecimalVector; never use saturating comparison parsing.
// PostgreSQL may answer beyond the finite carrier (ADR-0024 items 7 and 1).
// srcSchema is the arm's OWN schema, positionally aligned to target; rows still
// use source names, so coerce before AlignSetOpRows.
// See docs/internals/set-operation-box-coercion.md for the design.
func coerceSetOpArmRows(rows []map[string]any, srcSchema, target []parquet.Column) ([]map[string]any, error) {
	if len(target) == 0 || len(srcSchema) != len(target) {
		return rows, nil
	}
	var cols []int
	for i := range srcSchema {
		if setOpArmNeedsMove(srcSchema[i], target[i]) {
			cols = append(cols, i)
		}
	}
	if cols == nil {
		return rows, nil
	}
	for _, i := range cols {
		src, dst := srcSchema[i], target[i]
		if dst.Type == parquet.TypeDecimal && src.Type == parquet.TypeDecimal && dst.Scale < src.Scale {
			// The output scale is the maximum over the arms, so no arm is
			// ever asked to scale DOWN. Arriving here means the unified type
			// was built by something other than that rule, and answering
			// would drop digits the row actually holds — the defect #532 was.
			return nil, fmt.Errorf(
				"set operation: column %q would drop digits (scale %d to %d); a set operation's "+
					"output scale is the maximum over its arms, so no arm is ever scaled down",
				dst.Name, src.Scale, dst.Scale)
		}
		for _, row := range rows {
			v, ok := row[src.Name]
			if !ok || v == nil {
				continue
			}
			moved, err := setOpMoveValue(v, src, dst)
			if err != nil {
				return nil, err
			}
			row[src.Name] = moved
		}
	}
	return rows, nil
}

// setOpArmNeedsMove reports whether this arm's boxes have to be rewritten to
// land in the target column. A column the unification left alone never does.
func setOpArmNeedsMove(src, dst parquet.Column) bool {
	switch dst.Type {
	case parquet.TypeArray:
		// An ARRAY arm whose ELEMENT the unification moved:
		// the element's own rule, one level down, so `int[] ∪ float8[]`
		// writes the int arm's leaves as doubles and the dedup key sees one
		// value where `=` does.
		return src.Type == parquet.TypeArray && src.ElementType != nil && dst.ElementType != nil &&
			setOpArmNeedsMove(*src.ElementType, *dst.ElementType)
	case parquet.TypeDecimal:
		switch src.Type {
		case parquet.TypeInt32, parquet.TypeInt64:
			return true
		case parquet.TypeDecimal:
			// An arm already AT the unified type holds values that fit it by
			// construction, so it keeps the old cost: no per-row work. Any
			// other (p,s) is checked even when only the precision moved —
			// precision is the bound the parquet writer sizes the leaf from
			// (ADR-0018 §4), so a value past it is not storable.
			return src.Precision != dst.Precision || src.Scale != dst.Scale
		}
	case parquet.TypeFloat64:
		switch src.Type {
		case parquet.TypeDecimal, parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32,
			parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
			return true
		}
	case parquet.TypeFloat32:
		switch src.Type {
		case parquet.TypeDecimal, parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat64,
			parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
			return true
		}
	case parquet.TypeInt64:
		// PORT and PROTOCOL box as an int32 and DURATION as an int64, so the
		// move is the same widening an INT32 arm takes. Without it the boxes
		// reached a bigint column as int32s and the column was built at the
		// first arm's width, which WRAPS (#834's types on the numeric ladder).
		switch src.Type {
		case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
			return true
		}
	}
	return false
}

// setOpMoveValue converts one boxed value from its arm's column into the
// unified column's box. A box that is not the shape its declared type
// produces is returned untouched rather than guessed at.
func setOpMoveValue(v any, src, dst parquet.Column) (any, error) {
	switch dst.Type {
	case parquet.TypeArray:
		elems, ok := v.([]any)
		if !ok || src.ElementType == nil || dst.ElementType == nil {
			return v, nil
		}
		out := make([]any, len(elems))
		for i, e := range elems {
			if e == nil {
				continue
			}
			m, err := setOpMoveValue(e, *src.ElementType, *dst.ElementType)
			if err != nil {
				return nil, err
			}
			out[i] = m
		}
		return out, nil
	case parquet.TypeDecimal:
		if src.Type == parquet.TypeDecimal {
			return setOpCheckedDecimalText(v, dst.Name, dst.Precision, dst.Scale)
		}
		return setOpCheckedIntDecimalText(v, dst.Name, dst.Precision, dst.Scale)
	case parquet.TypeFloat64:
		return setOpFloatValue(v, dst.Name, "double precision")
	case parquet.TypeFloat32:
		f, err := setOpFloatValue(v, dst.Name, "real")
		if err != nil {
			return nil, err
		}
		if f64, ok := f.(float64); ok {
			// float32 in the BOX, not merely in the declaration: the dedup key
			// reads the box (physical.keyValueText's TypeFloat32 arm keys
			// through kernel.KeyFloat32Bits), and a float64 box that only
			// narrows later at the store would key as a different number from
			// the arm that arrived already narrowed.
			return float32(f64), nil
		}
		return f, nil
	case parquet.TypeInt64:
		switch iv := v.(type) {
		case int32:
			return int64(iv), nil
		case int:
			return int64(iv), nil
		case int64:
			return iv, nil
		}
	}
	return v, nil
}

// setOpCheckedIntDecimalText renders an integer box as the decimal text a
// DECIMAL(precision, scale) column reads at its own scale, after checking the
// value FITS that type. Anything that is not an integer box is returned
// untouched.
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
		ok = setOpDecimalFits(shifted, precision)
	}
	if !ok {
		return nil, setOpOverflowError(unscaled.FormatDecimal(0), name, precision, scale)
	}
	return strconv.FormatInt(n, 10), nil
}

// setOpCheckedDecimalText checks that a DECIMAL arm's rendered text FITS the
// widened DECIMAL(precision, scale) the set operation resolved, and hands the
// text on unchanged when it does — the text carries the exact value, and
// re-reading it at a scale no smaller than the one it was rendered at is
// exact in both directions.
//
// This is #553's site. batch.FromRows re-reads the text through
// ParseDecimalString, whose saturating arm answered Int128Max for a value
// with no carrier at the unified scale: a DECIMAL(38,0) arm holding 10^30
// came back as 17014118346046923173168730371.5884105727 under a
// DECIMAL(38,10) union, with no error anywhere. batch.FromRowsChecked now
// reports that too — this check is the one that names the COLUMN and the
// declared type the value failed, and it also catches the band inside the
// carrier but outside the declaration.
func setOpCheckedDecimalText(v any, name string, precision, scale int) (any, error) {
	s, ok := v.(string)
	if !ok {
		return v, nil
	}
	// NaN and the infinities are a value with no carrier, not unreadable text,
	// and they take 22003 here for the same reason and with the same wording
	// batch.ParseDecimalStringChecked gives them (#534, ADR-0024 item 6). One
	// classification for the two value-producing readers: routing them
	// through the 22P02 below would answer a different SQLSTATE than the
	// checked writer does for the identical text.
	if err := batch.DecimalSpecialValueError(s); err != nil {
		return nil, err
	}
	sd, textOK := batch.DecimalTextAt(s, scale)
	if !textOK {
		return nil, sqlerr.New("22P02", "invalid input syntax for type numeric: %q", s)
	}
	if sd.Sat != 0 || !setOpDecimalFits(sd.Unscaled, precision) {
		return nil, setOpOverflowError(s, name, precision, scale)
	}
	return s, nil
}

// setOpFloatValue moves a numeric box into the float a set operation resolved
// to, as a float64; the FLOAT32 caller narrows the result. A DECIMAL box is
// its rendered TEXT, which is why the float rung failed the store outright
// before this existed (#541 shape 2).
//
// typeName is the SQL spelling of the target, so an out-of-range or
// unparseable value is reported against the type the query actually resolved
// to rather than always against double precision.
func setOpFloatValue(v any, name, typeName string) (any, error) {
	switch fv := v.(type) {
	case float64:
		return fv, nil
	case float32:
		return float64(fv), nil
	case int64:
		return float64(fv), nil
	case int32:
		return float64(fv), nil
	case int:
		return float64(fv), nil
	case string:
		f, err := strconv.ParseFloat(fv, 64)
		if err != nil {
			if ne, isNum := err.(*strconv.NumError); isNum && ne.Err == strconv.ErrRange {
				return nil, sqlerr.New("22003",
					"numeric field overflow: %s is out of range for %s, the type this "+
						"set operation's arms agree on for column %q", fv, typeName, name)
			}
			return nil, sqlerr.New("22P02", "invalid input syntax for type %s: %s", typeName, sqlerr.Quote(fv))
		}
		return f, nil
	}
	return v, nil
}

// setOpOverflowError is the refusal both execution paths give for a value the
// set operation's own type decision cannot hold, worded identically to
// exec.coerceDecimalVector's so the two are one message. SQLSTATE 22003 is
// PostgreSQL's numeric_value_out_of_range (ADR-0024 item 4).
func setOpOverflowError(value, name string, precision, scale int) error {
	return sqlerr.New("22003",
		"numeric field overflow: %s does not fit DECIMAL(%d,%d), the type this set operation's "+
			"arms agree on for column %q — a field with precision %d, scale %d holds an "+
			"absolute value below 10^%d",
		value, precision, scale, name, precision, scale, precision-scale)
}

// setOpDecimalFits is batch.DecimalFitsPrecision with the limit resolved from
// the declared precision. The single fits-precision helper lives in batch so
// the single-process path and exec.coerceDecimalVector cannot come to
// different conclusions about the same value.
func setOpDecimalFits(v batch.Int128, precision int) bool {
	limit, ok := batch.DecimalPrecisionLimit(precision)
	if !ok {
		return true
	}
	return batch.DecimalFitsLimit(v, limit)
}
