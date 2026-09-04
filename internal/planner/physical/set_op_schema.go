package physical

import (
	"fmt"
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// unifySetOpSchemas is the result type of a set operation: the first arm's
// column NAMES — SQL says the result takes them — over the COMMON TYPE of the
// two arms per position.
//
// The common type is the stage DAG's, not a second rule of this path's own:
// setOpWiden is the ladder (INT32 → INT64 → DECIMAL → FLOAT64, pinned against
// live postgres:17 by TestSetOpWidenLadder) and setOpDecimalTarget is the
// (p,s) the DECIMAL rung resolves to. Both are called here, for EVERY rung,
// so the two execution paths cannot disagree about what the output type IS —
// which is a wire fact as well as an engine one, since a client reads the
// column's OID (#541 shape 3).
//
// The rungs and what each one costs when it is NOT reconciled:
//
//   - DECIMAL over DECIMAL. The rows reach batch.FromRows as their rendered
//     decimal TEXT, boxed at each arm's own scale, and FromRows re-reads that
//     text at the schema's scale — so handing it the first arm's scale
//     truncated the second arm's values: over `DECIMAL(9,2) UNION ALL
//     DECIMAL(18,4)`, 12.7501 came back as 12.75 and 12.7499 as 12.74, and
//     the UNION then counted 8 distinct values where PostgreSQL counts 9
//     (#532). The scale is the max over the arms — the only choice that moves
//     no value — and the precision is REBUILT from the widest integer part,
//     the DAG's rule, because max(precision) is not a bound on the widened
//     values: DECIMAL(18,2) alongside DECIMAL(9,4) needs 16 integer digits at
//     scale 4, i.e. 20, where max(precision) declares 18 and the type is too
//     small for its own values.
//
//   - DECIMAL over INTEGER. `numeric ∪ bigint` is `numeric` in PostgreSQL, so
//     the integer arm widens INTO the DECIMAL at the DECIMAL's scale. Left
//     unreconciled this was the silent corruption of #547: the integer arm's
//     box is an int64, NOT text, so FromRows read it into the DECIMAL vector
//     as an UNSCALED carrier and divided every integer by 10^scale (1 came
//     back as 0.01).
//
//   - A FLOAT over anything numeric. float4 and float8 are both PREFERRED
//     types of PostgreSQL's numeric category, so each beats the exact types
//     it meets and only float8 beats float4: `numeric ∪ double precision` is
//     double precision, `numeric ∪ real` is real, in EITHER arm order.
//     Unreconciled, the arm order decided the answer: with the DECIMAL arm
//     first the result stayed DECIMAL (a wrong OID on the wire, right-looking
//     values), and with the FLOAT arm first the DECIMAL arm's rendered text
//     was stored into a float vector and the #361 guard failed the query
//     outright — the two halves of #541. Keeping real REAL is a value
//     question as well as an OID one: a real column holding 0.1 renders 0.1,
//     and the same value widened to double precision renders
//     0.10000000149011612.
//
//   - INT32 over INT64. `integer ∪ bigint` is bigint. No VALUE moves here,
//     which is why this rung used to be skipped; the OID does, and a client
//     reading int4 for a column carrying int64 values is the same class of
//     defect as the DECIMAL one, one type family over.
//
// Anything else — a non-numeric type, or a DECIMAL whose (p,s) nothing could
// resolve — is left exactly as it was. A computed DECIMAL expression carries
// no declared (p,s) (#555, being fixed in the declared-type layer), and
// guessing one here would move values under a type nobody stated.
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
		col, ok := setOpUnifyColumn(l, r)
		if !ok {
			continue
		}
		set(i, col)
	}
	if out == nil {
		return left
	}
	return out
}

// setOpUnifyColumn resolves one column position. ok=false means "leave the
// first arm's column exactly as it is", which is the answer for every pair
// the numeric ladder does not describe and for every pair already agreed.
func setOpUnifyColumn(l, r parquet.Column) (parquet.Column, bool) {
	lc, ok1 := setOpColTypeFromColumn(l)
	rc, ok2 := setOpColTypeFromColumn(r)
	if !ok1 || !ok2 {
		// One side is not on the ladder. Two DECIMALs whose (p,s) could not
		// be read are the one case still worth widening: #532's truncation
		// happens whether or not a precision was declared, and max(scale) is
		// the answer that moves no value.
		if l.Type == parquet.TypeDecimal && r.Type == parquet.TypeDecimal {
			return setOpUnifyDecimalFallback(l, r)
		}
		return parquet.Column{}, false
	}
	widened, ok := setOpWiden(lc.typ, rc.typ)
	if !ok {
		return parquet.Column{}, false
	}
	col := l // the result takes the FIRST arm's NAME
	switch widened {
	case parquet.TypeDecimal:
		meta, ok := setOpDecimalTarget([]setOpColType{lc, rc})
		if !ok {
			if l.Type == parquet.TypeDecimal && r.Type == parquet.TypeDecimal {
				return setOpUnifyDecimalFallback(l, r)
			}
			return parquet.Column{}, false
		}
		if l.Type == parquet.TypeDecimal && l.Precision == meta.Precision && l.Scale == meta.Scale {
			return parquet.Column{}, false
		}
		col.Type = parquet.TypeDecimal
		col.Precision = meta.Precision
		col.Scale = meta.Scale
	case parquet.TypeFloat64:
		if l.Type == parquet.TypeFloat64 {
			return parquet.Column{}, false
		}
		col.Type = parquet.TypeFloat64
		col.Precision, col.Scale = 0, 0
	case parquet.TypeFloat32:
		if l.Type == parquet.TypeFloat32 {
			return parquet.Column{}, false
		}
		col.Type = parquet.TypeFloat32
		col.Precision, col.Scale = 0, 0
	case parquet.TypeInt64:
		if l.Type == parquet.TypeInt64 {
			return parquet.Column{}, false
		}
		col.Type = parquet.TypeInt64
		col.Precision, col.Scale = 0, 0
	default:
		// setOpWiden's a==b early return for two identical non-DECIMAL types.
		return parquet.Column{}, false
	}
	// A set operation's output column takes a NULL from either arm, and the
	// widened column is rebuilt from scratch, so it is declared nullable.
	col.Nullable = true
	return col, true
}

// setOpUnifyDecimalFallback is the DECIMAL ∪ DECIMAL rule for the pair
// setOpDecimalTarget declines: max(scale) so no arm's digits are dropped
// (#532), max(precision) because there is no declared integer part to rebuild
// one from. It is deliberately the PRE-existing behaviour of this path —
// leaving an unresolvable pair alone entirely would reopen #532 for it.
func setOpUnifyDecimalFallback(l, r parquet.Column) (parquet.Column, bool) {
	if r.Scale <= l.Scale && r.Precision <= l.Precision {
		return parquet.Column{}, false
	}
	col := l
	col.Scale = max(l.Scale, r.Scale)
	col.Precision = max(l.Precision, r.Precision)
	col.Nullable = true
	return col, true
}

// setOpColTypeFromColumn adapts a runtime parquet.Column into the setOpColType
// the ladder helpers take. It resolves only the numeric types the ladder
// describes; everything else is ok=false, which unifySetOpSchemas reads as
// "leave this column alone".
func setOpColTypeFromColumn(c parquet.Column) (setOpColType, bool) {
	switch c.Type {
	case parquet.TypeDecimal:
		if c.Precision <= 0 {
			// #458's "unconstrained" sentinel. Reported as unresolved rather
			// than taken at face value: setOpDecimalTarget would otherwise
			// widen every arm to scale 0 and truncate all of them.
			return setOpColType{}, false
		}
		return setOpColType{
			typ:      c.Type,
			known:    true,
			dec:      logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale},
			decKnown: true,
		}, true
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64:
		return setOpColType{typ: c.Type, known: true}, true
	case parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		// The three types whose WIRE declaration is an integer (#834). They are
		// on setOpWiden's ladder, so the stage DAG resolves `PORT ∪ INT64` to
		// bigint and builds the column as one — and this path used to decline
		// them here, leave the column at the FIRST arm's type, and materialise
		// the union in a PORT vector: `SELECT c_port … UNION ALL SELECT
		// 4000000000` came back as -294967296 on this path and 4000000000 on
		// the DAG, one query answered two ways by the fast-path threshold.
		return setOpColType{typ: c.Type, known: true}, true
	default:
		return setOpColType{}, false
	}
}

// coerceSetOpArmRows rewrites an arm's boxed rows so they carry the VALUE the
// unified column expects, for every rung of the ladder that moves one.
//
// The boxes are not uniform across types and that asymmetry is the whole
// problem: a DECIMAL boxes as its rendered TEXT (Vector.GetValue), an integer
// as a raw int64, a float as a float64. Handing those to batch.FromRows under
// a schema they were not boxed for is how the single-process path answered
// wrongly, or refused, depending on which arm came first:
//
//   - integer box → DECIMAL column: read as an UNSCALED carrier, dividing
//     every integer by 10^scale (1 → 0.01, #547). Rewritten to the integer's
//     decimal TEXT, which routes it through the same exact text path a native
//     DECIMAL box takes (ParseDecimalString at FromRows, DecimalTextAt at the
//     dedup key), so it arrives at its true value and keys the same as an
//     equal DECIMAL value.
//   - DECIMAL text box → FLOAT column: the #361 silent-write guard refuses
//     the store and the whole query fails, where PostgreSQL answers (#541
//     shape 2). Converted to the float the widened column holds — narrowed to
//     float32 in the BOX for a real result, so the dedup key sees the same
//     number the already-real arm produces.
//   - DECIMAL text box → wider DECIMAL column: exact as text, but the value
//     may not FIT the widened (p,s) — the union's own type decision can put a
//     value out of range that both arms held comfortably (#552). Checked, not
//     assumed.
//   - integer / float box → FLOAT32, FLOAT64 or INT64 column: converted here
//     rather than relying on SetValue's own conversions, so the dedup key
//     sees one box shape per column.
//
// Every value the unified DECIMAL cannot hold is a "numeric field overflow"
// ERROR carrying SQLSTATE 22003, worded to match the stage DAG
// (exec.coerceDecimalVector) so both paths refuse the same input the same way
// — NOT a silently saturated Int128Max, which is what routing an out-of-range
// value through DecimalTextAt's comparison-oriented saturating parser
// produces (#553). Wadjet's finite DECIMAL carrier cannot hold PostgreSQL's
// unconstrained numeric, so in the overflow band wadjet errors where
// PostgreSQL answers; ADR-0024 item 7 records that residual (#552) as the
// accepted cost of item 1's finite carrier.
//
// srcSchema is the arm's OWN schema; target is the unified result schema.
// They correspond by POSITION, and the arm's rows are still keyed by
// srcSchema's names (they have not been re-aligned to the result names yet),
// so the rewrite is applied before alignSetOpRows.
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
