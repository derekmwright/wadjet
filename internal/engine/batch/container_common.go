// SPDX-License-Identifier: MIT

package batch

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// CommonContainerColumn is the ONE rule by which two container declarations
// meet (arc CW round 5): wherever two array operands meet — a comparison
// operator, IN / ANY / ALL, a hash or sort-merge join key, a UNION /
// INTERSECT / EXCEPT arm, CASE / COALESCE / GREATEST / LEAST / NULLIF, an
// ARRAY[a, b] constructor — their ELEMENTS unify here, and nowhere else: the
// comparator kernel (kernel.compareElemAt, expr.commonContainerShapes), the
// join-key resolution (physical.resolveJoinKeyTypes), the set operation's arm
// target (physical.setOpElementTarget) and the declared-output walk
// (expr.CommonDeclType) all call it.
//
// The element rule is PostgreSQL's numeric promotion, the ladder a scalar set
// operation already climbs (physical.setOpWiden): INT32 → INT64 → DECIMAL →
// FLOAT32 → FLOAT64, independent of order. Two DECIMAL elements meet at
// DecimalCommon's (p,s) — max(scale), the integer part rebuilt — so no digit
// of either side is ever rounded away; an integer beside a DECIMAL brings its
// whole range at scale 0 (ADR-0024 item 2). Two identical element types are
// themselves. Anything else — a text element beside a number, a DECIMAL whose
// (p,s) nothing resolved — has no common type here, and ok is false: the
// caller keeps its own disposition (a refusal, or each side's declaration).
//
// Before this rule the pair was decided once per meeting point: `=` widened
// int ⊕ float8 to a double while the hash-join key, the set operations and
// the FULL join's matched set keyed each side's boxes under its own element
// type (`int[] JOIN float8[]` answered 0 rows for 49, UNION 98), the
// sort-merge key compared an INT64 child against a FLOAT64 one with the
// INT64 kernel (index out of range), and CASE / COALESCE declared the FIRST
// branch's DECIMAL scale and rounded the other branch's elements into it
// (`{0.043333}` answered `{0.04}`) — round-4 review B1 and B5.
func CommonContainerColumn(a, b parquet.Column) (parquet.Column, bool) {
	if IsContainerType(a.Type) || IsContainerType(b.Type) || a.Type == TypeRow || b.Type == TypeRow {
		if a.Type != b.Type {
			return parquet.Column{}, false
		}
		switch a.Type {
		case TypeArray, TypeMap:
			if a.ElementType == nil || b.ElementType == nil {
				return parquet.Column{}, false
			}
			el, ok := CommonContainerColumn(*a.ElementType, *b.ElementType)
			if !ok {
				return parquet.Column{}, false
			}
			out := a
			out.ElementType = &el
			out.Nullable = a.Nullable || b.Nullable
			return out, true
		case TypeRow:
			if len(a.Fields) == 0 || len(a.Fields) != len(b.Fields) {
				return parquet.Column{}, false
			}
			out := a
			out.Fields = make([]parquet.Column, len(a.Fields))
			for i := range a.Fields {
				f, ok := CommonContainerColumn(a.Fields[i], b.Fields[i])
				if !ok {
					return parquet.Column{}, false
				}
				f.Name = a.Fields[i].Name
				out.Fields[i] = f
			}
			return out, true
		case TypeVector:
			return a, true
		}
		return parquet.Column{}, false
	}
	return commonLeafColumn(a, b)
}

// commonLeafColumn is CommonContainerColumn's rule for two scalar leaves.
func commonLeafColumn(a, b parquet.Column) (parquet.Column, bool) {
	out := a
	out.Nullable = a.Nullable || b.Nullable
	if a.Type == b.Type {
		if a.Type != TypeDecimal || (a.Precision == b.Precision && a.Scale == b.Scale) {
			return out, true
		}
	}
	ra, rb := leafNumericRank(a.Type), leafNumericRank(b.Type)
	if ra == 0 || rb == 0 {
		return parquet.Column{}, false
	}
	switch {
	case ra == 5 || rb == 5:
		out.Type, out.Precision, out.Scale = TypeFloat64, 0, 0
	case ra == 4 || rb == 4:
		out.Type, out.Precision, out.Scale = TypeFloat32, 0, 0
	case ra == 3 || rb == 3:
		da, oka := DecimalTypeOf(a.Type, DecimalType{Precision: knownPrecision(a), Scale: a.Scale})
		db, okb := DecimalTypeOf(b.Type, DecimalType{Precision: knownPrecision(b), Scale: b.Scale})
		if !oka || !okb {
			return parquet.Column{}, false
		}
		m, ok := DecimalCommon([]DecimalType{da, db})
		if !ok {
			return parquet.Column{}, false
		}
		out.Type, out.Precision, out.Scale = TypeDecimal, m.Precision, m.Scale
	default:
		out.Type, out.Precision, out.Scale = TypeInt64, 0, 0
	}
	return out, true
}

// knownPrecision is a DECIMAL leaf's precision for the common-type rule: a
// declaration read off a VECTOR carries its scale and not its precision
// (VectorDecl), and the widest precision at that scale moves no digit.
func knownPrecision(c parquet.Column) int {
	if c.Type == TypeDecimal && c.Precision <= 0 {
		return MaxDecimalPrecision
	}
	return c.Precision
}

// leafNumericRank is the numeric promotion ladder's rung of a leaf type, 0
// for a type that is not on it.
func leafNumericRank(t TypeID) int {
	switch t {
	case TypeInt32:
		return 1
	case TypeInt64:
		return 2
	case TypeDecimal:
		return 3
	case TypeFloat32:
		return 4
	case TypeFloat64:
		return 5
	}
	return 0
}

// ContainerLeafType is the type at the bottom of an ARRAY chain — an int[]'s
// INT64, an int[][]'s INT64 — and false for a declaration that is not an
// array of arrays of … a scalar (a MAP, a ROW element, an undeclared element).
func ContainerLeafType(c parquet.Column) (TypeID, bool) {
	for c.Type == TypeArray {
		if c.ElementType == nil {
			return 0, false
		}
		c = *c.ElementType
	}
	if IsContainerType(c.Type) || c.Type == TypeRow {
		return 0, false
	}
	return c.Type, true
}
