// SPDX-License-Identifier: MIT

package expr

import (
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Every comparison of two CONTAINERS (#1021 and its spellings, arc CW rounds
// 1–3) orders through ONE function, containerOrder, and it orders through the
// sort's kernel: each box is written into a one-row vector of the pair's
// declared shape and kernel.CompareValuesAt orders the two — element-wise, a
// NULL element after every value and equal to another NULL, a shorter prefix
// first. ORDER BY, MIN, MAX, DISTINCT and the window order read the same
// kernel over their vectors, so no comparator can disagree with the sort.
//
// Round 2 wired the six operators (`=`, `<>`, `<`, `<=`, `>`, `>=`) to it and
// left the other spellings on compare()'s text order of the boxes' Go text:
// GREATEST/LEAST (`GREATEST(ARRAY[2], ARRAY[10])` = `{2}`), BETWEEN (2283 rows
// where `>= AND <=` answered 914). Round 3 routes it at the comparators' SHARED
// seams instead of per spelling:
//
//	boxedPair.order      the six operators, IN, BETWEEN, a simple CASE's WHEN,
//	                     IS [NOT] DISTINCT FROM — with both operands' declarations
//	extremumArms.order   GREATEST, LEAST, NULLIF — with every argument's declaration
//	compare()            every remaining caller (the last resort, shape read off
//	                     the boxes)
//
// A declaration (operand_decl.go) decides the shape when the operand has one,
// which is what orders a DECIMAL element (boxed as its text) as a number and a
// DATE element by its day, whatever its box.

// containerOrder orders two container boxes under their declared shapes (nil
// when an operand has none: its shape is read off its box), and false when the
// pair has no common shape to compare under (a ROW whose fields nothing
// declares).
func containerOrder(ld, rd *parquet.Column, lv, rv any) (int, bool) {
	if ld == nil {
		ld = boxShape(lv)
	}
	if rd == nil {
		rd = boxShape(rv)
	}
	lcol, rcol, ok := commonContainerShapes(ld, rd)
	if !ok {
		return 0, false
	}
	lvec := batch.NewColumnVector(*lcol, 1)
	rvec := batch.NewColumnVector(*rcol, 1)
	lvec.SetValue(0, conformBox(lv, lcol))
	rvec.SetValue(0, conformBox(rv, rcol))
	return kernel.CompareValuesAt(lvec, 0, rvec, 0), true
}

// containerMember reports whether the container box lv equals the member rv
// under their declarations — the membership test of `IN (SELECT …)`,
// `= ANY (SELECT …)` and `<> ALL (SELECT …)`, which is `=` quantified and so
// the same ordering the six operators use (round 4, B4). decided is false for
// a pair the kernel has no common shape for; the caller then keeps compare().
func containerMember(ld, rd *parquet.Column, lv, rv any) (eq, decided bool) {
	c, ok := containerOrder(ld, rd, lv, rv)
	if !ok {
		return false, false
	}
	return c == 0, true
}

// containerCompare is containerOrder under a comparison operator, for the
// sites that answer a boolean.
func containerCompare(ld, rd *parquet.Column, lv, rv any, op CmpOp) (bool, bool) {
	c, ok := containerOrder(ld, rd, lv, rv)
	if !ok {
		return false, false
	}
	return cmpOrder(c, op), true
}

// boxShape reads an ARRAY box's shape off its values, for an operand nothing
// declares (a constructor over literals): the element type of the first
// non-NULL element, recursively. A ROW or MAP box is not read this way — a
// map's field ORDER is the declaration's, not the box's.
func boxShape(v any) *parquet.Column {
	elems, ok := v.([]any)
	if !ok {
		return nil
	}
	var el *parquet.Column
	for _, e := range elems {
		if e == nil {
			continue
		}
		switch x := e.(type) {
		case int64, int32, int:
			el = &parquet.Column{Type: parquet.TypeInt64}
		case float64, float32:
			el = &parquet.Column{Type: parquet.TypeFloat64}
		case string:
			el = &parquet.Column{Type: parquet.TypeString}
		case bool:
			el = &parquet.Column{Type: parquet.TypeBool}
		case []any:
			el = boxShape(x)
		}
		break
	}
	if el == nil {
		// Every element NULL, or none: any element type orders it (NULLs
		// and lengths only).
		el = &parquet.Column{Type: parquet.TypeInt64}
	}
	el.Name, el.Nullable = "element", true
	return &parquet.Column{Type: parquet.TypeArray, Nullable: true, ElementType: el}
}

// commonContainerShapes are the shapes the two sides are written under: the
// pair's COMMON declaration, batch.CommonContainerColumn — the one rule every
// meeting point of two containers unifies through (arc CW round 5: the join
// keys, the set operations and CASE / COALESCE read the same function), so
// `int[] = float8[]` compares at float8, `int[] = numeric(9,2)[]` exactly at
// scale 2, and `numeric(5,2)[] = numeric(9,4)[]` at (9,4) — never through a
// double for two exact sides, never rounding either side's digits. A pair
// with no common declaration (a DECIMAL whose (p,s) nothing resolved) keeps
// EACH SIDE'S OWN declaration where the two agree in type: the kernel
// compares two DECIMAL vectors at their common scale exactly
// (kernel.CompareDecimalValues) and two numeric element vectors of different
// types at their common type (kernel.compareMixedLeafAt).
func commonContainerShapes(a, b *parquet.Column) (*parquet.Column, *parquet.Column, bool) {
	switch {
	case a == nil && b == nil:
		return nil, nil, false
	case a == nil:
		return b, b, shapeComparable(b)
	case b == nil:
		return a, a, shapeComparable(a)
	}
	if c, ok := batch.CommonContainerColumn(*a, *b); ok && shapeComparable(&c) {
		return &c, &c, true
	}
	return ownContainerShapes(a, b)
}

// ownContainerShapes is commonContainerShapes' fallback for a pair with no
// common declaration: each side keeps its own, level by level, when the two
// agree in type.
func ownContainerShapes(a, b *parquet.Column) (*parquet.Column, *parquet.Column, bool) {
	if a.Type != b.Type {
		return nil, nil, false
	}
	switch a.Type {
	case parquet.TypeArray, parquet.TypeMap:
		if a.ElementType == nil || b.ElementType == nil {
			return nil, nil, false
		}
		la, lb, ok := ownContainerShapes(a.ElementType, b.ElementType)
		if !ok {
			return nil, nil, false
		}
		outA, outB := *a, *b
		outA.ElementType, outB.ElementType = la, lb
		return &outA, &outB, true
	case parquet.TypeRow:
		if len(a.Fields) == 0 || len(a.Fields) != len(b.Fields) {
			return nil, nil, false
		}
		outA, outB := *a, *b
		outA.Fields = make([]parquet.Column, len(a.Fields))
		outB.Fields = make([]parquet.Column, len(a.Fields))
		for i := range a.Fields {
			fa, fb, ok := ownContainerShapes(&a.Fields[i], &b.Fields[i])
			if !ok {
				return nil, nil, false
			}
			outA.Fields[i], outB.Fields[i] = *fa, *fb
			outA.Fields[i].Name, outB.Fields[i].Name = a.Fields[i].Name, a.Fields[i].Name
		}
		return &outA, &outB, true
	}
	return a, b, true
}

// shapeComparable reports whether a lone declaration can allocate both sides:
// every container in it carries its element or fields.
func shapeComparable(c *parquet.Column) bool {
	switch c.Type {
	case parquet.TypeArray, parquet.TypeMap:
		return c.ElementType != nil && shapeComparable(c.ElementType)
	case parquet.TypeRow:
		return len(c.Fields) > 0
	}
	return true
}

func numericLeaf(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64, parquet.TypeDecimal:
		return true
	}
	return false
}

// conformBox rewrites a box's numeric leaves into the common shape's carrier:
// a DECIMAL's text as a float where the shape widened to a float, an integer
// as its text where it widened to a DECIMAL.
func conformBox(v any, col *parquet.Column) any {
	if v == nil || col == nil {
		return v
	}
	switch col.Type {
	case parquet.TypeDecimal:
		// An integer box written into a DECIMAL vector would be read as an
		// UNSCALED carrier (ADR-0018 §4): 2 into a scale-2 child is 0.02. The
		// common shape of `int[]` and `numeric(9,2)[]` is a scale-2 DECIMAL,
		// so the integer side goes in as its exact text.
		switch x := v.(type) {
		case int64:
			return strconv.FormatInt(x, 10)
		case int32:
			return strconv.FormatInt(int64(x), 10)
		case int:
			return strconv.Itoa(x)
		}
		return v
	case parquet.TypeFloat64, parquet.TypeFloat32:
		if s, ok := v.(string); ok {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		}
		return v
	case parquet.TypeArray, parquet.TypeMap:
		elems, ok := v.([]any)
		if !ok || col.ElementType == nil {
			return v
		}
		out := make([]any, len(elems))
		for i, e := range elems {
			out[i] = conformBox(e, col.ElementType)
		}
		return out
	case parquet.TypeRow:
		m, ok := v.(map[string]any)
		if !ok {
			return v
		}
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = e
		}
		for i := range col.Fields {
			f := &col.Fields[i]
			if e, ok := out[f.Name]; ok {
				out[f.Name] = conformBox(e, f)
			}
		}
		return out
	}
	return v
}
