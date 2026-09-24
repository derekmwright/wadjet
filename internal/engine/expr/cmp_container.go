// SPDX-License-Identifier: MIT

package expr

import (
	"strconv"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The comparison OPERATORS over two containers (#1021's second spelling, arc
// CW round 2). ORDER BY, MIN, MAX and DISTINCT compare arrays element-wise
// through kernel.CompareValuesAt since round 1; `<`, `>`, `=` … compared the
// two boxes through compare()'s string fallback — Go's `[10]` against `[9]` —
// so `ARRAY[10] > ARRAY[9]` was false and `ARRAY[1,2] < ARRAY[1,2,0]` false
// where PostgreSQL answers true for both. This hands the pair to the SAME
// kernel: each box is written into a one-row vector of its declared shape and
// kernel.CompareValuesAt orders the two, so the operator and the sort cannot
// disagree about an order (element-wise, a NULL element after every value and
// equal to another NULL, a shorter prefix first).

// containerCmpOrder orders two container boxes, and false when the pair has
// no common shape to compare under (a ROW whose fields nothing declares).
func containerCmpOrder(b *batch.RecordBatch, row int, left, right Expr, lv, rv any) (int, bool) {
	ld := containerOperandDecl(b, row, left)
	rd := containerOperandDecl(b, row, right)
	if ld == nil {
		ld = boxShape(lv)
	}
	if rd == nil {
		rd = boxShape(rv)
	}
	col, ok := commonContainerShape(ld, rd)
	if !ok {
		return 0, false
	}
	lvec := batch.NewColumnVector(*col, 1)
	rvec := batch.NewColumnVector(*col, 1)
	lvec.SetValue(0, conformBox(lv, col))
	rvec.SetValue(0, conformBox(rv, col))
	return kernel.CompareValuesAt(lvec, 0, rvec, 0), true
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

// commonContainerShape is the one shape both sides are written under: the
// declaration they share, or — for two numeric leaves of different types —
// double precision, PostgreSQL's common type for the pairs this engine boxes
// (`int[] < numeric[]` compares numerically there).
func commonContainerShape(a, b *parquet.Column) (*parquet.Column, bool) {
	switch {
	case a == nil && b == nil:
		return nil, false
	case a == nil:
		return b, shapeComparable(b)
	case b == nil:
		return a, shapeComparable(a)
	}
	if a.Type != b.Type {
		if numericLeaf(a.Type) && numericLeaf(b.Type) {
			return &parquet.Column{Type: parquet.TypeFloat64, Nullable: true}, true
		}
		return nil, false
	}
	switch a.Type {
	case parquet.TypeArray, parquet.TypeMap:
		if a.ElementType == nil || b.ElementType == nil {
			return nil, false
		}
		el, ok := commonContainerShape(a.ElementType, b.ElementType)
		if !ok {
			return nil, false
		}
		out := *a
		out.ElementType = el
		return &out, true
	case parquet.TypeRow:
		if len(a.Fields) == 0 || len(a.Fields) != len(b.Fields) {
			return nil, false
		}
		out := *a
		out.Fields = make([]parquet.Column, len(a.Fields))
		for i := range a.Fields {
			f, ok := commonContainerShape(&a.Fields[i], &b.Fields[i])
			if !ok {
				return nil, false
			}
			out.Fields[i] = *f
			out.Fields[i].Name = a.Fields[i].Name
		}
		return &out, true
	case parquet.TypeDecimal:
		if a.Scale != b.Scale {
			return &parquet.Column{Type: parquet.TypeFloat64, Nullable: true}, true
		}
	}
	return a, true
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

// conformBox rewrites a box's numeric leaves to float64 where the common
// shape widened them, so the vector write takes a DECIMAL's text carrier.
func conformBox(v any, col *parquet.Column) any {
	if v == nil || col == nil {
		return v
	}
	switch col.Type {
	case parquet.TypeFloat64:
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
