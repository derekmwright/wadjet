// SPDX-License-Identifier: MIT

package expr

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// containerChoice is the VALUE half of CommonDeclType's container arm (arc CW
// round 5, review B5): a construct that CHOOSES among container operands —
// CASE, COALESCE, GREATEST, LEAST, an ARRAY[a, b] of arrays — declares their
// common shape (batch.CommonContainerColumn), and the operand it returns is
// moved into that shape before the declared vector receives it: an integer
// leaf becomes its exact text for a DECIMAL element (a vector reads an
// integer box as an UNSCALED carrier, so 2 would land as 0.02 at scale 2), a
// DECIMAL's text becomes a double for a float element. A DECIMAL element at a
// narrower scale needs no move: its text is read at the wider declared scale
// exactly. When every operand already declares the common shape — every
// single-type CASE — nothing is resolved per row beyond one atomic load.
type containerChoice struct {
	decls []*operandDecl
	res   atomic.Pointer[resolvedDecl]
}

func newContainerChoice(decls []*operandDecl) *containerChoice {
	if len(decls) < 2 {
		return nil
	}
	return &containerChoice{decls: decls}
}

// conform moves a chosen container box into the operands' common shape.
func (c *containerChoice) conform(b *batch.RecordBatch, v any) any {
	if c == nil {
		return v
	}
	if _, ok := v.([]any); !ok {
		return v
	}
	r := c.res.Load()
	if r == nil {
		r = &resolvedDecl{col: c.common(b)}
		if b != nil {
			c.res.Store(r)
		}
	}
	if r.col == nil {
		return v
	}
	return conformBox(v, r.col)
}

// common is the operands' common container declaration when they DIFFER, nil
// when they agree (nothing to move) or when one is not a declared container.
func (c *containerChoice) common(b *batch.RecordBatch) *parquet.Column {
	var common *parquet.Column
	differ := false
	for _, d := range c.decls {
		if d == nil {
			continue
		}
		s := d.shape(b, 0, nil)
		if s == nil || s.Type != parquet.TypeArray || s.ElementType == nil {
			// A NULL, or an operand nothing declares: it contributes no
			// element, and a box it does return is left as it is.
			continue
		}
		if common == nil {
			cc := s.Clone()
			common = &cc
			continue
		}
		if !sameLeafShape(*common, *s) {
			differ = true
		}
		u, ok := batch.CommonContainerColumn(*common, *s)
		if !ok {
			return nil
		}
		common = &u
	}
	if !differ {
		return nil
	}
	return common
}

// sameLeafShape reports whether two declarations allocate the same vector:
// type, a DECIMAL's (p,s), and the same again for every element and field.
func sameLeafShape(a, b parquet.Column) bool {
	if a.Type != b.Type || a.Precision != b.Precision || a.Scale != b.Scale || len(a.Fields) != len(b.Fields) {
		return false
	}
	if (a.ElementType == nil) != (b.ElementType == nil) {
		return false
	}
	if a.ElementType != nil && !sameLeafShape(*a.ElementType, *b.ElementType) {
		return false
	}
	for i := range a.Fields {
		if !sameLeafShape(a.Fields[i], b.Fields[i]) {
			return false
		}
	}
	return true
}
