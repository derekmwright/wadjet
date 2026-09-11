package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// Resolve MAP/ARRAY identity and element types from the expression's
// DECLARATION, recursively as for ROW fields (ADR-0022; #669, #635).
// MAP and ARRAY(ROW(key,value)) have indistinguishable runtime values.
// Use Vector.Child/Children as the nested declaration carried by the batch.
// Return nil when no shape is declared, including untyped dynamic functions
// and UDFs; do not guess MAP versus ARRAY from a slice.
// fnElementAt still directly keys genuine Go maps.
// See docs/internals/container-expression-declared-shape.md for the design.
func containerVector(e Expr, b *batch.RecordBatch) *batch.Vector {
	if b == nil {
		return nil
	}
	switch v := e.(type) {
	case *ColRef:
		v.resolve(b)
		if v.idx < 0 || v.idx >= len(b.Columns) {
			return nil
		}
		col := b.Columns[v.idx]
		if v.structField == "" {
			return col
		}
		// A ROW field path names one child, and the field index is what
		// ColRef.resolve already worked out.
		if col.Type != batch.TypeRow || v.fieldIdx < 0 || v.fieldIdx >= len(col.Children) {
			return nil
		}
		return col.Children[v.fieldIdx]
	case *elementAtExpr:
		return containerElementVector(containerVector(v.arg0, b), b)
	case *Coalesce:
		return firstContainerVector(v.Args, b)
	case *Case:
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return firstContainerVector(arms, b)
	case *FuncCall:
		// The transparent functions — the ones that answer WITH one of their
		// arguments rather than with a value of their own. Same set
		// classifyOperand and castBoolDeclared use, for the same reason.
		switch strings.ToLower(v.Name) {
		case "greatest", "least", "coalesce", "if":
			return firstContainerVector(v.Args, b)
		case "nullif":
			if len(v.Args) > 0 {
				return firstContainerVector(v.Args[:1], b)
			}
		}
		return nil
	}
	return nil
}

func firstContainerVector(args []Expr, b *batch.RecordBatch) *batch.Vector {
	for _, a := range args {
		if a == nil {
			continue
		}
		if v := containerVector(a, b); v != nil {
			return v
		}
	}
	return nil
}

// containerElementVector is the vector element_at LIFTS OUT of a container: an
// ARRAY's element, and a MAP's VALUE — never the entry row, which is the
// storage shape rather than the value the lookup answers with.
func containerElementVector(v *batch.Vector, b *batch.RecordBatch) *batch.Vector {
	if v == nil || v.Child == nil {
		return nil
	}
	if v.Type == batch.TypeMap {
		if len(v.Child.Children) != 2 {
			return nil
		}
		return v.Child.Children[1]
	}
	if v.Type != batch.TypeArray {
		return nil
	}
	return v.Child
}

// containerKeyVector is a MAP's KEY vector, which is what says at what SCALE a
// DECIMAL key must be spelled for the lookup to find it.
func containerKeyVector(v *batch.Vector) *batch.Vector {
	if v == nil || v.Type != batch.TypeMap || v.Child == nil || len(v.Child.Children) != 2 {
		return nil
	}
	return v.Child.Children[0]
}
