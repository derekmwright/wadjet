package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// A container expression's SHAPE — is it a MAP or an ARRAY, and what is its
// element declared as — is a property of the container's declaration, resolved
// from its parent's the way ADR-0022 resolves a ROW field. This file answers
// that question for an arbitrary container-valued expression rather than for a
// bare column reference alone, which is what #669 and #635 are:
//
//   - `element_at(element_at(aa, 1), 1)` over an ARRAY of ARRAY of DECIMAL was
//     unclassified, so `GREATEST(<that>, '2')` answered "2" by byte order where
//     the server compares 12.75 numerically, and a predicate over it lost every
//     row (#669 item 1).
//   - `element_at(COALESCE(m, m), 'a')` routed to POSITIONAL array indexing and
//     answered NULL, because only a `*ColRef` and a fixed-RetMap function were
//     recognised as a MAP (#635). A MAP column materializes as
//     ARRAY(ROW("key","value")), which is byte-for-byte the shape map_entries()
//     produces, so the runtime VALUE cannot decide this and the declaration
//     must.
//
// The vector is the declaration at this layer: batch.RecordBatch carries the
// parquet schema into Vector.Child (ARRAY element, MAP entry ROW) and
// Vector.Children (ROW fields), and a compiled expression has no other handle
// on a nested declared type.
//
// nil means "no declared shape here", which is the honest answer for a value
// whose producer declares none — a RetDynamic function, a UDF that names no
// container type. That case is genuinely ambiguous rather than merely
// unresolved: a MAP and an ARRAY of two-field ROWs are one runtime shape, and
// PostgreSQL has no MAP at all to arbitrate. `fnElementAt` still keys a Go map
// directly, which is the shape json_extract and map_from_entries produce.
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
