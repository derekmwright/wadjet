// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// This file is the CAST table for a CONTAINER operand (an ARRAY, MAP or ROW
// box) and for a VECTOR destination — the one place Cast.Eval decides what a
// container becomes, before any scalar arm can read it (arc CW round 2,
// ADR-0045 §2).
//
// Before it, a container reached whichever scalar arm its destination named
// and each arm read the []any box its own way: `CAST(ARRAY[1,2] AS INT)` was
// 0 (ToFloat64 of a slice), `AS DATE` was NULL, `AS DECIMAL` refused over the
// Go text, and every destination the engine does not convert to handed back
// the array's TEXT — including VECTOR(n), which the engine DOES convert to, so
// cosine_similarity over `CAST(ARRAY[…] AS VECTOR(n))` read a string and
// answered NULL. The table, measured on PostgreSQL 17.11 (pgvector for
// VECTOR, whose type PostgreSQL itself lacks):
//
//	destination                       container operand
//	text, varchar(n), char(n)         its PostgreSQL text (array_out / record_out)
//	T[] / ARRAY(T)                    element by element (castToArray)
//	VECTOR(n), VECTOR                 a VECTOR: numeric elements, n checked (22000),
//	                                  a NULL element 22004 — pgvector's array_to_vector
//	JSON                              its JSON text, to_json's (PostgreSQL has no
//	                                  cast and raises 42846; the superset answer is
//	                                  the value its JSON functions read)
//	every other type                  42846 cannot cast, as PostgreSQL raises

// isContainerBox reports whether v is the engine's box for an ARRAY / MAP
// (a []any) or a ROW (a map[string]any).
func isContainerBox(v any) bool {
	switch v.(type) {
	case []any, map[string]any:
		return true
	}
	return false
}

// VectorCastDim reports whether typeName is a VECTOR cast destination and its
// declared dimension — 0 for the unconstrained `VECTOR`, which takes the
// operand's own length. err is pgvector's refusal of a modifier below 1 or
// above its 16000 limit.
func VectorCastDim(typeName string) (int, error, bool) {
	t := strings.ToLower(strings.TrimSpace(typeName))
	if t == "vector" {
		return 0, nil, true
	}
	if !strings.HasPrefix(t, "vector(") || !strings.HasSuffix(t, ")") {
		return 0, nil, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(t[len("vector(") : len(t)-1]))
	switch {
	case err != nil:
		return 0, sqlerr.New("22P02", "invalid input syntax for type integer: %s", sqlerr.Quote(t[len("vector("):len(t)-1])), true
	case n < 1:
		return 0, sqlerr.New("22023", "dimensions for type vector must be at least 1"), true
	case n > 16000:
		return 0, sqlerr.New("22023", "dimensions for type vector cannot exceed 16000"), true
	}
	return n, nil, true
}

// castContainerDest is the container half of the table above. ok=false means
// the destination is a text one and Cast.Eval's text arms render the value.
func (e *Cast) castContainerDest(b *batch.RecordBatch, row int, v any, dest string) (any, bool) {
	d := strings.TrimSpace(dest)
	if textCastDest(d) {
		return nil, false
	}
	if d == "json" {
		col := e.containerShape(b, row, v)
		return batch.FormatPGJSON(declaredBox(v, col), col), true
	}
	panic(fatalEval{sqlerr.New("42846", "cannot cast type %s to %s",
		containerTypeName(e.containerShape(b, row, v), v), d)})
}

// textCastDest reports whether d (lower-cased, trimmed) is a destination of
// the string family: text, varchar, char, and their length-carrying spellings.
func textCastDest(d string) bool {
	switch d {
	case "char", "varchar", "text", "string", "character", "character varying":
		return true
	}
	_, _, ok := parquet.StringTypeLength(d)
	return ok
}

// containerTypeName names a container operand as PostgreSQL's cast error
// does: `integer[]`, `record`.
func containerTypeName(col *parquet.Column, v any) string {
	if _, ok := v.(map[string]any); ok {
		return "record"
	}
	if col != nil && col.Type == parquet.TypeMap {
		return "map"
	}
	if col != nil && col.ElementType != nil {
		// A nested array is one type in PostgreSQL (`integer[]` whatever its
		// dimensions): name its leaf.
		el := col.ElementType
		for el.Type == parquet.TypeArray && el.ElementType != nil {
			el = el.ElementType
		}
		return pgCastSourceName(el.Type) + "[]"
	}
	return "array"
}

// castToVector is pgvector's conversion into VECTOR(dim): an array of numbers
// (array_to_vector), the vector text `[1,2,3]` (vector_in), or a VECTOR. dim
// 0 is the unconstrained type. The refusals are pgvector's, class and all:
// a wrong dimension and an empty or non-finite vector 22000, a NULL element
// 22004, malformed text 22P02, anything else 42846.
func castToVector(v any, dim int) []float32 {
	var out []float32
	switch tv := v.(type) {
	case []float32:
		out = tv
	case []any:
		out = make([]float32, len(tv))
		for i, el := range tv {
			if el == nil {
				panic(fatalEval{sqlerr.New("22004", "array must not contain nulls")})
			}
			f, ok := vectorComponent(el)
			if !ok {
				panic(fatalEval{sqlerr.New("22000", "unsupported array type")})
			}
			out[i] = f
		}
	default:
		s, isText := stringOperand(v)
		if !isText {
			panic(fatalEval{sqlerr.New("42846", "cannot cast type %T to vector", v)})
		}
		vec, err := batch.ParseVectorText(s)
		if err != nil {
			panic(fatalEval{err})
		}
		out = vec
	}
	if len(out) == 0 {
		panic(fatalEval{sqlerr.New("22000", "vector must have at least 1 dimension")})
	}
	for _, f := range out {
		if math.IsNaN(float64(f)) {
			panic(fatalEval{sqlerr.New("22000", "NaN not allowed in vector")})
		}
		if math.IsInf(float64(f), 0) {
			panic(fatalEval{sqlerr.New("22000", "infinite value not allowed in vector")})
		}
	}
	if dim > 0 && len(out) != dim {
		panic(fatalEval{sqlerr.New("22000", "expected %d dimensions, not %d", dim, len(out))})
	}
	return out
}

// vectorComponent reads one array element as a float32 component: a number,
// or a DECIMAL element's text carrier.
func vectorComponent(el any) (float32, bool) {
	switch x := el.(type) {
	case float32:
		return x, true
	case float64:
		return float32(x), true
	case int64:
		return float32(x), true
	case int32:
		return float32(x), true
	case int:
		return float32(x), true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, false
		}
		return float32(f), true
	}
	return 0, false
}

// containerShape is the operand's declaration when it describes the box in
// hand — an ARRAY or MAP declaration for a []any, a ROW one for a Go map — and
// nil otherwise (arc CW round 3, operand_decl.go).
func (e *Cast) containerShape(b *batch.RecordBatch, row int, v any) *parquet.Column {
	col := e.operandShape(b, row)
	if col == nil {
		return nil
	}
	switch v.(type) {
	case []any:
		if (col.Type == parquet.TypeArray || col.Type == parquet.TypeMap) && col.ElementType != nil {
			return col
		}
	case map[string]any:
		if col.Type == parquet.TypeRow && len(col.Fields) > 0 {
			return col
		}
	}
	return nil
}

// containerText is PostgreSQL's text of a container under its declaration:
// the box as the declared type's vector holds it (batch.DeclaredValue — an
// element's day count, address or epoch milliseconds read back as that
// type), rendered by the one renderer every door uses.
func containerText(v any, col *parquet.Column) string {
	return batch.FormatPGText(declaredBox(v, col), col)
}

// declaredBox is v read through its declaration, or — with no declaration —
// v itself once every leaf is a box whose own rendering is its value. A leaf
// that is not (an INTERVAL's struct: this engine has no interval text form
// yet) refuses, because its only rendering is Go's `{0 0 0 1 0 0}`, a value
// no PostgreSQL type prints (ADR-0045 §2: loud, never plausible).
func declaredBox(v any, col *parquet.Column) any {
	refuseUnrenderable(v)
	if col != nil && boxKeysDeclared(v, col) {
		return batch.DeclaredValue(v, col)
	}
	return v
}

// refuseUnrenderable raises when v holds a leaf with no PostgreSQL text form
// here — checked BEFORE any declaration is applied, because a declaration
// that says text would otherwise store the leaf's Go rendering as the text.
func refuseUnrenderable(v any) {
	if leaf, ok := unrenderableLeaf(v); ok {
		panic(fatalEval{sqlerr.New("0A000",
			"a container element of type %s has no text form here", leafTypeName(leaf))})
	}
}

// leafTypeName names an unrenderable leaf for the refusal: the SQL type where
// the box is one this engine knows.
func leafTypeName(v any) string {
	if _, ok := v.(IntervalValue); ok {
		return "interval"
	}
	return fmt.Sprintf("%T", v)
}

// boxKeysDeclared reports whether a ROW box's every key is a field the
// declaration names (at any depth the box is a ROW); writing a box through a
// declaration that does not name its keys would drop their values.
func boxKeysDeclared(v any, col *parquet.Column) bool {
	switch tv := v.(type) {
	case map[string]any:
		if col.Type != parquet.TypeRow {
			return false
		}
		for k, e := range tv {
			var f *parquet.Column
			for i := range col.Fields {
				if col.Fields[i].Name == k {
					f = &col.Fields[i]
					break
				}
			}
			if f == nil || (e != nil && !boxKeysDeclared(e, f)) {
				return false
			}
		}
	case []any:
		if col.ElementType == nil {
			return false
		}
		if col.Type == parquet.TypeMap {
			return true
		}
		for _, e := range tv {
			if e != nil && !boxKeysDeclared(e, col.ElementType) {
				return false
			}
		}
	}
	return true
}

// unrenderableLeaf finds a leaf whose box is not a value FormatPGText renders
// as itself.
func unrenderableLeaf(v any) (any, bool) {
	switch tv := v.(type) {
	case nil, string, bool, int, int32, int64, uint32, uint64, float32, float64, []byte, []float32:
		return nil, false
	case []any:
		for _, e := range tv {
			if l, ok := unrenderableLeaf(e); ok {
				return l, true
			}
		}
		return nil, false
	case map[string]any:
		for _, e := range tv {
			if l, ok := unrenderableLeaf(e); ok {
				return l, true
			}
		}
		return nil, false
	}
	return v, true
}
