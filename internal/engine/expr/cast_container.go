// SPDX-License-Identifier: MIT

package expr

import (
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
	switch d {
	case "char", "varchar", "text", "string", "character", "character varying":
		return nil, false
	case "json":
		return batch.FormatPGJSON(v, containerOperandDecl(b, row, e.Operand)), true
	}
	if _, _, ok := parquet.StringTypeLength(d); ok {
		return nil, false
	}
	panic(fatalEval{sqlerr.New("42846", "cannot cast type %s to %s",
		containerTypeName(containerOperandDecl(b, row, e.Operand), v), d)})
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
		return pgCastSourceName(col.ElementType.Type) + "[]"
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
