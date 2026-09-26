// SPDX-License-Identifier: MIT

package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ArrayCastElement reports the ELEMENT spelling of an array cast destination
// — the parser writes `int[]` and `ARRAY(INT)` alike as `array(int)` — and
// false for every other destination.
//
// The planner declares such a cast as the array OF the element's own scalar
// declaration (physical.nodeDeclaredType), and Cast.Eval converts every
// element to it, so the declaration and the value agree: before arc CW the
// evaluator passed the operand through unchanged under a STRING declaration,
// and `ARRAY['1','2']::int[]` was the text `[1 2]`.
func ArrayCastElement(typeName string) (string, bool) {
	t := strings.TrimSpace(typeName)
	if len(t) < len("array()") || !strings.EqualFold(t[:len("array(")], "array(") || t[len(t)-1] != ')' {
		return "", false
	}
	return strings.TrimSpace(t[len("array(") : len(t)-1]), true
}

// castToArray converts v to an array whose every element is cast to elem: an
// array operand element by element, a text operand through PostgreSQL's
// one-dimensional array input syntax (`{1,2,"a b",NULL}`).
//
// from is the operand's declaration (nil when nothing declares it). With it,
// each element is cast exactly as a COLUMN of the element's declared type is:
// the elements are written into one vector of that type and the scalar cast
// reads them there, so a TIMESTAMP element converts as a timestamp (its text,
// its date) and a DECIMAL one as a number — not as the epoch milliseconds or
// the text its box holds (arc CW round 3: `CAST(ARRAY[ts] AS TEXT[])` was
// `{1704070800000}`).
func castToArray(v any, elem string, from *parquet.Column) any {
	var elems []any
	switch tv := v.(type) {
	case []any:
		elems = tv
	case string:
		if MultiDimArrayText(tv) {
			// Multi-dimensional input passes through as its text, as it did
			// before arc CW (round 5; the planner declares it text): this
			// engine has no multi-dimensional array semantics to read it
			// into (castToArray's array arm, above).
			return tv
		}
		elems = parsePGArrayText(tv)
	default:
		panic(fatalEval{sqlerr.New("42846", "cannot cast a %T value to %s[]", v, elem)})
	}
	// An element with no container text form here (an INTERVAL, which a
	// container carries as a DURATION nanosecond count) is refused before
	// any element is cast (arc CW round 3, ADR-0045 §2).
	if textCastDest(strings.ToLower(strings.TrimSpace(elem))) {
		refuseUnrenderable(elems)
	}
	// A MULTI-DIMENSIONAL array passes through unchanged, as it did before
	// arc CW (arc CW round 5): this engine holds one as an array of arrays
	// and does not have PostgreSQL's multi-dimensional semantics — its
	// leaves, its dimensions, unnest over its leaves — so the round-4 cast
	// that converted the leaves and kept the dimensions answered a shape the
	// rest of the engine then read as the outer array's inner arrays
	// (`unnest` returned `{1,2}` rows where PostgreSQL returns leaves). The
	// value keeps its own declaration (physical.nodeDeclaredType) and is
	// recorded as not PostgreSQL's in postgres-differences; the
	// multi-dimensional semantics are a follow-up of their own.
	if nestedArrayOperand(v, from) {
		return v
	}
	if _, isArr := v.([]any); isArr && from != nil && from.Type == parquet.TypeArray &&
		from.ElementType != nil && ambiguousBoxDecl(from.ElementType) {
		return castElementsAsColumn(elems, elem, *from.ElementType)
	}
	out := make([]any, len(elems))
	for i, e := range elems {
		if e == nil {
			continue
		}
		out[i] = (&Cast{Operand: &Lit{Val: e}, DestType: elem}).Eval(nil, 0)
	}
	return out
}

// nestedArrayOperand reports whether an array operand is multi-dimensional:
// its declaration's element is an array, or an element is one.
func nestedArrayOperand(v any, from *parquet.Column) bool {
	elems, ok := v.([]any)
	if !ok {
		return false
	}
	if from != nil && from.Type == parquet.TypeArray && from.ElementType != nil &&
		from.ElementType.Type == parquet.TypeArray {
		return true
	}
	for _, e := range elems {
		if _, nested := e.([]any); nested {
			return true
		}
	}
	return false
}

// castElementsAsColumn casts each element as a column of the declared element
// type: one vector holds them all, and the same scalar Cast a column operand
// takes reads each row.
func castElementsAsColumn(elems []any, elem string, decl parquet.Column) []any {
	decl.Name, decl.Nullable = "element", true
	eb := batch.NewRecordBatch([]parquet.Column{decl}, len(elems))
	for i, e := range elems {
		eb.Columns[0].SetValue(i, e)
	}
	c := &Cast{Operand: &ColRef{Name: "element"}, DestType: elem}
	out := make([]any, len(elems))
	for i, e := range elems {
		if e == nil {
			continue
		}
		out[i] = c.Eval(eb, i)
	}
	return out
}

// ambiguousBoxDecl reports whether c declares a scalar whose BOX does not say what
// it is — the types a vector boxes as something other than their value (an
// epoch-milliseconds int64, a day count, an address's integer, a DECIMAL's
// text, a REAL widened to a double) — which is where casting the box would
// cast the wrong thing. Every other element's box IS its value and keeps the
// per-box cast, literal spelling and all (`ARRAY[1.5, 2.5]::int[]` rounds its
// literals as numerics, `{2,3}`).
func ambiguousBoxDecl(c *parquet.Column) bool {
	switch c.Type {
	case parquet.TypeTimestamp, parquet.TypeDate, parquet.TypeIPv4, parquet.TypeIPv6,
		parquet.TypeMAC, parquet.TypeUUID, parquet.TypeCIDR, parquet.TypeFloat32:
		return true
	case parquet.TypeDecimal:
		return c.Precision > 0
	}
	return false
}

// MultiDimArrayText reports whether s spells a multi-dimensional array in
// PostgreSQL's text form: an unquoted brace inside the outer pair.
func MultiDimArrayText(s string) bool {
	t := strings.TrimSpace(s)
	if len(t) < 2 || t[0] != '{' {
		return false
	}
	inQuotes := false
	for i := 1; i < len(t)-1; i++ {
		switch c := t[i]; {
		case c == '\\':
			i++
		case c == '"':
			inQuotes = !inQuotes
		case !inQuotes && (c == '{' || c == '}'):
			return true
		}
	}
	return false
}

// parsePGArrayText reads PostgreSQL's array text form for ONE dimension:
// braces around comma-separated elements, an element double-quoted when it
// holds a structural character (a backslash escapes the next character
// inside quotes and out), and an unquoted NULL (any case) as the NULL
// element. A nested brace is a second dimension, which this engine's ragged
// arrays are not; it is refused rather than read as text.
func parsePGArrayText(s string) []any {
	malformed := func(detail string) {
		panic(fatalEval{sqlerr.New("22P02", "malformed array literal: %s (%s)", sqlerr.Quote(s), detail)})
	}
	t := strings.TrimSpace(s)
	if len(t) < 2 || t[0] != '{' || t[len(t)-1] != '}' {
		malformed(`Array value must start with "{" or dimension information.`)
	}
	body := t[1 : len(t)-1]
	if strings.TrimSpace(body) == "" {
		return []any{}
	}
	var out []any
	var cur strings.Builder
	quoted, inQuotes, sawAny := false, false, false
	flush := func() {
		if !sawAny {
			malformed("Unexpected \",\" character.")
		}
		txt := cur.String()
		if !quoted {
			txt = strings.TrimSpace(txt)
			if strings.EqualFold(txt, "NULL") {
				out = append(out, nil)
				cur.Reset()
				quoted, sawAny = false, false
				return
			}
		}
		out = append(out, txt)
		cur.Reset()
		quoted, sawAny = false, false
	}
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case c == '\\':
			if i+1 >= len(body) {
				malformed("Unexpected end of input.")
			}
			i++
			cur.WriteByte(body[i])
			sawAny = true
		case c == '"':
			inQuotes = !inQuotes
			quoted, sawAny = true, true
		case inQuotes:
			cur.WriteByte(c)
		case c == '{' || c == '}':
			panic(fatalEval{sqlerr.New("0A000",
				"multidimensional array input is not supported: %s", sqlerr.Quote(s))})
		case c == ',':
			flush()
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f':
			if sawAny && !quoted {
				cur.WriteByte(c)
			}
		default:
			if quoted {
				malformed("Unexpected array element.")
			}
			cur.WriteByte(c)
			sawAny = true
		}
	}
	if inQuotes {
		malformed("Unexpected end of input.")
	}
	flush()
	return out
}
