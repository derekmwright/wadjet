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
func castToArray(v any, elem string) any {
	var elems []any
	switch tv := v.(type) {
	case []any:
		elems = tv
	case string:
		elems = parsePGArrayText(tv)
	default:
		panic(fatalEval{sqlerr.New("42846", "cannot cast a %T value to %s[]", v, elem)})
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

// containerOperandDecl is the declaration a container operand's value is
// rendered under, as far as the operand itself can say it: a column's (or
// field's) own vector, an array cast's element, a constructor's temporal
// elements. The renderer needs it for one reason — a TIMESTAMP element is
// boxed as its epoch milliseconds and a DATE element built by an expression
// as its day count — and degrades to the value's own rendering (correct for
// every other element type) when nothing can say.
func containerOperandDecl(b *batch.RecordBatch, row int, operand Expr) *parquet.Column {
	switch x := operand.(type) {
	case *ColRef:
		if b == nil {
			return nil
		}
		x.resolve(b)
		if v, _, ok := x.valueVector(b, row); ok && v != nil {
			c := batch.VectorDecl("", v)
			return &c
		}
	case *Cast:
		if elem, ok := ArrayCastElement(x.DestType); ok {
			if t, ok := castElementTemporal(elem); ok {
				el := parquet.Column{Name: "element", Type: t, Nullable: true}
				return &parquet.Column{Type: parquet.TypeArray, ElementType: &el}
			}
		}
	case *ArrayLitExpr:
		for _, e := range x.Elements {
			if c, ok := e.(*Cast); ok {
				if t, ok := castElementTemporal(c.DestType); ok {
					el := parquet.Column{Name: "element", Type: t, Nullable: true}
					return &parquet.Column{Type: parquet.TypeArray, ElementType: &el}
				}
			}
			if inner := containerOperandDecl(b, row, e); inner != nil {
				return &parquet.Column{Type: parquet.TypeArray, ElementType: inner}
			}
		}
	}
	return nil
}

// castElementTemporal names the temporal type a cast destination produces,
// the two whose boxed element is a number the renderer must not print raw.
func castElementTemporal(dest string) (parquet.TypeID, bool) {
	switch castTemporalKind(dest) {
	case castToDateKind:
		return parquet.TypeDate, true
	case castToTimestampKind:
		return parquet.TypeTimestamp, true
	}
	return 0, false
}
