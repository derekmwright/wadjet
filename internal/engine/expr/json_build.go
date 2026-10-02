// SPDX-License-Identifier: MIT

package expr

import (
	"encoding/json"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// json_build_object writes each argument as PostgreSQL's datum_to_json does:
// by the argument's DECLARED type, through batch.FormatPGJSON — the one JSON
// renderer CAST(container AS JSON) already uses — never by the Go type of its
// box. The box alone cannot say what it holds: a DATE boxes as its day count
// and a TIMESTAMP as epoch milliseconds (both were written as bare numbers,
// `{"d" : 19786}`, #1466), a DECIMAL as its text (written as a JSON string
// until arc SS), a nested json_build_object as the Go string of its text
// (written as an escaped JSON STRING, `{"o" : "{\"a\" : 1}"}`, #1474).
//
// The declaration is the one the planner's declared-output walk gives the
// argument (argDecls, operand_decl.go). A bare numeric literal is read at its
// own scale (decimalLitText), as PostgreSQL writes 1.50.
//
// A JSON value is the one kind the declaration cannot name: this engine has
// no JSON type, and json_build_object declares text (#1470). A value whose
// own expression PRODUCES json — a json_build_object call, a CAST to json, a
// COALESCE or CASE all of whose arms do — is written as the JSON it is,
// unquoted, as PostgreSQL nests a json value. A json value that reaches the
// call through a column (a derived table's, a CTE's, a stored one) or a scalar
// subquery's answer arrives declared text and is written as a JSON string;
// that residual is the declaration's (#1470), not this renderer's.

// jsonText is an argument already written as JSON text.
type jsonText string

// typeJSONArgs writes every argument of a json_build_object call as JSON
// text: the keys (even positions) as a JSON string of their JSON text, the
// values as their JSON text.
func (e *FuncCall) typeJSONArgs(b *batch.RecordBatch, row int, args []any) {
	for i := range args {
		if args[i] == nil {
			continue
		}
		key := i%2 == 0
		var col *parquet.Column
		if i < len(e.argDecls) && i < len(e.Args) {
			col = e.argDecls[i].shape(b, row, e.Args[i])
		}
		if i < len(e.Args) && jsonProducer(e.Args[i]) {
			if key {
				raiseJSONKeyNotScalar()
			}
			s, ok := args[i].(string)
			if ok {
				if !json.Valid([]byte(s)) {
					panic(fatalEval{sqlerr.New("22P02", "invalid input syntax for type json")})
				}
				args[i] = jsonText(s)
				continue
			}
		}
		text := e.jsonArgText(b, row, i, args[i], col)
		if key {
			if col != nil && (col.Type == parquet.TypeArray || col.Type == parquet.TypeRow || col.Type == parquet.TypeMap) {
				raiseJSONKeyNotScalar()
			}
			switch args[i].(type) {
			case []any, map[string]any:
				raiseJSONKeyNotScalar()
			}
			if !strings.HasPrefix(text, `"`) {
				var sb strings.Builder
				batch.WriteJSONString(&sb, text)
				text = sb.String()
			}
		}
		args[i] = jsonText(text)
	}
}

// jsonArgText is argument i's JSON text under its declaration col (nil when
// the walk has none: a temporal box is then read by the unit its producer
// names, and every other box renders as itself).
func (e *FuncCall) jsonArgText(b *batch.RecordBatch, row, i int, v any, col *parquet.Column) string {
	if i < len(e.Args) {
		if s, ok := decimalLitText(e.Args[i], b, row); ok {
			return batch.FormatPGJSON(s, &parquet.Column{Type: parquet.TypeDecimal})
		}
	}
	if col == nil && i < len(e.Args) {
		switch producedTemporal(e.Args[i], b) {
		case castToDateKind:
			col = &parquet.Column{Type: parquet.TypeDate}
		case castToTimestampKind:
			col = &parquet.Column{Type: parquet.TypeTimestamp}
		}
	}
	switch v.(type) {
	case []any, map[string]any:
		refuseUnrenderable(v)
	}
	return batch.FormatPGJSON(jsonLeafBoxes(v, col), col)
}

// jsonLeafBoxes is a value's box with each network leaf rewritten to the
// value its declaration names (an IPV4 / MAC value or element boxes as its
// encoded integer, which was written as a JSON number: {"ip" : 167772161}). Every other leaf FormatPGJSON reads under its declaration as it
// stands — a DATE's day count, a TIMESTAMP's milliseconds, a DECIMAL's text —
// and a numeric[] element keeps its own digits (`json_build_object('v',
// ARRAY[t.n, 1])` is {"v" : [2.25,1]}, as on PostgreSQL), which writing the
// whole box through a vector would not: a vector stores the array's
// elements at one scale ([2.25,1.00]).
func jsonLeafBoxes(v any, col *parquet.Column) any {
	if v == nil || col == nil {
		return v
	}
	switch x := v.(type) {
	case []any:
		if col.Type != parquet.TypeArray || col.ElementType == nil {
			return v
		}
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonLeafBoxes(e, col.ElementType)
		}
		return out
	case map[string]any:
		if col.Type != parquet.TypeRow {
			return v
		}
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = e
		}
		for i := range col.Fields {
			f := &col.Fields[i]
			if e, ok := x[f.Name]; ok {
				out[f.Name] = jsonLeafBoxes(e, f)
			}
		}
		return out
	}
	switch col.Type {
	case parquet.TypeIPv4, parquet.TypeMAC:
		return batch.DeclaredValue(v, col)
	}
	return v
}

func raiseJSONKeyNotScalar() {
	panic(fatalEval{sqlerr.New("22023", "key value must be scalar, not array, composite, or json")})
}

// jsonProducer reports whether e's own expression produces a json value.
func jsonProducer(e Expr) bool {
	switch x := e.(type) {
	case *FuncCall:
		return strings.EqualFold(x.Name, "json_build_object")
	case *Cast:
		return strings.EqualFold(strings.TrimSpace(x.DestType), "json")
	case *Coalesce:
		return allJSONArms(x.Args)
	case *Case:
		arms := make([]Expr, 0, len(x.Whens)+1)
		for _, w := range x.Whens {
			arms = append(arms, w.Result)
		}
		if x.Else != nil {
			arms = append(arms, x.Else)
		}
		return allJSONArms(arms)
	}
	return false
}

// allJSONArms: every arm that is not a NULL literal produces json, and at
// least one does.
func allJSONArms(arms []Expr) bool {
	n := 0
	for _, a := range arms {
		if l, ok := a.(*Lit); ok && l.Val == nil {
			continue
		}
		if !jsonProducer(a) {
			return false
		}
		n++
	}
	return n > 0
}

// json_build_object(variadic "any"): a JSON object of alternating keys and
// values, rendered the way PostgreSQL renders one — `{"a" : 1, "b" : "x"}`.
// FuncCall.typeJSONArgs has written every argument as JSON text first.
func fnJSONBuildObject(args []any) any {
	if len(args)%2 != 0 {
		panic(fatalEval{sqlerr.New("22023",
			"argument list must have even number of elements")})
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(args); i += 2 {
		if args[i] == nil {
			panic(fatalEval{sqlerr.New("22004", "null value not allowed for object key")})
		}
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(jsonArg(args[i], true))
		b.WriteString(" : ")
		b.WriteString(jsonArg(args[i+1], false))
	}
	b.WriteByte('}')
	return b.String()
}

// jsonArg is an argument's JSON text: typeJSONArgs's, or — for a box that
// did not pass through it — the box rendered without a declaration.
func jsonArg(v any, key bool) string {
	if v == nil {
		return "null"
	}
	if t, ok := v.(jsonText); ok {
		return string(t)
	}
	s := batch.FormatPGJSON(v, nil)
	if key && !strings.HasPrefix(s, `"`) {
		var sb strings.Builder
		batch.WriteJSONString(&sb, s)
		return sb.String()
	}
	return s
}
