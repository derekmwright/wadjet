// SPDX-License-Identifier: MIT

package expr

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
)

// castTextToInt reads TEXT with an integer destination's own INPUT FUNCTION,
// and with nothing else.
//
// PostgreSQL has TWO casts to an integer type and they answer differently for
// the same characters. int4in/int8in/int2in read a whole number and refuse
// anything else — measured on 17.11, `'2.5'`, `'2.0'`, `'26.7'`, `'-0.4'` and
// `'1e3'` are each `22P02 invalid input syntax for type <T>` for integer,
// bigint and smallint alike — while numeric→int ROUNDS half away from zero, so
// `2.5` is 3 and `-2.5` is -3. A DECIMAL COLUMN takes the second cast; a quoted
// literal and a STRING column take this one.
//
// The grammar is kernel.IntLitText, which is PostgreSQL's and a strict superset
// of Go's base-10 one: `'0x1A'` is 26, `'0o17'` 15, `'0b101'` 5, `'1_000'` 1000,
// `'017'` decimal seventeen, and C whitespace is trimmed at both ends so
// `' 12 '` is 12. It is the one reader the comparison kernels, the row path and
// the plan-time refusal already share, so the CAST door cannot disagree with
// them about which strings name an integer (#634).
//
// PORT and PROTOCOL are routed to their OWN input function instead, because
// they are types with a text form of their own: PROTOCOL reads the IANA name
// `protocol_name()` prints, and neither reads int4's radix prefixes or
// underscores, which the writer refuses. One grammar per type, arc NT's rule.
// Their DOMAIN is still int4's carrier held to the type's range — that split is
// ADR-0012's and #901's.
//
// There is deliberately NO float fallback. The one that stood here claimed
// `'26.7'::integer` rounds to 27 on the server; it does not, and the claim made
// every quoted fractional literal answer a number PostgreSQL refuses (#1141).
func castTextToInt(s, dest string) any {
	if dest == "port" || dest == "protocol" {
		return castPortProtocolText(s, dest)
	}
	typ := intCastTypeName(dest)
	switch n, st := kernel.IntLitText(s); st {
	case kernel.NumConstOK:
		return castIntInRange(n, dest)
	case kernel.NumConstRange:
		raiseNumericOutOfRange(typ, s)
	}
	raiseInvalidTextRepresentation(typ, s)
	return nil
}

// intCastTypeName is the name PostgreSQL puts in an integer destination's own
// messages — `invalid input syntax for type smallint`, `value "99999" is out of
// range for type smallint`. It is separate from raiseIntegerOutOfRange's name,
// which spells the shorter `smallint out of range` form the ARITHMETIC path
// uses, so that each message keeps the wording of the operation that raised it.
func intCastTypeName(dest string) string {
	switch dest {
	case "bigint", "int8", "int64", "signed":
		// INT64 is BIGINT's wadjet spelling, and physical.inferCastType has
		// always read it as one; the message has to agree, or a client sees
		// `type integer` for a cast whose declared OID is int8.
		return "bigint"
	case "smallint", "int2":
		return "smallint"
	}
	return "integer"
}

// castOperandDeclaresText reports whether an expression's own DECLARATION says
// its values are TEXT — without looking at any value.
//
// It is the question `CAST(x AS INTEGER)` has to answer before it reads x,
// because a DECIMAL and a STRING arrive at Cast.Eval in the SAME Go box (both
// are a string), and the two take different casts. Asking the box cannot
// separate them; asking the expression can.
//
// The shapes that decide:
//
//	'2.5'                  a quoted literal — SQL's `unknown`, read as text here
//	s                      a STRING column
//	CAST(x AS STRING)      a cast whose destination is a text type
//	UPPER(s)               a call whose registered return type is fixed STRING
//	COALESCE(s, 'x')       a choice all of whose arms are themselves text
//	CASE … THEN s ELSE 'x' END
//
// Everything else answers false and keeps the numeric reading, which is the
// safe direction: a DECIMAL column, an arithmetic result, a scalar subquery and
// a container element all box as something this cannot claim is text, and the
// value path reaches castTextToInt anyway when the box turns out to hold a
// string (see Cast.Eval's integer arm). The DECLARATION decides which cast; the
// box only decides whether there is text to read at all.
func castOperandDeclaresText(operand Expr) bool {
	switch v := operand.(type) {
	case *Lit:
		_, isText := v.Val.(string)
		return isText && v.Text == ""
	case *ColRef:
		return v.valueType() == batch.TypeString
	case *Cast:
		t, ok := castDestType(v.DestType)
		return ok && t == batch.TypeString
	case *FuncCall:
		return DefaultRegistry.ReturnType(v.Name).Text()
	case *Coalesce:
		return allDeclareText(v.Args)
	case *Case:
		arms := make([]Expr, 0, len(v.Whens)+1)
		for _, w := range v.Whens {
			arms = append(arms, w.Result)
		}
		if v.Else != nil {
			arms = append(arms, v.Else)
		}
		return allDeclareText(arms)
	}
	return false
}

// allDeclareText is castOperandDeclaresText over the alternatives one value is
// chosen from: every arm must declare text, and an empty set declares nothing.
// A NULL arm is skipped, because `COALESCE(s, NULL)` is as much a text
// expression as `s` is — the rule joinCastBoolDeclared states for its own join.
func allDeclareText(args []Expr) bool {
	seen := false
	for _, a := range args {
		if a == nil {
			continue
		}
		if lit, ok := a.(*Lit); ok && lit.Val == nil {
			continue
		}
		if !castOperandDeclaresText(a) {
			return false
		}
		seen = true
	}
	return seen
}
