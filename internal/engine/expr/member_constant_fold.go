// SPDX-License-Identifier: MIT

package expr

import (
	"math/big"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A membership's outer operand that is not a literal but whose VALUE is made
// of constants — a choice (CASE / COALESCE / NULLIF / GREATEST / LEAST) over
// constant results, `-(-14.0000000000000000001)`, `'…'::numeric::numeric`,
// `CAST(CAST('…' AS TEXT) AS NUMERIC)`, `CAST(14 AS NUMERIC)` — is the seam's
// "unquoted constant" row: PostgreSQL computes the exact numeric its constants
// spell, while ADR-0024 evaluates these forms as a double here, so the digits
// were gone before the membership saw them (docs/adr/0012-divergences/comparison-membership.md, #1372).
//
// memberConstantProbe folds such an operand at plan time, exactly (math/big),
// and types the RESULT by memberNumericType (batch.DecimalValueType): `CASE
// WHEN true THEN 14.0000000000000000001 END` is the NUMERIC(38,19) value. A
// choice whose conditions or other results read a column has each CONSTANT
// result typed at one NUMERIC(38,S), S the widest result scale, which the
// choice then evaluates exactly (a double beside a float8 column, as in
// PostgreSQL). A numeric quotient
// keeps select_div_scale digits, rounded half away from zero as div_var does.
// A function the fold does not read (sqrt, exp, ln, power) is left to the
// engine unless its value box is a double fed by a constant PostgreSQL types
// numeric: then the membership is 0A000 naming the operand. An explicit float
// CAST or a float-only function (sin) keeps its float reading, as there.

// mcKind is the type category PostgreSQL resolves a folded constant to.
type mcKind int

const (
	mcUnsupported mcKind = iota // the fold does not read this form
	mcNull
	mcInt   // an integer type: int2 / int4 / int8
	mcNum   // numeric
	mcFloat // float4 / float8: PostgreSQL's own approximate value
	mcUnknown
	mcText
	mcBool
)

// mcVal is a folded constant: r for mcInt / mcNum (with scale, PostgreSQL's
// display scale, which a numeric division reads), s for mcUnknown / mcText,
// b for mcBool.
type mcVal struct {
	kind  mcKind
	r     *big.Rat
	scale int
	s     string
	b     bool
}

var mcNo = mcVal{kind: mcUnsupported}

// memberConstantProbe types a constant-valued outer operand against a set
// declared t (DECIMAL, INT32 or INT64). ok is false where there is nothing
// to type: a literal (memberNumericProbe's and MemberLiteralCast's rows), an
// operand that reads a column other than as a choice's condition or result,
// no numeric constant, a folded value that is NULL, text or PostgreSQL's own
// float. err is the literal rule's 22003, or a 0A000: a form the fold does
// not read that this engine evaluates as a double, or a choice whose constant
// results share no DECIMAL(38,S).
func memberConstantProbe(left plansql.Node, t parquet.TypeID) (plansql.Node, bool, error) {
	if !memberConstantCandidate(left) {
		return left, false, nil
	}
	var out plansql.Node
	if mcConstantOnly(left) {
		v, err := mcFold(left)
		if err != nil {
			return left, false, err
		}
		switch v.kind {
		case mcInt, mcNum:
			n, err := mcTypedNumber(v.r, t)
			if err != nil {
				return left, false, err
			}
			out = n
		case mcUnsupported:
			return left, false, mcRefuseDouble(left, left)
		default:
			return left, false, nil
		}
	} else {
		n, ok, err := mcTypeChoiceResults(left)
		if err != nil || !ok {
			return left, false, err
		}
		out = n
	}
	if out.String() == left.String() {
		return left, false, nil
	}
	return out, true, nil
}

// memberConstantCandidate reports whether memberConstantProbe has anything
// to fold: an operand that is not itself a literal, holds a numeric constant
// (or a NUMERIC CAST), and is constant-only or a choice with a constant
// result.
func memberConstantCandidate(left plansql.Node) bool {
	u := plansql.Unparen(left)
	if _, isLit := u.(*plansql.Lit); isLit {
		return false
	}
	if _, ok := memberNumericText(left); ok {
		return false
	}
	if c, ok := u.(*plansql.CastNode); ok && strings.Contains(c.TypeName, "(") {
		// A literal under an explicit NUMERIC(p,s) is the typed cast's own
		// exact reading.
		if _, isLit := plansql.Unparen(c.Inner).(*plansql.Lit); isLit {
			return false
		}
	}
	if !mcHasNumeric(u) {
		return false
	}
	if mcConstantOnly(u) {
		return true
	}
	leaves, ok := mcChoiceResults(u)
	return ok && len(leaves) > 0
}

// mcTypedNumber is a folded number as the typed literal a set declared t
// compares: NUMERIC(38, its own scale) by memberNumericType's one rule, or
// — an integer against an integer set — the integer constant itself.
func mcTypedNumber(r *big.Rat, t parquet.TypeID) (plansql.Node, error) {
	text, _ := mcRatText(r)
	if r.IsInt() && (t == parquet.TypeInt32 || t == parquet.TypeInt64) {
		if _, err := strconv.ParseInt(text, 10, 64); err == nil {
			if abs, neg := strings.CutPrefix(text, "-"); neg {
				return &plansql.UnaryOp{Op: "-", Inner: &plansql.Lit{Value: abs, Kind: plansql.LitNumber}}, nil
			}
			return &plansql.Lit{Value: text, Kind: plansql.LitNumber}, nil
		}
	}
	name, err := memberNumericType(text)
	if err != nil {
		return nil, err
	}
	return &plansql.CastNode{Inner: &plansql.Lit{Value: text, Kind: plansql.LitString}, TypeName: name}, nil
}

// mcRatText is r's exact decimal text and its scale: the fewest fraction
// digits that spell it (a folded division is rounded at its scale, so every
// folded denominator is a product of 2s and 5s).
func mcRatText(r *big.Rat) (string, int) {
	d := new(big.Int).Set(r.Denom())
	two, five := big.NewInt(2), big.NewInt(5)
	var a, b int
	m := new(big.Int)
	for d.Cmp(big.NewInt(1)) != 0 {
		switch {
		case m.Mod(d, two).Sign() == 0:
			d.Quo(d, two)
			a++
		case m.Mod(d, five).Sign() == 0:
			d.Quo(d, five)
			b++
		default:
			// Not a terminating decimal; the fold never builds one.
			return r.FloatString(batch.MaxDecimalScale + 1), batch.MaxDecimalScale + 1
		}
	}
	s := max(a, b)
	return r.FloatString(s), s
}

// mcRefuseDouble is the 0A000 for a constant-valued operand the fold does
// not read, when this engine would compare it as a double although a
// constant PostgreSQL types numeric feeds it; nil when it would not.
func mcRefuseDouble(whole, part plansql.Node) error {
	if !mcHasNumericOutsideFloat(part) || !mcEvalsDouble(part) {
		return nil
	}
	return sqlerr.New("0A000",
		"the membership's outer operand %s is computed from numeric constants that this engine "+
			"evaluates in double precision and cannot fold to their exact value (%s), so it is not "+
			"compared with the subquery at float8 (PostgreSQL computes it as numeric)",
		whole.String(), part.String())
}

// mcEvalsDouble reports whether a constant-only expression's value box is a
// double — what the membership would compare.
func mcEvalsDouble(n plansql.Node) (isDouble bool) {
	defer func() {
		if recover() != nil {
			isDouble = false
		}
	}()
	e, err := Compile(n)
	if err != nil {
		return false
	}
	switch e.Eval(nil, 0).(type) {
	case float64, float32:
		return true
	}
	return false
}

// mcTypeChoiceResults folds every constant RESULT of a choice (its
// conditions, a simple CASE's subject and its other results may read
// columns) and types them all NUMERIC(38,S). A column result keeps its own
// type, and the choice's common type is then the engine's over a DECIMAL
// constant — exact against a DECIMAL or integer column, a double against a
// float8 column, as PostgreSQL's. ok is false when no constant result is a
// numeric (an integer or text choice has no double to lose), or one is
// PostgreSQL's own float.
func mcTypeChoiceResults(left plansql.Node) (plansql.Node, bool, error) {
	u := plansql.Unparen(left)
	results, ok := mcChoiceResults(u)
	if !ok {
		return left, false, nil
	}
	vals := make([]mcVal, len(results))
	hasNum := false
	for i, r := range results {
		v, err := mcFold(r)
		if err != nil {
			return left, false, err
		}
		switch v.kind {
		case mcUnsupported:
			return left, false, mcRefuseDouble(left, r)
		case mcFloat, mcText, mcBool:
			return left, false, nil
		case mcNum:
			hasNum = true
		}
		vals[i] = v
	}
	if !hasNum {
		return left, false, nil
	}
	scale, intDigits := 0, 0
	for i, v := range vals {
		switch v.kind {
		case mcUnknown:
			r, ok, err := mcParseNumeric(v.s)
			if err != nil || !ok {
				// PostgreSQL's numeric input refuses it; so does the engine.
				return left, false, err
			}
			vals[i] = mcVal{kind: mcNum, r: r}
		case mcNull:
			continue
		}
		text, s := mcRatText(vals[i].r)
		scale = max(scale, s)
		ip, _, _ := strings.Cut(strings.TrimPrefix(text, "-"), ".")
		intDigits = max(intDigits, len(strings.TrimLeft(ip, "0")))
	}
	if scale+intDigits > batch.MaxDecimalPrecision {
		return left, false, sqlerr.New("0A000",
			"the membership's outer operand %s chooses among numeric constants that share no exact "+
				"DECIMAL(%d,s) in this engine, so it is not compared with the subquery at float8",
			left.String(), batch.MaxDecimalPrecision)
	}
	typeName := "NUMERIC(" + strconv.Itoa(batch.MaxDecimalPrecision) + "," + strconv.Itoa(scale) + ")"
	typed := make(map[plansql.Node]plansql.Node, len(results))
	for i, r := range results {
		if vals[i].kind == mcNull {
			continue
		}
		typed[r] = &plansql.CastNode{
			Inner:    &plansql.Lit{Value: vals[i].r.FloatString(scale), Kind: plansql.LitString},
			TypeName: typeName,
		}
	}
	return mcReplaceResults(u, typed), true, nil
}

// mcChoiceResults is the constant-only value-bearing leaves of a choice —
// CASE results and ELSE, COALESCE / GREATEST / LEAST / NULLIF arguments,
// recursively through nested choices; a leaf that reads a column is the
// engine's own value and is not listed. ok is false when n is no choice.
func mcChoiceResults(n plansql.Node) ([]plansql.Node, bool) {
	var out []plansql.Node
	var walk func(n plansql.Node)
	walk = func(n plansql.Node) {
		u := plansql.Unparen(n)
		switch v := u.(type) {
		case *plansql.CaseNode:
			for _, w := range v.Whens {
				walk(w.Result)
			}
			if v.Else != nil {
				walk(v.Else)
			}
			return
		case *plansql.FuncCallNode:
			if mcChoiceFunc(v) {
				for _, a := range v.Args {
					walk(a)
				}
				return
			}
		}
		if mcConstantOnly(u) {
			out = append(out, u)
		}
	}
	switch v := plansql.Unparen(n).(type) {
	case *plansql.CaseNode:
	case *plansql.FuncCallNode:
		if !mcChoiceFunc(v) {
			return nil, false
		}
	default:
		return nil, false
	}
	walk(n)
	return out, true
}

// mcReplaceResults copies a choice with each result leaf in typed replaced.
func mcReplaceResults(n plansql.Node, typed map[plansql.Node]plansql.Node) plansql.Node {
	u := plansql.Unparen(n)
	if t, ok := typed[u]; ok {
		return t
	}
	switch v := u.(type) {
	case *plansql.CaseNode:
		c := &plansql.CaseNode{Subject: v.Subject, Whens: make([]plansql.WhenClause, len(v.Whens))}
		for i, w := range v.Whens {
			c.Whens[i] = plansql.WhenClause{Cond: w.Cond, Result: mcReplaceResults(w.Result, typed)}
		}
		if v.Else != nil {
			c.Else = mcReplaceResults(v.Else, typed)
		}
		return c
	case *plansql.FuncCallNode:
		if mcChoiceFunc(v) {
			f := *v
			f.Args = make([]plansql.Node, len(v.Args))
			for i, a := range v.Args {
				f.Args[i] = mcReplaceResults(a, typed)
			}
			return &f
		}
	}
	return n
}

func mcChoiceFunc(f *plansql.FuncCallNode) bool {
	if f.Distinct || f.Star {
		return false
	}
	switch strings.ToLower(f.Name) {
	case "coalesce", "greatest", "least":
		return len(f.Args) > 0
	case "nullif":
		return len(f.Args) == 2
	}
	return false
}

// mcConstantOnly reports whether an expression reads nothing but constants:
// literals under the operators, casts, CASE and the choice functions the
// fold reads, and any other deterministic function (mcDeterministicFunc)
// over constants (which the fold may not read, but whose value is still
// fixed at plan time).
func mcConstantOnly(n plansql.Node) bool {
	switch v := plansql.Unparen(n).(type) {
	case *plansql.Lit:
		return true
	case *plansql.UnaryOp:
		return mcConstantOnly(v.Inner)
	case *plansql.BinaryOp:
		return mcConstantOnly(v.Left) && mcConstantOnly(v.Right)
	case *plansql.CmpExpr:
		return mcConstantOnly(v.Left) && mcConstantOnly(v.Right)
	case *plansql.AndNode:
		return mcConstantOnly(v.Left) && mcConstantOnly(v.Right)
	case *plansql.OrNode:
		return mcConstantOnly(v.Left) && mcConstantOnly(v.Right)
	case *plansql.NotNode:
		return mcConstantOnly(v.Inner)
	case *plansql.IsExpr:
		return mcConstantOnly(v.Left)
	case *plansql.CastNode:
		return mcConstantOnly(v.Inner)
	case *plansql.CaseNode:
		if v.Subject != nil && !mcConstantOnly(v.Subject) {
			return false
		}
		for _, w := range v.Whens {
			if !mcConstantOnly(w.Cond) || !mcConstantOnly(w.Result) {
				return false
			}
		}
		return v.Else == nil || mcConstantOnly(v.Else)
	case *plansql.FuncCallNode:
		if v.Star || v.Distinct || !mcDeterministicFunc(v.Name) {
			return false
		}
		for _, a := range v.Args {
			if !mcConstantOnly(a) {
				return false
			}
		}
		return true
	}
	return false
}

// mcDeterministicFunc excludes the functions whose value is not fixed by
// their arguments (and the aggregates, which read rows).
func mcDeterministicFunc(name string) bool {
	switch strings.ToLower(name) {
	case "random", "now", "current_timestamp", "current_date", "clock_timestamp",
		"statement_timestamp", "transaction_timestamp", "timeofday", "gen_random_uuid",
		"uuid_generate_v4", "embed", "embed_dim", "embed_model", "pg_sleep", "sleep",
		"count", "sum", "avg", "min", "max", "string_agg", "array_agg", "row_field",
		"nextval", "setval", "currval":
		return false
	}
	return true
}

// mcHasNumeric reports whether an expression holds a numeric constant or a
// NUMERIC CAST — something whose exact value a double could lose.
func mcHasNumeric(n plansql.Node) bool {
	found := false
	mcWalk(n, func(x plansql.Node) bool {
		switch v := x.(type) {
		case *plansql.Lit:
			if v.Kind == plansql.LitNumber {
				found = true
			}
		case *plansql.CastNode:
			if k, _, _ := mcCastTarget(v.TypeName); k == mcNum {
				found = true
			}
		}
		return !found
	})
	return found
}

// mcHasNumericOutsideFloat reports whether a constant PostgreSQL types
// numeric — a non-integer (or wider than bigint) numeric constant, or a
// quoted literal under a NUMERIC CAST — feeds the expression other than
// through an explicit float CAST or a function PostgreSQL defines over
// double precision alone (where PostgreSQL's value is a double too).
func mcHasNumericOutsideFloat(n plansql.Node) bool {
	found := false
	mcWalk(n, func(x plansql.Node) bool {
		switch v := x.(type) {
		case *plansql.Lit:
			if v.Kind == plansql.LitNumber {
				if _, err := strconv.ParseInt(v.Value, 10, 64); err != nil {
					found = true
				}
			}
		case *plansql.FuncCallNode:
			if mcFloatOnlyFunc(v.Name) {
				return false // PostgreSQL's own double precision
			}
		case *plansql.CastNode:
			k, _, _ := mcCastTarget(v.TypeName)
			if k == mcFloat {
				return false // do not descend
			}
			if l, ok := plansql.Unparen(v.Inner).(*plansql.Lit); ok && l.Kind == plansql.LitString && k == mcNum {
				found = true
			}
		}
		return !found
	})
	return found
}

// mcFloatOnlyFunc names the functions PostgreSQL defines over double
// precision alone, so a numeric argument reaches them as a float8 there too.
func mcFloatOnlyFunc(name string) bool {
	switch strings.ToLower(name) {
	case "sin", "cos", "tan", "cot", "asin", "acos", "atan", "atan2",
		"sind", "cosd", "tand", "cotd", "asind", "acosd", "atand", "atan2d",
		"sinh", "cosh", "tanh", "asinh", "acosh", "atanh", "degrees", "radians", "cbrt":
		return true
	}
	return false
}

// mcWalk visits an expression's constant-bearing nodes; visit returns false
// to stop descending below a node.
func mcWalk(n plansql.Node, visit func(plansql.Node) bool) {
	if n == nil {
		return
	}
	u := plansql.Unparen(n)
	if !visit(u) {
		return
	}
	switch v := u.(type) {
	case *plansql.UnaryOp:
		mcWalk(v.Inner, visit)
	case *plansql.BinaryOp:
		mcWalk(v.Left, visit)
		mcWalk(v.Right, visit)
	case *plansql.CmpExpr:
		mcWalk(v.Left, visit)
		mcWalk(v.Right, visit)
	case *plansql.AndNode:
		mcWalk(v.Left, visit)
		mcWalk(v.Right, visit)
	case *plansql.OrNode:
		mcWalk(v.Left, visit)
		mcWalk(v.Right, visit)
	case *plansql.NotNode:
		mcWalk(v.Inner, visit)
	case *plansql.IsExpr:
		mcWalk(v.Left, visit)
	case *plansql.CastNode:
		mcWalk(v.Inner, visit)
	case *plansql.CaseNode:
		mcWalk(v.Subject, visit)
		for _, w := range v.Whens {
			mcWalk(w.Cond, visit)
			mcWalk(w.Result, visit)
		}
		mcWalk(v.Else, visit)
	case *plansql.FuncCallNode:
		for _, a := range v.Args {
			mcWalk(a, visit)
		}
	}
}

// mcCastTarget classifies a CAST's type name: the category, and for an
// explicit NUMERIC(p,s) its precision and scale (p = -1 for none). A type
// the fold does not read is mcUnsupported.
func mcCastTarget(typeName string) (kind mcKind, p, s int) {
	name := strings.ToUpper(strings.TrimSpace(typeName))
	base, mod, hasMod := strings.Cut(name, "(")
	base = strings.TrimSpace(base)
	switch base {
	case "NUMERIC", "DECIMAL":
		if !hasMod {
			return mcNum, -1, 0
		}
		ps, _, _ := strings.Cut(mod, ")")
		pt, st, hasScale := strings.Cut(ps, ",")
		pv, err := strconv.Atoi(strings.TrimSpace(pt))
		if err != nil {
			return mcUnsupported, 0, 0
		}
		sv := 0
		if hasScale {
			if sv, err = strconv.Atoi(strings.TrimSpace(st)); err != nil {
				return mcUnsupported, 0, 0
			}
		}
		return mcNum, pv, sv
	case "INT", "INTEGER", "INT4", "INT2", "SMALLINT", "BIGINT", "INT8":
		if hasMod {
			return mcUnsupported, 0, 0
		}
		return mcInt, -1, 0
	case "FLOAT", "FLOAT4", "FLOAT8", "REAL", "DOUBLE", "DOUBLE PRECISION":
		return mcFloat, -1, 0
	case "TEXT", "VARCHAR", "CHARACTER VARYING", "STRING":
		if hasMod {
			return mcUnsupported, 0, 0
		}
		return mcText, -1, 0
	}
	return mcUnsupported, 0, 0
}

// mcIntRange is the range of an integer CAST target.
func mcIntRange(typeName string) (lo, hi int64) {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "INT2", "SMALLINT":
		return -1 << 15, 1<<15 - 1
	case "INT", "INTEGER", "INT4":
		return -1 << 31, 1<<31 - 1
	}
	return -1 << 63, 1<<63 - 1
}

// mcParseNumeric reads text as PostgreSQL's numeric input does, through the
// literal's one rule: a number no DECIMAL(38,s) holds is memberNumericType's
// 22003; text that is no number is ok=false (its input function refuses it).
func mcParseNumeric(text string) (*big.Rat, bool, error) {
	if _, ok := batch.DecimalValueType(text); !ok {
		if _, err := memberNumericType(text); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	r, ok := new(big.Rat).SetString(strings.TrimSpace(text))
	return r, ok, nil
}

// mcFold computes a constant-only expression exactly, as PostgreSQL types
// and computes it; mcUnsupported where it does not read the form or where
// PostgreSQL would raise (the engine then answers for itself). err is the
// literal rule's 22003 for a numeric constant no DECIMAL(38,s) holds
// (mcParseNumeric), which PostgreSQL's unconstrained numeric would compare.
func mcFold(n plansql.Node) (mcVal, error) {
	switch v := plansql.Unparen(n).(type) {
	case *plansql.Lit:
		switch v.Kind {
		case plansql.LitNull:
			return mcVal{kind: mcNull}, nil
		case plansql.LitBool:
			return mcVal{kind: mcBool, b: strings.EqualFold(v.Value, "true")}, nil
		case plansql.LitString:
			return mcVal{kind: mcUnknown, s: v.Value}, nil
		case plansql.LitNumber:
			if i, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
				return mcVal{kind: mcInt, r: new(big.Rat).SetInt64(i)}, nil
			}
			r, ok, err := mcParseNumeric(v.Value)
			if err != nil || !ok {
				return mcNo, err
			}
			return mcVal{kind: mcNum, r: r, scale: mcTextScale(v.Value)}, nil
		}
	case *plansql.UnaryOp:
		x, err := mcFold(v.Inner)
		if err != nil {
			return mcNo, err
		}
		switch x.kind {
		case mcNull, mcFloat:
			return x, nil
		case mcInt, mcNum:
			if v.Op == "-" {
				return mcNumber(x.kind, new(big.Rat).Neg(x.r), x.scale), nil
			}
			if v.Op == "+" {
				return x, nil
			}
		}
	case *plansql.BinaryOp:
		return mcFoldArith(v)
	case *plansql.CmpExpr:
		return mcFoldCmp(v.Left, v.Op, v.Right)
	case *plansql.AndNode, *plansql.OrNode:
		var l, r plansql.Node
		and := false
		if a, ok := v.(*plansql.AndNode); ok {
			l, r, and = a.Left, a.Right, true
		} else {
			o := v.(*plansql.OrNode)
			l, r = o.Left, o.Right
		}
		a, err := mcFoldBool(l)
		if err != nil || a.kind == mcUnsupported {
			return mcNo, err
		}
		b, err := mcFoldBool(r)
		if err != nil || b.kind == mcUnsupported {
			return mcNo, err
		}
		return mcLogic(a, b, and), nil
	case *plansql.NotNode:
		a, err := mcFoldBool(v.Inner)
		if err != nil || a.kind != mcBool {
			return a, err
		}
		return mcVal{kind: mcBool, b: !a.b}, nil
	case *plansql.IsExpr:
		if strings.ToLower(v.Check) != "null" {
			return mcNo, nil
		}
		x, err := mcFold(v.Left)
		if err != nil || x.kind == mcUnsupported {
			return mcNo, err
		}
		return mcVal{kind: mcBool, b: (x.kind == mcNull) != v.Not}, nil
	case *plansql.CastNode:
		return mcFoldCast(v)
	case *plansql.CaseNode:
		return mcFoldCase(v)
	case *plansql.FuncCallNode:
		if mcChoiceFunc(v) {
			return mcFoldChoice(v)
		}
	}
	return mcNo, nil
}

func mcNumber(k mcKind, r *big.Rat, scale int) mcVal {
	if k == mcInt && (!r.IsInt() || !r.Num().IsInt64()) {
		return mcNo // PostgreSQL raises integer out of range
	}
	if k == mcInt {
		scale = 0
	}
	return mcVal{kind: k, r: r, scale: scale}
}

// mcTextScale is the display scale PostgreSQL's numeric input gives a
// spelling: its fraction digits less its exponent, never below zero.
func mcTextScale(text string) int {
	t := strings.TrimLeft(strings.TrimSpace(text), "+-")
	mant, exp := t, 0
	if i := strings.IndexAny(t, "eE"); i >= 0 {
		mant = t[:i]
		e, err := strconv.Atoi(t[i+1:])
		if err != nil {
			return 0
		}
		exp = e
	}
	frac := 0
	if _, f, ok := strings.Cut(mant, "."); ok {
		frac = len(f)
	}
	return max(0, frac-exp)
}

// mcFoldArith is + - * / over numbers, at PostgreSQL's result scales (a
// sum or difference the wider operand's, a product the sum of both, a
// numeric quotient select_div_scale's, an integer quotient truncated); a
// float operand makes PostgreSQL's value a double, which is left as one.
func mcFoldArith(v *plansql.BinaryOp) (mcVal, error) {
	a, err := mcFold(v.Left)
	if err != nil {
		return mcNo, err
	}
	b, err := mcFold(v.Right)
	if err != nil {
		return mcNo, err
	}
	if a.kind == mcUnsupported || b.kind == mcUnsupported {
		return mcNo, nil
	}
	if a.kind == mcFloat || b.kind == mcFloat {
		if (a.kind == mcFloat || a.kind == mcInt || a.kind == mcNum || a.kind == mcNull) &&
			(b.kind == mcFloat || b.kind == mcInt || b.kind == mcNum || b.kind == mcNull) {
			return mcVal{kind: mcFloat}, nil
		}
		return mcNo, nil
	}
	if !mcNumeric(a.kind) || !mcNumeric(b.kind) {
		return mcNo, nil
	}
	k := mcInt
	if a.kind == mcNum || b.kind == mcNum {
		k = mcNum
	}
	if a.kind == mcNull || b.kind == mcNull {
		switch v.Op {
		case "+", "-", "*", "/":
			return mcVal{kind: mcNull}, nil
		}
		return mcNo, nil
	}
	switch v.Op {
	case "+":
		return mcNumber(k, new(big.Rat).Add(a.r, b.r), max(a.scale, b.scale)), nil
	case "-":
		return mcNumber(k, new(big.Rat).Sub(a.r, b.r), max(a.scale, b.scale)), nil
	case "*":
		return mcNumber(k, new(big.Rat).Mul(a.r, b.r), a.scale+b.scale), nil
	case "/":
		if b.r.Sign() == 0 {
			return mcNo, nil // PostgreSQL's 22012; the engine raises its own
		}
		if k == mcInt {
			return mcNumber(mcInt, new(big.Rat).SetInt(new(big.Int).Quo(a.r.Num(), b.r.Num())), 0), nil
		}
		rscale, ok := mcDivScale(a, b)
		if !ok {
			return mcNo, nil
		}
		return mcNumber(mcNum, mcRoundHalfAway(new(big.Rat).Quo(a.r, b.r), rscale), rscale), nil
	}
	return mcNo, nil
}

func mcNumeric(k mcKind) bool { return k == mcInt || k == mcNum || k == mcNull }

// mcDivScale is PostgreSQL's select_div_scale: the quotient keeps at least
// NUMERIC_MIN_SIG_DIGITS (16) significant digits, counted from the weight of
// the first base-10000 digit of each operand, and never fewer fraction
// digits than either operand displays; div_var rounds it there, half away
// from zero.
func mcDivScale(a, b mcVal) (int, bool) {
	w1, d1, ok1 := mcBase10000Lead(a.r)
	w2, d2, ok2 := mcBase10000Lead(b.r)
	if !ok1 || !ok2 {
		return 0, false
	}
	qweight := w1 - w2
	if d1 <= d2 {
		qweight--
	}
	rscale := 16 - qweight*4
	rscale = max(rscale, a.scale, b.scale, 0)
	return min(rscale, 1000), true
}

// mcBase10000Lead is the weight and value of the first non-zero base-10000
// digit of |r| (weight 0, digit 0 for zero), as PostgreSQL's NumericVar
// stores it.
func mcBase10000Lead(r *big.Rat) (weight int, digit int64, ok bool) {
	v := new(big.Rat).Abs(r)
	if v.Sign() == 0 {
		return 0, 0, true
	}
	base := big.NewRat(10000, 1)
	one := big.NewRat(1, 1)
	for weight = 0; v.Cmp(base) >= 0; weight++ {
		v.Quo(v, base)
		if weight > 300 {
			return 0, 0, false
		}
	}
	for v.Cmp(one) < 0 {
		v.Mul(v, base)
		weight--
		if weight < -300 {
			return 0, 0, false
		}
	}
	q := new(big.Int).Quo(v.Num(), v.Denom())
	return weight, q.Int64(), true
}

// mcCommon resolves the values of a choice (or the two sides of a
// comparison) to one category as PostgreSQL's select_common_type does for
// the categories the fold reads: all unknown is text; numbers take the
// widest of integer < numeric < float, and an unknown among them is read as
// that type. ok is false for a mix PostgreSQL refuses or the fold does not
// read.
func mcCommon(vals []mcVal) ([]mcVal, mcKind, bool, error) {
	cat := mcNull
	for _, v := range vals {
		switch v.kind {
		case mcUnsupported:
			return nil, 0, false, nil
		case mcNull, mcUnknown:
		case mcInt, mcNum, mcFloat:
			if cat == mcNull || cat == mcInt || (cat == mcNum && v.kind == mcFloat) {
				cat = v.kind
			} else if cat != mcNum && cat != mcFloat {
				return nil, 0, false, nil
			}
		case mcText, mcBool:
			if cat != mcNull && cat != v.kind {
				return nil, 0, false, nil
			}
			cat = v.kind
		}
	}
	if cat == mcNull {
		for _, v := range vals {
			if v.kind == mcUnknown {
				cat = mcText
			}
		}
	}
	out := make([]mcVal, len(vals))
	for i, v := range vals {
		out[i] = v
		if v.kind == mcNull {
			continue
		}
		switch cat {
		case mcFloat:
			out[i] = mcVal{kind: mcFloat}
		case mcNum, mcInt:
			if v.kind == mcUnknown {
				if cat == mcInt {
					iv, err := strconv.ParseInt(strings.TrimSpace(v.s), 10, 64)
					if err != nil {
						return nil, 0, false, nil
					}
					out[i] = mcVal{kind: mcInt, r: new(big.Rat).SetInt64(iv)}
					continue
				}
				r, ok, err := mcParseNumeric(v.s)
				if err != nil || !ok {
					return nil, 0, false, err
				}
				out[i] = mcVal{kind: mcNum, r: r, scale: mcTextScale(v.s)}
				continue
			}
			out[i].kind = cat
		case mcText:
			if v.kind == mcUnknown {
				out[i] = mcVal{kind: mcText, s: v.s}
			}
		}
	}
	return out, cat, true, nil
}

// mcFoldCmp is a comparison of two folded values: numbers exactly, anything
// else unread.
func mcFoldCmp(l plansql.Node, op string, r plansql.Node) (mcVal, error) {
	a, err := mcFold(l)
	if err != nil {
		return mcNo, err
	}
	b, err := mcFold(r)
	if err != nil {
		return mcNo, err
	}
	vals, cat, ok, err := mcCommon([]mcVal{a, b})
	if err != nil || !ok || (cat != mcInt && cat != mcNum && cat != mcNull) {
		return mcNo, err
	}
	if vals[0].kind == mcNull || vals[1].kind == mcNull {
		return mcVal{kind: mcNull}, nil
	}
	c := vals[0].r.Cmp(vals[1].r)
	var res bool
	switch strings.TrimSpace(op) {
	case "=":
		res = c == 0
	case "<>", "!=":
		res = c != 0
	case "<":
		res = c < 0
	case "<=":
		res = c <= 0
	case ">":
		res = c > 0
	case ">=":
		res = c >= 0
	default:
		return mcNo, nil
	}
	return mcVal{kind: mcBool, b: res}, nil
}

func mcFoldBool(n plansql.Node) (mcVal, error) {
	v, err := mcFold(n)
	if err != nil {
		return mcNo, err
	}
	if v.kind == mcUnknown {
		switch strings.ToLower(strings.TrimSpace(v.s)) {
		case "t", "true", "yes", "on", "1":
			return mcVal{kind: mcBool, b: true}, nil
		case "f", "false", "no", "off", "0":
			return mcVal{kind: mcBool, b: false}, nil
		}
		return mcNo, nil
	}
	if v.kind != mcBool && v.kind != mcNull {
		return mcNo, nil
	}
	return v, nil
}

// mcLogic is three-valued AND / OR.
func mcLogic(a, b mcVal, and bool) mcVal {
	if and {
		if (a.kind == mcBool && !a.b) || (b.kind == mcBool && !b.b) {
			return mcVal{kind: mcBool, b: false}
		}
	} else if (a.kind == mcBool && a.b) || (b.kind == mcBool && b.b) {
		return mcVal{kind: mcBool, b: true}
	}
	if a.kind == mcNull || b.kind == mcNull {
		return mcVal{kind: mcNull}
	}
	return mcVal{kind: mcBool, b: and}
}

// mcFoldCast is a CAST to a type the fold reads: NUMERIC (an explicit
// NUMERIC(p,s) rounds half away from zero, as PostgreSQL's does), an integer
// type (rounding the same way), text, or a float (PostgreSQL's own double).
func mcFoldCast(c *plansql.CastNode) (mcVal, error) {
	kind, p, s := mcCastTarget(c.TypeName)
	if kind == mcUnsupported {
		return mcNo, nil
	}
	x, err := mcFold(c.Inner)
	if err != nil || x.kind == mcUnsupported {
		return mcNo, err
	}
	if x.kind == mcNull {
		return x, nil
	}
	if kind == mcFloat {
		switch x.kind {
		case mcInt, mcNum, mcFloat, mcUnknown, mcText:
			return mcVal{kind: mcFloat}, nil
		}
		return mcNo, nil
	}
	if kind == mcText {
		switch x.kind {
		case mcUnknown, mcText:
			return mcVal{kind: mcText, s: x.s}, nil
		case mcInt, mcNum:
			// PostgreSQL renders a numeric at its display scale.
			_, minScale := mcRatText(x.r)
			return mcVal{kind: mcText, s: x.r.FloatString(max(x.scale, minScale))}, nil
		}
		return mcNo, nil
	}
	// A number.
	var r *big.Rat
	scale := x.scale
	switch x.kind {
	case mcInt, mcNum:
		r = x.r
	case mcUnknown, mcText:
		if kind == mcInt {
			iv, err := strconv.ParseInt(strings.TrimSpace(x.s), 10, 64)
			if err != nil {
				return mcNo, nil
			}
			r = new(big.Rat).SetInt64(iv)
		} else {
			pr, ok, err := mcParseNumeric(x.s)
			if err != nil || !ok {
				return mcNo, err
			}
			r = pr
			scale = mcTextScale(x.s)
		}
	default:
		return mcNo, nil
	}
	if kind == mcInt {
		r = mcRoundHalfAway(r, 0)
		lo, hi := mcIntRange(c.TypeName)
		if !r.Num().IsInt64() || r.Num().Int64() < lo || r.Num().Int64() > hi {
			return mcNo, nil
		}
		return mcVal{kind: mcInt, r: r}, nil
	}
	if p >= 0 {
		if p < 1 || s < 0 || s > p {
			return mcNo, nil
		}
		r = mcRoundHalfAway(r, s)
		scale = s
		t, _ := mcRatText(new(big.Rat).Abs(r))
		ip, _, _ := strings.Cut(t, ".")
		if len(strings.TrimLeft(ip, "0")) > p-s {
			return mcNo, nil // PostgreSQL's 22003; the engine raises its own
		}
	}
	return mcVal{kind: mcNum, r: r, scale: scale}, nil
}

// mcRoundHalfAway rounds r to s fraction digits, half away from zero.
func mcRoundHalfAway(r *big.Rat, s int) *big.Rat {
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(s)), nil)
	x := new(big.Rat).Mul(r, new(big.Rat).SetInt(scale))
	num, den := x.Num(), x.Denom()
	q, m := new(big.Int).QuoRem(num, den, new(big.Int))
	if new(big.Int).Mul(new(big.Int).Abs(m), big.NewInt(2)).Cmp(den) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return new(big.Rat).SetFrac(q, scale)
}

// mcFoldCase evaluates a CASE whose every part is a constant: its type is
// the common type of ALL its results (an unchosen float result makes the
// chosen one a double in PostgreSQL too).
func mcFoldCase(c *plansql.CaseNode) (mcVal, error) {
	results := make([]mcVal, 0, len(c.Whens)+1)
	for _, w := range c.Whens {
		v, err := mcFold(w.Result)
		if err != nil {
			return mcNo, err
		}
		results = append(results, v)
	}
	if c.Else != nil {
		v, err := mcFold(c.Else)
		if err != nil {
			return mcNo, err
		}
		results = append(results, v)
	} else {
		results = append(results, mcVal{kind: mcNull})
	}
	vals, cat, ok, err := mcCommon(results)
	if err != nil || !ok {
		return mcNo, err
	}
	if cat == mcFloat {
		return mcVal{kind: mcFloat}, nil
	}
	for i, w := range c.Whens {
		var cond mcVal
		if c.Subject != nil {
			cond, err = mcFoldCmp(c.Subject, "=", w.Cond)
		} else {
			cond, err = mcFoldBool(w.Cond)
		}
		if err != nil || cond.kind == mcUnsupported {
			return mcNo, err
		}
		if cond.kind == mcBool && cond.b {
			return vals[i], nil
		}
	}
	return vals[len(vals)-1], nil
}

// mcFoldChoice is COALESCE / GREATEST / LEAST / NULLIF over constants.
func mcFoldChoice(f *plansql.FuncCallNode) (mcVal, error) {
	args := make([]mcVal, len(f.Args))
	for i, a := range f.Args {
		v, err := mcFold(a)
		if err != nil {
			return mcNo, err
		}
		args[i] = v
	}
	name := strings.ToLower(f.Name)
	if name == "nullif" {
		// NULLIF's value is its first argument's, compared by `=` at the
		// two arguments' common type.
		vals, cat, ok, err := mcCommon(args)
		if err != nil || !ok {
			return mcNo, err
		}
		first := args[0]
		if first.kind == mcUnknown && (cat == mcInt || cat == mcNum) {
			first = vals[0]
		}
		if cat == mcFloat {
			return mcVal{kind: mcFloat}, nil
		}
		if first.kind == mcNull || vals[1].kind == mcNull {
			return first, nil
		}
		if cat != mcInt && cat != mcNum {
			return mcNo, nil
		}
		if vals[0].r.Cmp(vals[1].r) == 0 {
			return mcVal{kind: mcNull}, nil
		}
		return first, nil
	}
	vals, cat, ok, err := mcCommon(args)
	if err != nil || !ok {
		return mcNo, err
	}
	if cat == mcFloat {
		return mcVal{kind: mcFloat}, nil
	}
	if name == "coalesce" {
		for _, v := range vals {
			if v.kind != mcNull {
				return v, nil
			}
		}
		return mcVal{kind: mcNull}, nil
	}
	if cat != mcInt && cat != mcNum && cat != mcNull {
		return mcNo, nil
	}
	best := mcVal{kind: mcNull}
	for _, v := range vals {
		if v.kind == mcNull {
			continue
		}
		if best.kind == mcNull || (name == "greatest" && v.r.Cmp(best.r) > 0) || (name == "least" && v.r.Cmp(best.r) < 0) {
			best = v
		}
	}
	return best, nil
}
