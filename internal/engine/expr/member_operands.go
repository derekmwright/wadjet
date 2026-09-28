// SPDX-License-Identifier: MIT

package expr

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The two operands of a membership against a subquery — `x IN (SELECT …)`,
// `x = ANY (SELECT …)` and their negations — are ONE comparison, and on the
// single-process arm they meet row by row in InSubquery / CorrelatedInSubquery
// as boxes: the probe as its expression evaluates, the set as the subquery's
// rows were boxed. Two boxes of one declared type can differ, and the
// membership then missed every member: a DATE probe boxes as its day count
// while a DATE set member comes back as its ISO text, so `d IN (SELECT d …
// UNION ALL …)` or `d IN (SELECT DATE '…' FROM …)` answered 0 rows where the
// DAG — which inlines the set as an IN list of literals — and PostgreSQL
// answer the matching rows (#1373); and a quoted literal probe stayed TEXT
// against a typed set, so `'12' IN (SELECT bigint …)` answered 0 where
// PostgreSQL resolves the literal to bigint and answers every row (#1372).
// The functions below state the one reading both constructs apply. The
// literal's type is MemberLiteralCast's, of the set's TypeID alone — and
// against a NUMERIC set, the value its text spells (memberNumericType):
// physical.comparisonTyper.memberPair refuses a literal that is no value of
// it at plan time (CheckMemberProbe), and MemberProbe builds the typed
// literal — physical.typeMemberLiterals writes it into the logical plan, so
// every arm (the DAG's inlined IN list included) compares an already-typed
// value.

// MemberLiteralCast is the CAST an UNKNOWN-typed (quoted) literal takes on
// the OUTER side of a membership whose set is declared t: PostgreSQL
// resolves the literal to the set's TYPE (#1372), never its typmod. It
// takes the TypeID alone so that no caller can hand it a precision. ok is
// false where the literal keeps its own reading — a TEXT set, a container,
// a type this engine names no input cast for — and for a DECIMAL set, whose
// literal is typed by its own digits instead (memberNumericType): there is
// no bare NUMERIC here, because a bare NUMERIC of a literal boxes as a
// double (ADR-0024), and a double read '14.0000000000000000001' as the
// member 14.
func MemberLiteralCast(t parquet.TypeID) (string, bool) {
	switch t {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		return "INTEGER", true
	case parquet.TypeInt64, parquet.TypeDuration:
		return "BIGINT", true
	case parquet.TypeFloat32:
		return "REAL", true
	case parquet.TypeFloat64:
		return "DOUBLE PRECISION", true
	case parquet.TypeDate:
		return "DATE", true
	case parquet.TypeTimestamp:
		return "TIMESTAMP", true
	case parquet.TypeBool:
		return "BOOLEAN", true
	case parquet.TypeUUID:
		return "UUID", true
	case parquet.TypeIPv4:
		return "IPV4", true
	case parquet.TypeIPv6:
		return "IPV6", true
	case parquet.TypeCIDR:
		return "CIDR", true
	case parquet.TypeMAC:
		return "MACADDR", true
	}
	return "", false
}

// CheckMemberProbe reads a membership's outer operand as the set's type, at
// plan time, and reports the typed literal's own refusal of its text (22P02
// / 22007 in PostgreSQL's words) or — against a NUMERIC set — the 22003 of a
// number no DECIMAL(38,s) holds: PostgreSQL coerces the constant while it
// analyses the statement, so `'zz' IN (SELECT bigint …)` is refused before
// any row, on every arm — it answered 0 rows on all five. An operand
// MemberProbe does not type is not checked here.
func CheckMemberProbe(left plansql.Node, t parquet.TypeID) (err error) {
	typed, ok, err := MemberProbe(left, t)
	if err != nil || !ok {
		return err
	}
	c, ok := typed.(*plansql.CastNode)
	if !ok {
		return nil
	}
	lit, ok := c.Inner.(*plansql.Lit)
	if !ok {
		return nil
	}
	defer func() {
		if p := recover(); p != nil {
			fe, ok := p.(fatalEval)
			if !ok {
				panic(p)
			}
			err = fe.err
		}
	}()
	(&Cast{Operand: &Lit{Val: lit.Value}, DestType: strings.ToLower(c.TypeName)}).Eval(nil, 0)
	return nil
}

// MemberProbe is a membership's outer operand read as the set's type: an
// unknown-typed (quoted) literal against a set declared t is the literal
// CAST to MemberLiteralCast(t), a numeric literal against a NUMERIC set —
// and, one that is not a plain integer, against an integer set — is the
// literal CAST to NUMERIC(38, its own scale) (memberNumericProbe), and
// every other operand is itself (ok false). It is the ONE constructor of the
// typed literal: physical.typeMemberLiterals writes it into the logical plan
// every arm consumes (the DAG's inlined IN list included), and the compiler
// applies it to an expression that reached it without a plan — a DML door's
// WHERE — where it finds the literal still untyped. A literal the plan
// already typed is a CAST with a precision, which no rule here matches, so
// the two never both apply. err is the 22003 of a numeric literal no
// DECIMAL(38,s) holds.
func MemberProbe(left plansql.Node, t parquet.TypeID) (plansql.Node, bool, error) {
	switch t {
	case parquet.TypeDecimal:
		return memberNumericProbe(left)
	case parquet.TypeInt32, parquet.TypeInt64:
		// numeric = integer is numeric in PostgreSQL: a numeric literal that
		// is not already an integer the set's own rung reads exactly — a
		// fractional, exponent or wide constant, or a quoted literal under
		// a bare NUMERIC CAST — is typed as against a NUMERIC set. A QUOTED
		// literal alone takes the set's integer type below (#1372).
		if lit, ok := plansql.Unparen(left).(*plansql.Lit); !ok || lit.Kind != plansql.LitString {
			if text, ok := memberNumericText(left); ok {
				if _, err := strconv.ParseInt(text, 10, 64); err == nil {
					return left, false, nil
				}
				return memberNumericProbe(left)
			}
		}
	}
	lit, ok := plansql.Unparen(left).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return left, false, nil
	}
	name, ok := MemberLiteralCast(t)
	if !ok {
		return left, false, nil
	}
	return &plansql.CastNode{Inner: lit, TypeName: name}, true, nil
}

// MemberProbeCandidate reports whether MemberProbe can type this operand
// against SOME set — so a caller that must resolve the set's type first asks
// for it only when there is something to type.
func MemberProbeCandidate(left plansql.Node) bool {
	if lit, ok := plansql.Unparen(left).(*plansql.Lit); ok && lit.Kind == plansql.LitString {
		return true
	}
	_, ok := memberNumericText(left)
	return ok
}

// memberNumericProbe is the outer operand of a membership against a NUMERIC
// set, when that operand is a numeric LITERAL — a quoted literal, a quoted
// literal under a bare `CAST(… AS NUMERIC | DECIMAL)` / `::numeric`, or an
// unquoted numeric constant (signed or not): the literal CAST to
// NUMERIC(38, s), s its value's own scale. PostgreSQL reads every one of
// these as the exact numeric the text spells. This engine's bare NUMERIC of a
// literal boxes as a double (ADR-0024: a bare destination over text, and a
// numeric constant's own box), which InSubquery's float set and the per-row
// memberDecimalEqual compare at float8's 15-17 digits — so
// '14.0000000000000000001' matched the member 14 in every spelling a hand
// scan of the text did not read. The type comes from ONE rule,
// batch.DecimalValueType, over every text PostgreSQL's numeric input
// accepts; a number no DECIMAL(38,s) holds is refused 22003 rather than
// compared approximately. Text that is no number keeps the CAST, whose own
// input function refuses it (22P02).
func memberNumericProbe(left plansql.Node) (plansql.Node, bool, error) {
	text, ok := memberNumericText(left)
	if !ok {
		return left, false, nil
	}
	name, err := memberNumericType(text)
	if err != nil {
		return left, false, err
	}
	var lit *plansql.Lit
	if l, isLit := literalUnder(left); isLit && l.Kind == plansql.LitString {
		lit = l
	} else {
		lit = &plansql.Lit{Value: text, Kind: plansql.LitString}
	}
	return &plansql.CastNode{Inner: lit, TypeName: name}, true, nil
}

// memberNumericText is the literal text of an outer operand memberNumericProbe
// types: a quoted literal, a quoted literal under a bare NUMERIC / DECIMAL
// CAST, or a numeric constant with its sign.
func memberNumericText(left plansql.Node) (string, bool) {
	switch n := plansql.Unparen(left).(type) {
	case *plansql.Lit:
		if n.Kind == plansql.LitString || n.Kind == plansql.LitNumber {
			return n.Value, true
		}
	case *plansql.CastNode:
		switch strings.ToUpper(strings.TrimSpace(n.TypeName)) {
		case "NUMERIC", "DECIMAL":
			if l, ok := plansql.Unparen(n.Inner).(*plansql.Lit); ok && l.Kind == plansql.LitString {
				return l.Value, true
			}
		}
	case *plansql.UnaryOp:
		if l, ok := plansql.Unparen(n.Inner).(*plansql.Lit); ok && l.Kind == plansql.LitNumber && (n.Op == "-" || n.Op == "+") {
			return n.Op + l.Value, true
		}
	}
	return "", false
}

// literalUnder is the quoted literal a memberNumericText operand carries.
func literalUnder(left plansql.Node) (*plansql.Lit, bool) {
	switch n := plansql.Unparen(left).(type) {
	case *plansql.Lit:
		return n, true
	case *plansql.CastNode:
		l, ok := plansql.Unparen(n.Inner).(*plansql.Lit)
		return l, ok
	}
	return nil, false
}

// memberNumericType is the NUMERIC(38, s) a numeric literal's text is read
// as against a NUMERIC set — the one rule, batch.DecimalValueType — or the
// 22003 of a number this engine's 38-digit DECIMAL cannot carry exactly:
// PostgreSQL's numeric is unconstrained and compares it, and comparing it
// here at any other precision would be a plausible wrong value
// (docs/postgres-differences.md). NaN and the infinities are values no
// DECIMAL holds (ADR-0024 item 6). Text that names no number is NUMERIC(38,0),
// whose input function refuses it 22P02 as PostgreSQL's does.
func memberNumericType(text string) (string, error) {
	if t, ok := batch.DecimalValueType(text); ok {
		return "NUMERIC(38," + strconv.Itoa(t.Scale) + ")", nil
	}
	_, _, _, isNumber := parquet.DecimalTextParts(text)
	if !isNumber && parquet.DecimalSpecialText(text) == parquet.DecimalFinite {
		return "NUMERIC(38,0)", nil
	}
	return "", sqlerr.New("22003",
		"numeric field overflow: the literal %q has no exact value in this engine's "+
			"DECIMAL, which holds at most %d significant digits and scale %d and no NaN or "+
			"infinity, so it is not compared with a NUMERIC member at any other precision "+
			"(PostgreSQL's numeric is unconstrained and answers)",
		strings.TrimSpace(text), batch.MaxDecimalPrecision, batch.MaxDecimalScale)
}

// memberProbe is MemberProbe for the compiler, over the set's declaration.
func memberProbe(left plansql.Node, set *parquet.Column) (plansql.Node, error) {
	if set == nil {
		return left, nil
	}
	n, _, err := MemberProbe(left, set.Type)
	return n, err
}

// memberSetBox is one set member as the probe carries a value of the set's
// DECLARED type: a DATE member's row box is its ISO text, and the probe of a
// DATE is its day count. The declaration decides, never the box's shape — a
// TEXT set holding date-looking text stays text.
func memberSetBox(v any, set *parquet.Column) any {
	if set == nil || set.Type != parquet.TypeDate {
		return v
	}
	s, ok := v.(string)
	if !ok {
		return v
	}
	days, err := parquet.ParseDateDays(s)
	if err != nil {
		return v
	}
	return int64(days)
}

// memberDecimalEqual is the per-row membership's reading of a DECIMAL set
// member — and of an INTEGER member against a decimal probe — the rung
// InSubquery's decSet / fltSet take for the whole set: the
// member is its rendered text at the set's scale, and the probe is decimal
// text at its OWN scale or an integer — compared by canonical decimal value
// — or a float. A float probe is an operand DECLARED float8 (a double
// column or expression, ADR-0024's float-declared bare CAST over a double or
// text), which PostgreSQL also compares with numeric at float8. A numeric
// LITERAL never arrives as one: MemberProbe typed it NUMERIC(38, its own
// scale), whose box is decimal text. Comparing the two boxes as they stood
// missed every member whose rendering differed — `'12.5' IN (SELECT
// numeric(38,10) … WHERE r.id = a.id)` answered 0 rows where PostgreSQL
// answers the row. decided is false for anything else.
func memberDecimalEqual(lv, v any, set *parquet.Column) (eq, decided bool) {
	if set == nil {
		return false, false
	}
	switch set.Type {
	case parquet.TypeInt32, parquet.TypeInt64:
		// An INTEGER member against a DECIMAL probe (its rendered text) is
		// numeric = integer, which PostgreSQL resolves as numeric: the two
		// meet by canonical value, as InSubquery's inSetInt set does on its
		// decimal rung. The boxes as they stood — "14.0000" against 14 —
		// missed every member.
		ps, isText := lv.(string)
		if !isText {
			return false, false
		}
		pk, ok := batch.CanonicalDecimalText(ps)
		if !ok {
			return false, false
		}
		mk, ok := inSubqueryDecimalKey(v)
		if !ok {
			return false, false
		}
		return pk == mk, true
	case parquet.TypeDecimal:
	default:
		return false, false
	}
	s, ok := v.(string)
	if !ok {
		return false, false
	}
	if _, isFloat := lv.(float64); isFloat {
		mf, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return false, false
		}
		return cmpFloat64Op(lv.(float64), mf, CmpEq), true
	}
	key, ok := batch.CanonicalDecimalText(s)
	if !ok {
		return false, false
	}
	pk, ok := inSubqueryDecimalKey(lv)
	if !ok {
		return false, false
	}
	return pk == key, true
}
