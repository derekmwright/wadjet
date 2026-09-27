// SPDX-License-Identifier: MIT

package expr

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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
// literal's type is MemberLiteralCast's, of the set's TypeID alone:
// physical.comparisonTyper.memberPair refuses a literal that is no value of
// it at plan time, and MemberProbe builds the typed literal —
// physical.typeMemberLiterals writes it into the logical plan, so every arm
// (the DAG's inlined IN list included) compares an already-typed value.

// MemberLiteralCast is the CAST an UNKNOWN-typed (quoted) literal takes on
// the OUTER side of a membership whose set is declared t: PostgreSQL
// resolves the literal to the set's TYPE (#1372) — never its typmod, so a
// DECIMAL set reads the literal as bare NUMERIC at the literal's own
// digits: `'12.50001' IN (SELECT numeric(18,4) …)` compares 12.50001, and
// rounding it to the column's scale answered every row where PostgreSQL
// answers none. It takes the TypeID alone so that no caller can hand it a
// precision: the plan-time check (CheckMemberLiteral) and the typed probe
// read one type. ok is false where the literal keeps its own reading — a
// TEXT set, a container, a type this engine names no input cast for.
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
	case parquet.TypeDecimal:
		return "NUMERIC", true
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

// CheckMemberLiteral reads a quoted literal as MemberLiteralCast's type for a
// set declared t, at plan time, and reports the cast's own refusal of its
// text (22P02 / 22007 in PostgreSQL's words): PostgreSQL coerces the constant
// while it analyses the statement, so `'zz' IN (SELECT bigint …)` is refused
// before any row, on every arm — it answered 0 rows on all five.
func CheckMemberLiteral(t parquet.TypeID, text string) (err error) {
	name, ok := MemberLiteralCast(t)
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
	(&Cast{Operand: &Lit{Val: text}, DestType: strings.ToLower(name)}).Eval(nil, 0)
	return nil
}

// MemberProbe is a membership's outer operand read as the set's type: an
// unknown-typed (quoted) literal against a set declared t is the literal
// CAST to MemberLiteralCast(t), and every other operand is itself (ok false).
// It is the ONE constructor of the typed literal: physical.typeMemberLiterals
// writes it into the logical plan every arm consumes (the DAG's inlined IN
// list included), and the compiler applies it to an expression that reached
// it without a plan — a DML door's WHERE — where it finds the literal still
// quoted. A literal the plan already typed is a CAST, not a quoted literal,
// so the two never both apply.
func MemberProbe(left plansql.Node, t parquet.TypeID) (plansql.Node, bool) {
	lit, ok := plansql.Unparen(left).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString {
		return left, false
	}
	name, ok := MemberLiteralCast(t)
	if !ok {
		return left, false
	}
	return &plansql.CastNode{Inner: lit, TypeName: name}, true
}

// memberProbe is MemberProbe for the compiler, over the set's declaration.
func memberProbe(left plansql.Node, set *parquet.Column) plansql.Node {
	if set == nil {
		return left
	}
	n, _ := MemberProbe(left, set.Type)
	return n
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
// member, the rung InSubquery's decSet / fltSet take for the whole set: the
// member is its rendered text at the set's scale, and the probe is a float
// (a numeric constant, the bare NUMERIC a quoted literal takes against the
// set, MemberLiteralCast), an integer, or decimal text at its OWN scale.
// Comparing the two boxes as they stood missed every member whose rendering
// differed — `'12.5' IN (SELECT numeric(38,10) … WHERE r.id = a.id)` and
// `12.5 IN (…)` answered 0 rows where PostgreSQL answers the row — so a
// float probe compares at float8 and every other one by its canonical
// decimal value, as InSubquery does. decided is false for anything else.
func memberDecimalEqual(lv, v any, set *parquet.Column) (eq, decided bool) {
	if set == nil || set.Type != parquet.TypeDecimal {
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
