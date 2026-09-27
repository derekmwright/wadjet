// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"strings"

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
// These two functions state the one reading both constructs apply, and
// physical.comparisonTyper.memberPair — which decides the pair at plan time
// on every arm — reads MemberLiteralCast's table for the literal's type.

// MemberLiteralCast is the CAST an UNKNOWN-typed (quoted) literal takes on
// the OUTER side of a membership whose set is declared set: PostgreSQL
// resolves the literal to the set's type (#1372) — a DECIMAL at the set's
// own precision and scale, so the literal boxes as the set's members do.
// ok is false where the literal keeps its own reading — a TEXT set, a
// container, a type this engine names no input cast for.
func MemberLiteralCast(set parquet.Column) (string, bool) {
	switch set.Type {
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		return "INTEGER", true
	case parquet.TypeInt64, parquet.TypeDuration:
		return "BIGINT", true
	case parquet.TypeFloat32:
		return "REAL", true
	case parquet.TypeFloat64:
		return "DOUBLE PRECISION", true
	case parquet.TypeDecimal:
		if set.Precision > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", set.Precision, set.Scale), true
		}
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
	name, ok := MemberLiteralCast(parquet.Column{Type: t})
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

// memberProbe is the probe node a membership compiles: an unknown-typed
// literal against a typed set is the literal CAST to the set's type
// (MemberLiteralCast), every other operand itself.
func memberProbe(left plansql.Node, set *parquet.Column) plansql.Node {
	lit, ok := plansql.Unparen(left).(*plansql.Lit)
	if !ok || lit.Kind != plansql.LitString || set == nil {
		return left
	}
	name, ok := MemberLiteralCast(*set)
	if !ok {
		return left
	}
	return &plansql.CastNode{Inner: lit, TypeName: name}
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
