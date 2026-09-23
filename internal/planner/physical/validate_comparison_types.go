// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TWO OPERANDS OF CLASSES POSTGRESQL HAS NO OPERATOR BETWEEN ARE REFUSED, NOT
// COMPARED (#1073, #1216 item 2).
//
// The comparison kernels compared a value of one class with a value of
// another and found them unequal, so a type mismatch answered like data.
// Measured over every pair of the 18 flat types on five arms against
// PostgreSQL 17.11 (arc BR's notes): `a.x = a.y` across classes answered ZERO
// ROWS for every one of the 146 pairs PostgreSQL refuses; `x IN (SELECT y …)`
// answered zero rows, or a runtime join-key error, or — date against integer —
// 11 and 128 rows, or with a set operation in the body a cast error on the
// DAG arms only; NOT IN answered every row; `1 = true` answered false and
// `id = true` was 22P02 on one arm and zero rows on the other. PostgreSQL
// raises 42883 `operator does not exist: bigint = text` for all of them, at
// parse analysis, before any row.
//
// The classes are PostgreSQL's type categories over this engine's types, read
// by what the WIRE declares: numbers (PORT and PROTOCOL are int4, DURATION is
// int8), text, bytea, boolean, the date/time pair, the inet family (IPV4,
// IPV6, CIDR), macaddr, uuid, and each container on its own.
//
// TEXT against a typed operand was read three ways at base, measured with the
// text holding each value's own rendering: as the other type's INPUT for
// DATE, TIMESTAMP, UUID, IPV6, CIDR and BOOL (every row matched, on every
// arm); as the other side's RENDERED text for the numbers (#504: "12.7500"
// against 12.75 is unequal); and as nothing at all for IPV4, MACADDR and BYTEA
// (zero rows where every row matched). Through IN (SELECT …) and = ANY it was
// worse — 0 rows on the single arm and every row on the DAG for DATE,
// TIMESTAMP and IPV4. So:
//
//   - a DIRECT comparison of text with DATE, TIMESTAMP, UUID, IPV6, CIDR or
//     BOOL keeps its reading, which is the input function's and the same on
//     every arm: a recorded superset (ADR-0012 §5, #826's column spelling);
//   - text against a number, IPV4, MACADDR or BYTEA is refused, and so is ANY
//     text membership test against a typed subquery or list, as PostgreSQL
//     refuses all of them.
//
// LITERALS keep their own rule. A QUOTED or NULL literal is SQL's `unknown`
// and takes the other side's type (validate_literal.go). An UNQUOTED numeric
// literal against a text or a temporal column is a recorded superset (ADR-0012
// §5: the literal's text, the epoch instant), so a literal is refused here only
// in the one pairing no reading makes sense of — a number against a boolean,
// either way round (`1 = true`, `id = true`, `(id > 1) = 1`).
//
// An operand is typed STRUCTURALLY — a column's declaration, a CAST, a
// predicate, an aggregate over those — and never from a scalar function's
// registered return type: several of those declare text where the value is a
// date or an address (`current_date`, `to_date`, `network_address`), and a
// refusal built on that would refuse `current_date = CAST(now() AS date)`,
// which PostgreSQL answers. An operand this layer cannot type is never
// refused.
//
// The operator in the message is PostgreSQL's: `=` for IN, = ANY and IS [NOT]
// DISTINCT FROM, `<>` for != and <>, `>=` then `<=` for BETWEEN.
type cmpClass int

const (
	cmpUnknown cmpClass = iota
	cmpNumber
	cmpText
	cmpBytes
	cmpBool
	cmpTemporal
	cmpInet
	cmpMAC
	cmpUUID
	cmpArray
	cmpRow
	cmpMap
	cmpVector
)

func comparisonClass(t parquet.TypeID) cmpClass {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64,
		parquet.TypeDecimal, parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration:
		return cmpNumber
	case parquet.TypeString:
		return cmpText
	case parquet.TypeBytes:
		return cmpBytes
	case parquet.TypeBool:
		return cmpBool
	case parquet.TypeDate, parquet.TypeTimestamp:
		return cmpTemporal
	case parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR:
		return cmpInet
	case parquet.TypeMAC:
		return cmpMAC
	case parquet.TypeUUID:
		return cmpUUID
	case parquet.TypeArray:
		return cmpArray
	case parquet.TypeRow:
		return cmpRow
	case parquet.TypeMap:
		return cmpMap
	case parquet.TypeVector:
		return cmpVector
	}
	return cmpUnknown
}

// comparisonTyper types one comparison operand for the class question.
type comparisonTyper struct {
	scope  *colScope
	typeOf func(plansql.Node) (parquet.TypeID, bool)
	// subquery types a subquery's output columns, or nil when it cannot.
	subquery func(sql string) []parquet.TypeID
	// shape is an operand's full declaration, for two ROWs.
	shape func(plansql.Node) (parquet.Column, bool)
}

func (c *comparisonTyper) operand(n plansql.Node) (parquet.TypeID, bool) {
	n = plansql.Unparen(n)
	switch v := n.(type) {
	case *plansql.Lit:
		switch v.Kind {
		case plansql.LitNumber:
			if strings.ContainsAny(v.Value, ".eE") {
				return parquet.TypeDecimal, true
			}
			return parquet.TypeInt32, true
		case plansql.LitBool:
			return parquet.TypeBool, true
		}
		// SQL's `unknown`.
		return 0, false
	case *plansql.UnaryOp:
		if lit, ok := plansql.Unparen(v.Inner).(*plansql.Lit); ok && lit.Kind == plansql.LitNumber {
			return c.operand(lit)
		}
	case *plansql.SubqueryNode:
		if c.subquery == nil {
			return 0, false
		}
		if ts := c.subquery(v.SQL); len(ts) == 1 {
			return ts[0], true
		}
		return 0, false
	case *plansql.FuncCallNode:
		// A SUBSCRIPT is lowered to element_at, whose declaration mirrors the
		// CONTAINER; what it produces is the element (validate_boolean.go's
		// reason). Typed by the element where the scope has it, else not at all.
		if strings.EqualFold(v.Name, "element_at") && len(v.Args) > 0 {
			return c.scope.provableElementType(v.Args[0])
		}
	case *plansql.IntervalLit:
		return 0, false
	}
	return c.typeOf(n)
}

// isTypedLiteral reports whether an operand is an unquoted numeric or boolean
// constant (a sign over one included).
func isTypedLiteral(n plansql.Node) bool {
	n = plansql.Unparen(n)
	if u, ok := n.(*plansql.UnaryOp); ok {
		n = plansql.Unparen(u.Inner)
	}
	lit, ok := n.(*plansql.Lit)
	return ok && (lit.Kind == plansql.LitNumber || lit.Kind == plansql.LitBool)
}

// textReadsAsInput is the set of classes a DIRECT comparison with a text
// operand reads through the other type's input function — the kept superset.
func textReadsAsInput(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeDate, parquet.TypeTimestamp, parquet.TypeUUID,
		parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeBool:
		return true
	}
	return false
}

// pair refuses one DIRECT comparison's two operands when both are typed and
// their classes differ.
func (c *comparisonTyper) pair(a, b plansql.Node, op string) error {
	return c.pairOf(a, b, op, false)
}

// pairOf is pair, and for a MEMBERSHIP test (IN, = ANY / ALL) when member is
// set — where no text reading is kept.
func (c *comparisonTyper) pairOf(a, b plansql.Node, op string, member bool) error {
	ta, ok := c.operand(a)
	if !ok {
		return nil
	}
	tb, ok := c.operand(b)
	if !ok {
		return nil
	}
	ca, cb := comparisonClass(ta), comparisonClass(tb)
	if ca == cmpRow && cb == cmpRow {
		return c.rowPair(a, b)
	}
	if ca == cmpUnknown || cb == cmpUnknown || ca == cb {
		return nil
	}
	if (isTypedLiteral(a) || isTypedLiteral(b)) &&
		!(ca == cmpBool && cb == cmpNumber) && !(ca == cmpNumber && cb == cmpBool) {
		return nil
	}
	if !member && ((ca == cmpText && textReadsAsInput(tb)) || (cb == cmpText && textReadsAsInput(ta))) {
		return nil
	}
	return sqlerr.New("42883", "operator does not exist: %s %s %s", cmpTypeName(ta), op, cmpTypeName(tb))
}

// structuralTypeOf types an operand from what the statement WROTE: a column's
// declaration, a CAST's target, a predicate, and an aggregate over those. A
// scalar function's registered return type is deliberately not read (see the
// file comment).
func structuralTypeOf(decls ColDecls) func(plansql.Node) (parquet.TypeID, bool) {
	var typeOf func(plansql.Node) (parquet.TypeID, bool)
	typeOf = func(n plansql.Node) (parquet.TypeID, bool) {
		switch v := plansql.Unparen(n).(type) {
		case *plansql.ColRef:
			col, ok := decls.colDecl(v)
			return col.Type, ok
		case *plansql.CastNode:
			return structuralCastType(v.TypeName)
		case *plansql.CmpExpr, *plansql.AndNode, *plansql.OrNode, *plansql.NotNode,
			*plansql.IsExpr, *plansql.LikeExpr, *plansql.BetweenExpr, *plansql.InExpr,
			*plansql.ExistsNode, *plansql.AnyAllExpr:
			return parquet.TypeBool, true
		case *plansql.UnaryOp:
			if t, ok := typeOf(v.Inner); ok && comparisonClass(t) == cmpNumber {
				return t, true
			}
		case *plansql.FuncCallNode:
			switch strings.ToLower(v.Name) {
			case "count":
				return parquet.TypeInt64, true
			case "min", "max":
				if len(v.Args) == 1 {
					return typeOf(v.Args[0])
				}
			case "sum", "avg":
				if len(v.Args) == 1 {
					if t, ok := typeOf(v.Args[0]); ok && comparisonClass(t) == cmpNumber {
						return parquet.TypeFloat64, true
					}
				}
			case "string_agg":
				return parquet.TypeString, true
			}
		}
		return 0, false
	}
	return typeOf
}

// rowPair compares two ROWs field by field, PostgreSQL's record comparison:
// a different width is 42804 `cannot compare record types with different
// numbers of columns`, a field pair of different classes 42804 `cannot compare
// dissimilar column types …` (measured: `c_row = c_rownest` answered false on
// every row here, #1060/#1065). Fields this layer does not know decide nothing.
func (c *comparisonTyper) rowPair(a, b plansql.Node) error {
	if c.shape == nil {
		return nil
	}
	sa, ok := c.shape(a)
	if !ok || len(sa.Fields) == 0 {
		return nil
	}
	sb, ok := c.shape(b)
	if !ok || len(sb.Fields) == 0 {
		return nil
	}
	if len(sa.Fields) != len(sb.Fields) {
		return sqlerr.New("42804", "cannot compare record types with different numbers of columns")
	}
	for i := range sa.Fields {
		fa, fb := comparisonClass(sa.Fields[i].Type), comparisonClass(sb.Fields[i].Type)
		if fa != fb && fa != cmpUnknown && fb != cmpUnknown {
			return sqlerr.New("42804", "cannot compare dissimilar column types %s and %s at record column %d",
				cmpTypeName(sa.Fields[i].Type), cmpTypeName(sb.Fields[i].Type), i+1)
		}
	}
	return nil
}

func cmpTypeName(t parquet.TypeID) string {
	if t == parquet.TypeCIDR {
		return "cidr"
	}
	return pgTypeName(t)
}

// pgComparisonOp is the operator PostgreSQL names for a comparison's spelling.
func pgComparisonOp(op string) string {
	switch strings.ToLower(op) {
	case "!=", "<>":
		return "<>"
	case "is distinct from", "is not distinct from", "==":
		return "="
	}
	return op
}

// walk visits every comparison in node, CHILDREN FIRST — PostgreSQL analyses
// inside-out, so `(id > 1) = 1` names the outer `boolean = integer`.
// Subqueries are their own blocks and are not entered.
func (c *comparisonTyper) walk(node plansql.Node) error {
	switch n := node.(type) {
	case nil, *plansql.SubqueryNode, *plansql.ExistsNode:
		return nil
	case *plansql.WindowFuncNode:
		if n.Func != nil {
			if err := c.walk(n.Func); err != nil {
				return err
			}
		}
		for _, p := range n.PartitionBy {
			if err := c.walk(p); err != nil {
				return err
			}
		}
		for _, o := range n.OrderBy {
			if err := c.walk(o.Expr); err != nil {
				return err
			}
		}
		return nil
	}
	for _, child := range exprOperands(node) {
		if err := c.walk(child); err != nil {
			return err
		}
	}
	switch n := node.(type) {
	case *plansql.CmpExpr:
		op := pgComparisonOp(n.Op)
		lt, lok := plansql.Unparen(n.Left).(*plansql.TupleNode)
		rt, rok := plansql.Unparen(n.Right).(*plansql.TupleNode)
		if lok && rok && len(lt.Elements) == len(rt.Elements) {
			for i := range lt.Elements {
				if err := c.pair(lt.Elements[i], rt.Elements[i], op); err != nil {
					return err
				}
			}
			return nil
		}
		return c.pair(n.Left, n.Right, op)
	case *plansql.InExpr:
		for _, v := range n.Values {
			if err := c.inPair(n.Left, v, "="); err != nil {
				return err
			}
		}
	case *plansql.AnyAllExpr:
		for _, v := range n.Values {
			if err := c.inPair(n.Left, v, pgComparisonOp(n.Op)); err != nil {
				return err
			}
		}
	case *plansql.BetweenExpr:
		if err := c.pair(n.Left, n.Low, ">="); err != nil {
			return err
		}
		return c.pair(n.Left, n.High, "<=")
	case *plansql.FuncCallNode:
		// NULLIF(x, y) is an EQUALITY between its arguments.
		if strings.EqualFold(n.Name, "nullif") && len(n.Args) == 2 {
			return c.pair(n.Args[0], n.Args[1], "=")
		}
	case *plansql.CaseNode:
		if n.Subject != nil {
			for _, w := range n.Whens {
				if err := c.pair(n.Subject, w.Cond, "="); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// inPair is one IN / ANY / ALL member: a row constructor against a
// multi-column subquery compares column by column.
func (c *comparisonTyper) inPair(left, member plansql.Node, op string) error {
	if tup, ok := plansql.Unparen(left).(*plansql.TupleNode); ok {
		sub, isSub := plansql.Unparen(member).(*plansql.SubqueryNode)
		if !isSub || c.subquery == nil {
			return nil
		}
		ts := c.subquery(sub.SQL)
		if len(ts) != len(tup.Elements) {
			return nil
		}
		for i, e := range tup.Elements {
			te, ok := c.operand(e)
			if !ok {
				continue
			}
			if ca, cb := comparisonClass(te), comparisonClass(ts[i]); ca != cmpUnknown && cb != cmpUnknown && ca != cb {
				return sqlerr.New("42883", "operator does not exist: %s %s %s", cmpTypeName(te), op, cmpTypeName(ts[i]))
			}
		}
		return nil
	}
	return c.pairOf(left, member, op, true)
}
