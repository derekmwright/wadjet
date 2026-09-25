// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// cmpClass groups structurally known operands for comparison validation.
// Unresolved operands defer; unknown literals use the other operand type.
// Text conversion follows textConversionAnswers per pair and context, with
// set-operation membership additionally requiring a matching CAST origin.
// Two typed/text column join keys refuse; recorded numeric-literal readings
// remain accepted. Number/boolean pairs refuse 42883. Scalar function return
// labels alone do not prove a class. See ADR-0012 §5, #1073 and #1216.
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
	// setOrigins is a SET-OPERATION body's per-column text-cast origins
	// (binder.textOrigin), nil when the body is not a set operation.
	setOrigins func(sql string) []parquet.TypeID
	// shape is an operand's full declaration, for two ROWs.
	shape func(plansql.Node) (parquet.Column, bool)
	// joinKeys is set for a JOIN's ON clause, where two plain COLUMNS of a
	// text/typed pair become hash-join keys: that path failed on three arms
	// (#615's key-type error) and answered zero rows on the shuffled one at
	// base, so no text reading is kept for it (arc BR round 2).
	joinKeys bool
	// decls is the scope's declarations, for the temporal-arithmetic rule's
	// DECLARED operand types (temporalArithmetic).
	decls ColDecls
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

// textConversionAnswers is the MEASURED per-pair table for text against a
// typed operand (arc BR round 2, br_codex/corpus.json text*/, base 260fc569):
// the pair is kept where the base engine had ONE conversion that answered the
// same on all five arms — a value compared with its own rendering matched all
// 20 rows everywhere — and refused where it did not.
//
//	type            direct / CAST JOIN / IN list   IN (subquery), = ANY
//	int4, int8, float8, numeric,
//	port, protocol, duration        keep            keep
//	uuid, ipv6, cidr                keep            keep
//	date, timestamp, boolean        keep            REFUSE: 0 rows single, 20 DAG
//	real                            REFUSE: 3 of 20 matched (a wrong conversion)
//	bytea, ipv4, macaddr            REFUSE: 0 of 20 directly; 0 single, 20 DAG as a subquery
//
// subquery is set for a membership test against a subquery body, a
// set-operation body included; setOpMemberPair also requires the CAST origin.
// Two plain text/typed column join keys are refused separately.
func textConversionAnswers(t parquet.TypeID, subquery bool) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat64, parquet.TypeDecimal,
		parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration,
		parquet.TypeUUID, parquet.TypeIPv6, parquet.TypeCIDR:
		return true
	case parquet.TypeDate, parquet.TypeTimestamp, parquet.TypeBool:
		return !subquery
	}
	return false
}

// pair refuses one DIRECT comparison's two operands when both are typed and
// their classes differ.
func (c *comparisonTyper) pair(a, b plansql.Node, op string) error {
	return c.pairOf(a, b, op, false)
}

// pairOf is pair, and for a membership test against a SUBQUERY (IN, = ANY /
// ALL) when subquery is set.
func (c *comparisonTyper) pairOf(a, b plansql.Node, op string, subquery bool) error {
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
	_, colA := plansql.Unparen(a).(*plansql.ColRef)
	_, colB := plansql.Unparen(b).(*plansql.ColRef)
	hashKey := c.joinKeys && colA && colB
	if !hashKey && ((ca == cmpText && textConversionAnswers(tb, subquery)) || (cb == cmpText && textConversionAnswers(ta, subquery))) {
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
			// `x = ANY(arr)` over an ARRAY expression compares x with the
			// array's ELEMENTS — PostgreSQL's scalar-op-ANY(array) form, which
			// every catalog query writes (`oid = ANY(pol.polroles)`,
			// `a.attnum = ANY(ix.indkey)`). Pairing x with the array itself
			// refused it 42883; x is paired with the ELEMENT instead.
			if _, isSub := plansql.Unparen(v).(*plansql.SubqueryNode); !isSub {
				if te, ok := c.arrayElement(v); ok {
					if err := c.elementPair(n.Left, te, pgComparisonOp(n.Op)); err != nil {
						return err
					}
					continue
				}
				if t, ok := c.operand(v); ok && t == parquet.TypeArray {
					continue
				}
			}
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
	case *plansql.BinaryOp:
		return c.temporalArithmetic(n)
	}
	return nil
}

// temporalArithmetic refuses the `+` / `-` pairs PostgreSQL has no operator
// for once one side is a DATE or a TIMESTAMP: a timestamp and a number
// (`ts + 0`, `now() - 1`), a date and a fractional number (`d + 1.5`), a
// number minus a date or a timestamp, and the sum of two temporal values.
// Each answered a NUMBER here — the epoch count plus the operand, under no
// temporal declaration — and the two execution paths typed that number
// differently (arc VL round 3's census: `MAX(c_ts) + 0` answered
// 1.772532e+12 on one arm and 1.7e+12 on the DAG). `date ± integer`,
// `date - date`, `timestamp - timestamp` and any INTERVAL shift keep their
// meaning (binOpTemporalType). A side typed by neither the statement's
// structure nor its declarations is never refused.
func (c *comparisonTyper) temporalArithmetic(n *plansql.BinaryOp) error {
	if n.Op != "+" && n.Op != "-" {
		return nil
	}
	if nodeIsInterval(n.Left, c.decls) || nodeIsInterval(n.Right, c.decls) {
		return nil
	}
	if r, err := c.unknownTemporal(n); r != expr.UnknownNotTemporal || err != nil {
		return err
	}
	lt, lok := c.arithOperand(n.Left)
	rt, rok := c.arithOperand(n.Right)
	if !lok || !rok {
		return nil
	}
	lTemp := lt == parquet.TypeDate || lt == parquet.TypeTimestamp
	rTemp := rt == parquet.TypeDate || rt == parquet.TypeTimestamp
	refuse := false
	switch {
	case lTemp && rTemp:
		// date - date and timestamp - timestamp have a meaning; a sum of two
		// temporal values, or a date against a timestamp, is refused.
		refuse = n.Op == "+" || lt != rt
	case lTemp:
		refuse = comparisonClass(rt) == cmpNumber && (lt == parquet.TypeTimestamp || !intArithColumnType(rt))
	case rTemp:
		refuse = comparisonClass(lt) == cmpNumber &&
			(n.Op == "-" || rt == parquet.TypeTimestamp || !intArithColumnType(lt))
	}
	if !refuse {
		return nil
	}
	return sqlerr.New("42883", "operator does not exist: %s %s %s", pgTypeName(lt), n.Op, pgTypeName(rt))
}

// unknownTemporal is the unknown-literal branch of temporalArithmetic: a
// quoted literal (or NULL) beside an operand typed DATE or TIMESTAMP is
// resolved by PostgreSQL's operator resolution (expr.ResolveUnknownTemporal)
// — `date + unknown` is 42725, and a literal the resolution reads as a DATE,
// a TIMESTAMP or an INTERVAL is refused here when its text is not one, as
// PostgreSQL refuses the constant while it analyses the statement. Before arc
// VL round 5 arithOperand called the quoted side SQL's unknown and let the
// pair through, and the kernel read the literal's leading number (round-4
// review B3: `DATE '…' + '1.5'` stored 20516.5).
func (c *comparisonTyper) unknownTemporal(n *plansql.BinaryOp) (expr.UnknownTemporal, error) {
	lText, lUnknown := unknownOperand(n.Left)
	rText, rUnknown := unknownOperand(n.Right)
	if lUnknown == rUnknown {
		return expr.UnknownNotTemporal, nil
	}
	other, text := n.Left, rText
	if lUnknown {
		other, text = n.Right, lText
	}
	t, ok := c.arithOperand(other)
	if !ok || (t != parquet.TypeDate && t != parquet.TypeTimestamp) {
		return expr.UnknownNotTemporal, nil
	}
	r := expr.ResolveUnknownTemporal(n.Op, t == parquet.TypeTimestamp)
	if r == expr.UnknownAmbiguous {
		return r, expr.UnknownTemporalAmbiguous(n.Op, lUnknown)
	}
	if text == nil {
		return r, nil // NULL: the resolved operator answers NULL
	}
	return r, expr.CheckUnknownTemporalLiteral(r, *text)
}

// unknownOperand reports an unknown-typed operand — a quoted literal (its
// text) or NULL (nil text).
func unknownOperand(n plansql.Node) (*string, bool) {
	lit, ok := plansql.Unparen(n).(*plansql.Lit)
	if !ok {
		return nil, false
	}
	switch lit.Kind {
	case plansql.LitString:
		return &lit.Value, true
	case plansql.LitNull:
		return nil, true
	}
	return nil, false
}

// arithOperand types one side of a `+` / `-`: by the statement's structure
// (a column, a cast, a literal, an aggregate over those) and, past that, by
// its declaration — so a clock function or nested date arithmetic is typed
// too.
func (c *comparisonTyper) arithOperand(n plansql.Node) (parquet.TypeID, bool) {
	// A window aggregate is typed as its aggregate is: `MAX(ts) OVER ()` is
	// the timestamp `MAX(ts)` is, on every arm.
	if w, ok := plansql.Unparen(n).(*plansql.WindowFuncNode); ok && w.Func != nil {
		return c.arithOperand(w.Func)
	}
	if t, ok := c.operand(n); ok {
		if t == parquet.TypeString && isTextColRef(n, c.decls) {
			return 0, false // a VARCHAR column is a day to date arithmetic
		}
		return t, true
	}
	if lit, ok := plansql.Unparen(n).(*plansql.Lit); ok && lit.Kind == plansql.LitString {
		return 0, false // SQL's unknown
	}
	d, conf := nodeDeclaredType(n, c.decls)
	if conf != expr.Decided || d.Untyped {
		return 0, false
	}
	return d.ID, true
}

// elementPair is `x op ANY/ALL(arr)` over a TYPED array: x against the
// array's declared ELEMENT, by PostgreSQL's rule and nothing wider — two
// classes with no operator between them are 42883, a typed literal
// included (`1 = ANY(text[])`, `'x'::text = ANY(bigint[])`). The text
// conversions textConversionAnswers keeps and the unquoted-literal superset
// were measured for DIRECT comparisons, not for an element read out of an
// array at run time, so neither extends to this form (arc PC round 3: the
// element pair was not checked at all, and a stored bigint[] against text
// answered false where the base engine and PostgreSQL raise 42883). An
// array whose element this layer cannot type — an ARRAY[…] of constants
// included, as before — is never refused.
func (c *comparisonTyper) elementPair(left plansql.Node, te parquet.TypeID, op string) error {
	tl, ok := c.operand(left)
	if !ok {
		return nil
	}
	cl, ce := comparisonClass(tl), comparisonClass(te)
	if cl == cmpUnknown || ce == cmpUnknown || cl == ce {
		return nil
	}
	return sqlerr.New("42883", "operator does not exist: %s %s %s", cmpTypeName(tl), op, cmpTypeName(te))
}

// arrayElement is a typed array operand's declared ELEMENT: a column's
// (qualified or bare), or a CAST's `T[]` target (`array(T)` as parsed).
func (c *comparisonTyper) arrayElement(arr plansql.Node) (parquet.TypeID, bool) {
	switch v := plansql.Unparen(arr).(type) {
	case *plansql.ColRef:
		return c.scope.provableElementType(v)
	case *plansql.CastNode:
		// The parser spells `T[]` as `array(T)`; a nested array's element
		// is itself an array, which this layer does not type.
		name := strings.TrimSpace(v.TypeName)
		lower := strings.ToLower(name)
		switch {
		case strings.HasPrefix(lower, "array(") && strings.HasSuffix(lower, ")"):
			name = strings.TrimSpace(name[len("array(") : len(name)-1])
		case strings.HasSuffix(name, "[]"):
			name = strings.TrimSpace(strings.TrimSuffix(name, "[]"))
		default:
			return 0, false
		}
		if l := strings.ToLower(name); strings.HasSuffix(l, "]") || strings.HasPrefix(l, "array(") {
			return 0, false
		}
		return structuralCastType(name)
	}
	return 0, false
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
			if ca, cb := comparisonClass(te), comparisonClass(ts[i]); ca != cmpUnknown && cb != cmpUnknown && ca != cb &&
				!(ca == cmpText && textConversionAnswers(ts[i], true)) && !(cb == cmpText && textConversionAnswers(te, true)) {
				return sqlerr.New("42883", "operator does not exist: %s %s %s", cmpTypeName(te), op, cmpTypeName(ts[i]))
			}
		}
		return nil
	}
	sub, isSub := plansql.Unparen(member).(*plansql.SubqueryNode)
	if isSub && c.setOrigins != nil {
		if origins := c.setOrigins(sub.SQL); origins != nil {
			return c.setOpMemberPair(left, member, op, origins)
		}
	}
	return c.pairOf(left, member, op, isSub)
}

// setOpMemberPair is a membership against a SET-OPERATION body. There the
// DAG resolves the body's text to the typed side's type and CASTS every
// value, while the single-process arms compare the text as it stands — so
// whether the arms agree depends on the DATA, not on the type (#1073: a
// bigint against `product` text answered 0 rows on the single arms and
// failed the cast on the DAG; the same pair over `CAST(x AS TEXT)` text
// answered identically everywhere, br_codex2 setin/*). The pair is kept only
// where the text PROVABLY converts: it was made by a CAST from a value of
// the typed side's own class in every arm of the body, and that class is a
// kept one (textConversionAnswers). Anything else is PostgreSQL's 42883.
func (c *comparisonTyper) setOpMemberPair(left, member plansql.Node, op string, origins []parquet.TypeID) error {
	tl, ok := c.operand(left)
	if !ok {
		return nil
	}
	tr, ok := c.operand(member)
	if !ok {
		return nil
	}
	cl, cr := comparisonClass(tl), comparisonClass(tr)
	if cl == cmpUnknown || cr == cmpUnknown || cl == cr || (cl != cmpText && cr != cmpText) {
		return c.pairOf(left, member, op, true)
	}
	if cr == cmpText && len(origins) == 1 && origins[0] != typeAmbiguous &&
		comparisonClass(origins[0]) == cl && textConversionAnswers(tl, true) {
		return nil
	}
	return sqlerr.New("42883", "operator does not exist: %s %s %s", cmpTypeName(tl), op, cmpTypeName(tr))
}

// textCastOrigin is the structural type a `CAST(x AS text)` read, or
// typeAmbiguous when the node is not such a cast of a typed value.
func textCastOrigin(n plansql.Node, typeOf func(plansql.Node) (parquet.TypeID, bool)) parquet.TypeID {
	c, ok := plansql.Unparen(n).(*plansql.CastNode)
	if !ok {
		return typeAmbiguous
	}
	if t, ok := structuralCastType(c.TypeName); !ok || t != parquet.TypeString {
		return typeAmbiguous
	}
	if t, ok := typeOf(c.Inner); ok {
		return t
	}
	return typeAmbiguous
}

// RefuseTemporalArithmetic is the expression-typing rule temporalArithmetic
// states, for an expression a DML door evaluates with no SELECT around it: an
// INSERT … VALUES cell, an UPDATE SET value, an UPDATE / DELETE WHERE, a
// MERGE clause. Every `+` / `-` in the tree is typed exactly as the SELECT
// binder types it (comparisonTyper.arithOperand over the target's declared
// columns — the structure first, then nodeDeclaredType), so `ts + 0`,
// `d + 1.5`, `1 - d` and `d + ts` are 42883 on every statement, not only on
// the ones the binder's clause walk reaches (round-3 review B3: VALUES stored
// the epoch-count number, UPDATE SET stored it, and UPDATE / DELETE WHERE
// compared it). alias is the name the target answers to ("" for none); a
// subquery's body is its own statement and is not entered.
func RefuseTemporalArithmetic(node plansql.Node, alias string, schema []parquet.Column) error {
	if node == nil {
		return nil
	}
	scope := newColScope()
	for _, c := range schema {
		scope.addQualifiedTyped(alias, c.Name, c.Type)
		scope.addRowColumn(c)
		scope.addElementType(c)
	}
	decls := rowFieldScopeDecls(scope)
	c := &comparisonTyper{scope: scope, typeOf: structuralTypeOf(decls), shape: foldTypeOf(decls), decls: decls,
		subquery:   func(string) []parquet.TypeID { return nil },
		setOrigins: func(string) []parquet.TypeID { return nil }}
	return c.walkTemporalArithmetic(node)
}

// walkTemporalArithmetic visits every operand the comparison walk visits
// (exprOperands, a window's argument / PARTITION BY / ORDER BY) and applies
// temporalArithmetic to each `+` / `-`, innermost first.
func (c *comparisonTyper) walkTemporalArithmetic(node plansql.Node) error {
	switch n := node.(type) {
	case nil, *plansql.SubqueryNode, *plansql.ExistsNode:
		return nil
	case *plansql.WindowFuncNode:
		var kids []plansql.Node
		if n.Func != nil {
			kids = append(kids, n.Func)
		}
		kids = append(kids, n.PartitionBy...)
		for _, o := range n.OrderBy {
			kids = append(kids, o.Expr)
		}
		for _, k := range kids {
			if err := c.walkTemporalArithmetic(k); err != nil {
				return err
			}
		}
		return nil
	}
	for _, child := range exprOperands(node) {
		if err := c.walkTemporalArithmetic(child); err != nil {
			return err
		}
	}
	if b, ok := node.(*plansql.BinaryOp); ok {
		return c.temporalArithmetic(b)
	}
	return nil
}
