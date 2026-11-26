// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The INTERVAL rule (#1713d, the maintainer's A+ interim; ADR-0012 temporal
// r28 / r20 / r30): this engine has no interval column type, so an
// interval-valued expression may not LEAVE the expression that computes it.
//
// It is decided on the statement as written — the binder's original,
// resolved tree, before logical construction and before any rewrite
// (pushdown through a projection, projection pruning, DISTINCT to grouping,
// decorrelation, table-function argument folding) could erase the evidence —
// in the shared validation every door runs (bindColumns, stamped or not), and
// for the write doors on every expression they evaluate (RefuseIntervalWrite).
//
// An expression is interval-valued by its PROVENANCE (expr.IsIntervalNode: an
// INTERVAL literal, a CAST to INTERVAL, the interval arithmetic, a CASE /
// COALESCE / NULLIF / GREATEST / LEAST that answers one), never by the text a
// column holds. A column is never one: a block that would publish an interval
// is refused, so no derived table, CTE, set-operation arm, VALUES row or
// subquery can hand one on — which is why no lineage needs to be carried past
// a block's output.
//
// An interval-valued node is admitted only by a parent on the ALLOWLIST below,
// each entry an operation whose every cell in the kept matrix
// (internal/coordinator/arc_1713c_interval_order_arms_test.go) agrees with
// PostgreSQL 17.11 in value, and in declared type where one is visible, on
// every arm:
//
//   - a comparison of two intervals (=, <>, <, <=, >, >=, IS [NOT] DISTINCT
//     FROM, BETWEEN, an inline IN list, a simple CASE's WHEN values), an
//     unknown literal or NULL standing for one, answering BOOLEAN; IS [NOT]
//     NULL;
//   - interval arithmetic (interval ± interval, interval × number, interval /
//     number, unary minus) and a CASE / COALESCE / NULLIF / GREATEST / LEAST
//     over intervals, as an INNER node whose own parent is then judged;
//   - date / timestamp ± interval (and date_add / date_sub with an interval),
//     answering a TIMESTAMP;
//   - time_bucket's stride; date_part over an interval (float8 on both);
//   - an explicit CAST(<interval> AS TEXT), the display workaround, or to
//     INTERVAL.
//
// Everything else that consumes an interval is refused: a query result
// column, a written value, a GROUP BY / ORDER BY / PARTITION BY / window
// ORDER BY key, an aggregate's or a window function's argument, any other
// function's argument (EXTRACT among them: numeric there, float8 here), an
// array or row element, a comparison with a subquery's column, `||`, and a
// table function's argument. Where PostgreSQL itself refuses the statement
// its SQLSTATE is given (42804 for a CASE / COALESCE / GREATEST / LEAST that
// mixes an interval with another type, 42883 for an operator or function
// that does not exist for an interval, 22007 for constant text that is no
// interval input); the rest is 0A000.
//
// What this rule cannot see — the ROW's text under CAST(s AS INTERVAL), which
// may be input this engine cannot hold (an opaque IntervalValue) — is guarded
// where it is applied, compared or rendered, 0A000 at run time
// (expr.compareIntervals, addInterval, time_bucket, the text cast).

// intervalRefusal is the 0A000 for an interval used where this engine cannot
// yet take one.
func intervalRefusal(use string) error {
	return sqlerr.New("0A000", "interval values cannot yet be used as %s: compare or apply an interval "+
		"inline, cast it to text explicitly for display (CAST(... AS TEXT)), or use DURATION for a "+
		"stored nanosecond duration", use)
}

// tsDiffRefusal is the 0A000 for a timestamp difference used beside an
// interval. `timestamp - timestamp` is a double of milliseconds here
// (ADR-0012 temporal r16) where PostgreSQL types it interval and answers the
// statement, so a 42883 / 42804 for a number beside an interval would claim
// a refusal PostgreSQL does not make, and an answer would compare or combine
// a millisecond count with an interval.
func tsDiffRefusal(use string) error {
	return sqlerr.New("0A000", "a timestamp difference (timestamp - timestamp) cannot yet be %s: "+
		"it is a number of milliseconds here, not an interval; compare the timestamps themselves "+
		"instead, for example ts > TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '30 days' or "+
		"a.ts > b.ts + INTERVAL '30 days'", use)
}

// isTimestampDifference reports `timestamp - timestamp` (r16's millisecond
// double), parentheses aside.
func (t intervalTyper) isTimestampDifference(n plansql.Node) bool {
	b, ok := plansql.Unparen(n).(*plansql.BinaryOp)
	if !ok || b.Op != "-" {
		return false
	}
	l, lok := t.declaredID(b.Left)
	r, rok := t.declaredID(b.Right)
	return lok && rok && l == parquet.TypeTimestamp && r == parquet.TypeTimestamp
}

// intervalTyper carries the declarations an operand's type is read from.
type intervalTyper struct {
	decls ColDecls
	// subquery types a scalar subquery's one output column (the binder's
	// declaration of its body), nil where no binder is at hand.
	subquery func(sql string) (parquet.TypeID, bool)
}

// isInterval is the provenance rule.
func (t intervalTyper) isInterval(n plansql.Node) bool {
	return n != nil && expr.IsIntervalNode(n)
}

// isUnknown reports a quoted literal or NULL: PostgreSQL resolves either to
// the interval beside it.
func isUnknownOperand(n plansql.Node) bool {
	l, ok := plansql.Unparen(n).(*plansql.Lit)
	return ok && (l.Kind == plansql.LitString || l.Kind == plansql.LitNull)
}

// quotedLiteral is n's quoted text, if n is a quoted literal.
func quotedLiteral(n plansql.Node) (string, bool) {
	l, ok := plansql.Unparen(n).(*plansql.Lit)
	if !ok || l.Kind != plansql.LitString {
		return "", false
	}
	return l.Value, true
}

// typeName is an operand's PostgreSQL type name for a 42804 / 42883 message.
func (t intervalTyper) typeName(n plansql.Node) string {
	if t.isInterval(n) {
		return "interval"
	}
	if l, ok := plansql.Unparen(n).(*plansql.Lit); ok {
		switch l.Kind {
		case plansql.LitString, plansql.LitNull:
			return "unknown"
		case plansql.LitNumber:
			if strings.ContainsAny(l.Value, ".eE") {
				return "numeric"
			}
			return "integer"
		}
	}
	if id, ok := t.declaredID(n); ok {
		return pgTypeName(id)
	}
	return "unknown"
}

// declaredID is an operand's declared type, when decided.
func (t intervalTyper) declaredID(n plansql.Node) (parquet.TypeID, bool) {
	switch v := plansql.Unparen(n).(type) {
	case *plansql.Lit:
		return 0, false
	case *plansql.SubqueryNode:
		if t.subquery != nil && !v.Array {
			return t.subquery(v.SQL)
		}
		return 0, false
	}
	d, c := nodeDeclaredType(n, t.decls)
	if c != expr.Decided || d.Untyped {
		return 0, false
	}
	return d.ID, true
}

// checkUnknown is an unknown literal resolved to interval: constant text is
// read at plan time, as PostgreSQL reads it when it resolves the statement.
func checkUnknown(n plansql.Node) error {
	if s, ok := quotedLiteral(n); ok {
		return expr.CheckIntervalLiteralText(s)
	}
	return nil
}

// refuseIntervalTop judges an expression at a position that MATERIALIZES its
// value (a result column, a key, a written value), then walks it.
func (t intervalTyper) refuseIntervalTop(n plansql.Node, use string) error {
	if n == nil {
		return nil
	}
	if err := t.walk(n); err != nil {
		return err
	}
	if t.isInterval(n) {
		// PostgreSQL folds a constant when it plans: its own refusal (the
		// literal's range, an overflow, a division by zero) comes first.
		if err := expr.ConstantEvalError(n); err != nil {
			return err
		}
		return intervalRefusal(use)
	}
	return nil
}

// refuseIntervalCondition judges a boolean clause (WHERE, ON, HAVING,
// QUALIFY): an interval there is no boolean.
func (t intervalTyper) refuseIntervalCondition(n plansql.Node, clause string) error {
	if n == nil {
		return nil
	}
	if err := t.walk(n); err != nil {
		return err
	}
	if t.isInterval(n) {
		return sqlerr.New("42804", "argument of %s must be type boolean, not type interval", clause)
	}
	return nil
}

// walk visits every node of an expression (not a subquery's body, which is
// its own block), innermost first, and judges each interval-valued child by
// its parent: an inner refusal (an aggregate's argument) is the one named.
func (t intervalTyper) walk(node plansql.Node) error {
	switch n := node.(type) {
	case nil, *plansql.SubqueryNode, *plansql.ExistsNode:
		return nil
	case *plansql.WindowFuncNode:
		if n.Func != nil {
			if err := t.walk(n.Func); err != nil {
				return err
			}
		}
		for _, pb := range n.PartitionBy {
			if err := t.refuseIntervalTop(pb, "a PARTITION BY key"); err != nil {
				return err
			}
		}
		for _, ob := range n.OrderBy {
			if err := t.refuseIntervalTop(ob.Expr, "a window ORDER BY key"); err != nil {
				return err
			}
		}
		return nil
	}
	for _, k := range exprOperands(node) {
		if err := t.walk(k); err != nil {
			return err
		}
	}
	return t.judge(node)
}

// judge decides one node over its (already walked) operands.
func (t intervalTyper) judge(node plansql.Node) error {
	switch n := node.(type) {
	case *plansql.CastNode:
		if strings.EqualFold(strings.TrimSpace(n.TypeName), "interval") {
			if err := checkUnknown(n.Inner); err != nil {
				return err
			}
			// Only text (and an interval) casts to INTERVAL: PostgreSQL's
			// 42846 for any other source comes before the rule's 0A000.
			if !t.isInterval(n.Inner) && !isUnknownOperand(n.Inner) {
				if id, ok := t.declaredID(n.Inner); ok && id != parquet.TypeString {
					return sqlerr.New("42846", "cannot cast type %s to interval", t.typeName(n.Inner))
				}
			}
		}
		if t.isInterval(n.Inner) && !castAdmitsInterval(n.TypeName) {
			return sqlerr.New("42846", "cannot cast type interval to %s", strings.ToLower(strings.TrimSpace(n.TypeName)))
		}
	case *plansql.UnaryOp:
		if t.isInterval(n.Inner) {
			switch n.Op {
			case "-":
			case "+":
				return sqlerr.New("42883", "operator does not exist: + interval")
			default:
				return sqlerr.New("42883", "operator does not exist: %s interval", n.Op)
			}
		}
	case *plansql.NotNode:
		if t.isInterval(n.Inner) {
			return sqlerr.New("42804", "argument of NOT must be type boolean, not type interval")
		}
	case *plansql.AndNode:
		if t.isInterval(n.Left) || t.isInterval(n.Right) {
			return sqlerr.New("42804", "argument of AND must be type boolean, not type interval")
		}
	case *plansql.OrNode:
		if t.isInterval(n.Left) || t.isInterval(n.Right) {
			return sqlerr.New("42804", "argument of OR must be type boolean, not type interval")
		}
	case *plansql.BinaryOp:
		return t.binaryOp(n)
	case *plansql.CmpExpr:
		return t.comparison(n.Op, n.Left, []plansql.Node{n.Right})
	case *plansql.InExpr:
		return t.comparison("IN", n.Left, n.Values)
	case *plansql.AnyAllExpr:
		return t.comparison(n.Op+" "+n.Modifier, n.Left, n.Values)
	case *plansql.BetweenExpr:
		return t.comparison("BETWEEN", n.Left, []plansql.Node{n.Low, n.High})
	case *plansql.LikeExpr:
		if t.isInterval(n.Left) || t.isInterval(n.Pattern) {
			return sqlerr.New("42883", "operator does not exist: interval ~~ text")
		}
	case *plansql.IsExpr:
		if t.isInterval(n.Left) && !strings.EqualFold(n.Check, "null") {
			return sqlerr.New("42804", "argument of IS %s must be type boolean, not type interval", strings.ToUpper(n.Check))
		}
	case *plansql.CaseNode:
		return t.caseNode(n)
	case *plansql.FuncCallNode:
		return t.funcCall(n)
	case *plansql.ArrayLitNode:
		for _, e := range n.Elements {
			if t.isInterval(e) {
				return intervalRefusal("an array element")
			}
		}
	case *plansql.TupleNode:
		for _, e := range n.Elements {
			if t.isInterval(e) {
				return intervalRefusal("a row element")
			}
		}
	}
	return nil
}

func (t intervalTyper) walkAll(ns []plansql.Node) error {
	for _, k := range ns {
		if err := t.walk(k); err != nil {
			return err
		}
	}
	return nil
}

// castAdmitsInterval is the target an interval may be cast to: TEXT (the
// explicit display workaround) and INTERVAL itself.
func castAdmitsInterval(typeName string) bool {
	base := strings.ToUpper(strings.TrimSpace(typeName))
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = strings.TrimSpace(base[:i])
	}
	switch base {
	case "INTERVAL", "TEXT", "VARCHAR", "CHARACTER VARYING", "STRING":
		return true
	}
	return false
}

// comparison judges a comparison's operands: an interval may only meet an
// interval, an unknown literal (resolved to interval and read now) or NULL.
// A subquery's column is never an interval (its block would have been
// refused), so an interval compared with one is refused as subquery
// membership.
func (t intervalTyper) comparison(op string, left plansql.Node, rights []plansql.Node) error {
	all := append([]plansql.Node{left}, rights...)
	sawInterval := false
	for _, k := range all {
		if t.isInterval(k) {
			sawInterval = true
			break
		}
	}
	if !sawInterval {
		return nil
	}
	for _, k := range all {
		switch {
		case t.isInterval(k):
		case isSubqueryOperand(k):
			return intervalRefusal("a comparison with a subquery's column (subquery membership)")
		case isUnknownOperand(k):
			if err := checkUnknown(k); err != nil {
				return err
			}
		case t.isTimestampDifference(k):
			return tsDiffRefusal("compared with an interval (" + strings.ToLower(op) + ")")
		default:
			l, r := "interval", t.typeName(k)
			if !t.isInterval(left) {
				l, r = t.typeName(left), "interval"
			}
			return sqlerr.New("42883", "operator does not exist: %s %s %s", l, strings.ToLower(op), r)
		}
	}
	return nil
}

func isSubqueryOperand(n plansql.Node) bool {
	switch plansql.Unparen(n).(type) {
	case *plansql.SubqueryNode, *plansql.ExistsNode:
		return true
	}
	return false
}

// binaryOp judges an arithmetic operator over an interval operand.
func (t intervalTyper) binaryOp(n *plansql.BinaryOp) error {
	li, ri := t.isInterval(n.Left), t.isInterval(n.Right)
	if !li && !ri {
		return nil
	}
	if (n.Op == "*" || n.Op == "/") && li != ri {
		// The factor must be a number (or a quoted one, read by the
		// arithmetic itself): PostgreSQL has no interval × text / boolean /
		// date (42883), whatever a row holds.
		factor := n.Right
		if ri {
			factor = n.Left
		}
		if id, ok := t.declaredID(factor); ok && !isNumericClass(id) {
			l, r := "interval", t.typeName(factor)
			if ri {
				l, r = r, "interval"
			}
			return sqlerr.New("42883", "operator does not exist: %s %s %s", l, n.Op, r)
		}
	}
	if t.isInterval(n) {
		// interval ± interval, interval × number, interval / number: an
		// inner interval, judged where it is consumed. A quoted factor or
		// addend is read by the arithmetic itself (22P02 / 22007 there).
		return nil
	}
	other, otherIsLeft := n.Right, false
	if ri {
		other, otherIsLeft = n.Left, true
	}
	if (n.Op == "+" || n.Op == "-") && t.isTimestampDifference(other) {
		return tsDiffRefusal("combined with an interval (" + n.Op + ")")
	}
	opName := func() error {
		l, r := "interval", t.typeName(other)
		if otherIsLeft {
			l, r = r, "interval"
		}
		return sqlerr.New("42883", "operator does not exist: %s %s %s", l, n.Op, r)
	}
	switch n.Op {
	case "+":
		return t.shiftOperand(other, opName, false)
	case "-":
		// x - interval shifts; interval - x exists only for an interval x.
		return t.shiftOperand(other, opName, li)
	case "||":
		return intervalRefusal("an operand of || (an implicit text conversion)")
	}
	return opName()
}

// shiftOperand admits `date/timestamp ± interval` (a TIMESTAMP), a TEXT
// column or quoted text under arc VL's date reading (a TIMESTAMP, unchanged)
// and an operand whose
// type the plan does not know (the expression refuses it at run time:
// `interval ± number` is 42883 there). A quoted operand that is no interval
// input is PostgreSQL's 22007 (it resolves the unknown to interval).
// intervalFirst: the interval is the LEFT operand of `-`, which has no
// timestamp form.
func (t intervalTyper) shiftOperand(other plansql.Node, opName func() error, intervalFirst bool) error {
	if isUnknownOperand(other) {
		// A quoted operand that is interval input is interval arithmetic
		// (classified above); any other quoted text keeps arc VL's date
		// reading, as a TEXT column below does (both base behaviour, not
		// interval-valued: PostgreSQL resolves the unknown to interval and
		// refuses 22007 / 42883 there — temporal r19's family, unchanged).
		return nil
	}
	id, ok := t.declaredID(other)
	if !ok {
		return nil
	}
	switch id {
	case parquet.TypeDate, parquet.TypeTimestamp:
		if intervalFirst {
			return opName()
		}
		return nil
	case parquet.TypeString:
		return nil
	}
	return opName()
}

// caseNode judges a CASE: a WHEN condition is boolean; a simple CASE's WHEN
// values are compared with its subject; the results are a choice.
func (t intervalTyper) caseNode(n *plansql.CaseNode) error {
	if n.Subject != nil {
		if _, quoted := quotedLiteral(n.Subject); quoted {
			// PostgreSQL types a quoted CASE subject text, and has no
			// `text = interval`.
			for _, w := range n.Whens {
				if t.isInterval(w.Cond) {
					return sqlerr.New("42883", "operator does not exist: text = interval")
				}
			}
		}
		vals := make([]plansql.Node, len(n.Whens))
		for i, w := range n.Whens {
			vals[i] = w.Cond
		}
		if err := t.comparison("=", n.Subject, vals); err != nil {
			return err
		}
	} else {
		for _, w := range n.Whens {
			if t.isInterval(w.Cond) {
				return sqlerr.New("42804", "argument of CASE/WHEN must be type boolean, not type interval")
			}
		}
	}
	results := make([]plansql.Node, 0, len(n.Whens)+1)
	if n.Else != nil {
		// PostgreSQL resolves the ELSE first (transformCaseExpr), so a mixed
		// CASE's message names it first.
		results = append(results, n.Else)
	}
	for _, w := range n.Whens {
		results = append(results, w.Result)
	}
	return t.choice("CASE", results)
}

// choice judges a choice's branches: if one is an interval, every other must
// be an interval, an unknown literal (read now) or NULL; any other type is
// PostgreSQL's 42804 `<what> types X and interval cannot be matched`.
func (t intervalTyper) choice(what string, branches []plansql.Node) error {
	saw := false
	for _, b := range branches {
		if t.isInterval(b) {
			saw = true
			break
		}
	}
	if !saw {
		return nil
	}
	for i, b := range branches {
		switch {
		case t.isInterval(b):
		case isUnknownOperand(b):
			if err := checkUnknown(b); err != nil {
				return err
			}
		case t.isTimestampDifference(b):
			return tsDiffRefusal("matched with an interval in " + what)
		default:
			first, second := t.typeName(b), "interval"
			for _, p := range branches[:i] {
				if t.isInterval(p) {
					first, second = "interval", t.typeName(b)
					break
				}
			}
			return sqlerr.New("42804", "%s types %s and %s cannot be matched", what, first, second)
		}
	}
	return nil
}

// intervalArgPositions are the allowlisted functions and the argument
// positions an interval may take there.
var intervalArgPositions = map[string]int{
	"date_add":    1, // date / timestamp shifted by an interval: TIMESTAMP
	"date_sub":    1,
	"time_bucket": 0, // the stride (calendar / opaque / non-positive refused there)
	"date_part":   1, // float8 on both
}

// funcCall judges a call's interval arguments.
func (t intervalTyper) funcCall(fc *plansql.FuncCallNode) error {
	name := strings.ToLower(strings.TrimSpace(fc.Name))
	hasInterval := false
	for _, a := range fc.Args {
		if t.isInterval(a) {
			hasInterval = true
			break
		}
	}
	switch name {
	case "coalesce", "greatest", "least":
		return t.choice(strings.ToUpper(name), fc.Args)
	case "nullif":
		if len(fc.Args) == 2 {
			return t.comparison("=", fc.Args[0], fc.Args[1:2])
		}
		return nil
	}
	if !hasInterval {
		return nil
	}
	if name == "mod" {
		return sqlerr.New("42883", "operator does not exist: interval %% %s", t.typeName(fc.Args[len(fc.Args)-1]))
	}
	if name == "ifnull" {
		// This engine's spelling of COALESCE; PostgreSQL has no ifnull.
		names := make([]string, len(fc.Args))
		for i, a := range fc.Args {
			names[i] = t.typeName(a)
		}
		return sqlerr.New("42883", "function ifnull(%s) does not exist", strings.Join(names, ", "))
	}
	if plansql.IsAggregate(name) || windowValueFunctions[name] {
		return sqlerr.New("0A000", "%s over an interval argument is not supported", name)
	}
	if pos, ok := intervalArgPositions[name]; ok {
		for i, a := range fc.Args {
			if t.isInterval(a) && i != pos {
				return sqlerr.New("0A000", "%s over an interval argument is not supported", name)
			}
		}
		return nil
	}
	label := name
	if fc.OutputLabel != "" {
		label = fc.OutputLabel
	}
	if strings.EqualFold(label, "extract") {
		return sqlerr.New("0A000", "EXTRACT over an interval argument is not supported: "+
			"date_part('<field>', <interval>) answers it")
	}
	return sqlerr.New("0A000", "%s over an interval argument is not supported", name)
}

// refuseIntervalBlock applies the rule to one query block's own expressions
// (its derived tables, CTE bodies, set-operation arms and subqueries are
// blocks of their own, each judged where the binder validates it).
func refuseIntervalBlock(info *plansql.SelectInfo, decls ColDecls, subquery func(string) (parquet.TypeID, bool)) error {
	t := intervalTyper{decls: decls, subquery: subquery}
	for i := range info.Columns {
		c := info.Columns[i]
		if c.Star || c.ASTExpr == nil {
			continue
		}
		if err := t.refuseIntervalTop(c.ASTExpr, "a query result column"); err != nil {
			return err
		}
	}
	for _, tr := range info.Tables {
		if err := t.tableFuncArgs(&tr); err != nil {
			return err
		}
	}
	for i := range info.Joins {
		if ref := joinRightRef(&info.Joins[i]); ref != nil {
			if err := t.tableFuncArgs(ref); err != nil {
				return err
			}
		}
		if err := t.refuseIntervalCondition(info.Joins[i].CondExpr, "JOIN/ON"); err != nil {
			return err
		}
	}
	if err := t.refuseIntervalCondition(info.WhereExpr, "WHERE"); err != nil {
		return err
	}
	for _, gb := range info.GroupByExprs {
		if err := t.refuseIntervalTop(gb, "a GROUP BY key"); err != nil {
			return err
		}
	}
	if err := t.refuseIntervalCondition(info.HavingExpr, "HAVING"); err != nil {
		return err
	}
	if err := t.refuseIntervalCondition(info.QualifyExpr, "QUALIFY"); err != nil {
		return err
	}
	for _, ob := range info.OrderBy {
		n := ob.Expr
		if n == nil {
			parsed, err := plansql.ParseExpression(ob.Column)
			if err != nil {
				continue
			}
			n = parsed
		}
		if err := t.refuseIntervalTop(n, "an ORDER BY key"); err != nil {
			return err
		}
	}
	return nil
}

// tableFuncArgs refuses an interval as a table function's argument
// (generate_series' step among them: it reads integers, and the folding of
// its arguments to text would erase the interval before execution).
func (t intervalTyper) tableFuncArgs(tr *plansql.TableRef) error {
	if tr == nil || !tr.IsFunction {
		return nil
	}
	for _, a := range tr.FuncArgExprs {
		if err := t.refuseIntervalTop(a, "a table function's argument ("+strings.ToLower(tr.Name)+")"); err != nil {
			return err
		}
	}
	return nil
}

// RefuseIntervalWrite is the rule for an expression a write door evaluates
// (an INSERT VALUES cell, an UPDATE / MERGE SET value, a WHERE or MERGE
// condition): its interval-valued nodes are judged by their parents as
// everywhere. The written value itself is the assignment's to judge, where
// the target column is known (IntervalWriteRefusal into TEXT, PostgreSQL's
// 42804 into anything else; wadjet's assignSource.check).
func RefuseIntervalWrite(node plansql.Node, alias string, schema []parquet.Column) error {
	if node == nil {
		return nil
	}
	scope := newColScope()
	for _, c := range schema {
		scope.addQualifiedTyped(alias, c.Name, c.Type)
		scope.addRowColumn(c)
		scope.addElementType(c)
	}
	return intervalTyper{decls: rowFieldScopeDecls(scope)}.walk(node)
}

// IntervalWriteRefusal is the rule's refusal of an interval written to a
// TEXT column: stored, it would be its text — the silent coercion the A+
// ruling forbids. PostgreSQL stores the text (an assignment cast).
func IntervalWriteRefusal() error {
	return intervalRefusal("a written value")
}
