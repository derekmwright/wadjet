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
// A timestamp difference (`timestamp - timestamp`, both operands TIMESTAMP)
// is a PRODUCER too (#1713d round 6, the maintainer's ruling):
// PostgreSQL types it interval, and this engine computes it as a double of
// milliseconds (ADR-0012 temporal r16). It is classified interval-valued like
// any other producer, so publication, keys, aggregates, function arguments,
// casts and PostgreSQL's own 42883 / 42804 for a number beside an interval
// apply to it unchanged; and because its value here is that millisecond
// number, not an interval, every allowlisted operation that would consume it
// (a comparison, a shift, a cast to text, an allowlisted function) refuses it
// instead (carriesTsDiff, tsDiffRefusal). Only IS [NOT] NULL takes it, where
// the number and the interval are NULL on the same rows.
//
// What this rule SEES: a difference whose operands it can type TIMESTAMP
// from the statement — a column, a CAST, a temporal expression, a
// TIMESTAMP-valued aggregate or window function (withTimestampLeaves: min /
// max / min_by / max_by / lag / lead / first_value / last_value /
// nth_value, lag / lead over a NULL value typed by their default), a scalar
// subquery whose body answers one (binder.timestampOut: a plain body, a star
// body over one column, an outer column the body publishes), any expression
// over those, and on the DML WHERE / MERGE doors a scalar subquery the
// planner declares TIMESTAMP (RefuseIntervalWriteWith). Where it sees one it
// gives PostgreSQL's diagnostics.
//
// What it does NOT see: any operand whose type only the plan knows and this
// walk does not decide (the walk is AST typing, not the plan's), and the
// UPDATE SET / INSERT VALUES doors' subqueries (no planner there). Round 6
// and round 7 each claimed the rule left no such operand; reviews found
// aggregates, windows, star bodies, outer columns and the write doors'
// subqueries answering the millisecond number (1713d_r7.md, 1713d_r8.md).
//
// So this rule does NOT make "no statement observes the number" hold on its
// own. The expression GUARD does (expr/timestamp_difference_guard.go): the
// minus operator refuses `timestamp - timestamp` by its operands' declared
// producers wherever it is compiled or evaluated, so an operand this walk
// misses still yields no number — the guard's own 0A000 instead of this
// rule's diagnostic. The guard is decided when the minus meets a row's batch;
// a statement that evaluates no row (CTAS WITH NO DATA) is refused by this
// rule alone.
//
// What this rule cannot see — the ROW's text under CAST(s AS INTERVAL), which
// may be input this engine cannot hold (an opaque IntervalValue) — is guarded
// where it is applied, compared or rendered, 0A000 at run time
// (expr.compareIntervals, addInterval, time_bucket, the text cast).

// intervalRefusal is the 0A000 for an interval used where this engine cannot
// yet take one.
func intervalRefusal(use string) error {
	return sqlerr.Refuse(useReason(use, "interval-use"), "interval values cannot yet be used as %s: compare or apply an interval "+
		"inline, cast it to text explicitly for display (CAST(... AS TEXT)), or use DURATION for a "+
		"stored nanosecond duration", use)
}

// tsDiffRefusal is the 0A000 for a timestamp difference used where
// PostgreSQL would use an interval and this engine cannot yet: the
// difference is computed here as a number of milliseconds (ADR-0012 temporal
// r16), never an interval, so no use of it may answer. Both workarounds were
// measured against PostgreSQL 17.11 (1713d_r3.md, 1713d_r6.md).
func tsDiffRefusal(use string) error {
	return sqlerr.Refuse(useReason(use, "timestamp-difference-to-number"), "a timestamp difference (timestamp - timestamp) is an interval and "+
		"cannot yet be used as %s: compare the timestamps themselves instead, for example "+
		"ts > TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '30 days' or a.ts > b.ts + INTERVAL '30 days', "+
		"or take a number of seconds as extract(epoch FROM ts2) - extract(epoch FROM ts1)", use)
}

// useReason is the stable identity (sqlerr.Refuse) of the refusal of an
// interval used as use: what is refused is the USE, whichever producer
// (an interval, or a timestamp difference) reaches it; fallback names every
// other use. The identities are the ones the interval corpus's manifest
// credits (internal/coordinator/arc_1680b_interval_manifest_rules_test.go,
// iv80Reasons), read there by identity and never by the sentence's words.
func useReason(use, fallback string) string {
	switch {
	case use == "a query result column":
		return "bare-interval-result"
	case use == "a written value":
		return "interval-write"
	case use == "an array element" || use == "a row element":
		return "container"
	case strings.HasPrefix(use, "an operand of || "):
		return "implicit-text-conversion"
	case strings.HasSuffix(use, " key"):
		return "interval-key"
	case use == "a table function's argument (generate_series)":
		return "function-generate_series"
	case strings.HasPrefix(use, "a table function's argument"):
		return "table-function-argument"
	case strings.Contains(use, "subquery's column"):
		return "subquery-comparison"
	}
	return fallback
}

// refusal is the 0A000 for an interval-valued n used as use: the timestamp
// difference's own wording when n's value derives from one.
func (t intervalTyper) refusal(n plansql.Node, use string) error {
	if t.carriesTsDiff(n) {
		return tsDiffRefusal(use)
	}
	return intervalRefusal(use)
}

// carriesTsDiff reports an interval-valued n whose value derives from a
// timestamp difference: the difference itself, or an interval construct
// (arithmetic, CASE, COALESCE, NULLIF, GREATEST, LEAST, a CAST to INTERVAL)
// over one. Such a value is a millisecond number at run time.
func (t intervalTyper) carriesTsDiff(n plansql.Node) bool {
	if !t.isInterval(n) {
		return false
	}
	if t.isTimestampDifference(n) {
		return true
	}
	for _, k := range exprOperands(n) {
		if t.carriesTsDiff(k) {
			return true
		}
	}
	return false
}

// anyCarriesTsDiff is carriesTsDiff over several operands.
func (t intervalTyper) anyCarriesTsDiff(ns []plansql.Node) bool {
	for _, k := range ns {
		if t.carriesTsDiff(k) {
			return true
		}
	}
	return false
}

// isTimestampDifference reports `timestamp - timestamp` (r16's millisecond
// double), parentheses aside.
func (t intervalTyper) isTimestampDifference(n plansql.Node) bool {
	return timestampDifference(n, t.isTimestampOperand)
}

// timestampDifference reports a `-` PostgreSQL types interval as the
// difference of two timestamps (timestamp_mi; with or without time zone,
// one engine type here): both operands declared TIMESTAMP (isTs), or one
// declared TIMESTAMP beside NULL or a quoted literal, which PostgreSQL
// resolves to timestamp (`ts - '2024-01-01'`, `'2024-01-03' - ts`, `ts -
// NULL`; measured, 1713d_r6.md). A quoted literal that is interval input
// (`ts - '1 day'`) is no difference: the date arms refuse it 22007, as
// PostgreSQL does. `date - date` is an integer on both sides and is not here.
func timestampDifference(n plansql.Node, isTs func(plansql.Node) bool) bool {
	if expr.TimestampDifferenceStatementRuleDisabled() {
		return false
	}
	b, ok := plansql.Unparen(n).(*plansql.BinaryOp)
	if !ok || b.Op != "-" {
		return false
	}
	unknown := func(k plansql.Node) bool {
		return isUnknownOperand(k) && !expr.IsQuotedIntervalText(k)
	}
	l, r := isTs(b.Left), isTs(b.Right)
	return (l && r) || (l && unknown(b.Right)) || (r && unknown(b.Left))
}

// The statement rule's timestamp-difference refusals can be turned off by a
// TEST seam, expr.DisableTimestampDifferenceStatementRuleForTest: with it set,
// no `-` is a timestamp difference to this rule, so every such statement
// reaches execution and the expression guard
// (expr/timestamp_difference_guard.go) alone must refuse it — the proof that
// the guard is complete. The seam lives in expr, beside the guard it proves,
// so that the AGPL packages' tests reach it without widening what they see of
// this package (tools/licensecheck holds that surface to a budget).

// timestampValueArg is, for each aggregate and window function that answers
// one of its arguments' own values, that argument's position: over a
// TIMESTAMP argument it answers a TIMESTAMP (PostgreSQL's min / max and
// window value functions are polymorphic in it; min_by / max_by answer their
// first argument here). The declared-type walk cannot type them from the
// statement's AST (an aggregate's output is typed by the logical plan), so
// before #1713d round 7 a difference over one — max(ts2) - min(ts),
// ts2 - lag(ts2) OVER (…) — was no difference to this rule and answered the
// millisecond number.
var timestampValueArg = map[string]int{
	"min": 0, "max": 0, "min_by": 0, "max_by": 0,
	"lag": 0, "lead": 0, "first_value": 0, "last_value": 0, "nth_value": 0,
}

// withTimestampLeaves is d with the INTERVAL rule's TIMESTAMP leaves
// installed (ColDecls.timestampLeaf): a TIMESTAMP-valued aggregate or window
// function, and a scalar subquery the subquery callback reports TIMESTAMP
// (nil: the walk's own SubqueryNode arm decides). Every node over them — a CASE, COALESCE,
// date_trunc, `+ interval`, a CAST — is then typed by the walk as it types
// a column.
func withTimestampLeaves(d ColDecls, subquery func(*plansql.SubqueryNode) bool) ColDecls {
	d.timestampLeaf = func(n plansql.Node) bool {
		switch v := n.(type) {
		case *plansql.WindowFuncNode:
			return v.Func != nil && d.timestampLeaf(v.Func)
		case *plansql.FuncCallNode:
			pos, ok := timestampValueArg[strings.ToLower(strings.TrimSpace(v.Name))]
			if !ok || v.Star || pos >= len(v.Args) {
				return false
			}
			// lag / lead over a NULL value: PostgreSQL resolves the unknown
			// value to the DEFAULT's type, so `lag(NULL, 1, ts)` answers a
			// TIMESTAMP (#1713d round 8).
			if name := strings.ToLower(strings.TrimSpace(v.Name)); (name == "lag" || name == "lead") &&
				len(v.Args) >= 3 && isNullLitNode(v.Args[pos]) {
				pos = 2
			}
			t, c := nodeDeclaredType(v.Args[pos], d)
			return c == expr.Decided && !t.Untyped && t.ID == parquet.TypeTimestamp
		case *plansql.SubqueryNode:
			return subquery != nil && !v.Array && subquery(v)
		}
		return false
	}
	return d
}

// isNullLitNode reports the NULL literal, parentheses aside.
func isNullLitNode(n plansql.Node) bool {
	l, ok := plansql.Unparen(n).(*plansql.Lit)
	return ok && l.Kind == plansql.LitNull
}

// intervalTyper carries the declarations an operand's type is read from.
type intervalTyper struct {
	decls ColDecls
	// tsDecls is decls with the TIMESTAMP leaves (withTimestampLeaves),
	// read by the timestamp-difference producer alone (isTimestampOperand).
	tsDecls ColDecls
	// subquery types a scalar subquery's one output column (the binder's
	// declaration of its body), nil where no binder is at hand.
	subquery func(sql string) (parquet.TypeID, bool)
	// ivSubquery reports a scalar subquery whose one output slot is declared
	// interval by lineage (binder.intervalOut), nil where no binder is at
	// hand (validate_interval_lineage.go).
	ivSubquery func(*plansql.SubqueryNode) bool
	// findings, when set, collects every refusal instead of returning the
	// first (chooseInterval decides among them); top says this block's
	// result columns are the statement's own result.
	findings *[]intervalFinding
	top      bool
}

// collect records err when the typer collects; it reports whether err was
// taken (the caller then carries on).
func (t intervalTyper) collect(err error, topResult bool) bool {
	if t.findings == nil || err == nil {
		return false
	}
	*t.findings = append(*t.findings, newIntervalFinding(err, topResult))
	return true
}

// newIntervalTyper is the typer over decls, its TIMESTAMP leaves installed
// for the producer (withTimestampLeaves): a scalar subquery is a TIMESTAMP
// leaf when tsSubquery says its body answers one (nil: none is).
func newIntervalTyper(decls ColDecls, subquery func(sql string) (parquet.TypeID, bool), tsSubquery func(sql string) bool) intervalTyper {
	var leaf func(*plansql.SubqueryNode) bool
	if tsSubquery != nil {
		leaf = func(v *plansql.SubqueryNode) bool { return tsSubquery(v.SQL) }
	}
	return intervalTyper{decls: decls, tsDecls: withTimestampLeaves(decls, leaf), subquery: subquery}
}

// isTimestampOperand reports an operand the producer reads as a TIMESTAMP:
// declaredID's answer, or the TIMESTAMP leaves' (an aggregate, a window
// function, a scalar subquery, an expression over them). Only the producer
// reads the leaves, so no other judgment's operand type (a 42846 / 42883
// message, a shift's operand) moves with them.
func (t intervalTyper) isTimestampOperand(k plansql.Node) bool {
	if _, lit := plansql.Unparen(k).(*plansql.Lit); lit {
		return false
	}
	if d, c := nodeDeclaredType(k, t.tsDecls); c == expr.Decided && !d.Untyped && d.ID == parquet.TypeTimestamp {
		return true
	}
	id, ok := t.declaredID(k)
	return ok && id == parquet.TypeTimestamp
}

// isInterval is the interval rule's typing: the provenance rule, with the
// producers this package declares (a timestamp difference, a slot declared
// interval by lineage, a scalar subquery whose slot is, an aggregate or
// window value function over an interval, a function PostgreSQL declares
// interval, a difference of a date and a timestamp).
func (t intervalTyper) isInterval(n plansql.Node) bool {
	return n != nil && expr.IsIntervalNodeWith(n, t.producer)
}

// producer reports a node this package declares interval-valued where the
// expression package's provenance rule cannot (stage A's lineage, §9.1).
func (t intervalTyper) producer(n plansql.Node) bool {
	if t.isTimestampDifference(n) || t.decls.isLineageInterval(n) {
		return true
	}
	switch v := plansql.Unparen(n).(type) {
	case *plansql.SubqueryNode:
		return t.ivSubquery != nil && !v.Array && t.ivSubquery(v)
	case *plansql.WindowFuncNode:
		return v.Func != nil && t.producer(v.Func)
	case *plansql.FuncCallNode:
		name := strings.ToLower(strings.TrimSpace(v.Name))
		if intervalReturningFunctions[name] {
			return true
		}
		if pos, ok := intervalValueArg[name]; ok && !v.Star && pos < len(v.Args) {
			return t.isInterval(v.Args[pos])
		}
	case *plansql.BinaryOp:
		// date - timestamp and timestamp - date: PostgreSQL's interval
		// (the date widened to a timestamp); this engine has no such
		// operator (42883 there), and the result is an interval wherever
		// it is consumed.
		if v.Op == "-" && !expr.TimestampDifferenceStatementRuleDisabled() {
			l, lok := t.declaredID(v.Left)
			r, rok := t.declaredID(v.Right)
			if lok && rok && ((l == parquet.TypeDate && r == parquet.TypeTimestamp) || (l == parquet.TypeTimestamp && r == parquet.TypeDate)) {
				return true
			}
		}
	}
	return false
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
		// A capability refusal the fold raises (0A000: a unit, a
		// sub-second or infinite result this engine cannot hold) does not
		// precede a refusal this program holds permanently (#1680c): the
		// use is refused whatever the value.
		topResult := t.top && use == "a query result column"
		refusal := t.refusal(n, use)
		if err := expr.ConstantEvalError(n); err != nil &&
			!(sqlerr.StateOf(err) == "0A000" && t.findings != nil && newIntervalFinding(refusal, topResult).permanent && literalsArePGInput(n)) {
			if t.collect(err, false) {
				return nil
			}
			return err
		}
		if t.collect(refusal, topResult) {
			return nil
		}
		return refusal
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
		err := sqlerr.New("42804", "argument of %s must be type boolean, not type interval", clause)
		if t.collect(err, false) {
			return nil
		}
		return err
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
	if err := t.judge(node); err != nil && !t.collect(err, false) {
		return err
	}
	return nil
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
		if t.carriesTsDiff(n.Inner) {
			// CAST(<difference> AS TEXT) would print the millisecond number.
			return tsDiffRefusal("the operand of a cast to " + strings.ToLower(strings.TrimSpace(n.TypeName)))
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
				return t.refusal(e, "an array element")
			}
		}
	case *plansql.TupleNode:
		for _, e := range n.Elements {
			if t.isInterval(e) {
				return t.refusal(e, "a row element")
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
			if t.anyCarriesTsDiff(all) {
				return tsDiffRefusal("an operand compared with a subquery's column (subquery membership)")
			}
			return intervalRefusal("a comparison with a subquery's column (subquery membership)")
		case isUnknownOperand(k):
			if err := checkUnknown(k); err != nil {
				return err
			}
		default:
			l, r := "interval", t.typeName(k)
			if !t.isInterval(left) {
				l, r = t.typeName(left), "interval"
			}
			return sqlerr.New("42883", "operator does not exist: %s %s %s", l, strings.ToLower(op), r)
		}
	}
	if t.anyCarriesTsDiff(all) {
		// Every operand is an interval to PostgreSQL, which answers; here the
		// difference is a millisecond number and the comparison would compare
		// that number with an interval.
		return tsDiffRefusal("an operand of a comparison (" + strings.ToLower(op) + ")")
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
	opName := func() error {
		l, r := "interval", t.typeName(other)
		if otherIsLeft {
			l, r = r, "interval"
		}
		return sqlerr.New("42883", "operator does not exist: %s %s %s", l, n.Op, r)
	}
	ivOperand := n.Left
	if ri {
		ivOperand = n.Right
	}
	var err error
	switch n.Op {
	case "+":
		err = t.shiftOperand(other, opName, false)
	case "-":
		// x - interval shifts; interval - x exists only for an interval x.
		err = t.shiftOperand(other, opName, li)
	case "||":
		return t.refusal(ivOperand, "an operand of || (an implicit text conversion)")
	default:
		return opName()
	}
	if err != nil || !t.carriesTsDiff(ivOperand) {
		return err
	}
	// The interval operand is a timestamp difference (a millisecond number
	// here). A number beside it is PostgreSQL's 42883 (no interval ± number),
	// which shiftOperand admits for the run time to refuse — where the
	// difference, a number, would be added instead. Any other operand it
	// admits (a date or timestamp shifted by the difference) would shift by
	// that number.
	if lit, ok := plansql.Unparen(other).(*plansql.Lit); ok && lit.Kind == plansql.LitNumber {
		return opName()
	}
	if id, ok := t.declaredID(other); ok && isNumericClass(id) {
		return opName()
	}
	return tsDiffRefusal("an operand of " + n.Op + " beside " + t.typeName(other))
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

// intervalFunctionReasons are the functions over an interval with an item of
// their own (the definition record, docs/design/interval-execution-type.md),
// each refused under its own identity.
var intervalFunctionReasons = map[string]string{
	"date_trunc":      "function-date_trunc",
	"to_char":         "function-to_char",
	"generate_series": "function-generate_series",
	"width_bucket":    "function-width_bucket",
	// PostgreSQL's internal hash support functions.
	"interval_hash":          "postgresql-internal-function",
	"interval_hash_extended": "postgresql-internal-function",
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
	label := name
	if fc.OutputLabel != "" {
		label = fc.OutputLabel
	}
	if t.anyCarriesTsDiff(fc.Args) {
		// EXTRACT / date_part of the difference would read the millisecond
		// number (epoch in milliseconds, not PostgreSQL's seconds); an
		// aggregate would sum or order it.
		return tsDiffRefusal("an argument of " + label)
	}
	if (name == "sum" || name == "avg") && fc.Distinct {
		// Refused in this program (ADR-0052 §3): sum / avg over DISTINCT
		// intervals.
		return sqlerr.Refuse("distinct-aggregate", "%s(DISTINCT interval) is not supported", name)
	}
	if plansql.IsAggregate(name) {
		return sqlerr.Refuse("interval-aggregate", "%s over an interval argument is not supported", name)
	}
	if windowValueFunctions[name] {
		return sqlerr.Refuse("interval-window-function", "%s over an interval argument is not supported", name)
	}
	if pos, ok := intervalArgPositions[name]; ok {
		for i, a := range fc.Args {
			if t.isInterval(a) && i != pos {
				return sqlerr.Refuse("interval-function-argument", "%s over an interval argument is not supported", name)
			}
		}
		return nil
	}
	if strings.EqualFold(label, "extract") {
		return sqlerr.Refuse("interval-extract", "EXTRACT over an interval argument is not supported: "+
			"date_part('<field>', <interval>) answers it")
	}
	if reason, ok := intervalFunctionReasons[name]; ok {
		return sqlerr.Refuse(reason, "%s over an interval argument is not supported", name)
	}
	return sqlerr.Refuse("interval-function-argument", "%s over an interval argument is not supported", name)
}

// refuseIntervalBlock applies the rule to one query block's own expressions
// (its derived tables, CTE bodies, set-operation arms and subqueries are
// blocks of their own, each judged where the binder validates it).
func refuseIntervalBlock(info *plansql.SelectInfo, decls ColDecls, subquery func(string) (parquet.TypeID, bool), tsSubquery func(string) bool) error {
	return chooseInterval(intervalBlockFindings(info, decls, subquery, tsSubquery, nil, true))
}

// intervalBlockFindings is every refusal of the rule over one block's own
// expressions, in the order the rule meets them; ivSubquery types a scalar
// subquery's slot by lineage, top says the block's result is the statement's.
func intervalBlockFindings(info *plansql.SelectInfo, decls ColDecls, subquery func(string) (parquet.TypeID, bool), tsSubquery func(string) bool, ivSubquery func(*plansql.SubqueryNode) bool, top bool) []intervalFinding {
	var fs []intervalFinding
	t := newIntervalTyper(decls, subquery, tsSubquery)
	t.ivSubquery = ivSubquery
	t.findings = &fs
	t.top = top
	_ = t.blockClauses(info)
	return fs
}

// blockClauses walks one block's clauses (refuseIntervalBlock).
func (t intervalTyper) blockClauses(info *plansql.SelectInfo) error {
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
	return RefuseIntervalWriteWith(node, alias, schema, nil)
}

// RefuseIntervalWriteWith is RefuseIntervalWrite with a scalar subquery's
// TIMESTAMP answer known to the timestamp-difference producer: tsSubquery
// (Planner.TimestampSubquery; nil: no subquery is one) reports a body that
// answers a TIMESTAMP, so `ts2 - (SELECT min(ts) FROM t) > 1000` in a DML
// WHERE or a MERGE condition gets PostgreSQL's 42883 at plan time rather
// than the expression guard's 0A000 at run time (#1713d round 8). Only the
// producer reads it, as on the SELECT door.
func RefuseIntervalWriteWith(node plansql.Node, alias string, schema []parquet.Column, tsSubquery func(sql string) bool) error {
	if node == nil {
		return nil
	}
	scope := newColScope()
	for _, c := range schema {
		scope.addQualifiedTyped(alias, c.Name, c.Type)
		scope.addRowColumn(c)
		scope.addElementType(c)
	}
	return newIntervalTyper(rowFieldScopeDecls(scope), nil, tsSubquery).walk(node)
}

// TimestampSubquery reports a scalar subquery whose planned single output
// column is a TIMESTAMP — the write doors' tsSubquery (RefuseIntervalWriteWith).
func (p *Planner) TimestampSubquery(sql string) bool {
	if p == nil {
		return false
	}
	col, ok := p.SubqueryOutputColumn(sql)
	return ok && col.Type == parquet.TypeTimestamp
}

// IntervalWriteRefusal is the rule's refusal of an interval written to a
// TEXT column: stored, it would be its text — the silent coercion the A+
// ruling forbids. PostgreSQL stores the text (an assignment cast).
func IntervalWriteRefusal() error {
	return intervalRefusal("a written value")
}

// IsIntervalValuedWith is the provenance rule for a write door's source
// expression, its columns declared by schema as DeclaredTypeOfNodeWith reads
// them: an interval producer, a timestamp difference among them.
func IsIntervalValuedWith(node plansql.Node, schema []parquet.Column, cat []expr.PGCategory, p *Planner) bool {
	// A scalar subquery is typed by the walk's own arm through p's plan (the
	// declarations DeclaredTypeOfNodeWith reads); an aggregate or window
	// function by the TIMESTAMP leaves, as the SELECT rule types them.
	decls := withTimestampLeaves(schemaDecls(schema, cat, p), nil)
	isTs := func(k plansql.Node) bool {
		if _, lit := plansql.Unparen(k).(*plansql.Lit); lit {
			return false
		}
		d, c := nodeDeclaredType(k, decls)
		return c == expr.Decided && !d.Untyped && d.ID == parquet.TypeTimestamp
	}
	return node != nil && expr.IsIntervalNodeWith(node, func(k plansql.Node) bool {
		return timestampDifference(k, isTs)
	})
}

// IsTimestampDifferenceWith reports a write door's source expression whose
// interval value derives from a timestamp difference (a millisecond number
// here): its refusal names the difference.
func IsTimestampDifferenceWith(node plansql.Node, schema []parquet.Column, cat []expr.PGCategory, p *Planner) bool {
	return IsIntervalValuedWith(node, schema, cat, p) && !expr.IsIntervalNode(node)
}

// TsDiffWriteRefusal is the 0A000 for a timestamp difference written to a
// TEXT column (stored, it would be the millisecond number's text).
func TsDiffWriteRefusal() error {
	return tsDiffRefusal("a written value")
}
