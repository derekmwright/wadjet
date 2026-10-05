// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// LAG / LEAD's DEFAULT (#1435). PostgreSQL declares the two functions
// `lag(anycompatible, integer, anycompatible)`: the result is the COMMON type
// of the value and the default, so `LAG(x, 1, 2.5)` over a bigint is numeric
// and answers 2.5 where the default fills a row, and a default is any
// expression, evaluated at the row it fills (`LAG(x, 1, id)` answers that
// row's id).
//
// The default used to reach the operator as a float64 or as its SQL text and
// was written into a vector of the VALUE's type: 2.5 truncated to 2 in a
// bigint column, a text, column, CAST or expression default failed the write,
// a DATE column's default answered NULL. Now there is one rule, read in one
// place (lagLeadWidening) by the three readers that need it:
//
//   - the DECLARATION (windowSpecOutputType) is the common type — expr.
//     CommonDeclType, the fold CASE / COALESCE arms declare through, with
//     batch.TemporalCommonType's DATE → TIMESTAMP rung in front of it;
//   - the INPUTS (resolveWindowKeys) are the value and the default, each
//     materialized as a window key CAST to that type, so both arrive as
//     vectors of the declared type on the single-process pipeline and the
//     DAG fragment alike (the keys travel as text, like every computed key);
//   - the SPEC (windowExecColumn) names those two columns, and every
//     evaluator — the in-memory columnar one, the spilled partition runs and
//     the streaming empty-PARTITION-BY one — copies the shifted value or the
//     current row's default out of a vector of the output's own type.
//
// A pair with no common type (a text value with a numeric default, a boolean
// one with an integer) is PostgreSQL's 42883 at plan time
// (refuseLagLeadDefaultType). Where the binder cannot type the pair, no CAST
// is written and the default is materialized as it stands, so a mismatch is
// refused loudly at the write rather than coerced.

// lagLeadWidening is the default's reading for one LAG / LEAD expression.
type lagLeadWidening struct {
	// out is the result's declared type, meaningful when coerced.
	out expr.DeclType
	// coerced says the value and the default are both read as out; false
	// where the pair could not be typed or spelled.
	coerced bool
	// value is the value argument re-spelled as a CAST to out, nil when the
	// value already has that type (it is read as before).
	value plansql.Node
	// def is the default as the key the operator reads it from.
	def plansql.Node
}

// lagLeadDefaultArg is the default argument's AST, nil for a function that
// is not LAG / LEAD, an omitted default, and a NULL one (no default at all:
// the rows past the edge answer NULL either way).
func lagLeadDefaultArg(we logical.WindowExpr) plansql.Node {
	switch strings.ToLower(strings.TrimSpace(we.Func)) {
	case "lag", "lead":
	default:
		return nil
	}
	args := we.Arguments()
	if len(args) < 3 {
		return nil
	}
	ast, err := plansql.ParseExpression(args[2])
	if err != nil {
		return nil
	}
	if lit, ok := plansql.Unparen(ast).(*plansql.Lit); ok && lit.Kind == plansql.LitNull {
		return nil
	}
	return ast
}

// lagLeadValueArg is the value argument's AST: the argument's own tree where
// it still spells the argument (it carries the marks a subquery body's plan
// stamps), else the respelled text — over a GROUP BY `SUM(b)` is the
// aggregate's output column, and the tree would compute the aggregate again.
func lagLeadValueArg(we logical.WindowExpr) plansql.Node {
	text := strings.TrimSpace(we.InputColumn())
	if we.InputExpr != nil && cleanExpr(we.InputExpr.String()) == cleanExpr(text) {
		return we.InputExpr
	}
	ast, err := plansql.ParseExpression(text)
	if err != nil {
		return nil
	}
	return ast
}

func lagLeadWideningOf(node *logical.Node, we logical.WindowExpr) (lagLeadWidening, bool) {
	return newDeclWalk().lagLeadWidening(node, we)
}

// lagLeadWidening reads the default of a LAG / LEAD on node. ok is false
// when there is no default to read.
func (w *declWalk) lagLeadWidening(node *logical.Node, we logical.WindowExpr) (lagLeadWidening, bool) {
	def := lagLeadDefaultArg(we)
	if def == nil || node == nil || len(node.Children) != 1 {
		return lagLeadWidening{}, false
	}
	val := lagLeadValueArg(we)
	if val == nil {
		return lagLeadWidening{}, false
	}
	v, vok := w.windowValueDecl(node, we)
	decls := withSubqueryDecls(w.emittedColDecls(node.Children[0]), node)
	d, dc := lagLeadDefaultDecl(def, decls, v, vok)
	out := v
	cast := vok && dc == expr.Decided && !d.Untyped && lagLeadCompatible(v, d)
	if cast {
		if t, ok := batch.TemporalCommonType(v.ID, d.ID); ok {
			out = expr.Decl(t)
		} else if c, ok := expr.CommonDeclType([]expr.DeclType{v, d}, false); ok {
			out = c
		}
	}
	spelled, spellable := castSpelling(out)
	r := lagLeadWidening{out: out}
	if !cast || !spellable {
		// Nothing to coerce TO: the default is materialized as written and
		// the operator's write refuses a value of another type.
		r.def = &plansql.ParenNode{Inner: def}
		return r, true
	}
	r.coerced = true
	r.def = &plansql.CastNode{Inner: def, TypeName: spelled}
	if !sameDecl(v, out) {
		r.value = &plansql.CastNode{Inner: val, TypeName: spelled}
	}
	return r, true
}

// windowValueDecl is the value argument's own DECIDED declaration: a column
// the window's input declares, or a computed argument typed from its AST.
func (w *declWalk) windowValueDecl(node *logical.Node, we logical.WindowExpr) (expr.DeclType, bool) {
	col := cleanExpr(we.InputColumn())
	if col == "" {
		return expr.DeclType{}, false
	}
	if t, conf := colRefDeclaredType(&plansql.ColRef{Column: col}, w.emittedColDecls(node.Children[0])); conf == expr.Decided {
		return t, true
	}
	if d, _, ok := w.windowComputedArgDecl(node, we); ok {
		return d, true
	}
	// A COMPUTED value beside an offset and a default: InputCol carries the
	// whole argument list (`b * 2, 1, d`), which windowComputedArgDecl's
	// whole-argument test declines, so the first argument's own tree is
	// typed. Undecided, nothing widened: `LAG(b * 2, 1, d)` answered bigint
	// 1 where PostgreSQL answers double 1.5, and a REAL value's 2.5 default
	// failed the write (#1435).
	if we.InputExpr != nil && node != nil && len(node.Children) > 0 && cleanExpr(we.InputExpr.String()) == col {
		if d, _, ok := w.windowArgExprDecl(node, we.InputExpr); ok {
			return d, true
		}
	}
	return expr.DeclType{}, false
}

// lagLeadCompatible says the value and the default have a common type in
// PostgreSQL: a quoted literal is `unknown` and takes the value's type; two
// typed operands must be of one class (numeric with numeric, DATE with
// TIMESTAMP, text with text, …). An array pair must agree through
// expr.CommonDeclType's container arm.
func lagLeadCompatible(v, d expr.DeclType) bool {
	if d.Quoted {
		return true
	}
	cv, cd := comparisonClass(v.ID), comparisonClass(d.ID)
	if cv == cmpUnknown || cv != cd {
		return false
	}
	switch cv {
	case cmpNumber:
		// PORT / PROTOCOL / DURATION share the comparison class and are no
		// numeric PostgreSQL has; only the numeric rungs fold.
		return lagLeadNumeric(v.ID) && lagLeadNumeric(d.ID)
	case cmpArray, cmpRow, cmpMap, cmpVector, cmpInet:
		return v.ID == d.ID
	}
	return true
}

func lagLeadNumeric(t batch.TypeID) bool {
	switch t {
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64, batch.TypeDecimal:
		return true
	}
	return false
}

// sameDecl says a value of declaration a is already a value of b.
func sameDecl(a, b expr.DeclType) bool {
	if a.ID != b.ID {
		return false
	}
	if a.ID == batch.TypeDecimal {
		return a.DecKnown && b.DecKnown && a.Precision == b.Precision && a.Scale == b.Scale
	}
	return true
}

// castSpelling is the CAST type name that materializes a declaration
// exactly; false for one it cannot spell (a container, an address type, a
// DECIMAL with no (p,s)), which is then not coerced.
func castSpelling(d expr.DeclType) (string, bool) {
	switch d.ID {
	case batch.TypeBool:
		return "BOOLEAN", true
	case batch.TypeInt32:
		return "INTEGER", true
	case batch.TypeInt64:
		return "BIGINT", true
	case batch.TypeFloat32:
		return "REAL", true
	case batch.TypeFloat64:
		return "DOUBLE", true
	case batch.TypeString:
		return "TEXT", true
	case batch.TypeDate:
		return "DATE", true
	case batch.TypeTimestamp:
		return "TIMESTAMP", true
	case batch.TypeDecimal:
		if !d.DecKnown || d.Precision <= 0 {
			return "", false
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", d.Precision, d.Scale), true
	}
	return "", false
}

// refuseLagLeadDefaultType raises PostgreSQL's 42883 for a LAG / LEAD whose
// value and default have no common type — `LAG(s, 1, 2.5)` over a text column,
// `LAG(b, 1, 7)` over a boolean: `function lag(text, integer, numeric) does
// not exist`, measured on 17.11. An operand the scope cannot type decides
// nothing.
func refuseLagLeadDefaultType(node plansql.Node, decls ColDecls) error {
	switch n := node.(type) {
	case nil, *plansql.SubqueryNode, *plansql.ExistsNode:
		return nil
	case *plansql.WindowFuncNode:
		if n.Func == nil {
			return nil
		}
		return lagLeadCallTypes(n.Func, decls)
	}
	for _, child := range exprOperands(node) {
		if err := refuseLagLeadDefaultType(child, decls); err != nil {
			return err
		}
	}
	return nil
}

func lagLeadCallTypes(fc *plansql.FuncCallNode, decls ColDecls) error {
	name := strings.ToLower(fc.Name)
	if (name != "lag" && name != "lead") || len(fc.Args) < 3 {
		return nil
	}
	if lit, ok := plansql.Unparen(fc.Args[2]).(*plansql.Lit); ok && lit.Kind == plansql.LitNull {
		return nil
	}
	v, vc := nodeDeclaredType(fc.Args[0], decls)
	d, dc := lagLeadDefaultDecl(fc.Args[2], decls, v, vc == expr.Decided)
	if vc != expr.Decided || dc != expr.Decided || v.Untyped || d.Untyped || v.Quoted || d.Quoted {
		return nil
	}
	if lagLeadCompatible(v, d) {
		return nil
	}
	if comparisonClass(v.ID) == cmpUnknown || comparisonClass(d.ID) == cmpUnknown {
		return nil
	}
	return sqlerr.New("42883", "function %s(%s, integer, %s) does not exist", name,
		lagLeadTypeName(v), lagLeadTypeName(d))
}

func lagLeadTypeName(d expr.DeclType) string {
	if d.ID == batch.TypeArray && d.Schema != nil && d.Schema.ElementType != nil {
		return pgTypeName(d.Schema.ElementType.Type) + "[]"
	}
	return pgTypeName(d.ID)
}

// lagLeadDefaultDecl is the declaration a LAG / LEAD default contributes to
// the common type. A NULL literal under a CAST (`CAST(NULL AS NUMERIC)`,
// `NULL::numeric`, and an expression whose every leaf is one, such as
// `-CAST(NULL AS NUMERIC)` or `COALESCE(CAST(NULL AS NUMERIC), NULL)`)
// contributes its type FAMILY and no width: it carries no
// value, so it names the type PostgreSQL resolves `lag(anycompatible,
// integer, anycompatible)` over and nothing about digits. For a numeric
// cast that is the exact family at no scale of its own — over a bigint
// value the result is DECIMAL(38,0), so `LAG(b * 1000000000000000 + 1, 1,
// CAST(NULL AS NUMERIC))` keeps 10000000000000001; over a DECIMAL(10,2)
// value it is that value's own type; over a double, double precision.
// Outside this seam a bare NUMERIC cast of the NULL literal keeps its
// planner-wide declaration (ADR-0024).
func lagLeadDefaultDecl(def plansql.Node, decls ColDecls, v expr.DeclType, vok bool) (expr.DeclType, expr.Confidence) {
	if nullOnlyTree(def) && castsToDecimal(def) {
		if vok && v.ID == batch.TypeDecimal && v.Precision > 0 {
			return v, expr.Decided // the family is the value's own
		}
		return expr.DeclDecimal(batch.MaxDecimalPrecision, 0), expr.Decided
	}
	return nodeDeclaredType(def, decls)
}

// typedNullNumeric reports a SELECT item that is PostgreSQL's plain numeric
// NULL and nothing else: `CAST(NULL AS NUMERIC)`, `NULL::numeric`, an
// expression whose every leaf is the NULL literal and whose cast names the
// numeric family, or a CASE whose every result is one of those or NULL. The
// planner declares it double precision (the float rung); a table created from
// it is PostgreSQL's unconstrained numeric column, and it carries no value
// whose digits a fixed scale could drop (ADR-0024 §10).
func typedNullNumeric(n plansql.Node) bool {
	if c, ok := plansql.Unparen(n).(*plansql.CaseNode); ok {
		typed := false
		arms := make([]plansql.Node, 0, len(c.Whens)+1)
		for _, w := range c.Whens {
			arms = append(arms, w.Result)
		}
		if c.Else != nil {
			arms = append(arms, c.Else)
		}
		for _, a := range arms {
			if l, ok := plansql.Unparen(a).(*plansql.Lit); ok && l.Kind == plansql.LitNull {
				continue
			}
			if !typedNullNumeric(a) {
				return false
			}
			typed = true
		}
		return typed
	}
	return n != nil && nullOnlyTree(n) && castsToDecimal(n)
}

// nullOnlyTree reports whether every leaf of an expression is the NULL
// literal, so the expression has no value of its own.
func nullOnlyTree(n plansql.Node) bool {
	switch e := plansql.Unparen(n).(type) {
	case *plansql.Lit:
		return e.Kind == plansql.LitNull
	case *plansql.CastNode:
		return nullOnlyTree(e.Inner)
	case *plansql.ColRef, *plansql.SubqueryNode, *plansql.ExistsNode, nil:
		return false
	}
	ops := exprOperands(n)
	if len(ops) == 0 {
		return false
	}
	for _, o := range ops {
		if !nullOnlyTree(o) {
			return false
		}
	}
	return true
}

// castsToDecimal reports whether a NULL-only expression is of the NUMERIC
// family: its outermost CAST, or one of its operands', names NUMERIC /
// DECIMAL.
func castsToDecimal(n plansql.Node) bool {
	if c, ok := plansql.Unparen(n).(*plansql.CastNode); ok {
		// The outermost CAST names the family.
		_, _, _, isDec := expr.DecimalCastDest(c.TypeName)
		return isDec
	}
	for _, o := range exprOperands(n) {
		if castsToDecimal(o) {
			return true
		}
	}
	return false
}
