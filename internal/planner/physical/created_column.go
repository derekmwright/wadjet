// SPDX-License-Identifier: MIT

// This file holds the declaration of a column CREATE TABLE AS creates,
// governed by ADR-0024 §10.
package physical

import (
	"math/big"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// CreatedColumns is what each column a CREATE TABLE AS creates from plan
// declares: declared (the plan's output declaration, positionally) with
// Unconstrained set on every column PostgreSQL creates as plain numeric
// (ADR-0024 §10). It is the one answer both arms of the statement read, and
// the storage declaration follows it (ingest.TableSchemaForQuery): a marked
// column is DECIMAL(38, max(s,10)), an unmarked one keeps its (p,s).
//
// PostgreSQL's rule is the typmod of the target expression (exprTypmod): a
// column reference keeps its column's, through a derived table, a CTE, a
// scalar subquery and a set operation whose arms all carry the same one; a
// CASE, COALESCE, GREATEST and LEAST keep one only when every value they
// choose from carries it (a NULL or a missing ELSE carries none); NULLIF
// keeps its first argument's; CAST to NUMERIC(p,s) imposes (p,s); every
// other expression — arithmetic, a function, an aggregate, a window
// function, a literal, CAST to plain NUMERIC — carries none. With planned
// set (WITH DATA) PostgreSQL decides it after constant folding: a CASE whose
// condition is a constant is the arm it selects, and COALESCE drops its NULL
// constants.
//
// The answer has three values, and the third is the point: where this walk
// cannot tell — a construct it does not model, a condition it cannot fold,
// a column it cannot trace — the column keeps the declaration the plan
// gives it. A mark set wrongly changes what a later write stores (1.255 kept
// where PostgreSQL rounds to 1.26); a mark left unset is the declaration
// this engine always used.
//
// A FLOAT64 column also records the PostgreSQL CATEGORY the plan gives it
// (parquet.Column.PGNumeric): numeric where PostgreSQL creates a numeric
// column from an expression this engine computes in a double, and unmarked
// everywhere else, so the stored column is read under the category the same
// expression has before it is stored.
func (p *Planner) CreatedColumns(plan *logical.Node, declared []parquet.Column, planned bool) []parquet.Column {
	out := make([]parquet.Column, len(declared))
	copy(out, declared)
	if plan == nil {
		return out
	}
	if len(plan.CTEs) > 0 {
		saved := p.Ctes
		p.Ctes = plan.CTEs
		defer func() { p.Ctes = saved }()
	}
	c := &createdWalk{p: p, w: newDeclWalk(), fold: planned}
	mods := c.output(plan)
	if len(mods) != len(out) {
		mods = nil
	}
	// The category half of the declaration, positionally aligned with it
	// (declaredOutputPGCategory); nil when the walk cannot line it up, which
	// leaves every column unmarked — the declaration this engine always used.
	cats := declaredOutputPGCategory(plan)
	if cats == nil && findOutputProjectionNode(plan) == nil {
		// A bare star publishes its source's columns, in the order the star's
		// own declaration lists them: each takes the category its source
		// emits it under — a marked column of the table, by the name the
		// declaration gives it there.
		if cols, ok := c.w.starOnlyDeclaredOutputSchema(plan, nil); ok {
			emitted := emittedColPGCategory(plan)
			cats = make([]pgCategory, len(cols))
			for i, col := range cols {
				cats[i] = lookupColPGCategory(emitted, col.Name)
			}
		}
	}
	if len(cats) != len(out) {
		cats = nil
	}
	var asts []plansql.Node
	if setOpRootOf(plan) == nil {
		if pn := findOutputProjectionNode(plan); pn != nil {
			if vis := logical.VisibleProjections(pn.Projections); len(vis) == len(out) {
				for _, proj := range vis {
					asts = append(asts, proj.ASTExpr)
				}
			}
		}
	}
	for i := range out {
		switch out[i].Type {
		case parquet.TypeDecimal:
			// A bare copy of a stored column created unconstrained carries
			// the walk's own mark (logical.DecimalMeta.Unconstrained).
			if mods != nil {
				m := mods[i]
				if m.cls == tmKept && (m.p != out[i].Precision || m.s != out[i].Scale) {
					// The typmod PostgreSQL keeps is not the (p,s) this plan
					// computes the column at — a constant CASE the walk folds
					// to one arm, whose other arm widens the plan's common
					// type. The kept typmod is the column's declaration, so a
					// later write stores what PostgreSQL stores.
					out[i].Precision, out[i].Scale = m.p, m.s
				}
				out[i].Unconstrained = out[i].Unconstrained || m.cls == tmNone
			}
		case parquet.TypeFloat64:
			// The planner carries a numeric typed NULL on the float rung.
			out[i].Unconstrained = asts != nil && asts[i] != nil && typedNullNumeric(asts[i])
			// A value PostgreSQL types numeric that this engine computes in
			// a double — `5 / 2.0 + id * 0`, `sqrt(6.25 + id * 0)`, a bare
			// copy of a column created from one — is created FLOAT64 marked
			// with that category (ADR-0024 §2c), so a reader of the stored
			// column rounds it as PostgreSQL rounds its numeric column.
			out[i].PGNumeric = !out[i].Unconstrained && cats != nil && cats[i] == pgCatNumeric
		default:
			out[i].Unconstrained = false
		}
		if out[i].Type != parquet.TypeFloat64 {
			out[i].PGNumeric = false
		}
	}
	return out
}

const (
	tmUnknown = iota
	tmKept
	tmNone
)

// createdTypmod is one expression's typmod: kept (p,s), none (PostgreSQL's
// −1), or unknown to this walk. null marks an expression PostgreSQL folds to
// a NULL constant, which COALESCE drops.
type createdTypmod struct {
	cls  int
	p, s int
	null bool
}

var tmUnknownAnswer = createdTypmod{cls: tmUnknown}

type createdWalk struct {
	p     *Planner
	w     *declWalk
	fold  bool
	depth int
}

// createdScope is what a projection's expression resolves against: the
// declarations of the node below it and the names that node computes.
type createdScope struct {
	decls    ColDecls
	computed map[string]bool
	child    *logical.Node
}

const createdWalkMaxDepth = 64

func (c *createdWalk) enter() bool {
	c.depth++
	return c.depth <= createdWalkMaxDepth
}

func (c *createdWalk) leave() { c.depth-- }

// setOpRootOf is the set operation a query's output IS, below the ORDER BY,
// LIMIT and DISTINCT that sit over one, or nil.
func setOpRootOf(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
			return n
		case logical.NodeSort, logical.NodeLimit, logical.NodeDistinct:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// output is the typmod of each output column of a plan, positionally; nil
// when the walk cannot line the columns up.
func (c *createdWalk) output(root *logical.Node) []createdTypmod {
	if !c.enter() {
		c.leave()
		return nil
	}
	defer c.leave()
	if so := setOpRootOf(root); so != nil {
		var arms [][]createdTypmod
		var collect func(n *logical.Node) bool
		collect = func(n *logical.Node) bool {
			for _, ch := range n.Children {
				if inner := setOpRoot(ch); inner != nil {
					if !collect(inner) {
						return false
					}
					continue
				}
				a := c.output(ch)
				if a == nil || (len(arms) > 0 && len(a) != len(arms[0])) {
					return false
				}
				arms = append(arms, a)
			}
			return true
		}
		if !collect(so) || len(arms) == 0 {
			return nil
		}
		out := make([]createdTypmod, len(arms[0]))
		for i := range out {
			col := make([]createdTypmod, len(arms))
			for j := range arms {
				col[j] = arms[j][i]
			}
			out[i] = commonTypmod(col)
		}
		return out
	}
	pn := findOutputProjectionNode(root)
	if pn == nil {
		return nil
	}
	projs := logical.VisibleProjections(pn.Projections)
	if len(projs) == 0 {
		return nil
	}
	sc := c.scopeOf(pn)
	out := make([]createdTypmod, len(projs))
	for i, proj := range projs {
		out[i] = c.projection(proj, sc)
	}
	return out
}

func (c *createdWalk) scopeOf(pn *logical.Node) createdScope {
	if len(pn.Children) != 1 {
		return createdScope{}
	}
	return createdScope{
		decls:    c.w.childDecls(pn.Children[0]),
		computed: c.w.emittedComputedCols(pn.Children[0]),
		child:    pn.Children[0],
	}
}

func (c *createdWalk) projection(proj logical.Projection, sc createdScope) createdTypmod {
	if proj.IsAgg {
		return createdTypmod{cls: tmNone}
	}
	if proj.ASTExpr == nil {
		return c.named(sc, sourceRefName(proj))
	}
	return c.expr(proj.ASTExpr, sc)
}

// named is the typmod of the input column a scope reads by name: traced
// below when the node under the scope computes it, else the column's own
// declaration.
func (c *createdWalk) named(sc createdScope, name string) createdTypmod {
	lc := strings.ToLower(strings.TrimSpace(name))
	if sc.computed[lc] {
		return c.below(sc.child, lc)
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 && sc.computed[lc[dot+1:]] {
		return c.below(sc.child, lc[dot+1:])
	}
	t, ok := lookupColType(sc.decls.Types, name)
	if !ok {
		return tmUnknownAnswer
	}
	if t != parquet.TypeDecimal {
		return createdTypmod{cls: tmNone}
	}
	m, ok := lookupColDecimal(sc.decls.Dec, name)
	return storedTypmod(m, ok)
}

func storedTypmod(m logical.DecimalMeta, ok bool) createdTypmod {
	switch {
	case !ok || m.Precision <= 0:
		return tmUnknownAnswer
	case m.Unconstrained:
		return createdTypmod{cls: tmNone}
	}
	return createdTypmod{cls: tmKept, p: m.Precision, s: m.Scale}
}

// below traces a column a subtree computes to the expression that computes
// it.
func (c *createdWalk) below(n *logical.Node, name string) createdTypmod {
	if n == nil || !c.enter() {
		c.leave()
		return tmUnknownAnswer
	}
	defer c.leave()
	switch n.Type {
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return tmUnknownAnswer
		}
		return c.below(n.Children[0], name)
	case logical.NodeProject:
		for _, proj := range n.Projections {
			if strings.EqualFold(declaredProjectionName(proj), name) {
				return c.projection(proj, c.scopeOf(n))
			}
		}
		return tmUnknownAnswer
	case logical.NodeWindow:
		for _, we := range n.WindowExprs {
			if strings.EqualFold(we.OutputCol, name) {
				return createdTypmod{cls: tmNone}
			}
		}
		if len(n.Children) != 1 {
			return tmUnknownAnswer
		}
		return c.below(n.Children[0], name)
	case logical.NodeAggregate:
		for _, agg := range n.AggExprs {
			if strings.EqualFold(agg.OutputCol, name) {
				return createdTypmod{cls: tmNone}
			}
		}
		if len(n.Children) != 1 {
			return tmUnknownAnswer
		}
		return c.below(n.Children[0], name)
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return tmUnknownAnswer
		}
		_, inL := lookupColType(c.w.emittedColTypes(n.Children[0]), name)
		_, inR := lookupColType(c.w.emittedColTypes(n.Children[1]), name)
		switch {
		case inL && !inR:
			return c.below(n.Children[0], name)
		case inR && !inL:
			return c.below(n.Children[1], name)
		}
		return tmUnknownAnswer
	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		mods := c.output(n)
		arms := c.w.setOpArmSchemas(n)
		if len(arms) == 0 || len(arms[0]) != len(mods) {
			return tmUnknownAnswer
		}
		for i, col := range arms[0] {
			if strings.EqualFold(col.Name, name) {
				return mods[i]
			}
		}
		return tmUnknownAnswer
	case logical.NodeScan:
		t, ok := lookupColType(n.ScanColTypes, name)
		if !ok {
			return tmUnknownAnswer
		}
		if t != parquet.TypeDecimal {
			return createdTypmod{cls: tmNone}
		}
		m, ok := lookupColDecimal(n.ScanColDecimal, name)
		return storedTypmod(m, ok)
	}
	return tmUnknownAnswer
}

// expr is exprTypmod over one SELECT-list expression.
func (c *createdWalk) expr(node plansql.Node, sc createdScope) createdTypmod {
	if !c.enter() {
		c.leave()
		return tmUnknownAnswer
	}
	defer c.leave()
	switch n := node.(type) {
	case nil:
		// A missing ELSE: a NULL of no typmod.
		return createdTypmod{cls: tmNone, null: true}
	case *plansql.ParenNode:
		return c.expr(n.Inner, sc)
	case *plansql.Lit:
		return createdTypmod{cls: tmNone, null: n.Kind == plansql.LitNull}
	case *plansql.CastNode:
		null := nullOnlyTree(n)
		p, s, hasParams, ok := expr.DecimalCastDest(n.TypeName)
		if ok && hasParams {
			return createdTypmod{cls: tmKept, p: p, s: s, null: null}
		}
		return createdTypmod{cls: tmNone, null: null}
	case *plansql.ColRef:
		if sc.computed[strings.ToLower(cleanExpr(n.String()))] || sc.computed[strings.ToLower(n.Column)] {
			return c.below(sc.child, n.Column)
		}
		col, ok := sc.decls.colDecl(n)
		if !ok {
			return tmUnknownAnswer
		}
		if col.Type != parquet.TypeDecimal {
			return createdTypmod{cls: tmNone}
		}
		return storedTypmod(logical.DecimalMeta{Precision: col.Precision, Scale: col.Scale, Unconstrained: col.Unconstrained}, true)
	case *plansql.CaseNode:
		return c.caseExpr(n, sc)
	case *plansql.FuncCallNode:
		return c.call(n, sc)
	case *plansql.SubqueryNode:
		if n.Array {
			return createdTypmod{cls: tmNone}
		}
		return c.scalarSubquery(n.SQL)
	case *plansql.BinaryOp, *plansql.UnaryOp, *plansql.WindowFuncNode:
		return createdTypmod{cls: tmNone, null: nullOnlyTree(n)}
	}
	return tmUnknownAnswer
}

// scalarSubquery is the typmod of a scalar subquery's single column, which
// PostgreSQL's EXPR sublink carries.
func (c *createdWalk) scalarSubquery(sql string) (out createdTypmod) {
	defer func() {
		if r := recover(); r != nil {
			out = tmUnknownAnswer
		}
	}()
	plan := c.p.scalarAnswerPlan(sql)
	if plan == nil {
		return tmUnknownAnswer
	}
	mods := c.output(plan)
	if len(mods) != 1 {
		return tmUnknownAnswer
	}
	return mods[0]
}

func (c *createdWalk) call(n *plansql.FuncCallNode, sc createdScope) createdTypmod {
	switch strings.ToLower(n.Name) {
	case "coalesce":
		args := n.Args
		if c.fold {
			// eval_const_expressions: a NULL constant is dropped, and the
			// first non-NULL constant ends the list.
			var kept []createdTypmod
			for _, a := range args {
				m := c.expr(a, sc)
				if m.null {
					continue
				}
				kept = append(kept, m)
				if !hasVariable(a) {
					break
				}
			}
			switch len(kept) {
			case 0:
				return createdTypmod{cls: tmNone, null: true}
			case 1:
				return kept[0]
			}
			return commonTypmod(kept)
		}
		return c.common(args, sc)
	case "nullif":
		if len(n.Args) == 0 {
			return tmUnknownAnswer
		}
		m := c.expr(n.Args[0], sc)
		m.null = false
		return m
	case "greatest", "least":
		return c.common(n.Args, sc)
	case "element_at":
		// A subscript (the parser's spelling of `a[i]`): the element's
		// typmod, which an ARRAY[...] constructor keeps when every element
		// carries the same one.
		if len(n.Args) == 2 {
			if arr, ok := plansql.Unparen(n.Args[0]).(*plansql.ArrayLitNode); ok {
				return c.common(arr.Elements, sc)
			}
		}
		return tmUnknownAnswer
	case "row_field":
		// A field of a ROW value: the field's declaration is not traced.
		return tmUnknownAnswer
	}
	if idx, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args)); poly {
		// This engine's own choosing functions (IFNULL, IF) answer one of
		// the arguments they list, as COALESCE does.
		args := make([]plansql.Node, 0, len(idx))
		for _, i := range idx {
			if i >= 0 && i < len(n.Args) {
				args = append(args, n.Args[i])
			}
		}
		return c.common(args, sc)
	}
	// A function result, an aggregate call: PostgreSQL gives it no typmod.
	return createdTypmod{cls: tmNone, null: nullOnlyTree(n)}
}

func (c *createdWalk) common(args []plansql.Node, sc createdScope) createdTypmod {
	ms := make([]createdTypmod, len(args))
	for i, a := range args {
		ms[i] = c.expr(a, sc)
	}
	m := commonTypmod(ms)
	m.null = false
	return m
}

// caseExpr is a CASE's typmod: every result it can answer, the missing ELSE
// included, must carry the same one. With constant folding a WHEN whose
// condition is a constant false or NULL is dropped and one that is a
// constant true ends the list as its ELSE; a CASE left with no WHEN is that
// ELSE.
func (c *createdWalk) caseExpr(n *plansql.CaseNode, sc createdScope) createdTypmod {
	var arms []plansql.Node
	def := n.Else
	if c.fold {
		for _, w := range n.Whens {
			cond := w.Cond
			if n.Subject != nil {
				cond = &plansql.CmpExpr{Left: n.Subject, Op: "=", Right: w.Cond}
			}
			if hasVariable(cond) {
				arms = append(arms, w.Result)
				continue
			}
			v, ok := constBool(cond)
			if !ok {
				// A constant PostgreSQL folds and this walk cannot.
				return tmUnknownAnswer
			}
			if v == constTrue {
				def = w.Result
				break
			}
		}
		if len(arms) == 0 {
			return c.expr(def, sc)
		}
	} else {
		for _, w := range n.Whens {
			arms = append(arms, w.Result)
		}
	}
	ms := make([]createdTypmod, 0, len(arms)+1)
	for _, a := range arms {
		ms = append(ms, c.expr(a, sc))
	}
	ms = append(ms, c.expr(def, sc))
	m := commonTypmod(ms)
	m.null = false
	return m
}

// commonTypmod is select_common_typmod: one value of no typmod makes the
// result none, whatever else is unknown; otherwise an unknown one makes it
// unknown; otherwise the result keeps a typmod only when every value
// carries the same one.
func commonTypmod(ms []createdTypmod) createdTypmod {
	if len(ms) == 0 {
		return tmUnknownAnswer
	}
	unknown := false
	for _, m := range ms {
		switch m.cls {
		case tmNone:
			return createdTypmod{cls: tmNone}
		case tmUnknown:
			unknown = true
		}
	}
	if unknown {
		return tmUnknownAnswer
	}
	for _, m := range ms[1:] {
		if m.p != ms[0].p || m.s != ms[0].s {
			return createdTypmod{cls: tmNone}
		}
	}
	return createdTypmod{cls: tmKept, p: ms[0].p, s: ms[0].s}
}

// hasVariable reports an expression whose value depends on a row or a
// relation: a column reference, a subquery, a window function. Anything
// else is a constant PostgreSQL may fold, which is the answer that sends an
// unfoldable condition to "unknown" rather than to a guess.
func hasVariable(n plansql.Node) bool {
	switch e := n.(type) {
	case nil:
		return false
	case *plansql.ColRef, *plansql.SubqueryNode, *plansql.ExistsNode, *plansql.WindowFuncNode:
		return true
	case *plansql.FuncCallNode:
		if unfoldedFunc[strings.ToLower(e.Name)] {
			return true
		}
	case *plansql.CaseNode:
		if hasVariable(e.Subject) || hasVariable(e.Else) {
			return true
		}
		for _, w := range e.Whens {
			if hasVariable(w.Cond) || hasVariable(w.Result) {
				return true
			}
		}
		return false
	}
	for _, o := range exprOperands(n) {
		if hasVariable(o) {
			return true
		}
	}
	return false
}

// unfoldedFunc names the functions whose value is not fixed by their
// arguments — PostgreSQL's volatile and stable ones, which constant folding
// leaves in place.
var unfoldedFunc = map[string]bool{
	"random": true, "rand": true, "setseed": true, "now": true, "current_timestamp": true,
	"current_date": true, "current_time": true, "localtimestamp": true, "localtime": true,
	"clock_timestamp": true, "statement_timestamp": true, "transaction_timestamp": true,
	"timeofday": true, "gen_random_uuid": true, "uuid": true, "uuid_generate_v4": true,
	"nextval": true, "setval": true, "currval": true, "pg_sleep": true, "sleep": true,
}

const (
	constFalse = iota
	constTrue
	constNull
)

// constBool evaluates a constant condition this walk can read: the boolean
// and NULL literals, NOT / AND / OR over them, and a comparison of two
// number literals or (=, <>) two string literals.
func constBool(n plansql.Node) (int, bool) {
	switch e := plansql.Unparen(n).(type) {
	case *plansql.Lit:
		switch e.Kind {
		case plansql.LitNull:
			return constNull, true
		case plansql.LitBool:
			switch strings.ToLower(e.Value) {
			case "true":
				return constTrue, true
			case "false":
				return constFalse, true
			}
		}
		return 0, false
	case *plansql.NotNode:
		v, ok := constBool(e.Inner)
		if !ok {
			return 0, false
		}
		switch v {
		case constTrue:
			return constFalse, true
		case constFalse:
			return constTrue, true
		}
		return constNull, true
	case *plansql.AndNode:
		l, lok := constBool(e.Left)
		r, rok := constBool(e.Right)
		if !lok || !rok {
			return 0, false
		}
		switch {
		case l == constFalse || r == constFalse:
			return constFalse, true
		case l == constNull || r == constNull:
			return constNull, true
		}
		return constTrue, true
	case *plansql.OrNode:
		l, lok := constBool(e.Left)
		r, rok := constBool(e.Right)
		if !lok || !rok {
			return 0, false
		}
		switch {
		case l == constTrue || r == constTrue:
			return constTrue, true
		case l == constNull || r == constNull:
			return constNull, true
		}
		return constFalse, true
	case *plansql.CmpExpr:
		return constCompare(e)
	case *plansql.IsExpr:
		return constIs(e)
	case *plansql.InExpr:
		return constIn(e)
	case *plansql.BetweenExpr:
		lo, lok := constCompare(&plansql.CmpExpr{Left: e.Left, Op: ">=", Right: e.Low})
		hi, hok := constCompare(&plansql.CmpExpr{Left: e.Left, Op: "<=", Right: e.High})
		if !lok || !hok {
			return 0, false
		}
		v := and3(lo, hi)
		if e.Not {
			v = not3(v)
		}
		return v, true
	}
	return 0, false
}

// constIs is `x IS [NOT] NULL / TRUE / FALSE` over a constant x this walk can
// read: a literal, or a condition constBool reads.
func constIs(e *plansql.IsExpr) (int, bool) {
	var v int
	switch l := plansql.Unparen(e.Left).(type) {
	case *plansql.Lit:
		switch l.Kind {
		case plansql.LitNull:
			v = constNull
		case plansql.LitBool:
			b, ok := constBool(l)
			if !ok {
				return 0, false
			}
			v = b
		default:
			if strings.ToLower(e.Check) != "null" {
				return 0, false
			}
			v = constTrue // a non-NULL literal: only its NULL-ness is read
		}
	default:
		b, ok := constBool(l)
		if !ok {
			return 0, false
		}
		v = b
	}
	var r int
	switch strings.ToLower(e.Check) {
	case "null":
		r = boolConst(v == constNull)
	case "true":
		r = boolConst(v == constTrue)
	case "false":
		r = boolConst(v == constFalse)
	default:
		return 0, false
	}
	if e.Not {
		r = not3(r)
	}
	return r, true
}

// constIn is `x [NOT] IN (v, …)` over literals: true on a match, NULL when
// no value matches and one compared NULL, false otherwise.
func constIn(e *plansql.InExpr) (int, bool) {
	if len(e.Values) == 0 {
		return 0, false
	}
	sawNull := false
	for _, v := range e.Values {
		c, ok := constCompare(&plansql.CmpExpr{Left: e.Left, Op: "=", Right: v})
		if !ok {
			return 0, false
		}
		switch c {
		case constTrue:
			if e.Not {
				return constFalse, true
			}
			return constTrue, true
		case constNull:
			sawNull = true
		}
	}
	if sawNull {
		return constNull, true
	}
	if e.Not {
		return constTrue, true
	}
	return constFalse, true
}

func not3(v int) int {
	switch v {
	case constTrue:
		return constFalse
	case constFalse:
		return constTrue
	}
	return constNull
}

func and3(a, b int) int {
	switch {
	case a == constFalse || b == constFalse:
		return constFalse
	case a == constNull || b == constNull:
		return constNull
	}
	return constTrue
}

func constCompare(e *plansql.CmpExpr) (int, bool) {
	l, r := plansql.Unparen(e.Left), plansql.Unparen(e.Right)
	if ll, ok := l.(*plansql.Lit); ok && ll.Kind == plansql.LitNull {
		return constNull, true
	}
	if rl, ok := r.(*plansql.Lit); ok && rl.Kind == plansql.LitNull {
		return constNull, true
	}
	var cmp int
	if a, ok := constNumber(l); ok {
		b, ok := constNumber(r)
		if !ok {
			return 0, false
		}
		cmp = a.Cmp(b)
	} else {
		ls, lok := l.(*plansql.Lit)
		rs, rok := r.(*plansql.Lit)
		if !lok || !rok || ls.Kind != plansql.LitString || rs.Kind != plansql.LitString {
			return 0, false
		}
		switch e.Op {
		case "=":
			return boolConst(ls.Value == rs.Value), true
		case "!=", "<>":
			return boolConst(ls.Value != rs.Value), true
		}
		return 0, false
	}
	switch e.Op {
	case "=":
		return boolConst(cmp == 0), true
	case "!=", "<>":
		return boolConst(cmp != 0), true
	case "<":
		return boolConst(cmp < 0), true
	case "<=":
		return boolConst(cmp <= 0), true
	case ">":
		return boolConst(cmp > 0), true
	case ">=":
		return boolConst(cmp >= 0), true
	}
	return 0, false
}

func constNumber(n plansql.Node) (*big.Rat, bool) {
	switch e := plansql.Unparen(n).(type) {
	case *plansql.Lit:
		if e.Kind != plansql.LitNumber {
			return nil, false
		}
		r, ok := new(big.Rat).SetString(e.Value)
		return r, ok
	case *plansql.UnaryOp:
		r, ok := constNumber(e.Inner)
		if !ok {
			return nil, false
		}
		switch e.Op {
		case "-":
			return r.Neg(r), true
		case "+":
			return r, true
		}
	case *plansql.BinaryOp:
		// Sums, differences and products are exact in every numeric type,
		// so the rational answer is PostgreSQL's whatever the operands'
		// types; a quotient is not (integer division truncates).
		a, ok := constNumber(e.Left)
		if !ok {
			return nil, false
		}
		b, ok := constNumber(e.Right)
		if !ok {
			return nil, false
		}
		switch e.Op {
		case "+":
			return new(big.Rat).Add(a, b), true
		case "-":
			return new(big.Rat).Sub(a, b), true
		case "*":
			return new(big.Rat).Mul(a, b), true
		}
	}
	return nil, false
}

func boolConst(b bool) int {
	if b {
		return constTrue
	}
	return constFalse
}
