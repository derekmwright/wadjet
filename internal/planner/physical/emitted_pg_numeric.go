// SPDX-License-Identifier: MIT

// This file carries PostgreSQL's NUMERIC CATEGORY (expr.DeclType.PGNumeric)
// through a plan: the companion to emittedColTypes (the carrier),
// emittedColDecimal (the (p,s)) and emittedColIntWidth (the integer width).
// Governed by ADR-0024 §2c.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// emittedColPGCategory is PostgreSQL's numeric CATEGORY of each column a node
// emits, where the walk can name it: `SELECT 5 / 2.0 AS x` publishes a FLOAT64
// `x` that PostgreSQL calls numeric, `SELECT NULLIF(2.5, f) AS x` a DECIMAL `x`
// it calls float8, and a reader of `x` one derived table, CTE, aggregate,
// window or set operation up must still know it. The walk has the node kinds
// emittedColIntWidth has, for the reason that one gives.
//
// An ABSENT entry is the carrier's reading, which for a base column is the
// catalog's type. A reader applies an entry only where it disagrees with the
// carrier (withPGCategory), so an entry that restates the carrier is inert.
func emittedColPGCategory(n *logical.Node) map[string]pgCategory {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		// A catalog table's columns are what the catalog says; a relation the
		// planner types itself — a recursive CTE reference, unnest over
		// numeric literals — says otherwise here (annotateScanColumns).
		return n.ScanColPGCategory
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		child := n.Children[0]
		decls := withSubqueryDecls(pgCategoryInputDecls(child), n)
		out := map[string]pgCategory{}
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			if cat := projectionPGCategory(proj, decls); cat != pgCatUnknown {
				out[strings.ToLower(name)] = cat
			}
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColPGCategory(n.Children[0])
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		child := n.Children[0]
		in := withSubqueryDecls(pgCategoryInputDecls(child), n)
		out := map[string]pgCategory{}
		// A GROUP KEY is the value its input carried, a derived one included
		// (it is emitted under its expression text).
		for i, g := range n.GroupBy {
			name := strings.ToLower(strings.TrimSpace(g))
			if name == "" {
				continue
			}
			if _, ok := lookupColType(in.Types, g); ok {
				if cat := lookupColPGCategory(in.pgCat, g); cat != pgCatUnknown {
					out[name] = cat
				}
				continue
			}
			// The key's own AST where the builder kept it: its TEXT is a
			// rendering, and `EXTRACT(EPOCH FROM ts)` renders as `epoch(ts)`,
			// which is not the construct PostgreSQL types numeric.
			ast, ok := plansql.Node(nil), false
			if i < len(n.GroupByExprs) && n.GroupByExprs[i] != nil {
				ast, ok = n.GroupByExprs[i], true
			} else {
				ast, ok = groupKeyAST(child, g)
			}
			if !ok {
				continue
			}
			if cat := pgCategoryOf(ast, in); cat != pgCatUnknown {
				out[name] = cat
			}
		}
		for _, agg := range n.AggExprs {
			name := strings.ToLower(agg.OutputCol)
			if name == "" {
				continue
			}
			if cat := pgAggregateCategory(agg.Func, aggArgCategory(agg.InputExpr, agg.InputCol, in)); cat != pgCatUnknown {
				out[name] = cat
			}
		}
		return out
	case logical.NodeWindow:
		// A Window appends its slots and drops nothing.
		if len(n.Children) != 1 {
			return nil
		}
		in := pgCategoryInputDecls(n.Children[0])
		out := make(map[string]pgCategory, len(in.pgCat)+len(n.WindowExprs))
		for k, v := range in.pgCat {
			out[k] = v
		}
		for _, we := range n.WindowExprs {
			name := strings.ToLower(we.OutputCol)
			if name == "" {
				continue
			}
			delete(out, name)
			fn := strings.ToLower(strings.TrimSpace(we.Func))
			arg := aggArgCategory(we.InputExpr, cleanExpr(we.InputColumn()), in)
			cat := pgAggregateCategory(fn, arg)
			if windowValueFunc(fn) {
				cat = arg
			}
			if cat != pgCatUnknown {
				out[name] = cat
			}
		}
		return out
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		left, leftKnown := joinArmPGCategory(n.Children[0])
		right, rightKnown := joinArmPGCategory(n.Children[1])
		merged := mergeJoinSides(left, right)
		if !leftKnown || !rightKnown {
			// An arm whose names this walk cannot list may publish any bare
			// name the other arm carries, so no bare entry is proven; the
			// qualified ones below still name their side.
			qualified := make(map[string]pgCategory, len(merged))
			for k, v := range merged {
				if strings.IndexByte(k, '.') >= 0 {
					qualified[k] = v
				}
			}
			merged = qualified
		}
		return withJoinArmQualifiers(n, left, right, merged)
	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		cols, ok := setOpDeclaredOutputSchema(n)
		if !ok {
			return nil
		}
		pos := setOpPGCategory(n, cols)
		out := map[string]pgCategory{}
		for i, col := range cols {
			if i < len(pos) && pos[i] != pgCatUnknown {
				out[strings.ToLower(col.Name)] = pos[i]
			}
		}
		return out
	}
	return nil
}

// joinArmPGCategory is one join arm's categories with an entry for EVERY
// column the arm emits — the carrier's category where the walk names no other
// — so the join's merge sees a name both arms publish at different categories
// and drops the bare name instead of keeping the one arm that said anything.
//
// The walk's own answer names only the columns it resolved: a float8 base
// column contributes no entry, so `s a JOIN (SELECT id, 5 / 2.0 AS y …) b`
// kept b's bare `y: numeric`, and `a.y` — which has no qualified entry when
// the map holds nothing for a — fell back to it and a float8 rounded half away
// from zero (#1353; the by-name lesson of #1177). known is
// false for an arm whose columns this walk cannot list at all.
func joinArmPGCategory(arm *logical.Node) (m map[string]pgCategory, known bool) {
	cats := emittedColPGCategory(arm)
	types := emittedColTypes(arm)
	out := make(map[string]pgCategory, len(types)+len(cats))
	for name, t := range types {
		out[name] = pgCategoryOfDecl(expr.Decl(t), expr.Decided)
	}
	for name, v := range cats {
		out[name] = v
	}
	return out, types != nil || arm.Type == logical.NodeDual
}

// pgCategoryInputDecls is what an expression over n's output reads: the
// carrier, the shape, the (p,s) and the category, from one set of walks.
func pgCategoryInputDecls(n *logical.Node) ColDecls {
	d := childDecls(n)
	d.pgCat = emittedColPGCategory(n)
	return d
}

// pgAggregateCategory is PostgreSQL's category of an aggregate's result over
// an argument of category arg: MIN and MAX are their argument's type; SUM,
// AVG and the STDDEV / VARIANCE family answer float8 over a float8 and
// numeric over a numeric, and AVG and the STDDEV family numeric over an
// integer too (ADR-0012 item 9's float64 accumulation is this engine's; the
// type is PostgreSQL's). SUM over an integer is an integer or a numeric with
// no fraction, and every other aggregate — CORR, the COVAR and REGR families,
// PERCENTILE_CONT — is float8 there, which the carrier already reads:
// unknown.
func pgAggregateCategory(fn string, arg pgCategory) pgCategory {
	switch strings.ToLower(strings.TrimSpace(fn)) {
	case "min", "max":
		return arg
	case "sum":
		if arg == pgCatNumeric || arg == pgCatFloat {
			return arg
		}
	case "avg", "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop":
		switch arg {
		case pgCatFloat:
			return pgCatFloat
		case pgCatNumeric, pgCatInteger:
			return pgCatNumeric
		}
	}
	return pgCatUnknown
}

// aggArgCategory is the category of an aggregate's or window's argument: its
// AST when it is computed, its column otherwise.
func aggArgCategory(ast plansql.Node, col string, in ColDecls) pgCategory {
	if ast != nil {
		return pgCategoryOf(ast, in)
	}
	if col = strings.TrimSpace(col); col != "" {
		return pgCategoryOf(&plansql.ColRef{Column: col}, in)
	}
	return pgCatUnknown
}

// lookupColPGCategory is lookupColIntWidth's category companion.
func lookupColPGCategory(m map[string]pgCategory, name string) pgCategory {
	if m == nil || name == "" {
		return pgCatUnknown
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if v, ok := m[lc]; ok {
		return v
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		return m[lc[dot+1:]]
	}
	return pgCatUnknown
}

// setOpPGCategory is select_common_type over each position of a set
// operation's arms: float8 when an arm is, numeric when an arm is numeric and
// none is float8. An UNKNOWN-typed literal arm takes no part, as it takes none
// in the type.
func setOpPGCategory(n *logical.Node, cols []parquet.Column) []pgCategory {
	var arms [][]pgCategory
	var walk func(*logical.Node) bool
	walk = func(m *logical.Node) bool {
		for _, c := range m.Children {
			if inner := setOpRoot(c); inner != nil {
				if !walk(inner) {
					return false
				}
				continue
			}
			schema := declaredOutputSchema(c, nil)
			pg := declaredOutputPGCategory(c)
			arm := make([]pgCategory, len(schema))
			for i, col := range schema {
				cat := pgCatUnknown
				if i < len(pg) {
					cat = pg[i]
				}
				arm[i] = pgCategoryOfDecl(withPGCategory(expr.Decl(col.Type), cat), expr.Decided)
			}
			arms = append(arms, arm)
		}
		return true
	}
	if !walk(n) || len(arms) == 0 {
		return nil
	}
	unknown := setOpArmUnknownLiteralSchemas(n, len(arms), len(cols))
	out := make([]pgCategory, len(cols))
	for i := range cols {
		cats := make([]pgCategory, 0, len(arms))
		for a, arm := range arms {
			if unknown[a] != nil && i < len(unknown[a]) && unknown[a][i] {
				continue
			}
			if i >= len(arm) {
				cats = append(cats, pgCatUnknown)
				continue
			}
			cats = append(cats, arm[i])
		}
		out[i] = pgCommon(cats...)
	}
	return out
}

// declaredOutputPGCategory is declaredOutputSchema's category half,
// POSITIONAL and aligned with it: PostgreSQL's category of each output column
// where the walk can name it. nil (or pgCatUnknown at a position) when it
// cannot say, which every reader takes as the carrier's reading — the answer
// it had before.
func declaredOutputPGCategory(root *logical.Node) []pgCategory {
	if root == nil {
		return nil
	}
	if cols, ok := setOpDeclaredOutputSchema(root); ok {
		return setOpPGCategory(setOpRoot(root), cols)
	}
	projs, childTypes, _, ok := declaredProjectionInputs(root)
	if !ok {
		return nil
	}
	if pn := findOutputProjectionNode(root); pn != nil && len(pn.Children) == 1 {
		childTypes.pgCat = emittedColPGCategory(pn.Children[0])
		childTypes = withSubqueryDecls(childTypes, pn)
	}
	out := make([]pgCategory, len(projs))
	for i, proj := range projs {
		out[i] = projectionPGCategory(proj, childTypes)
	}
	return out
}

// projectionPGCategory is PostgreSQL's category of one projection: a column
// the input publishes (an aggregate's output, a group key emitted under its
// expression text, a bare or parenthesized reference) carries what the input
// declared for it, and a computed expression is categorized from its AST.
//
// It reads the category, not the carrier: a reader honours it only where it
// disagrees with the carrier the column arrives in (withPGCategory).
func projectionPGCategory(proj logical.Projection, decls ColDecls) pgCategory {
	if proj.IsAgg {
		return lookupColPGCategory(decls.pgCat, declaredProjectionName(proj))
	}
	if cr, ok := bareColRefOf(proj.ASTExpr); ok {
		return decls.colPGCategory(cr)
	}
	if proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr) {
		// Published under its own text by the producer below (a derived
		// GROUP BY key, a DISTINCT item): the name, looked up EXACTLY — a
		// qualifier-stripping lookup would read `x + a.k` as the column `k`.
		key := strings.ToLower(strings.TrimSpace(proj.Expr))
		if _, named := decls.Types[key]; named {
			return decls.pgCat[key]
		}
		return pgCategoryOf(proj.ASTExpr, decls)
	}
	ref := proj.Column
	if ref == "" {
		ref = cleanExpr(proj.Expr)
	}
	return lookupColPGCategory(decls.pgCat, ref)
}
