// SPDX-License-Identifier: MIT

// This file carries PostgreSQL's NUMERIC CATEGORY (expr.DeclType.PGNumeric)
// through a plan: the companion to emittedColTypes (the carrier),
// emittedColDecimal (the (p,s)) and emittedColIntWidth (the integer width).
// Governed by ADR-0024 item 2 (its 2026-09-28 amendment).
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// emittedColPGNumeric names the columns a node emits in the FLOAT64 carrier
// whose PostgreSQL type is numeric: `SELECT 5 / 2.0 AS x` publishes a FLOAT64
// `x` that PostgreSQL calls numeric, and a reader of `x` one derived table,
// CTE, aggregate, window or set operation up must still know it. The walk has
// the node kinds emittedColIntWidth has, for the reason that one gives.
//
// An ABSENT entry is float8, which for a base column is the catalog's type.
func emittedColPGNumeric(n *logical.Node) map[string]bool {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeScan:
		// A catalog table's float column is float8; a relation the planner
		// types itself — a recursive CTE reference, unnest over numeric
		// literals — says otherwise here (annotateScanColumns).
		return n.ScanColPGNumeric
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		child := n.Children[0]
		decls := withSubqueryDecls(pgNumericInputDecls(child), n)
		out := map[string]bool{}
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			if projectionPGNumeric(proj, decls) {
				out[strings.ToLower(name)] = true
			}
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColPGNumeric(n.Children[0])
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		child := n.Children[0]
		in := withSubqueryDecls(pgNumericInputDecls(child), n)
		out := map[string]bool{}
		// A GROUP KEY is the value its input carried, a derived one included
		// (it is emitted under its expression text).
		for i, g := range n.GroupBy {
			name := strings.ToLower(strings.TrimSpace(g))
			if name == "" {
				continue
			}
			if t, ok := lookupColType(in.Types, g); ok {
				if t == parquet.TypeFloat64 && lookupColPGNumeric(in.pgNumeric, g) {
					out[name] = true
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
			if ok && pgCategoryOf(ast, in) == pgCatNumeric {
				out[name] = true
			}
		}
		for _, agg := range n.AggExprs {
			name := strings.ToLower(agg.OutputCol)
			if name == "" {
				continue
			}
			if t, known := aggSpecOutputType(n, agg); !known || t != parquet.TypeFloat64 {
				continue
			}
			if pgNumericAggregate(agg.Func, aggArgCategory(agg.InputExpr, agg.InputCol, in)) {
				out[name] = true
			}
		}
		return out
	case logical.NodeWindow:
		// A Window appends its slots and drops nothing.
		if len(n.Children) != 1 {
			return nil
		}
		in := pgNumericInputDecls(n.Children[0])
		out := make(map[string]bool, len(in.pgNumeric)+len(n.WindowExprs))
		for k, v := range in.pgNumeric {
			out[k] = v
		}
		for _, we := range n.WindowExprs {
			name := strings.ToLower(we.OutputCol)
			if name == "" {
				continue
			}
			delete(out, name)
			if windowSpecOutputType(n, we).ID != parquet.TypeFloat64 {
				continue
			}
			fn := strings.ToLower(strings.TrimSpace(we.Func))
			cat := aggArgCategory(we.InputExpr, cleanExpr(we.InputColumn()), in)
			if (windowValueFunc(fn) && cat == pgCatNumeric) || pgNumericAggregate(fn, cat) {
				out[name] = true
			}
		}
		return out
	case logical.NodeJoin:
		if len(n.Children) != 2 {
			return nil
		}
		left, leftKnown := joinArmPGNumeric(n.Children[0])
		right, rightKnown := joinArmPGNumeric(n.Children[1])
		merged := mergeJoinSides(left, right)
		if !leftKnown || !rightKnown {
			// An arm whose names this walk cannot list may publish any bare
			// name the other arm carries, so no bare entry is proven; the
			// qualified ones below still name their side.
			bare := make(map[string]bool, len(merged))
			for k, v := range merged {
				if strings.IndexByte(k, '.') >= 0 {
					bare[k] = v
				}
			}
			merged = bare
		}
		return withJoinArmQualifiers(n, left, right, merged)
	case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
		cols, ok := setOpDeclaredOutputSchema(n)
		if !ok {
			return nil
		}
		pos := setOpPGNumeric(n, cols)
		out := map[string]bool{}
		for i, col := range cols {
			if i < len(pos) && pos[i] {
				out[strings.ToLower(col.Name)] = true
			}
		}
		return out
	}
	return nil
}

// joinArmPGNumeric is one join arm's categories with an entry for EVERY
// column the arm emits — false where PostgreSQL's type is not numeric — so
// the join's merge sees a name both arms publish at different categories and
// drops the bare name instead of keeping the one arm that said anything.
//
// emittedColPGNumeric's own answer lists only the numeric columns: a float8
// base column contributes no entry, so `s a JOIN (SELECT id, 5 / 2.0 AS y …)
// b` kept b's bare `y: true`, and `a.y` — which has no qualified entry when
// the category map holds nothing for a — fell back to it and a float8 rounded
// half away from zero (#1353 round-1 review, B1; the by-name lesson of #1177).
// known is false for an arm whose columns this walk cannot list at all.
func joinArmPGNumeric(arm *logical.Node) (m map[string]bool, known bool) {
	cats := emittedColPGNumeric(arm)
	types := emittedColTypes(arm)
	out := make(map[string]bool, len(types)+len(cats))
	for name := range types {
		out[name] = false
	}
	for name, v := range cats {
		out[name] = v
	}
	return out, types != nil || arm.Type == logical.NodeDual
}

// pgNumericInputDecls is what an expression over n's output reads: the
// carrier, the shape, the (p,s) and the category, from one set of walks.
func pgNumericInputDecls(n *logical.Node) ColDecls {
	return ColDecls{
		Types:     emittedColTypes(n),
		Fields:    inputColFields(n),
		Dec:       emittedColDecimal(n),
		pgNumeric: emittedColPGNumeric(n),
	}
}

// pgNumericAggregate is PostgreSQL's category of an aggregate's result over an
// argument of category arg: MIN, MAX, SUM and AVG answer numeric over a
// numeric, and the STDDEV / VARIANCE family numeric over a numeric or an
// integer (ADR-0012 item 9's float64 accumulation is this engine's; the type
// is PostgreSQL's). Everything else — CORR, the COVAR and REGR families,
// PERCENTILE_CONT — is float8 there.
func pgNumericAggregate(fn string, arg pgCategory) bool {
	switch strings.ToLower(strings.TrimSpace(fn)) {
	case "min", "max", "sum", "avg":
		return arg == pgCatNumeric
	case "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop":
		return arg == pgCatNumeric || arg == pgCatInteger
	}
	return false
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

// lookupColPGNumeric is lookupColIntWidth's category companion.
func lookupColPGNumeric(m map[string]bool, name string) bool {
	if m == nil || name == "" {
		return false
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if v, ok := m[lc]; ok {
		return v
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		return m[lc[dot+1:]]
	}
	return false
}

// setOpPGNumeric is select_common_type over each position of a set
// operation's arms: numeric when an arm is numeric and none is float8. An
// UNKNOWN-typed literal arm takes no part, as it takes none in the type.
func setOpPGNumeric(n *logical.Node, cols []parquet.Column) []bool {
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
			pg := declaredOutputPGNumeric(c)
			arm := make([]pgCategory, len(schema))
			for i, col := range schema {
				d := withPGNumeric(expr.Decl(col.Type), i < len(pg) && pg[i])
				arm[i] = pgCategoryOfDecl(d, expr.Decided)
			}
			arms = append(arms, arm)
		}
		return true
	}
	if !walk(n) || len(arms) == 0 {
		return nil
	}
	unknown := setOpArmUnknownLiteralSchemas(n, len(arms), len(cols))
	out := make([]bool, len(cols))
	for i, col := range cols {
		if col.Type != parquet.TypeFloat64 {
			continue
		}
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
		out[i] = pgNumericOverNoFloat(cats...)
	}
	return out
}

// declaredOutputPGNumeric is declaredOutputSchema's category half, POSITIONAL
// and aligned with it: true at a FLOAT64 output column whose PostgreSQL type
// is numeric. nil when the walk cannot say, which every reader takes as
// float8 — the answer it had before.
func declaredOutputPGNumeric(root *logical.Node) []bool {
	if root == nil {
		return nil
	}
	if cols, ok := setOpDeclaredOutputSchema(root); ok {
		return setOpPGNumeric(setOpRoot(root), cols)
	}
	projs, childTypes, _, ok := declaredProjectionInputs(root)
	if !ok {
		return nil
	}
	if pn := findOutputProjectionNode(root); pn != nil && len(pn.Children) == 1 {
		childTypes.pgNumeric = emittedColPGNumeric(pn.Children[0])
		childTypes = withSubqueryDecls(childTypes, pn)
	}
	out := make([]bool, len(projs))
	for i, proj := range projs {
		out[i] = projectionPGNumeric(proj, childTypes)
	}
	return out
}

// projectionPGNumeric is PostgreSQL's category of one projection: a column
// the input publishes (an aggregate's output, a group key emitted under its
// expression text, a bare or parenthesized reference) carries what the input
// declared for it, and a computed expression is categorized from its AST.
//
// It reads the category, not the carrier: a reader honours it only where the
// column arrives as a double, so a category that sits beside an exact DECIMAL
// declaration changes nothing.
func projectionPGNumeric(proj logical.Projection, decls ColDecls) bool {
	if proj.IsAgg {
		return lookupColPGNumeric(decls.pgNumeric, declaredProjectionName(proj))
	}
	if cr, ok := bareColRefOf(proj.ASTExpr); ok {
		return decls.colPGNumeric(cr)
	}
	if proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr) {
		// Published under its own text by the producer below (a derived
		// GROUP BY key, a DISTINCT item): the name, looked up EXACTLY — a
		// qualifier-stripping lookup would read `x + a.k` as the column `k`.
		key := strings.ToLower(strings.TrimSpace(proj.Expr))
		if _, named := decls.Types[key]; named {
			return decls.pgNumeric[key]
		}
		return pgCategoryOf(proj.ASTExpr, decls) == pgCatNumeric
	}
	ref := proj.Column
	if ref == "" {
		ref = cleanExpr(proj.Expr)
	}
	return lookupColPGNumeric(decls.pgNumeric, ref)
}
