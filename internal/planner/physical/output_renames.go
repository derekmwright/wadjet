// SPDX-License-Identifier: MIT

// This file holds output renames for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// IsSimpleColRefForRename is the OutputRenames-specific variant of
// isSimpleColRef. The base helper at line 4308 also returns true for Lit
// nodes; here we only want to skip the eval path when the projection is
// strictly a column reference (or a parenthesized one).
func IsSimpleColRefForRename(n plansql.Node) bool {
	if n == nil {
		return false
	}
	if _, ok := n.(*plansql.ColRef); ok {
		return true
	}
	if p, ok := n.(*plansql.ParenNode); ok {
		return IsSimpleColRefForRename(p.Inner)
	}
	return false
}

// ReferencesSynthetic reports whether an AST contains any ColRef whose name
// starts with prefix — "__agg_" for the nested-aggregate rewrite, "__win_" for
// the nested-window rewrite (#610). It MUST traverse exactly the node set the
// logical rewrites do (plansql.ReplaceAllAggregates / ReplaceWindowFuncs):
// those rewrites can bury a __agg_/__win_ ColRef under a boolean/predicate
// wrapper (BETWEEN, AND/OR, IN, LIKE, IS, ANY/ALL), and a helper that stopped
// short there told extractOutputRenames the projection was a plain column
// rename. The gather then emitted the internal synthetic column (and the raw
// base columns beside it) to the client instead of evaluating the wrapper —
// the exact "wrong answer, right shape" leak #610 set out to kill, on the DAG.
func ReferencesSynthetic(n plansql.Node, prefix string) bool {
	if n == nil {
		return false
	}
	switch x := n.(type) {
	case *plansql.ColRef:
		return strings.HasPrefix(x.Column, prefix)
	case *plansql.BinaryOp:
		return ReferencesSynthetic(x.Left, prefix) || ReferencesSynthetic(x.Right, prefix)
	case *plansql.UnaryOp:
		return ReferencesSynthetic(x.Inner, prefix)
	case *plansql.CmpExpr:
		return ReferencesSynthetic(x.Left, prefix) || ReferencesSynthetic(x.Right, prefix)
	case *plansql.ParenNode:
		return ReferencesSynthetic(x.Inner, prefix)
	case *plansql.CastNode:
		return ReferencesSynthetic(x.Inner, prefix)
	case *plansql.FuncCallNode:
		for _, a := range x.Args {
			if ReferencesSynthetic(a, prefix) {
				return true
			}
		}
	case *plansql.CaseNode:
		if ReferencesSynthetic(x.Subject, prefix) {
			return true
		}
		for _, w := range x.Whens {
			if ReferencesSynthetic(w.Cond, prefix) || ReferencesSynthetic(w.Result, prefix) {
				return true
			}
		}
		return ReferencesSynthetic(x.Else, prefix)
	case *plansql.IsExpr:
		return ReferencesSynthetic(x.Left, prefix)
	case *plansql.NotNode:
		return ReferencesSynthetic(x.Inner, prefix)
	case *plansql.AndNode:
		return ReferencesSynthetic(x.Left, prefix) || ReferencesSynthetic(x.Right, prefix)
	case *plansql.OrNode:
		return ReferencesSynthetic(x.Left, prefix) || ReferencesSynthetic(x.Right, prefix)
	case *plansql.InExpr:
		if ReferencesSynthetic(x.Left, prefix) {
			return true
		}
		for _, v := range x.Values {
			if ReferencesSynthetic(v, prefix) {
				return true
			}
		}
	case *plansql.BetweenExpr:
		return ReferencesSynthetic(x.Left, prefix) ||
			ReferencesSynthetic(x.Low, prefix) || ReferencesSynthetic(x.High, prefix)
	case *plansql.LikeExpr:
		return ReferencesSynthetic(x.Left, prefix) || ReferencesSynthetic(x.Pattern, prefix)
	case *plansql.AnyAllExpr:
		if ReferencesSynthetic(x.Left, prefix) {
			return true
		}
		for _, v := range x.Values {
			if ReferencesSynthetic(v, prefix) {
				return true
			}
		}
	case *plansql.TupleNode:
		for _, e := range x.Elements {
			if ReferencesSynthetic(e, prefix) {
				return true
			}
		}
	case *plansql.ArrayLitNode:
		for _, e := range x.Elements {
			if ReferencesSynthetic(e, prefix) {
				return true
			}
		}
	}
	return false
}

// ReferencesSyntheticAgg reports whether an AST references a nested-aggregate
// synthetic column (__agg_N). Lets the gather rewrite distinguish "SUM(x)/7.0"
// (rewritten to "__agg_0/7.0", needs eval) from "SUBSTR(o_orderdate, 1, 4)"
// (worker-computed, needs rename).
func ReferencesSyntheticAgg(n plansql.Node) bool {
	return ReferencesSynthetic(n, "__agg_")
}

// FindOutputProjectionsForRename walks down through Sort/Limit/Filter wrappers
// to the outermost NodeProject and returns its projections. Returns nil when
// the outermost emitting node is not a projection (e.g., a top-level scan or
// aggregate without a SELECT-list rename layer).
func FindOutputProjectionsForRename(n *logical.Node) []logical.Projection {
	if p := FindOutputProjectionNode(n); p != nil {
		return p.Projections
	}
	return nil
}

// FindOutputProjectionNode is FindOutputProjectionsForRename returning the
// Project node itself, for callers that also need what feeds it — typing a
// projection expression takes the input's column types (inputColTypes).
func FindOutputProjectionNode(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeProject:
			return n
		case logical.NodeSort, logical.NodeLimit, logical.NodeFilter, logical.NodeDistinct:
			// Descend through Distinct too: the gather must project to the
			// SELECT-list columns (the Project under the Distinct) so the
			// coordinator's distinct dedup runs over the output columns, not
			// the full upstream schema (#163).
			if len(n.Children) == 1 {
				n = n.Children[0]
				continue
			}
			return nil
		default:
			return nil
		}
	}
	return nil
}

// HiddenSortTrimOp returns the projection that drops a materialized ORDER BY
// term from the single-process pipeline's output, or nil when the plan carries
// none.
//
// The DAG drops these at the gather: extractOutputRenames lists only the
// visible select items and the gather projects to exactly that set. The
// single-process pipeline has no equivalent stage — its result columns are the
// sink's schema — so without this the Sort would hand __sortkey_N straight to
// the client alongside the columns the query asked for. The projection below
// the Sort is never elided when it carries a hidden column (its alias always
// differs from its source, which is what buildProject's needsProject test
// looks for), so the names resolved here are the ones the pipeline emits.
func HiddenSortTrimOp(root *logical.Node) exec.UnaryOperator {
	projs := FindOutputProjectionsForRename(root)
	if !logical.HasHiddenProjection(projs) {
		return nil
	}
	visible := logical.VisibleProjections(projs)
	cols := make([]exec.ProjectColumn, 0, len(visible))
	for i, p := range visible {
		// Same output naming buildProject applies: the alias, else the column
		// reference, else the expression text.
		name := p.Alias
		if name == "" {
			name = p.Column
		}
		if name == "" {
			name = CleanExpr(p.Expr)
		}
		if name == "" || name == "*" || strings.HasSuffix(name, ".*") {
			// An unexpanded star (no catalog to resolve it against) has no
			// column list to trim to. Leave the plan alone: an extra column in
			// the result beats projecting every row to nulls.
			return nil
		}
		cols = append(cols, exec.ProjectColumn{
			Name: name,
			// POSITIONAL, not by name. The trim is a narrowing: hidden
			// columns go LAST and stay last (logical.resolveOrderBy keeps
			// that invariant for the gather's benefit too), so visible
			// output i IS input column i. Copying by name instead gave two
			// same-named outputs — which `SELECT abs(a), abs(b)` now
			// legitimately produces, PostgreSQL calling both `abs` — the
			// SAME input column, so the second carried the first's values.
			SourceIdx:    i,
			SourceIdxSet: true,
			// The name-based fields stay as the fallback for an input whose
			// column count does not match (nothing produces one today, and
			// an extra column beats projecting every row to nulls).
			DirectCopy: name,
			SourceCol:  name,
			// DirectCopy resolution can still miss on a qualified/bare
			// mismatch; ColumnRef resolves lazily and keeps Project.Execute
			// from invoking a nil Expr.
			Expr: exec.ColumnRef(name),
		})
	}
	return exec.NewProject(cols)
}
