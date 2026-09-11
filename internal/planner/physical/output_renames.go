// This file holds output renames for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// extractOutputRenames inspects the logical plan tree's outermost projection
// node and returns one (source-column → alias) pair per SELECT-list item — in
// SELECT-list order — describing the final output schema. The coordinator
// uses this list both to RENAME columns AND to DROP columns the worker
// emitted but the user didn't ask for (e.g., Q15's join output carries
// supplier/lineitem internals that the SELECT list doesn't project).
//
// For aggregate columns, the source equals the alias (planner sets
// AggSpec.OutputCol to the alias). Wrapped aggregates ("SUM(x)/7.0 AS x")
// still aren't handled here — the worker emits the raw aggregate, and
// applying the divisor needs a post-aggregate Project. Wrapped-aggregate
// projections are passed through with their source pointing at the wrapped
// expression text so that at least the rename is attempted (it'll miss
// gracefully and the column drops, surfacing the bug clearly in tests).
//
// Returns nil when the outermost emitting node isn't a projection (e.g.,
// top-level scan or aggregate without a SELECT-list rename layer).
func extractOutputRenames(root *logical.Node) []OutputRename {
	// Hidden projections are the planner's own — a materialized ORDER BY term
	// the SELECT list does not carry (#320). Leaving them out of the rename
	// list is what drops them from the client's result: the gather projects to
	// exactly the columns named here.
	proj := logical.VisibleProjections(findOutputProjectionsForRename(root))
	if len(proj) == 0 {
		return nil
	}
	// An expression the gather EVALUATES is evaluated over the producer's
	// output, where a derived GROUP BY key is one column and the input
	// columns it was computed from are gone. `(g + 1) + COUNT(*)` reached
	// here spelled over `g`, so the gather read a column the aggregate does
	// not emit and answered NULL for every row while the single-process path
	// answered correctly — the same identity gap as #723, one stage later.
	keyRefs := groupKeyByIdentity(aggregateUnderOutput(root))
	renames := make([]OutputRename, 0, len(proj))
	renameScope := findOutputProjectionNode(root)
	for _, p := range proj {
		var src, target string
		var astExpr plansql.Node
		var declType expr.DeclType
		declKnown := false
		// The CLASS of what this item refers to, not of the item itself. A
		// wrapper — one derived table or one CTE — makes an aggregate output
		// a plain column reference to the block above, and the gather pairs a
		// duplicate source name with the column of its own class (#575,
		// #785). Asking `p.IsAgg` asked about the wrapper (#785 round 2).
		isAgg := p.IsAgg
		if !isAgg && renameScope != nil && len(renameScope.Children) == 1 {
			src := p.Column
			if src == "" {
				src = strings.ToLower(strings.TrimSpace(p.Expr))
			}
			isAgg = renameIsAggregateOutput(src, renameScope.Children[0])
		}
		switch {
		case p.IsAgg:
			// AggSpec.OutputCol == alias; if no alias, fall back to expr.
			//
			// The SOURCE is the lowercased text because that is the spelling
			// the aggregate stage emits under. The TARGET is the name the
			// CLIENT is told, and it is the expression text VERBATIM —
			// `deriveColumns` (wadjet/wadjet.go) sends `col.Expr` unfolded on
			// the single-process path, and the two paths must describe one
			// query identically (#744). Case-folding here is what made
			// `SUM(a) OVER (...) + 1` arrive as `... over (...) + 1` from the
			// DAG and `... OVER (...) + 1` from the single-process engine.
			target = p.Alias
			src = target
			if target == "" {
				target = p.Expr
				src = strings.ToLower(p.Expr)
			}
		case p.ASTExpr != nil && !isSimpleColRefForRename(p.ASTExpr) &&
			(referencesSyntheticAgg(p.ASTExpr) || referencesSyntheticWindow(p.ASTExpr)):
			// Evaluate expressions over __agg_N at gather so wrappers around aggregates
			// are applied. Pure scalar GROUP BY/project outputs already exist under their
			// lowercased expression text and need a plain rename, not evaluation.
			// Nested window expressions over __win_N also need gather evaluation because
			// the DAG's passthrough Project does not apply the wrapper (#610).
			// Keep the client's target name verbatim, not lowercased (#744); resolve the
			// source separately with firstColRefName.
			target = p.Alias
			if target == "" {
				target = p.Expr
			}
			astExpr = plansql.ReplaceGroupKeyRefs(p.ASTExpr, keyRefs)
			src = firstColRefName(astExpr)
			// The declaration for the column the gather is about to build.
			// Inferred against the scope the RESPELLED expression reads —
			// the producer's output, where `__agg_N` and `__win_N` live —
			// which is the same scope attachScanSelectProjections types its
			// own specs against.
			declType, declKnown = inferRenameExprDecl(astExpr, renameScope)
		case p.Column != "" && p.Alias != "":
			// Bare column reference. Worker may emit qualified ("n1.n_name")
			// or unqualified ("n_name") depending on the upstream join chain.
			// Prefer Expr (qualified-preserving) when it's a colref, else Column.
			src = p.Column
			if p.Expr != "" {
				src = strings.ToLower(p.Expr)
			}
			target = p.Alias
		case p.Expr != "":
			src = strings.ToLower(p.Expr)
			target = p.Alias
			if target == "" {
				target = src
				// No AS on a plain column reference: SQL names the output
				// column after the COLUMN, not the qualified reference the
				// user typed — `SELECT d.label` is a column named "label",
				// which is what the single-process path returns. Projection
				// .Column is the parser's unqualified name and is set only
				// for a colref, so this branch is exactly that shape. The
				// SOURCE stays the qualified Expr; the gather resolves it
				// through the same qualified↔bare fallback everything else
				// uses.
				if p.Column != "" {
					target = p.Column
				}
			}
		case p.Column != "":
			src = p.Column
			target = src
		default:
			continue
		}
		// The TARGET is the name the CLIENT is told, and PostgreSQL's name for
		// an unaliased item is not its text: `SELECT g + 1` is `?column?`,
		// `SELECT COUNT(*)` is `count`, `SELECT CAST(g AS bigint)` is `g`
		// (#732). The SOURCE is untouched — it is how the gather FINDS the
		// column in the stage's stream, and that is still the resolution
		// spelling. This is the DAG's half of the split; the single-process
		// half is CollectSink.OutputNames.
		if p.PublishedName != "" {
			target = p.PublishedName
		}
		renames = append(renames, OutputRename{
			From: src, To: target, Expr: astExpr, IsAgg: isAgg,
			Type: declType.ID, TypeKnown: declKnown,
			Precision: declType.Precision, Scale: declType.Scale,
		})
	}
	return renames
}

// isSimpleColRefForRename is the OutputRenames-specific variant of
// isSimpleColRef. The base helper at line 4308 also returns true for Lit
// nodes; here we only want to skip the eval path when the projection is
// strictly a column reference (or a parenthesized one).
func isSimpleColRefForRename(n plansql.Node) bool {
	if n == nil {
		return false
	}
	if _, ok := n.(*plansql.ColRef); ok {
		return true
	}
	if p, ok := n.(*plansql.ParenNode); ok {
		return isSimpleColRefForRename(p.Inner)
	}
	return false
}

// referencesSynthetic reports whether an AST contains any ColRef whose name
// starts with prefix — "__agg_" for the nested-aggregate rewrite, "__win_" for
// the nested-window rewrite (#610). It MUST traverse exactly the node set the
// logical rewrites do (plansql.ReplaceAllAggregates / ReplaceWindowFuncs):
// those rewrites can bury a __agg_/__win_ ColRef under a boolean/predicate
// wrapper (BETWEEN, AND/OR, IN, LIKE, IS, ANY/ALL), and a helper that stopped
// short there told extractOutputRenames the projection was a plain column
// rename. The gather then emitted the internal synthetic column (and the raw
// base columns beside it) to the client instead of evaluating the wrapper —
// the exact "wrong answer, right shape" leak #610 set out to kill, on the DAG.
func referencesSynthetic(n plansql.Node, prefix string) bool {
	if n == nil {
		return false
	}
	switch x := n.(type) {
	case *plansql.ColRef:
		return strings.HasPrefix(x.Column, prefix)
	case *plansql.BinaryOp:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.UnaryOp:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.CmpExpr:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.ParenNode:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.CastNode:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.FuncCallNode:
		for _, a := range x.Args {
			if referencesSynthetic(a, prefix) {
				return true
			}
		}
	case *plansql.CaseNode:
		if referencesSynthetic(x.Subject, prefix) {
			return true
		}
		for _, w := range x.Whens {
			if referencesSynthetic(w.Cond, prefix) || referencesSynthetic(w.Result, prefix) {
				return true
			}
		}
		return referencesSynthetic(x.Else, prefix)
	case *plansql.IsExpr:
		return referencesSynthetic(x.Left, prefix)
	case *plansql.NotNode:
		return referencesSynthetic(x.Inner, prefix)
	case *plansql.AndNode:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.OrNode:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Right, prefix)
	case *plansql.InExpr:
		if referencesSynthetic(x.Left, prefix) {
			return true
		}
		for _, v := range x.Values {
			if referencesSynthetic(v, prefix) {
				return true
			}
		}
	case *plansql.BetweenExpr:
		return referencesSynthetic(x.Left, prefix) ||
			referencesSynthetic(x.Low, prefix) || referencesSynthetic(x.High, prefix)
	case *plansql.LikeExpr:
		return referencesSynthetic(x.Left, prefix) || referencesSynthetic(x.Pattern, prefix)
	case *plansql.AnyAllExpr:
		if referencesSynthetic(x.Left, prefix) {
			return true
		}
		for _, v := range x.Values {
			if referencesSynthetic(v, prefix) {
				return true
			}
		}
	case *plansql.TupleNode:
		for _, e := range x.Elements {
			if referencesSynthetic(e, prefix) {
				return true
			}
		}
	case *plansql.ArrayLitNode:
		for _, e := range x.Elements {
			if referencesSynthetic(e, prefix) {
				return true
			}
		}
	}
	return false
}

// referencesSyntheticAgg reports whether an AST references a nested-aggregate
// synthetic column (__agg_N). Lets the gather rewrite distinguish "SUM(x)/7.0"
// (rewritten to "__agg_0/7.0", needs eval) from "SUBSTR(o_orderdate, 1, 4)"
// (worker-computed, needs rename).
func referencesSyntheticAgg(n plansql.Node) bool {
	return referencesSynthetic(n, "__agg_")
}

// referencesSyntheticWindow reports whether an AST references a nested-window
// synthetic column (__win_N), the marker the logical builder's nested-window
// rewrite uses for a window extracted out of a larger expression (#610).
func referencesSyntheticWindow(n plansql.Node) bool {
	return referencesSynthetic(n, "__win_")
}

// firstColRefName returns the first column reference name in an AST, used as
// the existence anchor for the gather rewrite. Returns "" when no ColRef is
// found (very rare — pure-literal projections).
func firstColRefName(n plansql.Node) string {
	if n == nil {
		return ""
	}
	switch x := n.(type) {
	case *plansql.ColRef:
		if x.Table != "" {
			return x.Table + "." + x.Column
		}
		return x.Column
	case *plansql.BinaryOp:
		if v := firstColRefName(x.Left); v != "" {
			return v
		}
		return firstColRefName(x.Right)
	case *plansql.UnaryOp:
		return firstColRefName(x.Inner)
	case *plansql.CmpExpr:
		if v := firstColRefName(x.Left); v != "" {
			return v
		}
		return firstColRefName(x.Right)
	case *plansql.ParenNode:
		return firstColRefName(x.Inner)
	case *plansql.FuncCallNode:
		for _, a := range x.Args {
			if v := firstColRefName(a); v != "" {
				return v
			}
		}
	case *plansql.CastNode:
		return firstColRefName(x.Inner)
	}
	return ""
}

// findOutputProjectionsForRename walks down through Sort/Limit/Filter wrappers
// to the outermost NodeProject and returns its projections. Returns nil when
// the outermost emitting node is not a projection (e.g., a top-level scan or
// aggregate without a SELECT-list rename layer).
func findOutputProjectionsForRename(n *logical.Node) []logical.Projection {
	if p := findOutputProjectionNode(n); p != nil {
		return p.Projections
	}
	return nil
}

// findOutputProjectionNode is findOutputProjectionsForRename returning the
// Project node itself, for callers that also need what feeds it — typing a
// projection expression takes the input's column types (inputColTypes).
func findOutputProjectionNode(n *logical.Node) *logical.Node {
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

// hiddenSortTrimOp returns the projection that drops a materialized ORDER BY
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
func hiddenSortTrimOp(root *logical.Node) exec.UnaryOperator {
	projs := findOutputProjectionsForRename(root)
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
			name = cleanExpr(p.Expr)
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
