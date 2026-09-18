// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// setOpPublishedRenames is the gather's rename for a SET OPERATION, which has
// no projection of its own and therefore no entry in the walk above.
//
// A set operation's result columns are its LEFTMOST arm's (ADR-0026 §8b), and
// the PUBLISHED half of each is the arm's `Projection.PublishedName` — §2's
// pair. Without this the gather renamed nothing and the operation's columns
// reached the client under the arm's RESOLUTION spelling: `SELECT id, total+1
// FROM t UNION ALL …` published `total + 1` on the three DAG arms where the
// single-process sink publishes PostgreSQL's `?column?`, and
// `CAST(total AS VARCHAR)` published its own text where PostgreSQL publishes
// `total` (#1079). One statement, two engines, two column lists.
//
// It is NAMES ONLY and one per visible item, never a projection: the union
// stage already emits exactly the arm's list, so there is nothing to drop,
// nothing to evaluate and no class to pair (#575's IsAgg). A rename is
// emitted only where the two names DIFFER — everything else keeps the
// spelling it had, so an ordinary set operation's gather is unchanged.
func setOpPublishedRenames(root *logical.Node) []OutputRename {
	node := localPlanFacts.PublishedOutputProjectionNode(root)
	if node == nil {
		return nil
	}
	proj := logical.VisibleProjections(node.Projections)
	if len(proj) == 0 {
		return nil
	}
	renames := make([]OutputRename, 0, len(proj))
	differs := false
	for _, p := range proj {
		from := localPlanFacts.ProjectionOutputName(p)
		if from == "" {
			return nil
		}
		to := from
		if p.PublishedName != "" {
			to = p.PublishedName
		}
		if !strings.EqualFold(from, to) {
			differs = true
		}
		renames = append(renames, OutputRename{From: from, To: to})
	}
	if !differs {
		return nil
	}
	return renames
}

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
	proj := logical.VisibleProjections(localPlanFacts.FindOutputProjectionsForRename(root))
	if len(proj) == 0 {
		return setOpPublishedRenames(root)
	}
	// An expression the gather EVALUATES is evaluated over the producer's
	// output, where a derived GROUP BY key is one column and the input
	// columns it was computed from are gone. `(g + 1) + COUNT(*)` reached
	// here spelled over `g`, so the gather read a column the aggregate does
	// not emit and answered NULL for every row while the single-process path
	// answered correctly — the same identity gap as #723, one stage later.
	keyRefs := localPlanFacts.GroupKeyByIdentity(aggregateUnderOutput(root))
	renames := make([]OutputRename, 0, len(proj))
	renameScope := localPlanFacts.FindOutputProjectionNode(root)
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
		case p.ASTExpr != nil && !localPlanFacts.IsSimpleColRefForRename(p.ASTExpr) &&
			(localPlanFacts.ReferencesSyntheticAgg(p.ASTExpr) || referencesSyntheticWindow(p.ASTExpr)):
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
			Precision: declType.Precision, Scale: declType.Scale, Fields: declType.RowFields(),
		})
	}
	return renames
}

// referencesSyntheticWindow reports whether an AST references a nested-window
// synthetic column (__win_N), the marker the logical builder's nested-window
// rewrite uses for a window extracted out of a larger expression (#610).
func referencesSyntheticWindow(n plansql.Node) bool {
	return localPlanFacts.ReferencesSynthetic(n, "__win_")
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
