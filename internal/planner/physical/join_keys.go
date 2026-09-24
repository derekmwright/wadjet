// SPDX-License-Identifier: MIT

// This file holds join keys for the physical planner, governed by ADR-0023 and ADR-0026.
package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// parseJoinKeys structurally extracts bare-column equality pairs; return all
// other conjuncts (expressions, literals, non-equi operators, disjunctions) as
// residual for the caller to refuse, never as invented column names (#351).
// Preserve qualifiers so self-join chains resolve exactly; the executor can
// strip on miss for unqualified scan schemas, but ambiguous suffixes cannot
// choose a relation. Exception: constant-to-constant conjuncts pass unchanged
// as keys for the optimizer's 1 = 1 sentinel, preserving its cross product.
func parseJoinKeys(cond string) (leftKeys, rightKeys, residual []string) {
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return nil, nil, nil
	}
	expr := parseJoinCondExpr(cond)
	if expr == nil {
		// Unparseable. Refuse it: the lexical split used to invent column
		// names out of whatever text sat either side of an "=".
		return nil, nil, []string{cond}
	}
	for _, conj := range flattenJoinConjuncts(expr) {
		left, right, ok := joinKeyPair(conj)
		if !ok {
			residual = append(residual, conj.String())
			continue
		}
		leftKeys = append(leftKeys, left)
		rightKeys = append(rightKeys, right)
	}
	return leftKeys, rightKeys, residual
}

// parseJoinCondExpr parses a join condition into an expression AST. It borrows
// the WHERE slot of a dummy SELECT, the same trick logical.tryParseExpr uses.
func parseJoinCondExpr(cond string) plansql.Node {
	parsed, err := plansql.Parse("SELECT 1 FROM _dummy WHERE " + cond)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return nil
	}
	return info.WhereExpr
}

// flattenJoinConjuncts splits an ON expression on its top-level ANDs. Unlike
// the string split it replaces, it cannot be fooled by an " and " inside a
// string literal or under an OR.
func flattenJoinConjuncts(expr plansql.Node) []plansql.Node {
	switch e := expr.(type) {
	case *plansql.ParenNode:
		return flattenJoinConjuncts(e.Inner)
	case *plansql.AndNode:
		return append(flattenJoinConjuncts(e.Left), flattenJoinConjuncts(e.Right)...)
	}
	return []plansql.Node{expr}
}

// joinKeyPair reports the two key column names of one ON conjunct, and whether
// the conjunct is expressible as a key pair at all.
func joinKeyPair(conj plansql.Node) (left, right string, ok bool) {
	if p, isParen := conj.(*plansql.ParenNode); isParen {
		return joinKeyPair(p.Inner)
	}
	cmp, isCmp := conj.(*plansql.CmpExpr)
	if !isCmp || cmp.Op != "=" {
		return "", "", false
	}
	lName, lIsCol := joinKeyName(cmp.Left)
	rName, rIsCol := joinKeyName(cmp.Right)
	switch {
	case lIsCol && rIsCol:
		return lName, rName, true
	case !lIsCol && !rIsCol && lName != "" && rName != "":
		// Neither side names a column: the `1 = 1` ON-TRUE sentinel.
		return lName, rName, true
	}
	return "", "", false
}

// joinKeyName renders an ON operand the way the executor spells a column —
// qualifier kept, lowercased, which is what the text split produced. The bool
// reports whether the operand IS a bare column reference; a constant comes
// back with it false and its literal text as the name.
func joinKeyName(n plansql.Node) (string, bool) {
	switch e := n.(type) {
	case *plansql.ParenNode:
		return joinKeyName(e.Inner)
	case *plansql.ColRef:
		if e.Table != "" {
			return strings.ToLower(e.Table + "." + e.Column), true
		}
		return strings.ToLower(e.Column), true
	case *plansql.Lit:
		return strings.ToLower(e.String()), false
	}
	return "", false
}

// refuseJoinCond is the error a join whose ON clause the key representation
// cannot express. Both planning entry points raise it rather than let an
// unrepresentable conjunct reach the executor as a column name.
func refuseJoinCond(joinType, cond string, residual []string) error {
	return fmt.Errorf("join ON %q: %s cannot be represented as an equi-join key "+
		"(the %s join executor matches on column names, and only an equality between two "+
		"bare columns is one); it must be lifted into a filter above the join, which is legal "+
		"for an inner join only", cond, strings.Join(residual, ", "), joinType)
}

// joinArmAlias is the MATERIALIZED arm's enclosing-query identity, the only
// alias allowed to qualify that arm's duplicate columns. Base/derived tables
// use findScanAlias (setSubtreeAlias stamps scans); CTE references name their
// subtree root via CTEName/CTERefAlias, preserving inner relation identities.
// Single-process Project outputs use JoinArmAlias; ordinary DAG Projects do
// not run, so raw inner streams use BuildStreamAlias. Do not qualify a
// raw inner column as the arm's selected output (ADR-0025, #773, #706).
// See docs/internals/join-arm-aliases.md for the design.
func joinArmAlias(node *logical.Node) string {
	// The name is on the arm's SUBTREE ROOT — CTERefAlias for `FROM c AS x`,
	// CTEName for `FROM c`, DerivedAlias for `FROM (SELECT …) q` — and a pass
	// that wraps the arm (a pushed-down Filter, a Sort) leaves it one or more
	// single-child nodes down, so `NamedArmScope` descends to find it.
	if name := namedArmScope(node); name != "" {
		return name
	}
	// A base-table arm answers to its own alias, which the scan carries.
	return findScanAlias(node)
}

// buildStreamAlias is JoinArmAlias for the STAGE DAG, whose build stream
// is the arm's raw inner columns rather than its Project's output.
//
// A CTE reference still answers by name — `flattenCTEAliases` repoints the
// reference at the body's own producer, and a CTE's scope name is what every
// DAG resolver has spelled its columns by since #653 — and a DERIVED arm
// answers by the scan below it, because that is the name the inner join
// already qualified its duplicates with and therefore the name the stream
// really carries.
func buildStreamAlias(node *logical.Node) string {
	if node == nil {
		return ""
	}
	if node.CTERefAlias != "" {
		return node.CTERefAlias
	}
	if node.CTEName != "" {
		return node.CTEName
	}
	return findScanAlias(node)
}

// findScanAlias returns the table alias of the scan node in a subtree.
// Used to set BuildTableAlias for column disambiguation in self-joins.
func findScanAlias(node *logical.Node) string {
	if node == nil {
		return ""
	}
	if node.Type == logical.NodeScan {
		if node.TableAlias != "" {
			return node.TableAlias
		}
		return node.TableName
	}
	for _, child := range node.Children {
		if alias := findScanAlias(child); alias != "" {
			return alias
		}
	}
	return ""
}

// semiAntiBuildStoreCols returns the build-side columns a filtered semi/anti
// join must retain in stored build batches: the join keys (required to
// re-index spilled partitions and to survive FixKeyAssignment's rebuild)
// plus the JoinFilter's build-side columns. Returns nil when the filter is
// empty — unfiltered semi/anti builds are key-only and store nothing. Shared
// by the single-process planner and the worker fragment executor so both
// paths narrow their builds identically.
func semiAntiBuildStoreCols(rightKeys []string, joinFilter string) []string {
	if joinFilter == "" {
		return nil
	}
	filterCols := extractFilterBuildColumns(joinFilter)
	cols := make([]string, 0, len(rightKeys)+len(filterCols))
	seen := make(map[string]bool, len(rightKeys)+len(filterCols))
	for _, c := range rightKeys {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	for _, c := range filterCols {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	return cols
}

// extractFilterBuildColumns extracts the build-side column names from a
// semi/anti join filter string. Convention: right of operator = build column.
func extractFilterBuildColumns(filter string) []string {
	if filter == "" {
		return nil
	}
	seen := make(map[string]bool)
	parts := strings.Split(strings.ToLower(filter), " and ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		for _, op := range []string{"!=", ">=", "<=", "<>", ">", "<", "="} {
			sep := " " + op + " "
			idx := strings.Index(part, sep)
			if idx >= 0 {
				// Qualified, for BuildSemiAntiFilter's reason: the stored
				// build batch may carry the column under either spelling and
				// only the qualified one is unambiguous (#527). Both
				// consumers resolve with a bare-name fallback.
				right := strings.TrimSpace(part[idx+len(sep):])
				if right != "" {
					seen[right] = true
				}
				break
			}
		}
	}
	cols := make([]string, 0, len(seen))
	for c := range seen {
		cols = append(cols, c)
	}
	return cols
}

// refuseJoinResidual is the error an outer join whose ON residual cannot be
// evaluated at the join. It NAMES the construct rather than listing what the
// evaluator happens to support: the evaluator is the engine's own expression
// compiler now (#1153), so what remains unevaluable is a subquery in ON, a
// window function, or a function the compiler itself refuses — and err says
// which. Both planning entry points raise it rather than let the conjunct be
// dropped, which is the silent wrong answer this path exists to bury.
func refuseJoinResidual(filter, joinType string, err error) error {
	return fmt.Errorf("join ON residual %q on a %s join is not evaluable at the join: %w",
		filter, joinType, err)
}

// refuseStrandedJoinQualifier refuses a join whose condition QUALIFIES a
// column by a relation neither of its sides holds (#1299, ADR-0026 §8k).
//
// The executor matches keys on column names and strips a qualifier it cannot
// find, so `t.order_id = o.id` on a join of two copies of item — one where o
// is not below it at all — resolved `o.id` to whichever copy's `id` was
// there, and answered a wrong pairing with no error. The reorderer's edges now
// name the relations a conjunct reads (logical/join_edge_members.go), so no
// plan it builds hangs a conjunct where one is missing; this is the check that
// keeps a plan that does from answering. A qualifier is known to a side when
// any node below it answers to that name — a scan's table, alias or enclosing
// derived tables, a CTE reference, a derived table — compared without case, so
// the check can only refuse a name that is nowhere at all.
func refuseStrandedJoinQualifier(node *logical.Node) error {
	for _, cond := range []string{node.JoinCond, node.JoinFilter} {
		if strings.TrimSpace(cond) == "" {
			continue
		}
		expr := parseJoinCondExpr(cond)
		if expr == nil {
			continue
		}
		var stranded string
		walkJoinColRefs(expr, func(c *plansql.ColRef) {
			if stranded != "" || c.Table == "" {
				return
			}
			if !subtreeAnswersTo(node.Children[0], c.Table) &&
				!subtreeAnswersTo(node.Children[1], c.Table) {
				stranded = c.Table + "." + c.Column
			}
		})
		if stranded != "" {
			return fmt.Errorf("join ON %q: %s names a relation neither side of this join holds; "+
				"the join was placed where its condition cannot be evaluated (a planner defect, "+
				"refused rather than matched against another relation's column)", cond, stranded)
		}
	}
	return nil
}

// subtreeAnswersTo reports whether any node under n answers to the qualifier q.
func subtreeAnswersTo(n *logical.Node, q string) bool {
	if n == nil {
		return false
	}
	for _, name := range append(n.ScopeNames(), n.CTEName, n.CTERefAlias, n.DerivedAlias) {
		if name != "" && strings.EqualFold(name, q) {
			return true
		}
	}
	for _, c := range n.Children {
		if subtreeAnswersTo(c, q) {
			return true
		}
	}
	return false
}

// walkJoinColRefs visits every column reference of a join condition. A shape
// it does not descend into hides its references, which only means the check
// above does not see them.
func walkJoinColRefs(node plansql.Node, visit func(*plansql.ColRef)) {
	switch n := node.(type) {
	case *plansql.ColRef:
		visit(n)
	case *plansql.ParenNode:
		walkJoinColRefs(n.Inner, visit)
	case *plansql.NotNode:
		walkJoinColRefs(n.Inner, visit)
	case *plansql.UnaryOp:
		walkJoinColRefs(n.Inner, visit)
	case *plansql.AndNode:
		walkJoinColRefs(n.Left, visit)
		walkJoinColRefs(n.Right, visit)
	case *plansql.OrNode:
		walkJoinColRefs(n.Left, visit)
		walkJoinColRefs(n.Right, visit)
	case *plansql.BinaryOp:
		walkJoinColRefs(n.Left, visit)
		walkJoinColRefs(n.Right, visit)
	case *plansql.CmpExpr:
		walkJoinColRefs(n.Left, visit)
		walkJoinColRefs(n.Right, visit)
	case *plansql.IsExpr:
		walkJoinColRefs(n.Left, visit)
	case *plansql.LikeExpr:
		walkJoinColRefs(n.Left, visit)
		walkJoinColRefs(n.Pattern, visit)
	case *plansql.BetweenExpr:
		walkJoinColRefs(n.Left, visit)
		walkJoinColRefs(n.Low, visit)
		walkJoinColRefs(n.High, visit)
	case *plansql.InExpr:
		walkJoinColRefs(n.Left, visit)
		for _, v := range n.Values {
			walkJoinColRefs(v, visit)
		}
	case *plansql.CastNode:
		walkJoinColRefs(n.Inner, visit)
	case *plansql.FuncCallNode:
		for _, a := range n.Args {
			walkJoinColRefs(a, visit)
		}
	}
}
