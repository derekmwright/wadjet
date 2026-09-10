// This file holds join keys for the physical planner, governed by ADR-0023 and ADR-0026.
package physical

import (
	"fmt"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"strings"
)

// parseJoinKeys reads a join condition STRUCTURALLY and returns the equi-join
// key columns, plus every conjunct it cannot represent as a key pair.
//
// The join executor takes a condition as two parallel lists of COLUMN NAMES,
// so the only ON conjunct it can express is an equality between two bare
// column references. Anything else — an expression operand
// (`r.r_regionkey + 3`), a literal operand (`n.n_regionkey = 1`), a non-equi
// operator, a disjunction — comes back in residual, and the caller refuses the
// plan rather than handing the executor a name that is not a column.
//
// This used to split the TEXT on " and " and then on the first "=", passing
// whatever fell either side through as a column name. An unresolvable name
// resolves to index -1 in the executor, which hashes as a constant, so the two
// failure modes were a join that matched NOTHING (one side a real column, the
// other not: `n.n_regionkey = r.r_regionkey + 3` answered 0 for a 10-row
// query) and a join that matched EVERYTHING (neither side real: a silent cross
// product). `a.x <= b.y` split on its own "=" and produced the column name
// "a.x <" — the splitting was lexical where the condition is structural
// (#351). Both are the shape this codebase keeps relearning: a key that does
// not resolve must error or fall back, never silently match nothing.
//
// Table qualifiers are preserved ("n1.n_regionkey") so that probe-side lookups
// against a self-join chain's qualified output schema resolve directly. The
// columnIndexFallback in the join executor strips the qualifier on miss, so
// unqualified scan-source schemas still resolve. Stripping here would force
// the executor to suffix-match a qualified column from {n1.X, n2.X}, which is
// ambiguous and returns -1 → 0 rows from the join.
//
// A conjunct comparing two CONSTANTS is passed through as a key pair
// unchanged. That is the optimizer's `1 = 1` sentinel, written into JoinCond
// when every ON conjunct has been pushed to a child (optimizer.go,
// extractJoinCondPredicates): it means ON TRUE, and a constant on both sides
// puts every row in one hash bucket, which is the cross product it asks for.
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

// joinArmAlias is the name the ENCLOSING QUERY calls a join arm — which is
// what a qualified reference above the join is written against, and therefore
// the only alias a join may qualify that arm's duplicate columns with.
//
// For a base table and for a derived table it is `findScanAlias`, because
// `BuildFromTable`'s `setSubtreeAlias` stamps a derived alias onto every scan
// below it. A CTE reference records its name on the SUBTREE ROOT instead
// (`Node.CTEName`, plus `Node.CTERefAlias` for the name one reference gives it
// in `FROM c AS x`) — deliberately, so two relations comma-joined inside the
// CTE body keep separate identities (see subtreeNamesRelation) — and reading
// only the scan below it returned the CTE's underlying TABLE:
//
//	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
//	SELECT x.id AS xid, c.dv AS cdv, p.dv AS pdv
//	FROM (SELECT id, b - 100 AS dv FROM decpair) p
//	JOIN decpair x ON p.id = x.id JOIN c ON c.id = p.id
//	JOIN decpair y ON c.id = y.id ORDER BY x.id
//	-- PostgreSQL cdv 25.50, pdv -87.2500 (two different columns)
//	-- before: `c.dv` answered p's -87.2500 on every arm
//
// The join qualified c's column as `decpair.dv` while p's stayed bare, so
// `c.dv` matched neither spelling exactly, fell through to the resolver's
// qualifier strip, and bound the SIBLING arm's bare `dv`. Naming the arm `c`
// makes the exact match the one that wins, and leaves p's `p.dv` on the
// bare-strip path it already took.
// It has TWO answers, because the two engines hand the join two different
// STREAMS and a name describes a stream.
//
// On the single-process pipeline the arm's own Project is a real operator: the
// build side the join receives is the arm's OUTPUT — `id`, `w` — and no inner
// relation's columns are in it at all, so the ONE name the enclosing query
// writes is the only name those columns can answer to.
//
// On the stage DAG a Project emits NO STAGE (ADR-0025), so the stream the join
// receives is the arm's RAW inner columns — `d92` and `j.d92`, one per
// relation inside it — and the arm's name describes none of them: which of the
// two the arm publishes is exactly what the un-materialized Project knows and
// the stage does not. Qualifying them by the arm there put `m.d92` on the
// column the arm did NOT select, and every consumer read the wrong one.
//
// So `joinArmAlias` is the MATERIALIZED answer and `stageBuildTableAlias` is
// the raw one, and each engine's resolvers use its own — which is what makes
// the declaration and the value agree on each path (#773, #706 round 2).
func joinArmAlias(node *logical.Node) string {
	// The name is on the arm's SUBTREE ROOT — CTERefAlias for `FROM c AS x`,
	// CTEName for `FROM c`, DerivedAlias for `FROM (SELECT …) q` — and a pass
	// that wraps the arm (a pushed-down Filter, a Sort) leaves it one or more
	// single-child nodes down, so `namedArmScope` descends to find it.
	if name := namedArmScope(node); name != "" {
		return name
	}
	// A base-table arm answers to its own alias, which the scan carries.
	return findScanAlias(node)
}

// stageBuildTableAlias is joinArmAlias for the STAGE DAG, whose build stream
// is the arm's raw inner columns rather than its Project's output.
//
// A CTE reference still answers by name — `flattenCTEAliases` repoints the
// reference at the body's own producer, and a CTE's scope name is what every
// DAG resolver has spelled its columns by since #653 — and a DERIVED arm
// answers by the scan below it, because that is the name the inner join
// already qualified its duplicates with and therefore the name the stream
// really carries.
func stageBuildTableAlias(node *logical.Node) string {
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

// SemiAntiBuildStoreCols returns the build-side columns a filtered semi/anti
// join must retain in stored build batches: the join keys (required to
// re-index spilled partitions and to survive FixKeyAssignment's rebuild)
// plus the JoinFilter's build-side columns. Returns nil when the filter is
// empty — unfiltered semi/anti builds are key-only and store nothing. Shared
// by the single-process planner and the worker fragment executor so both
// paths narrow their builds identically.
func SemiAntiBuildStoreCols(rightKeys []string, joinFilter string) []string {
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
