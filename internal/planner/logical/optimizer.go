package logical

import (
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

// partitionKeys are the standard Hive-style partition key columns.
var partitionKeys = map[string]bool{
	"year": true, "month": true, "day": true, "hour": true,
}

// Optimize applies logical optimizations to the plan tree.
func Optimize(plan *Node) *Node {
	plan = pushdownPredicates(plan)
	plan = extractPartitionFilters(plan)
	plan = pruneProjections(plan)
	return plan
}

// pushdownPredicates pushes filter predicates closer to scan nodes.
func pushdownPredicates(n *Node) *Node {
	if n == nil {
		return nil
	}

	// Recursively optimize children first
	for i, child := range n.Children {
		n.Children[i] = pushdownPredicates(child)
	}

	// If this is a Filter above a Scan, keep it (already at leaf)
	// If this is a Filter above a Project, swap them
	if n.Type == NodeFilter && len(n.Children) == 1 {
		child := n.Children[0]
		if child.Type == NodeProject {
			// Filter-Project -> Project-Filter (push filter below project)
			n.Children[0] = child.Children[0]
			child.Children[0] = n
			return child
		}
	}

	return n
}

// extractPartitionFilters finds Filter nodes above Scan nodes and extracts
// equality predicates on partition key columns (year, month, day, hour).
// These are propagated to the Scan node's PartitionFilter for partition pruning.
func extractPartitionFilters(n *Node) *Node {
	if n == nil {
		return nil
	}

	for i, child := range n.Children {
		n.Children[i] = extractPartitionFilters(child)
	}

	if n.Type == NodeFilter && len(n.Children) == 1 {
		scan := findDescendantScan(n.Children[0])
		if scan != nil {
			extracted := extractPartitionEqualities(n)
			if len(extracted) > 0 {
				if scan.PartitionFilter == nil {
					scan.PartitionFilter = make(map[string]string)
				}
				for k, v := range extracted {
					scan.PartitionFilter[k] = v
				}
			}
		}
	}

	return n
}

// findDescendantScan walks through passthrough nodes (Filter, Project) to find
// the nearest Scan node.
func findDescendantScan(n *Node) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeScan {
		return n
	}
	// Walk through passthrough nodes
	if n.Type == NodeFilter || n.Type == NodeProject {
		if len(n.Children) > 0 {
			return findDescendantScan(n.Children[0])
		}
	}
	return nil
}

// extractPartitionEqualities extracts equality predicates on partition keys
// from a Filter node's predicates. It handles both AST expressions and raw strings.
func extractPartitionEqualities(filterNode *Node) map[string]string {
	result := make(map[string]string)

	for _, pred := range filterNode.Predicates {
		// Try AST-based extraction first
		if pred.ASTExpr != nil {
			extractFromAST(pred.ASTExpr, result)
			continue
		}

		// Fall back to raw string parsing
		if pred.Raw != "" {
			extractFromRaw(pred.Raw, result)
		}
	}

	return result
}

// extractFromAST walks a vitess AST expression to find column = literal patterns
// on partition key columns.
func extractFromAST(expr sqlparser.Expr, result map[string]string) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if e.Operator != "=" {
			return
		}
		col, val := extractColLiteral(e.Left, e.Right)
		if col != "" && partitionKeys[col] {
			result[col] = val
		}
	case *sqlparser.AndExpr:
		extractFromAST(e.Left, result)
		extractFromAST(e.Right, result)
	case *sqlparser.ParenExpr:
		extractFromAST(e.Expr, result)
	}
}

// extractColLiteral extracts column name and literal value from a comparison's sides.
func extractColLiteral(left, right sqlparser.Expr) (col, val string) {
	colName := exprColName(left)
	litVal := exprLiteral(right)
	if colName != "" && litVal != "" {
		return colName, litVal
	}
	// Try reversed
	colName = exprColName(right)
	litVal = exprLiteral(left)
	if colName != "" && litVal != "" {
		return colName, litVal
	}
	return "", ""
}

func exprColName(e sqlparser.Expr) string {
	switch n := e.(type) {
	case *sqlparser.ColName:
		name := n.Name.String()
		// Strip table qualifier
		if parts := strings.SplitN(name, ".", 2); len(parts) == 2 {
			return strings.ToLower(parts[1])
		}
		return strings.ToLower(name)
	}
	return ""
}

func exprLiteral(e sqlparser.Expr) string {
	switch n := e.(type) {
	case *sqlparser.SQLVal:
		val := string(n.Val)
		return val
	}
	return ""
}

// extractFromRaw parses "col = value" patterns from raw predicate strings.
func extractFromRaw(raw string, result map[string]string) {
	// Handle AND-joined predicates
	upper := strings.ToUpper(raw)
	parts := splitOnAnd(raw, upper)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		eqParts := strings.SplitN(part, "=", 2)
		if len(eqParts) != 2 {
			continue
		}
		// Make sure it's not >=, <=, !=
		left := strings.TrimSpace(eqParts[0])
		if len(left) > 0 && (left[len(left)-1] == '>' || left[len(left)-1] == '<' || left[len(left)-1] == '!') {
			continue
		}
		col := strings.ToLower(strings.TrimSpace(left))
		// Remove table qualifier
		if dotParts := strings.SplitN(col, ".", 2); len(dotParts) == 2 {
			col = dotParts[1]
		}
		if !partitionKeys[col] {
			continue
		}
		val := strings.TrimSpace(eqParts[1])
		val = strings.Trim(val, "'\"")
		result[col] = val
	}
}

// splitOnAnd splits a raw expression on " AND " boundaries.
func splitOnAnd(raw, upper string) []string {
	var parts []string
	for {
		idx := strings.Index(upper, " AND ")
		if idx < 0 {
			parts = append(parts, raw)
			break
		}
		parts = append(parts, raw[:idx])
		raw = raw[idx+5:]
		upper = upper[idx+5:]
	}
	return parts
}

// pruneProjections removes unnecessary projections (e.g., SELECT * passthrough).
func pruneProjections(n *Node) *Node {
	if n == nil {
		return nil
	}

	for i, child := range n.Children {
		n.Children[i] = pruneProjections(child)
	}

	// Remove identity projections (Project that just passes through all columns)
	if n.Type == NodeProject && len(n.Projections) == 0 && len(n.Children) == 1 {
		return n.Children[0]
	}

	return n
}
