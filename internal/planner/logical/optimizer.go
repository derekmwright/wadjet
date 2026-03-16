package logical

import (
	"strings"

	plansql "github.com/derekmwright/caelum/internal/planner/sql"
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
	if n.Type == NodeFilter && len(n.Children) == 1 {
		child := n.Children[0]
		if child.Type == NodeProject {
			// Filter-Project -> Project-Filter (push filter below project)
			n.Children[0] = child.Children[0]
			child.Children[0] = n
			return child
		}
		if child.Type == NodeJoin && len(child.Children) == 2 {
			return pushFilterThroughJoin(n, child)
		}
	}

	return n
}

// pushFilterThroughJoin decomposes a filter node above a join and pushes
// single-table predicates to the appropriate join child.
func pushFilterThroughJoin(filter, join *Node) *Node {
	flatPreds := flattenANDPredicates(filter.Predicates)
	if len(flatPreds) == 0 {
		return filter
	}

	leftTables, leftColMap := collectScanInfo(join.Children[0])
	rightTables, rightColMap := collectScanInfo(join.Children[1])

	// Merge column maps for resolving unqualified column references
	allColMap := make(map[string]string, len(leftColMap)+len(rightColMap))
	for k, v := range leftColMap {
		allColMap[k] = v
	}
	for k, v := range rightColMap {
		allColMap[k] = v
	}

	var leftPreds, rightPreds, remainingPreds []Predicate
	for _, pred := range flatPreds {
		refs := predicateTableRefs(pred, allColMap)
		if refs == nil || len(refs) == 0 {
			remainingPreds = append(remainingPreds, pred)
			continue
		}

		allLeft := true
		allRight := true
		for table := range refs {
			if !leftTables[table] {
				allLeft = false
			}
			if !rightTables[table] {
				allRight = false
			}
		}

		if allLeft {
			leftPreds = append(leftPreds, pred)
		} else if allRight {
			rightPreds = append(rightPreds, pred)
		} else {
			remainingPreds = append(remainingPreds, pred)
		}
	}

	if len(leftPreds) > 0 {
		join.Children[0] = NewFilter(join.Children[0], leftPreds)
		join.Children[0] = pushdownPredicates(join.Children[0])
	}
	if len(rightPreds) > 0 {
		join.Children[1] = NewFilter(join.Children[1], rightPreds)
		join.Children[1] = pushdownPredicates(join.Children[1])
	}

	if len(remainingPreds) == 0 {
		return join
	}

	filter.Predicates = remainingPreds
	filter.Children[0] = join
	return filter
}

// flattenANDPredicates splits compound AND predicates into individual predicates.
func flattenANDPredicates(preds []Predicate) []Predicate {
	var result []Predicate
	for _, pred := range preds {
		if pred.ASTExpr != nil {
			flattenASTAnd(pred.ASTExpr, &result)
		} else if pred.Raw != "" {
			upper := strings.ToUpper(pred.Raw)
			parts := splitOnAnd(pred.Raw, upper)
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part != "" {
					result = append(result, Predicate{Raw: part})
				}
			}
		}
	}
	return result
}

// flattenASTAnd recursively splits AND nodes into individual predicates.
func flattenASTAnd(expr plansql.Node, result *[]Predicate) {
	switch e := expr.(type) {
	case *plansql.AndNode:
		flattenASTAnd(e.Left, result)
		flattenASTAnd(e.Right, result)
	case *plansql.ParenNode:
		if _, ok := e.Inner.(*plansql.AndNode); ok {
			flattenASTAnd(e.Inner, result)
		} else {
			*result = append(*result, Predicate{ASTExpr: expr, Raw: expr.String()})
		}
	default:
		*result = append(*result, Predicate{ASTExpr: expr, Raw: expr.String()})
	}
}

// collectScanInfo returns table names/aliases and a column-to-table mapping from a subtree.
func collectScanInfo(n *Node) (tables map[string]bool, colToTable map[string]string) {
	tables = make(map[string]bool)
	colToTable = make(map[string]string)
	collectScanInfoRec(n, tables, colToTable)
	return
}

func collectScanInfoRec(n *Node, tables map[string]bool, colToTable map[string]string) {
	if n == nil {
		return
	}
	if n.Type == NodeScan {
		name := strings.ToLower(n.TableName)
		alias := strings.ToLower(n.TableAlias)
		if name != "" {
			tables[name] = true
		}
		if alias != "" && alias != name {
			tables[alias] = true
		}
		// Map column names to the table identifier (prefer alias)
		tableID := alias
		if tableID == "" {
			tableID = name
		}
		for _, col := range n.ScanColumns {
			colToTable[strings.ToLower(col)] = tableID
		}
	}
	for _, child := range n.Children {
		collectScanInfoRec(child, tables, colToTable)
	}
}

// predicateTableRefs returns the set of tables referenced by a predicate's column refs.
// Returns nil if the predicate can't be fully resolved (e.g., unqualified columns
// not found in ScanColumns, or no AST available).
func predicateTableRefs(pred Predicate, colToTable map[string]string) map[string]bool {
	if pred.ASTExpr == nil {
		return nil
	}
	refs := make(map[string]bool)
	resolved := true
	collectColTableRefs(pred.ASTExpr, refs, colToTable, &resolved)
	if !resolved {
		return nil
	}
	return refs
}

// collectColTableRefs walks an AST and collects the table names referenced by column refs.
func collectColTableRefs(expr plansql.Node, refs map[string]bool, colToTable map[string]string, resolved *bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *plansql.ColRef:
		if e.Table != "" {
			refs[strings.ToLower(e.Table)] = true
		} else {
			table, ok := colToTable[strings.ToLower(e.Column)]
			if ok {
				refs[table] = true
			} else {
				*resolved = false
			}
		}
	case *plansql.CmpExpr:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
		collectColTableRefs(e.Right, refs, colToTable, resolved)
	case *plansql.AndNode:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
		collectColTableRefs(e.Right, refs, colToTable, resolved)
	case *plansql.OrNode:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
		collectColTableRefs(e.Right, refs, colToTable, resolved)
	case *plansql.NotNode:
		collectColTableRefs(e.Inner, refs, colToTable, resolved)
	case *plansql.ParenNode:
		collectColTableRefs(e.Inner, refs, colToTable, resolved)
	case *plansql.BinaryOp:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
		collectColTableRefs(e.Right, refs, colToTable, resolved)
	case *plansql.UnaryOp:
		collectColTableRefs(e.Inner, refs, colToTable, resolved)
	case *plansql.InExpr:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
		for _, v := range e.Values {
			collectColTableRefs(v, refs, colToTable, resolved)
		}
	case *plansql.BetweenExpr:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
		collectColTableRefs(e.Low, refs, colToTable, resolved)
		collectColTableRefs(e.High, refs, colToTable, resolved)
	case *plansql.LikeExpr:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
		collectColTableRefs(e.Pattern, refs, colToTable, resolved)
	case *plansql.IsExpr:
		collectColTableRefs(e.Left, refs, colToTable, resolved)
	case *plansql.FuncCallNode:
		for _, arg := range e.Args {
			collectColTableRefs(arg, refs, colToTable, resolved)
		}
	case *plansql.CastNode:
		collectColTableRefs(e.Inner, refs, colToTable, resolved)
	case *plansql.CaseNode:
		if e.Subject != nil {
			collectColTableRefs(e.Subject, refs, colToTable, resolved)
		}
		for _, w := range e.Whens {
			collectColTableRefs(w.Cond, refs, colToTable, resolved)
			collectColTableRefs(w.Result, refs, colToTable, resolved)
		}
		if e.Else != nil {
			collectColTableRefs(e.Else, refs, colToTable, resolved)
		}
	case *plansql.Lit:
		// No column refs
	}
}

// extractPartitionFilters finds Filter nodes above Scan nodes and extracts
// equality predicates on partition key columns (year, month, day, hour).
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

// findDescendantScan walks through passthrough nodes to find the nearest Scan node.
func findDescendantScan(n *Node) *Node {
	if n == nil {
		return nil
	}
	if n.Type == NodeScan {
		return n
	}
	if n.Type == NodeFilter || n.Type == NodeProject {
		if len(n.Children) > 0 {
			return findDescendantScan(n.Children[0])
		}
	}
	return nil
}

// extractPartitionEqualities extracts equality predicates on partition keys.
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

// extractFromAST walks our AST expression to find column = literal patterns
// on partition key columns.
func extractFromAST(expr plansql.Node, result map[string]string) {
	switch e := expr.(type) {
	case *plansql.CmpExpr:
		if e.Op != "=" {
			return
		}
		col, val := extractColLiteral(e.Left, e.Right)
		if col != "" && partitionKeys[col] {
			result[col] = val
		}
	case *plansql.AndNode:
		extractFromAST(e.Left, result)
		extractFromAST(e.Right, result)
	case *plansql.ParenNode:
		extractFromAST(e.Inner, result)
	}
}

// extractColLiteral extracts column name and literal value from a comparison's sides.
func extractColLiteral(left, right plansql.Node) (col, val string) {
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

func exprColName(e plansql.Node) string {
	if n, ok := e.(*plansql.ColRef); ok {
		name := n.Column
		// Strip table qualifier
		if parts := strings.SplitN(name, ".", 2); len(parts) == 2 {
			return strings.ToLower(parts[1])
		}
		return strings.ToLower(name)
	}
	return ""
}

func exprLiteral(e plansql.Node) string {
	if n, ok := e.(*plansql.Lit); ok {
		return n.Value
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

// pruneProjections removes unnecessary projections.
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
