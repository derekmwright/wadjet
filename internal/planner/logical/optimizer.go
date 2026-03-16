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
	plan = decorrelateExists(plan)
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

// decorrelateExists converts correlated EXISTS/NOT EXISTS subqueries in Filter
// predicates into SemiJoin/AntiJoin nodes. This eliminates per-row subquery
// execution by materializing the subquery as a hash join build side.
func decorrelateExists(n *Node) *Node {
	if n == nil {
		return nil
	}

	// Recursively process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = decorrelateExists(child)
	}

	if n.Type != NodeFilter || len(n.Children) == 0 {
		return n
	}

	// Collect outer tables from the subtree below this filter
	outerTables := make(map[string]bool)
	collectTableNames(n.Children[0], outerTables)
	if len(outerTables) == 0 {
		return n
	}

	// Collect column-to-table mapping for resolving unqualified column references
	_, outerColMap := collectScanInfo(n.Children[0])

	// Flatten AND predicates so each EXISTS is a separate predicate
	flatPreds := flattenANDPredicates(n.Predicates)

	var remainingPreds []Predicate
	currentPlan := n.Children[0]

	for _, pred := range flatPreds {
		existsNode := findExistsNode(pred.ASTExpr)
		if existsNode == nil {
			remainingPreds = append(remainingPreds, pred)
			continue
		}

		joinNode := tryDecorrelateExists(existsNode, outerTables, outerColMap)
		if joinNode == nil {
			remainingPreds = append(remainingPreds, pred)
			continue
		}

		// Wire the current plan as the left (probe) child
		joinNode.Children[0] = currentPlan
		currentPlan = joinNode
	}

	if len(remainingPreds) == 0 {
		return currentPlan
	}

	n.Children[0] = currentPlan
	n.Predicates = remainingPreds
	return n
}

// tryDecorrelateExists attempts to convert an EXISTS/NOT EXISTS subquery
// into a SemiJoin/AntiJoin node. Returns nil if decorrelation is not possible.
// The returned join node has a nil left child (to be filled by the caller).
func tryDecorrelateExists(exists *plansql.ExistsNode, outerTables map[string]bool, outerColMap map[string]string) *Node {
	parsed, err := plansql.Parse(exists.SQL)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil
	}

	// Only handle single-table subqueries (no JOINs)
	if len(info.Tables) != 1 || len(info.Joins) > 0 {
		return nil
	}

	// Check for correlated references
	refs, err := plansql.FindCorrelatedRefs(exists.SQL, outerTables)
	if err != nil || len(refs) == 0 {
		return nil // uncorrelated, keep as-is
	}

	if info.WhereExpr == nil {
		return nil
	}

	innerTable := info.Tables[0]
	innerTables := map[string]bool{
		strings.ToLower(innerTable.Name): true,
	}
	if innerTable.Alias != "" {
		innerTables[strings.ToLower(innerTable.Alias)] = true
	}

	// Flatten the subquery WHERE into individual conditions
	var whereNodes []plansql.Node
	flattenASTNodes(info.WhereExpr, &whereNodes)

	// Classify each condition
	var eqKeys []string     // equality: "outer_col = inner_col" for hash join keys
	var filterConds []string // non-equality correlated: "outer_col != inner_col"
	var innerFilterNodes []plansql.Node // inner-only conditions for scan filter

	for _, node := range whereNodes {
		hasOuter, hasInner := nodeTableRefs(node, outerTables, innerTables, outerColMap)

		if hasOuter && hasInner {
			// Cross-table predicate: must be a simple comparison
			cmp, ok := node.(*plansql.CmpExpr)
			if !ok {
				return nil // can't decorrelate complex cross-table predicates
			}
			outerCol, innerCol, ok := extractCorrelatedCols(cmp, outerTables, innerTables, outerColMap)
			if !ok {
				return nil
			}
			if cmp.Op == "=" {
				eqKeys = append(eqKeys, outerCol+" = "+innerCol)
			} else {
				filterConds = append(filterConds, outerCol+" "+cmp.Op+" "+innerCol)
			}
		} else if hasInner {
			innerFilterNodes = append(innerFilterNodes, node)
		}
		// Outer-only conditions in subquery WHERE shouldn't happen, skip
	}

	if len(eqKeys) == 0 {
		return nil // no equality keys → can't use hash join
	}

	// Build inner scan node
	innerScan := NewScan(innerTable.Name, innerTable.Alias)

	// Apply inner-only filters to the scan
	var innerPlan *Node = innerScan
	if len(innerFilterNodes) > 0 {
		var innerPreds []Predicate
		for _, f := range innerFilterNodes {
			stripped := stripTableQualifiers(f)
			innerPreds = append(innerPreds, Predicate{
				Raw:     stripped.String(),
				ASTExpr: stripped,
			})
		}
		innerPlan = NewFilter(innerScan, innerPreds)
	}

	// Build semi/anti join
	joinType := "semi"
	if exists.Not {
		joinType = "anti"
	}

	joinCond := strings.Join(eqKeys, " AND ")
	joinNode := &Node{
		Type:     NodeJoin,
		Children: []*Node{nil, innerPlan}, // left child filled by caller
		JoinType: joinType,
		JoinCond: joinCond,
	}
	if len(filterConds) > 0 {
		joinNode.JoinFilter = strings.Join(filterConds, " AND ")
	}

	return joinNode
}

// findExistsNode checks if a predicate AST node is an EXISTS/NOT EXISTS.
func findExistsNode(node plansql.Node) *plansql.ExistsNode {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *plansql.ExistsNode:
		return n
	case *plansql.ParenNode:
		return findExistsNode(n.Inner)
	default:
		return nil
	}
}

// flattenASTNodes splits an AND tree into individual nodes.
func flattenASTNodes(node plansql.Node, result *[]plansql.Node) {
	if node == nil {
		return
	}
	switch e := node.(type) {
	case *plansql.AndNode:
		flattenASTNodes(e.Left, result)
		flattenASTNodes(e.Right, result)
	default:
		*result = append(*result, node)
	}
}

// nodeTableRefs checks if an AST node references outer and/or inner tables.
// outerColMap maps unqualified column names to their source table, enabling
// resolution of unqualified column references (e.g., TPC-H style l_orderkey).
func nodeTableRefs(node plansql.Node, outerTables, innerTables map[string]bool, outerColMap map[string]string) (hasOuter, hasInner bool) {
	if node == nil {
		return
	}
	switch e := node.(type) {
	case *plansql.ColRef:
		if e.Table != "" {
			tbl := strings.ToLower(e.Table)
			if outerTables[tbl] && !innerTables[tbl] {
				hasOuter = true
			}
			if innerTables[tbl] {
				hasInner = true
			}
		} else if outerColMap != nil {
			// Unqualified column: check outer column map
			col := strings.ToLower(e.Column)
			if _, ok := outerColMap[col]; ok {
				hasOuter = true
			} else {
				hasInner = true
			}
		}
	case *plansql.CmpExpr:
		lo, li := nodeTableRefs(e.Left, outerTables, innerTables, outerColMap)
		ro, ri := nodeTableRefs(e.Right, outerTables, innerTables, outerColMap)
		hasOuter = lo || ro
		hasInner = li || ri
	case *plansql.BinaryOp:
		lo, li := nodeTableRefs(e.Left, outerTables, innerTables, outerColMap)
		ro, ri := nodeTableRefs(e.Right, outerTables, innerTables, outerColMap)
		hasOuter = lo || ro
		hasInner = li || ri
	case *plansql.AndNode:
		lo, li := nodeTableRefs(e.Left, outerTables, innerTables, outerColMap)
		ro, ri := nodeTableRefs(e.Right, outerTables, innerTables, outerColMap)
		hasOuter = lo || ro
		hasInner = li || ri
	case *plansql.ParenNode:
		hasOuter, hasInner = nodeTableRefs(e.Inner, outerTables, innerTables, outerColMap)
	case *plansql.NotNode:
		hasOuter, hasInner = nodeTableRefs(e.Inner, outerTables, innerTables, outerColMap)
	case *plansql.FuncCallNode:
		for _, arg := range e.Args {
			o, i := nodeTableRefs(arg, outerTables, innerTables, outerColMap)
			hasOuter = hasOuter || o
			hasInner = hasInner || i
		}
	}
	return
}

// extractCorrelatedCols extracts the outer and inner column names from a
// comparison expression between outer and inner tables.
// Returns the unqualified column names (outer, inner) and ok=true if extraction succeeded.
func extractCorrelatedCols(cmp *plansql.CmpExpr, outerTables, innerTables map[string]bool, outerColMap map[string]string) (outerCol, innerCol string, ok bool) {
	leftCol, leftIsOuter := getColRefInfo(cmp.Left, outerTables, innerTables, outerColMap)
	rightCol, rightIsOuter := getColRefInfo(cmp.Right, outerTables, innerTables, outerColMap)
	if leftCol == "" || rightCol == "" {
		return "", "", false
	}
	if leftIsOuter {
		return leftCol, rightCol, true
	}
	if rightIsOuter {
		return rightCol, leftCol, true
	}
	return "", "", false
}

// getColRefInfo returns the unqualified column name and whether it's an outer reference.
// outerColMap enables resolution of unqualified column references.
func getColRefInfo(node plansql.Node, outerTables, innerTables map[string]bool, outerColMap map[string]string) (col string, isOuter bool) {
	ref, ok := node.(*plansql.ColRef)
	if !ok {
		return "", false
	}
	if ref.Table == "" {
		// Unqualified column: resolve using outer column map
		if outerColMap != nil {
			c := strings.ToLower(ref.Column)
			if _, ok := outerColMap[c]; ok {
				return ref.Column, true // outer column
			}
		}
		return ref.Column, false // assume inner
	}
	tbl := strings.ToLower(ref.Table)
	if outerTables[tbl] && !innerTables[tbl] {
		return ref.Column, true
	}
	if innerTables[tbl] {
		return ref.Column, false
	}
	return "", false
}

// collectTableNames collects all table names and aliases from scan nodes in a subtree.
func collectTableNames(n *Node, tables map[string]bool) {
	if n == nil {
		return
	}
	if n.Type == NodeScan {
		if n.TableName != "" {
			tables[strings.ToLower(n.TableName)] = true
		}
		if n.TableAlias != "" {
			tables[strings.ToLower(n.TableAlias)] = true
		}
	}
	for _, child := range n.Children {
		collectTableNames(child, tables)
	}
}

// stripTableQualifiers removes table qualifiers from ColRef nodes in an AST.
func stripTableQualifiers(node plansql.Node) plansql.Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		return &plansql.ColRef{Column: n.Column}
	case *plansql.CmpExpr:
		return &plansql.CmpExpr{
			Left:  stripTableQualifiers(n.Left),
			Op:    n.Op,
			Right: stripTableQualifiers(n.Right),
		}
	case *plansql.AndNode:
		return &plansql.AndNode{
			Left:  stripTableQualifiers(n.Left),
			Right: stripTableQualifiers(n.Right),
		}
	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  stripTableQualifiers(n.Left),
			Op:    n.Op,
			Right: stripTableQualifiers(n.Right),
		}
	case *plansql.ParenNode:
		return &plansql.ParenNode{Inner: stripTableQualifiers(n.Inner)}
	case *plansql.NotNode:
		return &plansql.NotNode{Inner: stripTableQualifiers(n.Inner)}
	case *plansql.FuncCallNode:
		newArgs := make([]plansql.Node, len(n.Args))
		for i, a := range n.Args {
			newArgs[i] = stripTableQualifiers(a)
		}
		return &plansql.FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	default:
		return node
	}
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
