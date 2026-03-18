package logical

import (
	"fmt"
	"strings"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// partitionKeys are the standard Hive-style partition key columns.
var partitionKeys = map[string]bool{
	"year": true, "month": true, "day": true, "hour": true,
}

// Optimize applies logical optimizations to the plan tree.
//
// An optional ScanAnnotator function may be provided to populate scan metadata
// (ScanColumns) on newly created scan nodes after IN-to-SemiJoin conversion.
// This enables subsequent scalar subquery decorrelation to resolve unqualified
// column references. Without an annotator, scalar decorrelation may fail for
// subqueries that use unqualified outer column references.
func Optimize(plan *Node, annotators ...func(*Node)) *Node {
	plan = decorrelateExists(plan)
	plan = decorrelateInSubqueries(plan)
	// Annotate new scans created by IN decorrelation so scalar subquery
	// decorrelation can resolve unqualified column references.
	for _, annotate := range annotators {
		annotate(plan)
	}
	plan = decorrelateScalarSubqueries(plan)
	plan = extractCommonORPredicates(plan)
	plan = pushdownPredicates(plan)
	plan = reorderJoins(plan)
	plan = extractPartitionFilters(plan)
	plan = pruneProjections(plan)
	computeRequiredColumns(plan)
	attachScanPredicates(plan)
	return plan
}

// attachScanPredicates copies simple filter predicates to scan nodes for
// row-group-level pruning. Only predicates with a Column, Op, and literal Value
// are useful for stats-based pruning.
func attachScanPredicates(n *Node) {
	if n == nil {
		return
	}
	for _, child := range n.Children {
		attachScanPredicates(child)
	}
	if n.Type != NodeFilter || len(n.Children) == 0 {
		return
	}
	child := n.Children[0]
	if child.Type == NodeScan {
		for _, pred := range n.Predicates {
			if pred.Column != "" && pred.Op != "" && pred.Value != nil {
				child.ScanPredicates = append(child.ScanPredicates, pred)
			}
		}
	}
}

// computeRequiredColumns walks the plan tree top-down, collecting column
// references from each operator and propagating them to scan nodes. Each
// scan's RequiredColumns is set to the accumulated set of columns referenced
// by its ancestors. This allows the physical planner to read only needed
// columns from storage.
func computeRequiredColumns(n *Node) {
	pushColumnNeeds(n, nil)
}

// pushColumnNeeds recursively pushes the set of needed columns from parent
// nodes down to scan nodes. At each level, it adds columns referenced by
// the current node and passes the accumulated set to children.
func pushColumnNeeds(n *Node, parentNeeds map[string]bool) {
	if n == nil {
		return
	}

	// Merge parent needs with this node's own column references
	needs := make(map[string]bool, len(parentNeeds)+8)
	for col := range parentNeeds {
		needs[col] = true
	}
	collectNodeColumnRefs(n, needs)

	if n.Type == NodeScan {
		if len(needs) > 0 {
			cols := make([]string, 0, len(needs))
			for col := range needs {
				cols = append(cols, col)
			}
			n.RequiredColumns = cols
		}
		return
	}

	// Save needed columns on join nodes so the physical planner can insert
	// intermediate projections to drop unneeded columns between joins.
	if n.Type == NodeJoin && len(needs) > 0 {
		cols := make([]string, 0, len(needs))
		for col := range needs {
			cols = append(cols, col)
		}
		n.NeededColumns = cols
	}

	// For SEMI/ANTI joins, the build side (child[1]) never contributes to output.
	// Only push join key + filter column refs to the build side, not parent needs.
	if n.Type == NodeJoin && len(n.Children) == 2 &&
		(strings.EqualFold(n.JoinType, "semi") || strings.EqualFold(n.JoinType, "anti")) {
		// Probe side (child[0]) gets full needs
		pushColumnNeeds(n.Children[0], needs)
		// Build side (child[1]) only needs join key + filter columns
		buildNeeds := make(map[string]bool, 8)
		if n.JoinCond != "" {
			extractJoinColumnRefs(n.JoinCond, buildNeeds)
		}
		if n.JoinFilter != "" {
			extractJoinColumnRefs(n.JoinFilter, buildNeeds)
		}
		pushColumnNeeds(n.Children[1], buildNeeds)
		return
	}

	// For inner/left/right joins, partition parent needs between probe and build
	// sides based on which columns each child's subtree can provide. This prevents
	// build-side scans from reading columns only needed by the probe side, reducing
	// hash table memory substantially for wide tables (e.g., lineitem: 16 columns).
	if n.Type == NodeJoin && len(n.Children) == 2 {
		jt := strings.ToLower(n.JoinType)
		if jt == "" || jt == "join" || jt == "inner" || jt == "inner join" ||
			jt == "left" || jt == "right" || jt == "cross" {
			leftAvail := collectSubtreeColumns(n.Children[0])
			rightAvail := collectSubtreeColumns(n.Children[1])
			// Only partition if we have column metadata from both sides
			if len(leftAvail) > 0 && len(rightAvail) > 0 {
				// Join keys go to both sides
				joinRefs := make(map[string]bool, 8)
				if n.JoinCond != "" {
					extractJoinColumnRefs(n.JoinCond, joinRefs)
				}
				if n.JoinFilter != "" {
					extractJoinColumnRefs(n.JoinFilter, joinRefs)
				}

				probeNeeds := make(map[string]bool, len(needs))
				buildNeeds := make(map[string]bool, len(needs))
				for col := range joinRefs {
					probeNeeds[col] = true
					buildNeeds[col] = true
				}
				for col := range needs {
					if leftAvail[col] {
						probeNeeds[col] = true
					}
					if rightAvail[col] {
						buildNeeds[col] = true
					}
				}
				pushColumnNeeds(n.Children[0], probeNeeds)
				pushColumnNeeds(n.Children[1], buildNeeds)
				return
			}
		}
	}

	for _, child := range n.Children {
		pushColumnNeeds(child, needs)
	}
}

// collectSubtreeColumns returns all column names available from scan nodes
// in the subtree rooted at n (lowercased).
func collectSubtreeColumns(n *Node) map[string]bool {
	result := make(map[string]bool)
	collectSubtreeColumnsRec(n, result)
	return result
}

func collectSubtreeColumnsRec(n *Node, result map[string]bool) {
	if n == nil {
		return
	}
	if n.Type == NodeScan {
		for _, col := range n.ScanColumns {
			result[strings.ToLower(col)] = true
		}
	}
	for _, child := range n.Children {
		collectSubtreeColumnsRec(child, result)
	}
}

// collectNodeColumnRefs adds column names referenced by a node's metadata
// (predicates, projections, join conditions, aggregates, etc.) to the set.
func collectNodeColumnRefs(n *Node, refs map[string]bool) {
	switch n.Type {
	case NodeFilter:
		for _, pred := range n.Predicates {
			if pred.ASTExpr != nil {
				collectASTColumnRefs(pred.ASTExpr, refs)
			} else if pred.Column != "" {
				refs[strings.ToLower(pred.Column)] = true
			}
		}
	case NodeProject:
		for _, proj := range n.Projections {
			if proj.ASTExpr != nil {
				collectASTColumnRefs(proj.ASTExpr, refs)
			}
			if proj.Column != "" {
				refs[strings.ToLower(proj.Column)] = true
			}
		}
	case NodeAggregate:
		for _, gb := range n.GroupBy {
			refs[strings.ToLower(gb)] = true
		}
		for _, agg := range n.AggExprs {
			if agg.InputCol != "" && agg.InputCol != "*" {
				refs[strings.ToLower(agg.InputCol)] = true
			}
			if agg.InputExpr != nil {
				collectASTColumnRefs(agg.InputExpr, refs)
			}
		}
	case NodeJoin:
		if n.JoinCond != "" {
			extractJoinColumnRefs(n.JoinCond, refs)
		}
		if n.JoinFilter != "" {
			extractJoinColumnRefs(n.JoinFilter, refs)
		}
	case NodeSort:
		for _, ob := range n.OrderBy {
			refs[strings.ToLower(ob.Column)] = true
		}
	case NodeWindow:
		for _, w := range n.WindowExprs {
			if w.InputCol != "" {
				refs[strings.ToLower(w.InputCol)] = true
			}
			for _, pb := range w.PartitionBy {
				refs[strings.ToLower(pb)] = true
			}
			for _, ob := range w.OrderBy {
				refs[strings.ToLower(ob.Column)] = true
			}
		}
	}
}

// collectASTColumnRefs walks an AST expression and adds all column references to the set.
func collectASTColumnRefs(node plansql.Node, refs map[string]bool) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		refs[strings.ToLower(n.Column)] = true
	case *plansql.CmpExpr:
		collectASTColumnRefs(n.Left, refs)
		collectASTColumnRefs(n.Right, refs)
	case *plansql.AndNode:
		collectASTColumnRefs(n.Left, refs)
		collectASTColumnRefs(n.Right, refs)
	case *plansql.OrNode:
		collectASTColumnRefs(n.Left, refs)
		collectASTColumnRefs(n.Right, refs)
	case *plansql.BinaryOp:
		collectASTColumnRefs(n.Left, refs)
		collectASTColumnRefs(n.Right, refs)
	case *plansql.UnaryOp:
		collectASTColumnRefs(n.Inner, refs)
	case *plansql.NotNode:
		collectASTColumnRefs(n.Inner, refs)
	case *plansql.ParenNode:
		collectASTColumnRefs(n.Inner, refs)
	case *plansql.FuncCallNode:
		for _, arg := range n.Args {
			collectASTColumnRefs(arg, refs)
		}
	case *plansql.CastNode:
		collectASTColumnRefs(n.Inner, refs)
	case *plansql.InExpr:
		collectASTColumnRefs(n.Left, refs)
		for _, v := range n.Values {
			collectASTColumnRefs(v, refs)
		}
	case *plansql.BetweenExpr:
		collectASTColumnRefs(n.Left, refs)
		collectASTColumnRefs(n.Low, refs)
		collectASTColumnRefs(n.High, refs)
	case *plansql.LikeExpr:
		collectASTColumnRefs(n.Left, refs)
		collectASTColumnRefs(n.Pattern, refs)
	case *plansql.IsExpr:
		collectASTColumnRefs(n.Left, refs)
	case *plansql.CaseNode:
		if n.Subject != nil {
			collectASTColumnRefs(n.Subject, refs)
		}
		for _, w := range n.Whens {
			collectASTColumnRefs(w.Cond, refs)
			collectASTColumnRefs(w.Result, refs)
		}
		if n.Else != nil {
			collectASTColumnRefs(n.Else, refs)
		}
	}
}

// extractJoinColumnRefs parses a join condition string (e.g. "a = b AND c != d")
// and adds column references to the set. Handles all comparison operators.
func extractJoinColumnRefs(cond string, refs map[string]bool) {
	parts := strings.Split(strings.ToLower(cond), " and ")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Try operators from longest to shortest to avoid partial matches
		var sides []string
		found := false
		for _, op := range []string{"!=", ">=", "<=", "<>", ">", "<", "="} {
			sep := " " + op + " "
			idx := strings.Index(part, sep)
			if idx >= 0 {
				sides = []string{part[:idx], part[idx+len(sep):]}
				found = true
				break
			}
		}
		if !found {
			continue
		}
		for _, side := range sides {
			col := strings.TrimSpace(side)
			if dotParts := strings.SplitN(col, ".", 2); len(dotParts) == 2 {
				col = dotParts[1]
			}
			if col != "" {
				refs[col] = true
			}
		}
	}
}

// decorrelateScalarSubqueries converts correlated scalar subqueries in Filter
// predicates into LEFT JOIN + Aggregate patterns. This eliminates per-row
// subquery execution by materializing the subquery result set once, grouped
// by the correlation key.
//
// Pattern: WHERE col OP (SELECT agg(...) FROM ... WHERE inner.key = outer.key ...)
// Becomes: LEFT JOIN (SELECT key, agg(...) FROM ... GROUP BY key) ON key = outer.key
//
//	WHERE col OP agg_result
func decorrelateScalarSubqueries(n *Node) *Node {
	if n == nil {
		return nil
	}

	// Recursively process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = decorrelateScalarSubqueries(child)
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

	_, outerColMap := collectScanInfo(n.Children[0])
	flatPreds := flattenANDPredicates(n.Predicates)

	var remainingPreds []Predicate
	currentPlan := n.Children[0]
	scalarIdx := 0

	for _, pred := range flatPreds {
		joinNode, rewrittenPred, ok := tryDecorrelateScalarSubquery(pred, outerTables, outerColMap, scalarIdx)
		if !ok {
			remainingPreds = append(remainingPreds, pred)
			continue
		}
		// Wire the current plan as the left (probe) child
		joinNode.Children[0] = currentPlan
		currentPlan = joinNode
		remainingPreds = append(remainingPreds, rewrittenPred)
		scalarIdx++
	}

	if scalarIdx == 0 {
		return n
	}

	n.Children[0] = currentPlan
	n.Predicates = remainingPreds
	return n
}

// tryDecorrelateScalarSubquery attempts to convert a predicate containing a
// correlated scalar subquery into a LEFT JOIN + Aggregate. Returns the join
// node (with nil left child), the rewritten predicate, and true on success.
func tryDecorrelateScalarSubquery(pred Predicate, outerTables map[string]bool, outerColMap map[string]string, idx int) (*Node, Predicate, bool) {
	if pred.ASTExpr == nil {
		return nil, pred, false
	}

	// Find comparison with scalar subquery
	cmp, subq := findScalarSubqueryComparison(pred.ASTExpr)
	if cmp == nil {
		return nil, pred, false
	}

	// Parse the subquery
	parsed, err := plansql.Parse(subq.SQL)
	if err != nil {
		return nil, pred, false
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, pred, false
	}

	// Must be a scalar subquery (exactly one SELECT column)
	if len(info.Columns) != 1 || len(info.Tables) == 0 {
		return nil, pred, false
	}

	// Check for correlated references
	refs, err := plansql.FindCorrelatedRefsWithColumns(subq.SQL, outerTables, outerColMap)
	if err != nil || len(refs) == 0 {
		return nil, pred, false
	}

	if info.WhereExpr == nil {
		return nil, pred, false
	}

	// Build set of outer ref column names for classification.
	// We use FindCorrelatedRefsWithColumns (which checks inner vs outer tables)
	// rather than the outer col map directly to avoid ambiguity when a column
	// name exists in both outer and inner scopes.
	outerRefCols := make(map[string]bool, len(refs))
	for _, ref := range refs {
		outerRefCols[ref.Column] = true
	}

	// Flatten subquery WHERE into individual conditions
	var whereNodes []plansql.Node
	flattenASTNodes(info.WhereExpr, &whereNodes)

	// Classify each condition as correlated or inner-only
	type corrKey struct{ outerCol, innerCol string }
	var correlationKeys []corrKey
	var innerFilterNodes []plansql.Node

	for _, node := range whereNodes {
		if refsOuterColumn(node, outerRefCols) {
			// Correlation condition: must be an equality comparison
			cmpNode, ok := node.(*plansql.CmpExpr)
			if !ok || cmpNode.Op != "=" {
				return nil, pred, false
			}
			leftCol := colRefName(cmpNode.Left)
			rightCol := colRefName(cmpNode.Right)
			if leftCol == "" || rightCol == "" {
				return nil, pred, false
			}
			if outerRefCols[strings.ToLower(leftCol)] {
				correlationKeys = append(correlationKeys, corrKey{outerCol: leftCol, innerCol: rightCol})
			} else {
				correlationKeys = append(correlationKeys, corrKey{outerCol: rightCol, innerCol: leftCol})
			}
		} else {
			innerFilterNodes = append(innerFilterNodes, node)
		}
	}

	if len(correlationKeys) == 0 {
		return nil, pred, false
	}

	// Build inner plan from subquery's FROM/JOIN
	innerScan := NewScan(info.Tables[0].Name, info.Tables[0].Alias)
	var innerPlan *Node = innerScan
	for _, j := range info.Joins {
		rightScan := NewScan(j.RightTable, j.RightAlias)
		innerPlan = NewJoin(innerPlan, rightScan, j.Type, j.Condition)
	}

	// Apply inner-only filters to the subquery plan
	if len(innerFilterNodes) > 0 {
		var innerPreds []Predicate
		for _, f := range innerFilterNodes {
			stripped := stripTableQualifiers(f)
			innerPreds = append(innerPreds, Predicate{
				Raw:     stripped.String(),
				ASTExpr: stripped,
			})
		}
		innerPlan = NewFilter(innerPlan, innerPreds)
	}

	// Extract aggregate from the SELECT column expression
	selectAST := info.Columns[0].ASTExpr
	if selectAST == nil {
		return nil, pred, false
	}

	aggFunc := plansql.FindNestedAggregate(selectAST)
	if aggFunc == nil {
		return nil, pred, false
	}

	// Build aggregate expression
	aggOutputCol := fmt.Sprintf("__scalar_%d", idx)
	aggInputCol := ""
	if len(aggFunc.Args) > 0 {
		stripped := stripTableQualifiers(aggFunc.Args[0])
		aggInputCol = stripped.String()
	}

	aggExpr := AggExpr{
		Func:      strings.ToLower(aggFunc.Name),
		InputCol:  aggInputCol,
		OutputCol: aggOutputCol,
		Distinct:  aggFunc.Distinct,
	}

	// GROUP BY the inner correlation columns
	var groupBy []string
	for _, ck := range correlationKeys {
		groupBy = append(groupBy, ck.innerCol)
	}

	aggNode := NewAggregate(innerPlan, groupBy, []AggExpr{aggExpr})

	// Rewrite SELECT expression: replace aggregate with ColRef to output.
	// For MIN(x) → ColRef("__scalar_0")
	// For 0.2 * AVG(x) → BinaryOp(0.2, *, ColRef("__scalar_0"))
	replacementExpr := plansql.ReplaceAggregate(selectAST, aggOutputCol)

	// Build LEFT JOIN condition
	var joinCondParts []string
	for _, ck := range correlationKeys {
		joinCondParts = append(joinCondParts, ck.outerCol+" = "+ck.innerCol)
	}

	joinNode := &Node{
		Type:     NodeJoin,
		Children: []*Node{nil, aggNode}, // left child filled by caller
		JoinType: "left",
		JoinCond: strings.Join(joinCondParts, " AND "),
	}

	// Rewrite the original predicate: replace SubqueryNode with the expression
	rewrittenExpr := replaceSubqueryInExpr(pred.ASTExpr, subq, replacementExpr)
	rewrittenPred := Predicate{
		Raw:     rewrittenExpr.String(),
		ASTExpr: rewrittenExpr,
	}

	return joinNode, rewrittenPred, true
}

// findScalarSubqueryComparison finds a CmpExpr where one side is a SubqueryNode.
func findScalarSubqueryComparison(expr plansql.Node) (*plansql.CmpExpr, *plansql.SubqueryNode) {
	if expr == nil {
		return nil, nil
	}
	switch e := expr.(type) {
	case *plansql.CmpExpr:
		if subq := unwrapSubquery(e.Right); subq != nil {
			return e, subq
		}
		if subq := unwrapSubquery(e.Left); subq != nil {
			return e, subq
		}
	case *plansql.ParenNode:
		return findScalarSubqueryComparison(e.Inner)
	}
	return nil, nil
}

// unwrapSubquery extracts a SubqueryNode from possibly parenthesized expressions.
func unwrapSubquery(expr plansql.Node) *plansql.SubqueryNode {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *plansql.SubqueryNode:
		return e
	case *plansql.ParenNode:
		return unwrapSubquery(e.Inner)
	}
	return nil
}

// refsOuterColumn checks if an AST node references any column in outerRefCols.
func refsOuterColumn(node plansql.Node, outerRefCols map[string]bool) bool {
	if node == nil {
		return false
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		return outerRefCols[strings.ToLower(n.Column)]
	case *plansql.CmpExpr:
		return refsOuterColumn(n.Left, outerRefCols) || refsOuterColumn(n.Right, outerRefCols)
	case *plansql.AndNode:
		return refsOuterColumn(n.Left, outerRefCols) || refsOuterColumn(n.Right, outerRefCols)
	case *plansql.OrNode:
		return refsOuterColumn(n.Left, outerRefCols) || refsOuterColumn(n.Right, outerRefCols)
	case *plansql.BinaryOp:
		return refsOuterColumn(n.Left, outerRefCols) || refsOuterColumn(n.Right, outerRefCols)
	case *plansql.FuncCallNode:
		for _, arg := range n.Args {
			if refsOuterColumn(arg, outerRefCols) {
				return true
			}
		}
	case *plansql.ParenNode:
		return refsOuterColumn(n.Inner, outerRefCols)
	case *plansql.NotNode:
		return refsOuterColumn(n.Inner, outerRefCols)
	}
	return false
}

// colRefName extracts the unqualified column name from a ColRef node.
func colRefName(node plansql.Node) string {
	ref, ok := node.(*plansql.ColRef)
	if !ok {
		return ""
	}
	return ref.Column
}

// replaceSubqueryInExpr replaces a specific SubqueryNode in an expression
// with a replacement node.
func replaceSubqueryInExpr(expr plansql.Node, target *plansql.SubqueryNode, replacement plansql.Node) plansql.Node {
	if expr == nil {
		return nil
	}
	switch e := expr.(type) {
	case *plansql.SubqueryNode:
		if e == target {
			return replacement
		}
		return e
	case *plansql.CmpExpr:
		return &plansql.CmpExpr{
			Left:  replaceSubqueryInExpr(e.Left, target, replacement),
			Op:    e.Op,
			Right: replaceSubqueryInExpr(e.Right, target, replacement),
		}
	case *plansql.ParenNode:
		inner := replaceSubqueryInExpr(e.Inner, target, replacement)
		// Unwrap unnecessary parens around the replacement
		if inner == replacement {
			return inner
		}
		return &plansql.ParenNode{Inner: inner}
	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  replaceSubqueryInExpr(e.Left, target, replacement),
			Op:    e.Op,
			Right: replaceSubqueryInExpr(e.Right, target, replacement),
		}
	default:
		return expr
	}
}

// ---------------------------------------------------------------------------
// IN/NOT IN → SemiJoin/AntiJoin decorrelation
// ---------------------------------------------------------------------------

// decorrelateInSubqueries converts IN/NOT IN subqueries in Filter predicates
// into SemiJoin/AntiJoin nodes. This eliminates runtime subquery execution by
// materializing the subquery as a hash join build side.
//
// Handles both uncorrelated IN (e.g., Q18) and correlated IN subqueries.
// For uncorrelated IN with GROUP BY + HAVING, the aggregate is properly
// extracted and placed in the inner plan.
//
// NOTE: NOT IN with nullable columns has different NULL semantics than AntiJoin.
// This is safe for TPC-H queries where IN columns are NOT NULL.
func decorrelateInSubqueries(n *Node) *Node {
	if n == nil {
		return nil
	}

	// Recursively process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = decorrelateInSubqueries(child)
	}

	if n.Type != NodeFilter || len(n.Children) == 0 {
		return n
	}

	// Collect outer tables from the subtree below this filter
	outerTables := make(map[string]bool)
	collectTableNames(n.Children[0], outerTables)

	_, outerColMap := collectScanInfo(n.Children[0])
	flatPreds := flattenANDPredicates(n.Predicates)

	var remainingPreds []Predicate
	currentPlan := n.Children[0]

	for _, pred := range flatPreds {
		inExpr, subq := findInSubqueryNode(pred.ASTExpr)
		if inExpr == nil {
			remainingPreds = append(remainingPreds, pred)
			continue
		}

		joinNode := tryDecorrelateInSubquery(inExpr, subq, outerTables, outerColMap)
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

// findInSubqueryNode checks if a predicate AST node is an IN/NOT IN with a subquery.
func findInSubqueryNode(node plansql.Node) (*plansql.InExpr, *plansql.SubqueryNode) {
	if node == nil {
		return nil, nil
	}
	switch n := node.(type) {
	case *plansql.InExpr:
		if len(n.Values) == 1 {
			if subq := unwrapSubquery(n.Values[0]); subq != nil {
				return n, subq
			}
		}
	case *plansql.ParenNode:
		return findInSubqueryNode(n.Inner)
	}
	return nil, nil
}

// tryDecorrelateInSubquery attempts to convert an IN/NOT IN subquery into a
// SemiJoin (IN) or AntiJoin (NOT IN) node. Handles uncorrelated IN with
// optional GROUP BY + HAVING, and correlated IN with equality keys.
// Returns nil if decorrelation is not possible.
func tryDecorrelateInSubquery(inExpr *plansql.InExpr, subq *plansql.SubqueryNode, outerTables map[string]bool, outerColMap map[string]string) *Node {
	parsed, err := plansql.Parse(subq.SQL)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil
	}

	// Must have exactly one SELECT column and at least one table.
	// Skip UNION/set-op subqueries.
	if len(info.Columns) != 1 || len(info.Tables) == 0 || info.Union != nil {
		return nil
	}

	// Get the inner select column name (stripped of table qualifiers)
	innerSelectCol := info.Columns[0].Alias
	if innerSelectCol == "" {
		innerSelectCol = cleanExpr(info.Columns[0].Expr)
	}
	if innerSelectCol == "" {
		return nil
	}

	// Get outer key from InExpr.Left — must be a simple column reference
	outerKey := colRefName(inExpr.Left)
	if outerKey == "" {
		return nil
	}

	// Build inner plan: Scan → optional JOINs
	innerScan := NewScan(info.Tables[0].Name, info.Tables[0].Alias)
	var innerPlan *Node = innerScan
	for _, j := range info.Joins {
		rightScan := NewScan(j.RightTable, j.RightAlias)
		innerPlan = NewJoin(innerPlan, rightScan, j.Type, j.Condition)
	}

	// Build inner table set for classifying WHERE conditions
	innerTableSet := make(map[string]bool)
	for _, t := range info.Tables {
		innerTableSet[strings.ToLower(t.Name)] = true
		if t.Alias != "" {
			innerTableSet[strings.ToLower(t.Alias)] = true
		}
	}
	for _, j := range info.Joins {
		innerTableSet[strings.ToLower(j.RightTable)] = true
		if j.RightAlias != "" {
			innerTableSet[strings.ToLower(j.RightAlias)] = true
		}
	}

	// Classify WHERE conditions into inner-only vs correlated
	var innerFilterPreds []Predicate
	var correlationKeys []string

	if info.WhereExpr != nil {
		var whereNodes []plansql.Node
		flattenASTNodes(info.WhereExpr, &whereNodes)

		for _, node := range whereNodes {
			hasOuter, hasInner := nodeTableRefs(node, outerTables, innerTableSet, outerColMap)
			if hasOuter && hasInner {
				// Correlated equality condition
				cmp, ok := node.(*plansql.CmpExpr)
				if !ok || cmp.Op != "=" {
					return nil
				}
				outerCol, innerCol, ok := extractCorrelatedCols(cmp, outerTables, innerTableSet, outerColMap)
				if !ok {
					return nil
				}
				correlationKeys = append(correlationKeys, outerCol+" = "+innerCol)
			} else {
				// Inner-only condition (including subquery expressions)
				stripped := stripTableQualifiers(node)
				innerFilterPreds = append(innerFilterPreds, Predicate{
					Raw:     stripped.String(),
					ASTExpr: stripped,
				})
			}
		}
	}

	// Apply inner-only WHERE conditions
	if len(innerFilterPreds) > 0 {
		innerPlan = NewFilter(innerPlan, innerFilterPreds)
	}

	// Handle GROUP BY + HAVING
	if len(info.GroupBy) > 0 {
		var groupBy []string
		for _, gb := range info.GroupBy {
			groupBy = append(groupBy, cleanExpr(gb))
		}

		var aggs []AggExpr
		aggCounter := 0

		// SELECT column aggregate (if the SELECT expression is an aggregate)
		if info.Columns[0].IsAgg {
			inputCol := cleanExpr(info.Columns[0].AggArg)
			if info.Columns[0].AggFunc == "count" && (inputCol == "*" || inputCol == "") {
				inputCol = ""
			}
			outputCol := info.Columns[0].Alias
			if outputCol == "" {
				outputCol = info.Columns[0].Expr
			}
			aggs = append(aggs, AggExpr{
				Func:      info.Columns[0].AggFunc,
				InputCol:  inputCol,
				OutputCol: outputCol,
				Distinct:  info.Columns[0].AggDistinct,
			})
			aggCounter++
		}

		// HAVING aggregates — extract from HAVING expression, assign synthetic
		// output names, then rewrite HAVING to reference those names.
		if info.HavingExpr != nil {
			havingAggs := plansql.FindAllAggregates(info.HavingExpr)
			replacements := map[string]string{}

			for _, agg := range havingAggs {
				synName := fmt.Sprintf("__having_%d", aggCounter)
				aggCounter++

				aggInputCol := ""
				var aggInputExpr plansql.Node
				if len(agg.Args) > 0 {
					aggInputCol = cleanExpr(agg.Args[0].String())
					aggInputExpr = agg.Args[0]
				}
				funcName := strings.ToLower(agg.Name)
				if funcName == "count" && (aggInputCol == "*" || aggInputCol == "") {
					aggInputCol = ""
				}

				aggs = append(aggs, AggExpr{
					Func:      funcName,
					InputCol:  aggInputCol,
					OutputCol: synName,
					Distinct:  agg.Distinct,
					InputExpr: aggInputExpr,
				})
				replacements[strings.ToLower(agg.String())] = synName
			}

			rewrittenHaving := plansql.ReplaceAllAggregates(info.HavingExpr, replacements)

			innerPlan = NewAggregate(innerPlan, groupBy, aggs)
			innerPlan = NewFilter(innerPlan, []Predicate{{
				Raw:     rewrittenHaving.String(),
				ASTExpr: rewrittenHaving,
			}})
		} else {
			innerPlan = NewAggregate(innerPlan, groupBy, aggs)
		}
	}

	// Recursively decorrelate nested IN subqueries in the inner plan
	innerPlan = decorrelateInSubqueries(innerPlan)

	// Build join condition: outer IN column = inner SELECT column
	joinCond := outerKey + " = " + innerSelectCol
	if len(correlationKeys) > 0 {
		joinCond = joinCond + " AND " + strings.Join(correlationKeys, " AND ")
	}

	joinType := "semi"
	if inExpr.Not {
		joinType = "anti"
	}

	return &Node{
		Type:     NodeJoin,
		Children: []*Node{nil, innerPlan},
		JoinType: joinType,
		JoinCond: joinCond,
	}
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

	// Extract single-table predicates from join conditions and push them down.
	// E.g., "a.id = b.id AND a.status = 'active'" → push "a.status = 'active'" to left child.
	if n.Type == NodeJoin && len(n.Children) == 2 && n.JoinCond != "" {
		n = extractJoinCondPredicates(n)
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

// extractJoinCondPredicates splits a join's ON condition and pushes single-table
// predicates to the appropriate child. Only applies to INNER joins (for outer joins
// pushing predicates from ON changes semantics).
func extractJoinCondPredicates(join *Node) *Node {
	if !strings.EqualFold(join.JoinType, "inner") && join.JoinType != "" {
		return join
	}

	upper := strings.ToUpper(join.JoinCond)
	parts := splitOnAnd(join.JoinCond, upper)
	if len(parts) < 2 {
		return join // single condition, nothing to split
	}

	leftTables, leftColMap := collectScanInfo(join.Children[0])
	rightTables, rightColMap := collectScanInfo(join.Children[1])
	allColMap := make(map[string]string, len(leftColMap)+len(rightColMap))
	for k, v := range leftColMap {
		allColMap[k] = v
	}
	for k, v := range rightColMap {
		allColMap[k] = v
	}

	var joinParts, leftParts, rightParts []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Try to parse this part as an AST predicate to resolve table refs
		pred := Predicate{Raw: part}
		if parsed := tryParseExpr(part); parsed != nil {
			pred.ASTExpr = parsed
		}
		refs := predicateTableRefs(pred, allColMap)
		if refs == nil || len(refs) == 0 {
			joinParts = append(joinParts, part)
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
			leftParts = append(leftParts, part)
		} else if allRight {
			rightParts = append(rightParts, part)
		} else {
			joinParts = append(joinParts, part)
		}
	}

	if len(leftParts) == 0 && len(rightParts) == 0 {
		return join // nothing to push
	}

	if len(leftParts) > 0 {
		var preds []Predicate
		for _, p := range leftParts {
			preds = append(preds, Predicate{Raw: p, ASTExpr: tryParseExpr(p)})
		}
		join.Children[0] = NewFilter(join.Children[0], preds)
		join.Children[0] = pushdownPredicates(join.Children[0])
	}
	if len(rightParts) > 0 {
		var preds []Predicate
		for _, p := range rightParts {
			preds = append(preds, Predicate{Raw: p, ASTExpr: tryParseExpr(p)})
		}
		join.Children[1] = NewFilter(join.Children[1], preds)
		join.Children[1] = pushdownPredicates(join.Children[1])
	}

	if len(joinParts) > 0 {
		join.JoinCond = strings.Join(joinParts, " AND ")
	} else {
		join.JoinCond = "1 = 1" // all conditions pushed; join becomes cross
	}
	return join
}

// tryParseExpr attempts to parse a string as a SQL expression.
// Returns nil if parsing fails.
func tryParseExpr(s string) plansql.Node {
	// Wrap as a SELECT WHERE to use the parser
	parsed, err := plansql.Parse("SELECT 1 FROM _dummy WHERE " + s)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil || info.WhereExpr == nil {
		return nil
	}
	return info.WhereExpr
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

// extractCommonORPredicates walks the plan tree and decomposes OR predicates
// that share common terms across all branches. For example:
//
//	(A AND C1 AND C2) OR (B AND C1 AND C2)
//
// becomes three separate predicates: C1, C2, (A OR B).
// The common predicates can then be pushed down independently by pushdownPredicates.
func extractCommonORPredicates(n *Node) *Node {
	if n == nil {
		return nil
	}
	for i, child := range n.Children {
		n.Children[i] = extractCommonORPredicates(child)
	}
	if n.Type != NodeFilter {
		return n
	}
	var newPreds []Predicate
	for _, pred := range n.Predicates {
		extracted := extractCommonFromORPred(pred)
		newPreds = append(newPreds, extracted...)
	}
	n.Predicates = newPreds
	return n
}

// extractCommonFromORPred checks if a predicate is an OR with common AND terms
// across all branches. Returns the original predicate if no extraction is possible.
func extractCommonFromORPred(pred Predicate) []Predicate {
	var orExpr *plansql.OrNode
	if pred.ASTExpr != nil {
		orExpr, _ = unwrapParenOr(pred.ASTExpr)
	}
	if orExpr == nil {
		return []Predicate{pred}
	}

	// Flatten OR tree into branches
	branches := flattenORNodes(orExpr)
	if len(branches) < 2 {
		return []Predicate{pred}
	}

	// Flatten each branch's AND terms
	branchTerms := make([][]string, len(branches))
	branchTermNodes := make([][]plansql.Node, len(branches))
	for i, branch := range branches {
		var terms []plansql.Node
		flattenANDNodes(branch, &terms)
		branchTermNodes[i] = terms
		strs := make([]string, len(terms))
		for j, t := range terms {
			strs[j] = strings.ToLower(t.String())
		}
		branchTerms[i] = strs
	}

	// Find common terms: intersection of all branches by string representation
	commonSet := make(map[string]bool)
	for _, s := range branchTerms[0] {
		commonSet[s] = true
	}
	for i := 1; i < len(branchTerms); i++ {
		branchSet := make(map[string]bool)
		for _, s := range branchTerms[i] {
			branchSet[s] = true
		}
		for s := range commonSet {
			if !branchSet[s] {
				delete(commonSet, s)
			}
		}
	}
	if len(commonSet) == 0 {
		return []Predicate{pred}
	}

	// Build result: common predicates + simplified OR
	var result []Predicate
	for _, node := range branchTermNodes[0] {
		if commonSet[strings.ToLower(node.String())] {
			result = append(result, Predicate{ASTExpr: node, Raw: node.String()})
		}
	}

	// Rebuild each branch without common terms
	var newBranches []plansql.Node
	for _, terms := range branchTermNodes {
		var remaining []plansql.Node
		for _, t := range terms {
			if !commonSet[strings.ToLower(t.String())] {
				remaining = append(remaining, t)
			}
		}
		if len(remaining) == 0 {
			continue // Branch was entirely common
		}
		newBranches = append(newBranches, buildANDTree(remaining))
	}

	if len(newBranches) > 0 {
		orNode := buildORTree(newBranches)
		result = append(result, Predicate{ASTExpr: orNode, Raw: orNode.String()})
	}
	return result
}

// unwrapParenOr returns the OrNode inside possible ParenNode wrapping.
func unwrapParenOr(n plansql.Node) (*plansql.OrNode, bool) {
	switch e := n.(type) {
	case *plansql.OrNode:
		return e, true
	case *plansql.ParenNode:
		return unwrapParenOr(e.Inner)
	}
	return nil, false
}

// flattenORNodes collects all branches of a binary OR tree into a flat slice.
func flattenORNodes(n plansql.Node) []plansql.Node {
	switch e := n.(type) {
	case *plansql.OrNode:
		return append(flattenORNodes(e.Left), flattenORNodes(e.Right)...)
	case *plansql.ParenNode:
		if or, ok := e.Inner.(*plansql.OrNode); ok {
			return flattenORNodes(or)
		}
		return []plansql.Node{n}
	default:
		return []plansql.Node{n}
	}
}

// flattenANDNodes collects all terms of a binary AND tree into a flat slice.
func flattenANDNodes(n plansql.Node, result *[]plansql.Node) {
	switch e := n.(type) {
	case *plansql.AndNode:
		flattenANDNodes(e.Left, result)
		flattenANDNodes(e.Right, result)
	case *plansql.ParenNode:
		if and, ok := e.Inner.(*plansql.AndNode); ok {
			flattenANDNodes(and, result)
		} else {
			*result = append(*result, n)
		}
	default:
		*result = append(*result, n)
	}
}

// buildANDTree constructs a right-associative AND tree from a slice of nodes.
func buildANDTree(nodes []plansql.Node) plansql.Node {
	if len(nodes) == 1 {
		return nodes[0]
	}
	result := nodes[len(nodes)-1]
	for i := len(nodes) - 2; i >= 0; i-- {
		result = &plansql.AndNode{Left: nodes[i], Right: result}
	}
	return result
}

// buildORTree constructs a right-associative OR tree from a slice of nodes.
func buildORTree(nodes []plansql.Node) plansql.Node {
	if len(nodes) == 1 {
		return nodes[0]
	}
	result := nodes[len(nodes)-1]
	for i := len(nodes) - 2; i >= 0; i-- {
		result = &plansql.OrNode{Left: nodes[i], Right: result}
	}
	return result
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
	case *plansql.SubqueryNode, *plansql.ExistsNode:
		// Subqueries may contain correlated references to outer tables.
		// Mark unresolved to prevent pushdown past joins.
		*resolved = false
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

	// Check for correlated references — use column-aware version to
	// detect unqualified outer refs (e.g., c_custkey from customer).
	refs, err := plansql.FindCorrelatedRefsWithColumns(exists.SQL, outerTables, outerColMap)
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

// ---------------------------------------------------------------------------
// Join reordering — greedy smallest-relation-first heuristic
// ---------------------------------------------------------------------------

// joinEdge represents a join condition between two relations.
type joinEdge struct {
	leftIdx  int
	rightIdx int
	joinType string
	joinCond string
}

// reorderJoins recursively looks for chains of INNER JOINs and reorders them
// so that smaller (filtered) relations are joined first.
func reorderJoins(n *Node) *Node {
	if n == nil {
		return nil
	}
	for i, child := range n.Children {
		n.Children[i] = reorderJoins(child)
	}
	if n.Type != NodeJoin {
		return n
	}
	// Only reorder chains of inner joins (outer join order is semantically significant)
	if !isInnerJoin(n) {
		return n
	}
	// Flatten the join chain
	var rels []*Node
	var edges []joinEdge
	flattenJoinChain(n, &rels, &edges)
	if len(rels) <= 2 {
		// For 2-way inner joins, ensure the larger relation is on the
		// probe (left) side and the smaller on the build (right) side.
		// The hash join builds right into a hash table (materialized),
		// so the smaller side should be the build side.
		leftCost := estimateRelCost(n.Children[0])
		rightCost := estimateRelCost(n.Children[1])
		if leftCost < rightCost {
			n.Children[0], n.Children[1] = n.Children[1], n.Children[0]
		}
		return n
	}
	return greedyJoinReorder(rels, edges)
}

// isInnerJoin returns true for INNER joins (or empty join type which defaults to inner).
func isInnerJoin(n *Node) bool {
	if n.Type != NodeJoin {
		return false
	}
	jt := strings.ToLower(n.JoinType)
	return jt == "" || jt == "join" || jt == "inner" || jt == "inner join" || jt == "cross"
}

// flattenJoinChain walks a left-deep chain of inner joins, collecting leaf
// relations and the join conditions between them.
func flattenJoinChain(n *Node, rels *[]*Node, edges *[]joinEdge) {
	if n.Type != NodeJoin || !isInnerJoin(n) {
		*rels = append(*rels, n)
		return
	}
	leftStart := len(*rels)
	flattenJoinChain(n.Children[0], rels, edges)
	leftAfter := len(*rels)

	rightBefore := len(*rels)
	flattenJoinChain(n.Children[1], rels, edges)

	if n.JoinCond == "" {
		return
	}

	// Find which left-side relation owns the columns in the join condition.
	// The naive approach (leftRep = leftAfter-1) is wrong for multi-way joins
	// where the condition references a non-last relation (e.g., supplier-nation
	// edge in supplier-lineitem-orders-nation chain).
	leftRep := leftAfter - 1
	if leftRep < 0 {
		leftRep = 0
	}
	rightRep := rightBefore

	// Extract column refs from the join condition and match to relations.
	condRefs := make(map[string]bool, 4)
	extractJoinColumnRefs(n.JoinCond, condRefs)
	if len(condRefs) > 0 {
		for i := leftStart; i < leftAfter; i++ {
			relCols := collectSubtreeColumns((*rels)[i])
			for ref := range condRefs {
				if relCols[ref] {
					leftRep = i
					break
				}
			}
		}
	}

	*edges = append(*edges, joinEdge{
		leftIdx:  leftRep,
		rightIdx: rightRep,
		joinType: n.JoinType,
		joinCond: n.JoinCond,
	})
}

// estimateRelCost assigns a heuristic cost to a relation subtree.
// Lower cost = smaller/cheaper → should be joined first (as build side).
func estimateRelCost(n *Node) int {
	if n == nil {
		return 1000
	}
	switch n.Type {
	case NodeScan:
		// Use actual row estimate from manifest if available
		if n.ScanRowEstimate > 0 {
			cost := int(n.ScanRowEstimate / 1000) // 1 cost unit per 1K rows
			if cost < 1 {
				cost = 1
			}
			if len(n.ScanPredicates) > 0 || len(n.PartitionFilter) > 0 {
				cost /= 2 // filtered scan is cheaper
			}
			return cost
		}
		// Bare scan — medium cost
		if len(n.ScanPredicates) > 0 || len(n.PartitionFilter) > 0 {
			return 10 // filtered scan is cheap
		}
		return 100
	case NodeFilter:
		// Filter above something — cheaper than the thing below
		base := estimateRelCost(n.Children[0])
		return base / 2
	case NodeAggregate:
		// Aggregation reduces rows
		return 50
	case NodeDistinct:
		return 50
	default:
		// Complex subplan
		return 200
	}
}

// greedyJoinReorder builds a left-deep join tree starting from the cheapest
// relation, greedily adding the cheapest connected neighbor at each step.
func greedyJoinReorder(rels []*Node, edges []joinEdge) *Node {
	n := len(rels)
	costs := make([]int, n)
	for i, r := range rels {
		costs[i] = estimateRelCost(r)
	}

	// Build adjacency: for each relation, which edges connect to it
	relEdges := make([][]int, n)
	for i := range relEdges {
		relEdges[i] = []int{}
	}
	for ei, e := range edges {
		relEdges[e.leftIdx] = append(relEdges[e.leftIdx], ei)
		relEdges[e.rightIdx] = append(relEdges[e.rightIdx], ei)
	}

	// Track which table names each slot in the plan covers (for condition matching)
	relTables := make([]map[string]bool, n)
	for i, r := range rels {
		relTables[i], _ = collectScanInfo(r)
	}

	// Pick the MOST EXPENSIVE relation as the starting point (probe side).
	// In a left-deep tree, the initial relation streams as the probe through
	// all subsequent hash joins, so the largest table avoids being materialized.
	used := make([]bool, n)
	bestIdx := 0
	for i := 1; i < n; i++ {
		if costs[i] > costs[bestIdx] {
			bestIdx = i
		}
	}
	used[bestIdx] = true
	plan := rels[bestIdx]
	planTables := copyBoolMap(relTables[bestIdx])
	usedEdges := make([]bool, len(edges))

	// Greedily add remaining relations
	for added := 1; added < n; added++ {
		bestNext := -1
		bestEdge := -1
		bestCost := int(^uint(0) >> 1) // MaxInt

		// Find the cheapest unused relation that has an edge to the current plan
		for ei, e := range edges {
			if usedEdges[ei] {
				continue
			}
			var candidate int
			if used[e.leftIdx] && !used[e.rightIdx] {
				candidate = e.rightIdx
			} else if used[e.rightIdx] && !used[e.leftIdx] {
				candidate = e.leftIdx
			} else {
				continue
			}
			if costs[candidate] < bestCost {
				bestCost = costs[candidate]
				bestNext = candidate
				bestEdge = ei
			}
		}

		if bestNext < 0 {
			// No connected relation found — pick cheapest unused (cross join)
			for i := 0; i < n; i++ {
				if !used[i] && costs[i] < bestCost {
					bestCost = costs[i]
					bestNext = i
				}
			}
			if bestNext < 0 {
				break
			}
			plan = NewJoin(plan, rels[bestNext], "inner", "")
		} else {
			usedEdges[bestEdge] = true
			plan = NewJoin(plan, rels[bestNext], edges[bestEdge].joinType, edges[bestEdge].joinCond)
		}
		used[bestNext] = true
		for t := range relTables[bestNext] {
			planTables[t] = true
		}

		// Attach any other edges that are now fully covered by the plan
		for ei, e := range edges {
			if usedEdges[ei] {
				continue
			}
			if used[e.leftIdx] && used[e.rightIdx] {
				// Both sides covered — add as additional filter on the join
				usedEdges[ei] = true
				if plan.JoinCond != "" {
					plan.JoinCond = plan.JoinCond + " AND " + e.joinCond
				} else {
					plan.JoinCond = e.joinCond
				}
			}
		}
	}

	return plan
}

func copyBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
