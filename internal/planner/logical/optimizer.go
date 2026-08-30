package logical

import (
	"log/slog"
	"math"
	"math/bits"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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
	// Before every rule that reads the SELECT list — computeRequiredColumns
	// most of all. An unexpanded star names no columns, so column pruning
	// narrowed the scan to whatever else the SELECT list mentioned and the
	// star's own columns came back NULL (#315). See star_expansion.go.
	ExpandStarProjections(plan)
	// The enclosing WITH, for the FROM items of the subqueries the next three
	// passes lower. They re-parse the subquery from its SQL text, which does
	// not carry the WITH, so without it a CTE named in a decorrelated
	// subquery's FROM is indistinguishable from a base table and becomes a
	// Scan of a name no catalog has — IN answers 0, NOT IN every row (#535,
	// #581, and the recursive #F1). innerRelationsAreScannable uses it to
	// decline the shape, exactly as it declines a derived table.
	ctes := plan.CTEs
	plan = decorrelateExists(plan, ctes)
	plan = decorrelateInSubqueries(plan, ctes)
	// Annotate new scans created by IN decorrelation so scalar subquery
	// decorrelation can resolve unqualified column references.
	for _, annotate := range annotators {
		annotate(plan)
	}
	plan = decorrelateScalarSubqueries(plan, ctes)
	plan = extractCommonORPredicates(plan)
	// Before pushdownPredicates: an ON conjunct the join cannot represent
	// becomes a filter above it, and pushdown is then what puts the
	// single-sided ones back down. Running it after would leave them
	// stranded above the join (#336).
	plan = liftInnerJoinOnResiduals(plan)
	plan = pushdownPredicates(plan)
	// After pushdownPredicates: extractJoinCondPredicates has pushed the
	// outer-join ON conjuncts that have a better home (a non-preserved
	// side's scan); what remains un-representable as a key pair becomes the
	// join's residual predicate, evaluated on the probe (#358).
	plan = routeOuterJoinOnResiduals(plan)
	// Move WHERE equalities onto comma-FROM cross joins (issue #281) so
	// semi pushdown and reorderJoins see real join conditions. After
	// pushdownPredicates: single-table predicates are already on scans,
	// leaving only cross-relation predicates in the filter.
	plan = liftWhereEquiPredsIntoJoins(plan)
	// After pushdown so the cloned key-source branch carries its final
	// filters (before pushdown, single-table predicates still sit in the
	// filter node above the join tree).
	plan = reduceDecorrelatedScalarAggs(plan)
	plan = dedupSemiAntiBuildSide(plan)
	// Push semi/anti joins (decorrelated IN / NOT IN) below inner joins so
	// they filter before the join multiplies work — reorderJoins then
	// treats the semi-filtered relation as a leaf of the chain it orders.
	// SF100 Q18: the o_orderkey IN (HAVING subquery) semijoin applied
	// above customer⋈orders meant joining all 150M orders×customer rows
	// (15.8s, 8.8 GB materialized) before filtering to 6,398. Kill switch
	// WADJET_SEMI_PUSHDOWN=0.
	plan = pushSemiAntiBelowInnerJoins(plan)
	plan = reorderJoins(plan)
	// Immediately after reorderJoins, which is what decides — from estimated
	// row counts — which relation's columns each inner join emits BARE and
	// which it qualifies. The IN / EXISTS decorrelations run at steps 35/36
	// and cannot know that, so they record what their build-side references
	// MEAN and this settles the text (#526, #527). Before every pass that
	// reads a join condition as text.
	plan = repairDecorrelatedSpelling(plan)
	plan = rewriteDistinctAsGroupBy(plan)
	plan = rewriteCountDistinctTwoLevel(plan)
	plan = extractPartitionFilters(plan)
	plan = pruneProjections(plan)
	// Re-annotate before column pruning: scans created AFTER the first
	// annotator pass (scalar-subquery decorrelation) have no ScanColumns,
	// and sanitizeScanNeeds can only drop other-table pollution when it
	// knows the scan's schema (it conservatively keeps everything
	// otherwise). Annotators are idempotent.
	for _, annotate := range annotators {
		annotate(plan)
	}
	computeRequiredColumns(plan)
	attachScanPredicates(plan)
	// After attachScanPredicates so scan-attached conjuncts are visible to
	// the walk: a column must be proven shape-only against EVERY predicate
	// that survives, wherever it ended up.
	computeShapeOnlyColumns(plan)
	return plan
}

// attachScanPredicates copies simple filter predicates to scan nodes for
// row-group-level pruning. Only predicates with a Column, Op, and literal Value
// are useful for stats-based pruning.
//
// The builder wraps the WHOLE WHERE clause in one Raw/AST predicate with
// Column/Op/Value empty, so the structured check alone attached NOTHING on
// the embedded path — zonemap pruning never saw a predicate there. When
// the structured fields are empty but an AST is present, decompose the
// AST's top-level AND-conjuncts and attach each `col <op> literal` shape.
// ScanPredicates feed PRUNING only (row filtering still runs the full
// Filter), so attaching a conservative subset is always safe.
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
	if child.Type != NodeScan {
		return
	}
	for _, pred := range n.Predicates {
		if pred.Column != "" && pred.Op != "" && pred.Value != nil {
			child.ScanPredicates = append(child.ScanPredicates, pred)
			continue
		}
		if pred.ASTExpr != nil {
			child.ScanPredicates = append(child.ScanPredicates, structuredConjuncts(pred.ASTExpr)...)
		}
	}
}

// SplitConjunctsForPushdown decomposes an AST filter into top-level
// AND-conjuncts and partitions them: structured `column <op> literal`
// comparisons (as Predicates) and everything else (residual ASTs). The
// physical planner uses this to push eligible conjuncts into the scan
// and compile only the residue as the exec filter.
func SplitConjunctsForPushdown(expr plansql.Node) ([]Predicate, []plansql.Node) {
	structured := structuredConjuncts(expr)
	// Recompute the conjunct list and subtract the structured ones by AST
	// identity (structuredConjuncts stores each conjunct's AST).
	matched := make(map[plansql.Node]bool, len(structured))
	for _, p := range structured {
		matched[p.ASTExpr] = true
	}
	var conj []plansql.Node
	var split func(n plansql.Node)
	split = func(n plansql.Node) {
		switch t := stripParens(n).(type) {
		case *plansql.AndNode:
			split(t.Left)
			split(t.Right)
		default:
			conj = append(conj, n)
		}
	}
	split(expr)
	var residual []plansql.Node
	for _, c := range conj {
		if sp, ok := stripParens(c).(*plansql.CmpExpr); ok && matched[plansql.Node(sp)] {
			continue
		}
		residual = append(residual, c)
	}
	return structured, residual
}

// structuredConjuncts splits an AST filter into top-level AND-conjuncts
// and returns a structured Predicate for each `column <op> literal` (or
// flipped) comparison. Anything else — ORs, functions, non-literal sides —
// contributes nothing; pruning must only ever see conjuncts that are
// necessary conditions of the whole filter.
func structuredConjuncts(expr plansql.Node) []Predicate {
	var conj []plansql.Node
	var split func(n plansql.Node)
	split = func(n plansql.Node) {
		switch t := stripParens(n).(type) {
		case *plansql.AndNode:
			split(t.Left)
			split(t.Right)
		default:
			conj = append(conj, n)
		}
	}
	split(expr)

	var out []Predicate
	for _, c := range conj {
		bin, ok := stripParens(c).(*plansql.CmpExpr)
		if !ok {
			continue
		}
		op := bin.Op
		var colNode plansql.Node
		var lit *plansql.Lit
		if cn, l := plainColAndLit(bin.Left, bin.Right); cn != nil {
			colNode, lit = cn, l
		} else if cn, l := plainColAndLitStr(bin.Right, bin.Left); cn != nil {
			colNode, lit = cn, l
			op = flipCompareOp(op)
		} else if cn, l := plainColAndLitStr(bin.Left, bin.Right); cn != nil {
			// plainColAndLit only accepts numeric literals; the string
			// variant covers `col = 'x'` shapes.
			colNode, lit = cn, l
		} else {
			continue
		}
		switch op {
		case "=", "<", "<=", ">", ">=", "!=", "<>":
		default:
			continue
		}
		if op == "<>" {
			op = "!="
		}
		col, ok := colNode.(*plansql.ColRef)
		if !ok {
			continue
		}
		var val any
		var valText string
		switch lit.Kind {
		case plansql.LitNumber:
			// The box is for the estimator and the typed comparisons; the
			// TEXT is what a DECIMAL column's prune and filter convert at the
			// column's scale, which a float64 box cannot survive (#452).
			valText = lit.Value
			if iv, err := strconv.ParseInt(lit.Value, 10, 64); err == nil {
				val = iv
			} else if fv, err := strconv.ParseFloat(lit.Value, 64); err == nil {
				val = fv
			} else {
				continue
			}
		case plansql.LitString:
			val = lit.Value
		default:
			continue
		}
		name := col.Column
		if col.Table != "" {
			name = col.Table + "." + col.Column
		}
		out = append(out, Predicate{Column: name, Op: op, Value: val, ValueText: valText,
			Raw: c.String(), ASTExpr: c, PruneOnly: true})
	}
	return out
}

// plainColAndLitStr is plainColAndLit without the numeric-kind restriction.
func plainColAndLitStr(a, b plansql.Node) (plansql.Node, *plansql.Lit) {
	col, ok := stripParens(a).(*plansql.ColRef)
	if !ok {
		return nil, nil
	}
	lit, ok := stripParens(b).(*plansql.Lit)
	if !ok {
		return nil, nil
	}
	return col, lit
}

// flipCompareOp mirrors a comparison for `literal <op> column` shapes.
func flipCompareOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op // =, !=, <> are symmetric
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

// RowCountOnlyColumn is the sentinel required-column emitted for scans
// that only need a row count (bare COUNT(*) / literal-only projections).
// The physical scan drops it when any real column is required and
// otherwise projects the narrowest schema column instead of all columns.
// The "__" prefix keeps it flowing through sanitizeScanNeeds and makes the
// distributed worker's all-or-nothing projection guard fall back to full
// width (safe) if it ever reaches that path.
const RowCountOnlyColumn = "__rowcount_only__"

// scanColSanitize gates sanitizeScanNeeds (WADJET_SCAN_COL_SANITIZE=0
// restores the pre-2026-07 polluted lists for A/B). Default on.
var scanColSanitize = os.Getenv("WADJET_SCAN_COL_SANITIZE") != "0"

// sanitizeScanNeeds turns the ancestor-accumulated needs set into a clean
// RequiredColumns list for one scan. The accumulated set carries junk the
// scan can never produce — alias-qualified duplicates ("l1.l_receiptdate"
// next to "l_receiptdate") and the OTHER side's join-key columns
// ("s_suppkey" landing on a lineitem scan via "s_suppkey = l1.l_suppkey").
// Any such name trips the worker's all-or-nothing parquet projection guard
// (cachedFileStreamSource.projectColumns) and silently reverts the scan —
// and every shuffle fed by it — to full width: Q21's l1 leg measured
// 143 B/row against the 25 B/row its clean sibling leg achieves
// (docs/design/exchange-reuse.md §2 A1).
//
// Rules, conservative toward keeping:
//   - "alias.col" where alias is THIS scan (TableAlias or TableName):
//     rewritten to bare col. Other aliases: dropped — provably another
//     relation's column.
//   - bare names when ScanColumns (catalog schema, AnnotateScanColumns) is
//     known: kept iff in the schema, EXCEPT "__"-prefixed derived names
//     (e.g. __having_0), which are kept so the worker guard's
//     derived-column semantics are preserved exactly.
//   - bare names when ScanColumns is empty (no catalog at plan time):
//     kept — we cannot judge, and full width is the safe failure mode.
//
// Output is sorted for deterministic plans.
func sanitizeScanNeeds(n *Node, needs map[string]bool) []string {
	if !scanColSanitize {
		cols := make([]string, 0, len(needs))
		for col := range needs {
			cols = append(cols, col)
		}
		sort.Strings(cols)
		return cols
	}
	// lower → canonical schema spelling. Refs arrive lowercased from
	// collectASTColumnRefs; downstream projection (buildReadSchema and the
	// scan readers) matches column names case-SENSITIVELY, so the kept
	// names must carry the schema's case. On all-lowercase schemas (TPC-H)
	// this was invisible; on CamelCase schemas (ClickBench hits) the
	// lowercased refs matched nothing and every scan silently read all
	// 105 columns — the entire dataset per query.
	inSchema := make(map[string]string, len(n.ScanColumns))
	for _, c := range n.ScanColumns {
		inSchema[strings.ToLower(c)] = c
	}
	keep := make(map[string]bool, len(needs))
	for name := range needs {
		// A delimited identifier names one column, dots included, so it
		// must not be read as alias.column. Zeek JSON columns arrive this
		// way: "id.orig_h" is the whole name.
		if strings.Contains(name, `"`) {
			if qual, col, ok := plansql.SplitIdentRef(name); ok && qual == "" {
				if canon, ok := inSchema[strings.ToLower(col)]; ok {
					keep[canon] = true
				} else {
					keep[col] = true
				}
				continue
			}
		}
		// A flat schema column whose own name contains a dot matches
		// before any qualifier split is attempted.
		if canon, ok := inSchema[strings.ToLower(name)]; ok {
			keep[canon] = true
			continue
		}
		if alias, col, ok := strings.Cut(name, "."); ok {
			if strings.EqualFold(alias, n.TableAlias) || strings.EqualFold(alias, n.TableName) {
				if canon, ok := inSchema[strings.ToLower(col)]; ok {
					keep[canon] = true
				} else {
					keep[col] = true
				}
				continue
			}
			// "attrs.score" where attrs is a ROW-typed column of THIS
			// scan is a field path, not an alias qualifier. What the scan
			// must read is the BASE column — the expression compiler
			// resolves the field out of it — so that is what is kept.
			// Dropping the reference entirely broke every dotted ROW access
			// (filter column "attrs.score" does not exist) when sanitization
			// landed in #249, and keeping the DOTTED spelling was the other
			// half of the same mistake: a stage's requested-column list is
			// intersected with the file schema, which has no such column, so
			// the parent went unread and every field came back NULL on the
			// stage DAG while the single-process path answered correctly
			// (#568 — the "parquet projection narrowed" warning names it).
			if canon, ok := inSchema[strings.ToLower(alias)]; ok {
				keep[canon] = true
			}
			continue // other relation's qualified column: drop
		}
		if canon, ok := inSchema[strings.ToLower(name)]; ok {
			keep[canon] = true
		} else if len(inSchema) == 0 || strings.HasPrefix(name, "__") {
			keep[name] = true
		}
	}
	cols := make([]string, 0, len(keep))
	for col := range keep {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	return cols
}

// pushColumnNeeds recursively pushes the set of needed columns from parent
// nodes down to scan nodes. At each level, it adds columns referenced by
// the current node and passes the accumulated set to children.
func pushColumnNeeds(n *Node, parentNeeds map[string]bool) {
	if n == nil {
		return
	}

	// nil parentNeeds means "all columns" (SELECT * semantics). Only
	// column-restricting nodes (Project, Aggregate) should create a concrete
	// needs set; pass-through nodes (Sort, Filter, Limit, Distinct) propagate
	// nil so downstream scans read all columns and joins skip OutputFilter.
	if parentNeeds == nil {
		if n.Type == NodeScan {
			// No RequiredColumns → scan reads all columns.
			return
		}
		// Project and Aggregate restrict output columns — they create the
		// initial needs set even when the parent wants all columns.
		if n.Type == NodeProject || n.Type == NodeAggregate {
			needs := make(map[string]bool, 8)
			collectNodeColumnRefs(n, needs)
			if len(needs) == 0 {
				// Zero column refs — SELECT COUNT(*) or SELECT <literal>.
				// Previously this fell through to "all columns", so a bare
				// COUNT(*) read (and decoded) the entire table. The
				// sentinel tells the scan "any single narrow column is
				// enough to count rows"; intermediate Filter nodes still
				// add their predicate columns on the way down.
				needs[RowCountOnlyColumn] = true
			}
			for _, child := range n.Children {
				pushColumnNeeds(child, needs)
			}
			return
		}
		// SEMI/ANTI join build side never contributes columns to output,
		// so restrict it even in "all columns" mode.
		if n.Type == NodeJoin && len(n.Children) == 2 &&
			(strings.EqualFold(n.JoinType, "semi") || strings.EqualFold(n.JoinType, "anti")) {
			pushColumnNeeds(n.Children[0], nil)
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
		// All other nodes: propagate nil (all columns) to children.
		for _, child := range n.Children {
			pushColumnNeeds(child, nil)
		}
		return
	}

	// parentNeeds is non-nil — parent wants specific columns.
	// Merge parent needs with this node's own column references.
	needs := make(map[string]bool, len(parentNeeds)+8)
	for col := range parentNeeds {
		needs[col] = true
	}
	collectNodeColumnRefs(n, needs)

	// A WINDOW PRODUCES its output columns; nothing below it stores them. The
	// SELECT list above references `__win_0`, so without this the name is
	// pushed down as a required column and lands in the SCAN's read set —
	// `scan-0 cols=[__win_0 a id]` for a table that has no such column. The
	// join above then partitions its needs over a name neither side provides,
	// the stream that reaches the gather does not carry it, and the gather's
	// `OutputRename{__win_0 -> w}` silently falls back to the producer's raw
	// columns: `SELECT x.id, x.w FROM (SELECT id, SUM(a) OVER () AS w FROM t) x
	// JOIN t y ON …` came back as `[id, y.id]` (#694 round 2, R1).
	//
	// This is the rule the NodeSort arm of collectNodeColumnRefs already
	// applies to `__sortkey_N` and for the same reason. It is stated here
	// rather than there because a window's output is not skipped when it is
	// COLLECTED — the Project above genuinely reads it — only when it is
	// PUSHED PAST the node that computes it.
	if n.Type == NodeWindow {
		for _, w := range n.WindowExprs {
			if w.OutputCol != "" {
				delete(needs, strings.ToLower(w.OutputCol))
			}
		}
		if len(needs) == 0 {
			// Everything above wanted only the window's own outputs. The
			// window still has to see the rows it computes them from, and an
			// empty set means "read nothing", so fall back to the row-count
			// sentinel the Project/Aggregate arm uses for the same situation.
			needs[RowCountOnlyColumn] = true
		}
	}

	// A Filter directly over a Scan: record which columns the filter alone
	// requires (referenced by the filter, not by anything above it). When
	// the physical planner pushes a conjunct into the scan's row filter,
	// a filter-only column needs no materialization at all — the scan
	// evaluates it from pages and the value never reaches a vector.
	if n.Type == NodeFilter && len(n.Children) == 1 && n.Children[0].Type == NodeScan {
		filterRefs := make(map[string]bool, 4)
		collectNodeColumnRefs(n, filterRefs)
		var only []string
		for col := range filterRefs {
			if !parentNeeds[col] {
				only = append(only, col)
			}
		}
		sort.Strings(only)
		n.Children[0].FilterOnlyColumns = only
	}

	if n.Type == NodeScan {
		if len(needs) > 0 {
			n.RequiredColumns = sanitizeScanNeeds(n, needs)
		}
		return
	}

	// Save needed columns on join nodes so the physical planner can apply
	// OutputFilter to skip gathering unneeded columns.
	if n.Type == NodeJoin {
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
		pushColumnNeeds(n.Children[0], needs)
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
			if len(leftAvail) > 0 && len(rightAvail) > 0 {
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
	// Semi/anti joins only output probe-side (child[0]) columns.
	// Skip the build side (child[1]) so downstream column partitioning
	// doesn't treat build-side columns as available from this subtree.
	if n.Type == NodeJoin && len(n.Children) == 2 {
		jt := strings.ToLower(n.JoinType)
		if jt == "semi" || jt == "anti" {
			collectSubtreeColumnsRec(n.Children[0], result)
			return
		}
	}
	for _, child := range n.Children {
		collectSubtreeColumnsRec(child, result)
	}
}

// NodeColumnRefs returns the column names referenced by a node's own
// expressions (filter predicates, sort keys, projections, ...) — the same
// collection column pruning uses, exported for the physical planner's
// top-N late-materialization rewrite.
func NodeColumnRefs(n *Node) []string {
	refs := make(map[string]bool, 8)
	collectNodeColumnRefs(n, refs)
	cols := make([]string, 0, len(refs))
	for c := range refs {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
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
		// Walk GROUP BY AST expressions to capture actual column refs
		// (e.g., EXTRACT(year FROM o_orderdate) → needs "o_orderdate")
		for _, expr := range n.GroupByExprs {
			collectASTColumnRefs(expr, refs)
		}
		for _, agg := range n.AggExprs {
			if agg.InputCol != "" && agg.InputCol != "*" {
				refs[strings.ToLower(agg.InputCol)] = true
			}
			// The second column of CORR/COVAR_* and the ordering column of
			// MIN_BY/MAX_BY are read per row exactly as InputCol is, so they
			// have to survive pruning the same way. A pruned ordering column
			// is not a missing column downstream — HashAggregate resolves it
			// to -1 and skips every row, which is a NULL answer.
			if agg.InputCol2 != "" {
				refs[strings.ToLower(agg.InputCol2)] = true
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
			// A materialized ORDER BY term (#320) names a column the Project
			// below computes, not one any scan stores. Pushing the synthetic
			// name down would land it in a scan's RequiredColumns, where the
			// worker's all-or-nothing parquet projection guard treats an
			// unknown "__" name as a reason to read full width. The columns it
			// really reads arrive through the projection's own AST refs.
			if IsHiddenSortColumn(ob.Column) {
				continue
			}
			refs[strings.ToLower(ob.Column)] = true
		}
	case NodeWindow:
		for _, w := range n.WindowExprs {
			// InputColumn, not InputCol: LAG/LEAD/NTH_VALUE carry their
			// offset, default or N in the same string, and registering
			// "s, 2" as the required column left the real one, s, unread —
			// the window operator then resolved no input vector and
			// nil-dereferenced it.
			// The ARGUMENT takes the same parse as the keys below: a ROW
			// field path (`SUM(rw.f) OVER ()`) needs the ROW column `rw`
			// read, and registering the path's text leaves it pruned away
			// (#603).
			if col := w.InputColumn(); col != "" && col != "*" {
				collectWindowKeyRefs(col, refs)
			}
			// A window key is an EXPRESSION as often as it is a column
			// (`PARTITION BY id % 3`, `PARTITION BY upper(s)`), and
			// registering the expression's TEXT as a required column left
			// the columns it reads unread — the pre-window projection then
			// computed the key over a column the scan had pruned away, every
			// row got the same value, and the window ran over one partition
			// (#585, the same shape as InputColumn's above).
			for _, pb := range w.PartitionBy {
				collectWindowKeyRefs(pb, refs)
			}
			for _, ob := range w.OrderBy {
				collectWindowKeyRefs(ob.Column, refs)
			}
		}
	}
}

// collectWindowKeyRefs registers the columns one PARTITION BY / window ORDER
// BY term reads.
//
// The qualifier is registered as a column name of its own, which
// collectASTColumnRefs does not do: `PARTITION BY rw.f` over a ROW column is
// a FIELD PATH, so the column the scan has to read is `rw` and neither `f`
// nor `rw.f` names anything it stores. Registering a name no table carries
// costs nothing — the pruner keeps the columns it recognizes and ignores the
// rest — and the alternative is a pruned-away ROW column and a key that is
// NULL in every row.
func collectWindowKeyRefs(term string, refs map[string]bool) {
	term = strings.TrimSpace(term)
	if term == "" {
		return
	}
	ast, err := plansql.ParseExpression(term)
	if err != nil {
		refs[strings.ToLower(term)] = true
		return
	}
	collectASTColumnRefs(ast, refs)
	if col, ok := ast.(*plansql.ColRef); ok && col.Table != "" {
		refs[strings.ToLower(col.Table)] = true
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
		if n.Table != "" {
			refs[strings.ToLower(n.Table+"."+n.Column)] = true
		}
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
	// A subquery that survives decorrelation is evaluated per outer row, and
	// the outer columns it correlates on are read out of the outer batch. They
	// are as required as any column the SELECT list names — and the walk used
	// to have no case for a subquery node at all, so it kept none of them
	// (#347). See plansql.OuterColumnCandidates for what counts as one.
	case *plansql.SubqueryNode:
		collectSubqueryOuterRefs(n.SQL, refs)
	case *plansql.ExistsNode:
		collectSubqueryOuterRefs(n.SQL, refs)
	case *plansql.AnyAllExpr:
		collectASTColumnRefs(n.Left, refs)
		for _, v := range n.Values {
			collectASTColumnRefs(v, refs)
		}
	case *plansql.TupleNode:
		for _, e := range n.Elements {
			collectASTColumnRefs(e, refs)
		}
	// ARRAY[expr, expr, ...] is a container literal built from column
	// expressions the same way TupleNode is. The walk had no case for it,
	// so a bare ARRAY[c] select-list expression contributed no names, the
	// scan pruned the referenced column away, and the compiled ARRAY
	// constructor read a column that was not in the batch — a silent NULL
	// per element (#596).
	case *plansql.ArrayLitNode:
		for _, e := range n.Elements {
			collectASTColumnRefs(e, refs)
		}
	}
}

// collectSubqueryOuterRefs adds the outer-query columns a correlated subquery
// reads to the needs set. Over-inclusive by design: a name the scan's schema
// does not have is dropped by sanitizeScanNeeds, while a name it does have
// and pruning discards is a correlated reference that resolves to nothing.
func collectSubqueryOuterRefs(sql string, refs map[string]bool) {
	for _, col := range plansql.OuterColumnCandidates(sql) {
		refs[col] = true
	}
}

// extractJoinColumnRefs parses a join condition string (e.g. "a = b AND c != d")
// and adds column references to the set.
//
// Structural first: the condition is parsed and its ColRefs collected the
// same way WHERE predicates are (bare and qualified spellings both;
// sanitizeScanNeeds resolves or drops them per scan). The lexical fallback
// below split on operators and passed each SIDE through as a name, which for
// an expression operand invented a "column" like "r_regionkey + 3" — the
// scan kept the fiction and dropped the real r_regionkey, so an outer join's
// residual (#358) read NULL for a column the file held. The fallback remains
// for text no expression parser accepts.
func extractJoinColumnRefs(cond string, refs map[string]bool) {
	if expr := tryParseExpr(cond); expr != nil {
		collectASTColumnRefs(expr, refs)
		return
	}
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
func decorrelateScalarSubqueries(n *Node, ctes []plansql.CTEDef) *Node {
	if n == nil {
		return nil
	}
	if len(n.CTEs) > 0 {
		ctes = n.CTEs
	}

	// Recursively process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = decorrelateScalarSubqueries(child, ctes)
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
		joinNode, rewrittenPred, ok := tryDecorrelateScalarSubquery(pred, outerTables, outerColMap, scalarIdx, ctes)
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
func tryDecorrelateScalarSubquery(pred Predicate, outerTables map[string]bool, outerColMap map[string]string, idx int, ctes []plansql.CTEDef) (*Node, Predicate, bool) {
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
	if !innerRelationsAreScannable(info, ctes) {
		return nil, pred, false
	}
	innerScan := NewScan(info.Tables[0].Name, info.Tables[0].Alias)
	var innerPlan *Node = innerScan
	for _, j := range info.Joins {
		rightScan := NewScan(j.RightTable, j.RightAlias)
		innerPlan = NewJoin(innerPlan, rightScan, j.Type, j.Condition)
	}

	// Apply inner-only filters to the subquery plan
	if len(innerFilterNodes) > 0 {
		joinedInner := len(info.Joins) > 0 || len(info.Tables) > 1
		var innerPreds []Predicate
		for _, f := range innerFilterNodes {
			// A condition naming more than one inner relation has no
			// spelling that survives this rewrite — decline and leave the
			// scalar subquery as written (see innerOnlyPredicate).
			inner, ok := innerOnlyPredicate(f, joinedInner)
			if !ok {
				return nil, pred, false
			}
			innerPreds = append(innerPreds, inner)
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
	aggOutputCol := plansql.SlotName(plansql.SlotScalar, idx)
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
	// Arguments past the first (CORR's second column, STRING_AGG's
	// separator, PERCENTILE's fraction). Decline the rewrite rather than
	// plan an aggregate missing one of them — the general builder path
	// carries them and answers correctly (#353).
	if err := parseAggExtraArgs(&aggExpr, aggFunc.Args); err != nil {
		return nil, pred, false
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
		Type:               NodeJoin,
		Children:           []*Node{nil, aggNode}, // left child filled by caller
		JoinType:           "left",
		JoinCond:           strings.Join(joinCondParts, " AND "),
		ScalarDecorrelated: true,
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
func decorrelateInSubqueries(n *Node, ctes []plansql.CTEDef) *Node {
	if n == nil {
		return nil
	}
	if len(n.CTEs) > 0 {
		ctes = n.CTEs
	}

	// Recursively process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = decorrelateInSubqueries(child, ctes)
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

		joinNode := tryDecorrelateInSubquery(inExpr, subq, outerTables, outerColMap, ctes)
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

// innerSemiJoinKey names the column the decorrelated inner plan EMITS for the
// subquery's single SELECT item — the build-side key of the semi/anti join
// tryDecorrelateInSubquery returns. ok=false declines the rewrite.
//
// The rewrite builds the inner plan as Scan → [Join …] → [Filter] →
// [Aggregate] and never a Project, so the build side carries the SOURCE
// column names of the relations it reads — which is also why the inner WHERE
// goes through stripTableQualifiers. A key spelled any other way names a
// column that is not in the build schema, and the failure is SILENT: the
// physical planner splits the condition literally, exec.HashJoin's
// FixKeyAssignment then finds the OTHER side's bare name present in the build
// schema — which on a self-join it always is — and swaps the pair, leaving
// the probe to resolve a name only the build side has. The join matches
// nothing and `WHERE a.x IN (SELECT b.x FROM t b …)` answers zero rows where
// PostgreSQL answers every one of them (#516).
//
// Two spellings the caller used to produce and nothing emits: the item's
// ALIAS (`SELECT b.id AS bid` — no Project materializes `bid`) and a
// qualifier on the subquery's own leading relation (`b.id`, where the Scan
// under it emits `id`).
func innerSemiJoinKey(info *plansql.SelectInfo) (InnerKeyRef, bool) {
	col := info.Columns[0]
	if col.IsAgg {
		// Only the GROUP BY branch below builds an aggregate, and it names
		// the output exactly this. Without a GROUP BY nothing in the inner
		// plan computes the aggregate at all.
		if len(info.GroupBy) == 0 {
			return InnerKeyRef{}, false
		}
		name := cleanExpr(col.Alias)
		if name == "" {
			name = cleanExpr(col.Expr)
		}
		// An aggregate output is computed, not read from a relation: there
		// is no qualifier for repairDecorrelatedSpelling to resolve, and the
		// name the Aggregate node declares is the name it emits.
		return InnerKeyRef{Text: name}, name != ""
	}
	ref := plainColRef(col.ASTExpr)
	if ref == nil {
		// A computed item (`b.id + 0`): no node in the inner plan
		// materializes it, and the physical planner refuses the resulting
		// non-column equi-join key outright.
		return InnerKeyRef{}, false
	}
	if ref.Column == "" {
		return InnerKeyRef{}, false
	}
	key := InnerKeyRef{Qualifier: ref.Table, Column: ref.Column}
	// Text is the spelling this rewrite commits to NOW, and the one that
	// survives if the repair cannot resolve the reference later (an
	// un-annotated Scan). Over a single relation it is provably what the
	// bottom Scan emits (#516); over a joined inner nothing is provable
	// here, so the item stays as written and repairDecorrelatedSpelling
	// settles it against the join's real output (#526).
	if ref.Table != "" && !namesInnerLeadRelation(info, ref.Table) {
		key.Text = cleanExpr(col.Expr)
	} else {
		key.Text = ref.Column
	}
	return key, true
}

// plainColRef returns node as a bare column reference, or nil when it is any
// other expression. Parentheses are transparent.
func plainColRef(node plansql.Node) *plansql.ColRef {
	switch n := node.(type) {
	case *plansql.ColRef:
		return n
	case *plansql.ParenNode:
		return plainColRef(n.Inner)
	}
	return nil
}

// namesInnerLeadRelation reports whether qualifier names the relation whose
// columns the inner plan is known to emit UNQUALIFIED — the bottom Scan of
// tryDecorrelateInSubquery's Scan → [Join …] → [Filter] → [Aggregate].
//
// That is knowable only when the subquery reads a SINGLE relation. With a
// JOIN in it, info.Tables[0] is merely the relation written first: which side
// ends up probe-most, and therefore which relation's bare names come out of
// the join, is decided by reorderJoins (optimizer.go, "Join reordering") from
// ESTIMATED ROWS at Optimize step 73 — long after decorrelateInSubqueries has
// run at step 36. Stripping a qualifier on the strength of write order then
// names whichever relation the estimator happened to put on the probe:
// `a.x IN (SELECT c.x FROM uu c JOIN tt b ON b.id = c.k)` with a small `uu`
// answers over tt.x instead of uu.x — a silent wrong answer, 2 rows where
// PostgreSQL says 1.
//
// So a joined inner reports false and its select item is left spelled as the
// user wrote it, which is what this rewrite did before #516 named the key.
// (A qualifier the join's own output does not carry is a separate, older
// defect — see #526 — but leaving it alone keeps this rewrite from turning
// one wrong answer into a different one.)
func namesInnerLeadRelation(info *plansql.SelectInfo, qualifier string) bool {
	// A COMMA join populates info.Tables, not info.Joins (the parser appends
	// the second relation to Tables), so gating on Joins alone would strip a
	// qualifier for `FROM uu c, tt b WHERE b.id = c.k` on exactly the premise
	// this rule exists to deny. Both spellings of a multi-relation inner are
	// covered by the one test.
	if qualifier == "" || len(info.Tables) == 0 || len(info.Tables) > 1 || len(info.Joins) > 0 {
		return false
	}
	lead := info.Tables[0]
	return strings.EqualFold(qualifier, lead.Alias) || strings.EqualFold(qualifier, lead.Name)
}

// innerGroupKey spells a GROUP BY term the way the inner plan's Scan emits it,
// for innerSemiJoinKey's reason: the aggregate's output column IS its group
// key's text, and a key spelled `b.g` over a Scan emitting `g` puts the
// mismatch one node higher instead of removing it.
func innerGroupKey(info *plansql.SelectInfo, term string) InnerKeyRef {
	term = cleanExpr(term)
	dot := strings.IndexByte(term, '.')
	if dot <= 0 || strings.ContainsAny(term, "() ") {
		// Bare, or an expression this rewrite does not take apart: the text
		// is the whole of what it means.
		return InnerKeyRef{Text: term}
	}
	ref := InnerKeyRef{Qualifier: term[:dot], Column: term[dot+1:], Text: term}
	if namesInnerLeadRelation(info, ref.Qualifier) {
		ref.Text = ref.Column
	}
	return ref
}

// innerRelationsAreScannable reports whether every relation the subquery's
// FROM/JOIN list names is one the decorrelation rewrites can turn into a plain
// Scan node.
//
// A DERIVED TABLE is not. The parser keeps a FROM-subquery as a table whose
// NAME is its own SQL text — `(SELECT …)` — and the plan BUILDER recognises
// that prefix and recurses into it (builder.go, "Check for derived table").
// The three decorrelations below do not: each calls NewScan on the name it was
// handed, producing a Scan of a table the catalog has never heard of. That
// scan is not an error. It yields ZERO batches, so the semi/anti join's build
// side is empty and `IN` answers nothing while `NOT IN` answers every row —
// silently, on both paths (#571).
//
// Declining is the answer rather than building the derived plan here, for the
// reason ADR-0021 §1 gives: the rewrite would then have to NAME the derived
// side's columns, and inner_key_spelling.go's model of what a subtree emits
// has no arm for a derived scan, so the reference it settled on would be a
// guess. A declined IN stays a subquery predicate, executed as written — the
// single-process path resolves it through expr.InSubquery and the stage DAG
// materializes the set (ADR-0021 §2). A slower right answer beats a wrong one.
//
// A CTE name has the same exposure by a different spelling, and it IS covered
// now that the enclosing WITH is threaded here (ctes): a CTE reference resolves
// to NewScan of the CTE's bare name — a table no catalog has — exactly as a
// derived table's SQL-text name does, so IN answered 0 and NOT IN every row
// for a CTE-fed subquery too (#535, #581). Declining routes it to the same
// materialize/local paths, which resolve the CTE (buildSubqueryPipeline merges
// the enclosing WITH). A recursive CTE is declined here as well; the
// materializer then REFUSES it rather than reading its cacheless set as empty
// (#F1, physical/in_subquery_set.go).
func innerRelationsAreScannable(info *plansql.SelectInfo, ctes []plansql.CTEDef) bool {
	cteName := make(map[string]bool, len(ctes)+len(info.CTEs))
	for _, c := range ctes {
		cteName[strings.ToLower(c.Name)] = true
	}
	for _, c := range info.CTEs {
		cteName[strings.ToLower(c.Name)] = true
	}
	scannable := func(name string) bool {
		name = strings.TrimSpace(name)
		if strings.HasPrefix(name, "(") {
			return false // derived table: builder recurses into its text
		}
		return !cteName[strings.ToLower(name)] // a CTE name is not a base scan
	}
	for _, t := range info.Tables {
		if !scannable(t.Name) {
			return false
		}
	}
	for _, j := range info.Joins {
		if !scannable(j.RightTable) {
			return false
		}
	}
	return true
}

// tryDecorrelateInSubquery attempts to convert an IN/NOT IN subquery into a
// SemiJoin (IN) or AntiJoin (NOT IN) node. Handles uncorrelated IN with
// optional GROUP BY + HAVING, and correlated IN with equality keys.
// Returns nil if decorrelation is not possible.
func tryDecorrelateInSubquery(inExpr *plansql.InExpr, subq *plansql.SubqueryNode, outerTables map[string]bool, outerColMap map[string]string, ctes []plansql.CTEDef) *Node {
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

	// A LIMIT/OFFSET says WHICH rows the subquery yields, and the semi join
	// this rewrite builds has nowhere to put that bound: its build side IS
	// the relation the subquery reads, so the membership set was the whole
	// unbounded column and `x IN (SELECT y FROM t ORDER BY y LIMIT 3)`
	// matched every row for any n (#482). Decline, and the subquery stays a
	// filter predicate — an uncorrelated one is executed once as written,
	// LIMIT, OFFSET and ORDER BY included, and its result IS the bounded
	// membership set. Deciding a bound here instead would mean re-deriving
	// the subquery's own ordering, and a per-task bound is not a global one.
	if strings.TrimSpace(info.Limit) != "" || strings.TrimSpace(info.Offset) != "" {
		return nil
	}

	// Name the inner key the way the inner plan built below actually emits
	// it. Declining here leaves the IN as a subquery filter, which is the
	// correct answer for every shape this cannot name (#516).
	innerSelectRef, ok := innerSemiJoinKey(info)
	if !ok {
		return nil
	}

	// Get outer key from InExpr.Left — must be a simple column reference
	outerKey := colRefName(inExpr.Left)
	if outerKey == "" {
		return nil
	}

	// Build inner plan: Scan → optional JOINs
	if !innerRelationsAreScannable(info, ctes) {
		return nil
	}
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
	var correlationKeys []DecorrelatedKey

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
				outerCol, innerRef, ok := extractCorrelatedRefs(cmp, outerTables, innerTableSet, outerColMap)
				if !ok {
					return nil
				}
				correlationKeys = append(correlationKeys, DecorrelatedKey{Outer: outerCol, Op: "=", Inner: innerRef})
			} else {
				// Inner-only condition (including subquery expressions)
				pred, ok := innerOnlyPredicate(node, len(info.Joins) > 0 || len(info.Tables) > 1)
				if !ok {
					return nil
				}
				innerFilterPreds = append(innerFilterPreds, pred)
			}
		}
	}

	// Apply inner-only WHERE conditions
	if len(innerFilterPreds) > 0 {
		innerPlan = NewFilter(innerPlan, innerFilterPreds)
	}

	// Handle GROUP BY + HAVING
	var groupRefs []InnerKeyRef
	if len(info.GroupBy) > 0 {
		var groupBy []string
		for _, gb := range info.GroupBy {
			ref := innerGroupKey(info, gb)
			groupRefs = append(groupRefs, ref)
			groupBy = append(groupBy, ref.spelled())
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
			ae := AggExpr{
				Func:      info.Columns[0].AggFunc,
				InputCol:  inputCol,
				OutputCol: outputCol,
				Distinct:  info.Columns[0].AggDistinct,
			}
			// See the note in tryDecorrelateScalarSubquery: an aggregate
			// whose extra arguments this rewrite cannot carry declines the
			// rewrite instead of losing them (#353).
			if err := parseAggExtraArgs(&ae, info.Columns[0].AggArgs); err != nil {
				return nil
			}
			aggs = append(aggs, ae)
			aggCounter++
		}

		// HAVING aggregates — extract from HAVING expression, assign synthetic
		// output names, then rewrite HAVING to reference those names.
		if info.HavingExpr != nil {
			havingAggs := plansql.FindAllAggregates(info.HavingExpr)
			replacements := map[string]string{}

			for _, agg := range havingAggs {
				synName := plansql.SlotName(plansql.SlotHaving, aggCounter)
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

				ae := AggExpr{
					Func:      funcName,
					InputCol:  aggInputCol,
					OutputCol: synName,
					Distinct:  agg.Distinct,
					InputExpr: aggInputExpr,
				}
				if err := parseAggExtraArgs(&ae, agg.Args); err != nil {
					return nil
				}
				aggs = append(aggs, ae)
				replacements[strings.ToLower(agg.String())] = synName
			}

			rewrittenHaving := plansql.ReplaceAllAggregates(info.HavingExpr, replacements)

			agg := NewAggregate(innerPlan, groupBy, aggs)
			agg.InnerGroupRefs = groupRefs
			innerPlan = NewFilter(agg, []Predicate{{
				Raw:     rewrittenHaving.String(),
				ASTExpr: rewrittenHaving,
			}})
		} else {
			innerPlan = NewAggregate(innerPlan, groupBy, aggs)
			innerPlan.InnerGroupRefs = groupRefs
		}
	}

	// Recursively decorrelate nested IN subqueries in the inner plan
	innerPlan = decorrelateInSubqueries(innerPlan, ctes)

	// Build join condition: outer IN column = inner SELECT column, plus any
	// correlated equalities. The build-side spelling of each is what
	// repairDecorrelatedSpelling settles once reorderJoins has decided which
	// inner relation's columns come out bare (#526).
	//
	// A grouped inner names the key by the AGGREGATE's output column, and an
	// aggregate's output column IS its group term's text — so the key
	// resolves against the group terms the same repair has already spelled
	// (repairDecorrelatedSpelling walks bottom-up for exactly that reason).
	keys := append([]DecorrelatedKey{{Outer: outerKey, Op: "=", Inner: innerSelectRef}}, correlationKeys...)

	joinType := "semi"
	if inExpr.Not {
		joinType = "anti"
	}

	return &Node{
		Type:      NodeJoin,
		Children:  []*Node{nil, innerPlan},
		JoinType:  joinType,
		JoinCond:  renderDecorrelatedKeys(keys),
		InnerKeys: keys,
		// An anti join answers "did nothing match", which is NOT what NOT IN
		// asks (#507). See Node.NullAwareAnti: only the UNCORRELATED form
		// carries the flag, because a correlated NOT IN's poison is per
		// correlation group and one flag on the operator cannot say that.
		NullAwareAnti: inExpr.Not && len(correlationKeys) == 0,
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
		// A CTEName tag marks a materialization fence: the physical
		// planner substitutes the cached CTE result for the ENTIRE tagged
		// subtree, so a predicate pushed below the tag is silently dropped
		// on replay (outer `WHERE` over a CTE reference returned the full
		// CTE — wrong results). Predicates stay above the fence and are
		// applied to the replayed batches instead.
		if child.Type == NodeProject && child.CTEName == "" {
			// Filter-Project -> Project-Filter (push filter below project).
			// #384: the swap used to be unconditional, but a predicate
			// referencing a column the Project COMPUTES (or renames) cannot
			// cross unchanged — the schema below does not carry the alias
			// (the single-process filter errored, the DAG's scan-stage
			// filter matched nothing). Each predicate is pushed as-is,
			// pushed with the alias's defining expression substituted in,
			// or kept above the Project when substitution is unsound
			// (aggregate outputs, volatile functions, subqueries). See
			// filter_project_pushdown.go.
			pushed, kept := splitFilterForProjectPush(n.Predicates, child)
			if len(pushed) == 0 {
				return n
			}
			n.Predicates = pushed
			n.Children[0] = child.Children[0]
			child.Children[0] = n
			if len(kept) > 0 {
				return NewFilter(child, kept)
			}
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

	// A WHERE predicate sits ABOVE the join, so it sees whatever the join
	// emitted — including the NULLs an outer join manufactures for a row
	// that found no partner. Pushing it below that padding is legal only
	// once the predicate is proven to reject those NULLs, and then the join
	// itself simplifies, because no padded row can survive (#335).
	kind := joinKind(join.JoinType)
	leftPadded, rightPadded := nullSupplyingSides(kind)

	// A semi/anti join emits its PROBE (left) side's columns alone; the
	// build (right) side is not visible above the join. So a predicate here
	// can only reference left-side columns, and an UNQUALIFIED name must
	// resolve against the probe side, never the build. Merging the build's
	// columns into the resolution map would let a bare name that also exists
	// on the build — a self-EXISTS decorrelates to `orders t0` over `orders
	// sub`, both carrying `o_totalprice` — resolve to the build relation and
	// be pushed onto the subquery's scan, filtering the wrong relation and
	// silently changing the row set (#584). The build side is likewise never
	// a legal push target for such a predicate.
	semiAnti := kind == "semi" || kind == "anti"

	// Merge column maps for resolving unqualified column references. For a
	// semi/anti join only the probe side's columns are in scope.
	allColMap := make(map[string]string, len(leftColMap)+len(rightColMap))
	for k, v := range leftColMap {
		allColMap[k] = v
	}
	if !semiAnti {
		for k, v := range rightColMap {
			allColMap[k] = v
		}
	}
	demoteLeft, demoteRight := false, false

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
		// A semi/anti join's build side is not a legal push target: a
		// predicate that resolves to it (e.g. a qualified build reference,
		// which cannot appear in valid SQL above the join) stays put rather
		// than filtering the subquery's scan (#584).
		if semiAnti {
			allRight = false
		}

		switch {
		case allLeft:
			switch {
			case !leftPadded:
				leftPreds = append(leftPreds, pred)
			case rejectsNulls(pred):
				leftPreds = append(leftPreds, pred)
				demoteLeft = true
			default:
				remainingPreds = append(remainingPreds, pred)
			}
		case allRight:
			switch {
			case !rightPadded:
				rightPreds = append(rightPreds, pred)
			case rejectsNulls(pred):
				rightPreds = append(rightPreds, pred)
				demoteRight = true
			default:
				remainingPreds = append(remainingPreds, pred)
			}
		default:
			remainingPreds = append(remainingPreds, pred)
		}
	}

	if demoteLeft || demoteRight {
		join.JoinType = demoteJoinKind(kind, demoteLeft, demoteRight)
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
// predicates to the appropriate child.
//
// Outer-join correctness: pushing a single-side predicate from the ON clause
// is safe only on the *inner* side of the join (the side whose unmatched rows
// are NOT padded with NULLs). Pushing on the outer side would change LEFT/
// RIGHT JOIN semantics — outer-side rows must always survive the join.
//
//	INNER JOIN  : push left + right
//	LEFT JOIN   : push right only (left is preserved)
//	RIGHT JOIN  : push left only (right is preserved)
//	FULL JOIN   : push neither (both preserved)
//	CROSS JOIN  : push left + right (no padding semantics)
//
// Without this pushdown, the non-equi part of an ON clause survives in
// JoinCond, where parseJoinKeys silently drops anything without an "=" sign,
// producing wrong results for queries like Q13 ("LEFT JOIN orders ON
// c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'").
func extractJoinCondPredicates(join *Node) *Node {
	jt := strings.ToLower(strings.TrimSpace(join.JoinType))
	pushLeft, pushRight := false, false
	switch jt {
	case "", "inner", "inner join", "join", "cross":
		pushLeft, pushRight = true, true
	case "left", "left join", "left outer", "left outer join":
		pushRight = true
	case "right", "right join", "right outer", "right outer join":
		pushLeft = true
	default:
		// full outer / semi / anti — safer to leave the ON clause alone.
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

		switch {
		case allLeft && pushLeft:
			leftParts = append(leftParts, part)
		case allRight && pushRight:
			rightParts = append(rightParts, part)
		default:
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
	var result []Predicate
	if len(commonSet) > 0 {
		// Build result: common predicates + simplified OR
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
	} else {
		// Keep the original OR predicate
		result = append(result, pred)
	}

	// Also extract per-column value sets from OR branches. When every branch
	// constrains the same column with equality (e.g., (n1.n_name = 'FRANCE' AND ...)
	// OR (n1.n_name = 'GERMANY' AND ...)), we can derive n1.n_name IN ('FRANCE',
	// 'GERMANY') and push it down independently — even though the full OR can't be
	// decomposed because the branches differ.
	// Use remaining branch terms (after common removal) to avoid redundancy.
	var remainingTerms [][]plansql.Node
	if len(commonSet) > 0 {
		for _, terms := range branchTermNodes {
			var rem []plansql.Node
			for _, t := range terms {
				if !commonSet[strings.ToLower(t.String())] {
					rem = append(rem, t)
				}
			}
			if len(rem) > 0 {
				remainingTerms = append(remainingTerms, rem)
			}
		}
	} else {
		remainingTerms = branchTermNodes
	}
	valuePreds := extractColumnValueSetsFromOR(remainingTerms)
	result = append(result, valuePreds...)

	return result
}

// extractColumnValueSetsFromOR finds columns that appear with equality comparisons
// in every OR branch and generates implied IN predicates. For example:
//
//	(n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
//	OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
//
// implies n1.n_name IN ('FRANCE', 'GERMANY') AND n2.n_name IN ('FRANCE', 'GERMANY').
// These predicates are weaker than the original OR but can be pushed down to
// individual table scans, filtering early in the join chain.
func extractColumnValueSetsFromOR(branchTermNodes [][]plansql.Node) []Predicate {
	if len(branchTermNodes) < 2 {
		return nil
	}

	type colEntry struct {
		colNode plansql.Node            // representative ColRef for the IN predicate
		values  map[string]plansql.Node // deduped literal values by string repr
	}

	// Collect column=literal pairs across all branches.
	// Track which columns appear with equality in every branch.
	colValues := make(map[string]*colEntry)
	for branchIdx, terms := range branchTermNodes {
		branchCols := make(map[string]bool)
		for _, term := range terms {
			cmp, ok := term.(*plansql.CmpExpr)
			if !ok || cmp.Op != "=" {
				continue
			}
			colNode, litNode := extractColAndLit(cmp)
			if colNode == nil {
				continue
			}
			colKey := strings.ToLower(colNode.String())
			branchCols[colKey] = true
			if entry, exists := colValues[colKey]; exists {
				litKey := litNode.String()
				if _, seen := entry.values[litKey]; !seen {
					entry.values[litKey] = litNode
				}
			} else {
				colValues[colKey] = &colEntry{
					colNode: colNode,
					values:  map[string]plansql.Node{litNode.String(): litNode},
				}
			}
		}
		// Remove columns that don't have equality in this branch
		if branchIdx > 0 {
			for colKey := range colValues {
				if !branchCols[colKey] {
					delete(colValues, colKey)
				}
			}
		}
	}

	if len(colValues) == 0 {
		return nil
	}

	// Skip when every branch is a single-column equality on the same column.
	// In that case the remaining OR (e.g., col='A' OR col='B' OR col='C') is
	// already semantically equivalent to the IN predicate, so extracting it
	// would be redundant.
	if len(colValues) == 1 {
		allSingle := true
		for _, terms := range branchTermNodes {
			if len(terms) != 1 {
				allSingle = false
				break
			}
		}
		if allSingle {
			return nil
		}
	}

	// Build IN predicates for columns with 2+ distinct values
	var result []Predicate
	for _, entry := range colValues {
		if len(entry.values) < 2 {
			continue // single value — no benefit over the original OR
		}
		vals := make([]plansql.Node, 0, len(entry.values))
		for _, v := range entry.values {
			vals = append(vals, v)
		}
		inExpr := &plansql.InExpr{
			Left:   entry.colNode,
			Values: vals,
		}
		result = append(result, Predicate{ASTExpr: inExpr, Raw: inExpr.String()})
	}
	return result
}

// extractColAndLit returns the ColRef and Lit from a CmpExpr if one side is a
// column reference and the other is a literal. Returns nil, nil otherwise.
func extractColAndLit(cmp *plansql.CmpExpr) (plansql.Node, plansql.Node) {
	_, leftIsCol := cmp.Left.(*plansql.ColRef)
	_, rightIsLit := cmp.Right.(*plansql.Lit)
	if leftIsCol && rightIsLit {
		return cmp.Left, cmp.Right
	}
	_, rightIsCol := cmp.Right.(*plansql.ColRef)
	_, leftIsLit := cmp.Left.(*plansql.Lit)
	if rightIsCol && leftIsLit {
		return cmp.Right, cmp.Left
	}
	return nil, nil
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
		// Both halves are OUTER-scope questions, so both go through the
		// scope helpers rather than through TableAlias: which names may
		// qualify a reference into this subtree, and what the enclosing
		// query calls the scan a bare column came from. Inside a derived
		// table that is the derived alias — the only name visible from
		// outside — which is what TableAlias used to be made to hold and
		// what #489 stopped overwriting it with.
		for _, name := range n.ScopeNames() {
			tables[strings.ToLower(name)] = true
		}
		tableID := strings.ToLower(n.OuterTableID())
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
func decorrelateExists(n *Node, ctes []plansql.CTEDef) *Node {
	if n == nil {
		return nil
	}
	if len(n.CTEs) > 0 {
		ctes = n.CTEs
	}

	// Recursively process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = decorrelateExists(child, ctes)
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

		joinNode := tryDecorrelateExists(existsNode, outerTables, outerColMap, ctes)
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
func tryDecorrelateExists(exists *plansql.ExistsNode, outerTables map[string]bool, outerColMap map[string]string, ctes []plansql.CTEDef) *Node {
	parsed, err := plansql.Parse(exists.SQL)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil
	}

	if len(info.Tables) == 0 {
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

	// Build inner table set from all tables and joins in the subquery
	innerTables := make(map[string]bool)
	for _, t := range info.Tables {
		innerTables[strings.ToLower(t.Name)] = true
		if t.Alias != "" {
			innerTables[strings.ToLower(t.Alias)] = true
		}
	}
	for _, j := range info.Joins {
		innerTables[strings.ToLower(j.RightTable)] = true
		if j.RightAlias != "" {
			innerTables[strings.ToLower(j.RightAlias)] = true
		}
	}

	// Flatten the subquery WHERE into individual conditions
	var whereNodes []plansql.Node
	flattenASTNodes(info.WhereExpr, &whereNodes)

	// Classify each condition
	var eqKeys []DecorrelatedKey        // equality: outer_col = inner_col, the hash join keys
	var filterConds []DecorrelatedKey   // non-equality correlated: outer_col != inner_col
	var innerFilterNodes []plansql.Node // inner-only conditions for scan filter

	for _, node := range whereNodes {
		hasOuter, hasInner := nodeTableRefs(node, outerTables, innerTables, outerColMap)

		if hasOuter && hasInner {
			// Cross-table predicate: must be a simple comparison
			cmp, ok := node.(*plansql.CmpExpr)
			if !ok {
				return nil // can't decorrelate complex cross-table predicates
			}
			outerCol, innerRef, ok := extractCorrelatedRefs(cmp, outerTables, innerTables, outerColMap)
			if !ok {
				return nil
			}
			if cmp.Op == "=" {
				eqKeys = append(eqKeys, DecorrelatedKey{Outer: outerCol, Op: "=", Inner: innerRef})
			} else {
				// JoinFilter convention is "probe(outer) OP build(inner)".
				// extractCorrelatedCols returns (outer, inner) regardless of
				// which side of the comparison each was written on, so when
				// the OUTER column was on the RIGHT (e.g. "i.date > o.date"),
				// the operator must flip ("o.date < i.date") to preserve the
				// comparison's meaning.
				op := cmp.Op
				if _, leftIsOuter := getColRefInfo(cmp.Left, outerTables, innerTables, outerColMap); !leftIsOuter {
					op = flipCmpOp(op)
				}
				filterConds = append(filterConds, DecorrelatedKey{Outer: outerCol, Op: op, Inner: innerRef})
			}
		} else if hasInner {
			innerFilterNodes = append(innerFilterNodes, node)
		}
		// Outer-only conditions in subquery WHERE shouldn't happen, skip
	}

	if len(eqKeys) == 0 {
		return nil // no equality keys → can't use hash join
	}

	// Build inner plan: Scan → optional JOINs
	if !innerRelationsAreScannable(info, ctes) {
		return nil
	}
	innerScan := NewScan(info.Tables[0].Name, info.Tables[0].Alias)
	var innerPlan *Node = innerScan
	for _, j := range info.Joins {
		rightScan := NewScan(j.RightTable, j.RightAlias)
		innerPlan = NewJoin(innerPlan, rightScan, j.Type, j.Condition)
	}

	// Apply inner-only filters
	if len(innerFilterNodes) > 0 {
		joinedInner := len(info.Joins) > 0 || len(info.Tables) > 1
		var innerPreds []Predicate
		for _, f := range innerFilterNodes {
			pred, ok := innerOnlyPredicate(f, joinedInner)
			if !ok {
				return nil
			}
			innerPreds = append(innerPreds, pred)
		}
		innerPlan = NewFilter(innerPlan, innerPreds)
	}

	// Build semi/anti join
	joinType := "semi"
	if exists.Not {
		joinType = "anti"
	}

	// The inner side of every correlated term is spelled by
	// repairDecorrelatedSpelling once reorderJoins has settled which inner
	// relation's columns the join emits bare: with a JOIN in the subquery,
	// the bare name a strip produces resolves to whichever relation the
	// estimator put on the probe, so EXISTS and NOT EXISTS both answered
	// over the other relation's column (#527).
	joinNode := &Node{
		Type:      NodeJoin,
		Children:  []*Node{nil, innerPlan}, // left child filled by caller
		JoinType:  joinType,
		JoinCond:  renderDecorrelatedKeys(eqKeys),
		InnerKeys: eqKeys,
	}
	if len(filterConds) > 0 {
		joinNode.JoinFilter = renderDecorrelatedKeys(filterConds)
		joinNode.InnerFilterKeys = filterConds
	}

	return joinNode
}

// HasRemainingSubqueries walks an optimized logical plan and returns true if
// any filter predicate still contains un-decorrelated subquery references
// (EXISTS, NOT EXISTS, IN (SELECT), scalar subqueries). After successful
// decorrelation these become semi/anti join nodes and the predicates are
// removed. If this returns false, the scan-node structure is deterministic
// and safe for distributed execution.
func HasRemainingSubqueries(n *Node) bool {
	if n == nil {
		return false
	}
	if n.Type == NodeFilter {
		for _, pred := range n.Predicates {
			if findExistsNode(pred.ASTExpr) != nil {
				return true
			}
			if inExpr, subq := findInSubqueryNode(pred.ASTExpr); inExpr != nil && subq != nil {
				return true
			}
			if hasScalarSubquery(pred.ASTExpr) {
				return true
			}
		}
	}
	for _, child := range n.Children {
		if HasRemainingSubqueries(child) {
			return true
		}
	}
	return false
}

// hasScalarSubquery checks if an AST node contains a scalar subquery reference.
func hasScalarSubquery(node plansql.Node) bool {
	if node == nil {
		return false
	}
	switch n := node.(type) {
	case *plansql.SubqueryNode:
		return true
	case *plansql.ExistsNode:
		return true
	case *plansql.ParenNode:
		return hasScalarSubquery(n.Inner)
	case *plansql.CmpExpr:
		return hasScalarSubquery(n.Left) || hasScalarSubquery(n.Right)
	case *plansql.AndNode:
		return hasScalarSubquery(n.Left) || hasScalarSubquery(n.Right)
	case *plansql.OrNode:
		return hasScalarSubquery(n.Left) || hasScalarSubquery(n.Right)
	case *plansql.NotNode:
		return hasScalarSubquery(n.Inner)
	case *plansql.InExpr:
		for _, v := range n.Values {
			if hasScalarSubquery(v) {
				return true
			}
		}
	}
	return false
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
// flipCmpOp mirrors a comparison operator for operand-order reversal:
// a OP b ⇔ b flip(OP) a. Equality and inequality are symmetric.
func flipCmpOp(op string) string {
	switch op {
	case ">":
		return "<"
	case "<":
		return ">"
	case ">=":
		return "<="
	case "<=":
		return ">="
	default: // =, !=, <> are symmetric
		return op
	}
}

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

// extractCorrelatedRefs is extractCorrelatedCols keeping the INNER side's
// relation qualifier, which the decorrelations need in order to spell the
// reference once the inner join order is final.
//
// getColRefInfo returns a bare column name for both sides. On the outer side
// that is right — the correlated predicate names the outer query's own
// columns and the semi join probes them where the outer plan emits them. On
// the inner side it is the #527 defect: `c.x` becomes `x`, and over a joined
// inner the bare `x` resolves to whichever relation reorderJoins put on the
// probe. The qualifier is kept here and resolved by
// repairDecorrelatedSpelling.
func extractCorrelatedRefs(cmp *plansql.CmpExpr, outerTables, innerTables map[string]bool, outerColMap map[string]string) (outerCol string, inner InnerKeyRef, ok bool) {
	outerCol, innerCol, ok := extractCorrelatedCols(cmp, outerTables, innerTables, outerColMap)
	if !ok {
		return "", InnerKeyRef{}, false
	}
	inner = InnerKeyRef{Column: innerCol, Text: innerCol}
	// Recover the qualifier from whichever side of the comparison was the
	// inner one. extractCorrelatedCols already decided that; matching on the
	// column name it returned identifies the side without repeating the
	// classification.
	for _, side := range []plansql.Node{cmp.Left, cmp.Right} {
		ref, isRef := side.(*plansql.ColRef)
		if !isRef || ref.Table == "" || !strings.EqualFold(ref.Column, innerCol) {
			continue
		}
		if innerTables[strings.ToLower(ref.Table)] {
			inner.Qualifier = ref.Table
			break
		}
	}
	return outerCol, inner, true
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
		// Node.ScopeNames, not TableName/TableAlias: a scan inside a derived
		// table also answers to that table's alias, and this set is exactly
		// "what may qualify an OUTER column reference" (#489 regression).
		for _, name := range n.ScopeNames() {
			tables[strings.ToLower(name)] = true
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
	// Don't reorder joins involving CTE references — the CTE's cardinality is
	// unknown at plan time, so cost estimates are unreliable and reordering can
	// break key assignment when both tables share column names.
	if hasCTERef(n.Children[0]) || hasCTERef(n.Children[1]) {
		return n
	}
	// Flatten the join chain
	var rels []*Node
	var edges []joinEdge
	flattenJoinChain(n, &rels, &edges)
	if len(rels) <= 2 {
		// For 2-way inner joins, use cardinality estimates to decide
		// probe vs build side. Larger relation should probe (stream),
		// smaller should build (hash table).
		leftStats := estimateSubtreeStats(n.Children[0])
		rightStats := estimateSubtreeStats(n.Children[1])
		if leftStats.Rows < rightStats.Rows {
			n.Children[0], n.Children[1] = n.Children[1], n.Children[0]
		}
		return n
	}
	return costBasedJoinReorder(rels, edges)
}

// hasCTERef returns true if the subtree contains a CTE reference scan.
func hasCTERef(n *Node) bool {
	if n == nil {
		return false
	}
	if n.CTEName != "" {
		return true
	}
	for _, c := range n.Children {
		if hasCTERef(c) {
			return true
		}
	}
	return false
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

	// Resolve which relation each SIDE of a condition belongs to.
	//
	// The naive answer (leftRep = leftAfter-1, rightRep = rightBefore) is
	// wrong for multi-way joins whose condition references a non-last
	// relation — the supplier-nation edge in a supplier-lineitem-orders-
	// nation chain — so both endpoints are matched against the condition's
	// own column references.
	defLeft := leftAfter - 1
	if defLeft < 0 {
		defLeft = 0
	}
	defRight := rightBefore

	leftAll := make(map[string]bool, 16)
	for i := leftStart; i < leftAfter; i++ {
		for c := range collectSubtreeColumns((*rels)[i]) {
			leftAll[c] = true
		}
	}

	// endpoints returns the (left, right) relation indexes cond relates.
	endpoints := func(cond string) (int, int) {
		condRefs := make(map[string]bool, 4)
		extractJoinColumnRefs(cond, condRefs)
		if len(condRefs) == 0 {
			return defLeft, defRight
		}
		// The RIGHT endpoint used to be left at rightBefore — the first
		// relation of the right subtree, which is the one the condition names
		// only by luck. For `part x (supplier ⋈ partsupp)` with the condition
		// `t1.ps_partkey = t2.p_partkey` the edge was recorded as
		// part-supplier, and the reorderer then hung a conjunct naming
		// partsupp on a join whose subtree does not contain it. A key that
		// resolves to nothing matches nothing, so the join answered no rows
		// and the query answered zero with no error, while the stranded
		// relation became a real cross product (#593, #594).
		//
		// Prefer a relation owning a referenced column the other side does
		// NOT expose; a shared bare name (self-joins) is ambiguous and is
		// only a fallback, which keeps the previous choice where the
		// condition says nothing new.
		right, ambiguous := -1, -1
		for i := rightBefore; i < len(*rels); i++ {
			relCols := collectSubtreeColumns((*rels)[i])
			for ref := range condRefs {
				if !relCols[ref] {
					continue
				}
				if !leftAll[ref] {
					right = i
					break
				}
				if ambiguous < 0 {
					ambiguous = i
				}
			}
			if right >= 0 {
				break
			}
		}
		if right < 0 {
			right = ambiguous
		}
		if right < 0 {
			right = defRight
		}

		// Left side: exclude the refs the chosen right relation owns, so a
		// self-join alias does not match on the wrong side.
		rightCols := collectSubtreeColumns((*rels)[right])
		left := defLeft
		for i := leftStart; i < leftAfter; i++ {
			relCols := collectSubtreeColumns((*rels)[i])
			for ref := range condRefs {
				if rightCols[ref] {
					continue
				}
				if relCols[ref] {
					left = i
					break
				}
			}
		}
		return left, right
	}

	// ONE EDGE PER CONJUNCT. A join carrying a single (leftIdx, rightIdx)
	// pair and its WHOLE condition text is only right while every conjunct
	// relates the same two relations, and a comma-join FROM list routinely
	// breaks that: the lift attaches each WHERE equality to the deepest join
	// covering its two relations, so one join can end up with
	// `l_suppkey = s_suppkey AND c_nationkey = s_nationkey` — two conjuncts
	// over THREE relations. Recorded as one edge, the reorderer moved
	// lineitem elsewhere and carried `l_suppkey = s_suppkey` onto a join
	// whose subtree no longer had it; the key resolved to nothing and
	// TPC-H Q5's revenue came back ~100x too large in its official
	// comma-join spelling. Split, each conjunct lands on a join that spans
	// the two relations it actually names — including the cycle edge, which
	// the DP applies once both its endpoints are joined.
	//
	// Splitting is only safe when every part is a self-contained comparison:
	// splitOnAnd is textual and would cut through a parenthesised OR.
	upper := strings.ToUpper(n.JoinCond)
	parts := splitOnAnd(n.JoinCond, upper)
	splittable := len(parts) > 1
	for _, part := range parts {
		if _, ok := tryParseExpr(part).(*plansql.CmpExpr); !ok {
			splittable = false
			break
		}
	}
	if !splittable {
		l, r := endpoints(n.JoinCond)
		*edges = append(*edges, joinEdge{
			leftIdx:  l,
			rightIdx: r,
			joinType: n.JoinType,
			joinCond: n.JoinCond,
		})
		return
	}

	type edgeKey struct{ l, r int }
	grouped := make(map[edgeKey][]string, len(parts))
	order := make([]edgeKey, 0, len(parts))
	for _, part := range parts {
		l, r := endpoints(part)
		k := edgeKey{l, r}
		if _, seen := grouped[k]; !seen {
			order = append(order, k)
		}
		grouped[k] = append(grouped[k], strings.TrimSpace(part))
	}
	for _, k := range order {
		*edges = append(*edges, joinEdge{
			leftIdx:  k.l,
			rightIdx: k.r,
			joinType: n.JoinType,
			joinCond: strings.Join(grouped[k], " AND "),
		})
	}
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
		// Aggregation reduces rows substantially
		base := estimateRelCost(n.Children[0])
		cost := base / 10
		if cost < 1 {
			cost = 1
		}
		return cost
	case NodeDistinct:
		base := estimateRelCost(n.Children[0])
		cost := base / 5
		if cost < 1 {
			cost = 1
		}
		return cost
	case NodeJoin:
		// Non-flattenable join subtree (e.g., outer join inside an inner join chain).
		// Estimate output as roughly max(left, right) — joins can expand or reduce.
		leftCost := estimateRelCost(n.Children[0])
		rightCost := estimateRelCost(n.Children[1])
		cost := leftCost
		if rightCost > cost {
			cost = rightCost
		}
		// Semi/anti joins reduce to at most left-side cardinality
		jt := strings.ToLower(n.JoinType)
		if jt == "semi" || jt == "anti" {
			cost = leftCost / 2
			if cost < 1 {
				cost = 1
			}
		}
		return cost
	case NodeLimit:
		// Limit caps output regardless of input size
		return 1
	case NodeProject:
		if len(n.Children) > 0 {
			return estimateRelCost(n.Children[0])
		}
		return 100
	default:
		// Unknown — moderate cost
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
	// Pre-compute which relations have selective filters.
	// Filtered relations provide selective bloom filters that eliminate
	// probe rows early, so they should be joined (as build side) first.
	filtered := make([]bool, n)
	for i, r := range rels {
		filtered[i] = hasSelectiveFilter(r)
	}

	for added := 1; added < n; added++ {
		bestNext := -1
		bestEdge := -1
		bestCost := int(^uint(0) >> 1) // MaxInt
		bestFiltered := false

		// Find the best unused relation that has an edge to the current plan.
		// Prefer filtered relations (they reduce probe volume for all subsequent joins)
		// as long as they're not excessively large (cost < 1000 ≈ <1M rows).
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
			cf := filtered[candidate] && costs[candidate] < 1000
			if bestNext < 0 {
				bestCost = costs[candidate]
				bestNext = candidate
				bestEdge = ei
				bestFiltered = cf
			} else if cf && !bestFiltered {
				// Prefer filtered candidate over unfiltered best
				bestCost = costs[candidate]
				bestNext = candidate
				bestEdge = ei
				bestFiltered = cf
			} else if !cf && bestFiltered {
				// Keep filtered best
			} else if costs[candidate] < bestCost {
				bestCost = costs[candidate]
				bestNext = candidate
				bestEdge = ei
				bestFiltered = cf
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

	// A disconnected relation joined the spine as an inner join with an EMPTY
	// condition (the "no connected relation found" arm above). An absent
	// condition IS a cross join — the physical planner has a Cartesian path
	// for it (#352) — but spelled "inner" it reached key extraction, which
	// refused the plan on the single-process path and emitted a keyless
	// hash_join the worker rejects on the DAG (#376: `FROM region t0 JOIN
	// nation t1 ON ..., supplier t2` — the comma relation contributes no ON
	// clause, each half planned alone, only the mixture failed). Walk only
	// the spine this function built; leaves are the caller's relations.
	for j := plan; j != nil && j.Type == NodeJoin && isInnerJoin(j); j = j.Children[0] {
		if strings.TrimSpace(j.JoinCond) == "" && j.JoinFilter == "" {
			j.JoinType = "cross"
		}
	}

	return plan
}

// costBasedJoinReorder uses dynamic programming (for up to 16 relations) or
// enhanced greedy to find the join order that minimizes total hash join cost,
// estimated from cardinality propagation through the join tree.
func costBasedJoinReorder(rels []*Node, edges []joinEdge) *Node {
	if len(rels) <= 16 {
		plan, bushyJoins := dpJoinReorder(rels, edges)
		// Validate: if the DP produced any inner join with an empty
		// condition (cross join where one shouldn't be), fall back to
		// the greedy algorithm which handles disconnected components.
		if hasEmptyJoinCond(plan) {
			return greedyJoinReorder(rels, edges)
		}
		if bushyJoins > 0 {
			BushyJoinsPlanned.Add(1)
			slog.Info("bushy join order chosen",
				"relations", len(rels), "bushy_joins", bushyJoins)
		}
		return plan
	}
	return greedyJoinReorder(rels, edges)
}

// hasEmptyJoinCond checks if the left-deep join spine has any inner join
// with an empty condition. Only walks the spine (DP-created joins), not
// leaf subtrees (original relations) which may legitimately contain
// cross-join-like structures from decorrelation.
func hasEmptyJoinCond(n *Node) bool {
	for n != nil && n.Type == NodeJoin {
		if isInnerJoin(n) && n.JoinCond == "" {
			return true
		}
		n = n.Children[0]
	}
	return false
}

// BushyJoinReorder enables bushy subset-partition transitions in the DP join
// reorder (docs/design/bushy-join-cbo.md §3.2). Process-wide, set once at
// startup from --bushy-join-reorder / wadjet.Config; default off. Read at
// plan time, so tests may toggle it around a planning call.
var BushyJoinReorder atomic.Bool

// BushyJoinsPlanned counts queries whose FINAL chosen join order contains at
// least one bushy join (a join of two composite intermediates). Mechanism
// marker for A/B runs: a nonzero count proves the enumeration actually
// changed a plan; dormancy tests assert it stays zero with the flag off.
var BushyJoinsPlanned atomic.Int64

// bushyMaxRels caps the subset-partition enumeration: it is O(3^N) over the
// relation count, so bushy transitions run only for N ≤ 10 (3^10 ≈ 59K
// partitions — negligible; TPC-H tops out at N=8 on Q08). Larger chains
// keep the left-deep DP.
const bushyMaxRels = 10

// dpEntry stores the optimal plan for a subset of relations in the DP table.
type dpEntry struct {
	cost       float64
	rows       float64
	width      float64 // output row width in columns (exchange cell pricing)
	colNDV     map[string]float64
	plan       *Node
	bushyJoins int // count of bushy (composite ⋈ composite) joins in plan
	// connected reports the plan joins its relations through real edges
	// only — no cross-join steps. Cross-join entries exist so disconnected
	// graphs stay plannable (validated + greedy-fallback later), but bushy
	// partitions must not compose them: a cross join has no keys, the
	// distributed executor refuses key-less joins, and a bushy pairing can
	// otherwise "launder" a cross-join subplan into a plan that LOOKS
	// edge-connected at the top (Q07's nation×nation, 2026-07-09).
	connected bool
}

// dpJoinReorder uses bitmask dynamic programming to find the optimal join
// order. For N relations, the left-deep pass evaluates O(2^N × N) states —
// each transition adds one relation as the build side of a new hash join.
// With BushyJoinReorder enabled (and N ≤ bushyMaxRels), each subset is
// additionally offered every connected two-way partition of itself
// (composite ⋈ composite), accepted only on STRICT cost improvement so
// FK→PK cost ties keep today's left-deep shapes.
//
// Returns the chosen plan and the number of bushy joins it contains
// (0 for pure left-deep plans).
func dpJoinReorder(rels []*Node, edges []joinEdge) (*Node, int) {
	n := len(rels)

	// Pre-compute statistics for each base relation
	baseStats := make([]RelStats, n)
	for i, rel := range rels {
		baseStats[i] = estimateSubtreeStats(rel)
	}

	// DP table indexed by bitmask of included relations
	maxMask := 1 << n
	dp := make([]dpEntry, maxMask)
	for i := range dp {
		dp[i].cost = math.Inf(1)
	}

	// Base cases: single relations (no join cost)
	for i := 0; i < n; i++ {
		mask := 1 << i
		dp[mask] = dpEntry{
			cost:      0,
			rows:      baseStats[i].Rows,
			width:     subtreeOutputWidth(rels[i]),
			colNDV:    baseStats[i].ColNDV,
			plan:      rels[i],
			connected: true,
		}
	}

	bushy := BushyJoinReorder.Load() && n <= bushyMaxRels

	// Fill DP table bottom-up by extending each reachable subset
	for mask := 1; mask < maxMask; mask++ {
		// Bushy pass: offer this subset every connected two-way partition of
		// itself (composite ⋈ composite). All proper submasks are numerically
		// smaller, so their entries are final by the time the loop reaches
		// mask. STRICT improvement only — a partition with a single-relation
		// build costs exactly what the left-deep transition already wrote, so
		// ties keep the left-deep shape and only genuinely cheaper bushy
		// shapes (typically: both sides pre-reduced by selective dimensions)
		// change the plan.
		if bushy && bits.OnesCount(uint(mask)) >= 3 {
			for sub := (mask - 1) & mask; sub > 0; sub = (sub - 1) & mask {
				other := mask ^ sub
				if sub < other {
					continue // each unordered pair once
				}
				if math.IsInf(dp[sub].cost, 1) || math.IsInf(dp[other].cost, 1) {
					continue
				}
				// Both sides must be edge-connected subplans (DPsub
				// invariant) — see dpEntry.connected.
				if !dp[sub].connected || !dp[other].connected {
					continue
				}
				// Collect edges crossing the cut; skip disconnected partitions
				// (no cross-join bushy shapes).
				var conds []string
				joinType := "inner"
				for _, e := range edges {
					l, r := 1<<e.leftIdx, 1<<e.rightIdx
					if (sub&l != 0 && other&r != 0) || (sub&r != 0 && other&l != 0) {
						if e.joinCond != "" {
							conds = append(conds, e.joinCond)
						}
						joinType = e.joinType
					}
				}
				if len(conds) == 0 {
					continue
				}
				// Orientation matches the 2-way rule: larger side probes.
				probe, build := sub, other
				if dp[build].rows > dp[probe].rows {
					probe, build = build, probe
				}
				joinCond := strings.Join(conds, " AND ")
				probeStats := RelStats{Rows: dp[probe].rows, ColNDV: dp[probe].colNDV}
				buildStats := RelStats{Rows: dp[build].rows, ColNDV: dp[build].colNDV}
				probeNDV, buildNDV := resolveJoinKeyNDVs(joinCond, probeStats, buildStats)
				outputRows := estimateJoinCard(dp[probe].rows, dp[build].rows, probeNDV, buildNDV, joinType)
				totalCost := dp[probe].cost + dp[build].cost +
					hashJoinCost(dp[probe].rows, dp[build].rows) +
					distributedExchangeCost(dp[probe].rows, dp[probe].width, dp[build].rows, dp[build].width)
				if totalCost < dp[mask].cost {
					dp[mask] = dpEntry{
						cost:       totalCost,
						rows:       outputRows,
						width:      dp[probe].width + dp[build].width,
						colNDV:     mergeNDVs(dp[probe].colNDV, dp[build].colNDV, outputRows),
						plan:       NewJoin(dp[probe].plan, dp[build].plan, joinType, joinCond),
						bushyJoins: dp[probe].bushyJoins + dp[build].bushyJoins + 1,
						connected:  true,
					}
				}
			}
		}

		if math.IsInf(dp[mask].cost, 1) {
			continue
		}

		// Try adding each relation not yet in the set
		for j := 0; j < n; j++ {
			if mask&(1<<j) != 0 {
				continue // j already joined
			}

			newMask := mask | (1 << j)

			// Collect all edges connecting j to relations in the current set
			var allConds []string
			joinType := "inner"
			connected := false
			for _, e := range edges {
				jLeft := e.leftIdx == j && (mask&(1<<e.rightIdx)) != 0
				jRight := e.rightIdx == j && (mask&(1<<e.leftIdx)) != 0
				if jLeft || jRight {
					connected = true
					if e.joinCond != "" {
						allConds = append(allConds, e.joinCond)
					}
					joinType = e.joinType
				}
			}

			var joinCond string
			var outputRows float64
			var joinCost float64

			if connected {
				joinCond = strings.Join(allConds, " AND ")

				// Estimate output cardinality using column NDV
				leftStats := RelStats{Rows: dp[mask].rows, ColNDV: dp[mask].colNDV}
				leftNDV, rightNDV := resolveJoinKeyNDVs(joinCond, leftStats, baseStats[j])
				outputRows = estimateJoinCard(dp[mask].rows, baseStats[j].Rows, leftNDV, rightNDV, joinType)

				// Hash join cost: build j (right side), probe current set (left side)
				joinCost = hashJoinCost(dp[mask].rows, baseStats[j].Rows)
				if bushy {
					// Exchange-aware regime: price the repartition a
					// non-broadcastable build forces (both transitions use
					// the same model so left-deep and bushy candidates
					// compare fairly). Flag-off keeps the pure-CPU model.
					joinCost += distributedExchangeCost(
						dp[mask].rows, dp[mask].width,
						baseStats[j].Rows, subtreeOutputWidth(rels[j]))
				}
			} else {
				// Cross join — heavy penalty to avoid unless necessary
				joinCond = ""
				outputRows = dp[mask].rows * baseStats[j].Rows
				joinCost = outputRows * 10
			}

			totalCost := dp[mask].cost + joinCost

			if totalCost < dp[newMask].cost {
				newPlan := NewJoin(dp[mask].plan, rels[j], joinType, joinCond)
				ndvs := mergeNDVs(dp[mask].colNDV, baseStats[j].ColNDV, outputRows)

				dp[newMask] = dpEntry{
					cost:       totalCost,
					rows:       outputRows,
					width:      dp[mask].width + subtreeOutputWidth(rels[j]),
					colNDV:     ndvs,
					plan:       newPlan,
					bushyJoins: dp[mask].bushyJoins,
					connected:  dp[mask].connected && connected,
				}
			}
		}
	}

	// Bushy join enumeration is DEFERRED.
	//
	// A bushy DP pass (enumerating non-trivial subset partitions) was
	// prototyped here. With Selinger's floor at max(L,R), bushy didn't
	// change costs on FK→PK shapes — but it changed PLAN STRUCTURES,
	// and the physical planner's column-resolution pipeline
	// (parseJoinKeys + fixJoinKeyOrder + scan-alias propagation) has
	// implicit assumptions about left-deep nesting that bushy plans
	// violate. Result: Q02/Q07/Q08/Q09/Q21 returned wrong rows at
	// SF0.01 even with same-cost bushy alternatives selected.
	//
	// Doing bushy properly requires:
	//   - A column-resolution pass that handles arbitrary Join
	//     subtrees on both sides without leaking unqualified names
	//   - Self-join column qualification that's structure-independent
	//   - Stronger physical-plan invariants captured as snapshot tests
	//
	// All multi-day work. Left as Phase 4. Current DP gives optimal
	// left-deep plans; with HLL+histogram inputs that's already a
	// substantial improvement over the rule-based predecessor.

	fullMask := maxMask - 1
	if math.IsInf(dp[fullMask].cost, 1) {
		// Disconnected graph — fall back to greedy
		return greedyJoinReorder(rels, edges), 0
	}

	return dp[fullMask].plan, dp[fullMask].bushyJoins
}

// hasSelectiveFilter checks whether a relation subtree contains a filter
// (NodeFilter or scan predicates) that reduces its output. Filtered relations
// provide selective bloom filters during hash join, making them beneficial
// as early build sides in multi-way join chains.
func hasSelectiveFilter(n *Node) bool {
	if n == nil {
		return false
	}
	if n.Type == NodeFilter {
		return true
	}
	if n.Type == NodeScan {
		return len(n.ScanPredicates) > 0 || len(n.PartitionFilter) > 0
	}
	for _, child := range n.Children {
		if hasSelectiveFilter(child) {
			return true
		}
	}
	return false
}

func copyBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
