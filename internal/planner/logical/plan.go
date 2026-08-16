// Package logical provides logical query plan representation and optimization.
package logical

import (
	"fmt"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ScanColumnStats holds aggregated column statistics from the catalog.
type ScanColumnStats struct {
	MinValue  any
	MaxValue  any
	NullCount int64
	TotalRows int64
	// NDV is the catalog's merged HyperLogLog estimate of distinct values
	// across all files of this column. Zero means HLL wasn't collected
	// (legacy files / pre-ANALYZE state); planner falls back to the
	// min/max-range heuristic. When >0 it's preferred — orders of
	// magnitude more accurate for sparse-int / string columns where the
	// range overstates true cardinality.
	NDV int64
	// Histogram is the catalog's equi-depth histogram for this column,
	// merged across files from reservoir samples. Nil when not collected.
	// Used by estimatePredSelectivity to compute range/equality
	// selectivity from real value distributions instead of hardcoded
	// fractions (0.33 for <, 0.1 for =). The opaque any allows the
	// logical package to receive *catalog.Histogram without importing
	// the catalog package (avoids circular import).
	Histogram any
}

// NodeType identifies the kind of logical plan node.
type NodeType int

const (
	NodeScan NodeType = iota
	NodeFilter
	NodeProject
	NodeAggregate
	NodeSort
	NodeLimit
	NodeJoin
	NodeDistinct
	NodeWindow
	NodeUnion
	NodeIntersect
	NodeExcept
	NodeDual // single-row, zero-column source for table-less SELECT
)

func (n NodeType) String() string {
	switch n {
	case NodeScan:
		return "Scan"
	case NodeFilter:
		return "Filter"
	case NodeProject:
		return "Project"
	case NodeAggregate:
		return "Aggregate"
	case NodeSort:
		return "Sort"
	case NodeLimit:
		return "Limit"
	case NodeJoin:
		return "Join"
	case NodeDistinct:
		return "Distinct"
	case NodeWindow:
		return "Window"
	case NodeUnion:
		return "Union"
	case NodeIntersect:
		return "Intersect"
	case NodeExcept:
		return "Except"
	case NodeDual:
		return "Dual"
	default:
		return fmt.Sprintf("Unknown(%d)", int(n))
	}
}

// Node is a node in the logical query plan tree.
type Node struct {
	Type     NodeType
	Children []*Node

	// Scan
	TableName       string
	TableAlias      string
	ScanColumns     []string          // column names available from this scan (populated by physical planner)
	RequiredColumns []string          // columns actually needed from this scan (set by optimizer column pruning)
	PartitionFilter map[string]string // extracted partition key filters (year, month, day, hour)
	ScanPredicates  []Predicate       // pushed-down filter predicates for row group pruning
	ScanRowEstimate int64             // estimated row count from manifest (0 = unknown)
	ScanColStats    map[string]ScanColumnStats // aggregated column stats from catalog (nil = unavailable)
	SampleMethod    string            // TABLESAMPLE method: BERNOULLI, SYSTEM
	SamplePercent   float64           // percentage for TABLESAMPLE (0-100)

	// Table Function (e.g., read_json, read_csv, unnest)
	IsTableFunc     bool              // true if this scan reads from a table function
	FuncName        string            // function name (e.g., "read_json")
	FuncArgs        []string          // positional arguments (e.g., URL/path)
	FuncNamedArgs   map[string]string // named arguments (e.g., delimiter="|")
	WithOrdinality  bool              // UNNEST(...) WITH ORDINALITY
	FuncColAliases  []string          // AS alias(col1, col2, ...)

	// Filter
	Predicates []Predicate

	// Project
	Projections []Projection
	// SecurityBarrier marks a projection injected by ABAC column-policy
	// enforcement (InjectColumnPolicies): masked columns replaced with
	// literal expressions, denied columns absent. The physical planner
	// must APPLY it at the scan (distributed walkStages treats ordinary
	// Projects as passthrough — a dropped barrier would leak raw values).
	SecurityBarrier bool

	// Aggregate
	GroupBy          []string
	GroupByExprs     []plansql.Node // AST for GROUP BY expressions (may be nil)
	AggExprs         []AggExpr
	GroupingSetNulls []string   // columns that should be NULL in this grouping set (legacy, per-node)
	GroupingSets     [][]string // single-pass grouping sets: each entry lists the columns in that set

	// Sort
	OrderBy []OrderExpr

	// Limit
	LimitVal  int
	OffsetVal int

	// Join
	JoinType      string // inner, left, right, full, cross, semi, anti
	JoinCond      string
	JoinFilter    string // non-equality join conditions for semi/anti join
	LeftKeys      []string
	RightKeys     []string
	NeededColumns []string // columns the parent needs from this join's output (set by optimizer)

	// Window
	WindowExprs []WindowExpr

	// Union
	UnionAll bool // true = UNION ALL, false = UNION (dedup)

	// CTEs — stored on the root node so the physical planner can resolve
	// CTE references inside scalar subqueries (e.g., Q15's HAVING/WHERE
	// subquery that references a CTE defined in the outer WITH clause).
	CTEs []plansql.CTEDef

	// CTEName — set on the root of a CTE sub-plan so the physical planner
	// can detect and materialize multi-referenced CTEs.
	CTEName string

	// ScalarDecorrelated marks a LEFT join produced by
	// decorrelateScalarSubqueries (children[1] is the grouped aggregate
	// materializing the subquery result). reduceDecorrelatedScalarAggs
	// uses it after predicate pushdown to semijoin-reduce the aggregate's
	// input by the outer plan's key-source branch.
	ScalarDecorrelated bool
}

// Predicate is a filter condition.
type Predicate struct {
	Column  string
	Op      string // =, !=, <, <=, >, >=, is_null, is_not_null, in, between
	Value   any
	Raw     string         // raw SQL expression
	ASTExpr plansql.Node // compiled AST expression node
	// PruneOnly marks predicates attached solely for storage-level pruning
	// (row-group stats / dictionary probes). The cardinality estimator
	// ignores them: attaching AST-decomposed conjuncts must not shift
	// distributed plan choices (Q08's orders join flipped shuffle →
	// broadcast off a 0.33^n selectivity guess the moment they appeared).
	// Estimate-visible attachment is a separate, SF100-validated change.
	PruneOnly bool
}

// Projection is a column expression in a SELECT.
type Projection struct {
	Column  string         // column reference
	Alias   string         // output name
	Expr    string         // raw expression
	IsAgg   bool
	ASTExpr plansql.Node // compiled AST expression node (nil for aggregates)
}

// AggExpr is an aggregation expression.
type AggExpr struct {
	Func      string // sum, count, min, max, avg
	InputCol  string
	OutputCol string
	Distinct  bool           // COUNT(DISTINCT col)
	InputExpr plansql.Node   // AST for aggregate argument (nil for simple column refs)
}

// WindowFrameSpec describes a window frame specification.
type WindowFrameSpec struct {
	Mode  string // "rows" or "range"
	Start WindowBound
	End   WindowBound
}

// WindowBound describes one end of a window frame.
type WindowBound struct {
	Type   string // "unbounded_preceding", "preceding", "current_row", "following", "unbounded_following"
	Offset int
}

// WindowExpr is a window function expression.
type WindowExpr struct {
	Func        string // row_number, rank, dense_rank, sum, count, avg, min, max
	InputCol    string // for aggregate window functions
	OutputCol   string
	PartitionBy []string
	OrderBy     []OrderExpr
	Frame       *WindowFrameSpec
}

// OrderExpr is a sort expression.
type OrderExpr struct {
	Column     string
	Desc       bool
	NullsFirst *bool // nil = default, true = NULLS FIRST, false = NULLS LAST
}

// StripTopSortLimit removes the outermost Sort and Limit nodes from a logical
// plan. Used by probe-split pipeline: each worker produces partial aggregates
// without ordering or truncation; the coordinator merges and applies final
// sort + limit.
func StripTopSortLimit(plan *Node) *Node {
	if plan == nil {
		return nil
	}
	n := plan
	if n.Type == NodeLimit && len(n.Children) > 0 {
		n = n.Children[0]
	}
	if n.Type == NodeSort && len(n.Children) > 0 {
		n = n.Children[0]
	}
	return n
}

// ExtractMergeInfo extracts the top-level aggregate and sort/limit information
// needed to merge probe-split partial results. Returns nil if the plan doesn't
// have a top-level aggregate (probe-split merge not needed).
func ExtractMergeInfo(plan *Node) *MergeInfo {
	if plan == nil {
		return nil
	}
	mi := &MergeInfo{}
	n := plan
	if n.Type == NodeLimit {
		mi.Limit = n.LimitVal
		if len(n.Children) > 0 {
			n = n.Children[0]
		}
	}
	if n.Type == NodeSort {
		mi.OrderBy = n.OrderBy
		if len(n.Children) > 0 {
			n = n.Children[0]
		}
	}
	// Capture Project rename mappings so we can translate logical aggregate
	// column names to the post-projection names that appear in worker batches.
	var projections []Projection
	if n.Type == NodeProject && len(n.Children) > 0 {
		projections = n.Projections
		n = n.Children[0]
	}
	if n.Type == NodeAggregate {
		mi.GroupBy = append([]string(nil), n.GroupBy...)
		mi.AggExprs = append([]AggExpr(nil), n.AggExprs...)
		mi.HasAggregate = true
		// Apply projection renames: map pre-projection names to aliases
		if len(projections) > 0 {
			rename := make(map[string]string, len(projections))
			for _, p := range projections {
				src := p.Column
				if src == "" {
					src = p.Expr
				}
				dst := p.Alias
				if dst == "" {
					dst = src
				}
				if src != dst {
					rename[src] = dst
				}
			}
			for i, col := range mi.GroupBy {
				if alias, ok := rename[col]; ok {
					mi.GroupBy[i] = alias
				}
			}
			for i, ae := range mi.AggExprs {
				if alias, ok := rename[ae.OutputCol]; ok {
					mi.AggExprs[i].OutputCol = alias
				}
			}
		}
		return mi
	}
	// DISTINCT: dedup across partials. Equivalent to GROUP BY all output columns.
	if n.Type == NodeDistinct {
		mi.HasDistinct = true
		return mi
	}
	// Non-aggregate, non-distinct query: merge by concatenation (UNION ALL).
	// Sort/limit are applied after concatenation if present. Even bare joins
	// with no ORDER BY or GROUP BY can probe-split — each worker produces its
	// partition's rows and the coordinator concatenates them.
	return mi
}

// MergeInfo describes how to merge probe-split partial results.
type MergeInfo struct {
	GroupBy      []string
	AggExprs     []AggExpr
	OrderBy      []OrderExpr
	Limit        int
	HasAggregate bool
	HasDistinct  bool // DISTINCT requires deduplication across partials
}

// InjectRowFilter walks the logical plan tree and wraps Scan nodes for the
// given table with an additional Filter node containing the row filter predicate.
// This is used by row-level security policies to restrict which rows a role can see.
func InjectRowFilter(plan *Node, tableName, filterSQL string) *Node {
	ast, err := plansql.ParseExpression(filterSQL)
	if err != nil {
		return plan // don't break queries on malformed filter
	}
	return injectRowFilter(plan, tableName, filterSQL, ast)
}

func injectRowFilter(n *Node, tableName, raw string, ast plansql.Node) *Node {
	if n == nil {
		return nil
	}

	// Process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = injectRowFilter(child, tableName, raw, ast)
	}

	// If this is a Scan for the target table, wrap it in a Filter
	if n.Type == NodeScan && (n.TableName == tableName || n.TableAlias == tableName) {
		filterNode := NewFilter(n, []Predicate{{
			Raw:     raw,
			ASTExpr: ast,
		}})
		return filterNode
	}

	return n
}

// ColumnPolicy describes a column-level security policy for plan-level enforcement.
type ColumnPolicy struct {
	Column   string
	Denied   bool   // true = column excluded from results
	MaskExpr string // non-empty = column replaced with this value (e.g., "'***'", "0")
}

// InjectColumnPolicies walks the logical plan and inserts a security projection
// above Scan nodes for the given table. Denied columns are removed and masked
// columns are replaced with literal expressions. This ensures restricted data
// never enters the execution pipeline.
func InjectColumnPolicies(plan *Node, tableName string, policies []ColumnPolicy) *Node {
	if len(policies) == 0 {
		return plan
	}
	return injectColumnPolicies(plan, tableName, policies)
}

func injectColumnPolicies(n *Node, tableName string, policies []ColumnPolicy) *Node {
	if n == nil {
		return nil
	}

	// Process children first (bottom-up)
	for i, child := range n.Children {
		n.Children[i] = injectColumnPolicies(child, tableName, policies)
	}

	// If this is a Scan for the target table, wrap in a security projection
	if n.Type == NodeScan && (n.TableName == tableName || n.TableAlias == tableName) {
		denySet := make(map[string]bool)
		maskMap := make(map[string]string)
		for _, p := range policies {
			if p.Denied {
				denySet[p.Column] = true
			} else if p.MaskExpr != "" {
				maskMap[p.Column] = p.MaskExpr
			}
		}

		// Build projection list from scan columns
		// ScanColumns may not be populated yet, so we also check RequiredColumns
		cols := n.ScanColumns
		if len(cols) == 0 {
			cols = n.RequiredColumns
		}
		if len(cols) == 0 {
			// No column info yet — can't rewrite projection. The post-execution
			// fallback in applyColumnPolicies will handle this case.
			return n
		}

		var projections []Projection
		for _, col := range cols {
			if denySet[col] {
				continue // skip denied columns
			}
			if mask, ok := maskMap[col]; ok {
				// Replace column with mask expression
				maskAST, err := plansql.ParseExpression(mask)
				if err != nil {
					// Fallback to string literal mask
					projections = append(projections, Projection{
						Alias: col,
						Expr:  "'" + mask + "'",
					})
				} else {
					projections = append(projections, Projection{
						Alias:   col,
						Expr:    mask,
						ASTExpr: maskAST,
					})
				}
			} else {
				projections = append(projections, Projection{
					Column: col,
					Alias:  col,
				})
			}
		}

		if len(projections) == 0 {
			return n // all columns denied — shouldn't happen (access denied upstream)
		}

		projectNode := &Node{
			Type:            NodeProject,
			Children:        []*Node{n},
			Projections:     projections,
			SecurityBarrier: true,
		}
		return projectNode
	}

	return n
}

// NewScan creates a scan node.
func NewScan(table, alias string) *Node {
	return &Node{Type: NodeScan, TableName: table, TableAlias: alias}
}

// NewFilter creates a filter node.
func NewFilter(child *Node, predicates []Predicate) *Node {
	return &Node{Type: NodeFilter, Children: []*Node{child}, Predicates: predicates}
}

// NewProject creates a projection node.
func NewProject(child *Node, projections []Projection) *Node {
	return &Node{Type: NodeProject, Children: []*Node{child}, Projections: projections}
}

// NewAggregate creates an aggregate node.
func NewAggregate(child *Node, groupBy []string, aggs []AggExpr) *Node {
	return &Node{Type: NodeAggregate, Children: []*Node{child}, GroupBy: groupBy, AggExprs: aggs}
}

// NewSort creates a sort node.
func NewSort(child *Node, orderBy []OrderExpr) *Node {
	return &Node{Type: NodeSort, Children: []*Node{child}, OrderBy: orderBy}
}

// NewLimit creates a limit node.
func NewLimit(child *Node, limit, offset int) *Node {
	return &Node{Type: NodeLimit, Children: []*Node{child}, LimitVal: limit, OffsetVal: offset}
}

// NewDistinct creates a distinct node.
func NewDistinct(child *Node) *Node {
	return &Node{Type: NodeDistinct, Children: []*Node{child}}
}

// NewWindow creates a window node.
func NewWindow(child *Node, exprs []WindowExpr) *Node {
	return &Node{Type: NodeWindow, Children: []*Node{child}, WindowExprs: exprs}
}

// NewJoin creates a join node.
func NewJoin(left, right *Node, joinType, condition string) *Node {
	return &Node{
		Type:     NodeJoin,
		Children: []*Node{left, right},
		JoinType: joinType,
		JoinCond: condition,
	}
}

// NewUnion creates a union node. If all is true, it represents UNION ALL
// (no deduplication); otherwise it represents UNION (with deduplication).
func NewUnion(left, right *Node, all bool) *Node {
	return &Node{
		Type:     NodeUnion,
		Children: []*Node{left, right},
		UnionAll: all,
	}
}

// NewIntersect creates an intersect node. Returns only rows present in both sides.
// If all is true (INTERSECT ALL), preserves duplicates; otherwise deduplicates.
func NewIntersect(left, right *Node, all bool) *Node {
	return &Node{
		Type:     NodeIntersect,
		Children: []*Node{left, right},
		UnionAll: all, // reuse field: true = ALL variant
	}
}

// NewExcept creates an except node. Returns rows from left that are not in right.
// If all is true (EXCEPT ALL), preserves duplicates; otherwise deduplicates.
func NewExcept(left, right *Node, all bool) *Node {
	return &Node{
		Type:     NodeExcept,
		Children: []*Node{left, right},
		UnionAll: all, // reuse field: true = ALL variant
	}
}

// PrettyPrint returns a formatted string representation of the plan tree.
func (n *Node) PrettyPrint(indent int) string {
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}

	var s string
	switch n.Type {
	case NodeScan:
		s = fmt.Sprintf("%sScan: %s", prefix, n.TableName)
		if n.TableAlias != "" && n.TableAlias != n.TableName {
			s += fmt.Sprintf(" AS %s", n.TableAlias)
		}
	case NodeFilter:
		s = fmt.Sprintf("%sFilter: %v", prefix, n.predicateStrings())
	case NodeProject:
		cols := make([]string, len(n.Projections))
		for i, p := range n.Projections {
			if p.Alias != "" {
				cols[i] = p.Alias
			} else {
				cols[i] = p.Expr
			}
		}
		s = fmt.Sprintf("%sProject: %v", prefix, cols)
	case NodeAggregate:
		aggs := make([]string, len(n.AggExprs))
		for i, a := range n.AggExprs {
			distinct := ""
		if a.Distinct {
			distinct = "DISTINCT "
		}
		aggs[i] = fmt.Sprintf("%s(%s%s) AS %s", a.Func, distinct, a.InputCol, a.OutputCol)
		}
		s = fmt.Sprintf("%sAggregate: group_by=%v aggs=%v", prefix, n.GroupBy, aggs)
	case NodeSort:
		s = fmt.Sprintf("%sSort: %v", prefix, n.orderStrings())
	case NodeLimit:
		s = fmt.Sprintf("%sLimit: %d offset: %d", prefix, n.LimitVal, n.OffsetVal)
	case NodeJoin:
		s = fmt.Sprintf("%sJoin: %s ON %s", prefix, n.JoinType, n.JoinCond)
	case NodeDistinct:
		s = fmt.Sprintf("%sDistinct", prefix)
	case NodeWindow:
		wins := make([]string, len(n.WindowExprs))
		for i, w := range n.WindowExprs {
			wins[i] = fmt.Sprintf("%s(%s) OVER(partition_by=%v order_by=%v) AS %s",
				w.Func, w.InputCol, w.PartitionBy, w.OrderBy, w.OutputCol)
		}
		s = fmt.Sprintf("%sWindow: %v", prefix, wins)
	case NodeUnion:
		mode := "UNION"
		if n.UnionAll {
			mode = "UNION ALL"
		}
		s = fmt.Sprintf("%s%s", prefix, mode)
	case NodeIntersect:
		mode := "INTERSECT"
		if n.UnionAll {
			mode = "INTERSECT ALL"
		}
		s = fmt.Sprintf("%s%s", prefix, mode)
	case NodeExcept:
		mode := "EXCEPT"
		if n.UnionAll {
			mode = "EXCEPT ALL"
		}
		s = fmt.Sprintf("%s%s", prefix, mode)
	default:
		s = fmt.Sprintf("%s%s", prefix, n.Type)
	}

	for _, child := range n.Children {
		s += "\n" + child.PrettyPrint(indent+1)
	}
	return s
}

func (n *Node) predicateStrings() []string {
	strs := make([]string, len(n.Predicates))
	for i, p := range n.Predicates {
		if p.Raw != "" {
			strs[i] = p.Raw
		} else {
			strs[i] = fmt.Sprintf("%s %s %v", p.Column, p.Op, p.Value)
		}
	}
	return strs
}

func (n *Node) orderStrings() []string {
	strs := make([]string, len(n.OrderBy))
	for i, o := range n.OrderBy {
		dir := "ASC"
		if o.Desc {
			dir = "DESC"
		}
		strs[i] = fmt.Sprintf("%s %s", o.Column, dir)
	}
	return strs
}
