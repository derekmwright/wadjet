// Package logical provides logical query plan representation and optimization.
package logical

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// DecimalMeta carries a DECIMAL column's declared precision and scale — the
// two facts a bare parquet.TypeID cannot express. See Node.ScanColDecimal.
type DecimalMeta struct {
	Precision int
	Scale     int
}

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
	ScanColumns     []string                   // column names available from this scan (populated by physical planner)
	RequiredColumns []string                   // columns actually needed from this scan (set by optimizer column pruning)
	PartitionFilter map[string]string          // extracted partition key filters (year, month, day, hour)
	ScanPredicates  []Predicate                // pushed-down filter predicates for row group pruning
	ScanRowEstimate int64                      // estimated row count from manifest (0 = unknown)
	ScanColStats    map[string]ScanColumnStats // aggregated column stats from catalog (nil = unavailable)
	// ScanColTypes maps this scan's lower-cased column names to their
	// catalog types (populated by physical.AnnotateScanColumns alongside
	// ScanColumns). It is what lets the planner declare a MIN/MAX output
	// type, which follows the input column rather than the function.
	ScanColTypes map[string]parquet.TypeID
	// ScanColDecimal maps this scan's lower-cased column names to their
	// DECIMAL precision/scale — entries exist only for TypeDecimal columns.
	// Populated by physical.AnnotateScanColumns alongside ScanColTypes. It is
	// what lets a zero-row DECIMAL result declare the same PostgreSQL typmod
	// a non-empty one does: ScanColTypes alone carries the bare TypeID, which
	// left every declared-schema DECIMAL column at Precision=0 (typmod -1,
	// "unconstrained") even when the underlying column had a real (p,s)
	// (#458).
	ScanColDecimal    map[string]DecimalMeta
	FilterOnlyColumns []string // columns needed ONLY by the filter directly above this scan (candidates for scan-level filter evaluation without materialization)
	ShapeOnlyColumns  []string // byte-array columns whose EVERY use in the plan reads shape, not contents (LENGTH/IS NULL/= ''/COUNT) — the scan decodes them as lengths, see shape_only_columns.go
	SampleMethod      string   // TABLESAMPLE method: BERNOULLI, SYSTEM
	SamplePercent     float64  // percentage for TABLESAMPLE (0-100)

	// Table Function (e.g., read_json, read_csv, unnest)
	IsTableFunc    bool              // true if this scan reads from a table function
	FuncName       string            // function name (e.g., "read_json")
	FuncArgs       []string          // positional arguments (e.g., URL/path)
	FuncNamedArgs  map[string]string // named arguments (e.g., delimiter="|")
	WithOrdinality bool              // UNNEST(...) WITH ORDINALITY
	FuncColAliases []string          // AS alias(col1, col2, ...)

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
	// PreservesAggOutputs marks a synthetic Project inserted by a rewrite
	// directly above an Aggregate that passes every aggregate output
	// through under its original name (possibly adding finalizations,
	// e.g. the two-level AVG division). The physical builder's
	// aggregate-ancestor resolution walks through such projections so
	// SELECT-list aggregate references still resolve by output name.
	PreservesAggOutputs bool
	// ScanStrictIntCols marks scan columns whose vectors are plain
	// Int64/Int32 at runtime — exactly the set expr.BinOpNumeric resolves
	// to integer arithmetic. Planner-side int typing must stay a subset
	// of the runtime rule (a declared-int column over a float-mode expr
	// would read as NULL through the typed getter), so this is
	// deliberately narrower than ScanIntCols' int-class set.
	ScanStrictIntCols map[string]bool
	// ScanIntCols marks scan columns whose types land on the typed
	// integer aggregation paths (set by physical.AnnotateScanColumns;
	// consumed by the two-level distinct rewrite's cost gate).
	ScanIntCols map[string]bool

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

	// BuildSideDedup marks a Distinct the PLANNER inserted, not one the
	// user wrote. Two passes create them: dedupSemiAntiBuildSide bounds a
	// semi/anti join's build hashtable by NDV, and scalar_agg_semijoin
	// builds a decorrelated semijoin's key source. Neither carries
	// user-visible semantics — a semi/anti join's result does not depend
	// on whether its build side has duplicates — and the physical planner
	// has dedicated handling for the shape (estimateDistinctKeyBytes sizes
	// Distinct(Project) subtrees by key count; the distinct-pair semi/anti
	// build fast path matches on it).
	//
	// rewriteDistinctAsGroupBy therefore leaves a marked Distinct alone and
	// rewrites every UNMARKED one — those are user SELECT DISTINCTs, and
	// each is an answer the engine has to actually compute (#466).
	BuildSideDedup bool
}

// Predicate is a filter condition.
type Predicate struct {
	Column string
	Op     string // =, !=, <, <=, >, >=, is_null, is_not_null, in, between
	Value  any
	// ValueText is a numeric Value's exact source text. Value is boxed for
	// arithmetic and a float64 cannot hold a DECIMAL past ~15-16 significant
	// digits, so the text is what the scan's prune and the row-at-a-time
	// filter convert at the column's own scale (#452). Empty when Value did
	// not come from a numeric literal.
	ValueText string
	Raw       string       // raw SQL expression
	ASTExpr   plansql.Node // compiled AST expression node
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
	Column  string // column reference
	Alias   string // output name
	Expr    string // raw expression
	IsAgg   bool
	ASTExpr plansql.Node // compiled AST expression node (nil for aggregates)
	// Hidden marks a projection the planner added for its own use rather
	// than one the user selected: the materialized value of an ORDER BY term
	// the SELECT list does not carry (#320). It computes and sorts like any
	// other projection, and is dropped before the rows reach the client —
	// extractOutputRenames leaves it out of the DAG's output schema, and
	// hiddenSortTrimOp drops it on the single-process pipeline.
	Hidden bool
}

// AggExpr is an aggregation expression.
type AggExpr struct {
	Func      string // sum, count, min, max, avg
	InputCol  string
	OutputCol string
	Distinct  bool         // COUNT(DISTINCT col)
	InputExpr plansql.Node // AST for aggregate argument (nil for simple column refs)
	// InputCol2, Separator and Percentile hold the arguments after the
	// first, for the functions that take more than one: the second column
	// of CORR/COVAR_SAMP/COVAR_POP and the ordering column of
	// MIN_BY/MAX_BY, STRING_AGG's separator literal, and
	// PERCENTILE_CONT/PERCENTILE_DISC's fraction. See parseAggExtraArgs.
	//
	// InputCol stays a single column name throughout: column pruning and
	// requiredColumns read it as one, and InputCol2 is registered beside
	// it rather than packed into the same string.
	InputCol2  string
	Separator  string
	Percentile float64
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
	InputCol    string // the argument list, verbatim — see InputColumn
	OutputCol   string
	PartitionBy []string
	OrderBy     []OrderExpr
	Frame       *WindowFrameSpec
}

// InputColumn returns the COLUMN argument of a window expression.
//
// InputCol carries the whole argument list as one string, so LAG, LEAD and
// NTH_VALUE spell their column alongside an offset, a default or an N —
// "l_quantity, 2, 0". The column is everything before the first comma;
// everything after belongs to the function, not to any table. NTILE's single
// argument is a bucket count and names no column at all.
//
// Every consumer needs the same answer: the column-pruning rule needs it to
// keep the column readable, and the physical planner needs it to type the
// output. Taking the raw string for a column name pruned the real one out of
// the scan, and the window operator then found no input vector and
// nil-dereferenced it — a crash, not a wrong answer.
func (w WindowExpr) InputColumn() string {
	if strings.EqualFold(strings.TrimSpace(w.Func), "ntile") {
		return ""
	}
	return strings.TrimSpace(strings.SplitN(w.InputCol, ",", 2)[0])
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
		mi.HasLimit = n.LimitVal != NoLimit
		mi.Offset = n.OffsetVal
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
	//
	// A CHAIN, not a single node: a derived table's own SELECT list and the
	// outer query's are separate Projects and nothing merges them, so
	// `SELECT c FROM (SELECT COUNT(*) AS c FROM t) u` stacks two above the
	// aggregate. Stopping at the first left the aggregate invisible, and a
	// probe-split merge then CONCATENATED the workers' partial groups
	// instead of re-aggregating them. Renames compose innermost-outward.
	var chain []*Node
	for n.Type == NodeProject && len(n.Children) > 0 {
		chain = append(chain, n)
		n = n.Children[0]
	}
	if n.Type == NodeAggregate {
		mi.GroupBy = append([]string(nil), n.GroupBy...)
		mi.AggExprs = append([]AggExpr(nil), n.AggExprs...)
		mi.HasAggregate = true
		for i := len(chain) - 1; i >= 0; i-- {
			mi.applyProjectionRenames(chain[i].Projections)
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

// applyProjectionRenames maps the aggregate's output names through one
// projection level, so the merge keys match the names the worker batches
// carry after that projection runs.
func (mi *MergeInfo) applyProjectionRenames(projections []Projection) {
	if len(projections) == 0 {
		return
	}
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

// MergeInfo describes how to merge probe-split partial results.
type MergeInfo struct {
	GroupBy  []string
	AggExprs []AggExpr
	OrderBy  []OrderExpr
	// Limit is the top-level LIMIT value; meaningful only when HasLimit is
	// true. HasLimit is false both when the statement has no LIMIT at all
	// and when it has only an OFFSET (NoLimit) — a companion bool rather
	// than folding NoLimit into Limit itself, because the many test
	// literals that construct a MergeInfo{} directly (never touching
	// Limit) must keep meaning "unbounded" by leaving both fields at their
	// zero value. Before HasLimit existed, `Limit > 0` doubled as that
	// same test, so a probe-split merge for `... LIMIT 0` silently kept
	// every row instead of zero (#481) — test HasLimit, never `Limit > 0`.
	Limit    int
	HasLimit bool
	// Offset is the top-level OFFSET. Rows to keep are [Offset, Offset+Limit)
	// — a merge that truncates to Limit before skipping Offset returns the
	// first page for every page (#337).
	Offset       int
	HasAggregate bool
	HasDistinct  bool // DISTINCT requires deduplication across partials
}

// KeepRows is the number of rows a merge step must hold on to before the
// offset is applied: everything up to Offset+Limit. Returns NoLimit (-1)
// when unbounded — never 0, which is itself a real, meaningful "keep
// nothing" answer for a top-level `LIMIT 0` (#481). Callers must test
// `KeepRows() >= 0`, never `KeepRows() > 0`.
func (mi *MergeInfo) KeepRows() int {
	if !mi.HasLimit {
		return NoLimit
	}
	return mi.Limit + mi.Offset
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

// NoLimit is the LimitVal of a Limit node that only skips rows: `OFFSET n`
// with no LIMIT, which is what a paginating client sends for the last page.
// Every row past the offset passes. Consumers must test `LimitVal !=
// NoLimit` before using it as a row bound — never `LimitVal > 0`, which
// mistakes a real `LIMIT 0` for "unbounded" and was the root cause of #481
// (`ORDER BY ... LIMIT 0` returning every row). An unbounded node has no
// bound to push down; a LimitVal of exactly 0 is a real, meaningful bound.
const NoLimit = -1

// NewLimit creates a limit node. limit is NoLimit when only the offset
// applies.
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
