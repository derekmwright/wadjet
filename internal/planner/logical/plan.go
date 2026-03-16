// Package logical provides logical query plan representation and optimization.
package logical

import (
	"fmt"

	plansql "github.com/derekmwright/caelum/internal/planner/sql"
)

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

	// Filter
	Predicates []Predicate

	// Project
	Projections []Projection

	// Aggregate
	GroupBy          []string
	GroupByExprs     []plansql.Node // AST for GROUP BY expressions (may be nil)
	AggExprs         []AggExpr
	GroupingSetNulls []string // columns that should be NULL in this grouping set

	// Sort
	OrderBy []OrderExpr

	// Limit
	LimitVal  int
	OffsetVal int

	// Join
	JoinType   string // inner, left, right, full, cross, semi, anti
	JoinCond   string
	JoinFilter string // non-equality join conditions for semi/anti join
	LeftKeys   []string
	RightKeys  []string

	// Window
	WindowExprs []WindowExpr

	// Union
	UnionAll bool // true = UNION ALL, false = UNION (dedup)

	// CTEs — stored on the root node so the physical planner can resolve
	// CTE references inside scalar subqueries (e.g., Q15's HAVING/WHERE
	// subquery that references a CTE defined in the outer WITH clause).
	CTEs []plansql.CTEDef
}

// Predicate is a filter condition.
type Predicate struct {
	Column  string
	Op      string // =, !=, <, <=, >, >=, is_null, is_not_null, in, between
	Value   any
	Raw     string         // raw SQL expression
	ASTExpr plansql.Node // compiled AST expression node
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
