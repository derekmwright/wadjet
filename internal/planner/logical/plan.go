// Package logical provides logical query plan representation and optimization.
package logical

import (
	"fmt"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
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
	default:
		return fmt.Sprintf("Unknown(%d)", int(n))
	}
}

// Node is a node in the logical query plan tree.
type Node struct {
	Type     NodeType
	Children []*Node

	// Scan
	TableName string
	TableAlias string

	// Filter
	Predicates []Predicate

	// Project
	Projections []Projection

	// Aggregate
	GroupBy    []string
	AggExprs  []AggExpr

	// Sort
	OrderBy []OrderExpr

	// Limit
	LimitVal  int
	OffsetVal int

	// Join
	JoinType  string // inner, left
	JoinCond  string
	LeftKeys  []string
	RightKeys []string
}

// Predicate is a filter condition.
type Predicate struct {
	Column  string
	Op      string // =, !=, <, <=, >, >=, is_null, is_not_null, in, between
	Value   any
	Raw     string         // raw SQL expression
	ASTExpr sqlparser.Expr // compiled AST expression node
}

// Projection is a column expression in a SELECT.
type Projection struct {
	Column  string         // column reference
	Alias   string         // output name
	Expr    string         // raw expression
	IsAgg   bool
	ASTExpr sqlparser.Expr // compiled AST expression node (nil for aggregates)
}

// AggExpr is an aggregation expression.
type AggExpr struct {
	Func      string // sum, count, min, max, avg
	InputCol  string
	OutputCol string
}

// OrderExpr is a sort expression.
type OrderExpr struct {
	Column string
	Desc   bool
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

// NewJoin creates a join node.
func NewJoin(left, right *Node, joinType, condition string) *Node {
	return &Node{
		Type:     NodeJoin,
		Children: []*Node{left, right},
		JoinType: joinType,
		JoinCond: condition,
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
			aggs[i] = fmt.Sprintf("%s(%s) AS %s", a.Func, a.InputCol, a.OutputCol)
		}
		s = fmt.Sprintf("%sAggregate: group_by=%v aggs=%v", prefix, n.GroupBy, aggs)
	case NodeSort:
		s = fmt.Sprintf("%sSort: %v", prefix, n.orderStrings())
	case NodeLimit:
		s = fmt.Sprintf("%sLimit: %d offset: %d", prefix, n.LimitVal, n.OffsetVal)
	case NodeJoin:
		s = fmt.Sprintf("%sJoin: %s ON %s", prefix, n.JoinType, n.JoinCond)
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
