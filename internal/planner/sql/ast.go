package sql

import (
	"fmt"
	"strings"
)

// Node is the base interface for all SQL expression AST nodes.
// Every concrete node type must implement nodeTag (a marker method) and String.
type Node interface {
	nodeTag()
	String() string
}

// --- Leaf nodes ---

// ColRef is a column reference, optionally qualified (table.column).
type ColRef struct {
	Table  string
	Column string
}

func (*ColRef) nodeTag() {}
func (c *ColRef) String() string {
	if c.Table != "" {
		return c.Table + "." + c.Column
	}
	return c.Column
}

// LiteralKind identifies the kind of literal value.
type LiteralKind int

const (
	LitString LiteralKind = iota
	LitNumber
	LitBool
	LitNull
)

// Lit is a literal value (string, number, bool, null).
type Lit struct {
	Value string
	Kind  LiteralKind
}

func (*Lit) nodeTag() {}
func (l *Lit) String() string {
	switch l.Kind {
	case LitString:
		return "'" + strings.ReplaceAll(l.Value, "'", "''") + "'"
	case LitNull:
		return "null"
	default:
		return l.Value
	}
}

// StarNode represents * or table.* in SELECT.
type StarNode struct {
	Table string
}

func (*StarNode) nodeTag() {}
func (s *StarNode) String() string {
	if s.Table != "" {
		return s.Table + ".*"
	}
	return "*"
}

// --- Arithmetic operators ---

// BinaryOp is a binary arithmetic/string expression.
type BinaryOp struct {
	Left  Node
	Op    string // +, -, *, /, %, ||
	Right Node
}

func (*BinaryOp) nodeTag() {}
func (b *BinaryOp) String() string {
	return b.Left.String() + " " + b.Op + " " + b.Right.String()
}

// UnaryOp is a unary operator expression (-, +).
type UnaryOp struct {
	Op    string // -, +
	Inner Node
}

func (*UnaryOp) nodeTag() {}
func (u *UnaryOp) String() string {
	return u.Op + u.Inner.String()
}

// --- Comparison ---

// CmpExpr is a comparison expression.
type CmpExpr struct {
	Left  Node
	Op    string // =, !=, <, <=, >, >=
	Right Node
}

func (*CmpExpr) nodeTag() {}
func (c *CmpExpr) String() string {
	return c.Left.String() + " " + c.Op + " " + c.Right.String()
}

// InExpr is expr [NOT] IN (values...) or expr [NOT] IN (SELECT ...).
type InExpr struct {
	Left   Node
	Not    bool
	Values []Node
}

func (*InExpr) nodeTag() {}
func (e *InExpr) String() string {
	notStr := ""
	if e.Not {
		notStr = "not "
	}
	vals := make([]string, len(e.Values))
	for i, v := range e.Values {
		vals[i] = v.String()
	}
	return e.Left.String() + " " + notStr + "in (" + strings.Join(vals, ", ") + ")"
}

// BetweenExpr is expr [NOT] BETWEEN low AND high.
type BetweenExpr struct {
	Left Node
	Not  bool
	Low  Node
	High Node
}

func (*BetweenExpr) nodeTag() {}
func (b *BetweenExpr) String() string {
	notStr := ""
	if b.Not {
		notStr = "not "
	}
	return b.Left.String() + " " + notStr + "between " + b.Low.String() + " and " + b.High.String()
}

// LikeExpr is expr [NOT] LIKE pattern.
type LikeExpr struct {
	Left    Node
	Not     bool
	Pattern Node
}

func (*LikeExpr) nodeTag() {}
func (l *LikeExpr) String() string {
	notStr := ""
	if l.Not {
		notStr = "not "
	}
	return l.Left.String() + " " + notStr + "like " + l.Pattern.String()
}

// IsExpr is expr IS [NOT] NULL/TRUE/FALSE.
type IsExpr struct {
	Left  Node
	Not   bool
	Check string // "null", "true", "false"
}

func (*IsExpr) nodeTag() {}
func (e *IsExpr) String() string {
	notStr := ""
	if e.Not {
		notStr = "not "
	}
	return e.Left.String() + " is " + notStr + e.Check
}

// --- Logical ---

// AndNode is a logical AND.
type AndNode struct {
	Left  Node
	Right Node
}

func (*AndNode) nodeTag() {}
func (a *AndNode) String() string {
	return a.Left.String() + " and " + a.Right.String()
}

// OrNode is a logical OR.
type OrNode struct {
	Left  Node
	Right Node
}

func (*OrNode) nodeTag() {}
func (o *OrNode) String() string {
	return o.Left.String() + " or " + o.Right.String()
}

// NotNode is a logical NOT.
type NotNode struct {
	Inner Node
}

func (*NotNode) nodeTag() {}
func (n *NotNode) String() string {
	return "not " + n.Inner.String()
}

// ParenNode wraps a parenthesized expression.
type ParenNode struct {
	Inner Node
}

func (*ParenNode) nodeTag() {}
func (p *ParenNode) String() string {
	return "(" + p.Inner.String() + ")"
}

// --- Function call ---

// FuncCallNode is a function call expression.
type FuncCallNode struct {
	Name     string
	Args     []Node
	Distinct bool // COUNT(DISTINCT col)
	Star     bool // COUNT(*)
}

func (*FuncCallNode) nodeTag() {}
func (f *FuncCallNode) String() string {
	if f.Star {
		return f.Name + "(*)"
	}
	args := make([]string, len(f.Args))
	for i, a := range f.Args {
		args[i] = a.String()
	}
	distinct := ""
	if f.Distinct {
		distinct = "distinct "
	}
	return f.Name + "(" + distinct + strings.Join(args, ", ") + ")"
}

// --- CASE WHEN ---

// CaseNode is a CASE expression.
type CaseNode struct {
	Subject Node         // nil for searched CASE
	Whens   []WhenClause // at least one
	Else    Node         // nil if no ELSE
}

// WhenClause is a single WHEN ... THEN ... clause.
type WhenClause struct {
	Cond   Node
	Result Node
}

func (*CaseNode) nodeTag() {}
func (c *CaseNode) String() string {
	var sb strings.Builder
	sb.WriteString("case")
	if c.Subject != nil {
		sb.WriteString(" ")
		sb.WriteString(c.Subject.String())
	}
	for _, w := range c.Whens {
		sb.WriteString(" when ")
		sb.WriteString(w.Cond.String())
		sb.WriteString(" then ")
		sb.WriteString(w.Result.String())
	}
	if c.Else != nil {
		sb.WriteString(" else ")
		sb.WriteString(c.Else.String())
	}
	sb.WriteString(" end")
	return sb.String()
}

// --- CAST ---

// CastNode is CAST(expr AS type).
type CastNode struct {
	Inner    Node
	TypeName string
}

func (*CastNode) nodeTag() {}
func (c *CastNode) String() string {
	return fmt.Sprintf("cast(%s as %s)", c.Inner.String(), c.TypeName)
}

// --- Subquery ---

// SubqueryNode wraps a subquery as raw SQL.
type SubqueryNode struct {
	SQL string
}

func (*SubqueryNode) nodeTag() {}
func (s *SubqueryNode) String() string {
	return "(" + s.SQL + ")"
}

// ExistsNode is [NOT] EXISTS (SELECT ...).
type ExistsNode struct {
	Not bool
	SQL string
}

func (*ExistsNode) nodeTag() {}
func (e *ExistsNode) String() string {
	notStr := ""
	if e.Not {
		notStr = "not "
	}
	return notStr + "exists (" + e.SQL + ")"
}

// --- Window function AST ---

// FrameMode identifies ROWS vs RANGE.
type FrameMode int

const (
	FrameRows FrameMode = iota
	FrameRange
)

// FrameBoundType identifies the type of a frame bound.
type FrameBoundType int

const (
	BoundUnboundedPreceding FrameBoundType = iota
	BoundPreceding
	BoundCurrentRow
	BoundFollowing
	BoundUnboundedFollowing
)

// FrameBound describes one end of a window frame.
type FrameBound struct {
	Type   FrameBoundType
	Offset Node // nil for UNBOUNDED/CURRENT ROW
}

// WindowFrame describes a window frame specification.
type WindowFrame struct {
	Mode  FrameMode
	Start FrameBound
	End   *FrameBound // nil means "to CURRENT ROW"
}

// WindowFuncNode represents a window function call: FUNC(...) OVER (...)
type WindowFuncNode struct {
	Func        *FuncCallNode
	PartitionBy []Node
	OrderBy     []WindowOrderBy
	Frame       *WindowFrame
}

// WindowOrderBy describes ordering in a window function's OVER clause.
type WindowOrderBy struct {
	Expr       Node
	Desc       bool
	NullsFirst *bool
}

func (n *WindowFuncNode) nodeTag() {}
func (n *WindowFuncNode) String() string {
	return n.Func.String() + " OVER (...)"
}

// --- Aggregates lookup ---

// knownAggregates is the set of standard aggregate function names.
var knownAggregates = map[string]bool{
	"sum":        true,
	"count":      true,
	"avg":        true,
	"min":        true,
	"max":        true,
	"grouping":   true,
	"string_agg": true,
	"bool_and":   true,
	"bool_or":    true,
	"every":      true,
	"stddev":     true,
	"stddev_samp": true,
	"stddev_pop": true,
	"variance":   true,
	"var_samp":   true,
	"var_pop":    true,
}

// IsAggregate returns true if the function name is a known aggregate.
func IsAggregate(name string) bool {
	return knownAggregates[strings.ToLower(name)]
}
