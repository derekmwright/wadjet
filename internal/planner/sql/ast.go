package sql

import (
	"fmt"
	"strings"
	"unicode"
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
		return QuoteIdent(c.Table) + "." + QuoteIdent(c.Column)
	}
	return QuoteIdent(c.Column)
}

// QuoteIdent renders an identifier so that re-parsing it yields the same
// identifier. Names the lexer would otherwise re-read as something else —
// embedded dots (a flat JSON column such as id.orig_h), spaces, other
// punctuation, a leading digit, or a keyword spelling — come back
// double-quoted with any interior quote doubled. Names that already lex as
// a single unquoted identifier are returned unchanged, so printed SQL for
// ordinary columns is byte-identical to what it was before delimited
// identifiers existed.
func QuoteIdent(name string) string {
	if !identNeedsQuoting(name) {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// SplitIdentRef interprets s as an identifier reference and returns its
// optional qualifier and its name. Quoting is honoured, so the qualifier
// split happens only at a dot the lexer actually produced:
//
//	l_orderkey     → ("", "l_orderkey")
//	o.o_custkey    → ("o", "o_custkey")
//	"id.orig_h"    → ("", "id.orig_h")   — one delimited name, not qualified
//	"my tbl"."c"   → ("my tbl", "c")
//
// ok is false for anything that is not a bare identifier reference (a
// function call, an arithmetic expression, a literal, a multi-level path);
// callers then fall back to their own string handling.
func SplitIdentRef(s string) (qualifier, name string, ok bool) {
	lx := newLexer(s)
	first := lx.nextToken()
	if first.typ != TokenIdent {
		return "", "", false
	}
	switch sep := lx.nextToken(); sep.typ {
	case TokenEOF:
		return "", first.val, true
	case TokenDot:
		last := lx.nextToken()
		if last.typ != TokenIdent || lx.nextToken().typ != TokenEOF {
			return "", "", false
		}
		return first.val, last.val, true
	default:
		return "", "", false
	}
}

// NormalizeIdentRef strips the delimiters from an identifier reference so
// that it reads as the plain column name the execution layer matches
// against batch schemas: `"id.orig_h"` → id.orig_h, `"my tbl"."c"` →
// my tbl.c. Strings that are not a bare identifier reference (expressions,
// function calls, literals) are returned unchanged.
func NormalizeIdentRef(s string) string {
	if !strings.Contains(s, `"`) {
		return s
	}
	qual, name, ok := SplitIdentRef(s)
	if !ok {
		return s
	}
	if qual == "" {
		return name
	}
	return qual + "." + name
}

func identNeedsQuoting(name string) bool {
	if name == "" {
		return true
	}
	for i, r := range name {
		switch {
		case r == '_' || unicode.IsLetter(r):
			// always allowed
		case r >= '0' && r <= '9':
			if i == 0 {
				return true // a leading digit would lex as a number
			}
		default:
			return true
		}
	}
	// A bare keyword spelling would come back as a keyword token.
	_, isKeyword := keywords[strings.ToUpper(name)]
	return isKeyword
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

// LiteralPlaceholder marks a deferred literal whose value is computed at
// stage-dispatch time from the output of a prerequisite stage. The physical
// planner inserts these when a filter expression's subquery references a CTE
// whose distributed-pipeline output would diverge from single-process
// evaluation; the native-DAG coordinator rewrites the serialized filter
// expression by string-replacing ":<Name>" with the concrete literal before
// dispatching the task. String renders as ":<Name>" so any code path that
// accidentally serializes the expression before substitution produces an
// unambiguous syntax error instead of silently coercing.
type LiteralPlaceholder struct {
	Name string
}

func (*LiteralPlaceholder) nodeTag()         {}
func (l *LiteralPlaceholder) String() string { return ":" + l.Name }

// StarNode represents * or table.* in SELECT.
type StarNode struct {
	Table string
}

func (*StarNode) nodeTag() {}
func (s *StarNode) String() string {
	if s.Table != "" {
		return QuoteIdent(s.Table) + ".*"
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

// --- Array literal ---

// ArrayLitNode is ARRAY[expr, expr, ...].
type ArrayLitNode struct {
	Elements []Node
}

func (*ArrayLitNode) nodeTag() {}
func (a *ArrayLitNode) String() string {
	parts := make([]string, len(a.Elements))
	for i, e := range a.Elements {
		parts[i] = e.String()
	}
	return "ARRAY[" + strings.Join(parts, ", ") + "]"
}

// --- Interval literal ---

// IntervalLit represents INTERVAL 'N' DAY or INTERVAL 'N days' expressions.
type IntervalLit struct {
	Value int
	Unit  string // "day", "month", "year", "hour", "minute", "second"
}

func (*IntervalLit) nodeTag() {}
func (i *IntervalLit) String() string {
	return fmt.Sprintf("INTERVAL '%d' %s", i.Value, strings.ToUpper(i.Unit))
}

// --- Tuple (row constructor) ---

// TupleNode represents a tuple expression: (a, b, c).
type TupleNode struct {
	Elements []Node
}

func (*TupleNode) nodeTag() {}
func (t *TupleNode) String() string {
	parts := make([]string, len(t.Elements))
	for i, e := range t.Elements {
		parts[i] = e.String()
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// --- ANY/ALL/SOME ---

// AnyAllExpr is expr op ANY/ALL/SOME (subquery or values).
type AnyAllExpr struct {
	Left     Node
	Op       string // =, !=, <, <=, >, >=
	Modifier string // "ANY", "ALL", "SOME"
	Values   []Node // value list or single SubqueryNode
}

func (*AnyAllExpr) nodeTag() {}
func (a *AnyAllExpr) String() string {
	vals := make([]string, len(a.Values))
	for i, v := range a.Values {
		vals[i] = v.String()
	}
	return a.Left.String() + " " + a.Op + " " + a.Modifier + " (" + strings.Join(vals, ", ") + ")"
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
	"sum":             true,
	"count":           true,
	"avg":             true,
	"min":             true,
	"max":             true,
	"grouping":        true,
	"string_agg":      true,
	"bool_and":        true,
	"bool_or":         true,
	"every":           true,
	"stddev":          true,
	"stddev_samp":     true,
	"stddev_pop":      true,
	"variance":        true,
	"var_samp":        true,
	"var_pop":         true,
	"approx_distinct": true,
	"corr":            true,
	"covar_samp":      true,
	"covar_pop":       true,
	"percentile_cont": true,
	"percentile_disc": true,
	// quantile_cont / quantile_disc are DuckDB's spelling of the same two
	// functions with the arguments the other way round — (column,
	// fraction). Accepted so the same SQL text runs on both engines, which
	// is what lets the DuckDB ground-truth gate cover the percentile family
	// at all: DuckDB's percentile_cont is ordered-set syntax
	// (WITHIN GROUP), which this parser does not accept.
	"quantile_cont": true,
	"quantile_disc": true,
	"mode":          true,
	"min_by":        true,
	"max_by":        true,
	"median":        true,
}

// IsAggregate returns true if the function name is a known aggregate.
func IsAggregate(name string) bool {
	return knownAggregates[strings.ToLower(name)]
}

// FindNestedAggregate walks an expression tree and returns the first aggregate
// function call found, or nil if none exists. This detects aggregates nested
// inside binary expressions like SUM(x) * 0.0001.
func FindNestedAggregate(node Node) *FuncCallNode {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *FuncCallNode:
		if IsAggregate(n.Name) {
			return n
		}
		for _, arg := range n.Args {
			if found := FindNestedAggregate(arg); found != nil {
				return found
			}
		}
	case *BinaryOp:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		return FindNestedAggregate(n.Right)
	case *UnaryOp:
		return FindNestedAggregate(n.Inner)
	case *ParenNode:
		return FindNestedAggregate(n.Inner)
	case *CmpExpr:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		return FindNestedAggregate(n.Right)
	case *CaseNode:
		if found := FindNestedAggregate(n.Subject); found != nil {
			return found
		}
		for _, w := range n.Whens {
			if found := FindNestedAggregate(w.Cond); found != nil {
				return found
			}
			if found := FindNestedAggregate(w.Result); found != nil {
				return found
			}
		}
		return FindNestedAggregate(n.Else)
	case *CastNode:
		return FindNestedAggregate(n.Inner)
	}
	return nil
}

// FindAllAggregates walks an expression tree and returns all aggregate function
// calls found. For multi-aggregate expressions like MAX(x) - MIN(x), this
// returns both aggregates.
func FindAllAggregates(node Node) []*FuncCallNode {
	var result []*FuncCallNode
	findAllAggsHelper(node, &result)
	return result
}

func findAllAggsHelper(node Node, result *[]*FuncCallNode) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *FuncCallNode:
		if IsAggregate(n.Name) {
			*result = append(*result, n)
			return // don't recurse into aggregate arguments
		}
		for _, arg := range n.Args {
			findAllAggsHelper(arg, result)
		}
	case *BinaryOp:
		findAllAggsHelper(n.Left, result)
		findAllAggsHelper(n.Right, result)
	case *UnaryOp:
		findAllAggsHelper(n.Inner, result)
	case *ParenNode:
		findAllAggsHelper(n.Inner, result)
	case *CmpExpr:
		findAllAggsHelper(n.Left, result)
		findAllAggsHelper(n.Right, result)
	case *CaseNode:
		findAllAggsHelper(n.Subject, result)
		for _, w := range n.Whens {
			findAllAggsHelper(w.Cond, result)
			findAllAggsHelper(w.Result, result)
		}
		findAllAggsHelper(n.Else, result)
	case *CastNode:
		findAllAggsHelper(n.Inner, result)
	}
}

// ReplaceAllAggregates replaces all aggregate function calls in the expression
// tree with ColRef nodes. The replacements map maps lowercase aggregate
// expression strings (e.g., "sum(rx_bytes)") to output column names.
func ReplaceAllAggregates(node Node, replacements map[string]string) Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *FuncCallNode:
		if IsAggregate(n.Name) {
			key := strings.ToLower(n.String())
			if colName, ok := replacements[key]; ok {
				return &ColRef{Column: colName}
			}
			return node
		}
		newArgs := make([]Node, len(n.Args))
		changed := false
		for i, arg := range n.Args {
			newArgs[i] = ReplaceAllAggregates(arg, replacements)
			if newArgs[i] != arg {
				changed = true
			}
		}
		if !changed {
			return node
		}
		return &FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *BinaryOp:
		left := ReplaceAllAggregates(n.Left, replacements)
		right := ReplaceAllAggregates(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &BinaryOp{Left: left, Op: n.Op, Right: right}
	case *UnaryOp:
		inner := ReplaceAllAggregates(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &UnaryOp{Inner: inner, Op: n.Op}
	case *ParenNode:
		inner := ReplaceAllAggregates(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &ParenNode{Inner: inner}
	case *CastNode:
		inner := ReplaceAllAggregates(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &CastNode{Inner: inner, TypeName: n.TypeName}
	case *CmpExpr:
		left := ReplaceAllAggregates(n.Left, replacements)
		right := ReplaceAllAggregates(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &CmpExpr{Left: left, Op: n.Op, Right: right}
	case *CaseNode:
		changed := false
		newWhens := make([]WhenClause, len(n.Whens))
		for i, w := range n.Whens {
			cond := ReplaceAllAggregates(w.Cond, replacements)
			result := ReplaceAllAggregates(w.Result, replacements)
			if cond != w.Cond || result != w.Result {
				changed = true
			}
			newWhens[i] = WhenClause{Cond: cond, Result: result}
		}
		var elseNode Node
		if n.Else != nil {
			elseNode = ReplaceAllAggregates(n.Else, replacements)
			if elseNode != n.Else {
				changed = true
			}
		}
		if !changed {
			return node
		}
		return &CaseNode{Subject: n.Subject, Whens: newWhens, Else: elseNode}
	default:
		return node
	}
}

// ReplaceAggregate replaces the first aggregate function call in the expression
// tree with a ColRef pointing to the aggregate output column name.
func ReplaceAggregate(node Node, aggName string) Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *FuncCallNode:
		if IsAggregate(n.Name) {
			return &ColRef{Column: aggName}
		}
		// Recurse into arguments of non-aggregate functions
		newArgs := make([]Node, len(n.Args))
		changed := false
		for i, arg := range n.Args {
			newArgs[i] = ReplaceAggregate(arg, aggName)
			if newArgs[i] != arg {
				changed = true
			}
		}
		if !changed {
			return node
		}
		return &FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *BinaryOp:
		return &BinaryOp{
			Left:  ReplaceAggregate(n.Left, aggName),
			Op:    n.Op,
			Right: ReplaceAggregate(n.Right, aggName),
		}
	case *UnaryOp:
		return &UnaryOp{
			Inner: ReplaceAggregate(n.Inner, aggName),
			Op:    n.Op,
		}
	case *ParenNode:
		return &ParenNode{
			Inner: ReplaceAggregate(n.Inner, aggName),
		}
	case *CastNode:
		return &CastNode{
			Inner:    ReplaceAggregate(n.Inner, aggName),
			TypeName: n.TypeName,
		}
	default:
		return node
	}
}
