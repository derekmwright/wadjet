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
	// Slot marks a reference the PLANNER planted at a hidden slot, as
	// opposed to one the user wrote. Nothing a query can contain sets it:
	// the parser never does, and re-parsing a rendered expression loses it,
	// because it is provenance and not spelling.
	//
	// It exists because the two are otherwise indistinguishable. A table may
	// legitimately store a column called `__win_0` (ADR-0025 rule 1: reading
	// such a table is never refused), and then `SUM(plain) OVER () + 0` has
	// a `__win_0` reference the nested-window rewrite planted BESIDE a
	// `__win_0` the scan really emits. Moving the planner's slot past the
	// stored column has to move the first and must not touch the second
	// (#750, #694).
	Slot bool
}

func (*ColRef) nodeTag() {}
func (c *ColRef) String() string {
	if c.Table != "" {
		return QuoteIdent(c.Table) + "." + QuoteIdent(c.Column)
	}
	return QuoteIdent(c.Column)
}

// FoldIdent is the case fold PostgreSQL applies to an UNQUOTED identifier:
// ASCII A-Z becomes a-z and NOTHING else changes.
//
// The ASCII restriction is PostgreSQL's own, measured on postgres:17-alpine
// with a UTF8 server encoding: `CREATE TABLE t (Ä int)` stores the column as
// `Ä`, and `SELECT 1 AS Ä` publishes `Ä`. strings.ToLower would fold it to
// `ä` and invent a name no PostgreSQL client would expect, so the fold is
// written out byte by byte rather than borrowed from unicode.
func FoldIdent(s string) string {
	hasUpper := false
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			hasUpper = true
			break
		}
	}
	if !hasUpper {
		return s
	}
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// QuoteIdent renders an identifier so that re-parsing it yields the same
// identifier. Names the lexer would otherwise re-read as something else —
// embedded dots (a flat JSON column such as id.orig_h), spaces, other
// punctuation, a leading digit, an ASCII UPPER-CASE letter (which the lexer
// folds, #731), or a keyword spelling — come back double-quoted with any
// interior quote doubled. Names that already lex as a single unquoted
// identifier are returned unchanged, so printed SQL for ordinary columns is
// byte-identical to what it was before delimited identifiers existed.
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
	lx := newVerbatimLexer(s)
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
		case r >= 'A' && r <= 'Z':
			// The lexer folds it, so an unquoted rendering would come back
			// as a DIFFERENT name. Parse(QuoteIdent(n)) == n is what the
			// planner relies on every time it renders a reference and
			// re-parses it (#731).
			return true
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
	// OutputLabel is the name PostgreSQL publishes this call under when the
	// SELECT list wrote no alias, for the calls the parser REWRITES into a
	// different function: `EXTRACT(YEAR FROM d)` becomes `year(d)` here and
	// PostgreSQL still labels the column `extract`. Empty means Name is the
	// label, which is the ordinary case (OutputColumnName, #732).
	OutputLabel string
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
	case *IsExpr:
		return FindNestedAggregate(n.Left)
	case *NotNode:
		return FindNestedAggregate(n.Inner)
	case *AndNode:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		return FindNestedAggregate(n.Right)
	case *OrNode:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		return FindNestedAggregate(n.Right)
	case *InExpr:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		for _, v := range n.Values {
			if found := FindNestedAggregate(v); found != nil {
				return found
			}
		}
	case *BetweenExpr:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		if found := FindNestedAggregate(n.Low); found != nil {
			return found
		}
		return FindNestedAggregate(n.High)
	case *LikeExpr:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		return FindNestedAggregate(n.Pattern)
	case *AnyAllExpr:
		if found := FindNestedAggregate(n.Left); found != nil {
			return found
		}
		for _, v := range n.Values {
			if found := FindNestedAggregate(v); found != nil {
				return found
			}
		}
	case *TupleNode:
		for _, e := range n.Elements {
			if found := FindNestedAggregate(e); found != nil {
				return found
			}
		}
	case *ArrayLitNode:
		for _, e := range n.Elements {
			if found := FindNestedAggregate(e); found != nil {
				return found
			}
		}
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
	case *IsExpr:
		findAllAggsHelper(n.Left, result)
	case *NotNode:
		findAllAggsHelper(n.Inner, result)
	case *AndNode:
		findAllAggsHelper(n.Left, result)
		findAllAggsHelper(n.Right, result)
	case *OrNode:
		findAllAggsHelper(n.Left, result)
		findAllAggsHelper(n.Right, result)
	case *InExpr:
		findAllAggsHelper(n.Left, result)
		for _, v := range n.Values {
			findAllAggsHelper(v, result)
		}
	case *BetweenExpr:
		findAllAggsHelper(n.Left, result)
		findAllAggsHelper(n.Low, result)
		findAllAggsHelper(n.High, result)
	case *LikeExpr:
		findAllAggsHelper(n.Left, result)
		findAllAggsHelper(n.Pattern, result)
	case *AnyAllExpr:
		findAllAggsHelper(n.Left, result)
		for _, v := range n.Values {
			findAllAggsHelper(v, result)
		}
	case *TupleNode:
		for _, e := range n.Elements {
			findAllAggsHelper(e, result)
		}
	case *ArrayLitNode:
		for _, e := range n.Elements {
			findAllAggsHelper(e, result)
		}
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
	case *IsExpr:
		left := ReplaceAllAggregates(n.Left, replacements)
		if left == n.Left {
			return node
		}
		return &IsExpr{Left: left, Not: n.Not, Check: n.Check}
	case *NotNode:
		inner := ReplaceAllAggregates(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &NotNode{Inner: inner}
	case *AndNode:
		left := ReplaceAllAggregates(n.Left, replacements)
		right := ReplaceAllAggregates(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &AndNode{Left: left, Right: right}
	case *OrNode:
		left := ReplaceAllAggregates(n.Left, replacements)
		right := ReplaceAllAggregates(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &OrNode{Left: left, Right: right}
	case *InExpr:
		left := ReplaceAllAggregates(n.Left, replacements)
		newVals, changed := replaceAggsInList(n.Values, replacements)
		if left == n.Left && !changed {
			return node
		}
		return &InExpr{Left: left, Not: n.Not, Values: newVals}
	case *BetweenExpr:
		left := ReplaceAllAggregates(n.Left, replacements)
		low := ReplaceAllAggregates(n.Low, replacements)
		high := ReplaceAllAggregates(n.High, replacements)
		if left == n.Left && low == n.Low && high == n.High {
			return node
		}
		return &BetweenExpr{Left: left, Not: n.Not, Low: low, High: high}
	case *LikeExpr:
		left := ReplaceAllAggregates(n.Left, replacements)
		pattern := ReplaceAllAggregates(n.Pattern, replacements)
		if left == n.Left && pattern == n.Pattern {
			return node
		}
		return &LikeExpr{Left: left, Not: n.Not, Pattern: pattern}
	case *AnyAllExpr:
		left := ReplaceAllAggregates(n.Left, replacements)
		newVals, changed := replaceAggsInList(n.Values, replacements)
		if left == n.Left && !changed {
			return node
		}
		return &AnyAllExpr{Left: left, Op: n.Op, Modifier: n.Modifier, Values: newVals}
	case *TupleNode:
		newEls, changed := replaceAggsInList(n.Elements, replacements)
		if !changed {
			return node
		}
		return &TupleNode{Elements: newEls}
	case *ArrayLitNode:
		newEls, changed := replaceAggsInList(n.Elements, replacements)
		if !changed {
			return node
		}
		return &ArrayLitNode{Elements: newEls}
	default:
		return node
	}
}

// replaceAggsInList maps ReplaceAllAggregates over a node list, reporting
// whether anything changed so the caller can return its input unmodified.
func replaceAggsInList(nodes []Node, replacements map[string]string) ([]Node, bool) {
	out := make([]Node, len(nodes))
	changed := false
	for i, n := range nodes {
		out[i] = ReplaceAllAggregates(n, replacements)
		if out[i] != n {
			changed = true
		}
	}
	if !changed {
		return nodes, false
	}
	return out, true
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

// --- Window function lookup / rewrite ---

// FindAllWindowFuncs walks an expression tree and returns every window
// function call found, in left-to-right order. It is the window analogue of
// FindAllAggregates and lets the logical builder detect a window call nested
// inside a larger expression — SUM(x) OVER (...) + 1, COALESCE(LAG(x) OVER
// (...), 0), a CASE branch — not just the bare top-level form. It does not
// recurse into a window node's own argument/OVER subtrees: a window over a
// window is not a shape this handles, and stopping keeps the returned nodes
// disjoint so each maps to one output column.
func FindAllWindowFuncs(node Node) []*WindowFuncNode {
	var result []*WindowFuncNode
	findAllWindowFuncsHelper(node, &result)
	return result
}

func findAllWindowFuncsHelper(node Node, result *[]*WindowFuncNode) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *WindowFuncNode:
		*result = append(*result, n)
		return // don't recurse into the window's own args/OVER subtrees
	case *FuncCallNode:
		for _, arg := range n.Args {
			findAllWindowFuncsHelper(arg, result)
		}
	case *BinaryOp:
		findAllWindowFuncsHelper(n.Left, result)
		findAllWindowFuncsHelper(n.Right, result)
	case *UnaryOp:
		findAllWindowFuncsHelper(n.Inner, result)
	case *ParenNode:
		findAllWindowFuncsHelper(n.Inner, result)
	case *CmpExpr:
		findAllWindowFuncsHelper(n.Left, result)
		findAllWindowFuncsHelper(n.Right, result)
	case *CaseNode:
		findAllWindowFuncsHelper(n.Subject, result)
		for _, w := range n.Whens {
			findAllWindowFuncsHelper(w.Cond, result)
			findAllWindowFuncsHelper(w.Result, result)
		}
		findAllWindowFuncsHelper(n.Else, result)
	case *CastNode:
		findAllWindowFuncsHelper(n.Inner, result)
	case *IsExpr:
		findAllWindowFuncsHelper(n.Left, result)
	case *NotNode:
		findAllWindowFuncsHelper(n.Inner, result)
	case *AndNode:
		findAllWindowFuncsHelper(n.Left, result)
		findAllWindowFuncsHelper(n.Right, result)
	case *OrNode:
		findAllWindowFuncsHelper(n.Left, result)
		findAllWindowFuncsHelper(n.Right, result)
	case *InExpr:
		findAllWindowFuncsHelper(n.Left, result)
		for _, v := range n.Values {
			findAllWindowFuncsHelper(v, result)
		}
	case *BetweenExpr:
		findAllWindowFuncsHelper(n.Left, result)
		findAllWindowFuncsHelper(n.Low, result)
		findAllWindowFuncsHelper(n.High, result)
	case *LikeExpr:
		findAllWindowFuncsHelper(n.Left, result)
		findAllWindowFuncsHelper(n.Pattern, result)
	case *AnyAllExpr:
		findAllWindowFuncsHelper(n.Left, result)
		for _, v := range n.Values {
			findAllWindowFuncsHelper(v, result)
		}
	case *TupleNode:
		for _, e := range n.Elements {
			findAllWindowFuncsHelper(e, result)
		}
	case *ArrayLitNode:
		for _, e := range n.Elements {
			findAllWindowFuncsHelper(e, result)
		}
	}
}

// ReplaceWindowFuncs replaces each window function call named in replacements
// with a ColRef to its precomputed output column, leaving the surrounding
// expression intact. Nodes are matched by pointer identity, not by rendered
// text, because WindowFuncNode.String() collapses the OVER clause and two
// distinct windows would otherwise collide. It is the window analogue of
// ReplaceAllAggregates: after the builder has extracted a nested window into a
// NodeWindow output column, this rewrites SUM(x) OVER (...) + 1 into
// __win_0 + 1 so the ordinary projection compiler evaluates the outer
// expression over the window's result.
func ReplaceWindowFuncs(node Node, replacements map[*WindowFuncNode]string) Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *WindowFuncNode:
		if colName, ok := replacements[n]; ok {
			// Slot: the reference is the PLANNER's, planted at the window's
			// own hidden output. A stored column of that name is a different
			// thing and must not move with it (#750).
			return &ColRef{Column: colName, Slot: true}
		}
		return node
	case *FuncCallNode:
		newArgs, changed := replaceWindowFuncsInList(n.Args, replacements)
		if !changed {
			return node
		}
		return &FuncCallNode{Name: n.Name, Args: newArgs, Distinct: n.Distinct, Star: n.Star}
	case *BinaryOp:
		left := ReplaceWindowFuncs(n.Left, replacements)
		right := ReplaceWindowFuncs(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &BinaryOp{Left: left, Op: n.Op, Right: right}
	case *UnaryOp:
		inner := ReplaceWindowFuncs(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &UnaryOp{Inner: inner, Op: n.Op}
	case *ParenNode:
		inner := ReplaceWindowFuncs(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &ParenNode{Inner: inner}
	case *CastNode:
		inner := ReplaceWindowFuncs(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &CastNode{Inner: inner, TypeName: n.TypeName}
	case *CmpExpr:
		left := ReplaceWindowFuncs(n.Left, replacements)
		right := ReplaceWindowFuncs(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &CmpExpr{Left: left, Op: n.Op, Right: right}
	case *CaseNode:
		changed := false
		newWhens := make([]WhenClause, len(n.Whens))
		for i, w := range n.Whens {
			cond := ReplaceWindowFuncs(w.Cond, replacements)
			result := ReplaceWindowFuncs(w.Result, replacements)
			if cond != w.Cond || result != w.Result {
				changed = true
			}
			newWhens[i] = WhenClause{Cond: cond, Result: result}
		}
		var subj Node
		if n.Subject != nil {
			subj = ReplaceWindowFuncs(n.Subject, replacements)
			if subj != n.Subject {
				changed = true
			}
		}
		var elseNode Node
		if n.Else != nil {
			elseNode = ReplaceWindowFuncs(n.Else, replacements)
			if elseNode != n.Else {
				changed = true
			}
		}
		if !changed {
			return node
		}
		return &CaseNode{Subject: subj, Whens: newWhens, Else: elseNode}
	case *IsExpr:
		left := ReplaceWindowFuncs(n.Left, replacements)
		if left == n.Left {
			return node
		}
		return &IsExpr{Left: left, Not: n.Not, Check: n.Check}
	case *NotNode:
		inner := ReplaceWindowFuncs(n.Inner, replacements)
		if inner == n.Inner {
			return node
		}
		return &NotNode{Inner: inner}
	case *AndNode:
		left := ReplaceWindowFuncs(n.Left, replacements)
		right := ReplaceWindowFuncs(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &AndNode{Left: left, Right: right}
	case *OrNode:
		left := ReplaceWindowFuncs(n.Left, replacements)
		right := ReplaceWindowFuncs(n.Right, replacements)
		if left == n.Left && right == n.Right {
			return node
		}
		return &OrNode{Left: left, Right: right}
	case *InExpr:
		left := ReplaceWindowFuncs(n.Left, replacements)
		newVals, changed := replaceWindowFuncsInList(n.Values, replacements)
		if left == n.Left && !changed {
			return node
		}
		return &InExpr{Left: left, Not: n.Not, Values: newVals}
	case *BetweenExpr:
		left := ReplaceWindowFuncs(n.Left, replacements)
		low := ReplaceWindowFuncs(n.Low, replacements)
		high := ReplaceWindowFuncs(n.High, replacements)
		if left == n.Left && low == n.Low && high == n.High {
			return node
		}
		return &BetweenExpr{Left: left, Not: n.Not, Low: low, High: high}
	case *LikeExpr:
		left := ReplaceWindowFuncs(n.Left, replacements)
		pattern := ReplaceWindowFuncs(n.Pattern, replacements)
		if left == n.Left && pattern == n.Pattern {
			return node
		}
		return &LikeExpr{Left: left, Not: n.Not, Pattern: pattern}
	case *AnyAllExpr:
		left := ReplaceWindowFuncs(n.Left, replacements)
		newVals, changed := replaceWindowFuncsInList(n.Values, replacements)
		if left == n.Left && !changed {
			return node
		}
		return &AnyAllExpr{Left: left, Op: n.Op, Modifier: n.Modifier, Values: newVals}
	case *TupleNode:
		newEls, changed := replaceWindowFuncsInList(n.Elements, replacements)
		if !changed {
			return node
		}
		return &TupleNode{Elements: newEls}
	case *ArrayLitNode:
		newEls, changed := replaceWindowFuncsInList(n.Elements, replacements)
		if !changed {
			return node
		}
		return &ArrayLitNode{Elements: newEls}
	default:
		return node
	}
}

func replaceWindowFuncsInList(nodes []Node, replacements map[*WindowFuncNode]string) ([]Node, bool) {
	out := make([]Node, len(nodes))
	changed := false
	for i, n := range nodes {
		out[i] = ReplaceWindowFuncs(n, replacements)
		if out[i] != n {
			changed = true
		}
	}
	if !changed {
		return nodes, false
	}
	return out, true
}
