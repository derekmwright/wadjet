package logical

import (
	"sort"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Shape-only column analysis — the planner half of the offsets-shape
// evaluation class.
//
// A byte-array column every one of whose uses reads its SHAPE rather than
// its CONTENTS never needs its bytes materialized: the scan can decode it
// as lengths alone (internal/engine/scan/lengths_decode.go). ClickBench
// Q28, `SELECT CounterID, AVG(LENGTH(URL)) ... WHERE URL <> '' GROUP BY
// CounterID`, uses URL twice and neither use reads a byte.
//
// Shape uses recognized here, and the engine paths that make each one
// byte-free:
//
//	LENGTH / octet_length / bit_length over a bare column
//	                          expr.ColShapeLen           (offsets subtraction)
//	col IS [NOT] NULL         expr.ColIsNull / exec.NullCheckFilter (null mask)
//	col = '' / col <> ''      expr.ColEmptyStr, kernel.emptyStringFilter,
//	                          exec.ColumnCompare's empty test, and the
//	                          scan-level RowPred, which reads pages not vectors
//	COUNT(col)                kernel.ResolveBatchCount   (null mask)
//
// The analysis is deliberately conservative and fails CLOSED: a column is
// marked only when every reference to it, anywhere in the plan, is one of
// those forms. Anything the walk cannot classify — an unhandled AST node,
// an unhandled plan node, more than one scan (column names are not
// table-qualified in the accumulated sets), a join, DISTINCT, a set
// operation, a wildcard scan with no pruned column list — abandons the
// analysis for the whole query. A false positive here is a wrong LENGTH
// value, so the bar is "provably every use", not "no use I happened to
// see".
//
// The correctness net behind it: a shape-only column that still reaches a
// value consumer panics at batch.BytesColumn.Value rather than answering
// from an empty arena.

// shapeLenFuncs are the function names whose result depends only on a
// value's byte length.
//
// length / len / char_length / character_length are all absent, and all four
// for the same reason: they count RUNES, and a rune count is a scan of the
// continuation bytes rather than a subtraction of two offsets. The first two
// were here until #856, which is what made `LENGTH('éàü')` answer 6 where
// PostgreSQL — and this engine's own CHARACTER_LENGTH — answer 3. The cost is
// ClickBench Q28's shape-only decode, and it is the price of the right answer:
// the optimization exists to skip materializing bytes a query does not read,
// and a character count reads them.
var shapeLenFuncs = map[string]bool{
	"octet_length": true,
	"bit_length":   true,
}

// shapeUses accumulates the classification of every column reference in a
// plan. bail abandons the analysis entirely.
type shapeUses struct {
	shape map[string]bool
	value map[string]bool
	bail  bool
}

func (u *shapeUses) markShape(col string) {
	if col == "" {
		return
	}
	u.shape[strings.ToLower(col)] = true
}

func (u *shapeUses) markValue(col string) {
	if col == "" {
		return
	}
	u.value[strings.ToLower(col)] = true
}

// computeShapeOnlyColumns annotates the plan's single scan with the columns
// whose every use is a shape use. Called after computeRequiredColumns so
// the candidate set is the pruned RequiredColumns list.
func computeShapeOnlyColumns(root *Node) {
	if root == nil {
		return
	}
	scans := collectScans(root)
	if len(scans) != 1 {
		// Column references are accumulated by bare name; with two scans a
		// name could belong to either table. Single-scan plans are where
		// the motivating shapes live.
		return
	}
	scan := scans[0]
	if len(scan.RequiredColumns) == 0 {
		// Empty means "read every column" — nothing was pruned, so nothing
		// was proven about how the columns are used.
		return
	}
	if !outputBoundedByProjection(root) {
		// The plan's own rows are its output. A plan that merely filters and
		// hands rows on — a distributed leaf fragment shipping its scan to
		// the next stage, or a bare SELECT under a LIMIT — carries the
		// column's VALUES in that output even though no node in this plan
		// reads them. Requiring a Project or Aggregate above everything
		// makes the output schema that node's, and a column projected or
		// grouped is classified as a value use, so a shape-only column can
		// never escape.
		return
	}

	u := &shapeUses{shape: map[string]bool{}, value: map[string]bool{}}
	walkShapeUses(root, u)
	if u.bail {
		return
	}

	var only []string
	for _, c := range scan.RequiredColumns {
		lc := strings.ToLower(c)
		if lc == strings.ToLower(RowCountOnlyColumn) {
			continue
		}
		if u.shape[lc] && !u.value[lc] {
			only = append(only, lc)
		}
	}
	sort.Strings(only)
	scan.ShapeOnlyColumns = only
}

// outputBoundedByProjection reports whether the plan's output rows are
// produced by a Project or an Aggregate — the two nodes whose output schema
// is an explicit list. LIMIT and SORT above them are transparent: they
// reorder and truncate the same rows.
func outputBoundedByProjection(n *Node) bool {
	for n != nil {
		switch n.Type {
		case NodeProject, NodeAggregate:
			return true
		case NodeLimit, NodeSort:
			if len(n.Children) != 1 {
				return false
			}
			n = n.Children[0]
		default:
			return false
		}
	}
	return false
}

func collectScans(n *Node) []*Node {
	if n == nil {
		return nil
	}
	var out []*Node
	if n.Type == NodeScan {
		out = append(out, n)
	}
	for _, c := range n.Children {
		out = append(out, collectScans(c)...)
	}
	return out
}

// walkShapeUses classifies every column reference reachable from n.
func walkShapeUses(n *Node, u *shapeUses) {
	if n == nil || u.bail {
		return
	}
	switch n.Type {
	case NodeScan:
		// Predicates already pushed onto the scan (pushdownPredicates) are
		// classified like any other filter conjunct.
		for _, p := range n.Predicates {
			classifyPredicate(p, u)
		}
		for _, p := range n.ScanPredicates {
			classifyPredicate(p, u)
		}
		if n.IsTableFunc {
			u.bail = true
		}

	case NodeFilter:
		for _, p := range n.Predicates {
			classifyPredicate(p, u)
		}

	case NodeProject:
		for _, proj := range n.Projections {
			if proj.ASTExpr != nil {
				classifyAST(proj.ASTExpr, u)
			}
			// A bare projected column is a value use.
			u.markValue(proj.Column)
		}

	case NodeAggregate:
		for _, gb := range n.GroupBy {
			u.markValue(gb)
		}
		for _, e := range n.GroupByExprs {
			classifyAST(e, u)
		}
		for _, agg := range n.AggExprs {
			// COUNT(col) reads the null mask only (kernel.ResolveBatchCount
			// is countSlice over the bitmap) — but COUNT(DISTINCT col)
			// hashes the values.
			countShape := strings.EqualFold(agg.Func, "count") && !agg.Distinct
			if agg.InputCol != "" && agg.InputCol != "*" {
				if countShape {
					u.markShape(agg.InputCol)
				} else {
					u.markValue(agg.InputCol)
				}
			}
			if agg.InputExpr != nil {
				if col, ok := agg.InputExpr.(*plansql.ColRef); ok && countShape {
					u.markShape(col.Column)
					continue
				}
				classifyAST(agg.InputExpr, u)
			}
		}

	case NodeSort:
		for _, ob := range n.OrderBy {
			u.markValue(ob.Column)
		}

	case NodeLimit:
		// Pass-through.

	case NodeDual:
		// No columns.

	default:
		// NodeJoin, NodeDistinct, NodeWindow, NodeUnion/Intersect/Except and
		// anything added later: their column consumption is either a value
		// read (join keys, DISTINCT, window frames) or not modeled here.
		u.bail = true
		return
	}

	for _, c := range n.Children {
		walkShapeUses(c, u)
	}
}

// classifyPredicate handles both predicate forms: the decomposed
// Column/Op/Value triple and the raw AST.
func classifyPredicate(p Predicate, u *shapeUses) {
	if p.ASTExpr != nil {
		classifyAST(p.ASTExpr, u)
		return
	}
	if p.Column == "" {
		// A predicate carrying only Raw SQL that never became an AST could
		// reference anything.
		if p.Raw != "" {
			u.bail = true
		}
		return
	}
	switch strings.ToLower(p.Op) {
	case "is_null", "is_not_null":
		u.markShape(p.Column)
	case "=", "!=", "<>":
		if s, ok := p.Value.(string); ok && s == "" {
			u.markShape(p.Column)
			return
		}
		u.markValue(p.Column)
	default:
		u.markValue(p.Column)
	}
}

// classifyAST walks one expression tree. Every column reference it reaches
// is a value use unless it sits directly under a recognized shape form.
// Unhandled node types bail: an unwalked subtree could hide a value use.
func classifyAST(node plansql.Node, u *shapeUses) {
	if node == nil || u.bail {
		return
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		u.markValue(n.Column)

	case *plansql.Lit:
		// No columns.

	case *plansql.FuncCallNode:
		if len(n.Args) == 1 && !n.Distinct && !n.Star {
			name := strings.ToLower(n.Name)
			// COUNT(col) is a null-mask scan; the length family is an
			// offsets subtraction. Both leave the bytes untouched.
			if shapeLenFuncs[name] || name == "count" {
				if col, ok := n.Args[0].(*plansql.ColRef); ok {
					u.markShape(col.Column)
					return
				}
			}
		}
		for _, arg := range n.Args {
			classifyAST(arg, u)
		}

	case *plansql.IsExpr:
		if strings.EqualFold(n.Check, "null") {
			if col, ok := n.Left.(*plansql.ColRef); ok {
				u.markShape(col.Column)
				return
			}
		}
		classifyAST(n.Left, u)

	case *plansql.CmpExpr:
		switch n.Op {
		case "=", "!=", "<>":
			if col, ok := emptyStrCmpOperands(n); ok {
				u.markShape(col)
				return
			}
		}
		classifyAST(n.Left, u)
		classifyAST(n.Right, u)

	case *plansql.AndNode:
		classifyAST(n.Left, u)
		classifyAST(n.Right, u)
	case *plansql.OrNode:
		classifyAST(n.Left, u)
		classifyAST(n.Right, u)
	case *plansql.NotNode:
		classifyAST(n.Inner, u)
	case *plansql.ParenNode:
		classifyAST(n.Inner, u)
	case *plansql.BinaryOp:
		classifyAST(n.Left, u)
		classifyAST(n.Right, u)
	case *plansql.UnaryOp:
		classifyAST(n.Inner, u)
	case *plansql.CastNode:
		classifyAST(n.Inner, u)
	case *plansql.InExpr:
		classifyAST(n.Left, u)
		for _, v := range n.Values {
			classifyAST(v, u)
		}
	case *plansql.BetweenExpr:
		classifyAST(n.Left, u)
		classifyAST(n.Low, u)
		classifyAST(n.High, u)
	case *plansql.LikeExpr:
		classifyAST(n.Left, u)
		classifyAST(n.Pattern, u)
	case *plansql.CaseNode:
		if n.Subject != nil {
			classifyAST(n.Subject, u)
		}
		for _, w := range n.Whens {
			classifyAST(w.Cond, u)
			classifyAST(w.Result, u)
		}
		if n.Else != nil {
			classifyAST(n.Else, u)
		}

	default:
		// Subqueries, EXISTS, ANY/ALL, tuples, array literals, window
		// functions, star — anything whose column references this walk does
		// not enumerate. Abandon rather than guess.
		u.bail = true
	}
}

// emptyStrCmpOperands returns the column name when the comparison is a bare
// column against the empty string literal, in either operand order.
func emptyStrCmpOperands(n *plansql.CmpExpr) (string, bool) {
	if col, ok := n.Left.(*plansql.ColRef); ok && isEmptyStrLitNode(n.Right) {
		return col.Column, true
	}
	if col, ok := n.Right.(*plansql.ColRef); ok && isEmptyStrLitNode(n.Left) {
		return col.Column, true
	}
	return "", false
}

func isEmptyStrLitNode(node plansql.Node) bool {
	lit, ok := node.(*plansql.Lit)
	return ok && lit.Kind == plansql.LitString && lit.Value == ""
}
