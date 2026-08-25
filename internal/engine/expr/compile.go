package expr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// compileContext holds optional state for expression compilation.
type compileContext struct {
	runner      SubqueryRunner
	outerTables map[string]bool      // table aliases from the outer query scope
	outerCols   map[string]string    // column name → table mapping for unqualified resolution
	innerCols   plansql.TableColumns // a subquery's own column namespace, for scoping unqualified names
	// budget charges an uncorrelated IN-subquery's membership set to the
	// caller's per-task memory tracker (ADR-0006, #528). nil (the default,
	// and every existing CompileWith* entry point below) keeps the
	// pre-#528 unbudgeted behavior — set via CompileWithBudget.
	budget MemoryAccountant
}

// Compile converts our AST Node into an Expr tree.
func Compile(node plansql.Node) (Expr, error) {
	return compileWithCtx(node, &compileContext{})
}

// CompileWithRunner converts our AST Node into an Expr tree,
// with support for subquery expressions (scalar subqueries, IN subquery, EXISTS).
func CompileWithRunner(node plansql.Node, runner SubqueryRunner) (Expr, error) {
	return compileWithCtx(node, &compileContext{runner: runner})
}

// CompileWithScope converts our AST Node into an Expr tree with full scope
// information, enabling correlated subquery detection and per-row execution.
// outerTables contains the table names and aliases from the outer query.
func CompileWithScope(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool) (Expr, error) {
	return compileWithCtx(node, &compileContext{runner: runner, outerTables: outerTables})
}

// CompileWithFullScope is like CompileWithScope but also accepts a column-to-table
// mapping for resolving unqualified column references in correlated subqueries.
func CompileWithFullScope(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool, outerCols map[string]string) (Expr, error) {
	return CompileWithScopeResolver(node, runner, outerTables, outerCols, nil)
}

// CompileWithScopeResolver is CompileWithFullScope plus a resolver for the
// column namespace of a subquery's own FROM clause. It is what makes an
// unqualified name inside a subquery bind to the subquery first, so a name
// that merely also exists in the outer query does not turn an uncorrelated
// subquery into a per-row correlated one (issue #334). A nil resolver keeps
// the weaker table-identifier heuristic.
func CompileWithScopeResolver(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool, outerCols map[string]string, innerCols plansql.TableColumns) (Expr, error) {
	return compileWithCtx(node, &compileContext{
		runner:      runner,
		outerTables: outerTables,
		outerCols:   outerCols,
		innerCols:   innerCols,
	})
}

// CompileWithBudget is CompileWithScopeResolver plus a memory budget that an
// uncorrelated InSubquery charges its membership set against (ADR-0006,
// #528). budget may be nil, which keeps the pre-#528 unbudgeted behavior
// every other CompileWith* entry point still has; any *memory.Tracker
// satisfies MemoryAccountant structurally; see that type's doc for why this
// package does not import internal/engine/memory to accept one.
//
// outerTables, outerCols and innerCols may be nil for a top-level,
// non-correlated compile — pass CompileWithScope's or CompileWithRunner's
// arguments through unchanged and add only the budget.
//
// NOTHING IN PRODUCTION CALLS THIS YET — only this package's tests do, so
// #528's mechanism is present and inert. #531 is the wiring, into
// Planner.makeSubqueryRunner (internal/planner/physical/plan.go). Two things
// that wiring must do beyond the one-line call-site change, both easy to
// miss because neither shows up while Budget is nil:
//
//   - Call InSubquery.Release when the compiled Expr tree is torn down.
//     Release has no caller today; without one, every uncorrelated
//     IN-subquery in a task keeps its charge for the task's lifetime, and a
//     task that plans several runs out of budget for work that has already
//     finished. This is the half that needs a decision (where an Expr tree's
//     lifetime ends), not just a line.
//   - Not mistake the charge for a guard. chargeMemory runs AFTER
//     resolveSlow has built the map, so it makes the set visible to the
//     budget; it does not prevent the allocation. See its doc.
func CompileWithBudget(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool, outerCols map[string]string, innerCols plansql.TableColumns, budget MemoryAccountant) (Expr, error) {
	return compileWithCtx(node, &compileContext{
		runner:      runner,
		outerTables: outerTables,
		outerCols:   outerCols,
		innerCols:   innerCols,
		budget:      budget,
	})
}

func compileWithCtx(node plansql.Node, ctx *compileContext) (Expr, error) {
	if node == nil {
		return &Lit{Val: nil}, nil
	}
	switch n := node.(type) {
	case *plansql.ColRef:
		name := n.Column
		if n.Table != "" {
			// Preserve table qualifier for disambiguation (e.g., self-joins)
			name = n.Table + "." + n.Column
		}
		return &ColRef{Name: name}, nil

	case *plansql.Lit:
		return compileLit(n)

	case *plansql.StarNode:
		return &Lit{Val: "*"}, nil

	case *plansql.IntervalLit:
		iv := IntervalValue{}
		switch n.Unit {
		case "year":
			iv.Years = n.Value
		case "month":
			iv.Months = n.Value
		case "week":
			iv.Days = n.Value * 7
		case "day":
			iv.Days = n.Value
		case "hour":
			iv.Hours = n.Value
		case "minute":
			iv.Minutes = n.Value
		case "second":
			iv.Seconds = n.Value
		default:
			iv.Days = n.Value // fallback
		}
		return &Lit{Val: iv}, nil

	case *plansql.BinaryOp:
		if n.Op == "||" {
			// SQL string concatenation. Compiled as concat() so it shares
			// that function's scalar and vectorized kernels and its
			// declared String return type — BinOp.Eval only knows
			// arithmetic, and returned NULL for every row (#328).
			return compileFuncCallNode(&plansql.FuncCallNode{
				Name: "concat",
				Args: []plansql.Node{n.Left, n.Right},
			}, ctx)
		}
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return compileBinOp(left, right, n.Op), nil

	case *plansql.UnaryOp:
		operand, err := compileWithCtx(n.Inner, ctx)
		if err != nil {
			return nil, err
		}
		// Fold a negated numeric literal into the literal itself, so `(-7)/2`
		// carries an int64 operand and takes the integer-division path (#369)
		// exactly as `7/2` does. Value-identical for floats.
		if n.Op == "-" {
			if lit, ok := operand.(*Lit); ok {
				// The source text is negated with the box: it is the only
				// exact record of a literal wider than a float64, and a
				// negative bound is where a DECIMAL column's extreme values
				// sit (#452).
				switch v := lit.Val.(type) {
				case int64:
					return &Lit{Val: -v, Text: negateLitText(lit.Text)}, nil
				case float64:
					return &Lit{Val: -v, Text: negateLitText(lit.Text)}, nil
				case string:
					// A numeric literal that neither an int64 nor a float64
					// can hold is boxed as its own text (compileLit's last
					// arm — 1e400 makes strconv.ParseFloat report ErrRange).
					// Leaving the minus outside as a UnaryOp hid the literal
					// from every binding that looks for one, so `d >= -1e400`
					// lost the exact text the DECIMAL comparison reads (#463).
					if lit.Text != "" {
						return &Lit{Val: negateLitText(v), Text: negateLitText(lit.Text)}, nil
					}
					// Text is empty here, so this is a QUOTED string literal,
					// not a numeric token straight from the parser — `-'abc'`
					// or `-'1e400'` (the exponent written INSIDE the quotes).
					// Left as a bare UnaryOp, the minus evaluated through the
					// generic numeric-coercion path, which reads anything it
					// cannot parse as the float64 zero — #463's exact failure
					// mode resurrected on the one shape #463 never touched:
					// `d = -'abc'` matched every row holding 0.00 (#505).
					//
					// A string whose CONTENT is a number folds the same way
					// an unquoted numeric literal already does two cases
					// above — Text carries the negated source text into the
					// DecimalLiteral path, so `d = -'5.00'` keeps working
					// exactly like `d = -5.00`. A string that is not a
					// number is refused HERE, at compile time: unary minus
					// has no reading of it in any context, so there is no
					// row to wait for the way a boxed comparison must (and
					// unlike `d = 'abc'`, which could in principle be a
					// legitimate string comparison against a non-DECIMAL
					// column, `-'abc'` cannot be a value of any type).
					if isNumericLitText(v) {
						return &Lit{Val: negateLitText(v), Text: negateLitText(v)}, nil
					}
					return nil, invalidNumericLiteralError(v)
				}
			}
		}
		return &UnaryOp{Operand: operand, Op: n.Op}, nil

	case *plansql.CmpExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		switch n.Op {
		case "is distinct from":
			return &IsDistinctFrom{Left: left, Right: right}, nil
		case "is not distinct from":
			return &IsDistinctFrom{Left: left, Right: right, Not: true}, nil
		}
		var op CmpOp
		switch n.Op {
		case "=":
			op = CmpEq
		case "!=", "<>":
			op = CmpNe
		case "<":
			op = CmpLt
		case "<=":
			op = CmpLe
		case ">":
			op = CmpGt
		case ">=":
			op = CmpGe
		default:
			op = CmpEq
		}
		return compileCmp(left, right, op), nil

	case *plansql.InExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		// Check for subquery IN: single SubqueryNode in values
		if len(n.Values) == 1 {
			if sq, ok := n.Values[0].(*plansql.SubqueryNode); ok {
				if ctx.runner == nil {
					return nil, fmt.Errorf("IN subquery requires a SubqueryRunner")
				}
				if len(ctx.outerTables) > 0 {
					var refs []plansql.OuterRef
					var err error
					if len(ctx.outerCols) > 0 {
						refs, err = plansql.FindCorrelatedRefsWithScope(sq.SQL, ctx.outerTables, ctx.outerCols, ctx.innerCols)
					} else {
						refs, err = plansql.FindCorrelatedRefs(sq.SQL, ctx.outerTables)
					}
					if err == nil && len(refs) > 0 {
						parsed, _ := plansql.Parse(sq.SQL)
						info, _ := plansql.ExtractSelect(parsed)
						if info != nil {
							return &CorrelatedInSubquery{
								Expr:            left,
								Runner:          ctx.runner,
								Not:             n.Not,
								OuterRefs:       refs,
								OuterTables:     ctx.outerTables,
								ParsedInfo:      info,
								UnqualOuterCols: buildUnqualOuterCols(refs, ctx.outerCols),
							}, nil
						}
					}
				}
				return &InSubquery{Expr: left, SQL: sq.SQL, Runner: ctx.runner, Not: n.Not, Budget: ctx.budget}, nil
			}
		}
		var values []Expr
		for _, v := range n.Values {
			compiled, err := compileWithCtx(v, ctx)
			if err != nil {
				return nil, err
			}
			values = append(values, compiled)
		}
		return NewIn(left, values, n.Not), nil

	case *plansql.BetweenExpr:
		e, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		low, err := compileWithCtx(n.Low, ctx)
		if err != nil {
			return nil, err
		}
		hi, err := compileWithCtx(n.High, ctx)
		if err != nil {
			return nil, err
		}
		return NewBetween(e, low, hi, n.Not), nil

	case *plansql.LikeExpr:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Pattern, ctx)
		if err != nil {
			return nil, err
		}
		return &Like{Expr: left, Pattern: right, Not: n.Not}, nil

	case *plansql.IsExpr:
		operand, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		switch n.Check {
		case "null":
			isNull := &IsNull{Operand: operand, Not: n.Not}
			// Offsets-shape: IS [NOT] NULL over a bare column is a null-mask
			// test. The generic node boxes ColRef.Eval's result — for a
			// string column that is a full copy of the value — only to
			// compare it against nil (shape_funcs.go).
			if col, ok := operand.(*ColRef); ok {
				return &ColIsNull{Col: col, Not: n.Not, Fallback: isNull}, nil
			}
			return isNull, nil
		case "true":
			// IS [NOT] TRUE/FALSE is a NULL-test, not a comparison: it
			// never answers UNKNOWN (NULL IS TRUE → false, NULL IS NOT
			// TRUE → true). The old Cmp spelling was equivalent only while
			// Cmp itself collapsed NULL to false (#370).
			return &IsBool{Operand: operand, Want: true, Not: n.Not}, nil
		case "false":
			return &IsBool{Operand: operand, Want: false, Not: n.Not}, nil
		default:
			return nil, fmt.Errorf("unsupported IS check: %s", n.Check)
		}

	case *plansql.AndNode:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return &And{Left: left, Right: right}, nil

	case *plansql.OrNode:
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return &Or{Left: left, Right: right}, nil

	case *plansql.NotNode:
		operand, err := compileWithCtx(n.Inner, ctx)
		if err != nil {
			return nil, err
		}
		return &Not{Operand: operand}, nil

	case *plansql.ParenNode:
		return compileWithCtx(n.Inner, ctx)

	case *plansql.FuncCallNode:
		return compileFuncCallNode(n, ctx)

	case *plansql.CaseNode:
		return compileCaseNode(n, ctx)

	case *plansql.CastNode:
		operand, err := compileWithCtx(n.Inner, ctx)
		if err != nil {
			return nil, err
		}
		return &Cast{Operand: operand, DestType: strings.ToLower(n.TypeName)}, nil

	case *plansql.SubqueryNode:
		// Scalar subquery: (SELECT ...)
		if ctx.runner == nil {
			return nil, fmt.Errorf("subqueries require a SubqueryRunner")
		}
		if len(ctx.outerTables) > 0 {
			var refs []plansql.OuterRef
			var err error
			if len(ctx.outerCols) > 0 {
				refs, err = plansql.FindCorrelatedRefsWithScope(n.SQL, ctx.outerTables, ctx.outerCols, ctx.innerCols)
			} else {
				refs, err = plansql.FindCorrelatedRefs(n.SQL, ctx.outerTables)
			}
			if err == nil && len(refs) > 0 {
				parsed, _ := plansql.Parse(n.SQL)
				info, _ := plansql.ExtractSelect(parsed)
				if info != nil {
					return &CorrelatedScalarSubquery{
						Runner:          ctx.runner,
						OuterRefs:       refs,
						OuterTables:     ctx.outerTables,
						ParsedInfo:      info,
						UnqualOuterCols: buildUnqualOuterCols(refs, ctx.outerCols),
					}, nil
				}
			}
		}
		return &ScalarSubquery{SQL: n.SQL, Runner: ctx.runner}, nil

	case *plansql.ExistsNode:
		if ctx.runner == nil {
			return nil, fmt.Errorf("EXISTS subquery requires a SubqueryRunner")
		}
		if len(ctx.outerTables) > 0 {
			var refs []plansql.OuterRef
			var err error
			if len(ctx.outerCols) > 0 {
				refs, err = plansql.FindCorrelatedRefsWithScope(n.SQL, ctx.outerTables, ctx.outerCols, ctx.innerCols)
			} else {
				refs, err = plansql.FindCorrelatedRefs(n.SQL, ctx.outerTables)
			}
			if err == nil && len(refs) > 0 {
				parsed, _ := plansql.Parse(n.SQL)
				info, _ := plansql.ExtractSelect(parsed)
				if info != nil {
					return &CorrelatedExistsSubquery{
						Runner:          ctx.runner,
						Not:             n.Not,
						OuterRefs:       refs,
						OuterTables:     ctx.outerTables,
						ParsedInfo:      info,
						UnqualOuterCols: buildUnqualOuterCols(refs, ctx.outerCols),
					}, nil
				}
			}
		}
		return &ExistsSubquery{SQL: n.SQL, Runner: ctx.runner, Not: n.Not}, nil

	case *plansql.ArrayLitNode:
		elems := make([]Expr, len(n.Elements))
		for i, e := range n.Elements {
			compiled, err := compileWithCtx(e, ctx)
			if err != nil {
				return nil, fmt.Errorf("compiling array element %d: %w", i, err)
			}
			elems[i] = compiled
		}
		return &ArrayLitExpr{Elements: elems}, nil

	default:
		return &Lit{Val: node.String()}, nil
	}
}

// compileCmp creates a typed comparison when both sides are provably numeric,
// falling back to the generic Cmp otherwise.
// We can't use typed paths for ColRef since column types are unknown at compile time.
func compileCmp(left, right Expr, op CmpOp) Expr {
	// Only use typed paths when both sides are provably numeric
	// (not ColRef, which implements Int64Expr/Float64Expr as fallback)
	if isProvablyInt64(left) && isProvablyInt64(right) {
		return &CmpInt64{Left: left.(Int64Expr), Right: right.(Int64Expr), Op: op}
	}
	if isProvablyFloat64(left) && isProvablyFloat64(right) {
		return &CmpFloat64{Left: left.(Float64Expr), Right: right.(Float64Expr), Op: op}
	}
	// Bare column vs a string literal that parses as a date/timestamp:
	// pre-parse the literal once (both temporal units) and pick the unit
	// per batch from the column's resolved type — the dominant pushed-
	// filter shape (`l_shipdate <= '1998-09-02'`). Column type is unknown
	// here, so CmpTemporalLit keeps a generic fallback for non-temporal
	// columns; semantics stay identical to Cmp in every sub-case.
	if col, ok := left.(*ColRef); ok && col.structField == "" {
		if tl := tryTemporalLit(col, right, op, false); tl != nil {
			return tl
		}
		if nl := tryNetworkLit(col, right, op, false); nl != nil {
			return nl
		}
	}
	if col, ok := right.(*ColRef); ok && col.structField == "" {
		if tl := tryTemporalLit(col, left, op, true); tl != nil {
			return tl
		}
		if nl := tryNetworkLit(col, left, op, true); nl != nil {
			return nl
		}
	}
	generic := NewCmp(left, right, op)
	// Offsets-shape: `col = ''` / `col <> ''` is a zero-length test on the
	// offsets array. WHERE SearchPhrase <> '' is the dominant filter shape
	// across ClickBench Q22-Q27; the generic path copies every value out of
	// the arena just to compare its length against zero (shape_funcs.go).
	if op == CmpEq || op == CmpNe {
		if col, other := emptyStrOperands(left, right); col != nil && other {
			return &ColEmptyStr{Col: col, Not: op == CmpNe, Fallback: generic}
		}
	}
	return generic
}

// emptyStrOperands reports whether the pair is a bare column reference
// against the empty string literal, in either operand order.
func emptyStrOperands(left, right Expr) (*ColRef, bool) {
	if col, ok := left.(*ColRef); ok && isEmptyStrLit(right) {
		return col, true
	}
	if col, ok := right.(*ColRef); ok && isEmptyStrLit(left) {
		return col, true
	}
	return nil, false
}

func isEmptyStrLit(e Expr) bool {
	lit, ok := e.(*Lit)
	if !ok {
		return false
	}
	s, ok := lit.Val.(string)
	return ok && s == ""
}

// tryTemporalLit builds a CmpTemporalLit when other is a string literal
// parseable as a date or timestamp; nil otherwise.
func tryTemporalLit(col *ColRef, other Expr, op CmpOp, flip bool) *CmpTemporalLit {
	lit, ok := other.(*Lit)
	if !ok {
		return nil
	}
	s, ok := lit.Val.(string)
	if !ok {
		return nil
	}
	days, dok := parseDateToEpochDaysOK(s)
	ms, mok := parseTimestampToEpochMsOK(s)
	if !dok && !mok {
		return nil
	}
	return &CmpTemporalLit{Col: col, Lit: s, Op: op, Flip: flip, days: days, ms: ms}
}

// tryNetworkLit builds a CmpNetworkLit when other is a string literal that
// parses as an IPv4 address, a MAC address, an IPv6 address, or a CIDR
// network; nil otherwise. Mirrors tryTemporalLit for the network-typed
// columns: see CmpNetworkLit for why they need it. UUID does not go through
// this path: ColRef.Eval already renders it as its zero-padded hex TEXT
// (Vector.GetValue's default case), and lexical order of that fixed-width
// text happens to equal the UUID's own byte order, which is enough for
// ordering too, not just equality — an accident that does NOT generalize to
// IPv6 (variable-width `::`-compressed hex) or CIDR (variable-width prefix
// notation), which is why those two DO need a typed comparator (#492): the
// column renders as text there too, but comparing that text lexically (<, >,
// <=, >=) is not the address's numeric/structural order, is not even
// consistent between this expr path's WHERE and SELECT evaluation, and used
// to disagree outright with the stage DAG for IPv6 (both compile predicates
// through this same function; before this fix, an IPv6/CIDR literal made
// tryNetworkLit return nil, so the predicate fell to a plain *expr.Cmp and
// its generic per-row path, which is where the lexical comparison happened).
//
// Column type is unknown at compile time (see tryTemporalLit's own comment),
// so this cannot be, and does not need to be, restricted to columns that are
// ACTUALLY network-typed: a STRING column whose literal happens to parse as
// an address (`s = '10.1.2.3'`) gets wrapped the same way, but
// extractFilterOps' *expr.CmpNetworkLit case (internal/planner/physical/
// plan.go) and CmpNetworkLit.EvalBoolNull's genericFallback both defer
// entirely to the column's REAL type at kernel-build/eval time — a STRING
// column takes its ordinary compareFilterString/lexical-compare path either
// way, never one of the typed branches.
// The IPv6 key comes from kernel.IPv6LitKey, which — unlike the local parse
// this used to do — accepts a v4 literal too, keying it BELOW every v6 row
// (PostgreSQL compares the address FAMILY first). The two encodings no longer
// have to be mutually exclusive because the branch is chosen by the COLUMN's
// resolved type, never by which parses the literal accepted: a v4 literal
// legitimately keys as an IPv4 int64, as a v6 family sentinel, and as a /32
// CIDR key all at once, and exactly one of those is read.
func tryNetworkLit(col *ColRef, other Expr, op CmpOp, flip bool) *CmpNetworkLit {
	lit, ok := other.(*Lit)
	if !ok {
		return nil
	}
	s, ok := lit.Val.(string)
	if !ok {
		return nil
	}
	ipv4, ipv4ok := ipv4LitToInt64(s)
	mac, macok := macLitToInt64(s)
	ipv6, ipv6ok := kernel.IPv6LitKey(s)
	cidr, cidrok := kernel.CidrSortKey(s)
	if !ipv4ok && !macok && !ipv6ok && !cidrok {
		return nil
	}
	return &CmpNetworkLit{
		Col: col, Lit: s, Op: op, Flip: flip,
		ipv4: ipv4, ipv4ok: ipv4ok,
		mac: mac, macok: macok,
		ipv6: ipv6, ipv6ok: ipv6ok,
		cidr: cidr, cidrok: cidrok,
	}
}

// isProvablyInt64 returns true if the expression definitely produces int64 values.
func isProvablyInt64(e Expr) bool {
	switch v := e.(type) {
	case *Lit:
		switch v.Val.(type) {
		case int64, int32, int:
			return true
		}
	case *BinOpInt64:
		return true
	}
	return false
}

// isProvablyFloat64 returns true if the expression definitely produces float64 values.
func isProvablyFloat64(e Expr) bool {
	switch v := e.(type) {
	case *Lit:
		switch v.Val.(type) {
		case float64, float32:
			return true
		}
	case *BinOpFloat64:
		return true
	}
	return false
}

// compileBinOp creates a typed BinOp when both sides implement typed interfaces,
// falling back to the generic BinOp otherwise.
func compileBinOp(left, right Expr, op string) Expr {
	// `date ± INTERVAL` is not arithmetic, and nothing below can tell: an
	// interval Lit satisfies Float64Expr like any other literal (ToFloat64 of
	// an IntervalValue is 0), so the typed nodes took the expression and
	// silently dropped the interval — `o_orderdate - INTERVAL '90' DAY`
	// projected the column's raw epoch-day number, and a string date column
	// projected NULL (issue #332). Only the generic BinOp knows this shape,
	// so route it there. A date LITERAL already arrived here as a CastNode,
	// which is not a Float64Expr, which is why that form always worked.
	if isIntervalLit(left) || isIntervalLit(right) {
		return &BinOp{Left: left, Right: right, Op: op}
	}
	// Try float64 typed path (covers float64 columns, int64 columns via promotion, and literals)
	lf, lfOk := left.(Float64Expr)
	rf, rfOk := right.(Float64Expr)
	if lfOk && rfOk {
		// Prefer int64 path if both sides are native int64 (not float
		// literals). This includes `/`: integer division truncates toward
		// zero, PostgreSQL's rule (#369, ADR-0012 — the previous float-`/`
		// pin followed DuckDB, which ADR-0012 overturns).
		li, liOk := left.(Int64Expr)
		ri, riOk := right.(Int64Expr)
		if liOk && riOk && isIntNative(left) && isIntNative(right) {
			return &BinOpInt64{Left: li, Right: ri, Op: op}
		}
		// Column operands: their types resolve on the first batch, so the
		// int-vs-float decision defers there (BinOpNumeric). Only when both
		// sides COULD be integers at runtime — a compile-time float operand
		// pins the whole expression float, so keep the direct float node.
		ln, lnOk := left.(numericOperand)
		rn, rnOk := right.(numericOperand)
		// The int-arith kill switch is read inside resolveMode rather than
		// here, so which NODE is built no longer depends on it: a disabled
		// switch means BinOpNumeric resolves to its float delegate, which is
		// bit-identical to the BinOpFloat64 below (integer DIVISION keeps its
		// truncation on both settings — semantics never ride a kill switch;
		// see BinOpNumeric.divTrunc). Keeping the node choice
		// switch-independent is what keeps `date_col - date_col` (resolved in
		// the same place, from the same column types — #340) answering the
		// same on both settings.
		if lnOk && rnOk &&
			possiblyIntAtRuntime(left) && possiblyIntAtRuntime(right) {
			return &BinOpNumeric{Left: ln, Right: rn, Op: op}
		}
		return &BinOpFloat64{Left: lf, Right: rf, Op: op}
	}
	return &BinOp{Left: left, Right: right, Op: op}
}

// isIntervalLit reports whether an operand is an INTERVAL literal — the one
// operand shape that makes a binary + or - date arithmetic rather than numeric.
func isIntervalLit(e Expr) bool {
	l, ok := e.(*Lit)
	if !ok {
		return false
	}
	_, ok = l.Val.(IntervalValue)
	return ok
}

// possiblyIntAtRuntime reports whether an operand might turn out integer
// once column types resolve: columns and deferred arithmetic qualify;
// literals and everything else are decided at compile time.
func possiblyIntAtRuntime(e Expr) bool {
	switch e.(type) {
	case *ColRef:
		return true
	case *BinOpNumeric:
		return true
	default:
		return isIntNative(e)
	}
}

// isIntNative returns true if the expression natively produces int64 values
// (not a float literal or float column that happens to implement Int64Expr).
func isIntNative(e Expr) bool {
	switch v := e.(type) {
	case *Lit:
		switch v.Val.(type) {
		case int64, int32, int:
			return true
		default:
			return false
		}
	case *ColRef:
		return false // column type unknown at compile time; use float64 path for safety
	case *BinOpInt64:
		return true
	default:
		return false
	}
}

func compileLit(n *plansql.Lit) (Expr, error) {
	switch n.Kind {
	case plansql.LitNumber:
		// The source text rides along on every numeric literal. The box below
		// is for arithmetic and is lossy by construction past a float64's
		// ~15-16 significant digits, so it cannot be the only record of what
		// the user wrote: a comparison against a DECIMAL column reads Text and
		// answers in the column's own exact domain instead (#452).
		//
		// Try integer first
		if i, err := strconv.ParseInt(n.Value, 10, 64); err == nil {
			return &Lit{Val: i, Text: n.Value}, nil
		}
		// Try float
		if f, err := strconv.ParseFloat(n.Value, 64); err == nil {
			return &Lit{Val: f, Text: n.Value}, nil
		}
		return &Lit{Val: n.Value, Text: n.Value}, nil
	case plansql.LitString:
		return &Lit{Val: n.Value}, nil
	case plansql.LitBool:
		return &Lit{Val: strings.ToLower(n.Value) == "true"}, nil
	case plansql.LitNull:
		return &Lit{Val: nil}, nil
	default:
		return &Lit{Val: n.Value}, nil
	}
}

func compileFuncCallNode(n *plansql.FuncCallNode, ctx *compileContext) (Expr, error) {
	name := strings.ToLower(n.Name)

	var args []Expr
	if n.Star {
		args = append(args, &Lit{Val: "*"})
	} else {
		for _, arg := range n.Args {
			compiled, err := compileWithCtx(arg, ctx)
			if err != nil {
				return nil, err
			}
			args = append(args, compiled)
		}
	}

	// Check for COALESCE special form
	if name == "coalesce" {
		return &Coalesce{Args: args}, nil
	}

	if err := checkKnown(name); err != nil {
		return nil, err
	}

	fc := &FuncCall{Name: name, Args: args}
	// ROUND on a DOUBLE PRECISION (or REAL/FLOAT — Wadjet's Cast collapses
	// all three to the same runtime float64) operand rounds half TO EVEN in
	// PostgreSQL; ROUND on NUMERIC — the default, no CAST at all — rounds
	// half AWAY from zero, which fnRound already implements correctly. The
	// two runtime values are indistinguishable bare float64s by the time a
	// kernel would see them (no numeric tower), so the type distinction has
	// to be caught here, from the immediate operand's own CAST, before that
	// boxing happens (#381). This is independent of CAST(x AS integer)'s
	// half-away-from-zero rounding rule (#373) — PostgreSQL specifies CAST
	// and ROUND separately, and unifying them would be wrong for one of them.
	if name == "round" && len(args) >= 1 && isBinaryFloatCast(args[0]) {
		fc.Name = "round_half_even"
	}
	// Offsets-shape: length()/octet_length()/bit_length() over a bare column
	// reference are offsets subtractions, not value reads (shape_funcs.go).
	// The node carries fc as its fallback and takes it for every column that
	// is not a flat byte-array column, so semantics are unchanged.
	if mul := shapeLenMul[name]; mul != 0 && len(args) == 1 {
		if col, ok := args[0].(*ColRef); ok {
			return &ColShapeLen{Col: col, Mul: mul, Fallback: fc}, nil
		}
	}
	// A function that always returns a number is wrapped so it satisfies
	// Float64Expr/Int64Expr and binary operators over it take the typed path.
	// This used to be a second hand-maintained name list, which had drifted
	// from the first: it carried `date_part` and `ascii`, neither of which is
	// registered at all, and missed every numeric function added since.
	if DefaultRegistry.ReturnType(name).Numeric() {
		return &numericFuncCall{fc}, nil
	}
	return fc, nil
}

// isBinaryFloatCast reports whether e is an explicit CAST to one of
// PostgreSQL's IEEE-754 binary floating-point types — double precision,
// real, or float — as opposed to NUMERIC/DECIMAL or a bare literal/column.
// It is the one signal left, post-parse, that an operand is declared
// DOUBLE PRECISION rather than NUMERIC: Wadjet represents both as a plain
// float64 at runtime (#381). Only the immediate operand is checked, matching
// the reproduction this fixes (ROUND(CAST(x AS double precision))) — a cast
// buried inside a larger expression (ROUND(CAST(x AS double precision) + 1))
// is out of scope.
func isBinaryFloatCast(e Expr) bool {
	c, ok := e.(*Cast)
	if !ok {
		return false
	}
	switch strings.ToLower(c.DestType) {
	case "double", "real", "float":
		return true
	default:
		return false
	}
}

// shapeLenMul maps the byte-length family to its multiplier. char_length /
// character_length are deliberately absent: they count runes and must read
// the bytes.
var shapeLenMul = map[string]int{
	"length":       1,
	"len":          1,
	"octet_length": 1,
	"bit_length":   8,
}

func compileCaseNode(n *plansql.CaseNode, ctx *compileContext) (Expr, error) {
	c := &Case{}

	if n.Subject != nil {
		var err error
		c.Operand, err = compileWithCtx(n.Subject, ctx)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range n.Whens {
		cond, err := compileWithCtx(when.Cond, ctx)
		if err != nil {
			return nil, err
		}
		result, err := compileWithCtx(when.Result, ctx)
		if err != nil {
			return nil, err
		}
		c.Whens = append(c.Whens, CaseWhen{Cond: cond, Result: result})
	}

	if n.Else != nil {
		var err error
		c.Else, err = compileWithCtx(n.Else, ctx)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

// buildUnqualOuterCols extracts unqualified outer column mappings from detected refs.
// These are refs that were resolved via outerCols (column→table mapping) rather than
// explicit table qualifiers.
func buildUnqualOuterCols(refs []plansql.OuterRef, outerCols map[string]string) map[string]string {
	if len(outerCols) == 0 {
		return nil
	}
	var result map[string]string
	for _, ref := range refs {
		col := ref.Column
		if tbl, ok := outerCols[col]; ok && tbl == ref.Table {
			if result == nil {
				result = make(map[string]string)
			}
			result[col] = ref.Table
		}
	}
	return result
}

// CompileSelectExpr compiles a SELECT column expression from our AST.
// Returns the compiled expression and the output column name.
func CompileSelectExpr(expr plansql.Node, alias string) (Expr, string, error) {
	compiled, err := Compile(expr)
	if err != nil {
		return nil, "", err
	}

	name := alias
	if name == "" {
		if colRef, ok := expr.(*plansql.ColRef); ok {
			name = colRef.Column
		} else {
			name = expr.String()
		}
	}

	return compiled, name, nil
}
