package expr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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
	// pre-#528 unbudgeted behavior — set via the WithBudget option.
	budget MemoryAccountant
	// trackInSubquery is handed every budgeted InSubquery this compile
	// builds, so the caller has something to call Release on. It is set by
	// WithBudget and only by WithBudget, because a charge with no teardown is
	// worse than no charge at all (#531).
	trackInSubquery func(*InSubquery)
	// colTypes is the DECLARED type of each input column, when the caller
	// knows it. compileBinOp needs it for one decision and one only: a pair
	// that COULD be DECIMAL at runtime has to reach BinOpNumeric to find out,
	// and a column whose type is already known to be FLOAT never could — so
	// `float_col * 2.0` keeps the BinOpFloat64 it has always compiled to,
	// with its vectorized path, instead of paying a per-row evaluator for a
	// question already answered (#555 review).
	//
	// nil means "not known", which is every caller that has no schema, and
	// answers exactly as this layer did before: assume a column might be a
	// DECIMAL and resolve the mode against the first batch.
	colTypes map[string]batch.TypeID
	// subqueryDecl answers the DECLARED type of a scalar subquery's single
	// output column, so `WHERE a > (SELECT AVG(a) FROM t)` compares as the
	// numbers the two sides ARE rather than as the strings they box to.
	//
	// A DECIMAL boxes as its rendered TEXT, and an operand this layer cannot
	// classify is boxUnknown — so the pair fell through to compare()'s
	// LEXICOGRAPHIC comparison and `"12.75" > "7.570000"` was FALSE. Over
	// decpair that made `a > (SELECT AVG(a))` select nothing where PostgreSQL
	// selects four rows (#696). The declaration is what fixes it: reading the
	// subquery's VALUE to decide the rule would be the same value-re-read-as-
	// type defect #727 was.
	//
	// nil means "not known", which is every caller that cannot plan a
	// subquery — the compile then classifies as it always did.
	subqueryDecl SubqueryDeclFunc
	// setRowBound bounds the MEMBERSHIP SET an IN-subquery may build, in
	// rows. Zero means unbounded, which is every caller that does not ask.
	//
	// It sits on the IN constructs and nowhere else because it is a bound on
	// a SET, and IN is the only construct that wants one: EXISTS wants to
	// know whether there is a row and a scalar subquery is an error past one,
	// so both read a bounded number of rows by construction
	// (plansql.AppendRowLimit) and neither is charged for a set it never
	// builds. Bounding them anyway refused `DELETE ... WHERE EXISTS (SELECT 1
	// FROM big)` where PostgreSQL answers, and reported a multi-row scalar
	// subquery as 54000 — a resource complaint — where this engine's own rule
	// is 21000, a statement about the data.
	setRowBound int
}

// SubqueryDeclFunc resolves a scalar subquery's SQL to the declared type of
// its single output column. ok=false for a subquery whose output type the
// caller cannot resolve; the comparison then falls back to the boxed rules it
// had before.
type SubqueryDeclFunc func(sql string) (typ batch.TypeID, precision, scale int, ok bool)

// A CompileOption is an optional input to the Compile* entry points. It is
// variadic so that adding one costs no caller a signature change — the
// entry-point set is already six wide and each new question would otherwise
// double it.
type CompileOption func(*compileContext)

// WithSubqueryDeclTypes supplies the resolver described on
// compileContext.subqueryDecl (#696).
func WithSubqueryDeclTypes(f SubqueryDeclFunc) CompileOption {
	return func(c *compileContext) { c.subqueryDecl = f }
}

// WithBudget charges an uncorrelated InSubquery's membership set to the
// caller's memory tracker (ADR-0006, #528, #531), and hands the caller each
// such node so it can Release the charge when the compiled tree's life ends.
//
// It is an OPTION rather than a seventh CompileWith* function because the
// options carry things a compile site already needs: swapping a call site to
// an entry point that takes a budget and nothing else silently drops
// WithSubqueryDeclTypes, and a scalar subquery then compares by the bytes of
// its box again (#696). Every existing entry point takes opts; this composes
// with them.
//
// release is REQUIRED and the option refuses a nil one, because the failure it
// prevents is worse than the bug it fixes: an InSubquery holds its membership
// map for the life of the compiled tree, so charging without a teardown turns
// an unaccounted map into a permanently-charged one, and a task that plans
// several of them runs out of budget for work that has already finished.
// InSubquery.Release is idempotent and safe on a node that never resolved.
//
// What this does NOT do is bound the ALLOCATION. chargeMemory runs after
// resolveSlow has built the map, so it makes the set visible to the budget and
// turns a set that is over budget on its own into a query error; it does not
// stop a subquery large enough to exhaust the machine from doing so. See
// chargeMemory's doc.
func WithBudget(budget MemoryAccountant, release func(*InSubquery)) CompileOption {
	if budget == nil || release == nil {
		return nil
	}
	return func(c *compileContext) {
		c.budget = budget
		c.trackInSubquery = release
	}
}

// WithSetRowBound bounds the membership set an IN-subquery may build, in
// rows, and refuses past it rather than truncating: a set short by one row is
// a different answer, and on a write door it deletes the wrong rows.
//
// It is the DML doors' knob (`WADJET_IN_SET_MAX`, the same number the query
// path gives its own inlining) and it is applied HERE, at the construct, and
// not in the runner those doors hand the compiler — a runner sees SQL text
// and cannot tell which construct asked, so bounding there charged EXISTS and
// a scalar subquery for rows neither reads. n <= 0 leaves the set unbounded.
func WithSetRowBound(n int) CompileOption {
	if n <= 0 {
		return nil
	}
	return func(c *compileContext) { c.setRowBound = n }
}

func applyCompileOptions(c *compileContext, opts []CompileOption) *compileContext {
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	return c
}

// Compile converts our AST Node into an Expr tree.
func Compile(node plansql.Node) (Expr, error) {
	return compileWithCtx(node, &compileContext{})
}

// CompileWithRunner converts our AST Node into an Expr tree,
// with support for subquery expressions (scalar subqueries, IN subquery, EXISTS).
func CompileWithRunner(node plansql.Node, runner SubqueryRunner, opts ...CompileOption) (Expr, error) {
	return compileWithCtx(node, applyCompileOptions(&compileContext{runner: runner}, opts))
}

// CompileWithScope converts our AST Node into an Expr tree with full scope
// information, enabling correlated subquery detection and per-row execution.
// outerTables contains the table names and aliases from the outer query.
func CompileWithScope(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool, opts ...CompileOption) (Expr, error) {
	return compileWithCtx(node, applyCompileOptions(&compileContext{runner: runner, outerTables: outerTables}, opts))
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
func CompileWithScopeResolver(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool, outerCols map[string]string, innerCols plansql.TableColumns, opts ...CompileOption) (Expr, error) {
	return compileWithCtx(node, applyCompileOptions(&compileContext{
		runner:      runner,
		outerTables: outerTables,
		outerCols:   outerCols,
		innerCols:   innerCols,
	}, opts))
}

// CompileWithBudget is CompileWithScopeResolver plus the WithBudget option,
// kept as the shape this package's own tests compile through. Production wires
// the budget through WithBudget on whichever entry point the call site already
// uses, so the compile keeps the other options it was passing (#531).
//
// budget may be nil, which keeps the pre-#528 unbudgeted behavior every other
// CompileWith* entry point still has; any *memory.Tracker satisfies
// MemoryAccountant structurally; see that type's doc for why this package does
// not import internal/engine/memory to accept one.
//
// outerTables, outerCols and innerCols may be nil for a top-level,
// non-correlated compile.
//
// release is the teardown hook WithBudget requires, and this entry point does
// not get to skip it: injecting a no-op here would be the exact state
// WithBudget refuses to construct, spelled differently. It may be nil only
// alongside a nil budget, where nothing is charged.
func CompileWithBudget(node plansql.Node, runner SubqueryRunner, outerTables map[string]bool, outerCols map[string]string, innerCols plansql.TableColumns, budget MemoryAccountant, release func(*InSubquery), opts ...CompileOption) (Expr, error) {
	if budget != nil {
		opts = append(opts, WithBudget(budget, release))
	}
	return compileWithCtx(node, applyCompileOptions(&compileContext{
		runner:      runner,
		outerTables: outerTables,
		outerCols:   outerCols,
		innerCols:   innerCols,
	}, opts))
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
			// SQL string concatenation. Compiled as a registered function so
			// it gets a scalar and a vectorized kernel and a declared String
			// return type — BinOp.Eval only knows arithmetic, and returned
			// NULL for every row (#328).
			//
			// The function it lowers to is ConcatOpFunc, NOT `concat`. They
			// were the same function until #609, and they answer differently
			// on a NULL: PostgreSQL's CONCAT() IGNORES a NULL argument while
			// `||` PROPAGATES it. Sharing one kernel meant only one of the
			// two could be right, and the one that was right was `||`.
			return compileFuncCallNamed(&plansql.FuncCallNode{
				Name: ConcatOpFunc,
				Args: []plansql.Node{n.Left, n.Right},
			}, ctx, false)
		}
		left, err := compileWithCtx(n.Left, ctx)
		if err != nil {
			return nil, err
		}
		right, err := compileWithCtx(n.Right, ctx)
		if err != nil {
			return nil, err
		}
		return compileBinOp(left, right, n.Op, ctx), nil

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
					if isFiniteNumericLitText(v) {
						return &Lit{Val: negateLitText(v), Text: negateLitText(v)}, nil
					}
					return nil, invalidNumericLiteralError(v)
				}
			}
		}
		return &UnaryOp{Operand: operand, Op: n.Op}, nil

	case *plansql.CmpExpr:
		// A ROW VALUE on either side is a row comparison, not two operands
		// (#710). Intercepted before the operands compile, because a
		// TupleNode has no scalar meaning to compile TO.
		if lt, lok := asTuple(unwrapCompileParens(n.Left)); lok {
			rt, rok := asTuple(unwrapCompileParens(n.Right))
			if !rok {
				return nil, sqlerr.New("42601",
					"row expression compared against a non-row value: %s", node.String())
			}
			op, ok := cmpOpFromSQL(n.Op)
			if !ok {
				return nil, sqlerr.New("0A000", "operator %q is not supported on row values", n.Op)
			}
			return compileRowCmp(lt, rt, op, ctx)
		}
		if _, rok := asTuple(unwrapCompileParens(n.Right)); rok {
			return nil, sqlerr.New("42601",
				"row expression compared against a non-row value: %s", node.String())
		}

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
		// `(a, b) IN ((1, 2), (3, 4))` is a disjunction of row equalities,
		// which is how PostgreSQL defines it (#710).
		if lt, ok := asTuple(unwrapCompileParens(n.Left)); ok {
			var arms []Expr
			for _, v := range n.Values {
				rt, ok := asTuple(unwrapCompileParens(v))
				if !ok {
					return nil, sqlerr.New("42601",
						"row expression compared against a non-row value: %s", v.String())
				}
				arm, err := compileRowCmp(lt, rt, CmpEq, ctx)
				if err != nil {
					return nil, err
				}
				arms = append(arms, arm)
			}
			if len(arms) == 0 {
				return nil, sqlerr.New("42601", "IN with no values: %s", node.String())
			}
			combined := arms[0]
			for _, a := range arms[1:] {
				combined = &Or{Left: combined, Right: a}
			}
			if n.Not {
				return &Not{Operand: combined}, nil
			}
			return combined, nil
		}

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
								SetBound:        ctx.setRowBound,
							}, nil
						}
					}
				}
				in := &InSubquery{Expr: left, SQL: sq.SQL, Runner: ctx.runner, Not: n.Not,
					Budget: ctx.budget, SetBound: ctx.setRowBound}
				if ctx.trackInSubquery != nil {
					ctx.trackInSubquery(in)
				}
				return in, nil
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
		// The destination NAMES a type, or the cast is 42704 — before the
		// operand is compiled, because a name that describes nothing cannot
		// describe this expression either. Without it the cast fell to
		// `default: return v` and the planner's inferCastType to `default:
		// return TypeString`, so `CAST(1 AS bogustype)` published a number
		// under a STRING declaration (#652, cast_dest.go).
		if !KnownCastDest(n.TypeName) {
			return nil, &UnknownTypeError{Name: n.TypeName}
		}
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
					cs := &CorrelatedScalarSubquery{
						Runner:          ctx.runner,
						OuterRefs:       refs,
						OuterTables:     ctx.outerTables,
						ParsedInfo:      info,
						UnqualOuterCols: buildUnqualOuterCols(refs, ctx.outerCols),
					}
					// The same declaration the uncorrelated form carries
					// (#696, #666). The resolver plans the subquery's SQL,
					// which for a correlated one still names its outer
					// references — a resolver that cannot plan it answers
					// not-known and the comparison keeps the boxed rules.
					if ctx.subqueryDecl != nil {
						if dt, dp, ds, ok := ctx.subqueryDecl(n.SQL); ok {
							cs.Decl, cs.DeclKnown = dt, true
							cs.DecPrecision, cs.DecScale = dp, ds
						}
					}
					return cs, nil
				}
			}
		}
		sq := &ScalarSubquery{SQL: n.SQL, Runner: ctx.runner}
		// The subquery's OUTPUT declaration, so the boxed comparison can read
		// this operand as the number it is rather than as the text it boxes
		// to (#696). Resolved once, at compile time, from the plan — never
		// from the value, which would be #727's defect pointed at a scalar.
		if ctx.subqueryDecl != nil {
			if t, p, s, ok := ctx.subqueryDecl(n.SQL); ok {
				sq.Decl, sq.DeclKnown = t, true
				sq.DecPrecision, sq.DecScale = p, s
			}
		}
		return sq, nil

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

	case *plansql.WindowFuncNode:
		// A window call must be extracted into a NodeWindow output column by
		// the logical builder and referenced here as a ColRef. Reaching the
		// compiler with a live WindowFuncNode means that extraction was
		// missed; the old default arm compiled it to a Lit of its own SQL
		// text, which silently produced a wrong answer (#610). Fail loudly.
		return nil, fmt.Errorf("window function %s reached the expression compiler unextracted", node.String())

	case *plansql.AnyAllExpr:
		// `x = ANY (…)` / `x <> ALL (…)`. Compiled to real comparisons with
		// PostgreSQL's three-valued fold; it used to fall through to the
		// default arm below and become a STRING constant, so the predicate
		// matched nothing at all (#710).
		return compileQuantified(n, ctx)

	case *plansql.TupleNode:
		// A row value is only meaningful as an operand of a comparison or of
		// IN, and both of those intercept it above. Reaching here means one
		// was written somewhere a row value has no meaning.
		return nil, sqlerr.New("42601", "row expression %s is not valid here", node.String())

	case *plansql.LiteralPlaceholder:
		// The coordinator substitutes the concrete literal into the
		// SERIALIZED expression before a fragment compiles it. One that
		// reaches the compiler means that substitution was missed, and the
		// old default arm turned it into the string ":name" — a silently
		// wrong predicate rather than a failure.
		return nil, fmt.Errorf("literal placeholder %s reached the expression compiler unsubstituted", node.String())

	default:
		// FAIL, do not guess. This arm returned `&Lit{Val: node.String()}` —
		// the node's own SQL text as a string constant — for every node type
		// the compiler had no case for. That is how #610 (a window function)
		// and #710 (ANY/ALL and row values) each produced a silently wrong
		// answer from one line. Every node type the parser can build is now
		// enumerated above; a new one fails here instead of compiling to
		// its own spelling.
		return nil, fmt.Errorf("expression %s (%T) reached the compiler with no rule for it", node.String(), node)
	}
}

// unwrapCompileParens strips redundant parentheses so a row value spelled
// `((a, b))` is still recognised as one.
func unwrapCompileParens(n plansql.Node) plansql.Node {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			return n
		}
		n = p.Inner
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
func compileBinOp(left, right Expr, op string, ctx *compileContext) Expr {
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
			((possiblyIntAtRuntime(left) && possiblyIntAtRuntime(right)) ||
				(possiblyDecimalAtRuntime(left, ctx) && possiblyDecimalAtRuntime(right, ctx))) {
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

// possiblyDecimalAtRuntime reports whether an operand might turn out to be an
// EXACT fixed-point value once column types resolve.
//
// It is possiblyIntAtRuntime's twin, and compileBinOp needs both for the same
// reason: the node is chosen at COMPILE time and the mode is resolved at
// runtime, so an operand pair that could be decimal has to reach BinOpNumeric
// to find out. Without it a FRACTIONAL LITERAL — which is a float64 box, so
// isIntNative rejects it — pinned the whole expression to BinOpFloat64 while
// the planner declared DECIMAL from the same literal's spelling. The float
// answer then met an exact vector: `d * 1.05` was 13.387500000000001 and
// failed the checked store with 22003, and `SELECT 0.1 + 0.2` did the same
// with no DECIMAL column in the query at all (#555 review, R2).
//
// A column qualifies because its type is unknown here; a numeric literal
// qualifies when its spelling names a value a DECIMAL can hold. Everything
// else is decided at compile time and does not.
func possiblyDecimalAtRuntime(e Expr, ctx *compileContext) bool {
	switch v := e.(type) {
	case *ColRef:
		// A column whose DECLARED type the caller handed over answers now:
		// only a DECIMAL (or an integer, which joins a decimal expression as
		// DECIMAL(19,0)) can make this pair exact. Without the declaration
		// the answer is yes, because the type does not exist until a batch
		// arrives and guessing no would put an exact expression on the float
		// path — the defect this whole question exists to avoid.
		if ctx != nil && ctx.colTypes != nil {
			t, known := ctx.colTypes[strings.ToLower(v.Name)]
			if known {
				switch t {
				case batch.TypeDecimal, batch.TypeInt32, batch.TypeInt64:
					return true
				}
				return false
			}
		}
		return true
	case *BinOpNumeric:
		return true
	case *Lit:
		if v.Text == "" {
			return false
		}
		_, ok := batch.DecimalTextType(v.Text)
		return ok
	case *UnaryOp:
		return (v.Op == "-" || v.Op == "+") && possiblyDecimalAtRuntime(v.Operand, ctx)
	}
	return false
}

// CompileWithColumnTypes compiles with the input's DECLARED column types in
// hand. See compileContext.colTypes: the types answer one compile-time
// question — whether an operand pair could be exact fixed-point — which
// without them has to be deferred to the first batch, at the cost of the
// vectorized float path for every pair that turns out not to be.
func CompileWithColumnTypes(node plansql.Node, runner SubqueryRunner, colTypes map[string]batch.TypeID, opts ...CompileOption) (Expr, error) {
	return compileWithCtx(node, applyCompileOptions(&compileContext{runner: runner, colTypes: colTypes}, opts))
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
	case *ColShapeLen:
		// The offsets-shape length node: an integer by construction, and the
		// node length()/octet_length()/bit_length() compile to over a bare
		// column (shape_funcs.go).
		return true
	case *numericFuncCall:
		// A function DECLARED integer makes the arithmetic over it integer
		// arithmetic — `length(s) / 2` is 2 in PostgreSQL, not 2.5 (#636).
		// The declaration is the compile-time shape compileBinOp was missing;
		// only a FIXED one answers, because a polymorphic declaration mirrors
		// an argument whose type no batch has resolved yet.
		return DefaultRegistry.ReturnType(v.Name).Integer()
	case *FuncCall:
		return DefaultRegistry.ReturnType(v.Name).Integer()
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
	return compileFuncCallNamed(n, ctx, true)
}

// compileFuncCallNamed is compileFuncCallNode with the "may a query spell
// this name" check made optional.
//
// checked=false is for a call this compiler MINTS rather than reads. Today
// that is only the `||` operator, whose kernels are registered under a name
// no query may spell precisely so that CONCAT cannot reach them (#609) — so
// the name check that exists to refuse `SELECT "||"(a, b)` must not also
// refuse the operator it implements.
func compileFuncCallNamed(n *plansql.FuncCallNode, ctx *compileContext, checked bool) (Expr, error) {
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

	if checked {
		if err := checkKnown(name); err != nil {
			return nil, err
		}
	}

	// element_at / the x[k] subscript route MAP key lookup vs. ARRAY index
	// from the compiled type of the first argument, which only a dedicated
	// node can see (a MAP and map_entries()'s ARRAY are the same runtime
	// shape — #607).
	if name == "element_at" && len(args) == 2 {
		return &elementAtExpr{arg0: args[0], arg1: args[1]}, nil
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
	// The scalar math functions that answer in their argument's OWN domain —
	// abs/ceil/floor/round/trunc/sign/mod — are exact over a DECIMAL, where
	// the FuncCall reads the column's rendered TEXT through ToFloat64 and
	// makes a round trip through a double before any rounding happens (#668).
	// The node carries fc as its fallback and takes it for every argument
	// that is not a DECIMAL, so semantics for the other types are unchanged.
	if ds := newDecimalScalarFn(fc); ds != nil {
		return ds, nil
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
	switch strings.ToLower(strings.TrimSpace(c.DestType)) {
	case "double", "double precision", "real", "float", "float4", "float8":
		return true
	default:
		return false
	}
}

// shapeLenMul maps the byte-length family to its multiplier. length / len /
// char_length / character_length are deliberately absent: all four count RUNES
// and must read the bytes (#856 moved the first two here — they were byte
// counts, which is what the defect was).
var shapeLenMul = map[string]int{
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
