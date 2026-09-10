// This file holds subquery resolution for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"fmt"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// ScalarSubqueriesAreDeferred reports whether the toggle above is on. Exported
// for the two-path gates, which must expect a lowered SELECT-list item to
// DECLINE when it is off: with no deferral there is no producer, and the
// lowering refuses to render a value it did not get from one.
func ScalarSubqueriesAreDeferred() bool { return ScalarDeferToggle.On() }

// deferredScalar carries a single CTE-referencing subquery whose resolution
// has been deferred until coordinator dispatch. The placeholder is a colon-
// prefixed identifier rendered into the serialized filter expression; the
// coordinator substitutes it after the producer stage completes.
type deferredScalar struct {
	Placeholder string // e.g. "scalar_1" (no leading colon)
	SubquerySQL string // the subquery to execute as a producer stage
}

// resolveFilterSubqueries finds embedded SQL subqueries in a filter expression
// string, executes them using the planner's standalone pipeline, and substitutes
// the scalar results as literals. This is needed for distributed mode where
// workers don't have catalog access to execute subqueries themselves.
//
// Subqueries whose FROM clause references a CTE are rewritten with
// :scalar_N placeholders instead of being pre-computed. The returned deferredScalar list describes the producer stages
// the caller must emit, and the filter-carrying stage's ScalarDependencies
// should point to those producer stage IDs. This eliminates the float-precision
// divergence between single-process cteCache evaluation and the distributed
// pipeline's accumulation order (root cause of Q15 SF0.1 0-row bug).
//
// Non-native-DAG mode keeps the legacy behavior: CTE-referencing subqueries
// are left unresolved (worker re-executes via SubqueryRunner), others are
// pre-computed and substituted in place.
func (p *Planner) resolveFilterSubqueries(exprStr string, decls colDecls) (string, []deferredScalar) {
	// Quick check: no subquery to resolve
	if !strings.Contains(strings.ToUpper(exprStr), "SELECT") {
		return exprStr, nil
	}

	ctx := p.planCtx
	if ctx == nil {
		return exprStr, nil
	}

	// Parse the expression to find SubqueryNode elements
	ast, err := plansql.ParseExpression(exprStr)
	if err != nil {
		return exprStr, nil
	}

	var deferred []deferredScalar
	resolved := p.resolveSubqueryAST(ctx, ast, &deferred, decls)
	if resolved != nil {
		return resolved.String(), deferred
	}
	return exprStr, deferred
}

// scalarSubqueryIsOneRow reports whether a subquery yields exactly one row for
// reasons the TEXT can prove, without executing it.
//
// One shape qualifies: a single select item that CONTAINS an aggregate, with
// no GROUP BY, no GROUPING SETS and no set operation. An ungrouped aggregate
// over any input — including an empty one — is exactly one row, which is why
// `SELECT MAX(x) FROM t` is a scalar subquery and `SELECT x FROM t` is a
// cardinality violation waiting to happen. The item may WRAP the aggregate
// (Q11's `SUM(…) * 0.0001`), which is why this asks for a nested aggregate
// rather than an aggregate call at the top.
//
// Anything it cannot prove is false, and a false answer costs a plan-time
// execution rather than a producer stage — the behaviour every scalar
// subquery had before the deferral existed.
func scalarSubqueryIsOneRow(sql string) bool {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return false
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return false
	}
	if info.Union != nil || len(info.GroupBy) > 0 || len(info.GroupingSets) > 0 {
		return false
	}
	if len(info.Columns) != 1 {
		return false
	}
	return plansql.FindNestedAggregate(info.Columns[0].ASTExpr) != nil
}

// allocScalarPlaceholder returns the next unused placeholder name (no leading
// colon) for this planner. Names are unique per Planner instance so that
// multiple deferred subqueries in the same query can coexist.
func (p *Planner) allocScalarPlaceholder() string {
	p.scalarPlaceholderSeq++
	return fmt.Sprintf("scalar_%d", p.scalarPlaceholderSeq)
}

// subqueryReferencesCTE returns true if the expression contains a scalar
// subquery whose FROM clause references a CTE defined in the current query.
func (p *Planner) subqueryReferencesCTE(exprStr string) bool {
	upper := strings.ToUpper(exprStr)
	for _, cte := range p.ctes {
		// Check if the CTE name appears after FROM in the subquery.
		// Use case-insensitive match since SQL is case-insensitive.
		if strings.Contains(upper, "FROM "+strings.ToUpper(cte.Name)) {
			return true
		}
	}
	return false
}

// resolveSubqueryAST recursively walks an AST node, replacing SubqueryNode
// elements with either literal values obtained by executing the subquery, or
// (when the subquery references a CTE under native-DAG) a LiteralPlaceholder
// whose concrete value will be substituted by the coordinator. Any deferred
// subqueries are appended to *deferred.
func (p *Planner) resolveSubqueryAST(ctx context.Context, node plansql.Node, deferred *[]deferredScalar, decls colDecls) plansql.Node {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *plansql.SubqueryNode:
		// A subquery that is not self-contained must NOT be deferred to a
		// producer stage or eagerly executed: standalone, its dangling outer
		// reference resolves to no column, evaluates NULL, and the query
		// silently answers 0 (#359). refuseCorrelatedSubqueries catches
		// correlation before stage generation with full scope; this is the
		// structural backstop for any expression that reaches deferral
		// without having passed through that pre-pass (e.g. inside a scalar
		// producer's own re-walk). Parked, not returned — walkStages has no
		// error path — and PlanDistributed turns it into the typed refusal
		// the coordinator routes on.
		if dangling := plansql.DanglingTableRefs(n.SQL); len(dangling) > 0 {
			p.refuseCorrelated(fmt.Errorf("%w: a scalar subquery references outer %s"+
				" and cannot execute as a standalone producer stage",
				ErrCorrelatedSubqueryDistributed, describeOuterRefs(dangling)))
			return node
		}
		// Defer scalar subqueries to producer stages so they share the
		// distributed accumulation path instead of running as a silent
		// single-process pipeline on the coordinator at plan time (Q11's
		// partsupp⨝supplier⨝nation subquery cost ~39s/query at SF100 this
		// way, Q22's customer avg ~10s). CTE-referencing subqueries MUST
		// defer regardless of the kill switch — eager evaluation over the
		// cteCache floats-drifts vs the outer query's distributed
		// aggregate (the Q15 SF0.1 0-row bug).
		// A subquery is DEFERRED to a producer stage only when it yields ONE
		// ROW BY CONSTRUCTION. Anything else has to be executed HERE, because
		// this is the only place in the distributed path that sees the
		// subquery's whole result and can therefore apply the one-row rule
		// (ADR-0021 §5): the coordinator's extractor reads the producer's
		// OUTPUT, and a producer's rows are neither one-per-row nor
		// one-file-per-task — a single-row producer can surface in more than
		// one file, so a count taken there is unsound in both directions.
		//
		// The perf lever the deferral exists for is untouched: Q11's and
		// Q22's subqueries are ungrouped aggregates, which is exactly the
		// shape that still defers.
		if scalarSubqueryIsOneRow(n.SQL) && (ScalarDeferToggle.On() || p.subqueryReferencesCTE(n.SQL)) {
			name := p.allocScalarPlaceholder()
			*deferred = append(*deferred, deferredScalar{Placeholder: name, SubquerySQL: n.SQL})
			return &plansql.LiteralPlaceholder{Name: name}
		}
		start := time.Now()
		rows, schema, err := p.executeSubquerySchema(ctx, n.SQL)
		slog.Info("plan-time scalar subquery executed on coordinator",
			"duration", time.Since(start).Round(time.Millisecond),
			"rows", len(rows), "error", err != nil)
		if err != nil {
			return node
		}
		// NO rows is not "no answer": a scalar subquery over an empty input
		// IS SQL NULL, and every comparison against it is UNKNOWN. Leaving
		// the subquery text in the filter instead — which is what this did —
		// ships a predicate the worker's compiler cannot read, and the task
		// fails. It was unreachable while every scalar subquery deferred to a
		// producer stage; restricting the deferral to the provably-one-row
		// shapes brought this path back.
		//
		// A SCALAR subquery is at most ONE row (ADR-0021 §5). Substituting
		// `rows[0]` of a multi-row result is a wrong answer wearing a
		// plausible one, and which row it picks is whichever the producer
		// emitted first — so the same query answers differently on different
		// paths. PostgreSQL raises 21000 here and so does this; the refusal is
		// PARKED because walkStages has no error return.
		v, cardErr := expr.ScalarSubqueryValue(n.SQL, rows)
		if cardErr != nil {
			p.refuseScalarRows(cardErr)
			return node
		}
		if v == nil {
			return &plansql.Lit{Kind: plansql.LitNull}
		}
		typ, typed := scalarColType(schema)
		return scalarToLiteral(v, typ, typed)

	case *plansql.ExistsNode:
		// `EXISTS (SELECT …)` that reached here did NOT decorrelate into a
		// semi/anti join, and the worker has no SubqueryRunner either — the
		// filter shipped verbatim and every task failed with "EXISTS subquery
		// requires a SubqueryRunner". That is #524's family with the EXISTS
		// arm never written: the sibling cases have handled a scalar subquery
		// (executed here) and an IN-subquery (materialized as a SET) since,
		// and `default:` shipped this one.
		//
		// An UNCORRELATED `EXISTS` is a query-wide CONSTANT — it reads no
		// outer row, so it is TRUE or FALSE for every row of every task — so
		// it is evaluated once here and the predicate becomes that boolean.
		// A subquery that is not self-contained is not evaluated: standalone,
		// its dangling reference resolves to no column and the constant would
		// be confidently wrong, so it takes the refusal the SubqueryNode arm
		// above takes and the coordinator answers on its local pipeline
		// (ADR-0021 §1c).
		if dangling := plansql.DanglingTableRefs(n.SQL); len(dangling) > 0 {
			p.refuseCorrelated(fmt.Errorf("%w: an EXISTS subquery references outer %s"+
				" and cannot be evaluated as a query-wide constant",
				ErrCorrelatedSubqueryDistributed, describeOuterRefs(dangling)))
			return node
		}
		rows, _, err := p.executeSubquerySchema(ctx, n.SQL)
		if err != nil {
			// An AUTHORIZATION refusal is the decision's own sentence, not a
			// planning narrative, and it is the query's answer on every path
			// (ADR-0034 item 6). Swallowing it shipped the filter and the
			// task failed with "EXISTS subquery requires a SubqueryRunner"
			// where the scalar and IN siblings say `permission denied for
			// table "…"` (round-1 review P1).
			if sqlerr.StateOf(err) == "42501" {
				p.refusePlanTimeAnswer(err)
			}
			return node
		}
		exists := len(rows) > 0
		if n.Not {
			exists = !exists
		}
		if exists {
			return &plansql.Lit{Value: "true", Kind: plansql.LitBool}
		}
		return &plansql.Lit{Value: "false", Kind: plansql.LitBool}

	case *plansql.InExpr:
		// `x IN (SELECT …)` that reached here did NOT decorrelate into a
		// semi/anti join, and the worker has no SubqueryRunner to execute it
		// with — the filter used to ship verbatim and fail (#524). An
		// uncorrelated IN-subquery is a SET, so it is materialized here and
		// the predicate becomes the literal list the expression layer already
		// evaluates. See in_subquery_set.go for the two bounds and the
		// refusal that routes past them.
		if subq := findInSubqueryValue(n); subq != nil {
			if rewritten, ok := p.materializeInSubquery(ctx, n, subq, decls); ok {
				return rewritten
			}
			return node
		}
		vals := make([]plansql.Node, len(n.Values))
		for i, v := range n.Values {
			vals[i] = p.resolveSubqueryAST(ctx, v, deferred, decls)
		}
		return &plansql.InExpr{
			Left:   p.resolveSubqueryAST(ctx, n.Left, deferred, decls),
			Not:    n.Not,
			Values: vals,
		}

	case *plansql.CmpExpr:
		return &plansql.CmpExpr{
			Left:  p.resolveSubqueryAST(ctx, n.Left, deferred, decls),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right, deferred, decls),
		}

	case *plansql.BinaryOp:
		return &plansql.BinaryOp{
			Left:  p.resolveSubqueryAST(ctx, n.Left, deferred, decls),
			Op:    n.Op,
			Right: p.resolveSubqueryAST(ctx, n.Right, deferred, decls),
		}

	case *plansql.UnaryOp:
		return &plansql.UnaryOp{
			Op:    n.Op,
			Inner: p.resolveSubqueryAST(ctx, n.Inner, deferred, decls),
		}

	case *plansql.ParenNode:
		inner := p.resolveSubqueryAST(ctx, n.Inner, deferred, decls)
		if inner != nil {
			return &plansql.ParenNode{Inner: inner}
		}
		return node

	// The BOOLEAN TREE. A subquery is a leaf of a predicate, not a predicate,
	// and the arms above only reach the positions a comparison or an
	// arithmetic operator puts it in. `AND` looked handled because the filter
	// is split into conjuncts BEFORE this walk; `OR` and `NOT` cannot be
	// split, so `… WHERE d.id < 2 OR EXISTS (…)` shipped verbatim and every
	// task failed with "EXISTS subquery requires a SubqueryRunner"
	// (round-1 review B2). Walking the tree is what makes the rule ADR-0021
	// §2b states — an uncorrelated EXISTS is a query-wide constant — true of
	// the predicate rather than of one shape of predicate.
	//
	// Only the EXISTS leaves. See resolveBooleanExists: a boolean connective
	// SHORT-CIRCUITS, and hoisting a scalar out of an arm the query may never
	// evaluate makes that arm's failure the query's answer.
	case *plansql.AndNode:
		return &plansql.AndNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}

	case *plansql.OrNode:
		return &plansql.OrNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}

	case *plansql.NotNode:
		return &plansql.NotNode{Inner: p.resolveBooleanExists(ctx, n.Inner, deferred, decls)}

	case *plansql.CaseNode:
		out := &plansql.CaseNode{}
		if n.Subject != nil {
			out.Subject = p.resolveBooleanExists(ctx, n.Subject, deferred, decls)
		}
		for _, w := range n.Whens {
			out.Whens = append(out.Whens, plansql.WhenClause{
				Cond:   p.resolveBooleanExists(ctx, w.Cond, deferred, decls),
				Result: p.resolveBooleanExists(ctx, w.Result, deferred, decls),
			})
		}
		if n.Else != nil {
			out.Else = p.resolveBooleanExists(ctx, n.Else, deferred, decls)
		}
		return out

	default:
		return node
	}
}

// resolveBooleanExists resolves the EXISTS leaves under a boolean connective
// and leaves every other leaf exactly as it found it.
//
// A boolean connective SHORT-CIRCUITS, and hoisting is unconditional
// evaluation: it turns a subquery the query may never reach into one the query
// always runs, so any way that subquery can FAIL becomes the query's answer.
// PostgreSQL 17 measured, and the single-process path agrees with it because
// it evaluates per row and lazily:
//
//	… WHERE d.id < 100 OR d.id > (SELECT id FROM t WHERE id < 5)   -- 9 rows
//	… WHERE d.id < 0   OR d.id > (SELECT id FROM t WHERE id < 5)   -- 21000
//
// The subquery returns five rows either way. The first answers because the
// left arm is true for every row and the right one is never needed; the second
// raises because it IS needed. Hoisting made the first 21000 as well
// (round-1 review P2) — a query PostgreSQL answers, refused.
//
// An EXISTS is the leaf where hoisting is sound: it reads no outer row, it is
// TRUE or FALSE rather than a value, and it cannot raise the cardinality
// violation that is the failure at issue. A SCALAR subquery in a
// short-circuitable position keeps whatever the path did before — which on the
// DAG is a loud task failure, pinned per arm beside PostgreSQL's answer in
// coordinator.TestArcI1AnUnqualifiedNameBindsTheInnerRelation, because
// answering it needs the DAG to evaluate a subquery lazily and that is not a
// scope repair.
func (p *Planner) resolveBooleanExists(ctx context.Context, node plansql.Node,
	deferred *[]deferredScalar, decls colDecls) plansql.Node {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *plansql.ExistsNode:
		return p.resolveSubqueryAST(ctx, n, deferred, decls)
	case *plansql.AndNode:
		return &plansql.AndNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}
	case *plansql.OrNode:
		return &plansql.OrNode{
			Left:  p.resolveBooleanExists(ctx, n.Left, deferred, decls),
			Right: p.resolveBooleanExists(ctx, n.Right, deferred, decls),
		}
	case *plansql.NotNode:
		return &plansql.NotNode{Inner: p.resolveBooleanExists(ctx, n.Inner, deferred, decls)}
	case *plansql.ParenNode:
		if inner := p.resolveBooleanExists(ctx, n.Inner, deferred, decls); inner != nil {
			return &plansql.ParenNode{Inner: inner}
		}
		return node
	case *plansql.CaseNode:
		out := &plansql.CaseNode{}
		if n.Subject != nil {
			out.Subject = p.resolveBooleanExists(ctx, n.Subject, deferred, decls)
		}
		for _, w := range n.Whens {
			out.Whens = append(out.Whens, plansql.WhenClause{
				Cond:   p.resolveBooleanExists(ctx, w.Cond, deferred, decls),
				Result: p.resolveBooleanExists(ctx, w.Result, deferred, decls),
			})
		}
		if n.Else != nil {
			out.Else = p.resolveBooleanExists(ctx, n.Else, deferred, decls)
		}
		return out
	default:
		return node
	}
}

// emitScalarProducerStages parses subquerySQL, walks its logical plan, and
// appends the resulting distributed stages to *stages. Returns the terminal
// stage's ID (the one whose single-row, single-column output holds the
// scalar). The coordinator awaits this producer at dispatch time, extracts
// the value, and substitutes it into the filter-carrying stage's expression.
//
// CTE definitions from the enclosing query are merged so the subquery can
// resolve :CTE references. The terminal stage is forced to Tasks=1 so its
// output is a single unpartitioned WSHF file suitable for scalar extraction.
func (p *Planner) emitScalarProducerStages(stages *[]Stage, subquerySQL string) (string, error) {
	id, _, _, err := p.emitScalarProducerStagesTyped(stages, subquerySQL)
	return id, err
}

// emitScalarProducerStagesTyped is emitScalarProducerStages with the TYPE the
// producer's own plan says its single output column emits, and whether that
// plan named one at all.
//
// The predicate path has no use for it — a substituted literal is compared
// against a column whose declaration decides the kernel. The SELECT-list path
// uses it to DECIDE, not to declare: a value whose literal spelling does not
// read back at the same type is not lowered at all (see
// scalarProducerValueIsLiteralSafe).
func (p *Planner) emitScalarProducerStagesTyped(stages *[]Stage, subquerySQL string) (string, parquet.TypeID, bool, error) {
	pq, err := plansql.Parse(subquerySQL)
	if err != nil {
		return "", 0, false, fmt.Errorf("parse subquery: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return "", 0, false, fmt.Errorf("extract subquery: %w", err)
	}
	var logicalPlan *logical.Node
	if len(p.ctes) > 0 {
		merged := append([]plansql.CTEDef(nil), p.ctes...)
		merged = append(merged, info.CTEs...)
		logicalPlan, err = logical.BuildFromSelectWithCTEs(info, merged)
	} else {
		logicalPlan, err = logical.BuildFromSelect(info)
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("build subquery plan: %w", err)
	}
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	p.AnnotateScanColumns(ctx, logicalPlan)

	// The DAG's counterpart of buildSubqueryPipeline: this is a whole second
	// query, planned here, and it must carry the same column policy as its
	// enclosing statement (#859) and ask the same ACCESS decision for every
	// relation it reads (#945). The barrier is absorbed into the scan stage by
	// walkStages below, exactly as it is for the outer plan; the refusal
	// happens here, before a stage is emitted, so a denied relation never
	// becomes a task.
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		if denied := pol.DeniedColumns(); len(denied) > 0 {
			// nil table hook: applyContextColumnPolicies below asks the
			// ACCESS decision for every relation this plan reads (#945), so
			// the binder's own refusal would be a second copy of it.
			if err := ValidateColumnsUnderPolicy(ctx, p.catalog, info, func(table string) map[string]bool {
				return denied[strings.ToLower(table)]
			}, nil); err != nil {
				return "", 0, false, err
			}
		}
		logicalPlan, err = p.applyContextColumnPolicies(ctx, logicalPlan)
		if err != nil {
			return "", 0, false, err
		}
	}

	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		p.AnnotateScanColumns(ctx, plan)
	})
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		logicalPlan, err = p.applyContextColumnPoliciesToNewScans(ctx, logicalPlan)
		if err != nil {
			return "", 0, false, err
		}
		if err := p.checkPolicyPlanOrderFromContext(ctx, logicalPlan); err != nil {
			return "", 0, false, err
		}
	}

	before := len(*stages)
	p.walkStages(logicalPlan, stages, nil)
	if len(*stages) == before {
		return "", 0, false, fmt.Errorf("subquery emitted no stages")
	}
	terminal := &(*stages)[len(*stages)-1]
	// Force Singleton: a scalar producer emits exactly one row. Tasks>1
	// here would fan out into partitioned WSHF output that the coordinator's
	// scalar extractor can't read.
	terminal.Tasks = 1
	// Pin the subquery's projection on the terminal so the scalar extractor
	// can apply post-aggregate wrappers like Q11's "SUM(...) * 0.0001". The
	// producer chain only emits the raw aggregate (e.g. __agg_0 = SUM); the
	// SELECT-level multiplier needs to be applied by the coordinator after
	// reading the producer output. Reuses Stage.OutputRenames the same way
	// Gather does — at extract time we'll detect the producer-vs-Gather
	// case via context.
	if renames := extractOutputRenames(logicalPlan); len(renames) > 0 {
		terminal.OutputRenames = renames
	}
	// The producer's own plan is the authority on the value's TYPE. One
	// output column is the whole shape a scalar producer has (the deferral
	// fires only for a single provably-one-row select item); a plan that
	// emits anything else names nothing, and the caller declines.
	var valueType parquet.TypeID
	typeKnown := false
	if emitted := emittedColTypes(logicalPlan); len(emitted) == 1 {
		for _, id := range emitted {
			valueType, typeKnown = id, true
		}
	}
	return terminal.ID, valueType, typeKnown, nil
}

// scalarToLiteral converts a Go value to an AST literal node.
//
// typ is the value's DECLARED type, for the one box that cannot say what it
// is: a DECIMAL arrives as its RENDERED TEXT, indistinguishable from a STRING
// column's value. Spelling it as a QUOTED literal makes it look like something
// a user wrote, and an unknown-typed literal is coerced with the OTHER
// operand's input function (ADR-0012 item 13) — so `HAVING COUNT(*) > (SELECT
// COUNT(*) * 0.3 …)` substituted `'0.0'` and asked bigint's input function to
// read it, which is 22P02 for a query PostgreSQL answers as numeric. A DECIMAL
// is spelled as the NUMBER it is, carrying its exact digits (item 6's carrier
// rule); every other string-boxed type keeps the quoted spelling.
func scalarToLiteral(v any, typ parquet.TypeID, typed bool) plansql.Node {
	if s, ok := v.(string); ok && typed && typ == parquet.TypeDecimal {
		return &plansql.Lit{Value: s, Kind: plansql.LitNumber}
	}
	switch val := v.(type) {
	case float64:
		return &plansql.Lit{Value: strconv.FormatFloat(val, 'f', -1, 64), Kind: plansql.LitNumber}
	case float32:
		return &plansql.Lit{Value: strconv.FormatFloat(float64(val), 'f', -1, 32), Kind: plansql.LitNumber}
	case int64:
		return &plansql.Lit{Value: fmt.Sprintf("%d", val), Kind: plansql.LitNumber}
	case int:
		return &plansql.Lit{Value: fmt.Sprintf("%d", val), Kind: plansql.LitNumber}
	case string:
		return &plansql.Lit{Value: val, Kind: plansql.LitString}
	default:
		return &plansql.Lit{Value: fmt.Sprint(v), Kind: plansql.LitNumber}
	}
}

// scalarColType is a single-column result schema's declared type, and
// typed=false when the schema does not name exactly one column (nothing to
// disambiguate a box with).
func scalarColType(schema []parquet.Column) (parquet.TypeID, bool) {
	if len(schema) != 1 {
		return 0, false
	}
	return schema[0].Type, true
}
