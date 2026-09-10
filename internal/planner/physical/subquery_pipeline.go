// This file holds subquery pipeline for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// makeSubqueryRunner creates a SubqueryRunner that executes SQL via this planner.
// Uses the planCtx stored during Plan() so subqueries respect the parent context's
// cancellation and timeout.
//
// Each call gets its own child planner. This is the runner baked into every
// compiled expression, so a correlated subquery reaches it once per row from
// every parallel pipeline goroutine — see forSubquery for why sharing the
// parent's build scratch across those goroutines is not an option.
func (p *Planner) makeSubqueryRunner() expr.SubqueryRunner {
	return func(sql string) ([]map[string]any, error) {
		ctx := p.planCtx
		if ctx == nil {
			ctx = context.Background()
		}
		return p.forSubquery().executeSubquery(ctx, sql)
	}
}

// forSubquery returns a child planner for building and running ONE subquery
// pipeline.
//
// A correlated subquery is executed per row from the pipeline's parallel
// worker goroutines (exec.Pipeline.runParallel), and each execution runs a
// full physical build. Those builds must not share the parent's per-build
// scratch, for two independent reasons:
//
//   - It is a data race. scanCounter is a plain map written by buildScan, so
//     concurrent builds crash the process with "fatal error: concurrent map
//     writes" — an unrecoverable throw that takes down every other connection
//     in server mode (issue #334).
//   - It is wrong even when serialized. scanCounter numbers the scans of one
//     build; letting the outer build's count leak in makes a subquery's first
//     scan of customer resolve as alias "customer:1", so the ScanFileFilter /
//     MaterializedInputs / StreamingSources lookups keyed by that alias miss.
//     A mutex would hide the crash and keep the mis-keying.
//
// So the child gets fresh build scratch, and shares everything that genuinely
// belongs to the query: the catalog, the CTE definitions and their
// materialized cache, the scan cache, the memory/spill resources, and all
// configuration. Sharing those is what keeps one budget, one spill directory,
// and one materialization of each CTE per query; a per-goroutine copy would
// leak spill directories that Plan's Cleanup never sees.
//
// The shared maps (cteCache, scanCache) are populated by Plan before execution
// begins and are read-only from here on; scanCached carries its own mutex for
// the concurrent-replay case.
//
// The scan-alias injections (MaterializedInputs, StreamingSources,
// ScanFileFilter) are dropped. They describe the ENCLOSING fragment's scans —
// a worker's probe-split file slice, a scan-split pre-scan — keyed by that
// plan's aliases. A subquery is its own query over the catalog and must see
// the whole table; binding it to the fragment's file slice would answer it
// from one worker's shard. Today they are missed only because the shared
// counter happens to push the subquery's aliases past the injected keys, so
// dropping them makes the existing behavior explicit rather than incidental.
func (p *Planner) forSubquery() *Planner {
	sub := *p
	sub.scanCounter = nil
	sub.ctePlannedTerminal = nil
	sub.scanDeletes = nil
	sub.scalarPlaceholderSeq = 0
	sub.MaterializedInputs = nil
	sub.StreamingSources = nil
	sub.ScanFileFilter = nil
	sub.res = p.resources()
	// Nested subqueries inside this one recurse through the same rule.
	sub.subqueryRunner = sub.makeSubqueryRunner()
	return &sub
}

// buildSubqueryPipeline parses, plans, and builds (but does not run) the
// physical pipeline for a SQL subquery, merging the enclosing WITH clause's
// CTEs so the subquery can reference them. Shared by executeSubquery (boxed
// results) and materializeCTEColumnar (columnar collection).
// subqueryDeclOption is the expr.CompileOption that lets a compiled scalar
// subquery carry its OUTPUT DECLARATION, so a comparison against it is made at
// the type the subquery answers rather than by the bytes of the box (#696).
//
// It plans the subquery's SQL — parse, logical build, annotate — and reads
// declaredOutputSchema, the same walk the top-level statement's own output
// schema comes from. No execution: the question is the TYPE, and the value is
// resolved once at evaluation as it always was. A subquery that does not
// resolve to exactly one column answers ok=false and the comparison keeps the
// boxed rules it had.
//
// The cost is one logical build per compiled scalar subquery, at plan time.
func (p *Planner) subqueryDeclOption() expr.CompileOption {
	env := expr.WithSubqueryEnv(func(sql string) (parquet.TypeID, int, int, bool) {
		cols, ok := p.subqueryOutputColumn(sql)
		if !ok {
			return 0, 0, 0, false
		}
		return cols.Type, cols.Precision, cols.Scale, true
	}, p.subqueryOutputArity)
	// …and the RELATION resolver the dangling-reference guard needs to tell a
	// ROW FIELD PATH from a lost correlation (#866). It travels with the
	// other two plan-time answers because it is the same question asked of
	// the same plan, and a compile site that took only the first two would
	// refuse `d.b IN (SELECT c_row.b FROM t)` — a query PostgreSQL answers.
	return expr.Options(env, expr.WithSubqueryScope(p.subqueryInnerColumns()))
}

// subqueryOutputArity is how many columns a subquery's SELECT list has, from
// the subquery's OWN PLAN.
//
// It is what lets a construct requiring ONE column refuse before the subquery
// runs, which is PostgreSQL's order — `subquery must return only one column`
// is a parse-analysis error there, so it fires over an EMPTY subquery too
// (round-1 P1). Counting rows cannot reach that case and counting the row map
// cannot see two columns that share a name (round-1 B1); the declared schema
// is positional and knows both.
//
// It recovers from a panic and answers not-known for anything it cannot plan,
// exactly as subqueryOutputColumn does and for the same reason: an unplannable
// subquery must cost the refusal its evidence, never the query its answer.
func (p *Planner) subqueryOutputArity(sql string) (n int, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			n, ok = 0, false
		}
	}()
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	pq, err := plansql.Parse(sql)
	if err != nil {
		return 0, false
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return 0, false
	}
	var plan *logical.Node
	if len(p.ctes) > 0 {
		plan, err = logical.BuildFromSelectWithCTEs(info,
			append(append([]plansql.CTEDef(nil), p.ctes...), info.CTEs...))
	} else {
		plan, err = logical.BuildFromSelect(info)
	}
	if err != nil || plan == nil {
		return 0, false
	}
	p.AnnotateScanColumns(ctx, plan)
	schema := declaredOutputSchema(plan, p.subqueryOutputColumn)
	if len(schema) == 0 {
		// A shape this walk cannot name — a star it could not expand, a
		// projection it cannot read. Not-known, and the row-count backstop
		// still applies.
		return 0, false
	}
	return len(schema), true
}

// DeclaredOutputSchema is the PLAN-TIME declaration of a statement's output
// columns — the same walk `Plan` stamps on a single-process pipeline as
// `Plan.OutputSchema` — for a door that assembles a result set from batches it
// may not have.
//
// The ASYNC door is that door (#1008 round 2): `SubmitSQL` plans stages and
// `GetQueryResults` reads the columns off the gathered batches, of which a
// ZERO-ROW query has none, and the DAG's own `GatherOutputSchema` describes a
// gather stage that a one-stage plan does not have. Every zero-row SELECT
// therefore came back with no columns at all on that door while the other
// three described it from exactly this walk. Calling it rather than copying it
// is what keeps the four doors' answers the same list.
//
// The caller has already annotated the plan; this does not re-annotate,
// because a second AnnotateScanColumns over an optimized plan is not free and
// the walk needs only what the first one left.
func (p *Planner) DeclaredOutputSchema(plan *logical.Node) []parquet.Column {
	if plan == nil {
		return nil
	}
	return declaredOutputSchema(plan, p.subqueryOutputColumn)
}

// subqueryOutputColumn resolves a scalar subquery's single declared output
// column. It recovers from a panic for the reason every plan-time helper on
// this path does: an unplannable subquery must cost the comparison its
// declaration, never the query.
func (p *Planner) subqueryOutputColumn(sql string) (col parquet.Column, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			col, ok = parquet.Column{}, false
		}
	}()
	ctx := p.planCtx
	if ctx == nil {
		ctx = context.Background()
	}
	pq, err := plansql.Parse(sql)
	if err != nil {
		return parquet.Column{}, false
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return parquet.Column{}, false
	}
	var plan *logical.Node
	if len(p.ctes) > 0 {
		plan, err = logical.BuildFromSelectWithCTEs(info, append(append([]plansql.CTEDef(nil), p.ctes...), info.CTEs...))
	} else {
		plan, err = logical.BuildFromSelect(info)
	}
	if err != nil || plan == nil {
		return parquet.Column{}, false
	}
	p.AnnotateScanColumns(ctx, plan)
	schema := declaredOutputSchema(plan, p.subqueryOutputColumn)
	if len(schema) != 1 {
		// Not a scalar subquery's shape. Declining is the honest answer: a
		// wrong declaration here would pick a comparison RULE, which is worse
		// than picking none (ADR-0012 item 8).
		return parquet.Column{}, false
	}
	return schema[0], true
}

// buildSubqueryPipelineScoped is buildSubqueryPipeline with the enclosing WITH
// list given explicitly. A CTE's own BODY is built with only the CTEs defined
// BEFORE it, because a non-recursive CTE's name is not in scope inside its own
// body — PostgreSQL's rule (#771). Passing the whole list made a CTE that
// SHADOWS a base table materialize a body that read ITSELF, and the query
// answered NULL for every column the CTE computes.
func (p *Planner) buildSubqueryPipelineScoped(ctx context.Context, sql string,
	ctes []plansql.CTEDef) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	saved := p.ctes
	p.ctes = ctes
	defer func() { p.ctes = saved }()
	return p.buildSubqueryPipeline(ctx, sql)
}

// buildSubqueryPipelineScopedFor is buildSubqueryPipelineScoped over an
// already-parsed block — see buildSubqueryPipelineFor.
func (p *Planner) buildSubqueryPipelineScopedFor(ctx context.Context, info *plansql.SelectInfo,
	ctes []plansql.CTEDef) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	saved := p.ctes
	p.ctes = ctes
	defer func() { p.ctes = saved }()
	return p.buildSubqueryPipelineFor(ctx, info)
}

func (p *Planner) buildSubqueryPipeline(ctx context.Context, sql string) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	// Parse using our SQL parser
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery parse error: %w", err)
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery extract error: %w", err)
	}
	return p.buildSubqueryPipelineFor(ctx, info)
}

// buildSubqueryPipelineFor is buildSubqueryPipeline over an ALREADY-PARSED
// block. A CTE body is planned from the tree the binder validated, so a
// decision recorded there — PostgreSQL's GROUP BY precedence, which needs a
// schema the parser does not have — reaches this path too (#851).
func (p *Planner) buildSubqueryPipelineFor(ctx context.Context, info *plansql.SelectInfo) (exec.Source, []exec.UnaryOperator, exec.Sink, error) {
	var err error

	// Build logical plan — merge outer CTEs so subqueries can reference
	// CTE tables defined in the enclosing WITH clause.
	var logicalPlan *logical.Node
	if len(p.ctes) > 0 {
		merged := append(p.ctes, info.CTEs...)
		logicalPlan, err = logical.BuildFromSelectWithCTEs(info, merged)
	} else {
		logicalPlan, err = logical.BuildFromSelect(info)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("subquery plan error: %w", err)
	}

	// Annotate scan nodes with column metadata so the optimizer can resolve
	// unqualified column references (needed for subquery decorrelation).
	p.AnnotateScanColumns(ctx, logicalPlan)

	// A subquery is a WHOLE SECOND QUERY, planned here and never through
	// auth.EnforcePlanPolicies — so before #859 `(SELECT MAX(ssn) FROM t)`
	// read the raw column on every door while the same column masked in the
	// enclosing SELECT list. The policies travel on the context; the
	// projection goes in before the optimizer, exactly as it does for the
	// statement's own plan.
	//
	// And the ACCESS decision with them (#945): every relation THIS plan
	// reads asks the context lookup inside applyContextColumnPolicies, which
	// refuses before a pipeline is built. The lookup alone is enough to enter
	// — a relation named only inside a subquery is in no resolved set.
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		// The name-binding pass runs only when something is DENIED. A
		// correlated subquery rebuilds this pipeline once per outer row, and
		// a mask-only policy — the common case — has nothing for the binder
		// to refuse.
		if denied := pol.DeniedColumns(); len(denied) > 0 {
			// nil table hook: applyContextColumnPolicies below asks the
			// ACCESS decision for every relation this plan reads (#945), so
			// the binder's own refusal would be a second copy of it.
			if err := ValidateColumnsUnderPolicy(ctx, p.catalog, info, func(table string) map[string]bool {
				return denied[strings.ToLower(table)]
			}, nil); err != nil {
				return nil, nil, nil, err
			}
		}
		logicalPlan, err = p.applyContextColumnPolicies(ctx, logicalPlan)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Optimize — pass scan annotator so new scans created by IN-to-SemiJoin
	// conversion get column metadata for scalar subquery decorrelation.
	logicalPlan = logical.Optimize(logicalPlan, func(plan *logical.Node) {
		p.AnnotateScanColumns(ctx, plan)
	})
	// The optimizer MINTS scans, after the policy went in above (#859).
	if pol := logical.ColumnPoliciesFromContext(ctx); len(pol) > 0 || logical.PolicyLookupFromContext(ctx) != nil {
		logicalPlan, err = p.applyContextColumnPoliciesToNewScans(ctx, logicalPlan)
		if err != nil {
			return nil, nil, nil, err
		}
		// The invariant over THIS plan too. A subquery's predicates are
		// pushed here, and one that ends up between a security projection and
		// its scan reads the stored column exactly as it would in the outer
		// plan (#859 round 4).
		if err := p.checkPolicyPlanOrderFromContext(ctx, logicalPlan); err != nil {
			return nil, nil, nil, err
		}
	}

	// Build physical pipeline
	source, ops, sink, err := p.buildPipeline(ctx, logicalPlan)
	if err != nil {
		// An authorization refusal raised while the pipeline is BUILT — the
		// table-function guard `buildScan` asks (#943) is the one that gets
		// here — is the decision's own sentence, not a planning narrative
		// about the subquery (ADR-0034 item 6; round-1 P1).
		if sqlerr.StateOf(err) == "42501" {
			return nil, nil, nil, err
		}
		return nil, nil, nil, fmt.Errorf("subquery execution plan error: %w", err)
	}

	// A SUBQUERY'S RESULT IS ITS SELECT LIST, and nothing else (#875).
	//
	// The logical builder materializes an ORDER BY term the SELECT list does
	// not carry as a HIDDEN column on the projection, and `Plan` drops it
	// again before the rows reach the client (#320, the call beside
	// buildPipeline there). This path had no such trim, so a subquery's rows
	// arrived carrying `__sortkey_N` beside the one column the query asked
	// for — and every consumer that reduces a subquery's row to ONE value
	// picks that value out of a Go MAP (expr.ScalarSubqueryValue,
	// InSubquery.resolveSlow's "first column only", CorrelatedInSubquery's).
	// Map iteration order is randomized per range statement, so `SELECT c_ts
	// FROM typemx ORDER BY id LIMIT 1` answered `id` about one run in five,
	// on ALL FOUR ARMS and in silence.
	//
	// The trim is the same operator the top-level statement gets, from the
	// same plan, so the two paths cannot disagree about which columns a
	// SELECT list has.
	if trim := hiddenSortTrimOp(logicalPlan); trim != nil {
		ops = append(ops, trim)
	}
	return source, ops, sink, nil
}

// executeSubquery parses and executes a SQL subquery, returning result rows.
func (p *Planner) executeSubquery(ctx context.Context, sql string) ([]map[string]any, error) {
	rows, _, err := p.executeSubquerySchema(ctx, sql)
	return rows, err
}

// executeSubquerySchema is executeSubquery with the result's DECLARED SCHEMA
// alongside the rows.
//
// A boxed row value cannot always say what it is — a DECIMAL and a STRING both
// arrive as a Go string — so a caller that has to re-spell those values (the
// IN-set materializer, which inlines them into filter TEXT) needs the
// declaration to tell them apart. That is ADR-0012 item 8's rule applied to
// the one place the boxing happens on the PLANNER's side of the wire.
func (p *Planner) executeSubquerySchema(ctx context.Context, sql string) ([]map[string]any, []parquet.Column, error) {
	source, ops, sink, err := p.buildSubqueryPipeline(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	// Ensure a CollectSink — buildPipeline may return nil sink for
	// non-blocking plans (e.g., CTE cache lookups, table-less SELECTs).
	collectSink, ok := sink.(*exec.CollectSink)
	if !ok {
		collectSink = &exec.CollectSink{}
		sink = collectSink
	}

	// Execute
	pipeline := &exec.Pipeline{Source: source, Ops: ops, Sink: sink}
	if err := pipeline.Run(ctx); err != nil {
		return nil, nil, fmt.Errorf("subquery execution error: %w", err)
	}

	// Schema BEFORE ToRows: the sink keeps its captured schema across that
	// call, but reading it first keeps the order a local fact.
	schema := collectSink.Schema()
	return subqueryRowsPerColumn(schema, collectSink), schema, nil
}

// subqueryRowsPerColumn is the sink's rows with ONE MAP ENTRY PER OUTPUT
// COLUMN, whatever the columns are called.
//
// PostgreSQL lets two output columns share a name — `SELECT ABS(a), ABS(b)`
// is two columns both called `abs`, and so is `SELECT x AS v, y AS v` — and
// `exec.CollectSink.convert` boxes a row into a map KEYED BY NAME, so the
// second column overwrites the first and the map holds ONE entry for TWO
// columns. Its own `ToRowValues` comment records that as lossy.
//
// Every consumer that reduces a subquery's row asks the map how many columns
// there are: `expr.ScalarSubqueryValue`, `InSubquery.resolveSlow`,
// `CorrelatedInSubquery.EvalBoolNull` and `materializeInSubquery` all count
// `len(row)`. With the map collapsed, a two-column subquery counted as ONE and
// walked straight through the 42601 refusal PostgreSQL raises for it —
// `SELECT (SELECT ABS(a), ABS(b) FROM decpair WHERE id = 1)` answered
// `12.7500` on every arm and every door, and its IN twin answered a row count.
//
// The count belongs to the SCHEMA, which is positional and cannot collapse,
// so the disambiguation happens HERE, once, at the seam every one of those
// consumers is fed from — rather than in four reducers that would each need a
// schema they are not given. `CollectSink.ToRowValues` already materializes
// the positional form for exactly this case and returns nil when the names are
// unique, which is the ordinary shape and costs nothing.
//
// The suffix is `:N`, the column's position: a colon cannot appear in an
// identifier the binder resolves, so a disambiguated key can collide with
// nothing.
//
// Every consumer of THESE rows iterates rather than reading a value by name,
// and the one that comes closest (`materializeInSubquery`) refuses anything
// but a single column first — but "no consumer reads by name" is NOT true of
// the runner's rows in general, and saying so would be the false claim a
// reviewer found: the RECURSIVE-CTE materialization keys its working row by
// name, so a duplicate-name column list already collapsed there before this
// pass existed. That path does not come through here, it is broken on both
// sides of this change, and it has its own filing; recorded so the next
// reader does not take the narrow statement for the wide one.
func subqueryRowsPerColumn(schema []parquet.Column, sink *exec.CollectSink) []map[string]any {
	rows := sink.ToRows()
	vals := sink.ToRowValues()
	if vals == nil || len(schema) == 0 {
		return rows
	}
	out := make([]map[string]any, 0, len(vals))
	for _, r := range vals {
		if len(r) != len(schema) {
			// The positional form and the declared schema disagree; the map
			// form is the answer this path always gave.
			return rows
		}
		m := make(map[string]any, len(r))
		for j, v := range r {
			name := schema[j].Name
			if _, dup := m[name]; dup {
				name = fmt.Sprintf("%s:%d", name, j)
			}
			m[name] = v
		}
		out = append(out, m)
	}
	return out
}
