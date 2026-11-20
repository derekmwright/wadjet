// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ADR-0050 §1 (revised 2026-11-20): a veto, then two plans. The statement's
// bound validation runs first, under its pinned authorization state, on every
// synchronous path; if it refuses, the statement is refused. Otherwise every
// answer comes from ONE plan, built and enforced by one complete planning run:
// L, bound (auth.BindStatementColumns, as the embedded engine plans it), for
// the local paths — the fast path and every refused-plan route — continuing
// from the veto's own validation; and D, unbound (auth.ValidateStatementColumns,
// exactly today's plan), for the stage DAG and distributed EXPLAIN, which
// cannot take a bound plan yet.

// statementPlanOptions is the planner configuration of ONE statement, read
// once when it is accepted and given to every planning run and every physical
// planner the statement builds (ADR-0050 §1.3 item 1).
type statementPlanOptions struct {
	logical             logical.Options
	queryLimits         *config.QueryLimits
	sortMergeJoinBytes  int64
	lateMaterialization bool
	bushyJoinReorder    bool
}

// statementPlanOptions reads this coordinator's planner configuration for the
// statement whose identity ctx carries.
func (c *Coordinator) statementPlanOptions(ctx context.Context) *statementPlanOptions {
	return &statementPlanOptions{
		logical:             c.logicalOptions(),
		queryLimits:         c.resolveQueryLimits(ctx),
		sortMergeJoinBytes:  c.config.SortMergeJoinBytes,
		lateMaterialization: c.config.LateMaterialization,
		bushyJoinReorder:    c.config.BushyJoinReorder,
	}
}

// applyLocal sets the options a local (single-process) physical planner
// reads.
func (o *statementPlanOptions) applyLocal(p *physical.Planner) {
	p.SortMergeJoinBytes = o.sortMergeJoinBytes
	p.LateMaterialization = o.lateMaterialization
	p.BushyJoinReorder = o.bushyJoinReorder
	p.QueryLimits = o.queryLimits
}

// statementPlan is one planning run's result: the optimized, enforced logical
// plan, the context its enforcement rebound (the column policies, the policy
// lookup and the identity's limits the physical planner reads), and its text.
//
// Physical planning mutates a logical plan, so a value is consumed by the
// first physical planner given it, and a second use is a coding error that
// consume reports. Reading it afterwards (merge info, categories) is what the
// stage DAG has always done with the plan it planned, and stays allowed.
type statementPlan struct {
	ctx   context.Context
	node  *logical.Node
	text  string
	bound bool
	opts  *statementPlanOptions
	// consumedBy names what physically planned this value; "" while unused.
	consumedBy string
}

// consume hands the plan to the physical planner named by, once.
func (p *statementPlan) consume(by string) (*logical.Node, error) {
	if p.consumedBy != "" {
		return nil, sqlerr.New("XX000", "internal error: the %s statement plan was already "+
			"physically planned by %s and cannot be planned again by %s", p.kind(), p.consumedBy, by)
	}
	p.consumedBy = by
	return p.node, nil
}

func (p *statementPlan) kind() string {
	if p.bound {
		return "bound"
	}
	return "unbound"
}

// vetoOutcome is what the bound validation decided about a statement
// (ADR-0050 §1.1). The three values are kept apart.
type vetoOutcome int

const (
	// vetoNotApplicable: the statement is not one a planning run plans (its
	// SELECT could not be extracted). The ordinary run reports why.
	vetoNotApplicable vetoOutcome = iota
	// vetoRefused: terminal, whatever the refusal is (validity, the
	// table-function capability, authorization).
	vetoRefused
	// vetoAccepted: the validation's parse, its bindings and its
	// table-function context are L's, reused when L is built.
	vetoAccepted
)

func (o vetoOutcome) String() string {
	switch o {
	case vetoRefused:
		return "refused"
	case vetoAccepted:
		return "accepted"
	}
	return "not applicable"
}

// boundValidation is the veto's result: its outcome, the refusal (or the
// reason it does not apply), and, when accepted, the statement context it took
// (the table-function context: the capability guard, the catalog access, the
// reader-schema probe) and the bound SELECT the first L continues from.
type boundValidation struct {
	outcome vetoOutcome
	err     error
	ctx     context.Context
	info    *plansql.SelectInfo
}

// validateBound is the veto (ADR-0050 §1.1): the statement's bound validation,
// under its pinned authorization state, on the dispatch parse. It is the
// validation step of the statement's first planning run, L's, not a second
// attempt: an accepted L continues from it (completePlan) and does not
// validate again. ExecuteSQL runs it on every synchronous path, with the fast
// path on or off, whatever the statement's size, distributed EXPLAIN included.
func (c *Coordinator) validateBound(ctx context.Context, parsed *plansql.ParsedQuery) *boundValidation {
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return &boundValidation{outcome: vetoNotApplicable, err: fmt.Errorf("extract: %w", err)}
	}
	stmtAuth := statementAuthOf(ctx)
	// The table-function CAPABILITY, BEFORE the binder and before the scan
	// annotation (ADR-0034, ADR-0039 §3). A denied identity is refused here
	// with nothing opened, and the context this returns is what lets the
	// planner read a reader's schema at all.
	vctx, err := auth.AuthorizeTableFunctions(ctx, stmtAuth, "coordinator", selectInfo)
	if err != nil {
		return &boundValidation{outcome: vetoRefused, err: err}
	}
	// Reject references to columns that resolve to no source (plan-time name
	// binding), against the schema the CALLING IDENTITY can see — a column an
	// ABAC policy denies is not in this caller's table, so it is not in the
	// error's "available:" hint either (#859) — and record each reference's
	// resolved column on the AST (ADR-0047), as the embedded engine does.
	if err := auth.BindStatementColumns(vctx, stmtAuth, c.catalog, selectInfo, "coordinator"); err != nil {
		return &boundValidation{outcome: vetoRefused, err: err}
	}
	statementStep("validated")
	return &boundValidation{outcome: vetoAccepted, ctx: vctx, info: selectInfo}
}

// planStatement is one complete planning run from the statement's text, from
// ExtractSelect to EnforceOptimizedPlan and the C2 verification after it
// (ADR-0050 §1.3 item 1). stamp=true binds the statement (a rebuilt L);
// stamp=false validates it unbound (D: the stage planner's own validation).
//
// It parses sql afresh: the builder and the optimizer mutate the tree in
// place, so two plans never share one. The first L does not come here: it
// continues from the veto's parse (completePlan).
//
// ctx is the statement's own context — its pinned authorization state and
// decision time, its manifest snapshot and its incarnation guard, which every
// run shares (ADR-0050 A1, A2, C4), and the table-function context the veto
// took. The returned plan carries the context its own enforcement rebound;
// another run's never leaks into it.
func (c *Coordinator) planStatement(ctx context.Context, sql string, stamp bool, opts *statementPlanOptions) (*statementPlan, error) {
	stmtAuth := statementAuthOf(ctx)
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return nil, err
	}
	selectInfo, err := plansql.ExtractSelect(parsed)
	if err != nil {
		return nil, fmt.Errorf("extract: %w", err)
	}
	// The capability again, for this run's own SELECT: the decision is the
	// pinned state's, so it repeats the veto's. On the context the veto took,
	// the reader-schema probe — and every reader schema it has read — is the
	// veto's (physical.ContextWithReaderSchemaProbe keeps an existing probe).
	ctx, err = auth.AuthorizeTableFunctions(ctx, stmtAuth, "coordinator", selectInfo)
	if err != nil {
		return nil, err
	}
	if stamp {
		err = auth.BindStatementColumns(ctx, stmtAuth, c.catalog, selectInfo, "coordinator")
	} else {
		err = auth.ValidateStatementColumns(ctx, stmtAuth, c.catalog, selectInfo, "coordinator")
	}
	if err != nil {
		return nil, err
	}
	statementStep("validated")
	return c.completePlan(ctx, selectInfo, stamp, opts)
}

// completePlan finishes a planning run whose SELECT has been validated, bound
// or not, under ctx: the logical build, the scan annotation, enforcement, the
// optimizer, the late enforcement and the C2 verification.
func (c *Coordinator) completePlan(ctx context.Context, selectInfo *plansql.SelectInfo, stamp bool, opts *statementPlanOptions) (*statementPlan, error) {
	stmtAuth := statementAuthOf(ctx)
	logicalPlan, err := logical.BuildFromSelect(selectInfo)
	if err != nil {
		if sqlerr.StateOf(err) != "" {
			// A coded refusal is its own sentence (#1145).
			return nil, err
		}
		return nil, fmt.Errorf("logical plan: %w", err)
	}

	// Annotate scan columns and optimize — pass scan annotator for IN decorrelation
	scanAnnotator := func(plan *logical.Node) {
		physical.NewPlannerForContext(ctx, c.catalog).AnnotateScanColumns(ctx, plan)
	}
	scanAnnotator(logicalPlan)
	statementStep("annotated")

	// ABAC enforcement at plan level — the same auth.EnforcePlanPolicies the
	// embedded engine applies (table denial, row filters, column deny/mask).
	// Runs BEFORE Optimize so every consumer of this plan sees it enforced.
	if stmtAuth != nil && stmtAuth.Enabled() {
		// ctx is REBOUND: it carries the column policies onward so the
		// physical planner applies the same projection to an expression
		// subquery, which it plans on its own (#859).
		ctx, logicalPlan, err = auth.EnforcePlanPolicies(ctx, stmtAuth, c.catalog, selectInfo, logicalPlan, "coordinator")
		if err != nil {
			return nil, err
		}
	} else if rowFilters := auth.RowFiltersFromContext(ctx); len(rowFilters) > 0 {
		// Legacy path for deployments without a provider wired into the
		// coordinator: the HTTP server pre-evaluates policies and passes
		// row filters through context.
		for table, filter := range rowFilters {
			logicalPlan = logical.InjectRowFilter(logicalPlan, table, filter)
		}
	}
	statementStep("enforced")

	logicalPlan = logical.OptimizeWith(logicalPlan, opts.logical, scanAnnotator)
	// The optimizer MINTS scans; those are created after enforcement (#859).
	logicalPlan, err = auth.EnforceOptimizedPlan(ctx, c.catalog, logicalPlan)
	if err != nil {
		return nil, err
	}
	statementStep("optimized")
	// C2 for every decision this run took, the late ones the optimizer asked
	// included, before the plan is physically planned or run.
	if err := statementGuard.VerifyIncarnations(ctx); err != nil {
		return nil, err
	}
	statementStep("verified")
	c.planRuns.Add(1)
	return &statementPlan{
		ctx:   ctx,
		node:  logicalPlan,
		text:  logicalPlan.PrettyPrint(0),
		bound: stamp,
		opts:  opts,
	}, nil
}

// statementAuthOf is the statement's pinned authorization state, which
// statementAuth put on ctx; nil when there is none.
func statementAuthOf(ctx context.Context) *auth.Provider {
	p, _ := ctx.Value(statementAuthKey{}).(*auth.Provider)
	return p
}

// terminalProvenance names where a refusal that ends the statement, once a
// plan of it observes it, comes from (ADR-0050 §1.2). Terminal means: once
// observed, no other plan is built or run. It is decided by PROVENANCE, never
// by SQLSTATE class. Everything else keeps the disposition it had before the
// veto: a failure of L after validation skips the fast path and the stage DAG
// plans D; a local execution failure follows #308's rule (classifyLocalFailure).
//
// terminalProvenances is the whole list; TestTerminalRefusalsAreNamedByProvenance
// names each one and fails when one is added, removed or decided by class.
type terminalProvenance string

const (
	// The bound validation's refusal (§1.1), returned where the veto runs.
	terminalVeto terminalProvenance = "the bound-validation veto"
	// 42501 from a decision under the statement's pinned state (A3).
	terminalAuthorization terminalProvenance = "an authorization refusal"
	// The typed incarnation refusal, 40001 (§3, C3).
	terminalIncarnation terminalProvenance = "the incarnation refusal"
	// 53400 from the cost walk over the plan being physically planned.
	terminalQueryLimit terminalProvenance = "the query-limit refusal"
	// The statement's own context cancelled or past its deadline, at planning
	// and at execution.
	terminalCancellation terminalProvenance = "the statement's cancellation"
)

var terminalProvenances = []terminalProvenance{
	terminalVeto, terminalAuthorization, terminalIncarnation, terminalQueryLimit, terminalCancellation,
}

// terminalPlanningRefusal reports whether err, observed while a plan of the
// statement was being PLANNED (logically or physically), ends the statement,
// with the error to report and its provenance. ctxErr is the statement
// context's error. The veto is not decided here: its refusal never reaches a
// plan (ExecuteSQL returns it where the veto runs).
func terminalPlanningRefusal(err, ctxErr error) (error, terminalProvenance, bool) {
	if ctxErr != nil {
		return err, terminalCancellation, true
	}
	if statementGuard.IsIncarnationChanged(err) {
		return err, terminalIncarnation, true
	}
	if refusal, ok := authorizationRefusal(err); ok {
		return refusal, terminalAuthorization, true
	}
	if sqlerr.StateOf(err) == physical.QueryLimitSQLState {
		return err, terminalQueryLimit, true
	}
	return nil, "", false
}

// finalPlanningError reports whether a failure to build L ends the statement
// (terminalPlanningRefusal). Any other failure of L leaves the stage DAG to
// plan D, as it did before L existed (ADR-0050 §1.2).
func finalPlanningError(ctx context.Context, err error) (error, bool) {
	final, _, ok := terminalPlanningRefusal(err, ctx.Err())
	return final, ok
}

// The transitions of ADR-0050 §3 ("Where the check must run"), each named.
// The incarnation check runs at each, and a mismatch it finds refuses the
// statement before the transition: no plan is built or run after it. The
// last item of that list, "any result being returned", is ExecuteSQL's
// deferred check.
const (
	transitionAfterVeto      = "after the veto"
	transitionLBuild         = "L's build"
	transitionDDirect        = "D after the veto, fast path off"
	transitionDExplain       = "D for distributed EXPLAIN"
	transitionDAfterLFailed  = "D after L failed to build"
	transitionDAfterDeclined = "D after the fast path declined (size, saturation)"
	transitionDAfterLocalRun = "D after a fast-path physical-planning failure or bail-out"
	transitionRouteEntry     = "a route's entry"
	transitionInputLostRerun = "the input-lost re-execution"
)

// transitionsAfterWork are the transitions that follow work of another plan
// (a stage plan physically planned, a local plan planned or run, a DAG run):
// the check there reads the record of EVERY relation the statement pinned,
// so a table re-created since its record was last read refuses even when no
// read has seen it since. The others directly follow the reads of a
// validation or a planning run, every one of which compared its record, and
// check what those reads and the decisions since the last check found.
var transitionsAfterWork = map[string]bool{
	transitionDAfterLocalRun: true,
	transitionRouteEntry:     true,
	transitionInputLostRerun: true,
}

// transitionCheckTestHook runs at each named transition, BEFORE its check,
// with the statement's context: a test re-creates a table there (and may read
// its record under ctx, as a validation or a planner would). Nil in every
// non-test build.
var transitionCheckTestHook func(ctx context.Context, transition string)

// checkTransition is the incarnation check at one named transition (ADR-0050
// §3): a mismatch any read of the statement recorded, or that the check's own
// reads find, refuses the statement with 40001 here.
func checkTransition(ctx context.Context, transition string) error {
	if transitionCheckTestHook != nil {
		transitionCheckTestHook(ctx, transition)
	}
	if transitionsAfterWork[transition] {
		return statementGuard.VerifyEveryPinnedIncarnation(ctx)
	}
	return statementGuard.VerifyIncarnations(ctx)
}

// localPlans holds a statement's bound plan L across the paths that may use
// it: the fast path first, then a route the stage planner sends it to. L is
// completed only when a local path will consume it (ADR-0050 §1.3 item 2).
type localPlans struct {
	c    *Coordinator
	ctx  context.Context // the statement's context, not a run's
	sql  string
	opts *statementPlanOptions
	// veto is the accepted bound validation the first L continues from; nil
	// once that L is built, so a later L is a fresh bound run.
	veto *boundValidation
	// plan is the current L, nil until built; err is why it could not be.
	plan *statementPlan
	err  error
}

// build plans L now: the first from the veto's accepted validation, a later
// one as a fresh bound run. The incarnation check runs first (§3: before L's
// logical build after the accepted validation).
func (l *localPlans) build() {
	l.plan = nil
	if l.err = checkTransition(l.ctx, transitionLBuild); l.err != nil {
		return
	}
	if v := l.veto; v != nil {
		l.veto = nil
		l.plan, l.err = l.c.completePlan(v.ctx, v.info, true, l.opts)
	} else {
		l.plan, l.err = l.c.planStatement(l.ctx, l.sql, true, l.opts)
	}
	if l.err != nil {
		l.c.localBuildFailed.Add(1)
	}
}

// forRoute is the L a route runs (ADR-0050 §1.3 item 4): the current one while
// nothing has physically planned it, a fresh one when something has, and L's
// own error when L could not be built.
func (l *localPlans) forRoute() (*statementPlan, error) {
	if l.err != nil {
		return nil, l.err
	}
	if l.plan != nil && l.plan.consumedBy == "" {
		return l.plan, nil
	}
	fresh := l.plan != nil
	l.build()
	if l.err != nil {
		return nil, l.err
	}
	if fresh {
		l.c.routeFreshLocal.Add(1)
	}
	return l.plan, nil
}
