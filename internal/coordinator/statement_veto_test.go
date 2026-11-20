// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ADR-0050 §1 as revised 2026-11-20: the bound validation's refusal is a veto
// on every synchronous path, then two plans (L bound, for the fast path and
// the routes; D unbound, for the stage DAG and distributed EXPLAIN), the
// refusals that end a statement named by provenance, and the incarnation
// check at every transition of §3's list.

// coordRoutes is the sum of a coordinator's refused-plan route counters.
func coordRoutes(c *Coordinator) int64 {
	return c.CorrelatedLocalRoutes() + c.DistinctLocalRoutes() + c.GroupingSetsLocalRoutes() +
		c.GroupKeyLocalRoutes() + c.InSubqueryLocalRoutes() + c.ScalarProjectionLocalRoutes() +
		c.NullAwareAntiLocalRoutes() + c.UnreachableOutputLocalRoutes() +
		c.LateralProjectionLocalRoutes() + c.TableLessLocalRoutes() +
		c.UnbuildableStageLocalRoutes() + c.ResidualSidesLocalRoutes() +
		c.PolicedWindowLocalRoutes() + c.WindowOverLateralLocalRoutes() +
		c.LateralIdentityLocalRoutes()
}

func coordTracked(c *Coordinator) int {
	c.tracker.mu.RLock()
	defer c.tracker.mu.RUnlock()
	return len(c.tracker.queries)
}

// TestTerminalRefusalsAreNamedByProvenance is ADR-0050 §1 Acceptance 5. The
// refusals that end a statement once a plan observes it are exactly the five
// of §1.2, named by where they come from: the static half reads the source and
// fails when a provenance is added or removed, when the classifier decides
// one by an SQLSTATE class (a prefix, a slice of the code), or when the veto's
// refusal stops being returned where the veto runs. The dynamic half shows
// each provenance is terminal, a same-class code of another provenance is
// not, and cancellation is terminal at planning and at execution.
func TestTerminalRefusalsAreNamedByProvenance(t *testing.T) {
	want := []terminalProvenance{
		"the bound-validation veto", "an authorization refusal", "the incarnation refusal",
		"the query-limit refusal", "the statement's cancellation",
	}
	if fmt.Sprint(terminalProvenances) != fmt.Sprint(want) {
		t.Fatalf("the terminal provenances are %q, ADR-0050 §1.2 names %q: a new one is a "+
			"decision of the record, not of the code", terminalProvenances, want)
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "statement_plan.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	consts := map[string]bool{}
	var classifier *ast.FuncDecl
	for _, d := range file.Decls {
		switch d := d.(type) {
		case *ast.GenDecl:
			for _, s := range d.Specs {
				if vs, ok := s.(*ast.ValueSpec); ok {
					if id, ok := vs.Type.(*ast.Ident); ok && id.Name == "terminalProvenance" {
						for _, n := range vs.Names {
							consts[n.Name] = true
						}
					}
				}
			}
		case *ast.FuncDecl:
			if d.Name.Name == "terminalPlanningRefusal" {
				classifier = d
			}
		}
	}
	if len(consts) != len(want) {
		t.Fatalf("statement_plan.go declares %d terminalProvenance constants %v, want %d", len(consts), consts, len(want))
	}
	if classifier == nil {
		t.Fatal("terminalPlanningRefusal is gone")
	}
	// Each provenance but the veto is returned by the classifier exactly once,
	// and no other is.
	returned := map[string]int{}
	ast.Inspect(classifier.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.ReturnStmt:
			for _, r := range n.Results {
				if id, ok := r.(*ast.Ident); ok && consts[id.Name] {
					returned[id.Name]++
				}
			}
		case *ast.SliceExpr:
			t.Errorf("terminalPlanningRefusal slices a value at %s: a SQLSTATE class is not a provenance", fset.Position(n.Pos()))
		case *ast.IndexExpr:
			t.Errorf("terminalPlanningRefusal indexes a value at %s: a SQLSTATE class is not a provenance", fset.Position(n.Pos()))
		case *ast.SelectorExpr:
			if n.Sel.Name == "HasPrefix" || n.Sel.Name == "HasSuffix" || n.Sel.Name == "Contains" {
				t.Errorf("terminalPlanningRefusal matches a string by %s at %s: a SQLSTATE class is not a provenance",
					n.Sel.Name, fset.Position(n.Pos()))
			}
		}
		return true
	})
	for name := range consts {
		wantN := 1
		if name == "terminalVeto" {
			wantN = 0
		}
		if returned[name] != wantN {
			t.Errorf("terminalPlanningRefusal returns %s %d time(s), want %d", name, returned[name], wantN)
		}
	}
	// The veto's refusal is returned where the veto runs: ExecuteSQL returns
	// veto.err on vetoRefused.
	cfile, err := parser.ParseFile(fset, "coordinator.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	vetoReturned := false
	ast.Inspect(cfile, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok || len(cc.List) != 1 {
			return true
		}
		if id, ok := cc.List[0].(*ast.Ident); !ok || id.Name != "vetoRefused" {
			return true
		}
		for _, st := range cc.Body {
			if rs, ok := st.(*ast.ReturnStmt); ok && len(rs.Results) == 2 {
				if se, ok := rs.Results[1].(*ast.SelectorExpr); ok && se.Sel.Name == "err" {
					vetoReturned = true
				}
			}
		}
		return true
	})
	if !vetoReturned {
		t.Error("ExecuteSQL no longer returns the veto's refusal where the veto runs")
	}

	// Dynamic: each provenance, and a same-class code of another one.
	cancelled := context.Canceled
	cases := []struct {
		name     string
		err      error
		ctxErr   error
		terminal bool
		prov     terminalProvenance
	}{
		{"authorization", sqlerr.New("42501", "permission denied for table %q", "t"), nil, true, terminalAuthorization},
		{"authorization, wrapped", fmt.Errorf("physical plan: %w", sqlerr.New("42501", "permission denied")), nil, true, terminalAuthorization},
		// The typed incarnation refusal is terminal on a real refusal in
		// TestClassifyLocalFailureIncarnationRefusalIsFinal and at every
		// transition in TestEveryTransitionChecksTheIncarnation; its type is
		// not named here (the license check counts every AGPL spelling).
		{"query limit", sqlerr.New(physical.QueryLimitSQLState, "query would scan too much"), nil, true, terminalQueryLimit},
		{"cancellation at planning", errors.New("anything"), cancelled, true, terminalCancellation},
		// The same classes, other provenances: not terminal.
		{"42 class, not authorization", sqlerr.New("42803", "must appear in the GROUP BY clause"), nil, false, ""},
		{"42 class, undefined column", sqlerr.New("42703", "column does not exist"), nil, false, ""},
		{"40 class, not the incarnation refusal", sqlerr.New("40001", "could not serialize access"), nil, false, ""},
		{"53 class, not the query limit", sqlerr.New("53200", "out of memory"), nil, false, ""},
		{"57 class without a cancelled context", sqlerr.New("57014", "canceling statement"), nil, false, ""},
		{"uncoded", errors.New("logical plan: something"), nil, false, ""},
	}
	for _, tc := range cases {
		_, prov, ok := terminalPlanningRefusal(tc.err, tc.ctxErr)
		if ok != tc.terminal || prov != tc.prov {
			t.Errorf("%s: terminal=%v (%q), want %v (%q)", tc.name, ok, prov, tc.terminal, tc.prov)
		}
	}
	// Cancellation is terminal at execution too (#308's classifier).
	for _, strict := range []bool{true, false} {
		if got := classifyLocalFailure(errors.New("x"), context.Canceled, strict); got != reportToClient {
			t.Errorf("strict=%v: a cancelled execution was classified %v", strict, got)
		}
	}
}

// TestTheVetoDecidesOnEveryPath is ADR-0050 §1 Acceptance 4: the bound
// validation decides the same on the fast path, on the stage DAG and for
// distributed EXPLAIN; its three outcomes are kept apart; a refusal builds no
// plan, an authorization refusal at the veto included; a statement only the
// bound validation refuses is refused on the stage DAG too.
func TestTheVetoDecidesOnEveryPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newPinFixture(t)
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"pin_t"}, Allow: []string{"read"}}},
	})
	f.provider.Update(authn, authz, nil)
	ctxT := auth.ContextWithIdentity(f.ctx, &auth.Identity{Name: "analyst", Role: "analyst",
		Method: "apikey", Tables: []string{"pin_t"}, Perms: []string{"read"}})
	cells := []struct {
		name, sql, state, msg string
		// veto: the refusal is the bound validation's. A table denial is
		// decided by enforcement inside the first planning run in this
		// fixture (measured: the validation accepts the statement), so those
		// cells show the refusal is terminal there and builds no plan either.
		veto bool
	}{
		{"accepted", "SELECT id FROM pin_t WHERE id = 3", "", "", false},
		{"refused_validity", "SELECT nosuch FROM pin_t", "42703", "", true},
		// An authorization refusal AT the veto: the table-function capability.
		{"refused_capability", "SELECT * FROM read_csv('/nonexistent/x.csv')", "42501",
			`permission denied for table function "read_csv": the "admin" permission is required`, true},
		{"refused_table_denial", "SELECT id FROM pin_u", "42501", `permission denied for table "pin_u"`, false},
		{"refused_table_denial_in_subquery", "SELECT id FROM pin_t WHERE id IN (SELECT id FROM pin_u)",
			"42501", `permission denied for table "pin_u"`, false},
		// Only the bound validation refuses it (PostgreSQL 17.11 refuses it
		// 42803 with this sentence); the unbound plan the stage DAG plans
		// accepts it.
		{"refused_bound_only", "SELECT (SELECT max(t.id + x.grp) FROM pin_t x) FROM pin_t t GROUP BY t.id + 1",
			"42803", `subquery uses ungrouped column "t.id" from outer query`, true},
	}
	for _, cell := range cells {
		for _, arm := range []struct {
			name string
			c    *Coordinator
			sql  string
		}{
			{"fastpath", f.fast, cell.sql}, {"dag", f.dag, cell.sql},
			{"fastpath_explain", f.fast, "EXPLAIN " + cell.sql}, {"dag_explain", f.dag, "EXPLAIN " + cell.sql},
		} {
			t.Run(cell.name+"/"+arm.name, func(t *testing.T) {
				v0, p0, q0 := arm.c.vetoRefusals.Load(), arm.c.planRuns.Load(), coordTracked(arm.c)
				res, err := arm.c.ExecuteSQL(ctxT, arm.sql)
				if err == nil && res != nil && res.Error != "" {
					err = errors.New(res.Error)
				}
				if cell.state == "" {
					if err != nil {
						t.Fatalf("accepted statement answered %v", err)
					}
					if arm.c.vetoRefusals.Load() != v0 {
						t.Error("the veto counted a refusal for an accepted statement")
					}
					return
				}
				if sqlerr.StateOf(err) != cell.state {
					t.Fatalf("got %v, want SQLSTATE %s", err, cell.state)
				}
				if cell.msg != "" && err.Error() != cell.msg {
					t.Errorf("message %q, want %q", err.Error(), cell.msg)
				}
				if vetoed := arm.c.vetoRefusals.Load() == v0+1; vetoed != cell.veto {
					t.Errorf("refused by the veto: %v, want %v", vetoed, cell.veto)
				}
				if arm.c.planRuns.Load() != p0 {
					t.Errorf("%d plan(s) completed for a refused statement", arm.c.planRuns.Load()-p0)
				}
				if coordTracked(arm.c) != q0 {
					t.Error("a DAG query was dispatched for a refused statement")
				}
			})
		}
	}
	// Not applicable: no SELECT to validate. The ordinary run reports it.
	if v := f.dag.validateBound(f.ctx, &plansql.ParsedQuery{}); v.outcome != vetoNotApplicable || v.err == nil {
		t.Errorf("a parse with no SELECT: outcome %v, err %v", v.outcome, v.err)
	}
}

// TestEveryTransitionChecksTheIncarnation is ADR-0050 §1 Acceptance 6: each
// transition of §3's list, by name. inc_t is re-created AT the transition,
// before its check: where the check follows a validation's or a planning
// run's reads, the cell also reads the record under the statement's context,
// as that validation would have, so the mismatch is recorded and only the
// check can return it; where it follows another plan's work, the check reads
// every pinned record itself and the cell reads nothing. The statement is
// refused 40001 at that check: no step runs after it and no plan, route,
// dispatch, fast-path hit or re-execution counter moves after it.
func TestEveryTransitionChecksTheIncarnation(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	const window = "SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM inc_t WHERE id < 3)) AS w FROM inc_u"
	for _, mode := range incProviderModes {
		t.Run(mode, func(t *testing.T) {
			f := newIncFixtureMode(t, nil, mode)
			bg := context.Background()
			tiny := tmdCoordinator(t, bg, f.infra, func(c *Config) { c.LocalFastPathBytes = 1 })
			fall := tmdCoordinator(t, bg, f.infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
			fall.localResultBudgetOverride = 1
			stream := tmdCoordinator(t, bg, f.infra, func(c *Config) { c.StreamingExchange = true })
			for _, c := range []*Coordinator{tiny, fall, stream} {
				if f.provider != nil {
					if err := c.SetAuthProvider(f.provider); err != nil {
						t.Fatal(err)
					}
				}
			}
			cells := []struct {
				name, transition, sql string
				c                     *Coordinator
				read                  bool
			}{
				{"after_veto/fastpath", transitionAfterVeto, "SELECT id, owner FROM inc_t", f.fast, true},
				{"after_veto/dag", transitionAfterVeto, "SELECT id, owner FROM inc_t", f.dag, true},
				{"l_build", transitionLBuild, "SELECT id, owner FROM inc_t", f.fast, true},
				{"d_direct", transitionDDirect, "SELECT id, owner FROM inc_t", f.dag, true},
				{"d_explain/dag", transitionDExplain, "EXPLAIN SELECT id, owner FROM inc_t", f.dag, true},
				{"d_explain/fastpath", transitionDExplain, "EXPLAIN SELECT id, owner FROM inc_t", f.fast, true},
				{"d_after_l_failed", transitionDAfterLFailed, lBuildFailsSQL, f.fast, true},
				{"d_after_declined", transitionDAfterDeclined, "SELECT id, owner FROM inc_t", tiny, true},
				{"d_after_local_run", transitionDAfterLocalRun, "SELECT id, owner FROM inc_t", fall, false},
				{"route_entry/refused_plan", transitionRouteEntry, window, f.dag, false},
				{"route_entry/unreachable_output", transitionRouteEntry,
					"SELECT CAST(id AS INT) AS k, COUNT(*) AS n FROM inc_t GROUP BY CAST(id AS INTEGER) ORDER BY k", f.dag, false},
				{"input_lost_rerun", transitionInputLostRerun, "SELECT id, owner FROM inc_t", stream, false},
			}
			for _, cell := range cells {
				t.Run(cell.name, func(t *testing.T) {
					c := cell.c
					if cell.transition == transitionInputLostRerun {
						lost := false
						stageDAGResultTestHook = func(err error) error {
							if lost || err != nil {
								return err
							}
							lost = true
							return errors.New("stage s0: " + inputLostMarker + " (test)")
						}
						defer func() { stageDAGResultTestHook = nil }()
					}
					// With nothing changed the statement reaches the transition.
					var seen []string
					transitionCheckTestHook = func(_ context.Context, tr string) { seen = append(seen, tr) }
					u0 := c.UnreachableOutputLocalRoutes()
					want := pinAnswer(c, f.ctx, cell.sql)
					transitionCheckTestHook = nil
					reached := false
					for _, s := range seen {
						reached = reached || s == cell.transition
					}
					if !reached {
						t.Fatalf("with nothing changed the statement never reached %q (%v; answered %s)", cell.transition, seen, want)
					}
					if cell.name == "route_entry/unreachable_output" && c.UnreachableOutputLocalRoutes() == u0 {
						t.Fatalf("with nothing changed the unreachable-output route did not answer (%s)", want)
					}

					type counters struct {
						plans, routes, hits, reruns int64
						tracked                     int
					}
					read := func() counters {
						return counters{c.planRuns.Load(), coordRoutes(c), c.LocalFastPathHits(), c.streamingReruns.Load(), coordTracked(c)}
					}
					var at counters
					fired := false
					var after []string
					statementStepTestHook = func(s string) {
						if fired {
							after = append(after, s)
						}
					}
					if cell.transition == transitionInputLostRerun {
						stageDAGResultTestHook = func(err error) error {
							if err != nil {
								return err
							}
							return errors.New("stage s0: " + inputLostMarker + " (test)")
						}
					}
					transitionCheckTestHook = func(ctx context.Context, tr string) {
						if tr != cell.transition || fired {
							return
						}
						fired = true
						// Schema B keeps every column the statements name, so
						// a plan built after the check would validate and run:
						// only the check can stop it.
						f.recreateAs(t, incSchemaB)
						if cell.read {
							_, _ = f.infra.cat.GetTable(ctx, "inc_t")
						}
						at = read()
					}
					got := pinAnswer(c, f.ctx, cell.sql)
					transitionCheckTestHook, statementStepTestHook = nil, nil
					f.restoreA(t)
					if !fired {
						t.Fatalf("the transition %q never ran", cell.transition)
					}
					if got != incRefusal {
						t.Fatalf("re-created at %q, the statement answered %s (nothing changed: %s)", cell.transition, got, want)
					}
					if len(after) > 0 {
						t.Errorf("steps ran after the check at %q: %v", cell.transition, after)
					}
					if now := read(); now != at {
						t.Errorf("counters moved after the check at %q: at the check %+v, after %+v", cell.transition, at, now)
					}
				})
			}
		})
	}
}

// lBuildFailsSQL is a statement the bound validation accepts and whose bound
// plan then fails to build (a check of the logical builder), so the fast path
// is skipped and the stage DAG plans D (ADR-0050 §1.2).
const lBuildFailsSQL = "SELECT id FROM inc_t GROUP BY id HAVING GROUPING(owner) = 0"

// TestTheAuditIsNotRepeatedWithinAStatement is ADR-0050 §1 Acceptance 7 and
// §2's audit paragraph: a column-policy audit emission is not repeated within
// a statement, however many plans reach enforcement — L alone, D alone, L then
// D, L then D then a rebuilt L — and a statement refused before enforcement
// writes none. It shows only that; it does not show every decision is
// audited (completeness is not promised, §2).
func TestTheAuditIsNotRepeatedWithinAStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newPinFixture(t)
	auditLog := &syncBuf{}
	authn, authz := f.provider.Authenticator(), f.provider.Authorizer()
	provider := auth.NewProvider(authn, authz, nil, slog.New(slog.NewTextHandler(auditLog, nil)))
	// Every local result bails out over the budget, so the statement goes on
	// to the stage DAG after its bound plan L was planned and run.
	fall := f.fast
	fall.localResultBudgetOverride = 1
	for _, c := range []*Coordinator{f.fast, f.dag} {
		if err := c.SetAuthProvider(provider); err != nil {
			t.Fatal(err)
		}
	}
	provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, false, true, false))
	if err := provider.BindError(); err != nil {
		t.Fatal(err)
	}
	count := func() int { return strings.Count(auditLog.String(), "column_policy_applied") }
	const window = "SELECT id, owner, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM pin_u)) AS w FROM pin_t"
	cells := []struct {
		name string
		c    *Coordinator
		sql  string
		want int
		// plans is the least number of planning runs the statement must make
		// for the cell to show what it names.
		plans int64
		fresh bool
	}{
		{"d_only", f.dag, "SELECT id, owner FROM pin_t", 1, 1, false},
		{"l_then_d", fall, "SELECT id, owner FROM pin_t", 1, 2, false},
		{"l_then_d_then_rebuilt_l", fall, window, 1, 3, true},
		{"refused_before_enforcement", f.dag, "SELECT nosuch FROM pin_t", 0, 0, false},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			n0, p0, r0 := count(), cell.c.planRuns.Load(), cell.c.routeFreshLocal.Load()
			got := pinAnswer(cell.c, f.ctx, cell.sql)
			if d := cell.c.planRuns.Load() - p0; d < cell.plans {
				t.Fatalf("the statement made %d planning run(s), the cell needs %d (answered %s)", d, cell.plans, got)
			}
			if cell.fresh && cell.c.routeFreshLocal.Load() == r0 {
				t.Fatalf("no rebuilt L (answered %s)", got)
			}
			if d := count() - n0; d != cell.want {
				t.Errorf("%d audit record(s) for one statement, want %d (answered %s)", d, cell.want, got)
			}
		})
	}
	// The emission is per statement: a second statement writes its own.
	n0 := count()
	pinAnswer(fall, f.ctx, "SELECT id, owner FROM pin_t")
	pinAnswer(fall, f.ctx, "SELECT id, owner FROM pin_t")
	if d := count() - n0; d != 2 {
		t.Errorf("two statements wrote %d audit record(s), want 2", d)
	}
}

// TestARouteAnswersUnderTheStatementsPolicy is ADR-0050 §1 Acceptance 9: a
// route runs L under the statement's policy — a mask, a row filter and a
// denial — on the stage DAG (L built by the route), and answers what the fast
// path (L) answers. The window key's subquery is planned by the route's
// pipeline when it first evaluates the key, so it reads pin_t under the mask
// only if the route plans under the context L's enforcement rebound.
func TestARouteAnswersUnderTheStatementsPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newPinFixture(t)
	authn, authz := f.provider.Authenticator(), f.provider.Authorizer()
	// mask owner as '***', row filter grp = 1 (ids 1, 3, 5, 7), pin_u readable.
	f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, true, false))
	// Under the mask the subquery's rows are ids 1 and 3 (filtered, owner
	// '***'): two partitions of two. Unmasked it has none: one partition of four.
	const sql = "SELECT id, owner, COUNT(*) OVER (PARTITION BY id IN " +
		"(SELECT id FROM pin_t WHERE owner = '***' AND id < 4)) AS w FROM pin_t"
	const want = "id=1;owner=***;w=2; | id=3;owner=***;w=2; | id=5;owner=***;w=2; | id=7;owner=***;w=2;"
	fastHits := f.fast.LocalFastPathHits()
	if got := pinAnswer(f.fast, f.ctx, sql); got != want {
		t.Fatalf("fast path (L): %s, want %s", got, want)
	}
	if f.fast.LocalFastPathHits() == fastHits {
		t.Fatal("the fast path did not answer")
	}
	r0 := f.dag.ScalarProjectionLocalRoutes()
	if got := pinAnswer(f.dag, f.ctx, sql); got != want {
		t.Errorf("stage DAG, routed (L built by the route): %s, want %s", got, want)
	}
	if f.dag.ScalarProjectionLocalRoutes() == r0 {
		t.Error("the stage DAG did not route the statement")
	}
	// A denial: pin_u denied by the evaluator. The statement reads it only in
	// the window key, whose plan the local pipeline builds when it first
	// evaluates the key, so on the stage DAG the route is entered and its L
	// refuses there, by the decision's own sentence, with no row — as the
	// fast path refuses it.
	f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, false, false))
	const denied = "SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM pin_u)) AS w FROM pin_t"
	for _, arm := range []struct {
		name   string
		c      *Coordinator
		routed bool
	}{{"fastpath", f.fast, false}, {"dag", f.dag, true}} {
		r := coordRoutes(arm.c)
		got := pinAnswer(arm.c, f.ctx, denied)
		if got != `ERROR 42501: permission denied for table "pin_u"` {
			t.Errorf("%s, denied: %s", arm.name, got)
		}
		if routed := coordRoutes(arm.c) != r; routed != arm.routed {
			t.Errorf("%s, denied: routed=%v, want %v", arm.name, routed, arm.routed)
		}
	}
}

// TestTheTableFunctionContextIsTakenOncePerStatement is ADR-0050 §1 Acceptance
// 10: whether L and D share the table-function context, measured with the
// reader's input changed between the plans. In this build they share it: D's
// run, and a rebuilt L's, take their capability on the context the veto took,
// so they find the veto's reader-schema probe and its cache. Measured: the
// input's schema is read once before D (the fast path declines a
// table-function plan as unestimable, so this read is the veto's validation or
// L's planning); the input is then changed; D's run, the stage planner and the
// route that runs L read it no more. The statement's reads stay at one. (The
// stage planner refuses both statements to a route before it builds a scan of
// the reader, so what D itself would have declared is not observable here.)
func TestTheTableFunctionContextIsTakenOncePerStatement(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newIncFixtureMode(t, nil, incProviderNone)
	fall := tmdCoordinator(t, context.Background(), f.infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
	fall.localResultBudgetOverride = 1
	path := filepath.Join(t.TempDir(), "tf.csv")
	write := func(body string) {
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		fmt.Sprintf("SELECT * FROM read_csv('%s')", path),
		fmt.Sprintf("SELECT u.id, r.b FROM inc_u u JOIN read_csv('%s') r ON u.id = r.a", path),
	} {
		write("a,b\n2,20\n3,30\n")
		r0, p0, fr0 := physical.ReaderSchemaReads.Load(), fall.planRuns.Load(), fall.routeFreshLocal.Load()
		var readsAtD int64 = -1
		var got string
		fired := atStep("dag-planning", func() {
			readsAtD = physical.ReaderSchemaReads.Load() - r0
			write("a,b,c\n2,21,1\n3,31,1\n")
		}, func() { got = pinAnswer(fall, f.ctx, sql) })
		reads, plans := physical.ReaderSchemaReads.Load()-r0, fall.planRuns.Load()-p0
		t.Logf("%s: %d planning run(s), rebuilt L %d, reader-schema reads before D %d, in all %d: %s",
			sql, plans, fall.routeFreshLocal.Load()-fr0, readsAtD, reads, got)
		if !fired {
			t.Fatalf("%s never reached D", sql)
		}
		if readsAtD != 1 {
			t.Fatalf("%s: the input was read %d time(s) before D: the cell does not change it between the plans", sql, readsAtD)
		}
		if reads != 1 {
			t.Errorf("%s: the changed input's schema was read again after L (%d reads in all): the plans did not share the context", sql, reads)
		}
	}
}
