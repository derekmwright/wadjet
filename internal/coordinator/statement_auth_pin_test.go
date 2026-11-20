// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ADR-0050 slice 1: a coordinator statement decides every authorization
// question against the ONE state it was accepted with (A1, A4), and an
// authorization refusal is final (A3).

func pinTables() []tmdTable {
	tSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "owner", Type: parquet.TypeString},
	}}
	uSchema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	var tRows, uRows []map[string]any
	for i := int64(1); i <= 8; i++ {
		tRows = append(tRows, map[string]any{"id": i, "grp": i % 2, "owner": fmt.Sprintf("o-%d", i)})
	}
	for _, i := range []int64{2, 3, 4} {
		uRows = append(uRows, map[string]any{"id": i})
	}
	return []tmdTable{{"pin_t", tSchema, tRows}, {"pin_u", uSchema, uRows}}
}

// pinPolicy builds the ABAC set: pin_t readable (optionally masked and
// filtered), pin_u readable unless denyU, and optionally a rule naming a
// relation the catalog does not hold (a set that fails to bind).
func pinPolicy(allowT, mask, filter, allowU, unbindable bool) *auth.PolicyEvaluator {
	var rules []auth.PolicyRule
	if allowT {
		var obl []auth.Obligation
		if mask {
			obl = append(obl, auth.Obligation{Type: "mask_column", Target: "owner", Value: "'***'"})
		}
		if filter {
			obl = append(obl, auth.Obligation{Type: "row_filter", Value: "grp = 1"})
		}
		rules = append(rules, auth.PolicyRule{
			ID: "t", EffectStr: "allow", Priority: 10,
			Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "pin_t"}},
			Actions:     []auth.Action{auth.ActionRead},
			Obligations: obl,
		})
	}
	if allowU {
		rules = append(rules, auth.PolicyRule{
			ID: "u", EffectStr: "allow", Priority: 10,
			Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "pin_u"}},
			Actions:   []auth.Action{auth.ActionRead},
		})
	}
	if unbindable {
		rules = append(rules, auth.PolicyRule{
			ID: "x", EffectStr: "allow", Priority: 10,
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "no_such_relation"}},
			Actions:   []auth.Action{auth.ActionRead},
		})
	}
	return auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{Name: "pin", Version: 1, Enabled: true, Rules: rules}})
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuf) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.Reset()
}

type pinFixture struct {
	fast, dag *Coordinator
	provider  *auth.Provider
	ctx       context.Context // carries the analyst identity
	logs      *syncBuf
}

func newPinFixture(t *testing.T) pinFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	logs := &syncBuf{}
	infra := tmdInfra(t, ctx)
	infra.logger = slog.New(slog.NewTextHandler(io.MultiWriter(logs, os.Stderr),
		&slog.HandlerOptions{Level: slog.LevelWarn}))
	tmdWriteTableList(t, ctx, infra, nil, pinTables())
	fast := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
	dag := tmdCoordinator(t, ctx, infra)
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := fast.SetAuthProvider(provider); err != nil {
		t.Fatal(err)
	}
	if err := dag.SetAuthProvider(provider); err != nil {
		t.Fatal(err)
	}
	id := &auth.Identity{Name: "analyst", Role: "analyst", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read"}}
	return pinFixture{fast: fast, dag: dag, provider: provider,
		ctx: auth.ContextWithIdentity(ctx, id), logs: logs}
}

// pinAnswer is a statement's outcome as one comparable string: its rows
// (sorted), or its SQLSTATE.
func pinAnswer(c *Coordinator, ctx context.Context, sql string) string {
	res, err := c.ExecuteSQL(ctx, sql)
	if err == nil && res != nil && res.Error != "" {
		err = fmt.Errorf("%s", res.Error)
	}
	if err != nil {
		return "ERROR " + sqlerr.StateOf(err) + ": " + err.Error()
	}
	rows, rerr := res.Rows()
	if rerr != nil {
		return "ROWS ERROR " + rerr.Error()
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&b, "%s=%v;", k, r[k])
		}
		out = append(out, b.String())
	}
	sort.Strings(out)
	return strings.Join(out, " | ")
}

// withStepReload runs fn with reload landing ONCE at the named step of the
// statement's decision sequence.
func withStepReload(step string, reload func(), fn func()) (fired bool) {
	statementStepTestHook = func(s string) {
		if s == step && !fired {
			fired = true
			reload()
		}
	}
	defer func() { statementStepTestHook = nil }()
	fn()
	return fired
}

// TestAStatementIsDecidedUnderTheStateItWasAcceptedWith is the A1/A4 gate on
// the coordinator: a policy reloaded between two steps of ONE statement does
// not change what that statement answers — tightened, loosened, a mask
// removed, or a reload that fails to bind — on either execution, including
// the decisions taken late (a table that only becomes a scan when the
// optimizer decorrelates an IN subquery; a system-catalog relation).
func TestAStatementIsDecidedUnderTheStateItWasAcceptedWith(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newPinFixture(t)
	authn, authz := f.provider.Authenticator(), f.provider.Authorizer()

	shapes := []struct {
		name     string
		accepted func()
		reloads  map[string]func()
	}{
		{
			name: "abac",
			accepted: func() {
				f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, true, false))
			},
			reloads: map[string]func(){
				"tightened":    func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, false, false)) },
				"tightened_t":  func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(false, false, false, true, false)) },
				"loosened":     func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, false, true, false)) },
				"mask_removed": func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, false, true, true, false)) },
				"fails_to_bind": func() {
					f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, false, false, true, true))
				},
			},
		},
		{
			// No evaluator: access is the role's, and the decision a scan the
			// optimizer mints asks is the provider's table access at that moment.
			name:     "roles",
			accepted: func() { f.provider.Update(authn, authz, nil) },
			reloads: map[string]func(){
				"tightened": func() { f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, false, false)) },
				"fails_to_bind": func() {
					f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, false, false, true, true))
				},
			},
		},
	}
	statements := []struct{ name, sql string }{
		{"plain", "SELECT id, owner FROM pin_t"},
		{"decorrelated_in", "SELECT id, owner FROM pin_t WHERE id IN (SELECT id FROM pin_u)"},
		{"system_catalog", "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE 'pin%'"},
	}
	steps := []string{"accepted", "validated", "enforced", "optimized", "distributed"}
	arms := []struct {
		name string
		c    *Coordinator
	}{{"fastpath", f.fast}, {"dag", f.dag}}

	moved := 0
	for _, sh := range shapes {
		sh.accepted()
		if err := f.provider.BindError(); err != nil {
			t.Fatalf("%s: the accepted set does not bind: %v", sh.name, err)
		}
		for _, st := range statements {
			for _, arm := range arms {
				want := pinAnswer(arm.c, f.ctx, st.sql)
				if strings.HasPrefix(want, "ERROR") || want == "" {
					t.Fatalf("%s/%s/%s: the reference answer under the accepted state is %q",
						sh.name, st.name, arm.name, want)
				}
				// The reload itself changes the answer of the NEXT statement;
				// a cell where it would not is no evidence, so it is skipped.
				for rname, reload := range sh.reloads {
					reload()
					next := pinAnswer(arm.c, f.ctx, st.sql)
					sh.accepted()
					if next == want {
						continue
					}
					for _, step := range steps {
						var got string
						fired := withStepReload(step, reload, func() { got = pinAnswer(arm.c, f.ctx, st.sql) })
						sh.accepted()
						if !fired {
							continue
						}
						if got != want {
							moved++
							t.Errorf("%s/%s/%s: a %s reload at step %q changed the statement's answer\n"+
								"   accepted state: %s\n   got:            %s",
								sh.name, st.name, arm.name, rname, step, want, got)
						}
					}
				}
			}
		}
	}
	if moved > 0 {
		t.Logf("%d cells decided (in part) under a state other than the accepted one", moved)
	}
}

// TestAFastPathAuthorizationRefusalIsFinal is A3: a statement the fast path
// refuses on authorization is refused with that refusal's SQLSTATE and
// sentence, and nothing else is planned or dispatched for it — whatever
// WADJET_FASTPATH_STRICT says, because a refusal is not an execution failure
// another path may be asked about.
func TestAFastPathAuthorizationRefusalIsFinal(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	for _, strict := range []bool{true, false} {
		t.Run(fmt.Sprintf("strict=%v", strict), func(t *testing.T) {
			prev := FastPathStrict.Set(strict)
			defer FastPathStrict.Set(prev)
			testAFastPathAuthorizationRefusalIsFinal(t)
		})
	}
}

func testAFastPathAuthorizationRefusalIsFinal(t *testing.T) {
	f := newPinFixture(t)
	authn, authz := f.provider.Authenticator(), f.provider.Authorizer()
	// The window's PARTITION BY carries an IN subquery over a relation this
	// identity may not read. The scan estimate admits the plan to the fast
	// path (a subquery in a window key is not one it looks for), and the
	// subquery's own plan, which the local pipeline builds when it first
	// evaluates the key, is where the refusal is decided.
	const sql = "SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM pin_u)) AS w FROM pin_t"
	cells := []struct {
		name string
		ctx  context.Context
		set  func()
	}{
		{"abac", f.ctx, func() {
			f.provider.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, false, false))
		}},
		{"roles", auth.ContextWithIdentity(f.ctx, &auth.Identity{Name: "analyst", Role: "analyst",
			Method: "apikey", Tables: []string{"pin_t"}, Perms: []string{"read"}}),
			func() { f.provider.Update(authn, authz, nil) }},
	}
	c := f.fast
	routes := func() int64 {
		return c.CorrelatedLocalRoutes() + c.DistinctLocalRoutes() + c.GroupingSetsLocalRoutes() +
			c.GroupKeyLocalRoutes() + c.InSubqueryLocalRoutes() + c.ScalarProjectionLocalRoutes() +
			c.NullAwareAntiLocalRoutes() + c.UnreachableOutputLocalRoutes() +
			c.LateralProjectionLocalRoutes() + c.TableLessLocalRoutes() +
			c.UnbuildableStageLocalRoutes() + c.ResidualSidesLocalRoutes() +
			c.PolicedWindowLocalRoutes() + c.WindowOverLateralLocalRoutes() +
			c.LateralIdentityLocalRoutes()
	}
	tracked := func() int {
		c.tracker.mu.RLock()
		defer c.tracker.mu.RUnlock()
		return len(c.tracker.queries)
	}
	reachedPhysical := 0
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			cell.set()
			f.logs.Reset()
			var steps []string
			statementStepTestHook = func(s string) { steps = append(steps, s) }
			defer func() { statementStepTestHook = nil }()
			r0, q0, h0 := routes(), tracked(), c.LocalFastPathHits()
			res, err := c.ExecuteSQL(cell.ctx, sql)
			if err == nil && res != nil && res.Error != "" {
				err = fmt.Errorf("%s", res.Error)
			}
			if err == nil {
				t.Fatalf("answered where the identity may not read pin_u")
			}
			if state := sqlerr.StateOf(err); state != "42501" {
				t.Fatalf("SQLSTATE %q, want 42501: %v", state, err)
			}
			if want := `permission denied for table "pin_u"`; err.Error() != want {
				t.Errorf("message %q, want the decision's own sentence %q", err.Error(), want)
			}
			last := ""
			if len(steps) > 0 {
				last = steps[len(steps)-1]
			}
			// "verified" (ADR-0050 slice 2) is the step after enforcement
			// was verified, the last before the fast path plans.
			if last != "verified" && last != "distributed" {
				t.Fatalf("refused before physical planning (last step %q): the cell no longer "+
					"exercises the fast path's refusal: %v", last, err)
			}
			reachedPhysical++
			if last == "distributed" {
				t.Errorf("a fast-path authorization refusal went on to the stage planner: %v", err)
			}
			if strings.Contains(f.logs.String(), "routing to DAG") {
				t.Errorf("the refusal was logged as routed to the DAG")
			}
			if d := routes() - r0; d != 0 {
				t.Errorf("%d refused-plan route(s) ran after the refusal", d)
			}
			if d := tracked() - q0; d != 0 {
				t.Errorf("%d DAG quer(ies) dispatched after the refusal", d)
			}
			if c.LocalFastPathHits() != h0 {
				t.Errorf("the fast path counted a hit for a refused statement")
			}
		})
	}
	if reachedPhysical == 0 {
		t.Fatal("no cell reached fast-path physical planning: the gate tests nothing")
	}
}

// TestAFastPathPlanningAuthorizationRefusalIsFinal is A3 at the fast path's
// PLANNING branch. No SQL statement was found that the scan estimate admits
// and physical planning then refuses on authorization (the refusals the
// shared shapes reach are decided at logical planning, at execution, or after
// the estimate declines), so the branch is driven with the real planner over
// a plan whose only authorization question is asked at physical planning: a
// table-function scan, which the planner authorizes through the context
// guard where it builds the source.
func TestAFastPathPlanningAuthorizationRefusalIsFinal(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newPinFixture(t)
	c := f.fast
	asked := 0
	ctx := logical.ContextWithTableFuncGuard(f.ctx, func(name string, _ []string, _ map[string]string) error {
		asked++
		return sqlerr.New("42501", "permission denied for table function %q: test refusal", name)
	})
	// TableName names a catalog table so the scan estimate is answerable and
	// the plan is admitted; the guard runs before anything is opened.
	plan := &logical.Node{Type: logical.NodeScan, TableName: "pin_t", IsTableFunc: true,
		FuncName: "read_csv", FuncArgs: []string{"'/nonexistent/pin.csv'"}}
	lp := &statementPlan{ctx: ctx, node: plan, bound: true, opts: c.statementPlanOptions(ctx)}
	res, handled, err := c.tryLocalFastPath("q-a3-plan", lp, time.Now())
	if asked == 0 {
		t.Fatal("the planner never asked the guard: the plan did not reach physical planning")
	}
	if !handled {
		t.Fatalf("an authorization refusal at physical planning was handed on to the DAG (err=%v)", err)
	}
	if res != nil || sqlerr.StateOf(err) != "42501" {
		t.Fatalf("res=%v err=%v, want the 42501 refusal", res, err)
	}
	if want := `permission denied for table function "read_csv": test refusal`; err.Error() != want {
		t.Fatalf("message %q, want the decision's own sentence %q", err.Error(), want)
	}
}

// TestAReExecutionKeepsTheStatementsPin: the ErrInputLost rerun re-enters
// ExecuteSQL with the statement's context, and must decide under the state
// (and at the time) the statement was accepted with, not take a second pin.
func TestAReExecutionKeepsTheStatementsPin(t *testing.T) {
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	live := auth.NewProvider(authn, authz, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	live.UpdateWithEvaluator(authn, authz, nil, pinPolicy(true, true, true, true, false))
	c := &Coordinator{authProvider: live}

	ctx1, first := c.statementAuth(context.Background())
	if first == nil || first == live {
		t.Fatalf("the statement did not take a pinned copy (got live=%v)", first == live)
	}
	t1 := auth.DecisionEnvironment(ctx1, "coordinator").Time
	live.UpdateWithEvaluator(authn, authz, nil, pinPolicy(false, false, false, false, false))
	time.Sleep(2 * time.Millisecond)
	ctx2, again := c.statementAuth(withStreamingExchangeDisabled(ctx1))
	if again != first {
		t.Fatal("the re-execution took a second pin: it would decide under the reloaded state")
	}
	if t2 := auth.DecisionEnvironment(ctx2, "coordinator").Time; !t2.Equal(t1) {
		t.Fatalf("the re-execution decides at %v, the statement at %v", t2, t1)
	}
	if first.Evaluator() == live.Evaluator() {
		t.Fatal("the reload reached the statement's pin")
	}

	// No provider pins nil, and the re-execution keeps that too.
	none := &Coordinator{}
	ctx3, p3 := none.statementAuth(context.Background())
	none.authProvider = live
	if _, p4 := none.statementAuth(ctx3); p3 != nil || p4 != nil {
		t.Fatalf("a statement accepted with no provider decided under one (%v, %v)", p3, p4)
	}
}
