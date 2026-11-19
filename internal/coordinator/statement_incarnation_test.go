// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ADR-0050 slice 2 (§3, C1-C4): an authorization decision is about the table
// incarnation the statement reads. A table dropped and re-created between the
// statement's manifest pin and a decision refuses the statement with 40001,
// and nothing else is planned or run in its place.

var (
	incSchemaA = parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "owner", Type: parquet.TypeString},
	}}
	// The re-created table: the same names, owner now a BIGINT, so a mask
	// chosen by the column's declared type (redact) is "0" for it and
	// '***' for the incarnation the statement pinned.
	incSchemaB = parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "owner", Type: parquet.TypeInt64},
	}}
	// The re-created table without the masked column at all.
	incSchemaC = parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
)

func incRowsA() []map[string]any {
	var rows []map[string]any
	for i := int64(1); i <= 4; i++ {
		rows = append(rows, map[string]any{"id": i, "owner": fmt.Sprintf("o-%d", i)})
	}
	return rows
}

func incRowsB() []map[string]any {
	var rows []map[string]any
	for i := int64(11); i <= 13; i++ {
		rows = append(rows, map[string]any{"id": i, "owner": i * 100})
	}
	return rows
}

// incPolicy: inc_t readable with owner masked by mask_func (no explicit
// value, so the mask is chosen from owner's declared type), inc_u readable.
// Every table in masked gets the same masked rule as inc_t.
func incPolicy(masked ...string) *auth.PolicyEvaluator {
	analyst := []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}}
	rules := []auth.PolicyRule{{
		ID: "u", EffectStr: "allow", Priority: 10, Subjects: analyst,
		Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "inc_u"}},
		Actions:   []auth.Action{auth.ActionRead},
	}}
	for _, tbl := range append([]string{"inc_t"}, masked...) {
		rules = append(rules, auth.PolicyRule{
			ID: tbl, EffectStr: "allow", Priority: 10, Subjects: analyst,
			Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: tbl}},
			Actions:     []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{{Type: "mask_column", Target: "owner", MaskFunc: "redact"}},
		})
	}
	return auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{Name: "inc", Version: 1, Enabled: true, Rules: rules}})
}

type incFixture struct {
	infra     tmdInfraT
	fast, dag *Coordinator
	provider  *auth.Provider
	ctx       context.Context
	logs      *syncBuf
	gen       int
}

func newIncFixture(t *testing.T, kvWrap func(catalog.MetaKV) catalog.MetaKV) *incFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	logs := &syncBuf{}
	infra := tmdInfra(t, ctx)
	if kvWrap != nil {
		infra.kv = kvWrap(infra.kv)
		infra.cat = catalog.New(infra.kv, infra.store, "test")
		if err := infra.cat.Init(ctx); err != nil {
			t.Fatal(err)
		}
	}
	infra.logger = slog.New(slog.NewTextHandler(io.MultiWriter(logs, os.Stderr),
		&slog.HandlerOptions{Level: slog.LevelWarn}))
	f := &incFixture{infra: infra, logs: logs}
	f.create(t, ctx, "inc_t", incSchemaA, incRowsA())
	f.create(t, ctx, "inc_u", parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}},
		[]map[string]any{{"id": int64(2)}, {"id": int64(3)}})
	f.fast = tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
	f.dag = tmdCoordinator(t, ctx, infra)
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "k", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	f.provider = auth.NewProvider(authn, authz, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := f.fast.SetAuthProvider(f.provider); err != nil {
		t.Fatal(err)
	}
	if err := f.dag.SetAuthProvider(f.provider); err != nil {
		t.Fatal(err)
	}
	f.provider.UpdateWithEvaluator(authn, authz, nil, incPolicy())
	if err := f.provider.BindError(); err != nil {
		t.Fatalf("the policy does not bind: %v", err)
	}
	id := &auth.Identity{Name: "analyst", Role: "analyst", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read"}}
	f.ctx = auth.ContextWithIdentity(ctx, id)
	return f
}

// create makes one incarnation of a table and writes its rows to files no
// other incarnation names (an engine write mints a fresh object name too).
func (f *incFixture) create(t *testing.T, ctx context.Context, name string, schema parquet.Schema, rows []map[string]any) {
	t.Helper()
	f.gen++
	if err := f.infra.cat.CreateTable(ctx, name, schema, nil); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if len(rows) == 0 {
		return
	}
	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("tables/%s/gen%d/chunk.parquet", name, f.gen)
	if _, err := f.infra.store.Put(ctx, "test", path, bytes.NewReader(buf.Bytes()),
		int64(buf.Len()), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := f.infra.cat.AddFiles(ctx, name, map[string]string{}, fmt.Sprintf("tables/%s/gen%d/", name, f.gen),
		[]catalog.FileEntry{{Path: path, SizeBytes: int64(buf.Len()), NumRows: int64(len(rows)), CreatedAt: time.Now()}}); err != nil {
		t.Fatal(err)
	}
}

// recreate drops inc_t and creates it again over schema B.
func (f *incFixture) recreate(t *testing.T) { f.recreateAs(t, incSchemaB) }

// recreateAs drops inc_t and creates it again over schema (B or C).
func (f *incFixture) recreateAs(t *testing.T, schema parquet.Schema) {
	t.Helper()
	ctx := context.Background()
	if err := f.infra.cat.DropTable(ctx, "inc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	rows := incRowsB()
	if len(schema.Columns) == 1 {
		for _, r := range rows {
			delete(r, "owner")
		}
	}
	f.create(t, ctx, "inc_t", schema, rows)
}

// restoreA puts inc_t back as a fresh incarnation over schema A.
func (f *incFixture) restoreA(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := f.infra.cat.DropTable(ctx, "inc_t"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	f.create(t, ctx, "inc_t", incSchemaA, incRowsA())
}

// atStep runs fn with action landing ONCE at the named statement step.
func atStep(step string, action func(), fn func()) (fired bool) {
	statementStepTestHook = func(s string) {
		if s == step && !fired {
			fired = true
			action()
		}
	}
	defer func() { statementStepTestHook = nil }()
	fn()
	return fired
}

// TestADecisionAboutAnotherIncarnationRefuses is Acceptance 7 on the
// coordinator: inc_t dropped and re-created between the statement's pin and
// an authorization decision refuses the statement with 40001 — in the first
// enforcement, and at a late decision (a scan the optimizer mints for a
// decorrelated IN subquery) — on both executions. Each cell first shows the
// statement's answer with nothing changed, and the answer the next statement
// gets once the table is re-created, so the cell is known to be a real race.
func TestADecisionAboutAnotherIncarnationRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newIncFixture(t, nil)
	cells := []struct {
		name, sql, step string
		to              parquet.Schema
	}{
		// Pinned by the scan annotation; EnforcePlanPolicies then builds the
		// security projection over the column list of the record it reads.
		// Re-created without owner, that projection publishes the pinned
		// files' rows without the column (a silent wrong answer at base).
		{"first_enforcement_projection", "SELECT * FROM inc_t", "annotated", incSchemaC},
		// The same step, owner re-created as a BIGINT: the mask is chosen
		// from the record's declared type ('0' instead of '***').
		{"first_enforcement_mask_type", "SELECT id, owner FROM inc_t", "annotated", incSchemaB},
		// The outer scan is decided at enforcement; the inner one is a LATE
		// decision, asked when the optimizer decorrelates the IN subquery:
		// its mask is chosen from the record it reads then.
		{"late_decorrelated_in", "SELECT id FROM inc_t WHERE id IN (SELECT id FROM inc_t WHERE owner = '***')", "enforced", incSchemaB},
	}
	arms := []struct {
		name string
		c    *Coordinator
	}{{"fastpath", f.fast}, {"dag", f.dag}}
	for _, cell := range cells {
		for _, arm := range arms {
			t.Run(cell.name+"/"+arm.name, func(t *testing.T) {
				want := pinAnswer(arm.c, f.ctx, cell.sql)
				if strings.HasPrefix(want, "ERROR") || want == "" {
					t.Fatalf("the answer with nothing changed is %q", want)
				}
				f.recreateAs(t, cell.to)
				next := pinAnswer(arm.c, f.ctx, cell.sql)
				f.restoreA(t)
				if next == want {
					t.Fatalf("the re-created table answers %q, the original %q: the cell is no race", next, want)
				}
				var got string
				var steps []string
				fired := false
				statementStepTestHook = func(s string) {
					steps = append(steps, s)
					if s == cell.step && !fired {
						fired = true
						f.recreateAs(t, cell.to)
					}
				}
				got = pinAnswer(arm.c, f.ctx, cell.sql)
				statementStepTestHook = nil
				f.restoreA(t)
				if !fired {
					t.Fatalf("step %q never ran", cell.step)
				}
				// Refused where enforcement is verified, before any plan is
				// physically planned or run.
				if last := steps[len(steps)-1]; last != "optimized" {
					t.Errorf("the refusal came after step %q, not right after enforcement (%v)", last, steps)
				}
				if !strings.HasPrefix(got, "ERROR 40001: ") {
					t.Fatalf("a decision about another incarnation answered\n   pinned table: %s\n"+
						"   re-created:   %s\n   got:          %s", want, next, got)
				}
				if wantMsg := `ERROR 40001: table "inc_t" was dropped or re-created under this statement; retry it`; got != wantMsg {
					t.Errorf("got %q, want %q", got, wantMsg)
				}
			})
		}
	}
}

// TestAnIncarnationRefusalAfterAFastPathBailOutIsFinal: a decision the fast
// path takes while it runs (the window key's IN subquery is planned when the
// local pipeline first evaluates it), then a bail-out over the result
// budget. The decision is verified before the statement is handed to the
// DAG, and the mismatch is reported: no stage plan is built, nothing is
// dispatched (C3).
func TestAnIncarnationRefusalAfterAFastPathBailOutIsFinal(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newIncFixture(t, nil)
	c := f.fast
	c.localResultBudgetOverride = 1 // every local result bails out to the DAG
	const sql = "SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM inc_t)) AS w FROM inc_u"

	tracked := func() int {
		c.tracker.mu.RLock()
		defer c.tracker.mu.RUnlock()
		return len(c.tracker.queries)
	}
	// With nothing changed the statement bails out and goes on to the stage
	// planner (which routes it back to a local pipeline, under the same
	// budget: what it answers then is not this gate's business).
	b0 := c.LocalFastPathBails()
	var baseSteps []string
	statementStepTestHook = func(s string) { baseSteps = append(baseSteps, s) }
	base := pinAnswer(c, f.ctx, sql)
	statementStepTestHook = nil
	if c.LocalFastPathBails() == b0 {
		t.Fatalf("the statement did not bail out of the fast path (%s): the cell tests nothing", base)
	}
	if baseSteps[len(baseSteps)-1] != "distributed" {
		t.Fatalf("with nothing changed the bail-out did not reach the stage planner: %v", baseSteps)
	}

	var steps []string
	statementStepTestHook = func(s string) {
		steps = append(steps, s)
		if s == "verified" {
			f.recreate(t) // after enforcement was verified, before the fast path runs
		}
	}
	q0, b1 := tracked(), c.LocalFastPathBails()
	f.logs.Reset()
	got := pinAnswer(c, f.ctx, sql)
	statementStepTestHook = nil
	f.restoreA(t)
	if !strings.HasPrefix(got, "ERROR 40001: ") {
		t.Fatalf("got %s, want the 40001 refusal", got)
	}
	if c.LocalFastPathBails() == b1 {
		t.Fatal("the refusal came before the bail-out: the cell no longer exercises it")
	}
	for _, s := range steps {
		if s == "distributed" {
			t.Errorf("the statement went on to the stage planner after the mismatch")
		}
	}
	if d := tracked() - q0; d != 0 {
		t.Errorf("%d DAG quer(ies) dispatched after the mismatch", d)
	}
	if strings.Contains(f.logs.String(), "routing to DAG") {
		t.Errorf("the mismatch was logged as routed to the DAG")
	}
}

// TestAReExecutionKeepsTheStatementsManifestPin is C4 for the ErrInputLost
// rerun, which re-enters ExecuteSQL with the statement's context: the rerun
// keeps the snapshot the statement pinned, so a table re-created since the
// pin is refused instead of answered from the other incarnation.
func TestAReExecutionKeepsTheStatementsManifestPin(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newIncFixture(t, nil)
	snap := physical.NewManifestSnapshot()
	ctx := dagplan.WithManifestSnapshot(f.ctx, snap)
	if _, err := snap.Get(ctx, f.infra.cat, "inc_t"); err != nil {
		t.Fatal(err)
	}
	f.recreate(t)
	defer f.restoreA(t)
	for _, arm := range []*Coordinator{f.fast, f.dag} {
		got := pinAnswer(arm, withStreamingExchangeDisabled(ctx), "SELECT id, owner FROM inc_t")
		if !strings.HasPrefix(got, "ERROR 40001: ") {
			t.Errorf("the re-execution answered %s: it took a second manifest pin", got)
		}
	}
}

// TestNoIncarnationRefusalWhenNothingChanged: the guard refuses nothing on a
// stable catalog — a never-written table, a table created before
// incarnations existed — and still refuses a legacy table that is dropped
// and re-created. A system relation has no incarnation and is not refused.
func TestNoIncarnationRefusalWhenNothingChanged(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newIncFixture(t, nil)
	ctx := context.Background()
	f.create(t, ctx, "inc_empty", incSchemaA, nil)

	// A legacy table: record and manifest without an incarnation, as written
	// before #919.
	f.create(t, ctx, "inc_legacy", incSchemaA, incRowsA())
	cid := f.infra.cat.ClusterID()
	for _, key := range []string{cid + ".table.inc_legacy", cid + ".manifest.inc_legacy"} {
		raw, _, err := f.infra.kv.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		delete(m, "incarnation")
		out, _ := json.Marshal(m)
		if _, err := f.infra.kv.Put(key, out); err != nil {
			t.Fatal(err)
		}
	}
	if inc, _ := f.infra.cat.TableIncarnation(ctx, "inc_legacy"); inc != "" {
		t.Fatalf("the legacy record still carries incarnation %q", inc)
	}
	authn, authz := f.provider.Authenticator(), f.provider.Authorizer()
	f.provider.UpdateWithEvaluator(authn, authz, nil, incPolicy("inc_legacy", "inc_empty"))
	if err := f.provider.BindError(); err != nil {
		t.Fatal(err)
	}

	stable := map[string]string{
		"SELECT id, owner FROM inc_empty":                                                        "",
		"SELECT id, owner FROM inc_legacy WHERE id = 1":                                          "id=1;owner=***;",
		"SELECT id, owner FROM inc_t WHERE id = 2":                                               "id=2;owner=***;",
		"SELECT id FROM inc_t WHERE id IN (SELECT id FROM inc_t WHERE owner = '***') AND id = 3": "id=3;",
	}
	for _, arm := range []struct {
		name string
		c    *Coordinator
	}{{"fastpath", f.fast}, {"dag", f.dag}} {
		for sql, want := range stable {
			if got := pinAnswer(arm.c, f.ctx, sql); got != want {
				t.Errorf("%s: %s answered %q, want %q", arm.name, sql, got, want)
			}
		}
		// A system relation has no table identity: it reads the catalog's
		// own records, not a manifest, and is answered while a table it
		// lists is re-created.
		var sys string
		atStep("enforced", func() { f.recreate(t) }, func() {
			sys = pinAnswer(arm.c, f.ctx, "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'inc_t'")
		})
		f.restoreA(t)
		if sys != "tablename=inc_t;" {
			t.Errorf("%s: the system relation answered %q", arm.name, sys)
		}

		// The legacy table, dropped and re-created between the pin and the
		// decision: the record now carries a minted incarnation and the
		// pinned manifest none.
		var got string
		fired := atStep("annotated", func() {
			if err := f.infra.cat.DropTable(ctx, "inc_legacy"); err != nil {
				t.Fatal(err)
			}
			f.create(t, ctx, "inc_legacy", incSchemaB, incRowsB())
		}, func() { got = pinAnswer(arm.c, f.ctx, "SELECT id, owner FROM inc_legacy") })
		if !fired || !strings.HasPrefix(got, "ERROR 40001: ") {
			t.Errorf("%s: a re-created legacy table answered %q", arm.name, got)
		}
		if err := f.infra.cat.DropTable(ctx, "inc_legacy"); err != nil {
			t.Fatal(err)
		}
		f.create(t, ctx, "inc_legacy", incSchemaA, incRowsA())
		break // the legacy shape is consumed (re-created with an incarnation)
	}
}

// countingKV counts the catalog reads a statement makes, by key family.
type countingKV struct {
	catalog.MetaKV
	mu     sync.Mutex
	counts map[string]int
}

func (k *countingKV) note(op, key string) {
	fam := key
	if i := strings.Index(key, "."); i >= 0 {
		fam = key[i+1:]
	}
	if j := strings.Index(fam, "."); j >= 0 {
		fam = fam[:j]
	}
	k.mu.Lock()
	k.counts[op+" "+fam]++
	k.mu.Unlock()
}

func (k *countingKV) Get(key string) ([]byte, uint64, error) {
	k.note("get", key)
	return k.MetaKV.Get(key)
}

// Generation forwards the NATS store's write generation (the system
// catalog's cache key), so the measured catalog behaves as the real one.
func (k *countingKV) Generation() (uint64, error) {
	return k.MetaKV.(catalog.GenerationReader).Generation()
}

func (k *countingKV) reset() map[string]int {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := k.counts
	k.counts = map[string]int{}
	return out
}

// TestIncarnationGuardCatalogReads MEASURES the catalog reads one simple
// policed SELECT makes (logged, not asserted beyond the guard's own bound).
func TestIncarnationGuardCatalogReads(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	var kv *countingKV
	f := newIncFixture(t, func(inner catalog.MetaKV) catalog.MetaKV {
		if _, ok := inner.(catalog.RevisionReader); ok {
			t.Fatal("the store now offers a revision probe: forward it so the count stays faithful")
		}
		kv = &countingKV{MetaKV: inner, counts: map[string]int{}}
		return kv
	})
	const sql = "SELECT id, owner FROM inc_t"
	for _, arm := range []struct {
		name string
		c    *Coordinator
	}{{"fastpath", f.fast}, {"dag", f.dag}} {
		pinAnswer(arm.c, f.ctx, sql) // warm
		kv.reset()
		if got := pinAnswer(arm.c, f.ctx, sql); strings.HasPrefix(got, "ERROR") {
			t.Fatal(got)
		}
		t.Logf("%s: catalog reads for %q: %v", arm.name, sql, kv.reset())
	}
}

// TestAnIncarnationRefusalOnARefusedPlanRoute: the stage planner refuses the
// plan (a subquery in a window key has no stage) and the statement is routed
// to the coordinator's local pipeline, which takes the subquery's decision
// when it first evaluates the key. inc_t re-created after enforcement was
// verified and before that decision: the statement's last verify refuses it.
func TestAnIncarnationRefusalOnARefusedPlanRoute(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newIncFixture(t, nil)
	c := f.dag
	const sql = "SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM inc_t)) AS w FROM inc_u"
	r0 := c.ScalarProjectionLocalRoutes()
	want := pinAnswer(c, f.ctx, sql)
	if strings.HasPrefix(want, "ERROR") || c.ScalarProjectionLocalRoutes() == r0 {
		t.Fatalf("with nothing changed the statement is not answered by the route: %s", want)
	}
	var got string
	fired := atStep("distributed", func() { f.recreate(t) }, func() { got = pinAnswer(c, f.ctx, sql) })
	f.restoreA(t)
	if !fired {
		t.Fatal("the statement never reached the stage planner")
	}
	if !strings.HasPrefix(got, "ERROR 40001: ") {
		t.Fatalf("a route's decision about another incarnation answered %s (nothing changed: %s)", got, want)
	}
}

// TestAnIncarnationRefusalBeforeTheDAGDispatches: the stage planner takes a
// decision of its own (the scalar subquery's plan); inc_t re-created after
// enforcement was verified and before that decision refuses the statement
// before any task is dispatched.
func TestAnIncarnationRefusalBeforeTheDAGDispatches(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	f := newIncFixture(t, nil)
	c := f.dag
	const sql = "SELECT id FROM inc_u WHERE id < (SELECT MAX(id) FROM inc_t)"
	tracked := func() int {
		c.tracker.mu.RLock()
		defer c.tracker.mu.RUnlock()
		return len(c.tracker.queries)
	}
	q0 := tracked()
	if want := pinAnswer(c, f.ctx, sql); want != "id=2; | id=3;" {
		t.Fatalf("with nothing changed: %s", want)
	}
	if tracked() == q0 {
		t.Fatal("with nothing changed nothing was dispatched: the cell tests nothing")
	}
	q1 := tracked()
	var got string
	fired := atStep("distributed", func() { f.recreate(t) }, func() { got = pinAnswer(c, f.ctx, sql) })
	f.restoreA(t)
	if !fired || !strings.HasPrefix(got, "ERROR 40001: ") {
		t.Fatalf("got %s, want the 40001 refusal", got)
	}
	if d := tracked() - q1; d != 0 {
		t.Errorf("%d DAG quer(ies) dispatched after the mismatch", d)
	}
}

// TestClassifyLocalFailureIncarnationRefusalIsFinal: the refusal is never
// retried on the DAG, whatever the strict setting (C3).
func TestClassifyLocalFailureIncarnationRefusalIsFinal(t *testing.T) {
	err := fmt.Errorf("local: %w", &physical.IncarnationChangedError{Table: "t"})
	for _, strict := range []bool{true, false} {
		if got := classifyLocalFailure(err, nil, strict); got != reportToClient {
			t.Errorf("strict=%v: the incarnation refusal was classified %v", strict, got)
		}
	}
}
