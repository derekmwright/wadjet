// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ADR-0050 §3 as amended 2026-11-19: the table-incarnation check runs for
// EVERY coordinator statement, with an authorization provider enabled,
// disabled or absent. The relation a statement reads is a correctness
// invariant, not an authorization feature: without a provider a statement
// racing a drop and re-create of inc_t answered the pinned incarnation's
// rows under the re-created record's column list (owner = NULL, or no owner
// column at all). Every cell runs in the three configurations and on both
// executions; the enabled configuration keeps slice 2's refusals.

// addIncV creates the third table of the three-table cells, readable under
// the enabled configuration's policy too.
func (f *incFixture) addIncV(t *testing.T) {
	t.Helper()
	f.create(t, context.Background(), "inc_v", parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}},
		[]map[string]any{{"id": int64(2)}, {"id": int64(3)}, {"id": int64(4)}})
	if f.provider == nil || !f.provider.Enabled() {
		return
	}
	// incPolicy's rules, and inc_v readable.
	analyst := []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}}
	readable := func(id, tbl string, obl ...auth.Obligation) auth.PolicyRule {
		return auth.PolicyRule{
			ID: id, EffectStr: "allow", Priority: 10, Subjects: analyst,
			Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: tbl}},
			Actions:     []auth.Action{auth.ActionRead},
			Obligations: obl,
		}
	}
	rules := []auth.PolicyRule{
		readable("u", "inc_u"),
		readable("v", "inc_v"),
		readable("inc_t", "inc_t", auth.Obligation{Type: "mask_column", Target: "owner", MaskFunc: "redact"}),
	}
	f.provider.UpdateWithEvaluator(f.provider.Authenticator(), f.provider.Authorizer(), nil,
		auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{Name: "inc", Version: 1, Enabled: true, Rules: rules}}))
	if err := f.provider.BindError(); err != nil {
		t.Fatal(err)
	}
}

// manifestKeysKV is countingKV that also records each manifest key read.
type manifestKeysKV struct {
	*countingKV
	mu   sync.Mutex
	keys []string
}

func (k *manifestKeysKV) Get(key string) ([]byte, uint64, error) {
	if strings.Contains(key, ".manifest.") {
		k.mu.Lock()
		k.keys = append(k.keys, key)
		k.mu.Unlock()
	}
	return k.countingKV.Get(key)
}

func (k *manifestKeysKV) resetKeys() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := k.keys
	k.keys = nil
	return out
}

const incRefusal = `ERROR 40001: table "inc_t" was dropped or re-created under this statement; retry it`

// TestEveryStatementRefusesAnotherIncarnation: inc_t dropped and re-created
// (without owner) at a statement step, after the statement first read its
// record, refuses 40001 in every configuration.
func TestEveryStatementRefusesAnotherIncarnation(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	cells := []struct {
		name, sql, step string
	}{
		// After the scan annotation pinned the manifest: the star and the
		// scan's declared schema are read from the re-created record. With
		// no provider, at base: `id=1..4` without owner on the fast path.
		{"first_planning_star", "SELECT * FROM inc_t", "annotated"},
		// The same step, named columns: owner=<nil> for the pinned rows.
		{"first_planning_columns", "SELECT id, owner FROM inc_t", "annotated"},
		// After the binder read the record and before anything pinned the
		// manifest: the binder's column list over the re-created
		// incarnation's files (owner=<nil> for ids 11..13 at base).
		{"binder_columns", "SELECT id, owner FROM inc_t", "validated"},
		// After every pre-physical verify: the physical planner's own read.
		{"physical_planning_columns", "SELECT id, owner FROM inc_t", "verified"},
		// Three tables; inc_t is one of them.
		{"three_table_join", "SELECT a.id, a.owner, b.id AS bid FROM inc_t a JOIN inc_u b ON a.id = b.id JOIN inc_v c ON c.id = a.id", "verified"},
		// inc_t is a scan only after the optimizer decorrelates the IN
		// subquery: re-created before the optimizer runs.
		{"decorrelated_only_before_optimize", "SELECT id FROM inc_u WHERE id IN (SELECT id FROM inc_t WHERE owner <> 'o-2')", "enforced"},
		// ... and after it ran, before physical planning.
		{"decorrelated_only_after_optimize", "SELECT id FROM inc_u WHERE id IN (SELECT id FROM inc_t WHERE owner <> 'o-2')", "optimized"},
	}
	for _, mode := range incProviderModes {
		t.Run(mode, func(t *testing.T) {
			f := newIncFixtureMode(t, nil, mode)
			f.addIncV(t)
			for _, cell := range cells {
				for _, arm := range []struct {
					name string
					c    *Coordinator
				}{{"fastpath", f.fast}, {"dag", f.dag}} {
					t.Run(cell.name+"/"+arm.name, func(t *testing.T) {
						want := pinAnswer(arm.c, f.ctx, cell.sql)
						if strings.HasPrefix(want, "ERROR") || want == "" {
							t.Fatalf("the answer with nothing changed is %q", want)
						}
						var got string
						fired := atStep(cell.step, func() { f.recreateAs(t, incSchemaC) },
							func() { got = pinAnswer(arm.c, f.ctx, cell.sql) })
						f.restoreA(t)
						if !fired {
							t.Fatalf("step %q never ran", cell.step)
						}
						if got != incRefusal {
							t.Fatalf("a statement racing a re-create of inc_t answered\n   nothing changed: %s\n   got:             %s", want, got)
						}
						if again := pinAnswer(arm.c, f.ctx, cell.sql); again != want {
							t.Errorf("after the refusal the statement answers %q, want %q", again, want)
						}
					})
				}
			}
		})
	}
}

// TestEveryStatementRefusesAfterAFastPathBailOut: the window key's IN
// subquery reads inc_t's record when the local pipeline first evaluates it;
// the local result then bails out over its budget. The mismatch is reported
// before the statement is handed to the stage DAG: no stage plan, no
// dispatch.
func TestEveryStatementRefusesAfterAFastPathBailOut(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	const sql = "SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM inc_t WHERE id < 3)) AS w FROM inc_u"
	for _, mode := range incProviderModes {
		t.Run(mode, func(t *testing.T) {
			f := newIncFixtureMode(t, nil, mode)
			c := f.fast
			c.localResultBudgetOverride = 1
			tracked := func() int {
				c.tracker.mu.RLock()
				defer c.tracker.mu.RUnlock()
				return len(c.tracker.queries)
			}
			b0 := c.LocalFastPathBails()
			var baseSteps []string
			statementStepTestHook = func(s string) { baseSteps = append(baseSteps, s) }
			base := pinAnswer(c, f.ctx, sql)
			statementStepTestHook = nil
			if c.LocalFastPathBails() == b0 || baseSteps[len(baseSteps)-1] != "distributed" {
				t.Fatalf("with nothing changed the statement did not bail out to the stage planner (%s, %v)", base, baseSteps)
			}
			var steps []string
			statementStepTestHook = func(s string) {
				steps = append(steps, s)
				if s == "verified" {
					f.recreateAs(t, incSchemaC)
				}
			}
			q0, b1 := tracked(), c.LocalFastPathBails()
			got := pinAnswer(c, f.ctx, sql)
			statementStepTestHook = nil
			f.restoreA(t)
			if got != incRefusal {
				t.Fatalf("got %s, want the refusal (nothing changed: %s)", got, base)
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
		})
	}
}

// TestEveryStatementRefusesOnARefusedPlanRoute: the stage planner refuses
// the plan (a subquery in a window key has no stage) and the statement is
// routed to the local pipeline, which reads inc_t's record when it first
// evaluates the key. Re-created before that read: refused.
func TestEveryStatementRefusesOnARefusedPlanRoute(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	const sql = "SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM inc_t WHERE id < 3)) AS w FROM inc_u"
	for _, mode := range incProviderModes {
		t.Run(mode, func(t *testing.T) {
			f := newIncFixtureMode(t, nil, mode)
			c := f.dag
			r0 := c.ScalarProjectionLocalRoutes()
			want := pinAnswer(c, f.ctx, sql)
			if want != "id=2;w=1; | id=3;w=1;" || c.ScalarProjectionLocalRoutes() == r0 {
				t.Fatalf("with nothing changed the statement is not answered by the route: %s", want)
			}
			r1 := c.ScalarProjectionLocalRoutes()
			var got string
			fired := atStep("distributed", func() { f.recreateAs(t, incSchemaC) }, func() { got = pinAnswer(c, f.ctx, sql) })
			f.restoreA(t)
			if !fired {
				t.Fatal("the statement never reached the stage planner")
			}
			if got != incRefusal {
				t.Fatalf("a route over another incarnation answered %s (nothing changed: %s)", got, want)
			}
			if c.ScalarProjectionLocalRoutes() == r1 {
				t.Error("the refusal came before the route: the cell no longer exercises it")
			}
		})
	}
}

// TestNoIncarnationRefusalWithoutAProvider: on a stable catalog nothing is
// refused without a provider or under a disabled one: a table read twice, a
// legacy table (no incarnation on its record or manifest), a table created
// and never written, a system relation (no manifest, no incarnation: not
// refused, and it pins nothing: its listing reads no manifest). A legacy
// table dropped and re-created is refused.
func TestNoIncarnationRefusalWithoutAProvider(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	for _, mode := range []string{incProviderNone, incProviderDisabled} {
		t.Run(mode, func(t *testing.T) {
			var kv *manifestKeysKV
			f := newIncFixtureMode(t, func(inner catalog.MetaKV) catalog.MetaKV {
				kv = &manifestKeysKV{countingKV: &countingKV{MetaKV: inner, counts: map[string]int{}}}
				return kv
			}, mode)
			ctx := context.Background()
			f.create(t, ctx, "inc_empty", incSchemaA, nil)
			legacy := func() {
				t.Helper()
				if _, err := f.infra.cat.GetTable(ctx, "inc_legacy"); err == nil {
					if err := f.infra.cat.DropTable(ctx, "inc_legacy"); err != nil {
						t.Fatal(err)
					}
				}
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
			}
			stable := map[string]string{
				"SELECT id, owner FROM inc_legacy WHERE id = 1":                                                     "id=1;owner=o-1;",
				"SELECT id, owner FROM inc_t WHERE id = 2":                                                          "id=2;owner=o-2;",
				"SELECT id FROM inc_t WHERE id IN (SELECT id FROM inc_t WHERE owner = 'o-3') AND id = 3":            "id=3;",
				"SELECT * FROM inc_t a JOIN inc_u b ON a.id = b.id WHERE a.id = 2":                                  "id=2;owner=o-2;",
				"SELECT id, COUNT(*) OVER (PARTITION BY id IN (SELECT id FROM inc_t WHERE id < 3)) AS w FROM inc_u": "id=2;w=1; | id=3;w=1;",
			}
			const emptySQL = "SELECT id, owner FROM inc_empty"
			const sysSQL = "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'inc_t'"
			const colsSQL = "SELECT column_name FROM information_schema.columns WHERE table_name = 'inc_t'"
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"fastpath", f.fast}, {"dag", f.dag}} {
				legacy()
				for sql, want := range stable {
					if got := pinAnswer(arm.c, f.ctx, sql); got != want {
						t.Errorf("%s: %s answered %q, want %q", arm.name, sql, got, want)
					}
				}
				// The never-written table answers as it does at base: no rows
				// on the fast path, the stage DAG's uncoded no-ScanFiles error
				// (recorded in TestNoIncarnationRefusalWhenNothingChanged).
				got := pinAnswer(arm.c, f.ctx, emptySQL)
				switch arm.name {
				case "fastpath":
					if got != "" {
						t.Errorf("%s: %s answered %q, want no rows", arm.name, emptySQL, got)
					}
				case "dag":
					if !strings.HasPrefix(got, "ERROR : ") || !strings.Contains(got, "has no dependencies and no ScanFiles") {
						t.Errorf("%s: %s answered %q, want the base DAG's uncoded no-ScanFiles error", arm.name, emptySQL, got)
					}
				}
				// The system relation, answered while a table it lists is
				// re-created, and reading no manifest at all.
				var sys string
				atStep("enforced", func() { f.recreate(t) }, func() { sys = pinAnswer(arm.c, f.ctx, sysSQL) })
				f.restoreA(t)
				if sys != "tablename=inc_t;" {
					t.Errorf("%s: the system relation answered %q", arm.name, sys)
				}
				// A listing that reads every table's RECORD (the columns of
				// each) reads no table's manifest: it pins nothing. Each is
				// measured cold (a catalog write first, so the system
				// catalog's generation-keyed cache reads every record again)
				// and on an otherwise stable catalog.
				for i, q := range []struct{ sql, want string }{
					{sysSQL, "tablename=inc_t;"},
					{colsSQL, "column_name=id; | column_name=owner;"},
				} {
					bump := fmt.Sprintf("inc_bump_%s_%d", arm.name, i)
					if err := f.infra.cat.CreateTable(ctx, bump, incSchemaC, nil); err != nil {
						t.Fatal(err)
					}
					kv.resetKeys()
					got := pinAnswer(arm.c, f.ctx, q.sql)
					keys := kv.resetKeys()
					if got != q.want {
						t.Errorf("%s: %s answered %q, want %q", arm.name, q.sql, got, q.want)
					}
					// The fast path's scan-size estimate asks for the system
					// relation's own manifest (there is none), as it does at
					// base; no listed table's manifest is read.
					for _, k := range keys {
						if strings.Contains(k, ".manifest.inc_") {
							t.Errorf("%s: %s read %s: its listing pinned a table (%v)", arm.name, q.sql, k, keys)
						}
					}
				}
				var cols string
				atStep("enforced", func() { f.recreate(t) }, func() { cols = pinAnswer(arm.c, f.ctx, colsSQL) })
				f.restoreA(t)
				if strings.HasPrefix(cols, "ERROR") {
					t.Errorf("%s: the column listing raced by a re-create answered %q", arm.name, cols)
				}
				// The legacy table dropped and re-created under the statement.
				got = ""
				fired := atStep("annotated", func() {
					if err := f.infra.cat.DropTable(ctx, "inc_legacy"); err != nil {
						t.Fatal(err)
					}
					f.create(t, ctx, "inc_legacy", incSchemaB, incRowsB())
				}, func() { got = pinAnswer(arm.c, f.ctx, "SELECT id, owner FROM inc_legacy") })
				if !fired || !strings.HasPrefix(got, "ERROR 40001: ") {
					t.Errorf("%s: a re-created legacy table answered %q", arm.name, got)
				}
			}
		})
	}
}

// TestIncarnationCheckCatalogReadsEveryConfiguration MEASURES the catalog
// reads one statement makes in each configuration (logged, not asserted):
// one table, and a three-table join.
func TestIncarnationCheckCatalogReadsEveryConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands up an embedded NATS cluster")
	}
	for _, mode := range incProviderModes {
		var kv *countingKV
		f := newIncFixtureMode(t, func(inner catalog.MetaKV) catalog.MetaKV {
			kv = &countingKV{MetaKV: inner, counts: map[string]int{}}
			return kv
		}, mode)
		f.addIncV(t)
		for _, sql := range []string{
			"SELECT id, owner FROM inc_t",
			"SELECT a.id, a.owner FROM inc_t a JOIN inc_u b ON a.id = b.id JOIN inc_v c ON c.id = a.id",
		} {
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"fastpath", f.fast}, {"dag", f.dag}} {
				pinAnswer(arm.c, f.ctx, sql) // warm
				kv.reset()
				if got := pinAnswer(arm.c, f.ctx, sql); strings.HasPrefix(got, "ERROR") {
					t.Fatal(got)
				}
				r := kv.reset()
				t.Logf("READS %s/%s %q: manifest=%d table=%d meta=%d all=%v", mode, arm.name, sql,
					r["get manifest"], r["get table"], r["get meta"], r)
			}
		}
	}
}
