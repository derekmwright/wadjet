package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// alertAuthFixture opens an alerts-enabled DB whose ABAC policy applies a
// row filter (severity='high') and denies secret_col for role "analyst".
// The analyst role also holds "admin" so it can create alerts. Returns the DB
// and the analyst identity.
func alertAuthFixture(t *testing.T) (*DB, *auth.Identity) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test", EnableAlerts: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "severity", Type: parquet.TypeString},
		{Name: "secret_col", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("findings", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "severity": "high", "secret_col": "a"},
		{"id": int64(2), "severity": "low", "secret_col": "b"},
		{"id": int64(3), "severity": "high", "secret_col": "c"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "p", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "restrict-findings", EffectStr: "allow", Priority: 10,
			Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "findings"}},
			Actions:   []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{
				{Type: "deny_column", Target: "secret_col"},
				{Type: "row_filter", Value: "severity = 'high'"},
			},
		}},
	}})
	// A rule allowing alert_history reads so the alert's INSERT-less query and
	// history table checks aren't blocked for the analyst (not exercised here,
	// but keeps the fixture realistic).
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"admin", "read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)
	db.SetAuthProvider(provider)

	return db, &auth.Identity{Name: "analyst", Role: "analyst", Method: "apikey", Perms: []string{"admin", "read"}}
}

// TestAlertDDLRequiresAdmin: CREATE/DROP/ALTER ALERT must be rejected without
// admin, accepted with it. Before the gate, any caller (or an unauthenticated
// MCP session) could persist a server-side job.
func TestAlertDDLRequiresAdmin(t *testing.T) {
	db, admin := alertAuthFixture(t)
	ctx := context.Background()
	const create = `CREATE ALERT a1 AS SELECT id FROM findings EVERY 24 HOURS WEBHOOK 'https://x'`

	t.Run("no identity rejected", func(t *testing.T) {
		if _, err := db.Query(ctx, create); err == nil {
			t.Fatal("expected rejection with no identity")
		}
	})

	t.Run("reader rejected", func(t *testing.T) {
		readerCtx := auth.ContextWithIdentity(ctx, &auth.Identity{Name: "bob", Role: "reader", Perms: []string{"read"}})
		if _, err := db.Query(readerCtx, create); err == nil {
			t.Fatal("expected rejection for a non-admin identity")
		}
	})

	t.Run("admin accepted", func(t *testing.T) {
		adminCtx := auth.ContextWithIdentity(ctx, admin)
		if _, err := db.Query(adminCtx, create); err != nil {
			t.Fatalf("admin CREATE ALERT should succeed, got %v", err)
		}
		// DROP also requires admin: reader rejected, admin ok.
		readerCtx := auth.ContextWithIdentity(ctx, &auth.Identity{Name: "bob", Role: "reader", Perms: []string{"read"}})
		if _, err := db.Query(readerCtx, `DROP ALERT a1`); err == nil {
			t.Fatal("expected DROP ALERT rejection for non-admin")
		}
		if _, err := db.Query(adminCtx, `DROP ALERT a1`); err != nil {
			t.Fatalf("admin DROP ALERT should succeed, got %v", err)
		}
	})
}

// rowCount runs the alert executor path (what the scheduler uses) and returns
// the visible row count and whether a column was present in the result schema.
func alertQuery(t *testing.T, db *DB, ctx context.Context, sql string) (int, map[string]bool, error) {
	t.Helper()
	ex := &dbExecutor{db: db}
	rows, schema, _, _, err := ex.Query(ctx, sql, 100)
	cols := make(map[string]bool, len(schema))
	for _, c := range schema {
		cols[c.Name] = true
	}
	return len(rows), cols, err
}

// TestAlertRunsUnderCreatorIdentity: the scheduler's eval decorator makes an
// alert query enforce its creator's ABAC (row filter + column deny). A bare
// context (no decorator) runs unfiltered — the exact pre-fix behavior — so the
// contrast proves the decorator is what carries the security.
func TestAlertRunsUnderCreatorIdentity(t *testing.T) {
	db, admin := alertAuthFixture(t)
	ctx := context.Background()
	adminCtx := auth.ContextWithIdentity(ctx, admin)

	const create = `CREATE ALERT a1 AS SELECT * FROM findings EVERY 24 HOURS WEBHOOK 'https://x'`
	if _, err := db.Query(adminCtx, create); err != nil {
		t.Fatalf("CREATE ALERT: %v", err)
	}
	m, err := db.catalog.GetAlert(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	if m.CreatedByRole != "analyst" {
		t.Fatalf("alert did not persist creator role: %+v", m)
	}

	decorate := db.alertEvalDecorator()

	// Definer's-rights: decorated context enforces the analyst policy.
	n, cols, err := alertQuery(t, db, decorate(ctx, *m), m.QueryText)
	if err != nil {
		t.Fatalf("decorated alert query failed: %v", err)
	}
	if n != 2 {
		t.Errorf("row filter severity='high' not applied under creator identity: got %d rows, want 2", n)
	}
	if cols["secret_col"] {
		t.Errorf("denied column secret_col leaked under creator identity")
	}

	// Contrast: without the decorator (bare context, no identity) ABAC
	// fail-opens — this is exactly the pre-fix unfiltered behavior.
	nBare, colsBare, err := alertQuery(t, db, ctx, m.QueryText)
	if err != nil {
		t.Fatalf("bare alert query failed: %v", err)
	}
	if nBare != 3 || !colsBare["secret_col"] {
		t.Fatalf("expected unfiltered result on a bare context (3 rows, secret_col present); got %d rows, cols=%v", nBare, colsBare)
	}
}

// TestLegacyAlertFailsClosed: an alert persisted before creator identity was
// captured (no CreatedByRole) must NOT run unfiltered under enabled auth. The
// decorator stamps a role-less identity → ABAC default-deny → the query is
// rejected rather than returning unfiltered rows.
func TestLegacyAlertFailsClosed(t *testing.T) {
	db, _ := alertAuthFixture(t)
	ctx := context.Background()

	legacy := catalog.AlertMeta{
		Name:            "legacy",
		QueryText:       "SELECT * FROM findings",
		IntervalSeconds: 86400,
		Enabled:         true,
		// No CreatedBy* identity fields — pre-definer-rights alert.
	}
	if err := db.catalog.CreateAlert(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	m, err := db.catalog.GetAlert(ctx, "legacy")
	if err != nil {
		t.Fatal(err)
	}

	decorate := db.alertEvalDecorator()
	n, _, err := alertQuery(t, db, decorate(ctx, *m), m.QueryText)
	if err == nil {
		t.Fatalf("legacy alert ran unfiltered (got %d rows) — must fail closed under enabled auth", n)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "denied") {
		t.Fatalf("expected an access-denied error for a legacy alert, got %v", err)
	}
}
