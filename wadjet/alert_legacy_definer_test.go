package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A scheduled alert runs under its creator on a `roles:`-only deployment.
//
// The definer's identity is rebuilt from a stored snapshot, and the snapshot
// carries no `Perms` or `Tables` — those are pure configuration, resolved from
// the ROLE. When the data paths began asking the role's `allow` list, the
// rebuilt definer held nothing at all and every alert on such a deployment
// stopped running. `StampDefiner` re-resolves the grants from the current role
// definitions now, which also means a role narrowed since the alert was
// created takes effect on the alert's next tick.

func legacyDefinerDB(t *testing.T, allow []string) (*DB, *auth.Provider) {
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
	}}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("findings", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "severity": "high"},
		{"id": int64(2), "severity": "low"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// NO evaluator: this is what `buildProviderFromConfig` produces for a YAML
	// carrying `roles:` and no `abac_policies:`.
	authn, authz, berr := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "ops-key", Name: "ops", Role: "ops"}},
		Roles:   []auth.RoleConfig{{Name: "ops", Tables: []string{"*"}, Allow: allow}},
	})
	if berr != nil {
		t.Fatal(berr)
	}
	p := auth.NewProvider(authn, authz, nil, nil)
	if p.Evaluator() != nil {
		t.Fatal("test setup: this provider must have NO ABAC evaluator")
	}
	if err := db.SetAuthProvider(p); err != nil {
		t.Fatal(err)
	}
	return db, p
}

// definerQuery runs a query the way the alert scheduler does: the creator's
// snapshot, replayed through StampDefiner.
func definerQuery(t *testing.T, db *DB, p *auth.Provider, snap auth.IdentitySnapshot,
	sql string) (int, error) {
	t.Helper()
	ctx, _ := auth.StampDefiner(context.Background(), p, snap)
	res, err := db.Query(ctx, sql)
	if err != nil {
		return 0, err
	}
	return len(res.Rows), nil
}

func TestALegacyDeploymentsAlertRunsUnderItsCreator(t *testing.T) {
	db, p := legacyDefinerDB(t, []string{"read", "write", "admin"})
	snap := auth.IdentitySnapshot{Name: "ops", Role: "ops", Method: "apikey"}

	n, err := definerQuery(t, db, p, snap, "SELECT id FROM findings")
	if err != nil {
		t.Fatalf("the alert's own creator was refused its query: %v", err)
	}
	if n != 2 {
		t.Fatalf("the alert query returned %d rows, want 2", n)
	}
}

func TestADefinerWhoseRoleLostWriteIsRefusedTheWrite(t *testing.T) {
	// The same creator, on a deployment whose `ops` role has since been
	// narrowed to read.
	db, p := legacyDefinerDB(t, []string{"read"})
	snap := auth.IdentitySnapshot{Name: "ops", Role: "ops", Method: "apikey"}

	if _, err := definerQuery(t, db, p, snap, "SELECT id FROM findings"); err != nil {
		t.Fatalf("the narrowed definer lost the read it still holds: %v", err)
	}
	ctx, _ := auth.StampDefiner(context.Background(), p, snap)
	if _, err := db.Execute(ctx, "DELETE FROM findings WHERE id = 1"); err == nil {
		t.Fatal("a definer whose role lost `write` still writes")
	} else if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("refusal is not an authorization refusal: %v", err)
	}
}

// A definer whose role no longer exists holds nothing.
func TestADefinerWhoseRoleWasRemovedIsRefused(t *testing.T) {
	db, p := legacyDefinerDB(t, []string{"read", "write", "admin"})
	gone := auth.IdentitySnapshot{Name: "retired", Role: "retired", Method: "apikey"}
	if _, err := definerQuery(t, db, p, gone, "SELECT id FROM findings"); err == nil {
		t.Fatal("a definer whose role the configuration no longer defines still reads")
	}
}
