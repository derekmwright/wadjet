package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/wadjet"
)

// The legacy-shape fail-open (found by SEC2's door census).
//
// A provider built from `roles:` alone — `auth.NewProvider(authn, authz, nil,
// nil)` with no ABAC evaluator, which is what an embedded caller of
// `SetAuthProvider` and several server shapes produce — installed no
// evaluator, and both shared enforcement paths returned early on
// `provider.Evaluator() == nil`. So NOTHING was enforced on that shape: a role
// scoped to one table could read another, and a role allowed only `read` could
// DELETE.
//
// The legacy rule was never gone; it was simply not asked. Both paths ask it
// now, through the one shared decision (`auth.TableAccess`), so the answer a
// metadata door gives and the answer the data path gives cannot differ.

// legacyProvider has NO evaluator: `reader` may read `e7emp` only, and
// `stranger`'s role lists a different table entirely.
func legacyRoleProvider(t *testing.T) *auth.Provider {
	t.Helper()
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader", Role: "reader"},
			{Key: "stranger-key", Name: "stranger", Role: "stranger"},
			{Key: "writer-key", Name: "writer", Role: "writer"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{pmTable}, Allow: []string{"read"}},
			{Name: "stranger", Tables: []string{"somewhere_else"}, Allow: []string{"read", "write"}},
			{Name: "writer", Tables: []string{pmTable}, Allow: []string{"read", "write"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := auth.NewProvider(authn, authz, nil, nil)
	if p.Evaluator() != nil {
		t.Fatal("test setup: this provider must have NO ABAC evaluator")
	}
	return p
}

func legacyIdentityCtx(t *testing.T, ctx context.Context, p *auth.Provider, key string) context.Context {
	t.Helper()
	id, err := p.Authenticator().AuthenticateToken(key)
	if err != nil {
		t.Fatalf("authenticate %q: %v", key, err)
	}
	return auth.ContextWithIdentity(ctx, id)
}

func legacyRig(t *testing.T, ctx context.Context) (*wadjet.DB, *auth.Provider, string) {
	t.Helper()
	p := legacyRoleProvider(t)
	db := pmEmbeddedDB(t, ctx, 0)
	if err := db.SetAuthProvider(p); err != nil {
		t.Fatal(err)
	}
	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: p}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)
	return db, p, pg.Addr()
}

// TestALegacyRoleShapeStillAuthorizesReads — a role whose `tables:` list does
// not name the relation may not read it, on the embedded door and on pgwire.
func TestALegacyRoleShapeStillAuthorizesReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db, p, addr := legacyRig(t, ctx)
	sql := "SELECT id FROM " + pmTable

	// embedded: allowed for the role that lists the table.
	if _, err := db.Query(legacyIdentityCtx(t, ctx, p, "reader-key"), sql); err != nil {
		t.Fatalf("embedded: the authorized read was refused: %v", err)
	}
	// embedded: refused for the role that does not.
	if _, err := db.Query(legacyIdentityCtx(t, ctx, p, "stranger-key"), sql); err == nil {
		t.Fatal("embedded: a role whose tables list does not name the relation READ it")
	} else if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("embedded: refusal is not an authorization refusal: %v", err)
	}

	// pgwire, both sides.
	read := func(key string) error {
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://wadjet:%s@%s/wadjet?sslmode=disable", key, addr))
		if err != nil {
			return err
		}
		defer conn.Close(ctx)
		var n int
		return conn.QueryRow(ctx, "SELECT count(*) FROM "+pmTable).Scan(&n)
	}
	if err := read("reader-key"); err != nil {
		t.Fatalf("pgwire: the authorized read was refused: %v", err)
	}
	if err := read("stranger-key"); err == nil {
		t.Fatal("pgwire: a role whose tables list does not name the relation READ it")
	}
}

// TestALegacyRoleShapeStillAuthorizesWrites — a role allowed only `read` may
// not DELETE, and a role that does not list the relation may not write it.
func TestALegacyRoleShapeStillAuthorizesWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db, p, addr := legacyRig(t, ctx)
	del := "DELETE FROM " + pmTable + " WHERE id = 1"

	// The read-only role may not delete.
	if _, err := db.Execute(legacyIdentityCtx(t, ctx, p, "reader-key"), del); err == nil {
		t.Fatal("embedded: a role allowed only `read` DELETEd rows")
	} else if !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("embedded: refusal is not an authorization refusal: %v", err)
	}
	// Nothing was destroyed: the boundary is a claim about SIDE EFFECTS.
	res, err := db.Query(legacyIdentityCtx(t, ctx, p, "writer-key"),
		"SELECT count(*) AS n FROM "+pmTable+" WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(res.Rows[0]["n"]); got != "1" {
		t.Fatalf("the refused DELETE still removed the row (count = %s)", got)
	}

	// The role that does list the relation with `write` still works.
	if _, err := db.Execute(legacyIdentityCtx(t, ctx, p, "writer-key"), del); err != nil {
		t.Fatalf("embedded: the authorized DELETE was refused: %v", err)
	}

	// pgwire says the same.
	conn, cerr := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:reader-key@%s/wadjet?sslmode=disable", addr))
	if cerr != nil {
		t.Fatal(cerr)
	}
	defer conn.Close(ctx)
	if _, qerr := conn.Exec(ctx, "DELETE FROM "+pmTable+" WHERE id = 2"); qerr == nil {
		t.Fatal("pgwire: a role allowed only `read` DELETEd rows")
	}
}

// Metadata and data agree on the legacy shape: what `auth.VisibleTables`
// lists is what the data path lets the identity read.
func TestTheLegacyShapeAgreesBetweenMetadataAndData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db, p, _ := legacyRig(t, ctx)

	for _, tc := range []struct {
		key     string
		visible bool
	}{
		{"reader-key", true},
		{"stranger-key", false},
	} {
		idCtx := legacyIdentityCtx(t, ctx, p, tc.key)
		listed := auth.VisibleTables(idCtx, p, []string{pmTable})
		_, readErr := db.Query(idCtx, "SELECT id FROM "+pmTable)
		if got := len(listed) == 1; got != tc.visible {
			t.Errorf("%s: VisibleTables says visible=%v, want %v", tc.key, got, tc.visible)
		}
		if (readErr == nil) != tc.visible {
			t.Errorf("%s: the data path says readable=%v while the listing says %v",
				tc.key, readErr == nil, tc.visible)
		}
	}
}

// The no-auth shape is untouched: no provider means nothing is enforced.
func TestTheLegacyShapeChangesNothingWithoutAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db := pmEmbeddedDB(t, ctx, 0)
	if _, err := db.Query(ctx, "SELECT id FROM "+pmTable); err != nil {
		t.Fatalf("a database with no auth provider refused a read: %v", err)
	}
	if _, err := db.Execute(ctx, "DELETE FROM "+pmTable+" WHERE id = 3"); err != nil {
		t.Fatalf("a database with no auth provider refused a write: %v", err)
	}
}
