package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
)

// The door census for #931. An authentication configuration that could not be
// built used to report `Enabled() == false`, and every network frontend reads
// that as permission to serve a request with NO credentials: HTTP
// `ProviderMiddleware`, pgwire `authenticate`, gRPC `grpcAuthenticateContext`
// and MCP `resolveMCPAuth` all skip authentication on it.
//
// Each cell here drives a real door under the issue's configuration — auth
// enabled, JWT named, no secret and no key — and asserts the door refuses
// rather than serves.

func brokenAuthProvider(t *testing.T) *auth.Provider {
	t.Helper()
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		JWT:     auth.JWTConfig{Enabled: true}, // missing secret / public key
		Roles:   []auth.RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err == nil {
		t.Fatal("test setup: this configuration should not build")
	}
	return auth.NewProvider(authn, authz, nil, nil)
}

func TestPGWireRefusesUnderABrokenAuthConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	provider := brokenAuthProvider(t)
	db := envDB(t, ctx, provider)

	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: provider}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)

	// No password at all — under the defect the server never even asks.
	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet@%s/wadjet?sslmode=disable", pg.Addr()))
	if err == nil {
		defer conn.Close(ctx)
		var n int
		if qerr := conn.QueryRow(ctx, "SELECT count(*) FROM "+pmTable).Scan(&n); qerr == nil {
			t.Fatalf("pgwire served %d rows to a connection with no credentials", n)
		}
		t.Fatal("pgwire accepted a connection with no credentials")
	}
	t.Logf("pgwire refused: %v", err)

	// A credential is refused too: the failure is the configuration.
	conn2, err2 := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:any-token@%s/wadjet?sslmode=disable", pg.Addr()))
	if err2 == nil {
		conn2.Close(ctx)
		t.Fatal("pgwire accepted a credential under a broken auth configuration")
	}
}

func TestGRPCRefusesUnderABrokenAuthConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	provider := brokenAuthProvider(t)
	db := envDB(t, ctx, provider)
	client, stop := envGRPCClient(t, db, provider)
	defer stop()

	// No metadata at all.
	if _, err := client.ListTables(ctx, &wadjetv1.ListTablesRequest{}); err == nil {
		t.Fatal("gRPC served a call with no credentials under a broken auth configuration")
	} else if st, _ := status.FromError(err); st.Code() != codes.Unauthenticated {
		t.Fatalf("gRPC refusal code = %v, want Unauthenticated", st.Code())
	}

	// A bearer token: refused for the same reason.
	md := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer any-token")
	if _, err := client.ListTables(md, &wadjetv1.ListTablesRequest{}); err == nil {
		t.Fatal("gRPC accepted a credential under a broken auth configuration")
	}
}

func TestHTTPRefusesUnderABrokenAuthConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	provider := brokenAuthProvider(t)
	db := envDB(t, ctx, provider)

	srv := New(Config{Addr: ":0", Catalog: db.Catalog(), Provider: provider}, nil)
	hs := httptest.NewServer(srv.Mux())
	t.Cleanup(hs.Close)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hs.URL+"/v1/tables", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("HTTP status = %d, want 401 under a broken auth configuration", resp.StatusCode)
	}
}

// The other side: the same three doors still SERVE an authenticated request
// under a configuration that builds. A refusal that took working deployments
// down would be a worse defect than the one it fixes.
func TestTheDoorsStillServeAWorkingAuthConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	authn, authz, err := auth.Build(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	if err != nil {
		t.Fatalf("a working config was refused: %v", err)
	}
	provider := auth.NewProvider(authn, authz, nil, nil)
	db := envDB(t, ctx, provider)

	pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: provider}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)
	conn, cerr := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:analyst-key@%s/wadjet?sslmode=disable", pg.Addr()))
	if cerr != nil {
		t.Fatalf("pgwire refused a valid credential: %v", cerr)
	}
	defer conn.Close(ctx)
	var n int
	if qerr := conn.QueryRow(ctx, "SELECT count(*) FROM "+pmTable).Scan(&n); qerr != nil {
		t.Fatalf("pgwire refused an authorized read: %v", qerr)
	}

	client, stop := envGRPCClient(t, db, provider)
	defer stop()
	md := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer analyst-key")
	if _, gerr := client.ListTables(md, &wadjetv1.ListTablesRequest{}); gerr != nil {
		t.Fatalf("gRPC refused a valid credential: %v", gerr)
	}
}
