package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/wadjet"
)

// The environment door census (#933).
//
// `env.source_ip`, `env.time` and `env.hour` are documented condition
// attributes, and until this arc NO door published any of them: every shared
// enforcement path built `Environment{Protocol: protocol}` by hand, with no
// clock and no address, and the one site that did pass an address passed
// `r.RemoteAddr` — `host:port` — which a rule written as an IP cannot match.
// An environment-conditioned DENY therefore matched nothing, and beside the
// broad allow every `roles:`-to-ABAC migration emits, a deny that matches
// nothing is a grant.
//
// Each cell below drives a REAL door with an authenticated identity that a
// broad allow grants and an environment deny takes away.

// envDenyProvider builds a provider whose policy set is a broad allow for
// `analyst` plus one deny conditioned on `attr`.
func envDenyProvider(t *testing.T, attr, op string, value any, actions []auth.Action) *auth.Provider {
	t.Helper()
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read", "write"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "env-gate", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{ID: "broad-allow", EffectStr: "allow", Priority: 100,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite, auth.ActionDescribe}},
			{ID: "env-deny", EffectStr: "deny", Priority: 10,
				Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:     actions,
				Environment: []auth.Condition{{Attribute: attr, Op: op, Value: value}}},
		},
	}}))
	return p
}

// envAllowOnlyProvider is the CONTROL arm: the same broad allow with a deny
// that cannot match (a source address nothing arrives from). Every cell has to
// succeed here, or "refused" proves nothing about the environment.
func envAllowOnlyProvider(t *testing.T) *auth.Provider {
	t.Helper()
	return envDenyProvider(t, "env.source_ip", "eq", "203.0.113.9",
		[]auth.Action{auth.ActionRead, auth.ActionWrite})
}

type envDoor struct {
	name string
	// run executes a statement under the API key and returns an error when
	// the door refused it.
	run func(t *testing.T, ctx context.Context, provider *auth.Provider, sql string) error
}

// envDoors stands up all four doors over one provider and returns a runner
// per door. Each is built per-provider because a door reads its policy set
// from the provider it was constructed with.
func envDoors() []envDoor {
	return []envDoor{
		{"embedded", func(t *testing.T, ctx context.Context, provider *auth.Provider, sql string) error {
			db := envDB(t, ctx, provider)
			_, err := db.Query(envIdentityCtx(t, ctx, provider), sql)
			return err
		}},
		{"pgwire", func(t *testing.T, ctx context.Context, provider *auth.Provider, sql string) error {
			db := envDB(t, ctx, provider)
			pg := pgwire.NewServer(db, pgwire.Config{AuthProvider: provider}, nil)
			if err := pg.Start("127.0.0.1:0"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pg.Shutdown)
			conn, err := pgx.Connect(ctx, fmt.Sprintf(
				"postgres://wadjet:analyst-key@%s/wadjet?sslmode=disable", pg.Addr()))
			if err != nil {
				return err
			}
			defer conn.Close(ctx)
			rows, qerr := conn.Query(ctx, sql)
			if qerr != nil {
				return qerr
			}
			defer rows.Close()
			for rows.Next() {
			}
			return rows.Err()
		}},
		{"http", func(t *testing.T, ctx context.Context, provider *auth.Provider, sql string) error {
			db := envDB(t, ctx, provider)
			srv := New(Config{Addr: ":0", Catalog: db.Catalog(), Provider: provider}, nil)
			hs := httptest.NewServer(srv.Mux())
			t.Cleanup(hs.Close)
			body, _ := json.Marshal(map[string]string{"sql": sql})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost,
				hs.URL+"/v1/queries", bytes.NewReader(body))
			if err != nil {
				return err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer analyst-key")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			var out struct {
				Error string `json:"error"`
			}
			_ = json.Unmarshal(raw, &out)
			if out.Error != "" {
				return fmt.Errorf("%s", out.Error)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
			}
			return nil
		}},
		{"grpc", func(t *testing.T, ctx context.Context, provider *auth.Provider, sql string) error {
			db := envDB(t, ctx, provider)
			client, stop := envGRPCClient(t, db, provider)
			defer stop()
			md := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer analyst-key")
			_, err := client.Query(md, &wadjetv1.QueryRequest{Sql: sql})
			return err
		}},
	}
}

// envDB is a small embedded database with one policed table, bound to the
// provider so every door decides against the same policy set.
func envDB(t *testing.T, ctx context.Context, provider *auth.Provider) *wadjet.DB {
	t.Helper()
	db := pmEmbeddedDB(t, ctx, 0)
	if err := db.SetAuthProvider(provider); err != nil {
		t.Fatalf("attach provider: %v", err)
	}
	return db
}

func envIdentityCtx(t *testing.T, ctx context.Context, provider *auth.Provider) context.Context {
	t.Helper()
	id, err := provider.Authenticator().AuthenticateToken("analyst-key")
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	// The embedded door has no socket, so it attaches no environment. The
	// decision stamps the clock itself, which is what makes the `env.hour`
	// cell reach it.
	return auth.ContextWithIdentity(ctx, id)
}

func envGRPCClient(t *testing.T, db *wadjet.DB, provider *auth.Provider) (wadjetv1.WadjetServiceClient, func()) {
	t.Helper()
	srv := NewGRPCServer(GRPCConfig{
		Catalog:      db.Catalog(),
		DB:           db,
		AuthProvider: provider,
	}, slog.Default())
	lis := bufconn.Listen(bufSize)
	srv.server = grpc.NewServer(
		grpc.UnaryInterceptor(srv.unaryAuthInterceptor()),
		grpc.StreamInterceptor(srv.streamAuthInterceptor()),
	)
	wadjetv1.RegisterWadjetServiceServer(srv.server, srv)
	go func() { _ = srv.server.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	return wadjetv1.NewWadjetServiceClient(conn), func() {
		conn.Close()
		srv.server.GracefulStop()
	}
}

// TestEveryDoorPublishesTheRequestTime — an `env.hour` deny that matches at
// every real hour must refuse on every door. It matched on none of them: no
// shared enforcement path put a clock on the environment (#933).
func TestEveryDoorPublishesTheRequestTime(t *testing.T) {
	for _, door := range envDoors() {
		t.Run(door.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			t.Cleanup(cancel)

			// Control: the same shape with a deny that cannot match.
			if err := door.run(t, ctx, envAllowOnlyProvider(t),
				"SELECT id FROM "+pmTable); err != nil {
				t.Fatalf("control read was refused: %v", err)
			}

			deny := envDenyProvider(t, "env.hour", "gte", 0, []auth.Action{auth.ActionRead})
			err := door.run(t, ctx, deny, "SELECT id FROM "+pmTable)
			if err == nil {
				t.Fatal("a deny conditioned on env.hour did not reach this door")
			}
			t.Logf("refused: %v", err)
		})
	}
}

// TestEveryDoorPublishesTheSourceAddress — a deny on `env.source_ip` for the
// loopback address. The network doors observe 127.0.0.1 (the bufconn gRPC peer
// is `bufconn`, so that door is checked with its own address), and the
// address must arrive WITHOUT the ephemeral port a socket carries.
func TestEveryDoorPublishesTheSourceAddress(t *testing.T) {
	for _, door := range []envDoor{envDoors()[1], envDoors()[2]} { // pgwire, http
		t.Run(door.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			t.Cleanup(cancel)

			if err := door.run(t, ctx, envAllowOnlyProvider(t),
				"SELECT id FROM "+pmTable); err != nil {
				t.Fatalf("control read was refused: %v", err)
			}

			// Written the way docs/security.md writes it: a bare IP, no port.
			deny := envDenyProvider(t, "env.source_ip", "eq", "127.0.0.1",
				[]auth.Action{auth.ActionRead})
			err := door.run(t, ctx, deny, "SELECT id FROM "+pmTable)
			if err == nil {
				t.Fatal("a deny conditioned on env.source_ip did not reach this door; " +
					"the address is missing, or it still carries its port")
			}
			t.Logf("refused: %v", err)
		})
	}
}

// TestTheDMLDoorPublishesTheEnvironment — the write path builds its own
// environment (EnforceDMLPolicies), so it is its own cell.
func TestTheDMLDoorPublishesTheEnvironment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	del := "DELETE FROM " + pmTable + " WHERE id = 1"
	// Control: the write goes through when the deny cannot match.
	if err := envDoors()[2].run(t, ctx, envAllowOnlyProvider(t), del); err != nil {
		t.Fatalf("control DELETE was refused: %v", err)
	}

	for _, tc := range []struct {
		name  string
		attr  string
		op    string
		value any
	}{
		{"time", "env.hour", "gte", 0},
		{"source address", "env.source_ip", "eq", "127.0.0.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deny := envDenyProvider(t, tc.attr, tc.op, tc.value, []auth.Action{auth.ActionWrite})
			err := envDoors()[2].run(t, ctx, deny, del)
			if err == nil {
				t.Fatalf("a DML deny conditioned on %s did not reach the HTTP door", tc.attr)
			}
			if !strings.Contains(err.Error(), "permission denied") &&
				!strings.Contains(err.Error(), "access denied") {
				t.Fatalf("DML refusal is not an authorization refusal: %v", err)
			}
		})
	}
}

// The protocol label an operator can condition on is the DOOR the client
// used, not the execution path — a statement that arrived over pgwire reports
// `pgwire`, even though the engine below labels its own enforcement call
// "embedded".
func TestTheProtocolAttributeNamesTheDoor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	for _, tc := range []struct {
		door     int
		protocol string
	}{
		{1, "pgwire"},
		{2, "http"},
		{3, "grpc"},
	} {
		door := envDoors()[tc.door]
		t.Run(tc.protocol, func(t *testing.T) {
			deny := envDenyProvider(t, "env.protocol", "eq", tc.protocol,
				[]auth.Action{auth.ActionRead})
			if err := door.run(t, ctx, deny, "SELECT id FROM "+pmTable); err == nil {
				t.Fatalf("a deny on env.protocol = %q did not reach the %s door",
					tc.protocol, door.name)
			}
		})
	}
}
