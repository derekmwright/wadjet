package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
)

// #943. A table-function scan was excluded from the ABAC relation census —
// `logical.PolicedScanTables` and `auth.StatementBaseTables` both skip a
// function scan — so it never became a Resource of any kind and no policy
// could reach it. A role limited to one catalog table read any file the server
// process could read, and made the server issue HTTP requests from inside its
// own network. Measured at 672bb5e1 under `reader` (tables [users], allow
// [read]):
//
//	SELECT * FROM read_csv('<tmp>/x.csv')                 server-local-value
//	SELECT * FROM read_csv('<tmp>/*.csv')                 server-local-value
//	SELECT * FROM read_json('http://127.0.0.1:<port>/x')  the loopback server
//	                                                      WAS HIT
//	postgres_query('postgres://…', 'SELECT 1')            dialled out
//	…and the same through pgwire, HTTP and gRPC, and with NO identity at all.
//
// A table function is now a capability: `Resource{Type: "table_function"}`
// evaluated with ActionRead, default deny under auth.

// tfRig is the SEC4 three-door rig plus the gRPC door, which reaches the same
// `wadjet.DB.Query` when the server runs standalone.
func tfGRPCDoor(t *testing.T, ctx context.Context, rig *sec4Rig) sec4Door {
	t.Helper()
	srv := NewGRPCServer(GRPCConfig{
		Catalog:      rig.db.Catalog(),
		DB:           rig.db,
		AuthProvider: rig.provider,
	}, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
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
	t.Cleanup(func() { conn.Close(); srv.server.GracefulStop() })
	client := wadjetv1.NewWadjetServiceClient(conn)

	return sec4Door{
		name: "grpc",
		run: func(t *testing.T, key, sql string) ([]string, string, error) {
			c := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
			resp, err := client.Query(c, &wadjetv1.QueryRequest{Sql: sql})
			if err != nil {
				// The gRPC door wraps the engine error in a status message;
				// the class travels in that text. Recovering it here keeps
				// this cell in the same census as the others — SEC2's arc owns
				// giving this door a proper code.
				return nil, classFromText(err.Error()), err
			}
			out := make([]string, 0, len(resp.Rows))
			for _, r := range resp.Rows {
				parts := make([]string, 0, len(resp.Columns))
				for _, c := range resp.Columns {
					if v, ok := r.Fields[c]; ok {
						parts = append(parts, strings.Trim(v.String(), `"`))
					} else {
						parts = append(parts, "")
					}
				}
				out = append(out, strings.Join(parts, "|"))
			}
			return out, "", nil
		},
		status: func() int { return 0 },
	}
}

// classFromText recovers a SQLSTATE this arc's refusals put in their message
// for a door that does not carry one structurally.
func classFromText(msg string) string {
	if strings.Contains(msg, "permission denied for table function") ||
		strings.Contains(msg, "permission denied") {
		return "42501"
	}
	return ""
}

func TestTableFunctionsAreACapabilityOnEveryDoor(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)
	doors := append(append([]sec4Door{}, rig.doors...), tfGRPCDoor(t, ctx, rig))

	dir := t.TempDir()
	real := filepath.Join(dir, "tf943.csv")
	if err := os.WriteFile(real, []byte("secret\nserver-local-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A path that WOULD error if it were opened. A refusal that carries 42501
	// rather than "no such file" is proof the plan never reached the reader.
	missing := filepath.Join(dir, "no", "such", "tf943.csv")

	// A loopback HTTP source that FAILS the test if a request ever arrives.
	var hit bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"a":1}]`))
	}))
	t.Cleanup(remote.Close)

	// A TCP port nothing listens on. A refusal that is 42501 rather than
	// "connection refused" proves the connector never dialled.
	deadPort := reservedDeadPort(t)

	cells := []struct{ name, sql string }{
		{"local file", fmt.Sprintf("SELECT * FROM read_csv('%s')", missing)},
		{"glob", fmt.Sprintf("SELECT * FROM read_csv('%s')", filepath.Join(dir, "*.csv"))},
		{"home relative", "SELECT * FROM read_csv('~/tf943_nosuch.csv')"},
		{"read_parquet", fmt.Sprintf("SELECT * FROM read_parquet('%s')", missing)},
		{"read_json", fmt.Sprintf("SELECT * FROM read_json('%s')", missing)},
		{"http loopback", fmt.Sprintf("SELECT * FROM read_json('%s/x.json')", remote.URL)},
		{"postgres_query", fmt.Sprintf(
			"SELECT * FROM postgres_query('postgres://u:p@127.0.0.1:%d/d?sslmode=disable&connect_timeout=1', 'SELECT 1')", deadPort)},
		{"mysql_query", fmt.Sprintf(
			"SELECT * FROM mysql_query('u:p@tcp(127.0.0.1:%d)/d', 'SELECT 1')", deadPort)},
		// The shapes whose table function is NOT in the statement's own plan:
		// a subquery is SQL text at enforcement time and becomes a second plan
		// inside the physical planner.
		{"cte", fmt.Sprintf("WITH c AS (SELECT * FROM read_csv('%s')) SELECT * FROM c", missing)},
		{"derived table", fmt.Sprintf("SELECT * FROM (SELECT * FROM read_csv('%s')) d", missing)},
		{"scalar subquery", fmt.Sprintf("SELECT (SELECT COUNT(*) FROM read_csv('%s')) AS c", missing)},
		{"scalar subquery over a table", fmt.Sprintf(
			"SELECT id, (SELECT COUNT(*) FROM read_csv('%s')) AS c FROM users", missing)},
		{"IN subquery", fmt.Sprintf(
			"SELECT id FROM users WHERE name IN (SELECT secret FROM read_csv('%s'))", missing)},
		{"EXISTS subquery", fmt.Sprintf(
			"SELECT id FROM users WHERE EXISTS (SELECT 1 FROM read_csv('%s'))", missing)},
		{"join arm", fmt.Sprintf(
			"SELECT u.id FROM users u JOIN read_csv('%s') f ON u.name = f.secret", missing)},
		{"union arm", fmt.Sprintf(
			"SELECT name FROM users UNION ALL SELECT secret FROM read_csv('%s')", missing)},
		{"EXPLAIN", fmt.Sprintf("EXPLAIN SELECT * FROM read_csv('%s')", missing)},
	}

	for _, d := range doors {
		d := d
		t.Run("reader-refused/"+d.name, func(t *testing.T) {
			for _, c := range cells {
				_, class, err := d.run(t, sec4Reader, c.sql)
				sec4Refused(t, d, c.name, class, err, "42501")
			}
		})
	}
	if hit {
		t.Error("a refused read_json('http://127.0.0.1:…') still issued the request")
	}

	// No identity at all under auth ENABLED: fail closed.
	emb := rig.door("embedded")
	_, class, err := emb.run(t, "", fmt.Sprintf("SELECT * FROM read_csv('%s')", real))
	sec4RefusedWith(t, "embedded", "read_csv/nil identity", class, err, "42501")

	// A pure table function computes over its arguments and opens nothing, so
	// it is NOT a capability and an ordinary reader keeps it.
	for _, d := range doors {
		rows, _, err := d.run(t, sec4Reader, "SELECT * FROM generate_series(1, 3)")
		if err != nil {
			t.Errorf("%s: reader generate_series must still work, got %v", d.name, err)
		} else if len(rows) != 3 {
			t.Errorf("%s: generate_series(1,3) returned %d rows: %v", d.name, len(rows), rows)
		}
	}

	// An identity holding `admin` keeps today's behaviour on every door.
	for _, d := range doors {
		rows, _, err := d.run(t, sec4Ops, fmt.Sprintf("SELECT * FROM read_csv('%s')", real))
		if err != nil {
			t.Errorf("%s: an admin identity must be able to read a local file, got %v", d.name, err)
			continue
		}
		if len(rows) != 1 || !strings.Contains(rows[0], "server-local-value") {
			t.Errorf("%s: admin read_csv returned %v", d.name, rows)
		}
	}
}

// A policy may SCOPE the capability to a destination. `resource.path` carries
// the expanded, cleaned local path and `resource.host` the host of a URL or of
// a connector's connection string, so `eq`, `in`, `contains` and `regex` all
// work as an allowlist.
func TestATableFunctionPolicyScopesTheDestination(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	allowed := filepath.Join(dir, "allowed.csv")
	denied := filepath.Join(dir, "denied.csv")
	for _, p := range []string{allowed, denied} {
		if err := os.WriteFile(p, []byte("secret\nv\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "tf", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "reader-one-file", EffectStr: "allow", Priority: 10,
			Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
			Resources: []auth.Condition{
				{Attribute: "resource.type", Op: "eq", Value: auth.ResourceTableFunction},
				{Attribute: "resource.name", Op: "eq", Value: "read_csv"},
				{Attribute: "resource.path", Op: "eq", Value: allowed},
			},
			Actions: []auth.Action{auth.ActionRead},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: sec4Reader, Name: "reader", Role: "reader"}},
		Roles:   []auth.RoleConfig{{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)

	db := sec4DB(t, ctx)
	if err := db.SetAuthProvider(provider); err != nil {
		t.Fatal(err)
	}
	id, err := provider.Authenticator().AuthenticateToken(sec4Reader)
	if err != nil {
		t.Fatal(err)
	}
	readerCtx := auth.ContextWithIdentity(ctx, id)

	if _, err := db.Query(readerCtx, fmt.Sprintf("SELECT * FROM read_csv('%s')", allowed)); err != nil {
		t.Fatalf("the policy names this path; it must be readable: %v", err)
	}
	// The same file reached by a path that CLEANS to it — the attribute is the
	// cleaned, home-expanded path, so a `..` detour is not a second string.
	detour := filepath.Join(dir, "sub", "..", "allowed.csv")
	if _, err := db.Query(readerCtx, fmt.Sprintf("SELECT * FROM read_csv('%s')", detour)); err != nil {
		t.Errorf("a path that cleans to the allowed one must be readable: %v", err)
	}
	// Another file in the same directory is not the one the policy named.
	if _, err := db.Query(readerCtx, fmt.Sprintf("SELECT * FROM read_csv('%s')", denied)); err == nil {
		t.Error("a path the policy does not name must be refused")
	}
	// A different function over the allowed path is not the one it named.
	if _, err := db.Query(readerCtx, fmt.Sprintf("SELECT * FROM read_json('%s')", allowed)); err == nil {
		t.Error("read_json must be refused by a policy that names read_csv")
	}
}

// The boundary claim: with NO provider — the CLI and the embedded default —
// table functions behave exactly as they did.
func TestTableFunctionsAreUnchangedWithoutAnAuthProvider(t *testing.T) {
	ctx := context.Background()
	db := sec4DB(t, ctx)
	dir := t.TempDir()
	p := filepath.Join(dir, "noauth.csv")
	if err := os.WriteFile(p, []byte("secret\nv\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	res, err := db.Query(ctx, fmt.Sprintf("SELECT * FROM read_csv('%s')", p))
	if err != nil {
		t.Fatalf("no-provider read_csv: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("no-provider read_csv returned %d rows", len(res.Rows))
	}
	if _, err := db.Query(ctx, fmt.Sprintf(
		"SELECT (SELECT COUNT(*) FROM read_csv('%s')) AS c", p)); err != nil {
		t.Fatalf("no-provider scalar subquery over read_csv: %v", err)
	}
}

// reservedDeadPort returns a TCP port on loopback that nothing is listening on.
func reservedDeadPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}
