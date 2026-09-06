package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/server"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The HTTP door census: EVERY route registered on the mux, against four
// identities, with the status each must answer (#937, ADR-0034).
//
// "Middleware present" is not authorization. `auth.ProviderMiddleware`
// resolves an identity and checks no permission at all, so at 672bb5e1 an
// authenticated read-only caller could read the dead-letter queue (which
// carries other identities' SQL inside `TaskData`), purge it, and trigger
// global result cleanup — and one of those handlers even carried a comment
// claiming the middleware checked admin for it.
//
// The census walks the router rather than listing what somebody remembered to
// test, so a route ADDED later without a row here fails this test. That is the
// point of it: the defect was three handlers nobody had written a row for.
//
// Not covered here, deliberately: the UDF surface (`CREATE FUNCTION` /
// `DROP FUNCTION` / `SHOW FUNCTIONS`) is a STATEMENT TYPE on POST /v1/queries,
// not a route, and SEC4 owns its authorization; and query OWNERSHIP (a
// non-owner reading another principal's status or results) needs two
// identities against one query, which sec3_query_owner_test.go carries.

const (
	// anyAllowed: the identity is authorized — the exact status depends on
	// what the fixture holds, so the census asserts only that the door did
	// not refuse it (401/403).
	anyAllowed = -1
)

type censusRow struct {
	method  string
	pattern string // as chi registered it
	path    string // a concrete request path
	body    string
	// want maps an API key name to the expected status. Keys: "", "reader",
	// "writer", "admin".
	want map[string]int
	// noRequest marks a route the census ENUMERATES but does not call
	// (pprof's profile and trace block for seconds by design).
	noRequest string
	note      string
}

func censusRows() []censusRow {
	// Every authenticated identity may run a query; only admin may touch the
	// operational endpoints.
	openToAll := map[string]int{"": 401, "reader": anyAllowed, "writer": anyAllowed, "admin": anyAllowed}
	adminOnly := func(adminWant int) map[string]int {
		return map[string]int{"": 401, "reader": 403, "writer": 403, "admin": adminWant}
	}
	writeOnly := func(writerWant int) map[string]int {
		return map[string]int{"": 401, "reader": 403, "writer": writerWant, "admin": writerWant}
	}
	return []censusRow{
		{method: "POST", pattern: "/v1/queries", path: "/v1/queries",
			body: `{"sql":"SELECT 1"}`, want: openToAll},
		{method: "GET", pattern: "/v1/queries", path: "/v1/queries", want: openToAll,
			note: "listing is filtered by owner — see sec3_query_owner_test.go"},
		{method: "POST", pattern: "/v1/queries/async", path: "/v1/queries/async",
			body: `{"sql":"SELECT 1"}`, want: openToAll},
		{method: "GET", pattern: "/v1/queries/{queryID}", path: "/v1/queries/no-such-query",
			want: map[string]int{"": 401, "reader": 404, "writer": 404, "admin": 404},
			note: "an ID nobody registered is 404 for everyone; ownership is the owner test"},
		{method: "GET", pattern: "/v1/queries/{queryID}/results", path: "/v1/queries/no-such-query/results",
			want: map[string]int{"": 401, "reader": 404, "writer": 404, "admin": 404}},
		{method: "DELETE", pattern: "/v1/queries/{queryID}", path: "/v1/queries/no-such-query",
			want: map[string]int{"": 401, "reader": 400, "writer": 400, "admin": 400},
			note: "cancel answers 400 where status and results answer 404 for the same " +
				"unknown ID — recorded, not changed by this arc"},

		{method: "GET", pattern: "/v1/tables", path: "/v1/tables", want: openToAll},
		{method: "POST", pattern: "/v1/tables", path: "/v1/tables",
			body: `{"name":"census_t","columns":[{"name":"id","type":"INT64"}]}`,
			want: writeOnly(anyAllowed)},
		{method: "GET", pattern: "/v1/tables/{name}", path: "/v1/tables/no_such_table",
			want: map[string]int{"": 401, "reader": 404, "writer": 404, "admin": 404}},
		{method: "DELETE", pattern: "/v1/tables/{name}", path: "/v1/tables/no_such_table",
			want: writeOnly(404)},

		{method: "GET", pattern: "/v1/health", path: "/v1/health",
			want: map[string]int{"": 200, "reader": 200, "writer": 200, "admin": 200},
			note: "the middleware exempts health so a liveness probe needs no credential"},
		{method: "GET", pattern: "/v1/ready", path: "/v1/ready", want: openToAll},

		{method: "GET", pattern: "/v1/dlq", path: "/v1/dlq", want: adminOnly(200)},
		{method: "GET", pattern: "/v1/dlq/{entryID}", path: "/v1/dlq/no-such-entry",
			want: adminOnly(404)},
		{method: "DELETE", pattern: "/v1/dlq", path: "/v1/dlq", want: adminOnly(200)},

		{method: "GET", pattern: "/v1/workers", path: "/v1/workers", want: adminOnly(200)},
		{method: "DELETE", pattern: "/v1/results/{queryID}", path: "/v1/results/no-such-query",
			want: adminOnly(503),
			note: "owner-or-admin: an ID the tracker no longer holds has no owner left " +
				"to check, so it is the administrator's. 503 because this fixture's " +
				"coordinator has no result store"},
		{method: "POST", pattern: "/v1/results/cleanup", path: "/v1/results/cleanup",
			body: "{}", want: adminOnly(503)},

		{method: "GET", pattern: "/v1/admin/config", path: "/v1/admin/config", want: adminOnly(200)},
		{method: "PUT", pattern: "/v1/admin/config", path: "/v1/admin/config",
			body: "{}", want: adminOnly(anyAllowed)},
		{method: "POST", pattern: "/v1/admin/config/reload", path: "/v1/admin/config/reload",
			want: adminOnly(anyAllowed)},
		{method: "POST", pattern: "/v1/admin/auth/keys", path: "/v1/admin/auth/keys",
			body: "{}", want: adminOnly(anyAllowed)},
		{method: "DELETE", pattern: "/v1/admin/auth/keys/{name}", path: "/v1/admin/auth/keys/nobody",
			want: adminOnly(anyAllowed)},
		{method: "GET", pattern: "/v1/admin/auth/roles", path: "/v1/admin/auth/roles",
			want: adminOnly(200)},
		{method: "PUT", pattern: "/v1/admin/auth/roles", path: "/v1/admin/auth/roles",
			body: "{}", want: adminOnly(anyAllowed)},
		{method: "PUT", pattern: "/v1/admin/auth/policies", path: "/v1/admin/auth/policies",
			body: "{}", want: adminOnly(anyAllowed)},
		{method: "PUT", pattern: "/v1/admin/tuning", path: "/v1/admin/tuning",
			body: "{}", want: adminOnly(anyAllowed)},
	}
}

// pprofRows are the profiling routes. They are RECORDED, not changed by this
// arc: today any authenticated identity may read them, which is a separate
// exposure from #937's (a profile carries stack traces, not stored rows) and
// is filed rather than silently widened into this fix.
func pprofRows() []censusRow {
	anyAuthenticated := map[string]int{"": 401, "reader": anyAllowed, "writer": anyAllowed, "admin": anyAllowed}
	rows := []censusRow{
		{method: "*", pattern: "/debug/pprof/", path: "/debug/pprof/", want: anyAuthenticated},
		{method: "*", pattern: "/debug/pprof/cmdline", path: "/debug/pprof/cmdline", want: anyAuthenticated},
		{method: "*", pattern: "/debug/pprof/profile", path: "/debug/pprof/profile",
			want: anyAuthenticated, noRequest: "a CPU profile blocks for 30s"},
		{method: "*", pattern: "/debug/pprof/symbol", path: "/debug/pprof/symbol", want: anyAuthenticated},
		{method: "*", pattern: "/debug/pprof/trace", path: "/debug/pprof/trace",
			want: anyAuthenticated, noRequest: "an execution trace blocks for 1s"},
		{method: "*", pattern: "/debug/pprof/goroutine", path: "/debug/pprof/goroutine?debug=1",
			want: anyAuthenticated},
		{method: "*", pattern: "/debug/pprof/heap", path: "/debug/pprof/heap?debug=1", want: anyAuthenticated},
		{method: "*", pattern: "/debug/pprof/allocs", path: "/debug/pprof/allocs?debug=1", want: anyAuthenticated},
	}
	return rows
}

var censusKeys = map[string]string{
	"":       "",
	"reader": "reader-key",
	"writer": "writer-key",
	"admin":  "admin-key",
}

// censusProvider: three keys, three roles, only one of them holding `admin`.
func censusProvider() *auth.Provider {
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "admin-key", Name: "admin-user", Role: "admin"},
			{Key: "writer-key", Name: "writer-user", Role: "writer"},
			{Key: "reader-key", Name: "reader-user", Role: "reader"},
		},
		Roles: []auth.RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	})
	return auth.NewProvider(authn, authz, nil, nil)
}

// censusServer is the whole door: the base mux plus the ops and admin routes,
// behind ProviderMiddleware, with a real coordinator and DLQ.
func censusServer(t *testing.T) (*httptest.Server, *coordinator.Coordinator, *catalog.Catalog) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embedded.Shutdown)
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	coord := coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test",
	}, cat, nc, js, logger)

	provider := censusProvider()
	// The coordinator carries the provider too — that is what makes the
	// query-lifecycle methods enforce ownership, and cmd/wadjet does it at
	// startup for the same reason.
	if err := coord.SetAuthProvider(provider); err != nil {
		t.Fatalf("attaching the auth provider: %v", err)
	}
	srv := server.New(server.Config{
		Addr: ":0", Catalog: cat, Coordinator: coord,
		DLQ: coordinator.NewDLQ(js), Provider: provider,
	}, logger)
	server.NewOpsAPI(coord, provider).RegisterRoutes(srv.Mux())
	server.NewAdminAPI(config.NewManager(&config.Config{Mode: "standalone"}, logger),
		provider, logger).RegisterRoutes(srv.Mux())

	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts, coord, cat
}

// registeredRoutes walks the mux. A route registered for every method (chi's
// Handle/HandleFunc, which is how the pprof handlers go on) is reported once
// under "*".
func registeredRoutes(t *testing.T, mux chi.Routes) map[string]bool {
	t.Helper()
	byPattern := map[string]map[string]bool{}
	err := chi.Walk(mux, func(method, route string, _ http.Handler,
		_ ...func(http.Handler) http.Handler) error {
		route = normalizeRoute(route)
		if byPattern[route] == nil {
			byPattern[route] = map[string]bool{}
		}
		byPattern[route][method] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walking routes: %v", err)
	}
	out := map[string]bool{}
	for route, methods := range byPattern {
		// chi registers a method-less Handle under every method it knows.
		if len(methods) >= 8 {
			out["* "+route] = true
			continue
		}
		for m := range methods {
			out[m+" "+route] = true
		}
	}
	return out
}

// normalizeRoute strips chi's trailing "/*" from a subtree mount so the census
// names the route the way it was registered.
func normalizeRoute(route string) string {
	if len(route) > 2 && route[len(route)-2:] == "/*" {
		return route[:len(route)-2] + "/*"
	}
	return route
}

func TestHTTPRouteCensusAuthorizesEveryRoute(t *testing.T) {
	ts, _, _ := censusServer(t)

	rows := append(censusRows(), pprofRows()...)

	// 1. Every registered route has a row, and every row names a registered
	//    route. A route added without a census row fails here.
	registered := registeredRoutes(t, ts.Config.Handler.(chi.Routes))
	inCensus := map[string]bool{}
	for _, row := range rows {
		inCensus[row.method+" "+row.pattern] = true
	}
	var missing, stale []string
	for route := range registered {
		if !inCensus[route] {
			missing = append(missing, route)
		}
	}
	for route := range inCensus {
		if !registered[route] {
			stale = append(stale, route)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		t.Errorf("routes registered on the mux with no census row (add one, with the "+
			"status each identity must get):\n  %v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("census rows for routes the mux does not register:\n  %v", stale)
	}

	// 2. Every row × every identity.
	for _, row := range rows {
		if row.noRequest != "" {
			continue
		}
		for _, id := range []string{"", "reader", "writer", "admin"} {
			want, ok := row.want[id]
			if !ok {
				t.Fatalf("census row %s %s has no expectation for identity %q",
					row.method, row.pattern, id)
			}
			name := fmt.Sprintf("%s %s/%s", row.method, row.pattern, orNone(id))
			t.Run(name, func(t *testing.T) {
				method := row.method
				if method == "*" {
					method = http.MethodGet
				}
				got, body := censusDo(t, ts, method, row.path, censusKeys[id], row.body)
				switch {
				case want == anyAllowed:
					if got == http.StatusUnauthorized || got == http.StatusForbidden {
						t.Errorf("%s refused an authorized identity with %d: %s", name, got, clip(body))
					}
				case got != want:
					t.Errorf("status %d; want %d (body %s)", got, want, clip(body))
				}
			})
		}
	}
}

// The refusal text is the shared authorizer's, so the same operation reads the
// same on every door (pgwire 42501, gRPC PermissionDenied carry it too).
func TestHTTPOperationalRefusalCarriesTheSharedText(t *testing.T) {
	ts, _, _ := censusServer(t)
	want := `unauthorized: "admin" permission required (identity "reader-user", role "reader")`
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/v1/dlq"},
		{http.MethodGet, "/v1/dlq/x"},
		{http.MethodDelete, "/v1/dlq"},
		{http.MethodGet, "/v1/workers"},
		{http.MethodPost, "/v1/results/cleanup"},
		// DELETE /v1/results/{id} is not here: it is owner-or-admin, so its
		// refusal is the query-ownership one, pinned in
		// TestQueryRefusalTextIsTheSameOnEveryDoor.
	} {
		code, body := censusDo(t, ts, r.method, r.path, "reader-key", "{}")
		if code != http.StatusForbidden {
			t.Errorf("%s %s: status %d; want 403", r.method, r.path, code)
			continue
		}
		var payload struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &payload); err != nil {
			t.Errorf("%s %s: body %s is not JSON: %v", r.method, r.path, body, err)
			continue
		}
		if payload.Error != want {
			t.Errorf("%s %s: refusal text %q; want %q", r.method, r.path, payload.Error, want)
		}
	}
}

// A refused operational mutation has NO side effect: the DLQ a reader tried to
// purge is still there for the admin that may read it.
func TestHTTPRefusedPurgeLeavesTheDLQIntact(t *testing.T) {
	ts, _, _ := censusServer(t)

	if code, body := censusDo(t, ts, http.MethodDelete, "/v1/dlq", "reader-key", ""); code != 403 {
		t.Fatalf("reader purge: status %d (%s); want 403", code, body)
	}
	// The admin still reaches a live queue rather than one the reader emptied.
	code, body := censusDo(t, ts, http.MethodGet, "/v1/dlq", "admin-key", "")
	if code != http.StatusOK {
		t.Fatalf("admin list after the refused purge: status %d (%s); want 200", code, body)
	}
}

func censusDo(t *testing.T, ts *httptest.Server, method, path, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.String()
}

// clip shortens a body for an error message. censusDo returns it whole,
// because a caller that decodes the listing needs all of it.
func clip(s string) string {
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}

func orNone(id string) string {
	if id == "" {
		return "no-credential"
	}
	return id
}
