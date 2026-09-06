package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/server"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// HTTP metadata asks the ONE table-access decision (#941/#935's HTTP half,
// ADR-0034).
//
// `handleListTables`, `handleGetTable`, `handleDescribe` and `handleShowTables`
// called `Authorizer.FilterTables` / `CanAccessTable`, which is the LEGACY
// role rule and only that. With an ABAC evaluator installed — the shape a
// `roles:`-to-ABAC migration produces, where the role keeps `tables: ["*"]` —
// an explicit DENY on a relation did not hide it from the listing and did not
// refuse a describe: the identity could read the schema of a table every data
// path refuses it.

// metadataProvider: `analyst` is allowed to read `public_t` and explicitly
// DENIED `secret_t`, while its legacy role still lists every table.
func metadataProvider() *auth.Provider {
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "sec3-metadata", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "analyst-public", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "public_t"}},
				Actions:   []auth.Action{auth.ActionRead},
			},
			{
				ID: "analyst-secret-denied", EffectStr: "deny", Priority: 100,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "secret_t"}},
				Actions:   []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
			{
				// `writer` may write every relation the policy does not deny
				// it, and the deny below covers `secret_t`.
				ID: "writer-all", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "writer"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
			{
				ID: "writer-secret-denied", EffectStr: "deny", Priority: 100,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "writer"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "secret_t"}},
				Actions:   []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
			{
				ID: "admin-all", EffectStr: "allow", Priority: 10,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "admin"}},
				Actions: []auth.Action{auth.ActionRead, auth.ActionWrite, auth.ActionAdmin,
					auth.ActionCreate, auth.ActionDrop, auth.ActionDescribe},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "analyst-key", Name: "analyst-user", Role: "analyst"},
			{Key: "writer-key", Name: "writer-user", Role: "writer"},
			{Key: "admin-key", Name: "admin-user", Role: "admin"},
		},
		// The legacy role lists EVERY table, which is what makes this a real
		// test: only the evaluator knows `secret_t` is denied.
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

func metadataServer(t *testing.T) *httptest.Server {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	for _, name := range []string{"public_t", "secret_t"} {
		if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}
	srv := server.New(server.Config{
		Addr: ":0", Catalog: cat, Provider: metadataProvider(),
	}, logger)
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func metadataDo(t *testing.T, ts *httptest.Server, method, path, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	return resp.StatusCode, buf.String()
}

func TestHTTPTableListingHidesAnABACDeniedRelation(t *testing.T) {
	ts := metadataServer(t)

	for _, tc := range []struct {
		name, method, path, body string
		extract                  func(string) []string
	}{
		{"GET /v1/tables", http.MethodGet, "/v1/tables", "", func(body string) []string {
			var out struct {
				Tables []string `json:"tables"`
			}
			json.Unmarshal([]byte(body), &out)
			return out.Tables
		}},
		{"SHOW TABLES", http.MethodPost, "/v1/queries", `{"sql":"SHOW TABLES"}`,
			func(body string) []string {
				var out struct {
					Rows []map[string]any `json:"rows"`
				}
				json.Unmarshal([]byte(body), &out)
				var names []string
				for _, r := range out.Rows {
					if n, ok := r["table_name"].(string); ok {
						names = append(names, n)
					}
				}
				return names
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := metadataDo(t, ts, tc.method, tc.path, "analyst-key", tc.body)
			if code != http.StatusOK {
				t.Fatalf("status %d; want 200 (%s)", code, body)
			}
			names := tc.extract(body)
			seen := map[string]bool{}
			for _, n := range names {
				seen[n] = true
			}
			if !seen["public_t"] {
				t.Errorf("the permitted table is missing from %v", names)
			}
			if seen["secret_t"] {
				t.Errorf("an explicitly DENIED relation is published by the listing: %v", names)
			}

			// An administrator sees both.
			code, body = metadataDo(t, ts, tc.method, tc.path, "admin-key", tc.body)
			if code != http.StatusOK {
				t.Fatalf("admin: status %d; want 200 (%s)", code, body)
			}
			adminSees := map[string]bool{}
			for _, n := range tc.extract(body) {
				adminSees[n] = true
			}
			if !adminSees["public_t"] || !adminSees["secret_t"] {
				t.Errorf("the administrator's listing lost a table: %v", tc.extract(body))
			}
		})
	}
}

// DDL on an EXISTING relation asks the table decision too: the `write`
// permission says the identity may write SOMETHING, not that it may write
// THIS. An identity holding `write` whom a policy explicitly denies on
// `secret_t` could drop it, because the permission check never consulted the
// policy (batch decision, ADR-0034).
func TestHTTPDDLOnADeniedRelationIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, method, path, body string }{
		{"DROP TABLE", http.MethodPost, "/v1/queries", `{"sql":"DROP TABLE secret_t"}`},
		{"ANALYZE TABLE", http.MethodPost, "/v1/queries", `{"sql":"ANALYZE TABLE secret_t"}`},
		{"DELETE /v1/tables/{name}", http.MethodDelete, "/v1/tables/secret_t", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := metadataServer(t)
			// `writer-user` holds `write` on every table by its ROLE, and the
			// policy denies it `secret_t`.
			code, body := metadataDo(t, ts, tc.method, tc.path, "writer-key", tc.body)
			if code != http.StatusForbidden {
				t.Fatalf("status %d; want 403 (%s)", code, body)
			}
			var payload struct {
				Error string `json:"error"`
			}
			json.Unmarshal([]byte(body), &payload)
			if want := `permission denied for table "secret_t"`; payload.Error != want {
				t.Errorf("refusal text %q; want %q", payload.Error, want)
			}
			// The table is still there — assert the CATALOG, not the status.
			code, body = metadataDo(t, ts, http.MethodGet, "/v1/tables/secret_t", "admin-key", "")
			if code != http.StatusOK {
				t.Errorf("the refused DDL removed the table: GET returned %d (%s)", code, body)
			}

			// A relation the same identity IS allowed to write still works.
			allowedPath, allowedBody := tc.path, tc.body
			if tc.body == "" {
				allowedPath = "/v1/tables/public_t"
			} else {
				allowedBody = strings.Replace(tc.body, "secret_t", "public_t", 1)
			}
			if code, body := metadataDo(t, ts, tc.method, allowedPath, "writer-key",
				allowedBody); code == http.StatusForbidden {
				t.Errorf("the permitted relation was refused: %d (%s)", code, body)
			}
		})
	}
}

func TestHTTPDescribeRefusesAnABACDeniedRelation(t *testing.T) {
	ts := metadataServer(t)

	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"GET /v1/tables/{name}", http.MethodGet, "/v1/tables/secret_t", ""},
		{"DESCRIBE", http.MethodPost, "/v1/queries", `{"sql":"DESCRIBE secret_t"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := metadataDo(t, ts, tc.method, tc.path, "analyst-key", tc.body)
			if code != http.StatusForbidden {
				t.Fatalf("status %d; want 403 for a denied relation (%s)", code, body)
			}
			var payload struct {
				Error string `json:"error"`
			}
			json.Unmarshal([]byte(body), &payload)
			if want := `permission denied for table "secret_t"`; payload.Error != want {
				t.Errorf("refusal text %q; want %q — the text every other door carries",
					payload.Error, want)
			}

			// The permitted relation still answers.
			permitted := tc.path
			permittedBody := tc.body
			if tc.body == "" {
				permitted = "/v1/tables/public_t"
			} else {
				permittedBody = `{"sql":"DESCRIBE public_t"}`
			}
			if code, body := metadataDo(t, ts, tc.method, permitted, "analyst-key",
				permittedBody); code != http.StatusOK {
				t.Errorf("the permitted relation was refused: %d (%s)", code, body)
			}
		})
	}
}
