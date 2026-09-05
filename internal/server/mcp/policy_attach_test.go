package mcp

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The MCP door is the one `wadjet mcp` serves, and it is the door
// `wadjet.Open(Config{AuthProvider: …})` reaches: `runMCP` builds the DB that
// way and hands it to NewServerWithIdentity, which enforces through db.Query.
// Until #882 round 3 that Open never bound the policy set, so a policy naming
// the relation in a spelling that is neither the catalog's nor the folded one
// matched nothing — and beside the broad allow an RBAC-to-ABAC migration
// emits, a rule that matches nothing is a grant: the mask and the deny were
// simply absent from an AI agent's query results.
//
// This cell drives the tool the agent calls, not the engine underneath it.

const mcpSecret = "true-secret-01"

func mcpPolicySchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "Region", Type: parquet.TypeString},
		{Name: "Secret", Type: parquet.TypeString},
		{Name: "Salary", Type: parquet.TypeInt64},
	}}
}

func mcpPolicyProvider(t *testing.T, resource string) *auth.Provider {
	t.Helper()
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "mcp", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{
			{
				ID: "scoped", EffectStr: "allow", Priority: 10,
				Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: resource}},
				Actions:   []auth.Action{auth.ActionRead, auth.ActionWrite},
				Obligations: []auth.Obligation{
					{Type: "deny_column", Target: "Salary"},
					{Type: "mask_column", Target: "Secret", Value: "'***'"},
				},
			},
			// The broad allow every roles-to-ABAC migration emits: without it
			// an unmatched scoped rule fails CLOSED and the disclosure this
			// cell is about cannot happen at all.
			{
				ID: "broad", EffectStr: "allow", Priority: 20,
				Subjects: []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
				Actions:  []auth.Action{auth.ActionRead, auth.ActionWrite},
			},
		},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "analyst-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read", "write"}}},
	})
	p := auth.NewProvider(authn, authz, nil, nil)
	p.UpdateWithEvaluator(authn, authz, nil, evaluator)
	return p
}

// TestTheMCPDoorEnforcesThePolicyItsOpenAttached is the `wadjet mcp` path.
func TestTheMCPDoorEnforcesThePolicyItsOpenAttached(t *testing.T) {
	for _, tc := range []struct {
		name, resource string
		bindable       bool
	}{
		{"the catalog spelling binds", "Hits", true},
		{"the folded spelling binds", "hits", true},
		{"an upper-case spelling refuses at Open", "HITS", false},
		{"a mixed-case spelling that is neither refuses at Open", "hItS", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := objstore.NewMemStore()
			kv := catalog.NewMemKV()
			seed, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test", MetaKV: kv})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { seed.Close() })
			if err := seed.Catalog().CreateTable(ctx, "Hits", mcpPolicySchema(), nil); err != nil {
				t.Fatal(err)
			}
			ing := seed.NewIngester("Hits", mcpPolicySchema(), nil,
				ingest.Config{MaxBufferRows: 10, RowGroupSize: 3})
			if err := ing.Ingest(ctx, []map[string]any{
				{"WatchID": int64(1), "Region": "us", "Secret": mcpSecret, "Salary": int64(700001)},
				{"WatchID": int64(2), "Region": "eu", "Secret": "true-secret-02", "Salary": int64(700002)},
			}); err != nil {
				t.Fatal(err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}

			provider := mcpPolicyProvider(t, tc.resource)
			// Exactly what cmd/wadjet's `mcp` command does.
			db, err := wadjet.Open(ctx, wadjet.Config{
				Store: store, Bucket: "test", MetaKV: kv, AuthProvider: provider,
			})
			if db != nil {
				t.Cleanup(func() { db.Close() })
			}
			if !tc.bindable {
				if err == nil {
					srv := NewServerWithIdentity(db, nil, mcpIdentity(t, provider))
					res := srv.toolQuery(auth.ContextWithIdentity(ctx, mcpIdentity(t, provider)),
						map[string]any{"sql": "SELECT * FROM Hits ORDER BY WatchID"})
					body := ""
					if len(res.Content) > 0 {
						body = res.Content[0].Text
					}
					t.Fatalf("`wadjet mcp` opened a DB with a policy set naming %q, which the "+
						"catalog does not hold, and the query tool answered:\n  %s", tc.resource, body)
				}
				return
			}
			if err != nil {
				t.Fatalf("a bindable policy set refused to open: %v", err)
			}

			srv := NewServerWithIdentity(db, nil, mcpIdentity(t, provider))
			// The identity `serve` stamps onto every request context
			// (server.go: "This is the single place identity enters query
			// execution"), applied here because this cell calls the tool
			// directly rather than through the JSON-RPC loop.
			actx := auth.ContextWithIdentity(ctx, mcpIdentity(t, provider))
			res := srv.toolQuery(actx, map[string]any{"sql": "SELECT * FROM Hits ORDER BY WatchID"})
			if res.IsError || len(res.Content) == 0 {
				t.Fatalf("the query tool refused a permitted read: %+v", res.Content)
			}
			body := res.Content[0].Text
			if strings.Contains(body, mcpSecret) {
				t.Errorf("the MCP door DISCLOSED the masked column in plaintext:\n  %s", body)
			}
			if strings.Contains(body, "700001") || strings.Contains(strings.ToLower(body), "salary") {
				t.Errorf("the MCP door returned the DENIED column:\n  %s", body)
			}
			if !strings.Contains(body, "***") {
				t.Errorf("the MCP door did not carry the mask:\n  %s", body)
			}
		})
	}
}

func mcpIdentity(t *testing.T, p *auth.Provider) *auth.Identity {
	t.Helper()
	id, err := p.Authenticator().AuthenticateToken("analyst-key")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
