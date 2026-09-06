package auth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The decision table for #943, cell by cell, without a plan or a door in the
// way: the reachability gate lives in internal/server.
func TestAuthorizeTableFunctionIsDefaultDenyUnderAuth(t *testing.T) {
	enabled := func(t *testing.T, allow []string) *Provider {
		t.Helper()
		authn, authz := New(Config{
			Enabled: true,
			APIKeys: []APIKeyDef{{Key: "k", Name: "u", Role: "r"}},
			Roles:   []RoleConfig{{Name: "r", Tables: []string{"*"}, Allow: allow}},
		})
		return NewProvider(authn, authz, nil, nil)
	}
	id := func(perms ...string) *Identity {
		return &Identity{Name: "u", Role: "r", Tables: []string{"*"}, Perms: perms}
	}

	t.Run("no provider changes nothing", func(t *testing.T) {
		if err := AuthorizeTableFunction(context.Background(), nil, "embedded",
			"read_csv", []string{"/etc/passwd"}, nil); err != nil {
			t.Fatalf("with no provider a table function must be allowed: %v", err)
		}
	})

	t.Run("a pure function is not a capability", func(t *testing.T) {
		p := enabled(t, []string{"read"})
		ctx := ContextWithIdentity(context.Background(), id("read"))
		for _, fn := range []string{"generate_series", "unnest", "GENERATE_SERIES"} {
			if err := AuthorizeTableFunction(ctx, p, "embedded", fn, []string{"1", "3"}, nil); err != nil {
				t.Errorf("%s opens nothing and must be allowed: %v", fn, err)
			}
		}
	})

	t.Run("legacy roles: admin holds the capability, read does not", func(t *testing.T) {
		p := enabled(t, []string{"read"})
		readCtx := ContextWithIdentity(context.Background(), id("read"))
		err := AuthorizeTableFunction(readCtx, p, "embedded", "read_csv", []string{"/etc/passwd"}, nil)
		if err == nil {
			t.Fatal("a role holding only `read` must be refused")
		}
		if got := sqlerr.StateOf(err); got != "42501" {
			t.Errorf("SQLSTATE = %q, want 42501", got)
		}
		if !strings.Contains(err.Error(), "read_csv") {
			t.Errorf("the refusal must name the function: %v", err)
		}
		if strings.Contains(err.Error(), "/etc/passwd") {
			t.Errorf("the refusal must not echo the destination: %v", err)
		}

		adminCtx := ContextWithIdentity(context.Background(), id("admin"))
		if err := AuthorizeTableFunction(adminCtx, p, "embedded", "read_csv",
			[]string{"/etc/passwd"}, nil); err != nil {
			t.Errorf("an identity holding `admin` must keep the capability: %v", err)
		}
	})

	t.Run("no identity under auth enabled fails closed", func(t *testing.T) {
		p := enabled(t, []string{"admin"})
		err := AuthorizeTableFunction(context.Background(), p, "embedded",
			"read_csv", []string{"/etc/passwd"}, nil)
		if err == nil {
			t.Fatal("a context with no identity must be refused")
		}
		if got := sqlerr.StateOf(err); got != "42501" {
			t.Errorf("SQLSTATE = %q, want 42501", got)
		}
	})

	t.Run("an evaluator decides, default deny", func(t *testing.T) {
		authn, authz := New(Config{
			Enabled: true,
			APIKeys: []APIKeyDef{{Key: "k", Name: "u", Role: "r"}},
			Roles:   []RoleConfig{{Name: "r", Tables: []string{"*"}, Allow: []string{"admin"}}},
		})
		// An evaluator with a rule about TABLES only. Note the identity holds
		// `admin`: with an evaluator installed the evaluator is the authority,
		// so the legacy permission does not leak the capability back in.
		ev := NewPolicyEvaluator([]AccessControlPolicy{{
			Name: "p", Version: 1, Enabled: true,
			Rules: []PolicyRule{{
				ID: "tables", EffectStr: "allow", Priority: 10,
				Subjects:  []Condition{{Attribute: "subject.role", Op: "eq", Value: "r"}},
				Resources: []Condition{{Attribute: "resource.type", Op: "eq", Value: "table"}},
				Actions:   []Action{ActionRead},
			}},
		}})
		p := NewProvider(authn, authz, nil, nil)
		p.UpdateWithEvaluator(authn, authz, nil, ev)
		ctx := ContextWithIdentity(context.Background(), id("admin"))
		if err := AuthorizeTableFunction(ctx, p, "embedded", "read_csv",
			[]string{"/etc/passwd"}, nil); err == nil {
			t.Fatal("a policy set that says nothing about table functions must refuse them")
		}

		// The same identity, with a rule that names the capability.
		ev2 := NewPolicyEvaluator([]AccessControlPolicy{{
			Name: "p", Version: 1, Enabled: true,
			Rules: []PolicyRule{{
				ID: "tf", EffectStr: "allow", Priority: 10,
				Subjects: []Condition{{Attribute: "subject.role", Op: "eq", Value: "r"}},
				Resources: []Condition{
					{Attribute: "resource.type", Op: "eq", Value: ResourceTableFunction},
					{Attribute: "resource.host", Op: "eq", Value: "files.internal"},
				},
				Actions: []Action{ActionRead},
			}},
		}})
		p.UpdateWithEvaluator(authn, authz, nil, ev2)
		if err := AuthorizeTableFunction(ctx, p, "embedded", "read_json",
			[]string{"https://files.internal/a.json"}, nil); err != nil {
			t.Errorf("the policy names this host: %v", err)
		}
		if err := AuthorizeTableFunction(ctx, p, "embedded", "read_json",
			[]string{"https://elsewhere.example/a.json"}, nil); err == nil {
			t.Error("a host the policy does not name must be refused")
		}
	})
}

func TestTableFunctionResourceDescribesTheDestination(t *testing.T) {
	home, _ := os.UserHomeDir()

	cases := []struct {
		name  string
		fn    string
		args  []string
		want  map[string]string
		unset []string
	}{
		{
			name: "a local path is cleaned",
			fn:   "read_csv", args: []string{"/data/../etc/passwd"},
			want: map[string]string{"path": "/etc/passwd"}, unset: []string{"url", "host"},
		},
		{
			name: "a home-relative path is expanded",
			fn:   "read_csv", args: []string{"~/x.csv"},
			want: map[string]string{"path": filepath.Join(home, "x.csv")},
		},
		{
			name: "a glob is the pattern as written",
			fn:   "read_parquet", args: []string{"/data/*.parquet"},
			want: map[string]string{"path": "/data/*.parquet"},
		},
		{
			name: "an http source carries url and host",
			fn:   "read_json", args: []string{"http://10.0.0.5:8080/a.json"},
			want:  map[string]string{"url": "http://10.0.0.5:8080/a.json", "host": "10.0.0.5"},
			unset: []string{"path"},
		},
		{
			name: "a URL connection string carries the host, never the password",
			fn:   "postgres_query", args: []string{"postgres://u:hunter2@db.internal:5432/app", "SELECT 1"},
			want:  map[string]string{"host": "db.internal:5432"},
			unset: []string{"path", "url"},
		},
		{
			name: "a key-value connection string carries the host too",
			fn:   "postgres_scan", args: []string{"host=db.internal port=5432 user=u password=hunter2", "t"},
			want: map[string]string{"host": "db.internal:5432"},
		},
		{
			name: "an unparseable connection string carries no host, so a scoped policy refuses",
			fn:   "mysql_query", args: []string{"u:hunter2@tcp(db.internal:3306)/app", "SELECT 1"},
			want: map[string]string{}, unset: []string{"host"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res := TableFunctionResource(c.fn, c.args, nil)
			if res.Type != ResourceTableFunction {
				t.Errorf("Type = %q, want %q", res.Type, ResourceTableFunction)
			}
			if res.Name != strings.ToLower(c.fn) {
				t.Errorf("Name = %q, want %q", res.Name, strings.ToLower(c.fn))
			}
			for k, want := range c.want {
				if got := res.Attributes.GetString(k); got != want {
					t.Errorf("attribute %q = %q, want %q", k, got, want)
				}
			}
			for _, k := range c.unset {
				if got := res.Attributes.Get(k); got != nil {
					t.Errorf("attribute %q = %v, want unset", k, got)
				}
			}
			// Whatever else the resource carries, it never carries a secret.
			for k, v := range res.Attributes {
				if s, ok := v.(string); ok && strings.Contains(s, "hunter2") {
					t.Errorf("attribute %q leaks the connection password: %q", k, s)
				}
			}
		})
	}
}

// MigrateRBACToABAC turns a `roles:` block into policies, and those policies
// are the shipped enforcement path for every deployment that has not written
// ABAC by hand. A role rule must not confer the table-function capability —
// before #943 a role with `tables: ["*"]` emitted a rule with NO resource
// condition at all, which matched every resource including `read_csv`.
func TestMigratedRolesGrantTableFunctionsOnlyToAdmins(t *testing.T) {
	policies, err := MigrateRBACToABAC([]RoleConfig{
		{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
		{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
		{Name: "ops", Tables: []string{"*"}, Allow: []string{"admin"}},
		{Name: "scoped", Tables: []string{"users"}, Allow: []string{"read"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev := NewPolicyEvaluator(policies)
	env := Environment{Protocol: "embedded"}
	tf := TableFunctionResource("read_csv", []string{"/etc/passwd"}, nil)
	tbl := Resource{Type: "table", Name: "users"}

	for _, c := range []struct {
		role                 string
		perms                []string
		wantTable, wantTFunc bool
	}{
		{"reader", []string{"read"}, true, false},
		{"writer", []string{"read", "write"}, true, false},
		{"ops", []string{"admin"}, true, true},
		{"scoped", []string{"read"}, true, false},
	} {
		subj := (&Identity{Name: "u", Role: c.role, Tables: []string{"*"}, Perms: c.perms}).ToSubject()
		if got := ev.Evaluate(subj, tbl, ActionRead, env).Allowed; got != c.wantTable {
			t.Errorf("role %q reading table users: allowed = %v, want %v", c.role, got, c.wantTable)
		}
		if got := ev.Evaluate(subj, tf, ActionRead, env).Allowed; got != c.wantTFunc {
			t.Errorf("role %q reading read_csv: allowed = %v, want %v", c.role, got, c.wantTFunc)
		}
	}
}
