package auth

import (
	"sync"
	"testing"
)

func TestProviderBasic(t *testing.T) {
	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "test-key", Name: "test", Role: "reader"},
		},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}
	authn, authz := New(cfg)
	p := NewProvider(authn, authz, nil, nil)

	if !p.Enabled() {
		t.Fatal("expected provider to be enabled")
	}
	if p.Authenticator() == nil {
		t.Fatal("expected non-nil authenticator")
	}
	if p.Authorizer() == nil {
		t.Fatal("expected non-nil authorizer")
	}
	if p.Policies() != nil {
		t.Fatal("expected nil policies")
	}
}

func TestProviderUpdate(t *testing.T) {
	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "key1", Name: "test1", Role: "reader"},
		},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}
	authn, authz := New(cfg)
	p := NewProvider(authn, authz, nil, nil)

	// Update with new config
	cfg2 := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "key2", Name: "test2", Role: "admin"},
		},
		Roles: []RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		},
	}
	authn2, authz2 := New(cfg2)
	policies := NewPolicySet()
	policies.Add(&AccessPolicy{Table: "users", Role: "admin", Columns: map[string]ColumnPolicy{"ssn": ColumnDeny}})

	p.Update(authn2, authz2, policies)

	if p.Authenticator() != authn2 {
		t.Fatal("authenticator not updated")
	}
	if p.Authorizer() != authz2 {
		t.Fatal("authorizer not updated")
	}
	if p.Policies() != policies {
		t.Fatal("policies not updated")
	}
}

func TestProviderUpdateFromConfig(t *testing.T) {
	p := NewProvider(nil, nil, nil, nil)

	if p.Enabled() {
		t.Fatal("expected disabled with nil auth")
	}

	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "ck_test", Name: "grafana", Role: "reader"},
		},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}
	policyCfgs := []PolicyConfig{
		{Table: "users", Role: "reader", Columns: map[string]string{"email": "mask"}},
	}

	p.UpdateFromConfig(cfg, policyCfgs)

	if !p.Enabled() {
		t.Fatal("expected enabled after config update")
	}
	if p.Policies() == nil {
		t.Fatal("expected policies after config update")
	}
}

func TestProviderConcurrentAccess(t *testing.T) {
	cfg := Config{
		Enabled: true,
		APIKeys: []APIKeyDef{
			{Key: "key1", Name: "test", Role: "reader"},
		},
		Roles: []RoleConfig{
			{Name: "reader", Tables: []string{"events"}, Allow: []string{"read"}},
		},
	}
	authn, authz := New(cfg)
	p := NewProvider(authn, authz, nil, nil)

	var wg sync.WaitGroup
	// Concurrent readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = p.Enabled()
				_ = p.Authenticator()
				_ = p.Authorizer()
				_ = p.Policies()
			}
		}()
	}

	// Concurrent writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			cfg2 := Config{
				Enabled: true,
				APIKeys: []APIKeyDef{
					{Key: "key-new", Name: "new", Role: "writer"},
				},
				Roles: []RoleConfig{
					{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
				},
			}
			authn2, authz2 := New(cfg2)
			p.Update(authn2, authz2, nil)
		}
	}()

	wg.Wait()
}

func TestProviderDisabledWhenNilAuth(t *testing.T) {
	p := NewProvider(nil, nil, nil, nil)
	if p.Enabled() {
		t.Fatal("expected disabled with nil auth")
	}
}
