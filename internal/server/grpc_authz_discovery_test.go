package server

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestGRPCDiscoveryFollowsTheTableDecision is #935's gate.
//
// `ListTables` published the whole catalog and `DescribeTable` handed out every
// column name, type and partition key to any identity that authenticated —
// object names, sensitive column names and an exact target map, across roles.
// The HTTP door already filtered its listing and refused a table the role
// cannot access; this door asked nobody.
//
// Both now go through auth.TableAccess / auth.VisibleTables, the ONE decision,
// so metadata says what a read of the same relation would say.
func TestGRPCDiscoveryFollowsTheTableDecision(t *testing.T) {
	for _, shape := range []string{grpcAuthzLegacy, grpcAuthzABAC} {
		t.Run(shape, func(t *testing.T) {
			rig := grpcAuthzUp(t, shape, grpcAuthzConfig())

			// --- the read-only identity sees only the table it may read ---
			list, err := rig.client.ListTables(grpcAuthzCtx("reader-key"), &wadjetv1.ListTablesRequest{})
			if err != nil {
				t.Fatalf("reader ListTables: %v", err)
			}
			if !slices.Equal(list.Tables, []string{"allowed"}) {
				t.Errorf("reader ListTables = %v, want [allowed]", list.Tables)
			}

			desc, err := rig.client.DescribeTable(grpcAuthzCtx("reader-key"),
				&wadjetv1.DescribeTableRequest{TableName: "secret"})
			if got := grpcAuthzCode(err); got != codes.PermissionDenied {
				t.Errorf("reader DescribeTable(secret): code %v, want PermissionDenied (err %v)", got, err)
			}
			if desc != nil && len(desc.Columns) > 0 {
				t.Errorf("reader DescribeTable(secret) returned %d columns on a refusal", len(desc.Columns))
			}
			// The refusal names the table, exactly as the data door's 42501
			// does — nothing new is disclosed by saying which table was denied.
			if err == nil || !strings.Contains(err.Error(), `permission denied for table "secret"`) {
				t.Errorf("reader DescribeTable(secret) message %v\n  want the 42501 text naming the table", err)
			}

			// The table it MAY read still describes, in full.
			desc, err = rig.client.DescribeTable(grpcAuthzCtx("reader-key"),
				&wadjetv1.DescribeTableRequest{TableName: "allowed"})
			if err != nil {
				t.Fatalf("reader DescribeTable(allowed): %v", err)
			}
			if len(desc.Columns) != 2 {
				t.Errorf("reader DescribeTable(allowed) = %d columns, want 2", len(desc.Columns))
			}

			// --- the wildcard identities see everything ---
			for _, key := range []string{"writer-key", "admin-key"} {
				list, err := rig.client.ListTables(grpcAuthzCtx(key), &wadjetv1.ListTablesRequest{})
				if err != nil {
					t.Fatalf("%s ListTables: %v", key, err)
				}
				for _, want := range []string{"allowed", "secret", "protected"} {
					if !slices.Contains(list.Tables, want) {
						t.Errorf("%s ListTables = %v, missing %q", key, list.Tables, want)
					}
				}
				if _, err := rig.client.DescribeTable(grpcAuthzCtx(key),
					&wadjetv1.DescribeTableRequest{TableName: "secret"}); err != nil {
					t.Errorf("%s DescribeTable(secret): %v", key, err)
				}
			}

			// --- a table that does not exist is still NotFound, for an
			// identity allowed to ask. The refusal above must not become this
			// door's answer for every miss.
			if _, err := rig.client.DescribeTable(grpcAuthzCtx("admin-key"),
				&wadjetv1.DescribeTableRequest{TableName: "no_such_table"}); grpcAuthzCode(err) != codes.NotFound {
				t.Errorf("admin DescribeTable(no_such_table): %v, want NotFound", err)
			}
		})
	}
}

// TestGRPCDiscoveryHonoursAnExplicitABACDeny is the cell legacy RBAC filtering
// cannot answer: the role's `tables:` list CONTAINS `secret`, and an ABAC rule
// denies it. ABAC takes precedence (deny-overrides), so the deny governs the
// listing and the describe — filtering on `FilterTables` alone would have
// published both.
func TestGRPCDiscoveryHonoursAnExplicitABACDeny(t *testing.T) {
	cfg := grpcAuthzConfig()
	cfg.Roles[0].Tables = []string{"allowed", "secret"} // the reader's role now grants both

	deny := auth.AccessControlPolicy{
		Name:    "deny-secret-to-readers",
		Version: 1,
		Enabled: true,
		Rules: []auth.PolicyRule{{
			ID:          "deny-secret-to-readers",
			EffectStr:   "deny",
			Priority:    1,
			Description: "readers may not see the secret table",
			Subjects:    []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "reader"}},
			Resources:   []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "secret"}},
			Actions:     []auth.Action{auth.ActionRead, auth.ActionDescribe},
		}},
	}

	rig := grpcAuthzUp(t, grpcAuthzABAC, cfg, deny)

	list, err := rig.client.ListTables(grpcAuthzCtx("reader-key"), &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("reader ListTables: %v", err)
	}
	if slices.Contains(list.Tables, "secret") {
		t.Errorf("reader ListTables = %v: an explicit ABAC deny did not hide the table", list.Tables)
	}
	if !slices.Contains(list.Tables, "allowed") {
		t.Errorf("reader ListTables = %v: the deny took away a table it does not name", list.Tables)
	}
	if _, err := rig.client.DescribeTable(grpcAuthzCtx("reader-key"),
		&wadjetv1.DescribeTableRequest{TableName: "secret"}); grpcAuthzCode(err) != codes.PermissionDenied {
		t.Errorf("reader DescribeTable(secret) under an explicit deny: %v, want PermissionDenied", err)
	}
}

// TestGRPCDiscoveryWithoutAuthProviderIsUnchanged: with no provider the listing
// is the catalog's own and DescribeTable answers — the dev/embedded shape, and
// every existing gRPC test in this package.
func TestGRPCDiscoveryWithoutAuthProviderIsUnchanged(t *testing.T) {
	ctx := context.Background()
	cat := catalog.NewWithStore(objstore.NewMemStore(), "test-bucket")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("init catalog: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if err := cat.CreateTable(ctx, name, grpcAuthzSchema(), nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	srv := NewGRPCServer(GRPCConfig{Catalog: cat}, slog.Default())

	list, err := srv.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables with no provider: %v", err)
	}
	if !slices.Equal(list.Tables, []string{"a", "b"}) {
		t.Errorf("ListTables with no provider = %v, want [a b]", list.Tables)
	}
	if _, err := srv.DescribeTable(ctx, &wadjetv1.DescribeTableRequest{TableName: "a"}); err != nil {
		t.Errorf("DescribeTable with no provider: %v", err)
	}
	if _, err := srv.DescribeTable(ctx, &wadjetv1.DescribeTableRequest{}); grpcAuthzCode(err) != codes.InvalidArgument {
		t.Errorf("DescribeTable(empty) with no provider: %v, want InvalidArgument", err)
	}
}
