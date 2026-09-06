package server

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestGRPCDDLRequiresWritePermission is #934's gate.
//
// A role with `allow: [read]` created arbitrary tables and permanently dropped
// existing ones over gRPC: the interceptor authenticated the key and every
// method ran unauthorized. The HTTP door has always refused the same two
// operations with 403 (server.go handleCreateTable / handleDeleteTable), so
// this was a protocol-specific bypass of a rule the product already had.
//
// Both halves of the boundary are asserted, on both provider shapes: the
// reader is refused AND the catalog is unchanged (a refusal that dropped the
// table first would satisfy an error-code-only test), and the writer succeeds
// AND the side effect is there.
func TestGRPCDDLRequiresWritePermission(t *testing.T) {
	for _, shape := range []string{grpcAuthzLegacy, grpcAuthzABAC} {
		t.Run(shape, func(t *testing.T) {
			rig := grpcAuthzUp(t, shape, grpcAuthzConfig())

			// --- the read-only identity: refused, with nothing changed ---
			_, err := rig.client.CreateTable(grpcAuthzCtx("reader-key"), &wadjetv1.CreateTableRequest{
				Name:    "unauthorized",
				Columns: []*wadjetv1.ColumnDef{{Name: "id", Type: "BIGINT"}},
			})
			if got := grpcAuthzCode(err); got != codes.PermissionDenied {
				t.Errorf("reader CreateTable: code %v, want PermissionDenied (err %v)", got, err)
			}
			if grpcAuthzHasTable(t, rig.cat, "unauthorized") {
				t.Error("reader CreateTable was refused but the table exists: the refusal came after the catalog write")
			}

			_, err = rig.client.DropTable(grpcAuthzCtx("reader-key"), &wadjetv1.DropTableRequest{Name: "protected"})
			if got := grpcAuthzCode(err); got != codes.PermissionDenied {
				t.Errorf("reader DropTable: code %v, want PermissionDenied (err %v)", got, err)
			}
			if !grpcAuthzHasTable(t, rig.cat, "protected") {
				t.Error("reader DropTable was refused but the table is gone: the refusal came after the drop")
			}

			// if_exists must not turn the refusal into a success: the swallow
			// is for a table that is not there, never for a caller who may not
			// drop one.
			_, err = rig.client.DropTable(grpcAuthzCtx("reader-key"),
				&wadjetv1.DropTableRequest{Name: "protected", IfExists: true})
			if got := grpcAuthzCode(err); got != codes.PermissionDenied {
				t.Errorf("reader DropTable(if_exists): code %v, want PermissionDenied (err %v)", got, err)
			}
			if !grpcAuthzHasTable(t, rig.cat, "protected") {
				t.Error("reader DropTable(if_exists) was refused but the table is gone")
			}

			// A refusal precedes request validation: an empty request from an
			// unauthorized caller is still PermissionDenied, not a hint about
			// the shape of the call it may not make.
			_, err = rig.client.CreateTable(grpcAuthzCtx("reader-key"), &wadjetv1.CreateTableRequest{})
			if got := grpcAuthzCode(err); got != codes.PermissionDenied {
				t.Errorf("reader CreateTable(empty): code %v, want PermissionDenied (err %v)", got, err)
			}

			// The message is auth.RequirePermission's own text, so this door
			// says what the embedded DDL door and the HTTP door say for the
			// same refusal. Naming the permission is the actionable half.
			if err == nil || !strings.Contains(err.Error(), `"write" permission required`) {
				t.Errorf("reader CreateTable message %v\n  want it to name the missing permission", err)
			}

			// --- the writer: allowed, with the side effect ---
			for _, key := range []string{"writer-key", "admin-key"} {
				name := "made_by_" + key
				if _, err := rig.client.CreateTable(grpcAuthzCtx(key), &wadjetv1.CreateTableRequest{
					Name:    name,
					Columns: []*wadjetv1.ColumnDef{{Name: "id", Type: "BIGINT"}},
				}); err != nil {
					t.Fatalf("%s CreateTable: %v", key, err)
				}
				if !grpcAuthzHasTable(t, rig.cat, name) {
					t.Errorf("%s CreateTable returned OK but the table does not exist", key)
				}
				if _, err := rig.client.DropTable(grpcAuthzCtx(key), &wadjetv1.DropTableRequest{Name: name}); err != nil {
					t.Fatalf("%s DropTable: %v", key, err)
				}
				if grpcAuthzHasTable(t, rig.cat, name) {
					t.Errorf("%s DropTable returned OK but the table still exists", key)
				}
			}

			// --- no credential: still the interceptor's Unauthenticated ---
			_, err = rig.client.CreateTable(grpcAuthzCtx(""), &wadjetv1.CreateTableRequest{
				Name:    "anonymous",
				Columns: []*wadjetv1.ColumnDef{{Name: "id", Type: "BIGINT"}},
			})
			if got := grpcAuthzCode(err); got != codes.Unauthenticated {
				t.Errorf("anonymous CreateTable: code %v, want Unauthenticated (err %v)", got, err)
			}
			if grpcAuthzHasTable(t, rig.cat, "anonymous") {
				t.Error("anonymous CreateTable was refused but the table exists")
			}
		})
	}
}

// TestGRPCDDLWithoutAuthProviderIsUnchanged is the other half of the fix's
// boundary: with no provider — the embedded/dev shape, and every existing gRPC
// test in this package — nothing is enforced and nothing changes.
func TestGRPCDDLWithoutAuthProviderIsUnchanged(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("init catalog: %v", err)
	}
	srv := NewGRPCServer(GRPCConfig{Catalog: cat}, slog.Default())

	if _, err := srv.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name:    "free",
		Columns: []*wadjetv1.ColumnDef{{Name: "id", Type: "BIGINT"}},
	}); err != nil {
		t.Fatalf("CreateTable with no provider: %v", err)
	}
	if !grpcAuthzHasTable(t, cat, "free") {
		t.Fatal("CreateTable with no provider did not create the table")
	}
	if _, err := srv.DropTable(ctx, &wadjetv1.DropTableRequest{Name: "free"}); err != nil {
		t.Fatalf("DropTable with no provider: %v", err)
	}
	if grpcAuthzHasTable(t, cat, "free") {
		t.Fatal("DropTable with no provider did not drop the table")
	}
	// The shape check still runs — the authorization gate is in front of it,
	// not instead of it.
	if _, err := srv.CreateTable(ctx, &wadjetv1.CreateTableRequest{}); grpcAuthzCode(err) != codes.InvalidArgument {
		t.Errorf("CreateTable(empty) with no provider: %v, want InvalidArgument", err)
	}
}
