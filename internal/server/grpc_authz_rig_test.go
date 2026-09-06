package server

import (
	"context"
	"log/slog"
	"net"
	"os"
	"slices"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The gRPC authorization fixture: ONE rig, driven through the REAL door — a
// bufconn server carrying the production unary and stream interceptors — with
// identities that are AUTHENTICATED but not authorized. "The middleware is
// present" is not authorization, so nothing here calls a handler method
// directly; every cell is a client RPC.
//
// It stands up in BOTH provider shapes, because they enforce differently and
// only one of them is what a YAML-configured server runs:
//
//   - grpcAuthzLegacy — auth.NewProvider(authn, authz, nil, nil): roles only,
//     no ABAC evaluator. The shape #934/#935 were filed against and the shape
//     this package's own gRPC tests use.
//   - grpcAuthzABAC — provider.UpdateFromConfig(cfg, nil), which auto-migrates
//     `roles:` to ABAC. The shape cmd/wadjet's buildProviderFromConfig ships.
//
// A door decision that held in only one of them would be a door that changes
// its mind when an operator adds an abac_policies block.
const (
	grpcAuthzLegacy = "legacy-roles-only"
	grpcAuthzABAC   = "abac-migrated"
)

type grpcAuthzRig struct {
	client   wadjetv1.WadjetServiceClient
	health   healthpb.HealthClient
	cat      *catalog.Catalog
	provider *auth.Provider
}

// grpcAuthzConfig is the identity fixture: a reader that may read exactly one
// table, a writer, an admin. Every key AUTHENTICATES.
func grpcAuthzConfig() auth.Config {
	return auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "reader-key", Name: "reader", Role: "reader"},
			{Key: "writer-key", Name: "writer", Role: "writer"},
			{Key: "admin-key", Name: "admin", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{"allowed"}, Allow: []string{"read"}},
			{Name: "writer", Tables: []string{"*"}, Allow: []string{"read", "write"}},
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		},
	}
}

func grpcAuthzSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: false},
		{Name: "ssn", Type: parquet.TypeString, Nullable: true},
	}}
}

// grpcAuthzUp stands the door up over an embedded DB (so Query/QueryStream
// have an engine) with tables `allowed`, `secret` and `protected`.
//
// cfg may widen the reader's role; extraABAC rides ALONGSIDE the migrated role
// policies, which is how an explicit deny beside a role that grants the table
// is expressed. Both are ignored in the legacy shape, which has no evaluator.
func grpcAuthzUp(t *testing.T, shape string, cfg auth.Config, extraABAC ...auth.AccessControlPolicy) grpcAuthzRig {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	cat := db.Catalog()
	// The tables exist BEFORE the policy set is installed: a policy set binds
	// its relation names to the catalog once at install and refuses a name the
	// catalog does not hold (#882), so installing first would refuse the load
	// rather than exercise the door.
	for _, name := range []string{"allowed", "secret", "protected"} {
		if err := cat.CreateTable(ctx, name, grpcAuthzSchema(), nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, logger)

	srv := NewGRPCServer(GRPCConfig{Catalog: cat, DB: db, AuthProvider: provider}, logger)
	if shape == grpcAuthzABAC {
		migrated, merr := auth.MigrateRBACToABAC(cfg.Roles, nil)
		if merr != nil {
			t.Fatalf("migrate roles: %v", merr)
		}
		if err := provider.UpdateFromConfig(cfg, nil, append(migrated, extraABAC...)...); err != nil {
			t.Fatalf("install policy set: %v", err)
		}
	}
	if err := db.SetAuthProvider(provider); err != nil {
		t.Fatalf("set auth provider: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv.server = grpc.NewServer(
		grpc.UnaryInterceptor(srv.unaryAuthInterceptor()),
		grpc.StreamInterceptor(srv.streamAuthInterceptor()),
	)
	wadjetv1.RegisterWadjetServiceServer(srv.server, srv)
	// The health service too, exactly as Start() registers it: the census has
	// to be able to call the one method that bypasses authentication by
	// design, or "health is exempt" would be an untested claim.
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv.server, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	go func() { _ = srv.server.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close(); srv.server.GracefulStop() })

	return grpcAuthzRig{
		client:   wadjetv1.NewWadjetServiceClient(conn),
		health:   healthpb.NewHealthClient(conn),
		cat:      cat,
		provider: provider,
	}
}

// grpcAuthzCtx is a client context carrying the API key as a bearer token, the
// way docs/grpc-api.md tells an operator to send it. "" sends no credential.
func grpcAuthzCtx(key string) context.Context {
	if key == "" {
		return context.Background()
	}
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+key)
}

// grpcAuthzHasTable reports whether the catalog holds name — the SIDE EFFECT
// half of every cell. An authorization gate that asserted only the error code
// would pass over a refusal that dropped the table first.
func grpcAuthzHasTable(t *testing.T, cat *catalog.Catalog, name string) bool {
	t.Helper()
	tables, err := cat.ListTables(context.Background())
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	return slices.Contains(tables, name)
}

func grpcAuthzCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	st, _ := status.FromError(err)
	return st.Code()
}
