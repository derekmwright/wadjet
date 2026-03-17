package server

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	wadjetv1 "github.com/citc-tech/wadjet/gen/wadjet/v1"
	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func setupGRPCTestWithAuth(t *testing.T) (wadjetv1.WadjetServiceClient, func()) {
	t.Helper()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatalf("init catalog: %v", err)
	}

	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "valid-key", Name: "test-user", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	logger := slog.Default()
	srv := NewGRPCServer(GRPCConfig{
		Catalog:      cat,
		AuthProvider: provider,
	}, logger)

	lis := bufconn.Listen(bufSize)
	srv.server = grpc.NewServer(
		grpc.UnaryInterceptor(srv.unaryAuthInterceptor()),
		grpc.StreamInterceptor(srv.streamAuthInterceptor()),
	)
	wadjetv1.RegisterWadjetServiceServer(srv.server, srv)

	go func() {
		if err := srv.server.Serve(lis); err != nil {
			// Server was stopped
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}

	client := wadjetv1.NewWadjetServiceClient(conn)

	cleanup := func() {
		conn.Close()
		srv.server.GracefulStop()
	}

	return client, cleanup
}

func TestGRPC_Auth_ValidToken(t *testing.T) {
	client, cleanup := setupGRPCTestWithAuth(t)
	defer cleanup()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer valid-key")
	resp, err := client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables with valid token: %v", err)
	}
	// Should succeed (empty tables)
	if resp.Tables == nil {
		resp.Tables = []string{}
	}
}

func TestGRPC_Auth_InvalidToken(t *testing.T) {
	client, cleanup := setupGRPCTestWithAuth(t)
	defer cleanup()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer wrong-key")
	_, err := client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err == nil {
		t.Fatal("expected error for invalid token")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestGRPC_Auth_MissingMetadata(t *testing.T) {
	client, cleanup := setupGRPCTestWithAuth(t)
	defer cleanup()

	// Call without any metadata (gRPC will still send metadata, but no auth header)
	_, err := client.ListTables(context.Background(), &wadjetv1.ListTablesRequest{})
	if err == nil {
		t.Fatal("expected error for missing credentials")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unauthenticated {
		t.Errorf("expected Unauthenticated, got %v", st.Code())
	}
}

func TestGRPC_Auth_TokenWithoutBearerPrefix(t *testing.T) {
	client, cleanup := setupGRPCTestWithAuth(t)
	defer cleanup()

	// Send token directly without "Bearer " prefix
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "valid-key")
	resp, err := client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables with plain token: %v", err)
	}
	_ = resp
}

// --- grpcAuthenticateContext unit tests ---

func TestGRPCAuthenticateContext_HealthBypass(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")

	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "k", Name: "n", Role: "admin"}},
		Roles:   []auth.RoleConfig{{Name: "admin", Tables: []string{"*"}, Allow: []string{"read"}}},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	srv := NewGRPCServer(GRPCConfig{
		Catalog:      cat,
		AuthProvider: provider,
	}, nil)

	// Health check should bypass auth
	ctx := context.Background()
	newCtx, err := srv.grpcAuthenticateContext(ctx, "/grpc.health.v1.Health/Check")
	if err != nil {
		t.Fatalf("health check should bypass auth: %v", err)
	}
	if newCtx == nil {
		t.Error("expected non-nil context from health check bypass")
	}
}

func TestGRPCAuthenticateContext_DisabledAuth(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")

	cfg := auth.Config{Enabled: false}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	srv := NewGRPCServer(GRPCConfig{
		Catalog:      cat,
		AuthProvider: provider,
	}, nil)

	ctx := context.Background()
	newCtx, err := srv.grpcAuthenticateContext(ctx, "/wadjet.v1.WadjetService/Query")
	if err != nil {
		t.Fatalf("disabled auth should pass through: %v", err)
	}
	if newCtx == nil {
		t.Error("expected non-nil context")
	}
}

// --- authServerStream ---

func TestAuthServerStream_Context(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "test")
	s := &authServerStream{ctx: ctx}
	if s.Context() != ctx {
		t.Error("expected overridden context")
	}
}

// --- GRPCServer Shutdown ---

func TestGRPCServer_Shutdown_NilServer(t *testing.T) {
	srv := &GRPCServer{}
	// Should not panic
	srv.Shutdown()
}

// --- NewGRPCServer nil logger ---

func TestNewGRPCServer_NilLogger(t *testing.T) {
	srv := NewGRPCServer(GRPCConfig{}, nil)
	if srv == nil {
		t.Fatal("expected non-nil GRPCServer")
	}
	if srv.logger == nil {
		t.Error("expected default logger")
	}
}
