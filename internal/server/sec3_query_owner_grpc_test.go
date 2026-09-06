package server

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// The gRPC half of query ownership (#936). `GetQueryStatus` and `CancelQuery`
// had the same query-ID-only shape the HTTP handlers did, and the coordinator
// is where the decision lives now, so the two doors cannot disagree: the
// refusal is `codes.PermissionDenied` carrying the same message text the HTTP
// door's 403 carries.

// sec3GRPCOwnerFixture is a bufconn server with the PRODUCTION interceptors
// and a real coordinator behind it.
func sec3GRPCOwnerFixture(t *testing.T) (wadjetv1.WadjetServiceClient, *coordinator.Coordinator) {
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
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	coord := coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test",
	}, cat, nc, js, logger)

	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "admin-key", Name: "admin-user", Role: "admin"},
			{Key: "reader-key", Name: "reader-user", Role: "reader"},
		},
		Roles: []auth.RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	if err := coord.SetAuthProvider(provider); err != nil {
		t.Fatalf("attaching the auth provider: %v", err)
	}

	srv := NewGRPCServer(GRPCConfig{Catalog: cat, Coord: coord, AuthProvider: provider}, logger)
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
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.server.GracefulStop()
	})
	return wadjetv1.NewWadjetServiceClient(conn), coord
}

func sec3BearerCtx(key string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+key)
}

func TestGRPCQueryStatusAndCancelAreOwnerOrAdmin(t *testing.T) {
	client, coord := sec3GRPCOwnerFixture(t)
	coord.Tracker().Register("victim-query", "SELECT secret_value FROM secret",
		auth.IdentitySnapshot{Name: "admin-user", Role: "admin", Method: "apikey"},
		map[string]*coordinator.StageInfo{}, nil)
	coord.Tracker().Start("victim-query")

	// A non-owner reads nothing.
	st, err := client.GetQueryStatus(sec3BearerCtx("reader-key"),
		&wadjetv1.GetQueryStatusRequest{QueryId: "victim-query"})
	if err == nil {
		t.Fatalf("GetQueryStatus answered a non-owner with sql %q", st.GetSql())
	}
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Errorf("GetQueryStatus refused with %v; want PermissionDenied (%v)", code, err)
	}
	if strings.Contains(err.Error(), "secret_value") {
		t.Errorf("the refusal published the SQL it refused: %v", err)
	}

	// And cancels nothing: the state after the refusal is the assertion.
	if _, err := client.CancelQuery(sec3BearerCtx("reader-key"),
		&wadjetv1.CancelQueryRequest{QueryId: "victim-query"}); err == nil {
		t.Fatal("CancelQuery let a non-owner cancel another principal's query")
	} else if code := status.Code(err); code != codes.PermissionDenied {
		t.Errorf("CancelQuery refused with %v; want PermissionDenied (%v)", code, err)
	}
	if info := coord.Tracker().Get("victim-query"); info == nil ||
		info.State != coordinator.QueryStateRunning {
		t.Fatalf("the refused cancel changed the query: %+v", info)
	}

	// The owner still reads and cancels its own.
	if st, err := client.GetQueryStatus(sec3BearerCtx("admin-key"),
		&wadjetv1.GetQueryStatusRequest{QueryId: "victim-query"}); err != nil {
		t.Fatalf("the owner was refused its own status: %v", err)
	} else if st.GetSql() != "SELECT secret_value FROM secret" {
		t.Errorf("the owner got sql %q", st.GetSql())
	}
	if _, err := client.CancelQuery(sec3BearerCtx("admin-key"),
		&wadjetv1.CancelQueryRequest{QueryId: "victim-query"}); err != nil {
		t.Fatalf("the owner was refused its own cancel: %v", err)
	}
	if info := coord.Tracker().Get("victim-query"); info == nil ||
		info.State != coordinator.QueryStateCancelled {
		t.Fatalf("the owner's cancel did not cancel: %+v", info)
	}
}

// A query submitted over gRPC records its submitter, and the refusal text is
// the one the HTTP door carries for the same refusal.
func TestGRPCRefusalTextMatchesTheOtherDoors(t *testing.T) {
	client, coord := sec3GRPCOwnerFixture(t)
	coord.Tracker().Register("victim-query", "SELECT 1",
		auth.IdentitySnapshot{Name: "admin-user", Role: "admin"},
		map[string]*coordinator.StageInfo{}, nil)

	_, err := client.GetQueryStatus(sec3BearerCtx("reader-key"),
		&wadjetv1.GetQueryStatusRequest{QueryId: "victim-query"})
	if err == nil {
		t.Fatal("a non-owner was not refused")
	}
	want := `permission denied: query "victim-query" belongs to another principal`
	if got := status.Convert(err).Message(); got != want {
		t.Errorf("refusal message %q; want %q", got, want)
	}
}
