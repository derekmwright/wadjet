package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	wadjetdb "github.com/derekmwright/wadjet/wadjet"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const streamBatchSize = 1000

// GRPCConfig holds configuration for the gRPC server.
type GRPCConfig struct {
	Addr           string
	Catalog        *catalog.Catalog
	Coord          *coordinator.Coordinator // nil = standalone
	DB             *wadjetdb.DB             // nil = distributed
	TLSConfig      *tls.Config              // nil = plain gRPC
	MaxConnections int                      // 0 = unlimited
	AuthProvider   *auth.Provider           // nil = no auth enforcement
}

// GRPCServer implements the WadjetService gRPC API.
type GRPCServer struct {
	wadjetv1.UnimplementedWadjetServiceServer

	catalog      *catalog.Catalog
	coord        *coordinator.Coordinator
	db           *wadjetdb.DB
	logger       *slog.Logger
	server       *grpc.Server
	addr         string
	tlsConfig    *tls.Config
	maxConns     int
	authProvider *auth.Provider
}

// NewGRPCServer creates a new gRPC server.
func NewGRPCServer(cfg GRPCConfig, logger *slog.Logger) *GRPCServer {
	if logger == nil {
		logger = slog.Default()
	}
	// Same attach rule as every other door: a policy set installed against a
	// catalog is BOUND to it here (ADR-0033 rule 2). This door executes
	// through the coordinator or the DB, so it would inherit a set they
	// bound — but "somebody else attached first" is caller discipline, not a
	// property, and #882 is what that costs.
	auth.AttachProvider(context.Background(), cfg.AuthProvider, cfg.Catalog, logger)
	return &GRPCServer{
		catalog:      cfg.Catalog,
		coord:        cfg.Coord,
		db:           cfg.DB,
		logger:       logger,
		addr:         cfg.Addr,
		tlsConfig:    cfg.TLSConfig,
		maxConns:     cfg.MaxConnections,
		authProvider: cfg.AuthProvider,
	}
}

// Start begins serving gRPC on the configured address.
func (g *GRPCServer) Start() error {
	lis, err := net.Listen("tcp", g.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	if g.maxConns > 0 {
		lis = netutil.LimitListener(lis, g.maxConns)
	}

	var opts []grpc.ServerOption
	opts = append(opts, grpc.MaxConcurrentStreams(256))
	if g.tlsConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(g.tlsConfig)))
	}
	if g.authProvider != nil {
		opts = append(opts,
			grpc.UnaryInterceptor(g.unaryAuthInterceptor()),
			grpc.StreamInterceptor(g.streamAuthInterceptor()),
		)
	}

	g.server = grpc.NewServer(opts...)
	wadjetv1.RegisterWadjetServiceServer(g.server, g)

	// Register health service
	hs := health.NewServer()
	healthpb.RegisterHealthServer(g.server, hs)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("wadjet.v1.WadjetService", healthpb.HealthCheckResponse_SERVING)

	g.logger.Info("gRPC server listening", "addr", g.addr, "tls", g.tlsConfig != nil,
		"max_connections", g.maxConns)
	return g.server.Serve(lis)
}

// Shutdown gracefully stops the gRPC server.
func (g *GRPCServer) Shutdown() {
	if g.server != nil {
		g.server.GracefulStop()
	}
}

// Query executes a SQL query and returns all results.
func (g *GRPCServer) Query(ctx context.Context, req *wadjetv1.QueryRequest) (*wadjetv1.QueryResponse, error) {
	if req.Sql == "" {
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}

	if g.coord != nil {
		result, err := g.coord.ExecuteSQL(ctx, req.Sql)
		if err != nil {
			return nil, grpcQueryError(err)
		}
		rows, rowsErr := result.Rows()
		if rowsErr != nil {
			return nil, status.Errorf(codes.Internal, "reading result batches: %v", rowsErr)
		}
		return &wadjetv1.QueryResponse{
			QueryId: result.QueryID,
			Columns: result.Columns,
			Rows:    rowsToProto(rows),
			Stats: &wadjetv1.QueryStats{
				TotalRows: result.TotalRows,
				Elapsed:   durationpb.New(result.Elapsed),
				Plan:      result.Plan,
			},
		}, nil
	}

	if g.db != nil {
		result, err := g.db.Query(ctx, req.Sql)
		if err != nil {
			return nil, grpcQueryError(err)
		}
		return &wadjetv1.QueryResponse{
			Columns: result.Columns,
			// The positional form rides along whenever the engine built one,
			// which is exactly when two columns share a name (B1).
			Rows: rowsToProtoWithValues(result.Rows, result.RowValues),
			Stats: &wadjetv1.QueryStats{
				TotalRows: int64(len(result.Rows)),
				Plan:      result.Plan,
			},
		}, nil
	}

	return nil, status.Error(codes.Unavailable, "no query engine available")
}

// QueryStream executes a SQL query and streams result batches.
func (g *GRPCServer) QueryStream(req *wadjetv1.QueryRequest, stream wadjetv1.WadjetService_QueryStreamServer) error {
	if req.Sql == "" {
		return status.Error(codes.InvalidArgument, "sql is required")
	}

	if g.coord != nil {
		result, err := g.coord.ExecuteSQL(stream.Context(), req.Sql)
		if err != nil {
			return grpcQueryError(err)
		}
		cs := &chunkStreamer{
			stream:  stream,
			columns: result.Columns,
			stats: &wadjetv1.QueryStats{
				TotalRows: result.TotalRows,
				Elapsed:   durationpb.New(result.Elapsed),
				Plan:      result.Plan,
			},
		}
		return streamResultBatches(cs, result)
	}

	if g.db != nil {
		result, err := g.db.Query(stream.Context(), req.Sql)
		if err != nil {
			return grpcQueryError(err)
		}
		// Legacy embedded path: db.Query returns pre-boxed rows, so the
		// full materialization already happened inside it — chunk as-is.
		cs := &chunkStreamer{
			stream:  stream,
			columns: result.Columns,
			stats: &wadjetv1.QueryStats{
				TotalRows: int64(len(result.Rows)),
				Plan:      result.Plan,
			},
		}
		if err := cs.pushRows(result.Rows, result.RowValues); err != nil {
			return err
		}
		return cs.finish()
	}

	return status.Error(codes.Unavailable, "no query engine available")
}

// streamResultBatches boxes and sends the coord result one RecordBatch at a
// time. The previous implementation called result.Rows(), materializing the
// entire result as map[string]any rows up front — a concurrent ~3-10x boxed
// copy held alive alongside result.Batches for the whole stream, violating
// SQLResult's own "prefer iterating Batches" contract on the default-on
// :9090 server. Peak boxed residency is now one batch (~2K rows) plus the
// one held-back chunk, and each batch reference is dropped after boxing so
// the columnar copy can be reclaimed while the stream proceeds. Lazy
// (gather-spilled) results replay from local scratch one batch at a time.
func streamResultBatches(cs *chunkStreamer, result *coordinator.SQLResult) error {
	stream := result.Stream()
	defer stream.Close()
	ctx := context.Background()
	for {
		b, err := stream.Next(ctx)
		if err != nil {
			return status.Errorf(codes.Internal, "reading result batches: %v", err)
		}
		if b == nil {
			break
		}
		// The batch's own positional form, boxed from the same batch so the
		// two are index-aligned by construction (B1) — and boxed ONLY when
		// the schema publishes a name twice, because that is the only case a
		// map-boxed row loses a value and this path's peak boxed residency is
		// deliberately one batch (see this function's own comment).
		var vals [][]any
		if batchNeedsPositionalRows(b) {
			vals = b.ToRowValues()
		}
		if err := cs.pushRows(b.ToRows(), vals); err != nil {
			return err
		}
	}
	return cs.finish()
}

// batchNeedsPositionalRows reports whether a batch's schema publishes one name
// TWICE, which is exactly when boxing its rows into a `map<string, Value>`
// loses a value — `SELECT g + 1, g + 2, g + 3` is three columns called
// `?column?` since #732. It mirrors `exec.hasDuplicateColumnName`, which the
// collecting sink asks for the same reason; the two cannot share a helper
// because that one is unexported and keyed on the sink's own schema.
func batchNeedsPositionalRows(b *batch.RecordBatch) bool {
	if b == nil || len(b.Schema) < 2 {
		return false
	}
	seen := make(map[string]bool, len(b.Schema))
	for _, c := range b.Schema {
		if seen[c.Name] {
			return true
		}
		seen[c.Name] = true
	}
	return false
}

// chunkStreamer sends row chunks of at most streamBatchSize, holding back
// one chunk so the final send carries IsLast + stats. Columns ride only the
// first response; an empty result still produces a single columns+stats
// response — both exactly the legacy wire behavior.
type chunkStreamer struct {
	stream      wadjetv1.WadjetService_QueryStreamServer
	columns     []string
	stats       *wadjetv1.QueryStats
	pending     []*wadjetv1.Row
	havePending bool
	sentColumns bool
}

// pushRows chunks one boxed batch of rows onto the stream.
//
// `values` is the same rows POSITIONALLY, index-aligned with `rows`, and it is
// carried for the reason the unary RPC carries it: a `map<string, Value>`
// cannot hold two output columns that publish ONE NAME, and since #732
// `SELECT g + 1, g + 2, g + 3` is three columns called `?column?`. Passing nil
// here is what left the STREAMING RPC sending three column names beside a
// one-key map with the LAST value in it, while the unary RPC sent all three
// (round-2 review B1). It may be nil or short — the engine materialises the
// positional form only when the names are not unique — and rowsToProtoWithValues
// leaves `Row.values` empty for every row it does not cover.
func (cs *chunkStreamer) pushRows(rows []map[string]any, values [][]any) error {
	for i := 0; i < len(rows); i += streamBatchSize {
		end := i + streamBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		if cs.havePending {
			if err := cs.send(cs.pending, false); err != nil {
				return err
			}
		}
		var chunkVals [][]any
		if i < len(values) {
			vend := end
			if vend > len(values) {
				vend = len(values)
			}
			chunkVals = values[i:vend]
		}
		cs.pending = rowsToProtoWithValues(rows[i:end], chunkVals)
		cs.havePending = true
	}
	return nil
}

func (cs *chunkStreamer) finish() error {
	return cs.send(cs.pending, true)
}

func (cs *chunkStreamer) send(rows []*wadjetv1.Row, isLast bool) error {
	resp := &wadjetv1.QueryStreamResponse{
		Rows:   rows,
		IsLast: isLast,
	}
	if !cs.sentColumns {
		resp.Columns = cs.columns
		cs.sentColumns = true
	}
	if isLast {
		resp.Stats = cs.stats
	}
	cs.pending = nil
	return cs.stream.Send(resp)
}

// SubmitQuery submits an async query (distributed mode only).
func (g *GRPCServer) SubmitQuery(ctx context.Context, req *wadjetv1.QueryRequest) (*wadjetv1.SubmitQueryResponse, error) {
	if g.coord == nil {
		return nil, status.Error(codes.Unavailable, "async queries require distributed mode")
	}
	if req.Sql == "" {
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}

	queryID, plan, err := g.coord.SubmitSQL(ctx, req.Sql)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "submit error: %v", err)
	}

	return &wadjetv1.SubmitQueryResponse{
		QueryId: queryID,
		Plan:    plan,
	}, nil
}

// GetQueryStatus returns the status of an async query.
func (g *GRPCServer) GetQueryStatus(ctx context.Context, req *wadjetv1.GetQueryStatusRequest) (*wadjetv1.GetQueryStatusResponse, error) {
	if g.coord == nil {
		return nil, status.Error(codes.Unavailable, "async queries require distributed mode")
	}
	if req.QueryId == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	qs, err := g.coord.GetQueryStatus(req.QueryId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}

	resp := &wadjetv1.GetQueryStatusResponse{
		QueryId:   qs.QueryID,
		Sql:       qs.SQL,
		State:     qs.State,
		Elapsed:   durationpb.New(qs.Elapsed),
		TotalRows: qs.TotalRows,
		Error:     qs.Error,
	}
	for _, s := range qs.Stages {
		resp.Stages = append(resp.Stages, &wadjetv1.StageStatus{
			StageId:     s.StageID,
			Type:        s.Type,
			TotalTasks:  int32(s.TotalTasks),
			DoneTasks:   int32(s.DoneTasks),
			FailedTasks: int32(s.FailedTasks),
		})
	}

	return resp, nil
}

// CancelQuery cancels a running query.
func (g *GRPCServer) CancelQuery(ctx context.Context, req *wadjetv1.CancelQueryRequest) (*wadjetv1.CancelQueryResponse, error) {
	if g.coord == nil {
		return nil, status.Error(codes.Unavailable, "async queries require distributed mode")
	}
	if req.QueryId == "" {
		return nil, status.Error(codes.InvalidArgument, "query_id is required")
	}

	if err := g.coord.CancelQuery(req.QueryId); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}

	return &wadjetv1.CancelQueryResponse{
		QueryId: req.QueryId,
		State:   "cancelled",
	}, nil
}

// ListTables returns the table names this identity may read.
//
// The listing is filtered by the SAME decision that governs reading the table
// (auth.VisibleTables → auth.TableAccess), so a name an identity may not read
// is not published to it either. That is the product's position rather than
// PostgreSQL's — `\d` shows every relation to anyone — and it is the behavior
// the HTTP door already had (`FilterTables` on /v1/tables and SHOW TABLES)
// while this door published the whole catalog to any authenticated caller
// (#935). Explicit ABAC denies govern it, because the evaluator is what
// TableAccess asks whenever one is installed.
func (g *GRPCServer) ListTables(ctx context.Context, _ *wadjetv1.ListTablesRequest) (*wadjetv1.ListTablesResponse, error) {
	tables, err := g.catalog.ListTables(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing tables: %v", err)
	}
	return &wadjetv1.ListTablesResponse{Tables: auth.VisibleTables(ctx, g.authProvider, tables)}, nil
}

// DescribeTable returns a table's schema to an identity that may read it.
//
// Column names, types and the partition design are an exact target map, and
// this door handed them to any authenticated caller for any table (#935). The
// decision is the table's own — auth.TableAccess with ActionRead — so it agrees
// with what a SELECT on the same relation would be told, and it is taken BEFORE
// the catalog read: a refusal reveals nothing, not even whether the table is
// there.
//
// The shape check comes first here, unlike the DDL RPCs: those ask a
// permission-only question that needs no request, while this one cannot decide
// anything without a table name.
func (g *GRPCServer) DescribeTable(ctx context.Context, req *wadjetv1.DescribeTableRequest) (*wadjetv1.DescribeTableResponse, error) {
	if req.TableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
	}

	// Decide on the CATALOG spelling: an unquoted identifier folds at the
	// lexer (#731) and a policy bound to `Users` must police a request that
	// spelled it `users`. The catalog read below keeps the caller's spelling,
	// which is byte-exact (GetTable) — resolving there too would make this RPC
	// newly find tables it used to miss, a functional change that is not this
	// fix. Where the two disagree the read simply misses, exactly as before.
	name := req.TableName
	if g.catalog != nil {
		name = g.catalog.ResolveTableName(req.TableName)
	}
	if err := auth.TableAccess(ctx, g.authProvider, name, auth.ActionRead); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	table, err := g.catalog.GetTable(ctx, req.TableName)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "table %q: %v", req.TableName, err)
	}

	resp := &wadjetv1.DescribeTableResponse{
		Name:          table.Name,
		PartitionKeys: table.PartitionKeys,
	}
	for _, col := range table.Schema.Columns {
		resp.Columns = append(resp.Columns, &wadjetv1.ColumnInfo{
			Name:     col.Name,
			Type:     col.Type.String(),
			Nullable: col.Nullable,
		})
	}

	return resp, nil
}

// CreateTable creates a new table.
func (g *GRPCServer) CreateTable(ctx context.Context, req *wadjetv1.CreateTableRequest) (*wadjetv1.CreateTableResponse, error) {
	// The interceptor proved WHO; creating a relation still has to ask MAY,
	// and it asks BEFORE the request is even inspected — a refusal must
	// precede every side effect, and an unauthorized caller learns nothing
	// about the request it was not allowed to make (#934).
	if err := grpcRequireWrite(g.authProvider, ctx); err != nil {
		return nil, err
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.Columns) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one column is required")
	}

	columns := make([]parquet.Column, len(req.Columns))
	for i, cd := range req.Columns {
		// parquet.DeclaredColumn, not ParseTypeID: a DECIMAL's precision and
		// scale live in the type text, and reading only the TypeID here gave
		// every DECIMAL column created over gRPC a Precision 0, Scale 0
		// declaration (#647 review).
		col, err := parquet.DeclaredColumn(cd.Name, cd.Type, cd.Nullable)
		if err != nil {
			// The SQLSTATE travels in the message: status.Errorf("%v") drops
			// it, and a client that gets 22023 from the SQL doors and a bare
			// string from this one cannot treat the two the same
			// (#647 re-review).
			return nil, status.Errorf(codes.InvalidArgument, "column %q: %s", cd.Name, sqlErrorText(err))
		}
		columns[i] = col
	}

	schema := parquet.Schema{Columns: columns}
	if err := g.catalog.CreateTable(ctx, req.Name, schema, req.PartitionKeys); err != nil {
		// sqlErrorText for the same reason the column arm above gives: the
		// catalog's refusals carry a class (42P07 for a duplicate, 42701 for a
		// duplicate column, 42602 for a name the object store cannot hold) and
		// status.Errorf("%v") drops it. This is also the ONLY door where a
		// relation name arrives without passing the lexer, so it is the one
		// where the class is load-bearing.
		return nil, status.Errorf(codes.Internal, "creating table: %s", sqlErrorText(err))
	}

	return &wadjetv1.CreateTableResponse{Name: req.Name}, nil
}

// grpcQueryError maps an engine error onto this door's status code.
//
// An authorization refusal is codes.PermissionDenied, not codes.Internal. Both
// SQL RPCs wrapped EVERY engine error as Internal, so a reader whose DELETE was
// refused with SQLSTATE 42501 before a single row was touched was told the
// server had failed — a client cannot tell "you may not do that" from "we
// broke", retries the second and gives up on the first, and an operator reading
// the code alone sees an outage where there is a working control. The other
// doors carry the class: HTTP 403, pgwire 42501 (SECURITY ADDENDUM 6).
//
// It branches on the CLASS the error CARRIES — the SQLSTATE 42501 that
// auth.EnforceDMLPolicies / auth.TableAccess attach, or auth.ErrUnauthorized
// that auth.RequirePermission wraps — never on message text. A door that
// pattern-matched sentences would silently reopen the day a message is
// reworded, and would be a second copy of the decision besides.
//
// One refusal does NOT reach this yet: a SELECT denied at the table level
// returns a plain error from internal/auth/plan_enforce.go, with no class on
// any door, so it still crosses as Internal here. Attaching 42501 there is an
// internal/auth change (it moves pgwire's SQLSTATE too); when it lands this
// mapping needs no edit.
func grpcQueryError(err error) error {
	if sqlerr.StateOf(err) == "42501" || errors.Is(err, auth.ErrUnauthorized) {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	return status.Errorf(codes.Internal, "query error: %v", err)
}

// grpcRequireWrite is this door's DDL authorization: the caller in ctx must
// hold `write`, and a refusal is codes.PermissionDenied.
//
// It is a two-line wrapper over auth.RequirePermission — the SHARED decision —
// and deliberately not a decision of its own: the identity's permissions are
// read by the Authorizer and nowhere else, so a door cannot drift from the
// rule the HTTP DDL handlers and the embedded DB apply. The message is
// RequirePermission's own text, so the three doors say the same sentence for
// the same refusal; only the transport's class differs (403 / 42501 /
// PermissionDenied).
//
// A missing identity under enabled auth is PermissionDenied here rather than
// Unauthenticated because it cannot arise on this door — the interceptor
// refuses an unauthenticated call with Unauthenticated before any method runs
// — so reaching it means an identity was expected and is not there, which is
// RequirePermission's fail-closed arm, not an authentication challenge.
func grpcRequireWrite(provider *auth.Provider, ctx context.Context) error {
	if err := auth.RequirePermission(provider, ctx, "write"); err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	return nil
}

// sqlErrorText renders an error with its SQLSTATE when it carries one, so a
// gRPC message says the same thing the HTTP body and the pgwire ErrorResponse
// do for the same refusal.
func sqlErrorText(err error) string {
	if state := sqlerr.StateOf(err); state != "" {
		return fmt.Sprintf("%v (SQLSTATE %s)", err, state)
	}
	return err.Error()
}

// DropTable removes a table.
func (g *GRPCServer) DropTable(ctx context.Context, req *wadjetv1.DropTableRequest) (*wadjetv1.DropTableResponse, error) {
	// Before the catalog, and before `if_exists` gets a say: a refusal that
	// came after the drop attempt would have already destroyed the table, and
	// one that came after `if_exists` swallowed the miss would answer OK to a
	// caller who may not drop anything at all (#934).
	if err := grpcRequireWrite(g.authProvider, ctx); err != nil {
		return nil, err
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	if err := g.catalog.DropTable(ctx, req.Name); err != nil {
		if req.IfExists {
			return &wadjetv1.DropTableResponse{Name: req.Name}, nil
		}
		return nil, status.Errorf(codes.NotFound, "table %q: %v", req.Name, err)
	}

	return &wadjetv1.DropTableResponse{Name: req.Name}, nil
}

// grpcAuthenticateContext extracts a bearer token from gRPC metadata,
// authenticates it, and returns a context with the resolved identity.
// Health check RPCs bypass authentication.
func (g *GRPCServer) grpcAuthenticateContext(ctx context.Context, fullMethod string) (context.Context, error) {
	// Health checks bypass auth
	if strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") {
		return ctx, nil
	}

	if !g.authProvider.Enabled() {
		return ctx, nil
	}
	authn := g.authProvider.Authenticator()
	if authn == nil {
		return ctx, nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	var token string
	if vals := md.Get("authorization"); len(vals) > 0 {
		v := vals[0]
		if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
			token = v[7:]
		} else {
			token = v
		}
	}

	id, err := authn.AuthenticateToken(token)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	g.logger.Debug("gRPC authenticated", "identity", id.String(), "method", fullMethod)
	// The trusted environment this call arrived on (SEC1/#933): the peer
	// address the server observed, never anything the caller sent as
	// metadata. The port is stripped by auth.
	var source string
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		source = p.Addr.String()
	}
	ctx = auth.ContextWithEnvironment(ctx, auth.Environment{SourceIP: source, Protocol: "grpc"})
	return auth.ContextWithIdentity(ctx, id), nil
}

func (g *GRPCServer) unaryAuthInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		newCtx, err := g.grpcAuthenticateContext(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

func (g *GRPCServer) streamAuthInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := g.grpcAuthenticateContext(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		wrapped := &authServerStream{ServerStream: ss, ctx: newCtx}
		return handler(srv, wrapped)
	}
}

// authServerStream wraps a grpc.ServerStream to override the context.
type authServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authServerStream) Context() context.Context { return s.ctx }

// rowsToProto converts Go row maps to protobuf Row messages.
func rowsToProto(rows []map[string]any) []*wadjetv1.Row {
	return rowsToProtoWithValues(rows, nil)
}

// rowsToProtoWithValues is rowsToProto with the POSITIONAL form beside the map.
//
// A `map<string, Value>` cannot represent two output columns that publish ONE
// NAME, and since #732 that is an ordinary result: `SELECT g + 1, g + 2` is two
// columns called `?column?`, so `fields` carries one key while `columns` names
// two — a client zipping the two got the LAST value under the FIRST name and
// nothing under the second. `values` is sent whenever the caller has the
// positional form, which the engine materialises exactly when the names are not
// unique (`CollectSink.ToRowValues`, #513). Nil elsewhere, so an ordinary
// response is byte-identical to before (round-1 review B1).
func rowsToProtoWithValues(rows []map[string]any, values [][]any) []*wadjetv1.Row {
	result := make([]*wadjetv1.Row, len(rows))
	for i, row := range rows {
		fields := make(map[string]*structpb.Value, len(row))
		for k, v := range row {
			fields[k] = anyToProtoValue(v)
		}
		r := &wadjetv1.Row{Fields: fields}
		if i < len(values) {
			r.Values = make([]*structpb.Value, len(values[i]))
			for j, v := range values[i] {
				r.Values[j] = anyToProtoValue(v)
			}
		}
		result[i] = r
	}
	return result
}

// anyToProtoValue converts a Go any value to a protobuf Value.
func anyToProtoValue(v any) *structpb.Value {
	if v == nil {
		return structpb.NewNullValue()
	}
	switch val := v.(type) {
	case bool:
		return structpb.NewBoolValue(val)
	case int:
		return structpb.NewNumberValue(float64(val))
	case int32:
		return structpb.NewNumberValue(float64(val))
	case int64:
		return structpb.NewNumberValue(float64(val))
	case float32:
		return structpb.NewNumberValue(float64(val))
	case float64:
		return structpb.NewNumberValue(val)
	case string:
		return structpb.NewStringValue(val)
	default:
		return structpb.NewStringValue(fmt.Sprint(val))
	}
}
