package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	wadjetv1 "github.com/citc-tech/wadjet/gen/wadjet/v1"
	wadjetdb "github.com/citc-tech/wadjet/wadjet"
	"github.com/citc-tech/wadjet/internal/coordinator"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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
}

// GRPCServer implements the WadjetService gRPC API.
type GRPCServer struct {
	wadjetv1.UnimplementedWadjetServiceServer

	catalog   *catalog.Catalog
	coord     *coordinator.Coordinator
	db        *wadjetdb.DB
	logger    *slog.Logger
	server    *grpc.Server
	addr      string
	tlsConfig *tls.Config
	maxConns  int
}

// NewGRPCServer creates a new gRPC server.
func NewGRPCServer(cfg GRPCConfig, logger *slog.Logger) *GRPCServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &GRPCServer{
		catalog:   cfg.Catalog,
		coord:     cfg.Coord,
		db:        cfg.DB,
		logger:    logger,
		addr:      cfg.Addr,
		tlsConfig: cfg.TLSConfig,
		maxConns:  cfg.MaxConnections,
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
			return nil, status.Errorf(codes.Internal, "query error: %v", err)
		}
		return &wadjetv1.QueryResponse{
			QueryId: result.QueryID,
			Columns: result.Columns,
			Rows:    rowsToProto(result.Rows),
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
			return nil, status.Errorf(codes.Internal, "query error: %v", err)
		}
		return &wadjetv1.QueryResponse{
			Columns: result.Columns,
			Rows:    rowsToProto(result.Rows),
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

	var rows []map[string]any
	var columns []string
	var stats *wadjetv1.QueryStats

	if g.coord != nil {
		result, err := g.coord.ExecuteSQL(stream.Context(), req.Sql)
		if err != nil {
			return status.Errorf(codes.Internal, "query error: %v", err)
		}
		rows = result.Rows
		columns = result.Columns
		stats = &wadjetv1.QueryStats{
			TotalRows: result.TotalRows,
			Elapsed:   durationpb.New(result.Elapsed),
			Plan:      result.Plan,
		}
	} else if g.db != nil {
		result, err := g.db.Query(stream.Context(), req.Sql)
		if err != nil {
			return status.Errorf(codes.Internal, "query error: %v", err)
		}
		rows = result.Rows
		columns = result.Columns
		stats = &wadjetv1.QueryStats{
			TotalRows: int64(len(result.Rows)),
			Plan:      result.Plan,
		}
	} else {
		return status.Error(codes.Unavailable, "no query engine available")
	}

	// Stream rows in batches
	for i := 0; i < len(rows); i += streamBatchSize {
		end := i + streamBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		isLast := end >= len(rows)

		resp := &wadjetv1.QueryStreamResponse{
			Rows:   rowsToProto(rows[i:end]),
			IsLast: isLast,
		}
		// Send columns and stats only on first batch
		if i == 0 {
			resp.Columns = columns
		}
		if isLast {
			resp.Stats = stats
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	// Empty result set
	if len(rows) == 0 {
		return stream.Send(&wadjetv1.QueryStreamResponse{
			Columns: columns,
			Stats:   stats,
			IsLast:  true,
		})
	}

	return nil
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

// ListTables returns all table names.
func (g *GRPCServer) ListTables(ctx context.Context, _ *wadjetv1.ListTablesRequest) (*wadjetv1.ListTablesResponse, error) {
	tables, err := g.catalog.ListTables(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing tables: %v", err)
	}
	return &wadjetv1.ListTablesResponse{Tables: tables}, nil
}

// DescribeTable returns a table's schema.
func (g *GRPCServer) DescribeTable(ctx context.Context, req *wadjetv1.DescribeTableRequest) (*wadjetv1.DescribeTableResponse, error) {
	if req.TableName == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name is required")
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
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.Columns) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one column is required")
	}

	columns := make([]parquet.Column, len(req.Columns))
	for i, cd := range req.Columns {
		typeID, err := parquet.ParseTypeID(cd.Type)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "column %q: %v", cd.Name, err)
		}
		columns[i] = parquet.Column{
			Name:     strings.ToLower(cd.Name),
			Type:     typeID,
			Nullable: cd.Nullable,
		}
	}

	schema := parquet.Schema{Columns: columns}
	if err := g.catalog.CreateTable(ctx, req.Name, schema, req.PartitionKeys); err != nil {
		return nil, status.Errorf(codes.Internal, "creating table: %v", err)
	}

	return &wadjetv1.CreateTableResponse{Name: req.Name}, nil
}

// DropTable removes a table.
func (g *GRPCServer) DropTable(ctx context.Context, req *wadjetv1.DropTableRequest) (*wadjetv1.DropTableResponse, error) {
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

// rowsToProto converts Go row maps to protobuf Row messages.
func rowsToProto(rows []map[string]any) []*wadjetv1.Row {
	result := make([]*wadjetv1.Row, len(rows))
	for i, row := range rows {
		fields := make(map[string]*structpb.Value, len(row))
		for k, v := range row {
			fields[k] = anyToProtoValue(v)
		}
		result[i] = &wadjetv1.Row{Fields: fields}
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
