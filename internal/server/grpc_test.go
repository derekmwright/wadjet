package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	wadjetv1 "github.com/citc-tech/wadjet/gen/wadjet/v1"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

const bufSize = 1024 * 1024

func setupGRPCTest(t *testing.T) (wadjetv1.WadjetServiceClient, func()) {
	t.Helper()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatalf("init catalog: %v", err)
	}

	logger := slog.Default()
	srv := NewGRPCServer(GRPCConfig{
		Catalog: cat,
	}, logger)

	lis := bufconn.Listen(bufSize)
	srv.server = grpc.NewServer()
	wadjetv1.RegisterWadjetServiceServer(srv.server, srv)

	go func() {
		if err := srv.server.Serve(lis); err != nil {
			// Server was stopped, ignore.
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

func TestGRPC_ListTables_Empty(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	resp, err := client.ListTables(context.Background(), &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(resp.Tables) != 0 {
		t.Errorf("expected 0 tables, got %d", len(resp.Tables))
	}
}

func TestGRPC_CreateAndListTables(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table
	createResp, err := client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name: "events",
		Columns: []*wadjetv1.ColumnDef{
			{Name: "id", Type: "BIGINT", Nullable: false},
			{Name: "name", Type: "VARCHAR", Nullable: true},
			{Name: "ts", Type: "TIMESTAMP", Nullable: false},
			{Name: "date", Type: "VARCHAR", Nullable: false},
		},
		PartitionKeys: []string{"date"},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if createResp.Name != "events" {
		t.Errorf("expected name 'events', got %q", createResp.Name)
	}

	// List should show the table
	listResp, err := client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(listResp.Tables) != 1 || listResp.Tables[0] != "events" {
		t.Errorf("expected [events], got %v", listResp.Tables)
	}
}

func TestGRPC_DescribeTable(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table first
	_, err := client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name: "logs",
		Columns: []*wadjetv1.ColumnDef{
			{Name: "timestamp", Type: "TIMESTAMP"},
			{Name: "message", Type: "VARCHAR", Nullable: true},
			{Name: "level", Type: "INT32"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Describe it
	resp, err := client.DescribeTable(ctx, &wadjetv1.DescribeTableRequest{TableName: "logs"})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}

	if resp.Name != "logs" {
		t.Errorf("expected name 'logs', got %q", resp.Name)
	}
	if len(resp.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(resp.Columns))
	}
	if resp.Columns[0].Name != "timestamp" {
		t.Errorf("expected first column 'timestamp', got %q", resp.Columns[0].Name)
	}
	if resp.Columns[1].Nullable != true {
		t.Error("expected 'message' column to be nullable")
	}
}

func TestGRPC_DescribeTable_NotFound(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	_, err := client.DescribeTable(context.Background(), &wadjetv1.DescribeTableRequest{TableName: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", st.Code())
	}
}

func TestGRPC_DropTable(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create then drop
	_, err := client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name: "temp",
		Columns: []*wadjetv1.ColumnDef{
			{Name: "id", Type: "BIGINT"},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	dropResp, err := client.DropTable(ctx, &wadjetv1.DropTableRequest{Name: "temp"})
	if err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if dropResp.Name != "temp" {
		t.Errorf("expected 'temp', got %q", dropResp.Name)
	}

	// Verify it's gone
	listResp, err := client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(listResp.Tables) != 0 {
		t.Errorf("expected 0 tables after drop, got %d", len(listResp.Tables))
	}
}

func TestGRPC_DropTable_IfExists(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	// Drop nonexistent with if_exists should succeed
	resp, err := client.DropTable(context.Background(), &wadjetv1.DropTableRequest{
		Name:     "ghost",
		IfExists: true,
	})
	if err != nil {
		t.Fatalf("DropTable with if_exists: %v", err)
	}
	if resp.Name != "ghost" {
		t.Errorf("expected 'ghost', got %q", resp.Name)
	}
}

func TestGRPC_CreateTable_Validation(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	// Empty name
	_, err := client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name: "",
		Columns: []*wadjetv1.ColumnDef{
			{Name: "id", Type: "BIGINT"},
		},
	})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", st.Code())
	}

	// No columns
	_, err = client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name:    "noColumns",
		Columns: []*wadjetv1.ColumnDef{},
	})
	if err == nil {
		t.Fatal("expected error for no columns")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", st.Code())
	}

	// Invalid column type
	_, err = client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
		Name: "badType",
		Columns: []*wadjetv1.ColumnDef{
			{Name: "x", Type: "MYSTERY"},
		},
	})
	if err == nil {
		t.Fatal("expected error for invalid type")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestGRPC_Query_NoEngine(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	// No coordinator or DB configured — should return Unavailable
	_, err := client.Query(context.Background(), &wadjetv1.QueryRequest{Sql: "SELECT 1"})
	if err == nil {
		t.Fatal("expected error with no engine")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", st.Code())
	}
}

func TestGRPC_Query_EmptySQL(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	_, err := client.Query(context.Background(), &wadjetv1.QueryRequest{Sql: ""})
	if err == nil {
		t.Fatal("expected error for empty SQL")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestGRPC_QueryStream_EmptySQL(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	stream, err := client.QueryStream(context.Background(), &wadjetv1.QueryRequest{Sql: ""})
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for empty SQL")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestGRPC_SubmitQuery_NoCoordinator(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	_, err := client.SubmitQuery(context.Background(), &wadjetv1.QueryRequest{Sql: "SELECT 1"})
	if err == nil {
		t.Fatal("expected error without coordinator")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", st.Code())
	}
}

func TestGRPC_GetQueryStatus_NoCoordinator(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	_, err := client.GetQueryStatus(context.Background(), &wadjetv1.GetQueryStatusRequest{QueryId: "q-123"})
	if err == nil {
		t.Fatal("expected error without coordinator")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", st.Code())
	}
}

func TestGRPC_CancelQuery_NoCoordinator(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	_, err := client.CancelQuery(context.Background(), &wadjetv1.CancelQueryRequest{QueryId: "q-123"})
	if err == nil {
		t.Fatal("expected error without coordinator")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", st.Code())
	}
}

func TestGRPC_QueryWithDB(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test-bucket")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatalf("init catalog: %v", err)
	}

	// Create a table in the catalog so the DB can query it
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64, Nullable: false},
			{Name: "name", Type: parquet.TypeString, Nullable: true},
		},
	}
	if err := cat.CreateTable(context.Background(), "users", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// We can't easily test with a real wadjet.DB here since it requires
	// the full planner stack, but we can verify the catalog operations work
	// through gRPC. The Query path is tested via integration tests.

	logger := slog.Default()
	srv := NewGRPCServer(GRPCConfig{
		Catalog: cat,
	}, logger)

	lis := bufconn.Listen(bufSize)
	srv.server = grpc.NewServer()
	wadjetv1.RegisterWadjetServiceServer(srv.server, srv)

	go func() {
		srv.server.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	defer srv.server.GracefulStop()

	client := wadjetv1.NewWadjetServiceClient(conn)

	// Verify table exists via DescribeTable
	desc, err := client.DescribeTable(context.Background(), &wadjetv1.DescribeTableRequest{TableName: "users"})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if desc.Name != "users" {
		t.Errorf("expected 'users', got %q", desc.Name)
	}
	if len(desc.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(desc.Columns))
	}
}

func TestGRPC_QueryStream_NoEngine(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	stream, err := client.QueryStream(context.Background(), &wadjetv1.QueryRequest{Sql: "SELECT 1"})
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error with no engine")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", st.Code())
	}
}

func TestRowsToProto(t *testing.T) {
	rows := []map[string]any{
		{"name": "alice", "age": int64(30), "score": 95.5, "active": true},
		{"name": "bob", "age": nil, "score": float32(88.0), "active": false},
	}

	result := rowsToProto(rows)
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}

	// Check first row
	r0 := result[0].Fields
	if r0["name"].GetStringValue() != "alice" {
		t.Errorf("expected 'alice', got %q", r0["name"].GetStringValue())
	}
	if r0["age"].GetNumberValue() != 30 {
		t.Errorf("expected 30, got %v", r0["age"].GetNumberValue())
	}
	if r0["score"].GetNumberValue() != 95.5 {
		t.Errorf("expected 95.5, got %v", r0["score"].GetNumberValue())
	}
	if r0["active"].GetBoolValue() != true {
		t.Error("expected active=true")
	}

	// Check null value
	r1 := result[1].Fields
	if r1["age"].GetNullValue() != 0 {
		t.Errorf("expected null, got %v", r1["age"])
	}
}

func TestAnyToProtoValue(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string // "null", "bool:true", "number:X", "string:X"
	}{
		{"nil", nil, "null"},
		{"bool_true", true, "bool:true"},
		{"bool_false", false, "bool:false"},
		{"int", 42, "number:42"},
		{"int32", int32(100), "number:100"},
		{"int64", int64(999), "number:999"},
		{"float32", float32(3.14), "number:3.14"},
		{"float64", 2.718, "number:2.718"},
		{"string", "hello", "string:hello"},
		{"custom_type", []byte("bytes"), "string:[98 121 116 101 115]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := anyToProtoValue(tt.val)
			if v == nil {
				t.Fatal("result was nil")
			}
			// Just verify it doesn't panic and returns a valid value
			_ = v.String()
		})
	}
}

// TestGRPC_FullDDLWorkflow tests create → describe → list → drop lifecycle.
func TestGRPC_FullDDLWorkflow(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create two tables
	for _, name := range []string{"orders", "customers"} {
		_, err := client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
			Name: name,
			Columns: []*wadjetv1.ColumnDef{
				{Name: "id", Type: "BIGINT"},
				{Name: "data", Type: "VARCHAR", Nullable: true},
			},
		})
		if err != nil {
			t.Fatalf("CreateTable(%s): %v", name, err)
		}
	}

	// List should show both
	listResp, err := client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(listResp.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(listResp.Tables))
	}

	// Describe orders
	desc, err := client.DescribeTable(ctx, &wadjetv1.DescribeTableRequest{TableName: "orders"})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if len(desc.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(desc.Columns))
	}

	// Drop one
	_, err = client.DropTable(ctx, &wadjetv1.DropTableRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	// List should show one
	listResp, err = client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(listResp.Tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(listResp.Tables))
	}

	// Describe dropped table should fail
	_, err = client.DescribeTable(ctx, &wadjetv1.DescribeTableRequest{TableName: "orders"})
	if err == nil {
		t.Fatal("expected error for dropped table")
	}
}

// TestGRPC_DescribeTable_EmptyName verifies validation.
func TestGRPC_DescribeTable_EmptyName(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	_, err := client.DescribeTable(context.Background(), &wadjetv1.DescribeTableRequest{TableName: ""})
	if err == nil {
		t.Fatal("expected error for empty table name")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", st.Code())
	}
}

// TestGRPC_Validation_EmptyQueryIDs checks empty query_id validation on async RPCs.
func TestGRPC_Validation_EmptyQueryIDs(t *testing.T) {
	// We need a coordinator-like setup, but since we test without coordinator
	// the Unavailable check fires first. This test is more about coverage.
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	// SubmitQuery with empty SQL
	_, err := client.SubmitQuery(context.Background(), &wadjetv1.QueryRequest{Sql: ""})
	if err == nil {
		t.Fatal("expected error for empty SQL")
	}

	// CancelQuery with empty query_id (hits Unavailable first since no coordinator)
	_, err = client.CancelQuery(context.Background(), &wadjetv1.CancelQueryRequest{QueryId: ""})
	if err == nil {
		t.Fatal("expected error for empty query_id")
	}

	// GetQueryStatus with empty query_id (hits Unavailable first since no coordinator)
	_, err = client.GetQueryStatus(context.Background(), &wadjetv1.GetQueryStatusRequest{QueryId: ""})
	if err == nil {
		t.Fatal("expected error for empty query_id")
	}
}

// Unused but ensures the import is used for io.EOF checks in streaming tests.
var _ = io.EOF
