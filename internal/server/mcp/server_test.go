package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

func setupTestDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	store.MakeBucket(ctx, "test")

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: "test",
	})
	if err != nil {
		t.Fatalf("opening DB: %v", err)
	}

	// Create a test table
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "date", Type: parquet.TypeDate, Nullable: false},
			{Name: "src_ip", Type: parquet.TypeIPv4, Nullable: false},
			{Name: "dst_ip", Type: parquet.TypeIPv4, Nullable: false},
			{Name: "dst_port", Type: parquet.TypePort, Nullable: false},
			{Name: "bytes_in", Type: parquet.TypeInt64, Nullable: true},
			{Name: "protocol", Type: parquet.TypeProtocol, Nullable: false},
		},
	}
	if err := db.CreateTable(ctx, "flow_logs", schema, []string{"date"}); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	return db
}

func sendRPC(t *testing.T, in io.Writer, method string, params any, id int) {
	t.Helper()
	req := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
	}
	if params != nil {
		p, _ := json.Marshal(params)
		req["params"] = json.RawMessage(p)
	}
	data, _ := json.Marshal(req)
	fmt.Fprintf(in, "%s\n", data)
}

func readResponse(t *testing.T, r *bufio.Reader) jsonRPCResponse {
	t.Helper()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var resp jsonRPCResponse
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			t.Fatalf("unmarshaling response %q: %v", line, err)
		}
		return resp
	}
}

// runStdio starts ServeStdio in a goroutine with an io.Pipe for output,
// returning a bufio.Reader that safely reads server responses without races.
func runStdio(t *testing.T, srv *Server, in io.Reader) *bufio.Reader {
	t.Helper()
	pr, pw := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	t.Cleanup(func() { pw.Close() })
	go srv.ServeStdio(ctx, in, pw)
	return bufio.NewReader(pr)
}

func TestInitialize(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test", "version": "1.0"},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var result initializeResult
	json.Unmarshal(data, &result)

	if result.ProtocolVersion != protocolVersion {
		t.Errorf("expected protocol version %s, got %s", protocolVersion, result.ProtocolVersion)
	}
	if result.ServerInfo.Name != "wadjet" {
		t.Errorf("expected server name 'wadjet', got %s", result.ServerInfo.Name)
	}
	if result.Capabilities.Tools == nil {
		t.Error("expected tools capability")
	}
}

func TestToolsList(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/list", nil, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var result toolsListResult
	json.Unmarshal(data, &result)

	expectedTools := map[string]bool{
		"list_tables":    false,
		"describe_table": false,
		"query":          false,
		"explain":        false,
		"list_functions": false,
	}

	for _, tool := range result.Tools {
		if _, ok := expectedTools[tool.Name]; ok {
			expectedTools[tool.Name] = true
		}
	}

	for name, found := range expectedTools {
		if !found {
			t.Errorf("missing tool: %s", name)
		}
	}
}

func TestToolListTables(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name": "list_tables",
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var result callToolResult
	json.Unmarshal(data, &result)

	if result.IsError {
		t.Fatalf("tool returned error: %s", result.Content[0].Text)
	}
	if !strings.Contains(result.Content[0].Text, "flow_logs") {
		t.Errorf("expected result to contain 'flow_logs', got: %s", result.Content[0].Text)
	}
}

func TestToolDescribeTable(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "describe_table",
		"arguments": map[string]any{"table": "flow_logs"},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var result callToolResult
	json.Unmarshal(data, &result)

	if result.IsError {
		t.Fatalf("tool returned error: %s", result.Content[0].Text)
	}

	text := result.Content[0].Text
	// Verify schema details
	if !strings.Contains(text, "src_ip") {
		t.Error("expected src_ip column")
	}
	if !strings.Contains(text, "IPV4") {
		t.Error("expected IPV4 type")
	}
	if !strings.Contains(text, "PORT") {
		t.Error("expected PORT type")
	}
	if !strings.Contains(text, "Network-typed columns") {
		t.Error("expected network type hint")
	}
	if !strings.Contains(text, "Partition keys: date") {
		t.Error("expected partition key info")
	}
}

func TestToolDescribeTableNotFound(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "describe_table",
		"arguments": map[string]any{"table": "nonexistent"},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	data, _ := json.Marshal(resp.Result)
	var result callToolResult
	json.Unmarshal(data, &result)

	if !result.IsError {
		t.Error("expected error for nonexistent table")
	}
}

func TestToolQuery(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "query",
		"arguments": map[string]any{"sql": "SHOW TABLES"},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var result callToolResult
	json.Unmarshal(data, &result)

	if result.IsError {
		t.Fatalf("tool returned error: %s", result.Content[0].Text)
	}

	// Parse the JSON output
	var queryOutput map[string]any
	if err := json.Unmarshal([]byte(result.Content[0].Text), &queryOutput); err != nil {
		t.Fatalf("failed to parse query output: %v", err)
	}

	// Verify compact format
	if _, ok := queryOutput["columns"]; !ok {
		t.Error("expected 'columns' in output")
	}
	if _, ok := queryOutput["rows"]; !ok {
		t.Error("expected 'rows' in output")
	}
	if _, ok := queryOutput["row_count"]; !ok {
		t.Error("expected 'row_count' in output")
	}
}

func TestToolQueryLimit(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	// Query with an explicit limit
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "query",
		"arguments": map[string]any{"sql": "SHOW TABLES", "limit": 5},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var result callToolResult
	json.Unmarshal(data, &result)

	if result.IsError {
		t.Fatalf("tool returned error: %s", result.Content[0].Text)
	}
}

func TestToolExplain(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "explain",
		"arguments": map[string]any{"sql": "SELECT * FROM flow_logs"},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var result callToolResult
	json.Unmarshal(data, &result)

	if result.IsError {
		t.Fatalf("tool returned error: %s", result.Content[0].Text)
	}
	if !strings.Contains(result.Content[0].Text, "Scan") {
		t.Errorf("expected plan to contain 'Scan', got: %s", result.Content[0].Text)
	}
}

func TestPing(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "ping", nil, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}
}

func TestUnknownMethod(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "nonexistent/method", nil, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error == nil {
		t.Fatal("expected error for unknown method")
	}
	if resp.Error.Code != -32601 {
		t.Errorf("expected error code -32601, got %d", resp.Error.Code)
	}
}

// TestMCPQueryEnforcesABAC proves the MCP query tool applies row/column
// security when the server is constructed with an identity against an
// AuthProvider-backed DB. Without identity stamping in serve(), the denied
// column would appear and the row filter would not apply — the exact bypass
// this change closes. A companion NewServer (no identity) run confirms the
// enforcement is driven by the stamped identity, not by the DB alone.
func TestMCPQueryEnforcesABAC(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	store.MakeBucket(ctx, "test")
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatalf("opening DB: %v", err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "severity", Type: parquet.TypeString},
		{Name: "secret_col", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("findings", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "severity": "high", "secret_col": "classified-a"},
		{"id": int64(2), "severity": "low", "secret_col": "classified-b"},
		{"id": int64(3), "severity": "high", "secret_col": "classified-c"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{{
		Name: "test-policy", Version: 1, Enabled: true,
		Rules: []auth.PolicyRule{{
			ID: "restrict-findings", EffectStr: "allow", Priority: 10,
			Subjects:  []auth.Condition{{Attribute: "subject.role", Op: "eq", Value: "analyst"}},
			Resources: []auth.Condition{{Attribute: "resource.name", Op: "eq", Value: "findings"}},
			Actions:   []auth.Action{auth.ActionRead},
			Obligations: []auth.Obligation{
				{Type: "deny_column", Target: "secret_col"},
				{Type: "row_filter", Value: "severity = 'high'"},
			},
		}},
	}})
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "test-key", Name: "analyst", Role: "analyst"}},
		Roles:   []auth.RoleConfig{{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}}},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)
	db.SetAuthProvider(provider)

	identity := &auth.Identity{Name: "analyst", Role: "analyst", Method: "apikey"}

	queryRows := func(t *testing.T, srv *Server) (cols []any, rowCount int) {
		t.Helper()
		in := &bytes.Buffer{}
		sendRPC(t, in, "tools/call", callToolParams{
			Name:      "query",
			Arguments: map[string]any{"sql": "SELECT * FROM findings"},
		}, 1)
		out := runStdio(t, srv, in)
		resp := readResponse(t, out)
		if resp.Error != nil {
			t.Fatalf("query error: %v", resp.Error.Message)
		}
		data, _ := json.Marshal(resp.Result)
		var result callToolResult
		json.Unmarshal(data, &result)
		if len(result.Content) == 0 {
			t.Fatal("empty tool result")
		}
		var payload struct {
			Columns  []any `json:"columns"`
			RowCount int   `json:"row_count"`
		}
		if err := json.Unmarshal([]byte(result.Content[0].Text), &payload); err != nil {
			t.Fatalf("unmarshaling query payload %q: %v", result.Content[0].Text, err)
		}
		return payload.Columns, payload.RowCount
	}

	t.Run("with_identity_enforces_row_and_column_policy", func(t *testing.T) {
		srv := NewServerWithIdentity(db, nil, identity)
		cols, rowCount := queryRows(t, srv)
		for _, c := range cols {
			if c == "secret_col" {
				t.Errorf("denied column 'secret_col' leaked through MCP query: cols=%v", cols)
			}
		}
		if rowCount != 2 {
			t.Errorf("row filter severity='high' not applied: got %d rows, want 2", rowCount)
		}
	})

	t.Run("without_identity_no_enforcement_context", func(t *testing.T) {
		// Sanity anchor: enforcement is identity-driven. A server with no
		// stamped identity (which mcpCmd only permits when no AuthProvider is
		// configured) does not gain policy from the DB alone — proving the
		// stamp in serve() is what carries security, so an unauthenticated
		// network path could never inherit it by accident.
		srv := NewServer(db, nil)
		cols, rowCount := queryRows(t, srv)
		leaked := false
		for _, c := range cols {
			if c == "secret_col" {
				leaked = true
			}
		}
		if !leaked || rowCount != 3 {
			t.Fatalf("expected unenforced result (secret_col present, 3 rows); got cols=%v rows=%d — "+
				"if this now enforces, the identity-driven invariant changed and mcpCmd's fail-closed guard must be re-audited",
				cols, rowCount)
		}
	})
}

func TestToolUnknown(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name": "nonexistent_tool",
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	data, _ := json.Marshal(resp.Result)
	var result callToolResult
	json.Unmarshal(data, &result)

	if !result.IsError {
		t.Error("expected error for unknown tool")
	}
}
