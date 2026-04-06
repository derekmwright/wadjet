package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/wadjet"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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

func TestHTTPSSE(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	hs, err := ServeHTTP(srv, ":0")
	if err != nil {
		t.Fatalf("starting HTTP server: %v", err)
	}
	defer hs.Shutdown()

	addr := hs.Addr()

	// Connect to SSE endpoint
	resp, err := http.Get(fmt.Sprintf("http://%s/sse", addr))
	if err != nil {
		t.Fatalf("connecting to SSE: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("expected text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}

	// Read the endpoint event
	scanner := strings.NewReader("")
	_ = scanner
	buf := make([]byte, 4096)
	n, err := resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("reading SSE: %v", err)
	}
	sseData := string(buf[:n])

	if !strings.Contains(sseData, "event: endpoint") {
		t.Fatalf("expected endpoint event, got: %s", sseData)
	}

	// Extract session ID from the data line
	var sessionID string
	for _, line := range strings.Split(sseData, "\n") {
		if strings.HasPrefix(line, "data: ") {
			endpoint := strings.TrimPrefix(line, "data: ")
			parts := strings.Split(endpoint, "sessionId=")
			if len(parts) == 2 {
				sessionID = parts[1]
			}
		}
	}
	if sessionID == "" {
		t.Fatalf("could not extract session ID from: %s", sseData)
	}

	// POST a tools/list request
	reqBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/list",
	})
	postResp, err := http.Post(
		fmt.Sprintf("http://%s/message?sessionId=%s", addr, sessionID),
		"application/json",
		bytes.NewReader(reqBody),
	)
	if err != nil {
		t.Fatalf("posting message: %v", err)
	}
	postResp.Body.Close()

	if postResp.StatusCode != http.StatusAccepted {
		t.Errorf("expected 202 Accepted, got %d", postResp.StatusCode)
	}

	// Read the SSE response
	n, err = resp.Body.Read(buf)
	if err != nil {
		t.Fatalf("reading SSE response: %v", err)
	}
	sseResp := string(buf[:n])
	if !strings.Contains(sseResp, "event: message") {
		t.Errorf("expected message event, got: %s", sseResp)
	}
	if !strings.Contains(sseResp, "list_tables") {
		t.Errorf("expected tool list in response, got: %s", sseResp)
	}
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
