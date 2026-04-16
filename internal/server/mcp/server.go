// Package mcp implements a Model Context Protocol (MCP) server for Wadjet.
// This allows AI agents (Claude, Cursor, etc.) to discover tables, inspect schemas,
// and execute SQL queries against a Wadjet instance.
//
// Supports two transports:
//   - stdio: JSON-RPC over stdin/stdout (for CLI integration with Claude Desktop/Code)
//   - HTTP+SSE: Server-Sent Events for server-to-client, HTTP POST for client-to-server
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/wadjet"
)

const (
	protocolVersion = "2024-11-05"
	serverName      = "wadjet"
	serverVersion   = "0.1.0"
)

// Server is an MCP server that exposes Wadjet tools to AI agents.
type Server struct {
	db     *wadjet.DB
	logger *slog.Logger
}

// NewServer creates a new MCP server backed by a Wadjet DB.
func NewServer(db *wadjet.DB, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{db: db, logger: logger}
}

// ServeStdio runs the MCP server over stdin/stdout using JSON-RPC 2.0.
// This is the transport used by Claude Desktop and Claude Code.
// It blocks until the input stream is closed or ctx is cancelled.
func (s *Server) ServeStdio(ctx context.Context, in io.Reader, out io.Writer) error {
	s.logger.Info("MCP server starting on stdio")
	t := &stdioTransport{
		scanner: bufio.NewScanner(in),
		out:     out,
		mu:      &sync.Mutex{},
	}
	t.scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
	return s.serve(ctx, t)
}

// ServeHTTP starts an HTTP+SSE MCP server on the given address.
// Returns after the listener is established. Call Shutdown to stop.
func ServeHTTP(s *Server, addr string) (*HTTPServer, error) {
	hs := &HTTPServer{
		mcp:    s,
		done:   make(chan struct{}),
		logger: s.logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", hs.handleSSE)
	mux.HandleFunc("/message", hs.handleMessage)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("mcp http listen: %w", err)
	}
	hs.listener = ln
	hs.httpServer = &http.Server{Handler: mux}

	s.logger.Info("MCP HTTP+SSE server listening", "addr", ln.Addr().String())
	go hs.httpServer.Serve(ln)
	return hs, nil
}

// HTTPServer wraps the HTTP+SSE transport lifecycle.
type HTTPServer struct {
	mcp        *Server
	httpServer *http.Server
	listener   net.Listener
	done       chan struct{}
	logger     *slog.Logger
}

// Addr returns the listener address.
func (h *HTTPServer) Addr() string {
	if h.listener == nil {
		return ""
	}
	return h.listener.Addr().String()
}

// Shutdown stops the HTTP server gracefully.
func (h *HTTPServer) Shutdown() {
	close(h.done)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.httpServer.Shutdown(ctx)
}

// JSON-RPC 2.0 types

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"` // number, string, or null
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// MCP protocol types

type serverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type initializeResult struct {
	ProtocolVersion    string         `json:"protocolVersion"`
	Capabilities       capabilities   `json:"capabilities"`
	ServerInfo         serverInfo     `json:"serverInfo"`
	WadjetCapabilities map[string]any `json:"wadjet,omitempty"`
}

type capabilities struct {
	Tools     *toolsCapability     `json:"tools,omitempty"`
	Resources *resourcesCapability `json:"resources,omitempty"`
}

type resourcesCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

type toolsCapability struct {
	ListChanged bool `json:"listChanged,omitempty"`
}

type toolInfo struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type toolsListResult struct {
	Tools []toolInfo `json:"tools"`
}

type callToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
}

type callToolResult struct {
	Content []contentBlock `json:"content"`
	IsError bool           `json:"isError,omitempty"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// transport abstracts stdio vs HTTP+SSE
type transport interface {
	Recv() (*jsonRPCRequest, error)
	Send(resp jsonRPCResponse) error
}

// serve is the main dispatch loop.
func (s *Server) serve(ctx context.Context, t transport) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		req, err := t.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("reading request: %w", err)
		}

		resp := s.handleRequest(ctx, req)
		if resp == nil {
			// Notification — no response needed
			continue
		}
		if err := t.Send(*resp); err != nil {
			return fmt.Errorf("sending response: %w", err)
		}
	}
}

func (s *Server) handleRequest(ctx context.Context, req *jsonRPCRequest) *jsonRPCResponse {
	switch req.Method {
	case "initialize":
		return s.handleInitialize(req)
	case "initialized":
		// Notification — no response
		return nil
	case "tools/list":
		return s.handleToolsList(req)
	case "tools/call":
		return s.handleToolsCall(ctx, req)
	case "resources/list":
		return s.handleResourcesList(req)
	case "resources/read":
		return s.handleResourcesRead(req)
	case "ping":
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{},
		}
	default:
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &jsonRPCError{
				Code:    -32601,
				Message: fmt.Sprintf("method not found: %s", req.Method),
			},
		}
	}
}

func (s *Server) handleInitialize(req *jsonRPCRequest) *jsonRPCResponse {
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: initializeResult{
			ProtocolVersion: protocolVersion,
			Capabilities: capabilities{
				Tools:     &toolsCapability{},
				Resources: &resourcesCapability{},
			},
			ServerInfo: serverInfo{
				Name:    serverName,
				Version: serverVersion,
			},
			WadjetCapabilities: map[string]any{
				"ddl.create_alert": map[string]any{
					"description": "Schedule a SQL query to evaluate periodically and deliver matches to a webhook or history table.",
					"example":     "CREATE ALERT failed_logins AS SELECT ... EVERY 5 MINUTES WEBHOOK 'https://...' INSERT INTO alert_history;",
					"docs_uri":    "wadjet://docs/alerts",
				},
			},
		},
	}
}

func (s *Server) handleToolsList(req *jsonRPCRequest) *jsonRPCResponse {
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  toolsListResult{Tools: s.toolDefinitions()},
	}
}

func (s *Server) handleToolsCall(ctx context.Context, req *jsonRPCRequest) *jsonRPCResponse {
	var params callToolParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &jsonRPCError{
				Code:    -32602,
				Message: "invalid params: " + err.Error(),
			},
		}
	}

	result := s.dispatchTool(ctx, params)
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
}

func (s *Server) handleResourcesList(req *jsonRPCRequest) *jsonRPCResponse {
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"resources": []map[string]any{{
				"uri":         alertsDocURI,
				"name":        "CREATE ALERT docs",
				"description": "Grammar, semantics, and limits for Wadjet alerts.",
				"mimeType":    "text/markdown",
			}},
		},
	}
}

func (s *Server) handleResourcesRead(req *jsonRPCRequest) *jsonRPCResponse {
	var p struct {
		URI string `json:"uri"`
	}
	_ = json.Unmarshal(req.Params, &p)
	if p.URI != alertsDocURI {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &jsonRPCError{
				Code:    -32602,
				Message: "unknown resource: " + p.URI,
			},
		}
	}
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: map[string]any{
			"contents": []map[string]any{{
				"uri":      alertsDocURI,
				"mimeType": "text/markdown",
				"text":     alertsDocMD,
			}},
		},
	}
}

func (s *Server) dispatchTool(ctx context.Context, params callToolParams) callToolResult {
	switch params.Name {
	case "list_tables":
		return s.toolListTables(ctx)
	case "describe_table":
		return s.toolDescribeTable(ctx, params.Arguments)
	case "query":
		return s.toolQuery(ctx, params.Arguments)
	case "explain":
		return s.toolExplain(ctx, params.Arguments)
	case "list_functions":
		return s.toolListFunctions(ctx)
	case "list_alerts":
		return s.handleListAlerts(ctx)
	case "describe_alert":
		return s.handleDescribeAlert(ctx, params.Arguments)
	default:
		return errorResult(fmt.Sprintf("unknown tool: %s", params.Name))
	}
}

// Tool definitions

func (s *Server) toolDefinitions() []toolInfo {
	return []toolInfo{
		{
			Name:        "list_tables",
			Description: "List all tables in the Wadjet catalog. Returns table names. Use this first to discover available data.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
		},
		{
			Name: "describe_table",
			Description: "Get the schema of a table including column names, data types (including network-native types like IPv4, IPv6, CIDR, MAC, Port, Protocol), " +
				"nullability, and partition keys. Use this to understand a table's structure before querying.",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"table": {
						"type": "string",
						"description": "Name of the table to describe"
					}
				},
				"required": ["table"]
			}`),
		},
		{
			Name: "query",
			Description: "Execute a SQL query against Wadjet and return results. Supports full analytical SQL: " +
				"JOINs, GROUP BY, window functions, CTEs, subqueries, CASE, CAST, LIKE, BETWEEN, IN, " +
				"and 58+ built-in functions including network functions (cidr_contains, ip_version, mask_ip). " +
				"Network-native column types: IPv4, IPv6, CIDR, MAC, Port, Protocol. " +
				"Results are returned as JSON with typed schema information.",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"sql": {
						"type": "string",
						"description": "SQL query to execute"
					},
					"limit": {
						"type": "integer",
						"description": "Maximum number of rows to return (default: 1000, max: 10000). Applied as a safety limit on top of any LIMIT in the query itself.",
						"default": 1000
					}
				},
				"required": ["sql"]
			}`),
		},
		{
			Name:        "explain",
			Description: "Show the query execution plan without running the query. Useful for understanding how a query will be executed and optimizing performance.",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"sql": {
						"type": "string",
						"description": "SQL query to explain"
					},
					"verbose": {
						"type": "boolean",
						"description": "Include physical plan details (default: false)",
						"default": false
					}
				},
				"required": ["sql"]
			}`),
		},
		{
			Name:        "list_functions",
			Description: "List all user-defined functions (UDFs) registered in the database, including their parameters and definitions.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
		},
		{
			Name:        "list_alerts",
			Description: "List all CREATE ALERT definitions in this cluster.",
			InputSchema: json.RawMessage(`{"type":"object","properties":{}}`),
		},
		{
			Name:        "describe_alert",
			Description: "Return the full AlertMeta for a given alert plus its 10 most recent fires.",
			InputSchema: json.RawMessage(`{
				"type": "object",
				"required": ["name"],
				"properties": {
					"name": {
						"type": "string",
						"description": "Name of the alert to describe"
					}
				}
			}`),
		},
	}
}

// Tool implementations

func (s *Server) toolListTables(ctx context.Context) callToolResult {
	tables, err := s.db.ListTables(ctx)
	if err != nil {
		return errorResult("failed to list tables: " + err.Error())
	}

	if len(tables) == 0 {
		return textResult("No tables found in the catalog.")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Found %d table(s):\n\n", len(tables)))
	for _, t := range tables {
		sb.WriteString("- ")
		sb.WriteString(t)
		sb.WriteString("\n")
	}
	sb.WriteString("\nUse describe_table to see the schema of a specific table.")
	return textResult(sb.String())
}

func (s *Server) toolDescribeTable(ctx context.Context, args map[string]any) callToolResult {
	tableName, _ := args["table"].(string)
	if tableName == "" {
		return errorResult("missing required argument: table")
	}

	table, err := s.db.Catalog().GetTable(ctx, tableName)
	if err != nil {
		return errorResult(fmt.Sprintf("table %q: %s", tableName, err.Error()))
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Table: %s\n", table.Name))
	sb.WriteString(fmt.Sprintf("Created: %s\n", table.CreatedAt.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Columns: %d\n\n", len(table.Schema.Columns)))

	// Column details
	sb.WriteString("| Column | Type | Nullable |\n")
	sb.WriteString("|--------|------|----------|\n")
	for _, col := range table.Schema.Columns {
		typeName := col.Type.String()
		if col.Precision > 0 {
			typeName = fmt.Sprintf("DECIMAL(%d,%d)", col.Precision, col.Scale)
		}
		nullable := "NO"
		if col.Nullable {
			nullable = "YES"
		}
		sb.WriteString(fmt.Sprintf("| %s | %s | %s |\n", col.Name, typeName, nullable))
	}

	if len(table.PartitionKeys) > 0 {
		sb.WriteString(fmt.Sprintf("\nPartition keys: %s\n", strings.Join(table.PartitionKeys, ", ")))
	}

	// Add type hints for network types
	networkCols := []string{}
	for _, col := range table.Schema.Columns {
		switch col.Type.String() {
		case "IPV4", "IPV6", "CIDR", "MAC", "PORT", "PROTOCOL":
			networkCols = append(networkCols, fmt.Sprintf("%s (%s)", col.Name, col.Type.String()))
		}
	}
	if len(networkCols) > 0 {
		sb.WriteString(fmt.Sprintf("\nNetwork-typed columns: %s\n", strings.Join(networkCols, ", ")))
		sb.WriteString("Tip: Use cidr_contains(), ip_version(), mask_ip() for network analysis.\n")
	}

	return textResult(sb.String())
}

func (s *Server) toolQuery(ctx context.Context, args map[string]any) callToolResult {
	sql, _ := args["sql"].(string)
	if sql == "" {
		return errorResult("missing required argument: sql")
	}

	maxRows := 1000
	if limit, ok := args["limit"].(float64); ok && limit > 0 {
		maxRows = int(limit)
	}
	if maxRows > 10000 {
		maxRows = 10000
	}

	result, err := s.db.Query(ctx, sql)
	if err != nil {
		return errorResult("query error: " + err.Error())
	}

	// Build compact JSON output optimized for AI consumption
	truncated := false
	rows := result.Rows
	if len(rows) > maxRows {
		rows = rows[:maxRows]
		truncated = true
	}

	output := map[string]any{
		"columns":   result.Columns,
		"row_count": len(rows),
		"truncated": truncated,
	}
	if len(result.Rows) > maxRows {
		output["total_rows"] = len(result.Rows)
	}

	// Convert rows to compact array-of-arrays format (not array-of-objects)
	// This is 2-5x more token-efficient for LLMs
	compactRows := make([][]any, len(rows))
	for i, row := range rows {
		vals := make([]any, len(result.Columns))
		for j, col := range result.Columns {
			vals[j] = row[col]
		}
		compactRows[i] = vals
	}
	output["rows"] = compactRows

	data, err := json.Marshal(output)
	if err != nil {
		return errorResult("failed to marshal results: " + err.Error())
	}

	return textResult(string(data))
}

func (s *Server) toolExplain(ctx context.Context, args map[string]any) callToolResult {
	sql, _ := args["sql"].(string)
	if sql == "" {
		return errorResult("missing required argument: sql")
	}

	verbose := false
	if v, ok := args["verbose"].(bool); ok {
		verbose = v
	}

	explainSQL := "EXPLAIN " + sql
	if verbose {
		explainSQL = "EXPLAIN VERBOSE " + sql
	}

	result, err := s.db.Query(ctx, explainSQL)
	if err != nil {
		return errorResult("explain error: " + err.Error())
	}

	var sb strings.Builder
	for _, row := range result.Rows {
		if line, ok := row["plan"].(string); ok {
			sb.WriteString(line)
			sb.WriteString("\n")
		}
	}
	return textResult(sb.String())
}

func (s *Server) toolListFunctions(ctx context.Context) callToolResult {
	result, err := s.db.Query(ctx, "SHOW FUNCTIONS")
	if err != nil {
		return errorResult("failed to list functions: " + err.Error())
	}

	type udfEntry struct {
		Name   string `json:"name"`
		Params string `json:"params,omitempty"`
		Body   string `json:"body,omitempty"`
	}
	udfs := make([]udfEntry, 0, len(result.Rows))
	for _, row := range result.Rows {
		name, _ := row["name"].(string)
		params, _ := row["params"].(string)
		body, _ := row["body"].(string)
		udfs = append(udfs, udfEntry{Name: name, Params: params, Body: body})
	}

	ddlCaps := []map[string]string{
		{"name": "CREATE ALERT", "description": "Schedule a SQL query; deliver matches to a webhook and/or alert_history."},
		{"name": "DROP ALERT", "description": "Remove an alert definition."},
		{"name": "ALTER ALERT ENABLE|DISABLE", "description": "Toggle evaluation without deleting the alert."},
	}

	out := map[string]any{
		"functions":       udfs,
		"ddl_capabilities": ddlCaps,
	}
	data, err := json.Marshal(out)
	if err != nil {
		return errorResult("failed to marshal functions: " + err.Error())
	}
	return textResult(string(data))
}

// Helpers

func textResult(text string) callToolResult {
	return callToolResult{
		Content: []contentBlock{{Type: "text", Text: text}},
	}
}

func errorResult(msg string) callToolResult {
	return callToolResult{
		Content: []contentBlock{{Type: "text", Text: msg}},
		IsError: true,
	}
}

// stdioTransport implements JSON-RPC 2.0 over stdin/stdout.
// Each message is a single line of JSON.
type stdioTransport struct {
	scanner *bufio.Scanner
	out     io.Writer
	mu      *sync.Mutex
}

func (t *stdioTransport) Recv() (*jsonRPCRequest, error) {
	if !t.scanner.Scan() {
		if err := t.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	line := t.scanner.Bytes()
	var req jsonRPCRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, fmt.Errorf("invalid JSON-RPC: %w", err)
	}
	return &req, nil
}

func (t *stdioTransport) Send(resp jsonRPCResponse) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err = fmt.Fprintf(t.out, "%s\n", data)
	return err
}

// HTTP+SSE transport

func (h *HTTPServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Generate a session ID and create a message channel
	sessionID := fmt.Sprintf("session-%d", time.Now().UnixNano())
	msgCh := make(chan jsonRPCResponse, 64)

	sseSessionsMu.Lock()
	sseSessions[sessionID] = msgCh
	sseSessionsMu.Unlock()

	defer func() {
		sseSessionsMu.Lock()
		delete(sseSessions, sessionID)
		sseSessionsMu.Unlock()
	}()

	// Send the endpoint event so the client knows where to POST
	fmt.Fprintf(w, "event: endpoint\ndata: /message?sessionId=%s\n\n", sessionID)
	flusher.Flush()

	// Stream responses
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			return
		case resp := <-msgCh:
			data, err := json.Marshal(resp)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (h *HTTPServer) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		http.Error(w, "missing sessionId", http.StatusBadRequest)
		return
	}

	sseSessionsMu.RLock()
	ch, ok := sseSessions[sessionID]
	sseSessionsMu.RUnlock()
	if !ok {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}

	var req jsonRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON-RPC: "+err.Error(), http.StatusBadRequest)
		return
	}

	resp := h.mcp.handleRequest(r.Context(), &req)
	if resp != nil {
		select {
		case ch <- *resp:
		default:
			h.logger.Warn("MCP SSE channel full, dropping response")
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

var (
	sseSessions   = make(map[string]chan jsonRPCResponse)
	sseSessionsMu sync.RWMutex
)
