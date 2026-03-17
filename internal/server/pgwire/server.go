// Package pgwire implements the PostgreSQL v3 wire protocol frontend.
// This allows psql, JDBC, ODBC, and any Postgres-compatible client to
// connect to Caelum and execute SQL queries.
package pgwire

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"strings"
	"sync"

	"github.com/derekmwright/caelum/caelum"
	"golang.org/x/net/netutil"
)

// Server listens for PostgreSQL wire protocol connections and dispatches
// queries to a Caelum DB instance.
type Server struct {
	db        *caelum.DB
	listener  net.Listener
	logger    *slog.Logger
	wg        sync.WaitGroup
	done      chan struct{}
	tlsConfig *tls.Config
	maxConns  int
}

// Config holds configuration for the pgwire server.
type Config struct {
	Addr           string     // listen address, e.g. ":5433"
	TLSConfig      *tls.Config // nil = plain TCP
	MaxConnections int         // 0 = unlimited
}

// NewServer creates a new PostgreSQL wire protocol server.
func NewServer(db *caelum.DB, cfg Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		db:        db,
		logger:    logger,
		done:      make(chan struct{}),
		tlsConfig: cfg.TLSConfig,
		maxConns:  cfg.MaxConnections,
	}
}

// Start begins listening for connections on the given address.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("pgwire listen: %w", err)
	}

	if s.maxConns > 0 {
		ln = netutil.LimitListener(ln, s.maxConns)
	}

	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}

	s.listener = ln
	s.logger.Info("PostgreSQL wire protocol server listening", "addr", addr,
		"tls", s.tlsConfig != nil, "max_connections", s.maxConns)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop()
	}()

	return nil
}

// Addr returns the listener's address (useful when using :0 for tests).
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown() {
	close(s.done)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				s.logger.Error("pgwire accept error", "err", err)
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	c := &pgConn{
		conn:    conn,
		db:      s.db,
		logger:  s.logger,
		buf:     make([]byte, 0, 4096),
		stmts:   make(map[string]string),
		txState: 'I',
	}
	c.run()
}

// pgConn handles a single PostgreSQL client connection.
type pgConn struct {
	conn   net.Conn
	db     *caelum.DB
	logger *slog.Logger
	buf    []byte

	// Extended Query protocol state
	preparedSQL     string            // last parsed statement SQL
	portalSQL       string            // last bound portal SQL
	stmts           map[string]string // named prepared statements
	described       bool              // true if Describe was sent for current portal
	resultFmtCodes  []int16           // result format codes from Bind (0=text, 1=binary)

	// Transaction state: 'I' = idle, 'T' = in transaction, 'E' = failed
	txState byte
}

func (c *pgConn) run() {
	// Phase 1: Startup
	if err := c.handleStartup(); err != nil {
		c.logger.Debug("pgwire startup failed", "err", err)
		return
	}

	// Phase 2: Query loop
	for {
		msgType, payload, err := c.readMessage()
		if err != nil {
			if err != io.EOF {
				c.logger.Debug("pgwire read error", "err", err)
			}
			return
		}

		switch msgType {
		case 'Q': // Simple Query
			sql := readCString(payload)
			c.handleQuery(sql)
		case 'P': // Parse (Extended Query)
			c.handleParse(payload)
		case 'B': // Bind (Extended Query)
			c.handleBind(payload)
		case 'D': // Describe (Extended Query)
			c.handleDescribe(payload)
		case 'E': // Execute (Extended Query)
			c.handleExecute(payload)
		case 'H': // Flush
			// no-op, we write eagerly
		case 'S': // Sync
			c.sendReadyForQuery()
		case 'C': // Close (prepared statement or portal)
			c.handleClose(payload)
		case 'X': // Terminate
			return
		default:
			c.sendError("ERROR", "08P01", fmt.Sprintf("unsupported message type: %c", msgType))
			c.sendReadyForQuery()
		}
	}
}

func (c *pgConn) handleStartup() error {
	// Read startup message (no type byte, just length + payload)
	var lenBuf [4]byte
	if _, err := io.ReadFull(c.conn, lenBuf[:]); err != nil {
		return fmt.Errorf("reading startup length: %w", err)
	}
	msgLen := int(binary.BigEndian.Uint32(lenBuf[:])) - 4
	if msgLen < 4 || msgLen > 10000 {
		return fmt.Errorf("invalid startup message length: %d", msgLen)
	}

	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return fmt.Errorf("reading startup payload: %w", err)
	}

	// Check protocol version
	version := binary.BigEndian.Uint32(payload[:4])

	// Handle SSL request (80877103)
	if version == 80877103 {
		// Decline SSL — send 'N'
		c.conn.Write([]byte{'N'})
		// Re-read the actual startup message
		return c.handleStartup()
	}

	// Handle cancel request (80877102)
	if version == 80877102 {
		return fmt.Errorf("cancel request not supported")
	}

	// Expect protocol version 3.0 (196608 = 3<<16)
	if version != 196608 {
		return fmt.Errorf("unsupported protocol version: %d", version)
	}

	// Parse startup parameters (key=value pairs, null-terminated)
	// We don't enforce authentication — just accept.
	params := parseStartupParams(payload[4:])
	_ = params // could log database, user, etc.

	// Send AuthenticationOk
	c.sendAuthOk()

	// Send ParameterStatus messages (clients like psql expect these)
	c.sendParamStatus("server_version", "15.0 (Caelum)")
	c.sendParamStatus("server_encoding", "UTF8")
	c.sendParamStatus("client_encoding", "UTF8")
	c.sendParamStatus("DateStyle", "ISO, MDY")
	c.sendParamStatus("integer_datetimes", "on")
	c.sendParamStatus("standard_conforming_strings", "on")
	c.sendParamStatus("TimeZone", "UTC")
	c.sendParamStatus("IntervalStyle", "postgres")
	c.sendParamStatus("is_superuser", "on")

	// Send BackendKeyData (process ID + secret key for cancellation)
	c.sendBackendKeyData(0, 0)

	// Send ReadyForQuery
	c.sendReadyForQuery()

	return nil
}

func (c *pgConn) handleQuery(sql string) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		c.sendEmptyQuery()
		c.sendReadyForQuery()
		return
	}

	// Handle ; at end
	sql = strings.TrimRight(sql, ";")
	sql = strings.TrimSpace(sql)
	if sql == "" {
		c.sendEmptyQuery()
		c.sendReadyForQuery()
		return
	}

	// Special handling for SET/RESET/DISCARD/BEGIN/COMMIT/ROLLBACK
	// that BI tools send during connection setup
	upper := strings.ToUpper(sql)
	if strings.HasPrefix(upper, "BEGIN") {
		c.txState = 'T'
		c.sendCommandComplete("BEGIN")
		c.sendReadyForQuery()
		return
	}
	if strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "END") {
		c.txState = 'I'
		c.sendCommandComplete("COMMIT")
		c.sendReadyForQuery()
		return
	}
	if strings.HasPrefix(upper, "ROLLBACK") {
		c.txState = 'I'
		c.sendCommandComplete("ROLLBACK")
		c.sendReadyForQuery()
		return
	}
	if strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ") ||
		strings.HasPrefix(upper, "DEALLOCATE") {
		c.sendCommandComplete("SET")
		c.sendReadyForQuery()
		return
	}

	// Handle introspection/synthetic queries (SELECT 1, version(), pg_catalog, etc.)
	if c.handleIntrospection(sql, upper) {
		c.sendReadyForQuery()
		return
	}

	ctx := context.Background()
	result, err := c.db.Query(ctx, sql)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		c.sendReadyForQuery()
		return
	}

	// Send RowDescription
	columns := result.Columns
	if len(columns) == 0 && len(result.Rows) > 0 {
		for k := range result.Rows[0] {
			columns = append(columns, k)
		}
	}
	if len(columns) == 0 && len(result.Rows) == 0 {
		c.sendEmptyQuery()
		c.sendReadyForQuery()
		return
	}
	if len(result.ColumnMetas) > 0 {
		c.sendTypedRowDescription(result.ColumnMetas)
	} else {
		c.sendRowDescription(columns)
	}

	// Send DataRow for each row
	for _, row := range result.Rows {
		c.sendDataRow(columns, row)
	}

	// Send CommandComplete
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", len(result.Rows)))
	c.sendReadyForQuery()
}

// Extended Query protocol handlers

func (c *pgConn) handleParse(payload []byte) {
	// Parse message: name\0 + query\0 + int16(numParamTypes) + int32[](paramOIDs)
	name := readCString(payload)
	payload = payload[len(name)+1:]
	sql := readCString(payload)

	if name == "" {
		c.preparedSQL = sql
	} else {
		c.stmts[name] = sql
	}
	c.described = false

	// Send ParseComplete ('1')
	c.sendMsg('1', nil)
}

func (c *pgConn) handleBind(payload []byte) {
	// Bind message: portal\0 + statement\0 + int16(numFmtCodes) + int16[](fmtCodes) +
	//   int16(numParams) + [int32(len) + bytes]... + int16(numResultFmtCodes) + int16[]...
	portal := readCString(payload)
	payload = payload[len(portal)+1:]
	stmtName := readCString(payload)
	payload = payload[len(stmtName)+1:]

	sql := c.preparedSQL
	if stmtName != "" {
		if s, ok := c.stmts[stmtName]; ok {
			sql = s
		}
	}

	// Parse parameter values and substitute $1, $2, ... into the SQL for
	// introspection queries (pg_catalog). This lets handleCatalogQuery see
	// literal values instead of placeholders.
	if len(payload) >= 2 {
		numFmt := int(binary.BigEndian.Uint16(payload[:2]))
		payload = payload[2:]
		// Skip format codes
		if len(payload) >= numFmt*2 {
			payload = payload[numFmt*2:]
		}
		if len(payload) >= 2 {
			numParams := int(binary.BigEndian.Uint16(payload[:2]))
			payload = payload[2:]
			for i := 0; i < numParams && len(payload) >= 4; i++ {
				paramLen := int(int32(binary.BigEndian.Uint32(payload[:4])))
				payload = payload[4:]
				if paramLen < 0 {
					// NULL parameter
					continue
				}
				if len(payload) >= paramLen {
					paramVal := string(payload[:paramLen])
					payload = payload[paramLen:]
					// Substitute $N with literal value (quoted for safety)
					placeholder := fmt.Sprintf("$%d", i+1)
					sql = strings.Replace(sql, placeholder, "'"+paramVal+"'", 1)
				}
			}
		}
	}

	c.portalSQL = sql

	// Parse result format codes (at the end of the Bind message)
	c.resultFmtCodes = nil
	if len(payload) >= 2 {
		numResultFmt := int(binary.BigEndian.Uint16(payload[:2]))
		payload = payload[2:]
		if numResultFmt > 0 && len(payload) >= numResultFmt*2 {
			c.resultFmtCodes = make([]int16, numResultFmt)
			for i := 0; i < numResultFmt; i++ {
				c.resultFmtCodes[i] = int16(binary.BigEndian.Uint16(payload[i*2:]))
			}
		}
	}

	// Send BindComplete ('2')
	c.sendMsg('2', nil)
}

func (c *pgConn) handleDescribe(payload []byte) {
	if len(payload) < 1 {
		return
	}
	descType := payload[0] // 'S' = statement, 'P' = portal

	if descType == 'S' {
		// ParameterDescription: no parameters
		c.buf = c.buf[:0]
		c.buf = appendInt16(c.buf, 0) // zero parameters
		c.sendMsg('t', c.buf)

		sql := c.preparedSQL
		if name := readCString(payload[1:]); name != "" {
			if s, ok := c.stmts[name]; ok {
				sql = s
			}
		}
		c.describeSQL(sql)
	} else {
		// Portal describe — send RowDescription based on portal SQL
		c.describeSQL(c.portalSQL)
	}
}

// describeSQL executes a SQL statement to discover its result columns
// and sends either a typed RowDescription or NoData.
func (c *pgConn) describeSQL(sql string) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimRight(sql, ";")
	sql = strings.TrimSpace(sql)

	if sql == "" || isCommandSQL(sql) {
		c.sendMsg('n', nil) // NoData
		return
	}

	// Check introspection queries — these return synthetic results
	upper := strings.ToUpper(sql)
	normalized := strings.Join(strings.Fields(upper), " ")
	if strings.HasPrefix(normalized, "SHOW ") ||
		strings.Contains(normalized, "PG_CATALOG") ||
		strings.Contains(normalized, "INFORMATION_SCHEMA") ||
		strings.Contains(normalized, "PG_TYPE") ||
		strings.Contains(normalized, "PG_CLASS") {
		// Return text columns based on SELECT list
		cols := extractSelectColumns(sql)
		if len(cols) == 0 {
			cols = []string{"?column?"}
		}
		c.sendRowDescription(cols)
		c.described = true
		return
	}

	// Execute the real query to get typed column metadata
	ctx := context.Background()
	result, err := c.db.Query(ctx, sql)
	if err != nil {
		// Can't describe — send NoData rather than error (the error
		// will surface when Execute runs).
		c.sendMsg('n', nil)
		return
	}

	if len(result.ColumnMetas) > 0 {
		c.sendTypedRowDescription(result.ColumnMetas)
		c.described = true
	} else if len(result.Columns) > 0 {
		c.sendRowDescription(result.Columns)
		c.described = true
	} else {
		c.sendMsg('n', nil)
	}
}

func (c *pgConn) handleExecute(payload []byte) {
	// Execute: portal\0 + int32(maxRows)
	sql := strings.TrimSpace(c.portalSQL)
	if sql == "" {
		sql = strings.TrimSpace(c.preparedSQL)
	}
	if sql == "" {
		c.sendCommandComplete("SELECT 0")
		return
	}

	// Strip trailing semicolons
	sql = strings.TrimRight(sql, ";")
	sql = strings.TrimSpace(sql)

	// Handle SET/RESET/BEGIN/etc
	upper := strings.ToUpper(sql)
	if strings.HasPrefix(upper, "BEGIN") {
		c.txState = 'T'
		c.sendCommandComplete("BEGIN")
		return
	}
	if strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "END") {
		c.txState = 'I'
		c.sendCommandComplete("COMMIT")
		return
	}
	if strings.HasPrefix(upper, "ROLLBACK") {
		c.txState = 'I'
		c.sendCommandComplete("ROLLBACK")
		return
	}
	if strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ") ||
		strings.HasPrefix(upper, "DEALLOCATE") ||
		strings.HasPrefix(upper, "CLOSE") {
		c.sendCommandComplete("SET")
		return
	}

	// Handle catalog/introspection queries from BI tools
	if result := c.handleIntrospection(sql, upper); result {
		return
	}

	ctx := context.Background()
	result, err := c.db.Query(ctx, sql)
	if err != nil {
		c.sendError("ERROR", "42000", err.Error())
		return
	}

	columns := result.Columns
	if len(columns) == 0 && len(result.Rows) > 0 {
		for k := range result.Rows[0] {
			columns = append(columns, k)
		}
	}
	if len(columns) == 0 {
		c.sendCommandComplete("SELECT 0")
		return
	}

	// Extended query protocol: Execute sends only DataRow + CommandComplete.
	// RowDescription was already sent by Describe. Do NOT send it again.
	for _, row := range result.Rows {
		if len(c.resultFmtCodes) > 0 {
			c.sendDataRowFormatted(columns, row, c.resultFmtCodes)
		} else {
			c.sendDataRow(columns, row)
		}
	}
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", len(result.Rows)))
}

func (c *pgConn) handleClose(payload []byte) {
	// Close: type('S'/'P') + name\0
	if len(payload) >= 1 && payload[0] == 'S' {
		name := readCString(payload[1:])
		delete(c.stmts, name)
	}
	// Send CloseComplete ('3')
	c.sendMsg('3', nil)
}

// handleIntrospection handles pg_catalog queries and synthetic expressions
// from BI tools like Superset, SQLAlchemy, DBeaver, and psql.
func (c *pgConn) handleIntrospection(sql, upper string) bool {
	// Normalize whitespace for matching (newlines, tabs → spaces)
	normalized := strings.Join(strings.Fields(upper), " ")

	// SHOW statements (SHOW TRANSACTION ISOLATION LEVEL, SHOW server_version, etc.)
	if strings.HasPrefix(normalized, "SHOW ") {
		return c.handleShow(normalized)
	}

	// Well-known functions that may reference pg_catalog — handle before the
	// blanket pg_catalog intercept so we return real values.
	if strings.Contains(normalized, "VERSION()") {
		c.sendSingleRow([]string{"version"}, map[string]any{
			"version": "PostgreSQL 15.0 (Caelum analytical query engine)",
		})
		return true
	}

	// pg_catalog / information_schema introspection — return real catalog data
	// for table/column discovery, empty results for everything else.
	isPgCatalog := strings.Contains(normalized, "PG_CATALOG") ||
		strings.Contains(normalized, "INFORMATION_SCHEMA") ||
		strings.Contains(normalized, "PG_TYPE") ||
		strings.Contains(normalized, "PG_NAMESPACE") ||
		strings.Contains(normalized, "PG_CLASS") ||
		strings.Contains(normalized, "PG_ATTRIBUTE") ||
		strings.Contains(normalized, "PG_INDEX") ||
		strings.Contains(normalized, "PG_CONSTRAINT") ||
		strings.Contains(normalized, "PG_AM") ||
		strings.Contains(normalized, "PG_ROLES") ||
		strings.Contains(normalized, "PG_DESCRIPTION") ||
		strings.Contains(normalized, "PG_PROC") ||
		strings.Contains(normalized, "PG_TABLES") ||
		strings.Contains(normalized, "PG_VIEWS") ||
		strings.Contains(normalized, "PG_MATVIEWS") ||
		strings.Contains(normalized, "PG_DATABASE")

	if isPgCatalog {
		if c.handleCatalogQuery(normalized) {
			return true
		}
		// Fallback: return empty result with proper columns
		cols := extractSelectColumns(sql)
		c.sendRowDescription(cols)
		c.sendCommandComplete("SELECT 0")
		return true
	}

	// SELECT with no FROM clause — evaluate common synthetic expressions.
	// Caelum requires FROM, but PostgreSQL clients expect these to work.
	if strings.HasPrefix(normalized, "SELECT ") && !strings.Contains(normalized, " FROM ") {
		return c.handleSyntheticSelect(sql, normalized)
	}

	return false
}

// handleSyntheticSelect handles SELECT expressions that have no FROM clause.
func (c *pgConn) handleSyntheticSelect(sql, upper string) bool {
	// version()
	if strings.Contains(upper, "VERSION()") {
		c.sendSingleRow([]string{"version"}, map[string]any{
			"version": "PostgreSQL 15.0 (Caelum analytical query engine)",
		})
		return true
	}

	// current_schema / current_schema()
	if strings.Contains(upper, "CURRENT_SCHEMA") {
		c.sendSingleRow([]string{"current_schema"}, map[string]any{
			"current_schema": "public",
		})
		return true
	}

	// current_database()
	if strings.Contains(upper, "CURRENT_DATABASE()") {
		c.sendSingleRow([]string{"current_database"}, map[string]any{
			"current_database": "caelum",
		})
		return true
	}

	// current_user / session_user
	if strings.Contains(upper, "CURRENT_USER") || strings.Contains(upper, "SESSION_USER") {
		c.sendSingleRow([]string{"current_user"}, map[string]any{
			"current_user": "caelum",
		})
		return true
	}

	// SELECT 1 (connection liveness check)
	if strings.TrimSpace(upper) == "SELECT 1" {
		c.sendSingleRow([]string{"?column?"}, map[string]any{
			"?column?": "1",
		})
		return true
	}

	// Any other constant SELECT without FROM — try to return a simple result
	// e.g. "SELECT 'test'" or "SELECT 1 AS id"
	// Fall back: return a single row with a generic column
	c.sendSingleRow([]string{"?column?"}, map[string]any{
		"?column?": "",
	})
	return true
}

// handleShow handles SHOW statements from PostgreSQL clients.
func (c *pgConn) handleShow(upper string) bool {
	switch {
	case strings.Contains(upper, "TRANSACTION ISOLATION LEVEL"):
		c.sendSingleRow([]string{"transaction_isolation"}, map[string]any{
			"transaction_isolation": "read committed",
		})
	case strings.Contains(upper, "STANDARD_CONFORMING_STRINGS"):
		c.sendSingleRow([]string{"standard_conforming_strings"}, map[string]any{
			"standard_conforming_strings": "on",
		})
	case strings.Contains(upper, "SERVER_VERSION"):
		c.sendSingleRow([]string{"server_version"}, map[string]any{
			"server_version": "15.0",
		})
	case strings.Contains(upper, "SERVER_ENCODING"):
		c.sendSingleRow([]string{"server_encoding"}, map[string]any{
			"server_encoding": "UTF8",
		})
	case strings.Contains(upper, "CLIENT_ENCODING"):
		c.sendSingleRow([]string{"client_encoding"}, map[string]any{
			"client_encoding": "UTF8",
		})
	case strings.Contains(upper, "DATESTYLE"):
		c.sendSingleRow([]string{"DateStyle"}, map[string]any{
			"DateStyle": "ISO, MDY",
		})
	default:
		// Unknown SHOW — return empty
		c.sendSingleRow([]string{"setting"}, map[string]any{
			"setting": "",
		})
	}
	return true
}

// sendSingleRow sends a one-row result set.
func (c *pgConn) sendSingleRow(cols []string, row map[string]any) {
	c.sendRowDescription(cols)
	c.sendDataRow(cols, row)
	c.sendCommandComplete("SELECT 1")
}

// tableOID returns a deterministic fake OID for a table name.
func tableOID(name string) int {
	// Simple hash to create stable OIDs starting at 16384 (user table range in PG)
	h := 16384
	for _, c := range name {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h
}

// pgTypeOID maps Caelum types to PostgreSQL type OIDs.
func pgTypeOID(typeName string) int {
	switch strings.ToUpper(typeName) {
	case "INT32":
		return 23 // int4
	case "INT64":
		return 20 // int8
	case "FLOAT32":
		return 700 // float4
	case "FLOAT64":
		return 701 // float8
	case "BOOLEAN", "BOOL":
		return 16 // bool
	case "TIMESTAMP":
		return 1114 // timestamp
	case "DATE":
		return 1082 // date
	default:
		return 25 // text
	}
}

// handleCatalogQuery returns real catalog data for specific pg_catalog queries
// that SQLAlchemy/Superset uses for schema introspection.
func (c *pgConn) handleCatalogQuery(normalized string) bool {
	ctx := context.Background()

	// pg_type — JDBC drivers query this to map OIDs to type names
	if strings.Contains(normalized, "PG_TYPE") && !strings.Contains(normalized, "PG_CLASS") &&
		!strings.Contains(normalized, "PG_ATTRIBUTE") {
		return c.handlePgType(normalized)
	}

	// information_schema.tables
	if strings.Contains(normalized, "INFORMATION_SCHEMA") && strings.Contains(normalized, "TABLES") &&
		!strings.Contains(normalized, "COLUMNS") {
		return c.handleInfoSchemaTables(ctx)
	}

	// information_schema.columns
	if strings.Contains(normalized, "INFORMATION_SCHEMA") && strings.Contains(normalized, "COLUMNS") {
		return c.handleInfoSchemaColumns(ctx, normalized)
	}

	// Schema listing: SELECT nspname FROM pg_namespace (used by get_schema_names)
	// Exclude queries that mention pg_type/pg_class/pg_attribute — those just JOIN pg_namespace.
	if strings.Contains(normalized, "PG_NAMESPACE") && strings.Contains(normalized, "NSPNAME") &&
		!strings.Contains(normalized, "PG_CLASS") &&
		!strings.Contains(normalized, "PG_TYPE") &&
		!strings.Contains(normalized, "PG_ATTRIBUTE") {
		cols := []string{"nspname"}
		c.sendRowDescription(cols)
		c.sendDataRow(cols, map[string]any{"nspname": "public"})
		c.sendCommandComplete("SELECT 1")
		return true
	}

	// Index/constraint queries that happen to JOIN pg_attribute — return empty
	if strings.Contains(normalized, "PG_INDEX") ||
		strings.Contains(normalized, "PG_CONSTRAINT") {
		cols := extractSelectColumns(c.portalSQL)
		if len(cols) == 0 {
			cols = extractSelectColumns(c.preparedSQL)
		}
		if len(cols) == 0 {
			cols = []string{"?column?"}
		}
		c.sendRowDescription(cols)
		c.sendCommandComplete("SELECT 0")
		return true
	}

	// Column info: pg_attribute query
	if strings.Contains(normalized, "PG_ATTRIBUTE") {
		return c.handleAttributeQuery(ctx, normalized)
	}

	// pg_class queries: table listing, OID lookup, or reverse OID lookup
	if strings.Contains(normalized, "PG_CLASS") && strings.Contains(normalized, "RELNAME") {
		tables, err := c.db.ListTables(ctx)
		if err != nil {
			return false
		}

		// Specific table lookup: relname = '<value>' in WHERE
		specificTable := extractParamValue(normalized, "RELNAME")
		if specificTable != "" {
			// OID lookup for a specific table
			for _, t := range tables {
				if t == specificTable {
					oid := tableOID(t)
					c.sendRowDescription([]string{"oid"})
					c.sendDataRow([]string{"oid"}, map[string]any{"oid": fmt.Sprintf("%d", oid)})
					c.sendCommandComplete("SELECT 1")
					return true
				}
			}
			c.sendRowDescription([]string{"oid"})
			c.sendCommandComplete("SELECT 0")
			return true
		}

		// Reverse OID lookup: SELECT relname FROM pg_class WHERE oid = '<value>'
		oidVal := extractParamValue(normalized, "OID")
		if oidVal != "" {
			for _, t := range tables {
				if fmt.Sprintf("%d", tableOID(t)) == oidVal {
					c.sendRowDescription([]string{"relname"})
					c.sendDataRow([]string{"relname"}, map[string]any{"relname": t})
					c.sendCommandComplete("SELECT 1")
					return true
				}
			}
			c.sendRowDescription([]string{"relname"})
			c.sendCommandComplete("SELECT 0")
			return true
		}

		// List all tables — only when RELKIND is present (real table listing query)
		if strings.Contains(normalized, "RELKIND") {
			cols := []string{"relname"}
			c.sendRowDescription(cols)
			for _, t := range tables {
				c.sendDataRow(cols, map[string]any{"relname": t})
			}
			c.sendCommandComplete(fmt.Sprintf("SELECT %d", len(tables)))
			return true
		}

		// Unknown pg_class query — don't handle, fall through to blanket
		return false
	}

	return false
}

// handleAttributeQuery returns column metadata from our catalog for pg_attribute queries.
func (c *pgConn) handleAttributeQuery(ctx context.Context, normalized string) bool {
	tables, err := c.db.ListTables(ctx)
	if err != nil || len(tables) == 0 {
		return false
	}

	// Try to find the target from the query text.
	// SQLAlchemy may send a table name or a numeric OID as the attrelid parameter.
	target := extractParamValue(normalized, "ATTRELID")

	// Determine which table(s) to describe
	var targetTables []string
	if target == "" {
		targetTables = tables
	} else {
		// Check if target is a table name (SQLAlchemy sends table name as table_oid param)
		for _, t := range tables {
			if t == target {
				targetTables = []string{t}
				break
			}
		}
		// If not a name, try matching as numeric OID
		if len(targetTables) == 0 {
			for _, t := range tables {
				if fmt.Sprintf("%d", tableOID(t)) == target {
					targetTables = []string{t}
					break
				}
			}
		}
		// If still no match, return empty
		if len(targetTables) == 0 {
			targetTables = nil
		}
	}

	// SQLAlchemy 1.4 PGDialect.get_columns unpacks exactly 8 values per row:
	//   name, format_type, default_, notnull, table_oid, comment, generated, identity
	cols := []string{"attname", "format_type", "default", "attnotnull",
		"table_oid", "comment", "generated", "identity"}

	var resultRows []map[string]any

	for _, tableName := range targetTables {
		table, err := c.db.Query(ctx, fmt.Sprintf("DESCRIBE %s", tableName))
		if err != nil {
			continue
		}

		oid := fmt.Sprintf("%d", tableOID(tableName))
		for _, row := range table.Rows {
			colName, _ := row["column_name"].(string)
			colType, _ := row["type"].(string)
			nullable, _ := row["nullable"].(string)
			if colName == "" || colName == "Partition Keys" {
				continue
			}

			resultRows = append(resultRows, map[string]any{
				"attname":     colName,
				"format_type": pgFormatType(colType),
				"default":     nil,
				"attnotnull":  boolStr(nullable == "NO"),
				"table_oid":   oid,
				"comment":     nil,
				"generated":   "",
				"identity":    "",
			})
		}
	}

	c.sendRowDescription(cols)
	for _, row := range resultRows {
		c.sendDataRow(cols, row)
	}
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", len(resultRows)))
	return true
}

// handlePgType returns rows from pg_type for JDBC/ODBC type mapping.
// Drivers like pgjdbc query pg_type to map OIDs to Java types.
func (c *pgConn) handlePgType(normalized string) bool {
	type pgType struct {
		oid      string
		typname  string
		typlen   string
		typtype  string
		typnamespace string
	}

	types := []pgType{
		{"16", "bool", "1", "b", "11"},
		{"17", "bytea", "-1", "b", "11"},
		{"20", "int8", "8", "b", "11"},
		{"21", "int2", "2", "b", "11"},
		{"23", "int4", "4", "b", "11"},
		{"25", "text", "-1", "b", "11"},
		{"26", "oid", "4", "b", "11"},
		{"700", "float4", "4", "b", "11"},
		{"701", "float8", "8", "b", "11"},
		{"1042", "bpchar", "-1", "b", "11"},
		{"1043", "varchar", "-1", "b", "11"},
		{"1082", "date", "4", "b", "11"},
		{"1114", "timestamp", "8", "b", "11"},
		{"1184", "timestamptz", "8", "b", "11"},
		{"1700", "numeric", "-1", "b", "11"},
	}

	// If query is looking for a specific OID, filter
	specificOID := extractParamValue(normalized, "OID")

	cols := []string{"oid", "typname", "typlen", "typtype", "typnamespace"}
	c.sendRowDescription(cols)

	count := 0
	for _, t := range types {
		if specificOID != "" && t.oid != specificOID {
			continue
		}
		c.sendDataRow(cols, map[string]any{
			"oid":          t.oid,
			"typname":      t.typname,
			"typlen":       t.typlen,
			"typtype":      t.typtype,
			"typnamespace": t.typnamespace,
		})
		count++
	}
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", count))
	return true
}

// handleInfoSchemaTables returns information_schema.tables data.
func (c *pgConn) handleInfoSchemaTables(ctx context.Context) bool {
	tables, err := c.db.ListTables(ctx)
	if err != nil {
		return false
	}

	cols := []string{"table_catalog", "table_schema", "table_name", "table_type"}
	c.sendRowDescription(cols)
	for _, t := range tables {
		c.sendDataRow(cols, map[string]any{
			"table_catalog": "caelum",
			"table_schema":  "public",
			"table_name":    t,
			"table_type":    "BASE TABLE",
		})
	}
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", len(tables)))
	return true
}

// handleInfoSchemaColumns returns information_schema.columns data.
func (c *pgConn) handleInfoSchemaColumns(ctx context.Context, normalized string) bool {
	tables, err := c.db.ListTables(ctx)
	if err != nil {
		return false
	}

	// Filter to specific table if referenced
	targetTable := extractParamValue(normalized, "TABLE_NAME")

	cols := []string{"table_catalog", "table_schema", "table_name",
		"column_name", "ordinal_position", "data_type", "is_nullable"}

	c.sendRowDescription(cols)
	count := 0
	for _, tableName := range tables {
		if targetTable != "" && tableName != targetTable {
			continue
		}
		table, err := c.db.Query(ctx, fmt.Sprintf("DESCRIBE %s", tableName))
		if err != nil {
			continue
		}
		for i, row := range table.Rows {
			colName, _ := row["column_name"].(string)
			colType, _ := row["type"].(string)
			nullable, _ := row["nullable"].(string)
			if colName == "" || colName == "Partition Keys" {
				continue
			}
			isNullable := "YES"
			if nullable == "NO" {
				isNullable = "NO"
			}
			c.sendDataRow(cols, map[string]any{
				"table_catalog":    "caelum",
				"table_schema":     "public",
				"table_name":       tableName,
				"column_name":      colName,
				"ordinal_position": fmt.Sprintf("%d", i+1),
				"data_type":        pgFormatType(colType),
				"is_nullable":      isNullable,
			})
			count++
		}
	}
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", count))
	return true
}

// pgFormatType maps Caelum type names to PostgreSQL format_type() output.
func pgFormatType(typeName string) string {
	switch strings.ToUpper(typeName) {
	case "INT32":
		return "integer"
	case "INT64":
		return "bigint"
	case "FLOAT32":
		return "real"
	case "FLOAT64":
		return "double precision"
	case "BOOLEAN", "BOOL":
		return "boolean"
	case "TIMESTAMP":
		return "timestamp without time zone"
	case "DATE":
		return "date"
	case "DECIMAL":
		return "numeric"
	default:
		return "text"
	}
}

func boolStr(b bool) string {
	if b {
		return "t"
	}
	return "f"
}

// extractParamValue tries to extract a literal value from a WHERE clause.
// e.g., "WHERE relname = 'client_traffic'" returns "client_traffic".
func extractParamValue(normalized, field string) string {
	idx := strings.Index(normalized, field+" = ")
	if idx < 0 {
		idx = strings.Index(normalized, field+" =")
	}
	if idx < 0 {
		return ""
	}
	rest := normalized[idx+len(field)+2:]
	rest = strings.TrimSpace(rest)
	// Look for quoted value
	if len(rest) > 0 && rest[0] == '\'' {
		end := strings.Index(rest[1:], "'")
		if end >= 0 {
			return strings.ToLower(rest[1 : end+1])
		}
	}
	return ""
}

// extractSelectColumns does a best-effort parse of column names/aliases from a SELECT.
// This is used to return proper column headers for pg_catalog queries so that
// clients like psycopg2 don't crash on empty tuples.
func extractSelectColumns(sql string) []string {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(upper, "SELECT ") {
		return nil
	}

	// Find the portion between SELECT and FROM
	fromIdx := strings.Index(upper, " FROM ")
	selectPart := sql[7:] // skip "SELECT "
	if fromIdx > 7 {
		selectPart = sql[7:fromIdx]
	}

	// Split on commas (rough — doesn't handle nested parens perfectly but good enough)
	parts := strings.Split(selectPart, ",")
	var cols []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Check for AS alias
		upperP := strings.ToUpper(p)
		if asIdx := strings.LastIndex(upperP, " AS "); asIdx >= 0 {
			alias := strings.TrimSpace(p[asIdx+4:])
			alias = strings.Trim(alias, "\"")
			cols = append(cols, alias)
			continue
		}
		// Use the last dot-separated part (e.g., "t.oid" -> "oid")
		if dotIdx := strings.LastIndex(p, "."); dotIdx >= 0 {
			cols = append(cols, strings.TrimSpace(p[dotIdx+1:]))
			continue
		}
		// Use as-is (trim any remaining spaces)
		cols = append(cols, p)
	}
	return cols
}

func isCommandSQL(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK")
}

// Wire protocol message reading/writing

func (c *pgConn) readMessage() (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	msgLen := int(binary.BigEndian.Uint32(header[1:])) - 4
	if msgLen < 0 || msgLen > 100*1024*1024 {
		return 0, nil, fmt.Errorf("invalid message length: %d", msgLen)
	}
	if msgLen == 0 {
		return msgType, nil, nil
	}
	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

func (c *pgConn) sendAuthOk() {
	// 'R' + int32(8) + int32(0) = AuthenticationOk
	msg := []byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}
	c.conn.Write(msg)
}

func (c *pgConn) sendParamStatus(key, value string) {
	// 'S' + int32(len) + key\0 + value\0
	payload := make([]byte, 0, len(key)+len(value)+2)
	payload = append(payload, key...)
	payload = append(payload, 0)
	payload = append(payload, value...)
	payload = append(payload, 0)
	c.sendMsg('S', payload)
}

func (c *pgConn) sendBackendKeyData(pid, secret int32) {
	// 'K' + int32(12) + int32(pid) + int32(secret)
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[0:], 12)
	binary.BigEndian.PutUint32(buf[4:], uint32(pid))
	binary.BigEndian.PutUint32(buf[8:], uint32(secret))
	c.conn.Write([]byte{'K'})
	c.conn.Write(buf[:])
}

func (c *pgConn) sendReadyForQuery() {
	// 'Z' + int32(5) + byte(txState)
	state := c.txState
	if state == 0 {
		state = 'I'
	}
	c.conn.Write([]byte{'Z', 0, 0, 0, 5, state})
}

func (c *pgConn) sendEmptyQuery() {
	// 'I' + int32(4)
	c.conn.Write([]byte{'I', 0, 0, 0, 4})
}

func (c *pgConn) sendRowDescription(columns []string) {
	c.buf = c.buf[:0]

	// Field count (int16)
	c.buf = appendInt16(c.buf, int16(len(columns)))

	for _, col := range columns {
		// Field name (null-terminated string)
		c.buf = append(c.buf, col...)
		c.buf = append(c.buf, 0)
		// Table OID (int32) = 0
		c.buf = appendInt32(c.buf, 0)
		// Column attr number (int16) = 0
		c.buf = appendInt16(c.buf, 0)
		// Data type OID (int32) = 25 (TEXT)
		c.buf = appendInt32(c.buf, 25)
		// Data type size (int16) = -1 (variable)
		c.buf = appendInt16(c.buf, -1)
		// Type modifier (int32) = -1
		c.buf = appendInt32(c.buf, -1)
		// Format code (int16) = 0 (text)
		c.buf = appendInt16(c.buf, 0)
	}

	c.sendMsg('T', c.buf)
}

// sendTypedRowDescription emits a RowDescription ('T') message with correct
// PostgreSQL type OIDs derived from ColumnMeta. This is critical for JDBC/ODBC
// drivers that use OIDs to determine Java/C types for result columns.
func (c *pgConn) sendTypedRowDescription(metas []caelum.ColumnMeta) {
	c.buf = c.buf[:0]
	c.buf = appendInt16(c.buf, int16(len(metas)))

	for _, m := range metas {
		// Field name (null-terminated)
		c.buf = append(c.buf, m.Name...)
		c.buf = append(c.buf, 0)
		// Table OID (int32) = 0
		c.buf = appendInt32(c.buf, 0)
		// Column attr number (int16) = 0
		c.buf = appendInt16(c.buf, 0)
		// Data type OID
		oid := pgTypeOID(m.TypeName)
		c.buf = appendInt32(c.buf, int32(oid))
		// Data type size
		c.buf = appendInt16(c.buf, pgTypeSize(oid))
		// Type modifier (int32) = -1
		c.buf = appendInt32(c.buf, -1)
		// Format code (int16) = 0 (text)
		c.buf = appendInt16(c.buf, 0)
	}

	c.sendMsg('T', c.buf)
}

// pgTypeSize returns the type size for a PostgreSQL type OID.
// Fixed-size types report their byte size; variable-length types report -1.
func pgTypeSize(oid int) int16 {
	switch oid {
	case 16: // bool
		return 1
	case 21: // int2
		return 2
	case 23: // int4
		return 4
	case 20: // int8
		return 8
	case 700: // float4
		return 4
	case 701: // float8
		return 8
	case 1082: // date
		return 4
	case 1114: // timestamp
		return 8
	default:
		return -1 // variable length
	}
}

func (c *pgConn) sendDataRow(columns []string, row map[string]any) {
	c.buf = c.buf[:0]

	// Column count (int16)
	c.buf = appendInt16(c.buf, int16(len(columns)))

	for _, col := range columns {
		val, ok := row[col]
		if !ok || val == nil {
			// NULL: length = -1
			c.buf = appendInt32(c.buf, -1)
			continue
		}
		s := formatPgValue(val)
		c.buf = appendInt32(c.buf, int32(len(s)))
		c.buf = append(c.buf, s...)
	}

	c.sendMsg('D', c.buf)
}

// sendDataRowFormatted sends a DataRow using the format codes from Bind.
// Columns with format code 1 (binary) get binary-encoded values.
func (c *pgConn) sendDataRowFormatted(columns []string, row map[string]any, fmtCodes []int16) {
	c.buf = c.buf[:0]
	c.buf = appendInt16(c.buf, int16(len(columns)))

	for i, col := range columns {
		val, ok := row[col]
		if !ok || val == nil {
			c.buf = appendInt32(c.buf, -1)
			continue
		}

		// Determine format for this column
		binary := false
		if len(fmtCodes) == 1 {
			binary = fmtCodes[0] == 1 // single code applies to all columns
		} else if i < len(fmtCodes) {
			binary = fmtCodes[i] == 1
		}

		if binary {
			c.buf = appendBinaryValue(c.buf, val)
		} else {
			s := formatPgValue(val)
			c.buf = appendInt32(c.buf, int32(len(s)))
			c.buf = append(c.buf, s...)
		}
	}

	c.sendMsg('D', c.buf)
}

// appendBinaryValue appends a value in PostgreSQL binary format.
func appendBinaryValue(buf []byte, val any) []byte {
	switch v := val.(type) {
	case bool:
		buf = appendInt32(buf, 1)
		if v {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case int32:
		buf = appendInt32(buf, 4)
		buf = appendInt32(buf, v)
	case int64:
		buf = appendInt32(buf, 8)
		buf = append(buf, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case int:
		v64 := int64(v)
		buf = appendInt32(buf, 8)
		buf = append(buf, byte(v64>>56), byte(v64>>48), byte(v64>>40), byte(v64>>32),
			byte(v64>>24), byte(v64>>16), byte(v64>>8), byte(v64))
	case float32:
		buf = appendInt32(buf, 4)
		bits := math.Float32bits(v)
		buf = append(buf, byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits))
	case float64:
		buf = appendInt32(buf, 8)
		bits := math.Float64bits(v)
		buf = append(buf, byte(bits>>56), byte(bits>>48), byte(bits>>40), byte(bits>>32),
			byte(bits>>24), byte(bits>>16), byte(bits>>8), byte(bits))
	case string:
		buf = appendInt32(buf, int32(len(v)))
		buf = append(buf, v...)
	default:
		// Fallback: text encoding
		s := fmt.Sprintf("%v", v)
		buf = appendInt32(buf, int32(len(s)))
		buf = append(buf, s...)
	}
	return buf
}

// formatPgValue formats a value for PgWire text output.
// Produces SQL-like formatting for nested types (arrays, maps, structs).
func formatPgValue(val any) string {
	switch tv := val.(type) {
	case []any:
		var buf strings.Builder
		buf.WriteByte('[')
		for i, elem := range tv {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(formatPgValue(elem))
		}
		buf.WriteByte(']')
		return buf.String()
	case map[string]any:
		var buf strings.Builder
		buf.WriteByte('{')
		first := true
		for k, v := range tv {
			if !first {
				buf.WriteString(", ")
			}
			first = false
			buf.WriteString(k)
			buf.WriteString(": ")
			buf.WriteString(formatPgValue(v))
		}
		buf.WriteByte('}')
		return buf.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (c *pgConn) sendCommandComplete(tag string) {
	payload := append([]byte(tag), 0)
	c.sendMsg('C', payload)
}

func (c *pgConn) sendError(severity, code, message string) {
	// ErrorResponse: 'E' + fields
	var payload []byte
	// Severity
	payload = append(payload, 'S')
	payload = append(payload, severity...)
	payload = append(payload, 0)
	// Severity (non-localized, V)
	payload = append(payload, 'V')
	payload = append(payload, severity...)
	payload = append(payload, 0)
	// SQLSTATE code
	payload = append(payload, 'C')
	payload = append(payload, code...)
	payload = append(payload, 0)
	// Message
	payload = append(payload, 'M')
	payload = append(payload, message...)
	payload = append(payload, 0)
	// Terminator
	payload = append(payload, 0)

	c.sendMsg('E', payload)
}

func (c *pgConn) sendMsg(typ byte, payload []byte) {
	var header [5]byte
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)+4))
	c.conn.Write(header[:])
	if len(payload) > 0 {
		c.conn.Write(payload)
	}
}

// Helpers

func parseStartupParams(data []byte) map[string]string {
	params := make(map[string]string)
	for len(data) > 1 {
		key := readCString(data)
		data = data[len(key)+1:]
		if len(data) == 0 || key == "" {
			break
		}
		value := readCString(data)
		data = data[len(value)+1:]
		params[key] = value
	}
	return params
}

func readCString(data []byte) string {
	for i, b := range data {
		if b == 0 {
			return string(data[:i])
		}
	}
	return string(data)
}

func appendInt16(buf []byte, v int16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

func appendInt32(buf []byte, v int32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
