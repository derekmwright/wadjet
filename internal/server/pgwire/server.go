// Package pgwire implements the PostgreSQL v3 wire protocol frontend.
// This allows psql, JDBC, ODBC, and any Postgres-compatible client to
// connect to Wadjet and execute SQL queries.
package pgwire

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
	"golang.org/x/net/netutil"
)

// Server listens for PostgreSQL wire protocol connections and dispatches
// queries to a Wadjet DB instance.
type Server struct {
	db           *wadjet.DB
	coord        *coordinator.Coordinator // optional; SELECT routes through native-DAG when set
	listener     net.Listener
	logger       *slog.Logger
	wg           sync.WaitGroup
	done         chan struct{}
	tlsConfig    *tls.Config
	maxConns     int
	queryTimeout time.Duration
	authProvider *auth.Provider // nil = no auth enforcement
	querySem     chan struct{}  // nil = unlimited concurrent queries
	queryQueue   int64          // atomic: number of queries waiting for admission

	// Live sessions keyed by the cancellation pid handed out in
	// BackendKeyData. A CancelRequest arrives on its own connection and
	// finds the target here. See cancel.go.
	sessionsMu sync.Mutex
	sessions   map[int32]*pgConn
}

// SetCoordinator attaches a coordinator so SELECT statements stream through
// coord.ExecuteSQL (native-DAG executor with batched output) instead of the
// legacy wadjet.DB.Query path which materializes all rows into a single
// CollectSink — root cause of the 2026-04-25 Q18 SF10 OOM.
//
// When unset, all paths fall back to db.Query (current behavior; safe for
// any caller that hasn't migrated). When set, only SELECT queries route
// through coord; DDL / DESCRIBE / introspection stay on db.Query for now.
func (s *Server) SetCoordinator(coord *coordinator.Coordinator) {
	s.coord = coord
}

// Config holds configuration for the pgwire server.
type Config struct {
	Addr             string         // listen address, e.g. ":5433"
	TLSConfig        *tls.Config    // nil = plain TCP
	MaxConnections   int            // 0 = unlimited
	MaxConcurrentQry int            // 0 = unlimited concurrent queries
	QueryTimeout     time.Duration  // 0 = no timeout
	AuthProvider     *auth.Provider // nil = no auth enforcement
}

// NewServer creates a new PostgreSQL wire protocol server.
func NewServer(db *wadjet.DB, cfg Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		db:           db,
		logger:       logger,
		done:         make(chan struct{}),
		tlsConfig:    cfg.TLSConfig,
		maxConns:     cfg.MaxConnections,
		queryTimeout: cfg.QueryTimeout,
		authProvider: cfg.AuthProvider,
	}
	if cfg.MaxConcurrentQry > 0 {
		s.querySem = make(chan struct{}, cfg.MaxConcurrentQry)
	}
	return s
}

// acquireQuery blocks until a query slot is available, or ctx is cancelled.
// Returns true if acquired, false if the context was cancelled.
func (s *Server) acquireQuery(ctx context.Context) bool {
	if s.querySem == nil {
		return true
	}
	atomic.AddInt64(&s.queryQueue, 1)
	defer atomic.AddInt64(&s.queryQueue, -1)
	select {
	case s.querySem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// releaseQuery returns a query slot to the pool.
func (s *Server) releaseQuery() {
	if s.querySem == nil {
		return
	}
	<-s.querySem
}

// QueuedQueries returns the number of queries waiting for admission.
func (s *Server) QueuedQueries() int64 {
	return atomic.LoadInt64(&s.queryQueue)
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

	// TLS is negotiated per-connection via the PostgreSQL SSLRequest protocol,
	// not by wrapping the listener. The client sends SSLRequest, we respond 'S',
	// then upgrade the raw connection to TLS before the regular StartupMessage.

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
	// Backstop under pgConn.dispatch's per-message boundary: a panic raised
	// during startup, or one that escapes the message boundary itself, drops
	// THIS connection and no other. Without it the accept-loop goroutine
	// dies and takes the process with it (#511).
	defer func() {
		if r := recover(); r != nil {
			_ = exec.RecoverQueryPanic(context.Background(), "pgwire connection", r)
		}
	}()
	c := &pgConn{
		conn:         conn,
		db:           s.db,
		coord:        s.coord,
		server:       s,
		logger:       s.logger,
		buf:          make([]byte, 0, 4096),
		tlsConfig:    s.tlsConfig,
		queryTimeout: s.queryTimeout,
		stmts:        make(map[string]string),
		stmtOIDs:     make(map[string][]uint32),
		sessionVars:  make(map[string]string),
		txState:      'I',
		authProvider: s.authProvider,
	}
	defer s.unregisterSession(c)
	c.run()
}

// pgConn handles a single PostgreSQL client connection.
type pgConn struct {
	conn         net.Conn
	db           *wadjet.DB
	coord        *coordinator.Coordinator // optional: routes SELECT through native-DAG when non-nil
	server       *Server                  // back-pointer for query admission
	logger       *slog.Logger
	buf          []byte
	tlsConfig    *tls.Config   // non-nil = offer TLS upgrade on SSLRequest
	queryTimeout time.Duration // server-level default; overridden by statement_timeout

	// Authentication
	authProvider *auth.Provider // nil = no auth
	identity     *auth.Identity // set after successful auth

	// skipUntilSync is PostgreSQL's extended-query error state: after an
	// error is reported for a Parse/Bind/Describe/Execute/Close message,
	// every further message is discarded until Sync, which is the one that
	// emits ReadyForQuery. See dispatch.
	skipUntilSync bool

	// Session variables (SET key = value)
	sessionVars map[string]string

	// Extended Query protocol state
	preparedSQL     string                  // last parsed statement SQL
	preparedOIDs    []uint32                // parameter type OIDs Parse declared for it
	portalSQL       string                  // last bound portal SQL
	stmts           map[string]string       // named prepared statements
	stmtOIDs        map[string][]uint32     // their declared parameter type OIDs
	described       bool                    // true if Describe was sent for current portal
	describedFields int                     // field count of the RowDescription Describe sent
	resultFmtCodes  []int16                 // result format codes from Bind (0=text, 1=binary)
	describeResult  *wadjet.QueryResult     // cached Describe result for Execute reuse
	describeStream  coordinator.BatchStream // columnar half of describeResult (coord path)
	// describeNestedSchema is the declared ROW/ARRAY/MAP structure (field
	// order, element type) sendDataRow needs to render a composite value in
	// PostgreSQL's text form, keyed by output column name — resolved
	// alongside describeResult so Execute reuses the SAME resolution rather
	// than re-deriving it (and, for the legacy query path, re-paying the
	// catalog round-trip nestedColumnSchemas makes).
	describeNestedSchema *nestedFieldSchema
	describeErr          error               // cached Describe-time execution failure for Execute replay
	describeCancel       string              // set when that failure was a cancellation: the 57014 message to replay
	describeSynth        *synthAnswer        // cached Describe-time introspection answer
	describedSQL         string              // statement the three caches above belong to
	paramOIDCache        map[string][]uint32 // per-statement inferred parameter OIDs (see paraminfer.go)

	// Transaction state: 'I' = idle, 'T' = in transaction, 'E' = failed
	txState byte

	// Cancellation key material advertised in BackendKeyData, and the
	// statement a CancelRequest carrying it stops. stmt is written by this
	// connection's goroutine and read by the cancelling connection's, so it
	// is guarded; it is nil whenever no statement is executing. See cancel.go.
	cancelPID    int32
	cancelSecret int32
	stmtMu       sync.Mutex
	stmt         *runningStmt
}

func (c *pgConn) run() {
	// Release a cached Describe result stream the client never Executed —
	// for over-budget results the stream pins spill scratch on disk.
	defer c.closeDescribeCache()

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

		// The last line of defence for process survival (#511). Every layer
		// below converts what it can, but this one owns the invariant that a
		// panic reached from ONE connection's message must not end the
		// server for every other connection. It reports the internal error
		// on this connection and returns to the read loop, so the session
		// stays usable — exactly what a client sees from PostgreSQL when the
		// backend hits an internal error it can report.
		if !c.dispatch(msgType, payload) {
			return
		}
	}
}

// dispatch handles one protocol message under the connection's panic
// boundary. It returns false when the connection should close (Terminate, or
// a panic that left the protocol stream unusable).
//
// Who owes a ReadyForQuery is not uniform, and getting it wrong is a SILENT
// WRONG ANSWER rather than a visible failure. handleQuery ('Q') sends its own
// Z on every path it can take. The extended-query handlers (P/B/D/E/C) send
// none — in that sub-protocol only Sync does. So a boundary that answered
// every panic with ErrorResponse + Z emitted a SPURIOUS Z whenever the panic
// came from an extended-query message: the client, still waiting for its own
// Sync's Z, consumed the stale one as the reply to its NEXT statement and got
// zero rows back on a healthy connection.
//
// PostgreSQL's own rule is the fix: on an error inside extended-query
// processing the backend reports it, then DISCARDS every message until Sync,
// and only Sync emits the ReadyForQuery. skipUntilSync is that state.
func (c *pgConn) dispatch(msgType byte, payload []byte) (keepGoing bool) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err := exec.RecoverQueryPanic(context.Background(), "pgwire message", r)
		code := sqlerr.StateOf(err)
		if code == "" {
			code = exec.SQLStateInternalError
		}
		c.sendError("ERROR", code, err.Error())
		if msgType == 'Q' {
			// Simple query: handleQuery owed the Z and died before sending
			// it, so the boundary owes it instead.
			c.sendReadyForQuery()
		} else {
			// Extended query: Sync owes the Z. Enter the error state and let
			// it arrive there, exactly once.
			c.skipUntilSync = true
		}
		keepGoing = true
	}()

	// In the error state every message is discarded until Sync ends it.
	// Terminate still closes the connection.
	if c.skipUntilSync && msgType != 'S' && msgType != 'X' {
		return true
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
		// Ends any extended-query error state and emits the single
		// ReadyForQuery the client has been waiting for.
		c.skipUntilSync = false
		c.sendReadyForQuery()
	case 'C': // Close (prepared statement or portal)
		c.handleClose(payload)
	case 'X': // Terminate
		return false
	default:
		// Same desync the recover() above fixes for a panic: this is never
		// 'Q' (Q has its own case), so it can arrive between an
		// extended-query message and that sequence's Sync. Answering it
		// with an immediate Z here is the same spurious-Z bug — the
		// client's own Sync hasn't run yet, so it consumes this one as the
		// reply to whatever it sends next. Report the error and let Sync,
		// the only message that owns a Z once we are in that state,
		// deliver it.
		c.sendError("ERROR", "08P01", fmt.Sprintf("unsupported message type: %c", msgType))
		c.skipUntilSync = true
	}
	return true
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
		if c.tlsConfig != nil {
			// Accept SSL — upgrade connection to TLS
			c.conn.Write([]byte{'S'})
			tlsConn := tls.Server(c.conn, c.tlsConfig)
			if err := tlsConn.Handshake(); err != nil {
				return fmt.Errorf("TLS handshake: %w", err)
			}
			c.conn = tlsConn
			c.logger.Debug("pgwire TLS connection established")
		} else {
			// Decline SSL — send 'N'
			c.conn.Write([]byte{'N'})
		}
		// Re-read the actual startup message
		return c.handleStartup()
	}

	// Handle cancel request (80877102). This connection exists only to carry
	// the request: PostgreSQL sends nothing back on it — no error, no
	// ReadyForQuery — and closes it, so the caller returns without writing.
	if version == cancelRequestCode {
		c.server.handleCancelRequest(payload[4:])
		return errCancelRequest
	}

	// Expect protocol version 3.0 (196608 = 3<<16)
	if version != 196608 {
		return fmt.Errorf("unsupported protocol version: %d", version)
	}

	// Parse startup parameters (key=value pairs, null-terminated)
	params := parseStartupParams(payload[4:])

	// Authenticate if auth is enabled
	if err := c.authenticate(params); err != nil {
		c.sendError("FATAL", "28P01", fmt.Sprintf("authentication failed: %v", err))
		return err
	}

	// Send AuthenticationOk
	c.sendAuthOk()

	// Send ParameterStatus messages (clients like psql expect these)
	c.sendParamStatus("server_version", "15.0 (Wadjet)")
	c.sendParamStatus("server_encoding", "UTF8")
	c.sendParamStatus("client_encoding", "UTF8")
	c.sendParamStatus("DateStyle", "ISO, MDY")
	c.sendParamStatus("integer_datetimes", "on")
	c.sendParamStatus("standard_conforming_strings", "on")
	c.sendParamStatus("TimeZone", "UTC")
	c.sendParamStatus("IntervalStyle", "postgres")
	if c.identity != nil && c.identity.Role != "admin" {
		c.sendParamStatus("is_superuser", "off")
	} else {
		c.sendParamStatus("is_superuser", "on")
	}

	// Send BackendKeyData (session handle + secret key for cancellation).
	// The pair must be real: it is the only way a client can later stop a
	// statement, and it identifies this session in the server's registry.
	if err := c.server.registerSession(c); err != nil {
		c.sendError("FATAL", "53000", err.Error())
		return err
	}
	c.sendBackendKeyData(c.cancelPID, c.cancelSecret)

	// Send ReadyForQuery
	c.sendReadyForQuery()

	return nil
}

// authenticate performs PostgreSQL cleartext password authentication.
// When auth is disabled (no provider), all connections are accepted.
// When enabled, sends AuthenticationCleartextPassword, reads the password
// response, and resolves identity via API key or JWT token.
func (c *pgConn) authenticate(params map[string]string) error {
	// No auth provider or auth disabled — accept all connections
	if c.authProvider == nil || !c.authProvider.Enabled() {
		return nil
	}

	authn := c.authProvider.Authenticator()
	if authn == nil {
		return nil
	}

	// Request cleartext password: 'R' + int32(8) + int32(3)
	c.conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 3})

	// Read PasswordMessage ('p')
	msgType, payload, err := c.readMessage()
	if err != nil {
		return fmt.Errorf("reading password: %w", err)
	}
	if msgType != 'p' {
		return fmt.Errorf("expected PasswordMessage, got '%c'", msgType)
	}

	token := readCString(payload)
	if token == "" {
		return auth.ErrNoCredentials
	}

	id, err := authn.AuthenticateToken(token)
	if err != nil {
		return err
	}

	c.identity = id
	c.logger.Debug("pgwire authenticated",
		"identity", id.String(),
		"user", params["user"],
	)
	return nil
}

// queryContext returns a context enriched with the connection's identity and
// timeout, registered as this connection's cancellable statement so a
// CancelRequest arriving on another connection can stop it. The returned
// CancelFunc unregisters and releases the statement; callers must defer it.
func (c *pgConn) queryContext() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if c.identity != nil {
		ctx = auth.ContextWithIdentity(ctx, c.identity)
	}
	// Session-level statement_timeout overrides server default
	timeout := c.queryTimeout
	if v, ok := c.sessionVars["statement_timeout"]; ok {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			timeout = time.Duration(ms) * time.Millisecond
		}
	}
	return c.beginStatement(ctx, timeout)
}

// handleSet parses "SET key = value" / "SET key TO value" and stores the session variable.
func (c *pgConn) handleSet(sql string) {
	// Strip "SET " prefix, handle optional "LOCAL" or "SESSION" keywords
	s := strings.TrimSpace(sql[4:])
	upper := strings.ToUpper(s)
	if strings.HasPrefix(upper, "LOCAL ") || strings.HasPrefix(upper, "SESSION ") {
		s = strings.TrimSpace(s[strings.IndexByte(s, ' ')+1:])
	}

	// Split on " = " or " TO "
	var key, val string
	if idx := strings.Index(strings.ToUpper(s), " TO "); idx >= 0 {
		key = strings.TrimSpace(s[:idx])
		val = strings.TrimSpace(s[idx+4:])
	} else if idx := strings.IndexByte(s, '='); idx >= 0 {
		key = strings.TrimSpace(s[:idx])
		val = strings.TrimSpace(s[idx+1:])
	} else {
		return // can't parse, silently accept
	}

	// Strip quotes from value
	val = strings.Trim(val, "'\"")
	key = strings.ToLower(key)
	c.sessionVars[key] = val
}

// parseCopySQL extracts the table name and optional column list from a
// COPY table [(col1, col2, ...)] FROM STDIN statement.
func parseCopySQL(sql string) (table string, columns []string, delimiter rune) {
	delimiter = '\t' // PostgreSQL default
	upper := strings.ToUpper(sql)

	// Strip "COPY " prefix
	rest := strings.TrimSpace(sql[5:])

	// Find table name (up to '(' or whitespace)
	var tableName string
	if idx := strings.IndexAny(rest, " (\t"); idx >= 0 {
		tableName = rest[:idx]
		rest = strings.TrimSpace(rest[idx:])
	} else {
		tableName = rest
		rest = ""
	}
	table = strings.Trim(tableName, "\"")

	// Parse optional column list
	if len(rest) > 0 && rest[0] == '(' {
		end := strings.IndexByte(rest, ')')
		if end > 0 {
			colStr := rest[1:end]
			for _, c := range strings.Split(colStr, ",") {
				col := strings.TrimSpace(c)
				col = strings.Trim(col, "\"")
				if col != "" {
					columns = append(columns, col)
				}
			}
			rest = strings.TrimSpace(rest[end+1:])
		}
	}

	// Check for delimiter option: WITH (DELIMITER 'x') or WITH DELIMITER 'x'
	if idx := strings.Index(upper, "DELIMITER"); idx >= 0 {
		after := strings.TrimSpace(sql[idx+9:])
		after = strings.TrimLeft(after, "( ")
		if len(after) >= 3 && after[0] == '\'' {
			delimiter = rune(after[1])
		}
	}

	// CSV format uses comma by default
	if strings.Contains(upper, "FORMAT CSV") || strings.Contains(upper, "(FORMAT CSV") ||
		strings.Contains(upper, "CSV") && !strings.Contains(upper, "DELIMITER") {
		if !strings.Contains(upper, "DELIMITER") {
			delimiter = ','
		}
	}

	return table, columns, delimiter
}

// handleCopyIn implements the PostgreSQL COPY FROM STDIN protocol.
// It sends a CopyInResponse, reads CopyData messages until CopyDone,
// then ingests the rows into the target table.
func (c *pgConn) handleCopyIn(sql string) {
	ctx, cancel := c.queryContext()
	defer cancel()

	tableName, copyColumns, delimiter := parseCopySQL(sql)

	// Look up table schema. COPY REFERENCES an existing table, so it takes
	// the same READ concession every other reference does — a mixed-case name
	// stays reachable unquoted (catalog.ResolveTableName) — and the resolved
	// spelling is what the rows are written under.
	tableName = c.db.Catalog().ResolveTableName(tableName)
	tableMeta, err := c.db.Catalog().GetTable(ctx, tableName)
	if err != nil {
		c.sendError("ERROR", "42P01", fmt.Sprintf("table %q does not exist", tableName))
		return
	}

	// Determine column ordering
	var columns []string
	if len(copyColumns) > 0 {
		columns = copyColumns
	} else {
		columns = make([]string, len(tableMeta.Schema.Columns))
		for i, col := range tableMeta.Schema.Columns {
			columns[i] = col.Name
		}
	}

	// Build type map for value conversion
	// The whole COLUMN, not its TypeID: a DECIMAL field is judged against the
	// declared (p, s) as the row is read, so COPY names the row that carried
	// a value the column cannot hold instead of failing a later flush (#647).
	colByName := make(map[string]parquet.Column, len(tableMeta.Schema.Columns))
	for _, col := range tableMeta.Schema.Columns {
		colByName[col.Name] = col
	}

	numCols := int16(len(columns))

	// Send CopyInResponse: 'G' + format(1 byte) + num_cols(int16) + col_formats(int16 each)
	payload := make([]byte, 0, 3+2*int(numCols))
	payload = append(payload, 0) // text format overall
	payload = appendInt16(payload, numCols)
	for range numCols {
		payload = appendInt16(payload, 0) // text format per column
	}
	c.sendMsg('G', payload)

	// Read CopyData messages and accumulate rows
	const flushBatch = 10000
	var rows []map[string]any
	var rowCount int64

	ing := ingest.New(c.db.Catalog(), tableName, tableMeta.Schema,
		tableMeta.PartitionKeys, ingest.DefaultConfig())

	for {
		msgType, msgPayload, err := c.readMessage()
		if err != nil {
			c.logger.Debug("COPY read error", "err", err)
			return
		}

		switch msgType {
		case 'd': // CopyData
			// Parse tab-delimited rows from the data chunk
			data := string(msgPayload)
			for _, line := range strings.Split(data, "\n") {
				line = strings.TrimRight(line, "\r")
				if line == "" || line == "\\." {
					continue
				}
				fields := strings.Split(line, string(delimiter))
				if len(fields) != len(columns) {
					c.sendError("ERROR", "22P04",
						fmt.Sprintf("COPY: expected %d columns, got %d", len(columns), len(fields)))
					// Drain remaining messages until CopyDone/CopyFail
					c.drainCopy()
					return
				}

				row := make(map[string]any, len(columns))
				for i, colName := range columns {
					val := fields[i]
					// Handle PostgreSQL NULL representation
					if val == "\\N" {
						row[colName] = nil
						continue
					}
					// Unescape backslash sequences
					val = unescapeCopyText(val)
					// ConvertTextForColumn, not ConvertValueForColumn: a COPY
					// field is raw text, not a SQL literal. The literal
					// converter reads the word `null` as the keyword and
					// strips a leading/trailing apostrophe, so a field
					// spelled `NULL` became a SQL NULL — even though COPY's
					// own NULL marker is `\N` and is handled above (#690).
					v, err := wadjet.ConvertTextForColumn(val, colByName[colName])
					if err != nil {
						// The converter's own SQLSTATE, not a hardcoded
						// 22P02: a DECIMAL field past the column's declared
						// precision is 22003 numeric_value_out_of_range and
						// only text that names no number is 22P02, and a COPY
						// client branches on which (#647 re-review). 22P02
						// stays the fallback for the converters that raise a
						// plain error.
						state := sqlerr.StateOf(err)
						if state == "" {
							state = "22P02"
						}
						c.sendError("ERROR", state,
							fmt.Sprintf("COPY: column %q: %v", colName, err))
						c.drainCopy()
						return
					}
					row[colName] = v
				}
				rows = append(rows, row)
				rowCount++

				// Batch flush to avoid unbounded memory
				if len(rows) >= flushBatch {
					if err := ing.Ingest(ctx, rows); err != nil {
						c.copyIngestError(err)
						c.drainCopy()
						return
					}
					rows = rows[:0]
				}
			}

		case 'c': // CopyDone
			// Ingest remaining rows
			if len(rows) > 0 {
				if err := ing.Ingest(ctx, rows); err != nil {
					c.copyIngestError(err)
					return
				}
			}
			if err := ing.FlushAll(ctx); err != nil {
				c.sendError("ERROR", "XX000", fmt.Sprintf("COPY flush: %v", err))
				return
			}
			c.sendCommandComplete(fmt.Sprintf("COPY %d", rowCount))
			return

		case 'f': // CopyFail
			errMsg := readCString(msgPayload)
			c.logger.Debug("COPY cancelled by client", "reason", errMsg)
			c.sendError("ERROR", "57014", fmt.Sprintf("COPY cancelled: %s", errMsg))
			return

		default:
			c.sendError("ERROR", "08P01",
				fmt.Sprintf("unexpected message type during COPY: %c", msgType))
			return
		}
	}
}

// drainCopy reads and discards messages until CopyDone or CopyFail is received.
func (c *pgConn) drainCopy() {
	for {
		msgType, _, err := c.readMessage()
		if err != nil || msgType == 'c' || msgType == 'f' {
			return
		}
	}
}

// unescapeCopyText handles PostgreSQL COPY text format escape sequences.
func unescapeCopyText(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case '\\':
				b.WriteByte('\\')
			default:
				b.WriteByte(s[i+1])
			}
			i++
		} else {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// handleQuery answers one simple-protocol Query message.
//
// A simple-protocol string can carry SEVERAL statements, and PostgreSQL runs
// them as a SEQUENCE: it parses the WHOLE string first, then executes the
// statements in order with one CommandComplete each, an error stopping the
// sequence, and exactly ONE ReadyForQuery ending the message. Before this the
// only semicolon handling here was `strings.TrimRight(sql, ";")`, so a
// two-statement string was ONE statement to everything below and the tail's
// fate depended on which sub-parser swallowed it: `INSERT …; INSERT …` ran the
// first and silently dropped the second, and `INSERT …; ZZZ NOT SQL` ran the
// INSERT and silently ignored the garbage (#711).
//
// WHO OWES THE ReadyForQuery is the thing to get right, and it is why the
// per-statement work is a separate function that never sends one. This
// function sends exactly one Z per message on every path it can take; a PANIC
// sends none from here and dispatch's recover sends it instead, which is the
// invariant that function documents and which a second Z would break for every
// client on the connection.
func (c *pgConn) handleQuery(sql string) {
	c.runSimpleQuery(sql)
	c.sendReadyForQuery()
}

// runSimpleQuery runs a simple-protocol string as a sequence. It never sends a
// ReadyForQuery: handleQuery owns the one this message gets.
func (c *pgConn) runSimpleQuery(sql string) {
	if c.logger != nil {
		c.logger.Debug("pgwire simple query", "sql", sql)
	}
	stmts := plansql.SplitStatements(sql)
	if len(stmts) == 0 {
		c.sendEmptyQuery()
		return
	}

	// PARSE THE WHOLE STRING FIRST. `INSERT …; ZZZ NOT SQL` runs NOTHING in
	// PostgreSQL — the syntax error is raised before the first statement
	// executes — and running the INSERT and dropping the garbage is the
	// silent half of #711.
	//
	// Only when there IS more than one statement: for a single statement
	// "parse the whole string first" and "parse it when you run it" are the
	// same thing, and paying for a second parse on every simple query to make
	// them look different would be a cost for nothing.
	if len(stmts) > 1 && !c.parseWholeString(stmts) {
		return
	}

	for _, stmt := range stmts {
		if !c.runSimpleStatement(stmt) {
			// An error stops the sequence, as it does in PostgreSQL. What
			// PostgreSQL ALSO does is roll the earlier statements back — the
			// simple string is one implicit transaction — and wadjet has no
			// transactions to roll back with, so the statements that already
			// ran stay. That divergence is recorded in ADR-0012's list and in
			// docs/sql-reference.md; it is the engine's transaction scope, not
			// this door's sequencing.
			return
		}
	}
}

// parseWholeString parses every statement of a multi-statement string before
// any of them runs, reporting the first failure. It returns whether to
// continue.
//
// Its own statement context, in its own function, so the cancel is scoped to
// the parse rather than to the whole sequence: beginStatement makes the
// connection's current statement the one it just started, and a `defer` inside
// an `if` block in runSimpleQuery would hold this one open across every
// statement that follows.
func (c *pgConn) parseWholeString(stmts []string) bool {
	ctx, cancel := c.queryContext()
	defer cancel()
	for _, stmt := range stmts {
		if err := c.checkSimpleStatement(stmt); err != nil {
			// 42601 is the fallback, not the answer: sendQueryError prefers
			// the class the error already carries, and a parse failure that
			// carries none is a syntax error.
			c.sendQueryError(ctx, "42601", err)
			return false
		}
	}
	return true
}

// checkSimpleStatement reports whether one statement of a multi-statement
// string is something this door can run, without running it.
//
// It has to agree with runSimpleStatement about what "can run" means, which is
// why it repeats that function's prefix tests rather than only calling
// plansql.Parse: `BEGIN`, `SET …`, `DISCARD ALL` and the introspection
// answers are statements this server handles WITHOUT the parser, and a check
// that ran the parser over them would refuse a script every BI tool sends.
func (c *pgConn) checkSimpleStatement(sql string) error {
	if simpleStatementIsHandledWithoutParsing(strings.ToUpper(sql)) {
		return nil
	}
	if ans := c.matchIntrospection(sql, strings.ToUpper(sql)); ans != nil {
		return nil
	}
	_, err := plansql.Parse(sql)
	return err
}

// simpleStatementIsHandledWithoutParsing lists the statement prefixes
// runSimpleStatement answers from the connection state rather than from the
// parser. It takes the UPPERCASED statement, as the dispatch below does.
func simpleStatementIsHandledWithoutParsing(upper string) bool {
	switch {
	case strings.HasPrefix(upper, "BEGIN"),
		strings.HasPrefix(upper, "COMMIT"),
		strings.HasPrefix(upper, "END"),
		strings.HasPrefix(upper, "ROLLBACK"),
		strings.HasPrefix(upper, "SET "),
		strings.HasPrefix(upper, "RESET "),
		strings.HasPrefix(upper, "DISCARD "),
		strings.HasPrefix(upper, "DEALLOCATE"):
		return true
	}
	return strings.HasPrefix(upper, "COPY ") && strings.Contains(upper, "FROM STDIN")
}

// runSimpleStatement runs ONE statement and sends its CommandComplete (or its
// rows and then its CommandComplete, or an ErrorResponse). It reports whether
// the sequence should continue.
func (c *pgConn) runSimpleStatement(sql string) bool {
	// Special handling for SET/RESET/DISCARD/BEGIN/COMMIT/ROLLBACK
	// that BI tools send during connection setup
	upper := strings.ToUpper(sql)
	if strings.HasPrefix(upper, "BEGIN") {
		c.txState = 'T'
		c.sendCommandComplete("BEGIN")
		return true
	}
	if strings.HasPrefix(upper, "COMMIT") || strings.HasPrefix(upper, "END") {
		c.txState = 'I'
		c.sendCommandComplete("COMMIT")
		return true
	}
	if strings.HasPrefix(upper, "ROLLBACK") {
		c.txState = 'I'
		c.sendCommandComplete("ROLLBACK")
		return true
	}
	if strings.HasPrefix(upper, "SET ") {
		c.handleSet(sql)
		c.sendCommandComplete("SET")
		return true
	}
	if strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ") ||
		strings.HasPrefix(upper, "DEALLOCATE") {
		c.sendCommandComplete("SET")
		return true
	}

	// Handle introspection/synthetic queries (SELECT 1, version(), pg_catalog, etc.)
	if ans := c.matchIntrospection(sql, upper); ans != nil {
		c.sendSynthAnswer(ans)
		return true
	}

	// Handle COPY FROM STDIN
	if strings.HasPrefix(upper, "COPY ") && strings.Contains(upper, "FROM STDIN") {
		c.handleCopyIn(sql)
		return true
	}

	// Handle DML (INSERT/UPDATE/DELETE/MERGE) via Execute path.
	//
	// MERGE was missing here, so it fell through to the QUERY path and every
	// merge reported `SELECT 1` — a command tag naming the wrong statement
	// and the wrong count, which for a client is the statement's whole
	// answer. PostgreSQL reports `MERGE <n>` (#686 R2-5).
	if isDMLSQL(sql) {
		ctx, cancel := c.queryContext()
		defer cancel()
		if !c.server.acquireQuery(ctx) {
			c.sendQueryError(ctx, "53300", errors.New("query queue timeout"))
			return false
		}
		result, err := c.db.Execute(ctx, sql)
		c.server.releaseQuery()
		if err != nil {
			c.sendQueryError(ctx, "42000", err)
			return false
		}
		c.sendCommandComplete(commandTag(result.Command, result.RowsAffected))
		return true
	}

	ctx, cancel := c.queryContext()
	defer cancel()
	if !c.server.acquireQuery(ctx) {
		c.sendQueryError(ctx, "53300", errors.New("query queue timeout"))
		return false
	}
	var result *wadjet.QueryResult
	var stream coordinator.BatchStream
	var nestedSchema *nestedFieldSchema
	var err error
	if c.coord != nil && c.canBypassDB() && shouldRouteThroughCoord(sql) {
		result, stream, nestedSchema, err = c.queryViaCoord(ctx, sql)
	} else {
		result, err = c.db.Query(ctx, sql)
		if err == nil {
			nestedSchema = c.nestedColumnSchemas(sql, result.ColumnMetas)
		}
	}
	c.server.releaseQuery()
	if err != nil {
		c.sendQueryError(ctx, "42000", err)
		return false
	}
	// A cancelled statement must never answer with rows. Execution does not
	// always surface the cancellation as an error — exec.Pipeline's parallel
	// path returns whatever its workers collected before they were stopped,
	// with a nil error — and a silently truncated result reported as success
	// is worse than no result: the client believes it saw the whole table.
	if msg := canceledMessage(ctx); msg != "" {
		if stream != nil {
			stream.Close()
		}
		c.sendError("ERROR", sqlstateQueryCanceled, msg)
		return false
	}

	// Send RowDescription
	columns := result.Columns
	if len(columns) == 0 && len(result.Rows) > 0 {
		for k := range result.Rows[0] {
			columns = append(columns, k)
		}
	}
	// Coord-path results always carry columns when any batch exists (the
	// gather receiver derives them from the first batch's schema), so an
	// empty columns list means an empty result on both paths.
	//
	// EmptyQueryResponse is NOT the answer here, and sending it was half of
	// #846. 'I' is PostgreSQL's reply to an empty query STRING, never to a
	// statement that ran and returned nothing: psql prints nothing at all for
	// it and pgJDBC's executeQuery throws "No results were returned by the
	// query", because the client was handed no result set to be empty. A
	// SELECT that produced no columns still gets a RowDescription — an empty
	// one when the plan could declare nothing — and its CommandComplete, so
	// the client sees an empty result set rather than no result set. The
	// remaining zero-field shape is `SELECT *` over a JOIN, whose declared
	// schema is deferred; this door is what keeps it a legal answer.
	if len(columns) == 0 && len(result.Rows) == 0 {
		if stream != nil {
			stream.Close()
		}
		c.sendRowDescription(nil, nil)
		c.sendCommandComplete("SELECT 0")
		return true
	}
	if len(result.ColumnMetas) > 0 {
		c.sendTypedRowDescription(result.ColumnMetas, nil)
	} else {
		c.sendRowDescription(columns, nil)
	}

	// Send DataRow for each row — coord-path batches are boxed and sent
	// one batch at a time, never materialized as a whole.
	sent, sendErr := c.sendResultRows(ctx, columns, stream, result, nil, result.ColumnMetas, nestedSchema)
	if sendErr != nil {
		// Partial DataRows followed by ErrorResponse is legal in the v3
		// protocol; the client discards the partial result.
		c.sendQueryError(ctx, "58030", fmt.Errorf("reading result batches: %w", sendErr))
		return false
	}

	// Send CommandComplete
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", sent))
	return true
}

// Extended Query protocol handlers

func (c *pgConn) handleParse(payload []byte) {
	// Parse message: name\0 + query\0 + int16(numParamTypes) + int32[](paramOIDs)
	name := readCString(payload)
	payload = payload[len(name)+1:]
	sql := readCString(payload)
	payload = payload[len(sql)+1:]

	// The declared parameter types. Bind needs them to know whether a
	// parameter renders as a bare number or a quoted literal, and Describe
	// echoes them back as the ParameterDescription. A client may declare
	// none, or declare OID 0 for a parameter it wants the server to infer.
	var oids []uint32
	if len(payload) >= 2 {
		n := int(binary.BigEndian.Uint16(payload[:2]))
		payload = payload[2:]
		if n > 0 && len(payload) >= n*4 {
			oids = make([]uint32, n)
			for i := 0; i < n; i++ {
				oids[i] = binary.BigEndian.Uint32(payload[i*4:])
			}
		}
	}

	// A PREPARED STATEMENT CARRIES ONE STATEMENT. PostgreSQL refuses a
	// multi-statement string here with 42601, `cannot insert multiple commands
	// into a prepared statement`, and accepts one only on the simple query
	// protocol — measured against 17.11 through pgx in QueryExecModeExec, for
	// `INSERT …; INSERT …`, `DELETE …; DELETE …`, `SELECT …; SELECT …`,
	// `DELETE …; SELECT …` and `SELECT …; DELETE …` alike.
	//
	// This is the door #711's sequencing must NOT reach: Bind/Execute answer
	// with one CommandComplete and one result set, so a second statement here
	// has nowhere to be reported. Refusing is not a limitation, it is what
	// PostgreSQL does.
	//
	// The refusal follows the extended protocol's error discipline: report and
	// enter skipUntilSync, so every message up to the client's Sync is
	// discarded and Sync — the only message that owes a Z in this
	// sub-protocol — delivers it exactly once. A ReadyForQuery from here would
	// be the spurious-Z desync dispatch's comment describes.
	if err := plansql.CheckSingleStatement(sql); err != nil {
		code := sqlerr.StateOf(err)
		if code == "" {
			code = "42601"
		}
		c.sendError("ERROR", code, err.Error())
		c.skipUntilSync = true
		return
	}

	if name == "" {
		c.preparedSQL = sql
		c.preparedOIDs = oids
	} else {
		c.stmts[name] = sql
		c.stmtOIDs[name] = oids
	}
	if c.logger != nil {
		c.logger.Debug("pgwire parse", "stmt", name, "sql", sql)
	}
	c.described = false
	c.describedFields = 0
	c.closeDescribeCache() // invalidate cached result for new statement

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
	oids := c.preparedOIDs
	if stmtName != "" {
		if s, ok := c.stmts[stmtName]; ok {
			sql = s
			oids = c.stmtOIDs[stmtName]
		}
	}

	// Fill in the parameter types the client left to the server. An OID-0
	// parameter's text bytes used to render as a quoted string, so an int
	// column compared against '7' matched the wrong row (#365). Inference
	// gives renderParam the same OID ParameterDescription reports.
	if inferred := c.inferParamOIDs(sql, oids); len(inferred) >= len(oids) {
		oids = inferred
	}

	// Read the parameters and render each as the SQL literal that stands in
	// for it. The planner takes SQL text, not bound values, so substitution
	// is how a parameter reaches it — and the literal has to carry the
	// parameter's type. See bindparams.go.
	if len(payload) >= 2 {
		numFmt := int(binary.BigEndian.Uint16(payload[:2]))
		payload = payload[2:]
		// Format codes: 0 of them means every parameter is text, 1 means that
		// one code applies to all of them, otherwise there is one per
		// parameter.
		fmtCodes := make([]int16, 0, numFmt)
		if len(payload) >= numFmt*2 {
			for i := 0; i < numFmt; i++ {
				fmtCodes = append(fmtCodes, int16(binary.BigEndian.Uint16(payload[i*2:])))
			}
			payload = payload[numFmt*2:]
		}
		if len(payload) >= 2 {
			numParams := int(binary.BigEndian.Uint16(payload[:2]))
			payload = payload[2:]
			literals := make([]string, numParams)
			for i := 0; i < numParams; i++ {
				literals[i] = "NULL"
				if len(payload) < 4 {
					break
				}
				paramLen := int(int32(binary.BigEndian.Uint32(payload[:4])))
				payload = payload[4:]
				if paramLen < 0 {
					continue // NULL parameter — the literal is already NULL
				}
				if len(payload) < paramLen {
					break
				}
				raw := payload[:paramLen]
				payload = payload[paramLen:]

				binaryFmt := false
				switch {
				case len(fmtCodes) == 1:
					binaryFmt = fmtCodes[0] == 1
				case i < len(fmtCodes):
					binaryFmt = fmtCodes[i] == 1
				}
				var oid uint32
				if i < len(oids) {
					oid = oids[i]
				}

				lit, err := renderParam(raw, binaryFmt, oid)
				if err != nil {
					c.sendError("ERROR", "22023", fmt.Sprintf("binding parameter $%d: %v", i+1, err))
					return
				}
				literals[i] = lit
			}
			sql = substituteParams(sql, literals)
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
		sql := c.preparedSQL
		oids := c.preparedOIDs
		if name := readCString(payload[1:]); name != "" {
			if s, ok := c.stmts[name]; ok {
				sql = s
				oids = c.stmtOIDs[name]
			}
		}

		// ParameterDescription: one OID per placeholder (issue #305 item 9).
		// A client that declared types at Parse gets them echoed back. The
		// ones it left to the server are INFERRED from their comparison
		// context where possible (#365) — the same answer Bind renders by,
		// so the client's encoding and the server's reading cannot disagree.
		// A placeholder inference cannot type stays OID 0, "unknown", which
		// every driver understands; claiming zero parameters for a statement
		// that has three was not honest, and pgJDBC reads the count to size
		// its parameter list.
		if inferred := c.inferParamOIDs(sql, oids); len(inferred) >= len(oids) {
			oids = inferred
		}
		c.buf = c.buf[:0]
		c.buf = appendInt16(c.buf, int16(len(oids)))
		for _, oid := range oids {
			c.buf = appendInt32(c.buf, int32(oid))
		}
		c.sendMsg('t', c.buf)

		// A statement Describe is unconditionally text — only a PORTAL
		// carries the Bind's result format codes (#362).
		c.describeSQL(sql, nil)
	} else {
		// Portal describe — send RowDescription based on portal SQL, declaring
		// the result format codes the portal's Bind requested (#362).
		c.describeSQL(c.portalSQL, c.resultFmtCodes)
	}
}

// describeSQL executes a SQL statement to discover its result columns
// and sends either a typed RowDescription or NoData. fmtCodes are the result
// format codes the RowDescription declares per field (see sendRowDescription).
func (c *pgConn) describeSQL(sql string, fmtCodes []int16) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimRight(sql, ";")
	sql = strings.TrimSpace(sql)

	if sql == "" || isCommandSQL(sql) {
		c.sendNoData()
		return
	}

	// A DML statement without RETURNING produces no tuples, so it describes
	// as NoData — PostgreSQL's answer, and the only honest one.
	//
	// It used to fall through to the execution below, which meant Describe
	// RAN THE WRITE to discover a shape it does not have, cached the
	// one-row `{"result": "DELETE 2"}` that DB.Query wraps a DML result in,
	// and let Execute report that row's shape: `SELECT 1` for every INSERT,
	// UPDATE, DELETE and MERGE a driver sent (#816). Every ORM reads
	// RowsAffected from that tag, so an optimistic-concurrency check —
	// `UPDATE … WHERE version = ?`, then "if 0 rows, someone else won" —
	// could never detect a conflict. Not describing it here is half the fix;
	// handleExecute's DML branch is the other half.
	if isDMLSQL(sql) {
		c.closeDescribeCache()
		c.describedSQL = sql
		c.sendNoData()
		return
	}

	// Introspection queries answer synthetically. Describe resolves them
	// through the same matcher Execute uses and describes the exact column
	// list that Execute will send rows for — a guessed description (the old
	// extractSelectColumns path) can disagree with the answer, and a driver
	// that trusts the description then misreads the tuples.
	upper := strings.ToUpper(sql)
	if ans := c.matchIntrospection(sql, upper); ans != nil {
		c.closeDescribeCache()
		c.describeSynth = ans
		c.describedSQL = sql
		c.sendSynthRowDescription(ans, fmtCodes)
		c.described, c.describedFields = true, len(ans.cols)
		return
	}

	// A statement still holding $N placeholders does not parse — Bind
	// substitutes them into the portal — but Describe has to answer the shape
	// now: pgJDBC ties a RowDescription to the Describe it sent and does not
	// pick up a later one. So the shape is discovered from the statement with
	// NULL standing in for the parameters. The rows that run produces are the
	// wrong rows for any portal, so nothing is cached from it.
	shapeSQL, parameterized := substituteNullParams(sql)

	// Execute the real query to get typed column metadata.
	// Cache the result so Execute can reuse it instead of re-executing,
	// which avoids column order mismatches for SELECT * (map iteration
	// order is non-deterministic in Go).
	ctx, cancel := c.queryContext()
	defer cancel()
	var result *wadjet.QueryResult
	var stream coordinator.BatchStream
	var nestedSchema *nestedFieldSchema
	var err error
	if c.coord != nil && c.canBypassDB() && shouldRouteThroughCoord(shapeSQL) {
		result, stream, nestedSchema, err = c.queryViaCoord(ctx, shapeSQL)
	} else {
		result, err = c.db.Query(ctx, shapeSQL)
		if err == nil {
			nestedSchema = c.nestedColumnSchemas(shapeSQL, result.ColumnMetas)
		}
	}
	if err != nil {
		// Can't describe — send NoData rather than error, and CACHE the
		// failure for Execute to replay. The query already ran to failure
		// here; re-executing it at Execute would double the cost of every
		// deterministic failure (and against a broken environment the
		// second run can behave far worse than the first — the 2026-08-10
		// disk-full repro's first execution failed in 4s, then the silent
		// re-execution hung to the query timeout on lost task results).
		c.closeDescribeCache()
		c.describeErr = err
		// Remember WHY it failed while the statement context still exists.
		// Execute replays this error after that context is gone, so a
		// cancelled statement would otherwise replay as a generic error and
		// the client would see its own stop button as a query failure.
		c.describeCancel = canceledMessage(ctx)
		c.describedSQL = sql
		// The client now has NoData for this statement; if a later Execute
		// nevertheless produces rows, drivers fail with "tuples but no
		// field structure" — this line is the breadcrumb for that hunt.
		if c.logger != nil {
			c.logger.Debug("pgwire describe: no data (execution failed)", "sql", sql, "err", err)
		}
		c.sendNoData()
		return
	}
	c.closeDescribeCache() // release any prior cached stream before overwriting
	if parameterized {
		// Shape only: these rows answer NULL parameters, not the portal's.
		if stream != nil {
			stream.Close()
		}
	} else {
		c.describeResult = result
		c.describeStream = stream
		c.describeNestedSchema = nestedSchema
		c.describedSQL = sql
	}

	if len(result.ColumnMetas) > 0 {
		c.sendTypedRowDescription(result.ColumnMetas, fmtCodes)
		c.described, c.describedFields = true, len(result.ColumnMetas)
	} else if len(result.Columns) > 0 {
		c.sendRowDescription(result.Columns, fmtCodes)
		c.described, c.describedFields = true, len(result.Columns)
	} else if parameterized {
		// ZERO COLUMNS HERE MEANS "UNKNOWN", NOT "KNOWN EMPTY".
		//
		// This shape was probed with NULL standing in for every parameter
		// (substituteNullParams above), so it answered the wrong rows on
		// purpose — and for a statement whose schema the planner does not
		// declare, no rows means no columns to read one off. Promising an
		// empty RowDescription here is promising something that was never
		// measured: `SELECT * FROM a JOIN b ON … WHERE a.c0 = $1` described
		// 0 fields, Execute produced 6, and shapeAgrees refused the tuples
		// with 42804 — a statement PostgreSQL and this door's own base
		// answered with a row. NoData keeps described=false, which is what
		// lets ensureDescribed send the REAL description at Execute, where
		// the portal's parameters are bound and the shape is knowable.
		//
		// The #846 guarantee is not weakened by this: a parameterized
		// statement whose schema the planner CAN declare took the typed
		// branch above with its real columns, zero rows or not. Only the
		// deferred join star reaches here, and it is answered at Execute.
		c.sendNoData()
	} else {
		// A QUERY describes as a RowDescription, empty or not — the other
		// two shapes that describe as NoData (a command, and DML without
		// RETURNING) have already returned above, and a parameterized probe
		// just above. NoData here was the extended-protocol half of #846:
		// pgJDBC's executeQuery ties itself to the Describe it sent, so a
		// zero-column answer arrived as "No results were returned by the
		// query" instead of an empty ResultSet. The only shape that still
		// reaches this line is one whose declared schema the planner could
		// not derive — `SELECT *` over a JOIN — run without parameters, so
		// its emptiness was MEASURED rather than assumed, and an empty
		// RowDescription is a legal result set where NoData is not one.
		c.sendRowDescription(nil, fmtCodes)
		c.described, c.describedFields = true, 0
	}
}

// sendNoData sends NoData ('n') and records that this statement has no
// promised result shape. Execute must not produce tuples after it.
func (c *pgConn) sendNoData() {
	c.described = false
	c.describedFields = 0
	c.sendMsg('n', nil)
}

func (c *pgConn) handleExecute(payload []byte) {
	// Execute: portal\0 + int32(maxRows)
	sql := strings.TrimSpace(c.portalSQL)
	if c.logger != nil {
		c.logger.Debug("pgwire execute", "sql", sql, "described", c.described)
	}
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
	if strings.HasPrefix(upper, "SET ") {
		c.handleSet(sql)
		c.sendCommandComplete("SET")
		return
	}
	if strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ") ||
		strings.HasPrefix(upper, "DEALLOCATE") ||
		strings.HasPrefix(upper, "CLOSE") {
		c.sendCommandComplete("SET")
		return
	}

	// DML on the EXTENDED protocol. This branch did not exist: a write fell
	// through to c.db.Query below, which wraps a DML result as the one row
	// `{"result": "DELETE 2"}`, and the SELECT tag was emitted for it — so
	// every INSERT, UPDATE, DELETE and MERGE sent by pgx, JDBC, psycopg or
	// any ORM completed with `SELECT 1` (#816). The table state was right; a
	// client's RowsAffected was 1 whatever happened.
	//
	// The same shape as the simple path's branch, minus the ReadyForQuery:
	// on this protocol Sync sends it, and sending one here would put a second
	// 'Z' on the wire for one Query message.
	if isDMLSQL(sql) {
		ctx, cancel := c.queryContext()
		defer cancel()
		if !c.server.acquireQuery(ctx) {
			c.sendQueryError(ctx, "53300", errors.New("query queue timeout"))
			return
		}
		result, err := c.db.Execute(ctx, sql)
		c.server.releaseQuery()
		if err != nil {
			c.sendQueryError(ctx, "42000", err)
			return
		}
		c.sendCommandComplete(commandTag(result.Command, result.RowsAffected))
		return
	}

	// Handle catalog/introspection queries from BI tools. Describe resolved
	// the same statement through the same matcher, so the RowDescription the
	// client holds already describes these columns; Execute sends tuples only.
	if ans := c.introspectionAnswer(sql, upper); ans != nil {
		// A Describe-time failure cached for this statement is moot once the
		// introspection layer answers it — dropping it here keeps a stale
		// error from replaying on the NEXT statement of this connection.
		c.closeDescribeCache()
		if !c.shapeAgrees(len(ans.cols), sql) {
			return
		}
		c.ensureDescribed(ans.cols, nil, c.resultFmtCodes)
		c.sendSynthRows(ans, c.resultFmtCodes)
		return
	}

	// Everything Describe cached belongs to the statement Describe ran. A
	// portal carrying different SQL — Bind substitutes parameter literals, so
	// `WHERE id = $1` becomes `WHERE id = '2'` — gets none of it: replaying a
	// placeholder statement's parse failure would fail an executable portal,
	// and reusing its rows would answer with the wrong ones.
	if c.describedSQL != sql {
		c.closeDescribeCache()
	}

	// Replay a Describe-time execution failure instead of re-executing:
	// the query already ran to failure once; Execute's job is to surface
	// that error to the client, not to run the query again.
	if c.describeErr != nil {
		err := c.describeErr
		cancelMsg := c.describeCancel
		c.describeErr, c.describeCancel = nil, ""
		// A statement cancelled during Describe must still report 57014 when
		// Execute replays its failure. Replaying the raw error made a
		// cancelled query surface as a generic 42000 "native DAG: context
		// canceled", which a client reads as a broken query rather than as
		// the cancellation it asked for — DataGrip showed an error dialog
		// for its own stop button.
		if cancelMsg != "" {
			c.sendError("ERROR", sqlstateQueryCanceled, cancelMsg)
			return
		}
		code := "42000"
		if s := sqlerr.StateOf(err); s != "" {
			code = s
		}
		c.sendError("ERROR", code, err.Error())
		return
	}

	// Reuse the cached result from Describe when available. This avoids
	// re-executing the query AND ensures column order matches the
	// RowDescription (critical for SELECT * where Go map iteration order
	// is non-deterministic).
	//
	// The statement context is created before the branch so that sending a
	// cached Describe result is cancellable too — that result is a replay
	// stream, and replaying millions of rows is exactly what a client hits
	// the stop button over.
	ctx, cancel := c.queryContext()
	defer cancel()
	var result *wadjet.QueryResult
	var stream coordinator.BatchStream
	var nestedSchema *nestedFieldSchema
	if c.describeResult != nil {
		result = c.describeResult
		stream = c.describeStream
		nestedSchema = c.describeNestedSchema
		c.describeResult = nil
		c.describeStream = nil
		c.describeNestedSchema = nil
	} else {
		var err error
		if c.coord != nil && c.canBypassDB() && shouldRouteThroughCoord(sql) {
			result, stream, nestedSchema, err = c.queryViaCoord(ctx, sql)
		} else {
			result, err = c.db.Query(ctx, sql)
			if err == nil {
				nestedSchema = c.nestedColumnSchemas(sql, result.ColumnMetas)
			}
		}
		if err != nil {
			c.sendQueryError(ctx, "42000", err)
			return
		}
	}
	// See handleQuery: a cancelled statement answers 57014, never a
	// truncated result that execution failed to report as an error.
	if msg := canceledMessage(ctx); msg != "" {
		if stream != nil {
			stream.Close()
		}
		c.sendError("ERROR", sqlstateQueryCanceled, msg)
		return
	}

	columns := result.Columns
	if len(columns) == 0 && len(result.Rows) > 0 {
		for k := range result.Rows[0] {
			columns = append(columns, k)
		}
	}
	if len(columns) == 0 {
		if stream != nil {
			stream.Close()
		}
		c.sendCommandComplete("SELECT 0")
		return
	}
	if !c.shapeAgrees(len(columns), sql) {
		if stream != nil {
			stream.Close()
		}
		return
	}
	c.ensureDescribed(columns, result.ColumnMetas, c.resultFmtCodes)

	// Extended query protocol: Execute sends only DataRow + CommandComplete.
	// RowDescription was already sent by Describe. Do NOT send it again.
	// Coord-path batches are boxed and sent one batch at a time.
	sent, sendErr := c.sendResultRows(ctx, columns, stream, result, c.resultFmtCodes, result.ColumnMetas, nestedSchema)
	if sendErr != nil {
		c.sendQueryError(ctx, "58030", fmt.Errorf("reading result batches: %w", sendErr))
		return
	}
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", sent))
}

// introspectionAnswer resolves sql for the Execute path, reusing the answer
// Describe already computed for the same statement. The reuse is keyed on the
// SQL text: Bind substitutes parameter literals into the portal, so a portal
// can carry a narrower query than the statement Describe saw, and that portal
// deserves its own answer (a pg_class OID lookup for `relname = $1` matches
// nothing; the bound `relname = 'users'` matches a table).
func (c *pgConn) introspectionAnswer(sql, upper string) *synthAnswer {
	if c.describeSynth != nil && c.describedSQL == sql {
		return c.describeSynth
	}
	return c.matchIntrospection(sql, upper)
}

// ensureDescribed sends a RowDescription for the result Execute is about to
// emit when the client holds none — Describe answered NoData (a parameterized
// statement it cannot plan yet, or a Describe-time failure) or the client
// skipped Describe entirely.
//
// PostgreSQL never needs this: it can describe a parameterized statement, so
// its Execute only ever carries tuples. Wadjet's Describe runs the statement
// to learn its shape and a statement still holding $N placeholders does not
// run, so the shape is only knowable once Bind has substituted them. Tuples
// with no field structure are the one thing a driver cannot recover from, so
// the structure goes out first.
func (c *pgConn) ensureDescribed(cols []string, metas []wadjet.ColumnMeta, fmtCodes []int16) {
	if c.described {
		return
	}
	if len(metas) > 0 {
		c.sendTypedRowDescription(metas, fmtCodes)
		c.describedFields = len(metas)
	} else {
		c.sendRowDescription(cols, fmtCodes)
		c.describedFields = len(cols)
	}
	c.described = true
}

// shapeAgrees enforces the extended-protocol invariant that Execute's tuples
// fit the field structure Describe promised. Describe and Execute route
// through the same decisions, so a disagreement is a server bug — reporting
// it as an error beats emitting DataRows a driver cannot read ("Received
// resultset tuples, but no field structure for them").
func (c *pgConn) shapeAgrees(cols int, sql string) bool {
	if !c.described || c.describedFields == cols {
		return true
	}
	if c.logger != nil {
		c.logger.Error("pgwire execute: result shape disagrees with Describe",
			"sql", sql, "described_fields", c.describedFields, "execute_fields", cols)
	}
	c.sendError("ERROR", "42804", fmt.Sprintf(
		"result shape changed between Describe and Execute: described %d columns, execute produced %d",
		c.describedFields, cols))
	return false
}

func (c *pgConn) handleClose(payload []byte) {
	// Close: type('S'/'P') + name\0
	if len(payload) >= 1 && payload[0] == 'S' {
		name := readCString(payload[1:])
		delete(c.stmts, name)
		delete(c.stmtOIDs, name)
	}
	// Send CloseComplete ('3')
	c.sendMsg('3', nil)
}

// synthAnswer is a fully materialized introspection answer: the columns the
// server promises in RowDescription and the rows it sends for them.
//
// Describe and Execute resolve a statement through the same matchIntrospection
// call, so the shape a driver is promised and the shape it receives come from
// one decision instead of two independent pattern matches. The pair drifting
// apart is what the 2026-08-17 DataGrip report was: Describe failed and sent
// NoData, Execute matched a substring and sent a one-column row anyway, and
// pgJDBC reported "Received resultset tuples, but no field structure for them".
type synthAnswer struct {
	cols []string
	rows []map[string]any
}

func singleRow(cols []string, row map[string]any) *synthAnswer {
	return &synthAnswer{cols: cols, rows: []map[string]any{row}}
}

// colOID infers the type OID a synthetic column must declare, from the first
// non-NULL value under it. Declaring everything as text (OID 25) sent DataGrip
// down pgJDBC's numeric accessor for boolean model fields — the column claimed
// VARCHAR, its reader called getInt, and toInt("f") threw "Bad value for type
// int : f" on a byte-perfect row. The values were never the problem; the
// declared type was.
func (ans *synthAnswer) colOID(col string) int32 {
	for _, row := range ans.rows {
		switch row[col].(type) {
		case nil:
		case bool:
			return 16
		case int32:
			return 23
		case int, int64:
			return 20
		case float32:
			return 700
		case float64:
			return 701
		default:
			return 25
		}
	}
	return 25
}

// matchIntrospection resolves pg_catalog queries and the synthetic expressions
// BI tools (Superset, SQLAlchemy, DBeaver, DataGrip, psql) send during
// connection setup. It returns nil when the statement is not one this layer
// answers — the caller then runs it on the query engine.
func (c *pgConn) matchIntrospection(sql, upper string) *synthAnswer {
	// Comments are not part of the statement. Strip them before anything
	// reads the text, so subject detection and column shaping both see what
	// the server would actually run (see stripSQLComments).
	if strings.Contains(sql, "--") || strings.Contains(sql, "/*") {
		sql = stripSQLComments(sql)
		upper = strings.ToUpper(sql)
	}

	// Normalize whitespace for matching (newlines, tabs → spaces)
	normalized := strings.Join(strings.Fields(upper), " ")

	// SHOW statements (SHOW TRANSACTION ISOLATION LEVEL, SHOW server_version, etc.)
	if strings.HasPrefix(normalized, "SHOW ") {
		return c.matchShow(normalized)
	}

	// version() is often spelled with a pg_catalog qualifier, which would
	// otherwise fall into the blanket pg_catalog intercept below and come
	// back empty. Only the bare one-expression form is claimed here; any
	// richer spelling (an alias, more columns) is a real query the engine
	// answers with version()'s value under the client's own labels.
	if list, ok := selectList(normalized); ok &&
		(list == "VERSION()" || list == "PG_CATALOG.VERSION()") {
		return singleRow([]string{"version"}, map[string]any{
			"version": expr.ServerVersion,
		})
	}

	// The catalog intercept matches on statement text, so it may only look at
	// statements that read. A write whose literal happens to mention a catalog
	// name — INSERT INTO audit(msg) VALUES ('pg_class scan failed') — used to
	// be answered with an empty result set and a success tag, and never
	// executed: a silent lost write.
	if !strings.HasPrefix(normalized, "SELECT ") && !strings.HasPrefix(normalized, "WITH ") {
		return nil
	}

	// pg_stat_ssl: pgJDBC probes its own connection's TLS state right after
	// startup (select ssl from pg_stat_ssl where pid = pg_backend_pid()).
	// Left unclaimed this reached the engine, which tried to SCAN a table
	// named pg_stat_ssl and aborted DataGrip's whole introspection pass.
	// Answer from the connection's actual state.
	if strings.Contains(normalized, "PG_STAT_SSL") {
		_, isTLS := c.conn.(*tls.Conn)
		return singleRow([]string{"ssl"}, map[string]any{"ssl": isTLS})
	}

	// pg_catalog / information_schema introspection — return real catalog data
	// for table/column discovery, empty results for everything else.
	//
	// A statement that reads relations is claimed if and only if one of them
	// is a system relation (pg_ / information_schema) that is not a real
	// table here. Substring matching claimed real queries: SELECT
	// pg_typeof(id) FROM users contains "PG_TYPE" and was answered with a
	// silent empty result (#305 item 8). A FROM-less statement has no
	// relations to protect, so the text match stands for it: the pg_catalog
	// functions clients call there include ones the engine deliberately does
	// not implement (see pgcompat.go's ledger), and the empty-but-coherent
	// answer keeps those clients moving.
	var isPgCatalog bool
	if refs := relationRefsAll(normalized); len(refs) > 0 {
		isPgCatalog = c.claimsSystemRelations(refs)
	} else {
		isPgCatalog = strings.Contains(normalized, "PG_CATALOG") ||
			strings.Contains(normalized, "INFORMATION_SCHEMA") ||
			strings.Contains(normalized, "PG_TYPE") ||
			strings.Contains(normalized, "PG_NAMESPACE") ||
			strings.Contains(normalized, "PG_CLASS") ||
			strings.Contains(normalized, "PG_ATTRIBUTE") ||
			strings.Contains(normalized, "PG_DATABASE")
	}

	if isPgCatalog {
		ctx, cancel := c.queryContext()
		defer cancel()
		if ans := c.matchCatalogQuery(ctx, sql, normalized); ans != nil {
			return ans
		}
		// Fallback: empty result with the columns the SELECT list names.
		cols := extractSelectColumns(sql)
		if len(cols) == 0 {
			cols = []string{"?column?"}
		}
		return &synthAnswer{cols: cols}
	}

	// SELECT with no FROM clause — evaluate common synthetic expressions.
	// Wadjet requires FROM, but PostgreSQL clients expect these to work.
	if strings.HasPrefix(normalized, "SELECT ") && !strings.Contains(normalized, " FROM ") {
		return c.matchSyntheticSelect(normalized)
	}

	return nil
}

// claimsSystemRelations reports whether refs — the relations a statement
// reads, at any depth — include one in PostgreSQL's reserved pg_ namespace
// (or information_schema) that this server does not have as a real table.
//
// The intercept above once was a list of catalog names spelled out one at a
// time, which meant every system relation nobody thought of reached the query
// engine, where it became a scan of a table that does not exist:
//
//	select usesuper from pg_user where usename = current_user
//	→ ERROR: stage scan-0 has no dependencies and no ScanFiles
//
// That is what DataGrip hit on pg_user after pg_stat_ssl was fixed the same
// way, one name at a time. PostgreSQL reserves the pg_ prefix for system
// catalogs, so a FROM/JOIN target under it is introspection by definition and
// belongs to this layer — answered with real data where this server knows it,
// and with an empty result of the right shape where it does not. The catalog
// is still consulted, so a table that genuinely exists is never intercepted.
func (c *pgConn) claimsSystemRelations(refs []string) bool {
	if len(refs) == 0 {
		return false
	}
	var systemRefs []string
	for _, r := range refs {
		if strings.HasPrefix(r, "PG_") || strings.HasPrefix(r, "INFORMATION_SCHEMA.") {
			systemRefs = append(systemRefs, r)
		}
	}
	if len(systemRefs) == 0 {
		return false
	}
	// A real table wins over the reserved-prefix rule: this layer must never
	// swallow a query the engine can actually answer.
	ctx, cancel := c.queryContext()
	defer cancel()
	tables, err := c.db.ListTables(ctx)
	if err != nil {
		return true // catalog unavailable; a pg_ scan would fail anyway
	}
	for _, r := range systemRefs {
		real := false
		for _, t := range tables {
			if strings.EqualFold(t, r) {
				real = true
				break
			}
		}
		if !real {
			return true
		}
	}
	return false
}

// catalogRank orders the catalog relations this layer models by how specific
// a subject they are. A statement that reads pg_attribute is about columns
// even when it enters through a pg_namespace/pg_class join chain, which is
// how pgJDBC's DatabaseMetaData.getColumns is written.
var catalogRank = map[string]int{
	"PG_ATTRIBUTE": 5,
	"PG_CLASS":     4,
	"PG_TABLES":    4,
	"PG_TYPE":      3,
	"PG_DATABASE":  2,
	"PG_NAMESPACE": 1,
	"PG_USER":      0,
	"PG_SHADOW":    0,
	"PG_ROLES":     0,
}

// catalogSubject returns the catalog relation a statement is about, or "" when
// this layer should not answer it as any of them.
//
// A branch that fires on "the statement mentions my relation somewhere" will
// answer a question nobody asked: DataGrip's foreign-data-wrapper query joins
// pg_namespace twice to resolve handler schemas, matched the pg_namespace
// branch on that alone, and came back as a one-row schema listing whose
// fdwname-labelled "name" column was NULL — which the client turned into
// "Argument for @NotNull parameter 'name' ... must not be null" and stopped
// introspecting. So the FROM target has to be a relation this layer models
// before any branch may claim the statement; when it is, the most specific
// relation among all its references wins.
func catalogSubject(normalized string) string {
	refs := relationRefs(normalized)
	if len(refs) == 0 {
		return ""
	}
	if _, ok := catalogRank[refs[0]]; !ok {
		// A CTE name is not a relation — it is a local name for a subquery,
		// so the statement's subject is whatever the outer query joins it to.
		// DataGrip reads columns as `from T join pg_catalog.pg_attribute C`
		// where T wraps pg_class; declining on T alone returned no columns at
		// all. Only the outer query's own relations are considered, never the
		// CTE's body: the rules query also wraps pg_class in a CTE but joins
		// pg_rewrite, and it is about rules, which this layer does not model.
		if !isCTEName(normalized, refs[0]) {
			// Subject is a relation this layer does not model (pg_tablespace,
			// pg_foreign_data_wrapper, pg_event_trigger, pg_locks, ...). It
			// gets the empty-but-coherent answer, not another relation's rows.
			return ""
		}
		refs = refs[1:]
	}
	best, bestRank := "", -1
	for _, r := range refs {
		if rank, ok := catalogRank[r]; ok && rank > bestRank {
			best, bestRank = r, rank
		}
	}
	return best
}

// isCTEName reports whether name is bound by the statement's WITH clause.
func isCTEName(normalized, name string) bool {
	if !strings.HasPrefix(normalized, "WITH ") {
		return false
	}
	return strings.Contains(normalized, " "+name+" AS (") ||
		strings.HasPrefix(normalized, "WITH "+name+" AS (")
}

// relationRefs returns the relation names a normalized statement reads: the
// token after each FROM or JOIN at paren depth zero, with any pg_catalog
// qualifier stripped.
//
// Depth is what makes FROM mean FROM. `extract(epoch from
// pg_postmaster_start_time())` carries the word inside a function call, and
// reading it as a clause makes the timestamp function look like a system
// relation — which turned DataGrip's startup-time query into an empty
// intercepted answer. Only a top-level FROM introduces a relation.
func relationRefs(normalized string) []string {
	return scanRelationRefs(normalized, false)
}

// relationRefsAll is relationRefs at every paren depth: a subquery's FROM
// reads a relation just as the outer one does, and the claim decision (does
// this statement read a system catalog?) has to see it — pgJDBC's type query
// joins pg_type against a subquery over pg_type. A token terminated by an
// opening paren is a function call, never a relation, which is what keeps
// `extract(epoch from pg_postmaster_start_time())` out of the refs here too.
func relationRefsAll(normalized string) []string {
	return scanRelationRefs(normalized, true)
}

func scanRelationRefs(normalized string, anyDepth bool) []string {
	var refs []string
	depth, tokDepth := 0, 0
	inSingle, inDouble := false, false
	start := -1
	want := false

	emit := func(tok string, d int, term byte) {
		if (!anyDepth && d != 0) || tok == "" {
			return
		}
		if want {
			want = false
			if term == '(' {
				return // a function call: extract(epoch from f()), substring(x from f())
			}
			name := strings.TrimPrefix(tok, "PG_CATALOG.")
			if name != "" {
				refs = append(refs, name)
			}
			return
		}
		if tok == "FROM" || tok == "JOIN" {
			want = true
		}
	}

	for i := 0; i <= len(normalized); i++ {
		ch := byte(' ')
		if i < len(normalized) {
			ch = normalized[i]
		}
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		}
		if ch != ' ' && ch != '(' && ch != ')' && ch != ',' && ch != ';' &&
			ch != '\'' && ch != '"' {
			if start < 0 {
				start, tokDepth = i, depth
			}
			continue
		}
		if start >= 0 {
			emit(normalized[start:i], tokDepth, ch)
			start = -1
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		}
	}
	return refs
}

// selectList returns the projection list of a normalized SELECT that has no
// FROM clause, and reports whether the statement is one.
func selectList(normalized string) (string, bool) {
	if !strings.HasPrefix(normalized, "SELECT ") || strings.Contains(normalized, " FROM ") {
		return "", false
	}
	return strings.TrimSpace(strings.TrimPrefix(normalized, "SELECT ")), true
}

// matchSyntheticSelect answers SELECT expressions that have no FROM clause.
// Matching is against the whole projection list, never a substring: these
// answers are one column wide, so they may only claim a statement that is one
// column wide. `select current_database(), current_schema(), current_user` —
// DataGrip's opening query, answered here with a single column until this
// changed — has three columns and belongs to the engine, which resolves
// current_user, current_schema, current_catalog and friends as ordinary
// niladic functions. Anything this layer does not claim exactly still returns
// a real result, with a RowDescription that matches it.
func (c *pgConn) matchSyntheticSelect(normalized string) *synthAnswer {
	list, ok := selectList(normalized)
	if !ok || strings.Contains(list, ",") {
		return nil
	}

	switch list {
	case "VERSION()":
		return singleRow([]string{"version"}, map[string]any{
			"version": expr.ServerVersion,
		})

	case "CURRENT_SCHEMA", "CURRENT_SCHEMA()":
		return singleRow([]string{"current_schema"}, map[string]any{
			"current_schema": expr.SessionSchema,
		})

	case "CURRENT_DATABASE()":
		return singleRow([]string{"current_database"}, map[string]any{
			"current_database": expr.SessionCatalog,
		})

	case "CURRENT_CATALOG":
		return singleRow([]string{"current_catalog"}, map[string]any{
			"current_catalog": expr.SessionCatalog,
		})

	case "CURRENT_USER", "CURRENT_USER()", "SESSION_USER", "USER", "CURRENT_ROLE":
		// The authenticated identity is known here and not in the engine —
		// the scalar registry is process-global and has no per-connection
		// context — so this is the one answer that beats the engine's
		// constant. Each spelling keeps its own label, the way PostgreSQL
		// labels it and the way the engine labels it for every spelling this
		// layer does not claim.
		user := expr.SessionUser
		if c.identity != nil {
			user = c.identity.Name
		}
		label := strings.ToLower(strings.TrimSuffix(list, "()"))
		return singleRow([]string{label}, map[string]any{label: user})

	case "1":
		// Connection liveness check; PostgreSQL labels the column ?column?.
		return singleRow([]string{"?column?"}, map[string]any{
			"?column?": "1",
		})
	}

	// Any other SELECT without FROM — delegate to the query engine, which
	// handles table-less SELECTs via DualSource (SELECT CURRENT_DATE,
	// SELECT 1+1, SELECT NOW(), multi-column session queries).
	return nil
}

// showDefaults answers SHOW for a variable no SET has touched, under the
// label PostgreSQL uses for it. server_version_num MUST parse as an integer —
// pgJDBC calls Integer.parseInt on it, and answering "15.0" threw (#305
// item 7). The values agree with what expr's session shims report.
var showDefaults = map[string]struct{ label, value string }{
	"transaction_isolation":       {"transaction_isolation", "read committed"},
	"standard_conforming_strings": {"standard_conforming_strings", "on"},
	"server_version":              {"server_version", "15.0"},
	"server_version_num":          {"server_version_num", "150000"},
	"server_encoding":             {"server_encoding", "UTF8"},
	"client_encoding":             {"client_encoding", "UTF8"},
	"datestyle":                   {"DateStyle", "ISO, MDY"},
	"timezone":                    {"TimeZone", "UTC"},
	"intervalstyle":               {"IntervalStyle", "postgres"},
	"integer_datetimes":           {"integer_datetimes", "on"},
	"is_superuser":                {"is_superuser", "off"},
	"max_identifier_length":       {"max_identifier_length", "63"},
	"search_path":                 {"search_path", `"$user", public`},
	"application_name":            {"application_name", ""},
}

// matchShow answers SHOW statements from PostgreSQL clients. A variable the
// session SET earlier answers with the stored value — SET search_path then
// SHOW search_path used to come back "" because SHOW never consulted
// sessionVars (#305 item 7).
func (c *pgConn) matchShow(upper string) *synthAnswer {
	if strings.Contains(upper, "TRANSACTION ISOLATION LEVEL") {
		return singleRow([]string{"transaction_isolation"}, map[string]any{
			"transaction_isolation": "read committed",
		})
	}
	if strings.HasPrefix(upper, "SHOW TABLES") || strings.HasPrefix(upper, "SHOW COLUMNS ") {
		// Route SHOW TABLES and SHOW COLUMNS FROM through the query engine,
		// which parses them as QueryShowTables / QueryDescribe respectively.
		return nil
	}
	name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(upper, "SHOW ")))
	if v, ok := c.sessionVars[name]; ok {
		label := name
		if d, ok := showDefaults[name]; ok {
			label = d.label
		}
		return singleRow([]string{label}, map[string]any{label: v})
	}
	if d, ok := showDefaults[name]; ok {
		return singleRow([]string{d.label}, map[string]any{d.label: d.value})
	}
	// Unknown variable — empty value under its own label. PostgreSQL raises
	// 42704 here; clients probe with SHOW enough that the empty answer is
	// kept deliberately (it predates #305 and nothing depends on the error).
	return singleRow([]string{name}, map[string]any{name: ""})
}

// sendSynthAnswer writes a complete simple-protocol result for ans:
// RowDescription, DataRows, CommandComplete. Simple-protocol results are
// always in text format.
func (c *pgConn) sendSynthAnswer(ans *synthAnswer) {
	c.sendSynthRowDescription(ans, nil)
	c.sendSynthRows(ans, nil)
}

// sendSynthRowDescription writes the RowDescription for a synthetic answer with
// each column's OID inferred from its values (see colOID). fmtCodes: see
// sendRowDescription — a portal Describe declares the Bind's format codes.
func (c *pgConn) sendSynthRowDescription(ans *synthAnswer, fmtCodes []int16) {
	c.buf = c.buf[:0]
	c.buf = appendInt16(c.buf, int16(len(ans.cols)))
	for i, col := range ans.cols {
		c.buf = append(c.buf, col...)
		c.buf = append(c.buf, 0)
		c.buf = appendInt32(c.buf, 0)
		c.buf = appendInt16(c.buf, 0)
		oid := ans.colOID(col)
		c.buf = appendInt32(c.buf, oid)
		c.buf = appendInt16(c.buf, pgTypeSize(int(oid)))
		c.buf = appendInt32(c.buf, -1)
		c.buf = appendInt16(c.buf, fmtCodeAt(fmtCodes, i))
	}
	c.sendMsg('T', c.buf)
}

// sendSynthRows writes the DataRows and CommandComplete for ans, without a
// RowDescription. The extended protocol takes the description from Describe
// alone; Execute contributes tuples.
func (c *pgConn) sendSynthRows(ans *synthAnswer, fmtCodes []int16) {
	for _, row := range ans.rows {
		// A synthetic catalog answer's column list is written out by hand
		// and carries no duplicates, so boxing its map row by name is exact.
		cells := cellsByName(ans.cols, row)
		if len(fmtCodes) > 0 {
			// Synthetic catalog answers carry no typed metas, so there is
			// no timestamp column to convert, and no nested-type schema
			// either — every catalog-emulation value is a scalar.
			c.sendDataRowFormatted(ans.cols, cells, fmtCodes, nil, nil)
		} else {
			c.sendDataRow(ans.cols, cells, nil, nil)
		}
	}
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", len(ans.rows)))
}

// tableOID returns a deterministic fake OID for a catalog object's name.
//
// The result stays inside PostgreSQL's OID range, above the 16384 the system
// objects end at. The previous hash accumulated without a bound and returned
// values in the trillions — nothing a client can hold in an oid or int4
// column, which is where a driver puts a catalog OID it reads.
func tableOID(name string) int {
	const base = 16384
	h := uint32(2166136261) // FNV-1a
	for _, c := range name {
		h = (h ^ uint32(c)) * 16777619
	}
	return base + int(h%(1<<31-base))
}

// pgTypeOID maps Wadjet types to PostgreSQL type OIDs.
// pgColumnOID is pgTypeOID with the whole column meta in hand, for the one
// type whose OID depends on more than its name: a STRING that carries a
// declared LENGTH is PostgreSQL's `character varying(n)` (1043) rather than
// unconstrained `text` (25), and the length rides in the type modifier beside
// it (#838).
//
// `character(n)` (1042) is deliberately NOT sent for `CAST(x AS CHAR(n))`.
// PostgreSQL's bpchar PADS a short value to n and then strips the blanks again
// for length(), for `||` and for every comparison; this engine has one
// TypeString and none of that, so declaring 1042 would name a type whose three
// defining behaviours it does not implement. `character varying(n)` states
// exactly what the value IS — at most n characters, compared by bytes — and
// the padding residual is recorded in ADR-0012 item 5.
func pgColumnOID(m wadjet.ColumnMeta) int {
	if m.TypeID == parquet.TypeString && m.StringLength > 0 {
		return oidVarchar
	}
	return pgTypeOID(m.TypeName)
}

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
	case "DECIMAL", "NUMERIC":
		// PostgreSQL's numeric. The values on the wire are exact — a DECIMAL
		// is boxed as its rendered text (#434 made that rendering carry all
		// 128 bits) and the binary form below encodes the same digits — so
		// the only thing OID 25 was buying was a client that reads an exact
		// decimal column as a String. pgFormatType already answered "numeric"
		// for the same type, so the catalog was contradicting the wire.
		return 1700 // numeric
	case "BYTES":
		// PostgreSQL's bytea. A BYTES value has a printed form there — `\x`
		// followed by lowercase hex, under the default bytea_output = hex —
		// and OID 17 is what tells a client to read the cell that way. Under
		// OID 25 the same bytes claimed to be TEXT, which a length-aware
		// client (pgx, JDBC) took at its word and a strlen-based one (libpq's
		// PQgetvalue) truncated at the first NUL: one query, two answers
		// (#570). oidBytea in bindparams.go is the same number, used for
		// inbound Bind parameters and the pg_type catalog row.
		return 17 // bytea
	case "PORT", "PROTOCOL":
		// PostgreSQL's int4. A PORT is a uint16 and a PROTOCOL a uint8, both
		// stored in Int32Data and boxed as an int32, and both already RENDER
		// as a plain integer on the wire ("443", "6") — so the text a client
		// reads does not change at all. What changes is that it is now told
		// so: under OID 25 the engine declared `text` for a column it
		// compares NUMERICALLY, which is ADR-0012 item 2's exact shape — one
		// type declared, another behaved as — and `port > 5` is legal
		// PostgreSQL under int4 while it is 42883 under text (#834).
		//
		// appendBinaryValue's int32 arm already writes the 4 bytes OID 23
		// promises, so no encoder arm is needed the way date/numeric/uuid
		// needed one: those box as TEXT and this boxes as the number.
		return 23 // int4
	case "DURATION":
		// PostgreSQL's int8, counting NANOSECONDS — the unit schema.go
		// defines and Vector.GetValue reads back. Deliberately NOT `interval`
		// (OID 1186): PostgreSQL's interval is microsecond-precision and has
		// its own text and binary forms, so declaring it would change the
		// rendering as well as the type. ADR-0012 records that as the open
		// alternative.
		return 20 // int8
	case "UUID":
		// PostgreSQL's uuid. The engine boxes a UUID as its canonical text —
		// the same 36 characters OID 2950's TEXT format carries — so the text
		// bytes on the wire do not change; what changes is that a driver can
		// now recognise the column (pgx's UUID scanner, pgJDBC's
		// java.util.UUID) instead of being handed a String under OID 25
		// (#839). appendBinaryUUID below writes the 16-byte binary form,
		// because under 2950 the raw text would be a lie the way it was for
		// numeric and date.
		return 2950 // uuid
	case "VECTOR":
		return 25 // text (pgvector uses custom OID, but text works for display)
	default:
		return 25 // text
	}
}

// matchCatalogQuery returns real catalog data for specific pg_catalog queries
// that SQLAlchemy/Superset uses for schema introspection. sql is the statement
// being answered; normalized is its uppercased, whitespace-collapsed form.
func (c *pgConn) matchCatalogQuery(ctx context.Context, sql, normalized string) *synthAnswer {
	// Which catalog relation this statement is about. A branch may only claim
	// a statement whose subject it models — see catalogSubject.
	subject := catalogSubject(normalized)

	// pg_database — the database picker's source.
	if subject == "PG_DATABASE" {
		if ans := c.matchPgDatabase(sql); ans != nil {
			return ans
		}
	}

	// pg_user / pg_shadow / pg_roles: the identity on this connection. Clients
	// ask right after startup (DataGrip: select usesuper from pg_user where
	// usename = current_user) to size their feature set.
	if subject == "PG_USER" || subject == "PG_SHADOW" || subject == "PG_ROLES" {
		user := expr.SessionUser
		if c.identity != nil {
			user = c.identity.Name
		}
		attrs := pgUserAttrs(user)
		if subject == "PG_ROLES" {
			// pg_roles names the same principal under rol* columns.
			attrs["rolname"] = user
			attrs["rolsuper"] = false
			attrs["rolcreatedb"] = false
			attrs["rolcanlogin"] = true
			attrs["rolreplication"] = false
			attrs["rolbypassrls"] = false
			attrs["oid"] = 10
		}
		if ans := catalogRowAnswer(sql, attrs, []string{"usename", "usesysid", "usesuper"}); ans != nil {
			return ans
		}
	}

	// pg_type — JDBC drivers query this to map OIDs to type names
	if subject == "PG_TYPE" {
		return c.matchPgType(sql, normalized)
	}

	// pg_tables — the user-facing table listing (SELECT * FROM pg_tables).
	// An empty answer while tables exist is a wrong answer (#305 item 6).
	if subject == "PG_TABLES" {
		tables, err := c.db.ListTables(ctx)
		if err != nil {
			return nil
		}
		user := expr.SessionUser
		if c.identity != nil {
			user = c.identity.Name
		}
		if schema := extractParamValue(normalized, "SCHEMANAME"); schema != "" && schema != expr.SessionSchema {
			tables = nil // every table here lives in the one schema
		}
		want := extractParamValue(normalized, "TABLENAME")
		rows := make([]map[string]any, 0, len(tables))
		for _, t := range tables {
			if want == "" || want == t {
				rows = append(rows, pgTablesAttrs(t, user))
			}
		}
		return catalogRowsAnswer(sql, pgTablesAttrs("", user), rows,
			[]string{"schemaname", "tablename", "tableowner", "tablespace",
				"hasindexes", "hasrules", "hastriggers", "rowsecurity"})
	}

	// information_schema — branch on the relation the statement reads, not on
	// a substring of its text: `... FROM information_schema.columns WHERE
	// table_name = 'alerts'` contains the word ALERTS but asks about columns,
	// and used to be answered with the alert listing (#305 item 8).
	infoSchema := map[string]bool{}
	for _, r := range relationRefsAll(normalized) {
		if strings.HasPrefix(r, "INFORMATION_SCHEMA.") {
			infoSchema[strings.TrimPrefix(r, "INFORMATION_SCHEMA.")] = true
		}
	}
	switch {
	case infoSchema["ALERTS"]:
		return c.matchInfoSchemaAlerts(ctx)
	case infoSchema["COLUMNS"]:
		return c.matchInfoSchemaColumns(ctx, sql, normalized)
	case infoSchema["TABLES"]:
		return c.matchInfoSchemaTables(ctx, sql)
	}

	// Schema listing: SELECT nspname FROM pg_namespace (used by get_schema_names
	// and by the schema picker, which asks for oid alongside nspname).
	// Exclude queries that mention pg_type/pg_class/pg_attribute — those just JOIN pg_namespace.
	if subject == "PG_NAMESPACE" && strings.Contains(normalized, "NSPNAME") {
		if ans := catalogRowAnswer(sql, pgNamespaceAttrs(), []string{"oid", "nspname"}); ans != nil {
			return ans
		}
		return singleRow([]string{"nspname"}, map[string]any{"nspname": expr.SessionSchema})
	}

	// Index/constraint queries that happen to JOIN pg_attribute — return empty.
	// The columns come from the statement being answered, not from portalSQL:
	// at a statement Describe the portal still holds the PREVIOUS query, which
	// described one shape and then executed another.
	if strings.Contains(normalized, "PG_INDEX") ||
		strings.Contains(normalized, "PG_CONSTRAINT") {
		cols := extractSelectColumns(sql)
		if len(cols) == 0 {
			cols = []string{"?column?"}
		}
		return &synthAnswer{cols: cols}
	}

	// Column info: pg_attribute query
	if subject == "PG_ATTRIBUTE" {
		return c.matchAttributeQuery(ctx, sql, normalized)
	}

	// pg_class queries: table listing, OID lookup, or reverse OID lookup.
	//
	// All three shapes are the same answer over a different row set, so they
	// share one builder: the statement's SELECT list decides the columns, the
	// WHERE decides which tables are in it. The branches used to hardcode a
	// single column each — a client selecting `relname, relkind` was described
	// one column and sent one, which is how DataGrip's table tree came back
	// without the kind it uses to tell a table from a view.
	if subject == "PG_CLASS" && strings.Contains(normalized, "RELNAME") {
		tables, err := c.db.ListTables(ctx)
		if err != nil {
			return nil
		}

		keep := func(string) bool { return true }
		switch {
		case extractParamValue(normalized, "RELNAME") != "":
			// Specific table lookup: relname = '<value>' in WHERE.
			want := extractParamValue(normalized, "RELNAME")
			keep = func(t string) bool { return t == want }
		case extractParamValue(normalized, "OID") != "":
			// Reverse lookup: WHERE oid = '<value>'.
			want := extractParamValue(normalized, "OID")
			keep = func(t string) bool { return strconv.Itoa(tableOID(t)) == want }
		case !strings.Contains(normalized, "RELKIND"):
			// Unknown pg_class query — don't handle, fall through to blanket.
			return nil
		}

		rows := make([]map[string]any, 0, len(tables))
		for _, t := range tables {
			if keep(t) {
				rows = append(rows, pgClassAttrs(t))
			}
		}
		return catalogRowsAnswer(sql, pgClassAttrs(""), rows,
			[]string{"oid", "relname", "relnamespace", "relkind"})
	}

	return nil
}

// matchAttributeQuery returns column metadata from our catalog for pg_attribute queries.
func (c *pgConn) matchAttributeQuery(ctx context.Context, sql, normalized string) *synthAnswer {
	tables, err := c.db.ListTables(ctx)
	if err != nil || len(tables) == 0 {
		return nil
	}

	// Try to find the target from the query text.
	// SQLAlchemy may send a table name or a numeric OID as the attrelid
	// parameter; clients that JOIN pg_class instead name the table in
	// relname (DataGrip's column query), which used to match nothing here —
	// so a request for one table's columns was answered with every table's
	// columns, and every table in the tree got every column in the database.
	target := extractParamValue(normalized, "ATTRELID")
	if target == "" {
		target = extractParamValue(normalized, "RELNAME")
	}

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
	aliases := cteAliases(sql)

	for _, tableName := range targetTables {
		table, err := c.db.Query(ctx, fmt.Sprintf("DESCRIBE %s", tableName))
		if err != nil {
			continue
		}

		oid := tableOID(tableName)
		attnum := 0
		for _, row := range table.Rows {
			colName, _ := row["column_name"].(string)
			colType, _ := row["type"].(string)
			nullable, _ := row["nullable"].(string)
			if colName == "" || colName == "Partition Keys" {
				continue
			}
			attnum++

			row := map[string]any{
				// SQLAlchemy's positional labels.
				"attname":     colName,
				"format_type": pgFormatType(colType),
				"default":     nil,
				"attnotnull":  nullable == "NO",
				"table_oid":   oid,
				"comment":     nil,
				"generated":   "",
				"identity":    "",
				// pg_attribute's own columns, for clients that select them
				// by name rather than unpacking a fixed tuple.
				"attrelid":      oid,
				"atttypid":      pgTypeOID(colType),
				"atttypmod":     -1,
				"attnum":        attnum,
				"attisdropped":  false,
				"atthasdef":     false,
				"attidentity":   "",
				"attgenerated":  "",
				"attndims":      0,
				"attcollation":  0,
				"attstattarget": -1,
				"attislocal":    true,
			}
			// The statement joins pg_class to pg_attribute, so one row of the
			// answer spans both relations: carry the table's own attributes
			// alongside the column's, then resolve whatever the statement's
			// CTE renamed them to (T.oid as table_id, T.relkind as kind, ...).
			for k, v := range pgClassAttrs(tableName) {
				if _, taken := row[k]; !taken {
					row[k] = v
				}
			}
			for alias, under := range aliases {
				if v, ok := row[under]; ok {
					if _, taken := row[alias]; !taken {
						row[alias] = v
					}
				}
			}
			resultRows = append(resultRows, row)
		}
	}

	// A client that asked for plain pg_attribute columns is answered in its
	// own shape. SQLAlchemy's query is a positional unpack of format_type()
	// calls and CASE expressions, which map to no attribute name — it keeps
	// the fixed 8-column tuple it expects.
	if shaped := shapedAttributeAnswer(sql, resultRows); shaped != nil {
		return shaped
	}
	return &synthAnswer{cols: cols, rows: resultRows}
}

// shapedAttributeAnswer reshapes a pg_attribute answer to the statement's own
// SELECT list, but only when every item in that list names an attribute this
// layer models. A partial match would hand back NULLs where the client asked
// for a computed value, which is worse than the fixed tuple.
func shapedAttributeAnswer(sql string, rows []map[string]any) *synthAnswer {
	items := selectItems(sql)
	if len(items) == 0 {
		return nil
	}
	for _, it := range items {
		if it.expr == "*" {
			return nil // let the fixed tuple answer SELECT *
		}
	}

	// Resolvability is judged against a real row, which carries the column's
	// attributes, its table's pg_class attributes (the statement joins them),
	// and whatever the statement's CTE renamed those to.
	probe := attributeShape()
	if len(rows) > 0 {
		probe = rows[0]
	}
	resolvable := 0
	for _, it := range items {
		if _, ok := probe[it.expr]; ok {
			resolvable++
			continue
		}
		if _, ok := evalCatalogExpr(it.raw, probe); ok {
			resolvable++
		}
	}
	if resolvable == 0 {
		return nil
	}
	return catalogRowsAnswer(sql, probe, rows, nil)
}

// attributeShape names the pg_attribute columns a statement may select by
// name. Values are irrelevant — only membership is read.
func attributeShape() map[string]any {
	return map[string]any{
		"attname": nil, "format_type": nil, "default": nil, "attnotnull": nil,
		"table_oid": nil, "comment": nil, "generated": nil, "identity": nil,
		"attrelid": nil, "atttypid": nil, "atttypmod": nil, "attnum": nil,
		"attisdropped": nil, "atthasdef": nil, "attidentity": nil,
		"attgenerated": nil, "attndims": nil, "attcollation": nil,
		"attstattarget": nil,
	}
}

// matchPgType returns rows from pg_type for JDBC/ODBC type mapping, shaped by
// the client's own SELECT list — pgJDBC's TypeInfoCache reads columns by
// position from the list it wrote, not from a fixed vocabulary (#305 item 2).
// A SELECT list naming nothing this relation has falls back to the old
// five-column answer rather than answering with a different shape.
func (c *pgConn) matchPgType(sql, normalized string) *synthAnswer {
	type pgType struct {
		oid     int
		typname string
		typlen  int
	}

	types := []pgType{
		{16, "bool", 1},
		{17, "bytea", -1},
		{20, "int8", 8},
		{21, "int2", 2},
		{23, "int4", 4},
		{25, "text", -1},
		{26, "oid", 4},
		{700, "float4", 4},
		{701, "float8", 8},
		{1042, "bpchar", -1},
		{1043, "varchar", -1},
		{1082, "date", 4},
		{1114, "timestamp", 8},
		{1184, "timestamptz", 8},
		{1700, "numeric", -1},
	}

	attrsFor := func(t pgType) map[string]any {
		return map[string]any{
			"oid":          t.oid,
			"typname":      t.typname,
			"typlen":       t.typlen,
			"typtype":      "b",
			"typnamespace": 11,
			"typelem":      0,
			"typarray":     0,
			"typdelim":     ",",
			"typrelid":     0,
			"typbasetype":  0,
			"typtypmod":    -1,
			"typnotnull":   false,
			"typcollation": 0,
			"typndims":     0,
			"typisdefined": true,
			// pg_namespace joined alongside: every one of these lives in pg_catalog.
			"nspname": "pg_catalog",
		}
	}

	// Narrowing predicates a driver sends: WHERE oid = 23 / typname = 'int4'.
	specificOID := extractParamValue(normalized, "OID")
	specificName := extractParamValue(normalized, "TYPNAME")

	var rows []map[string]any
	for _, t := range types {
		if specificOID != "" && strconv.Itoa(t.oid) != specificOID {
			continue
		}
		if specificName != "" && t.typname != specificName {
			continue
		}
		rows = append(rows, attrsFor(t))
	}

	fallbackCols := []string{"oid", "typname", "typlen", "typtype", "typnamespace"}
	if ans := catalogRowsAnswer(sql, attrsFor(pgType{}), rows, fallbackCols); ans != nil {
		return ans
	}
	ans := &synthAnswer{cols: fallbackCols}
	for _, r := range rows {
		ans.rows = append(ans.rows, map[string]any{
			"oid": r["oid"], "typname": r["typname"], "typlen": r["typlen"],
			"typtype": r["typtype"], "typnamespace": r["typnamespace"],
		})
	}
	return ans
}

// matchInfoSchemaTables returns information_schema.tables data, shaped by the
// client's own SELECT list (getTables() asks for its columns by name and
// label; a fixed vocabulary answered a different shape — #305 item 2).
func (c *pgConn) matchInfoSchemaTables(ctx context.Context, sql string) *synthAnswer {
	tables, err := c.db.ListTables(ctx)
	if err != nil {
		return nil
	}

	fallbackCols := []string{"table_catalog", "table_schema", "table_name", "table_type"}
	rows := make([]map[string]any, 0, len(tables))
	for _, t := range tables {
		rows = append(rows, map[string]any{
			"table_catalog": "wadjet",
			"table_schema":  "public",
			"table_name":    t,
			"table_type":    "BASE TABLE",
		})
	}
	shape := map[string]any{
		"table_catalog": nil, "table_schema": nil, "table_name": nil, "table_type": nil,
	}
	if ans := catalogRowsAnswer(sql, shape, rows, fallbackCols); ans != nil {
		return ans
	}
	return &synthAnswer{cols: fallbackCols, rows: rows}
}

// matchInfoSchemaColumns returns information_schema.columns data, shaped by
// the client's own SELECT list (#305 item 2), scoped to the table its WHERE
// names.
func (c *pgConn) matchInfoSchemaColumns(ctx context.Context, sql, normalized string) *synthAnswer {
	tables, err := c.db.ListTables(ctx)
	if err != nil {
		return nil
	}

	// Filter to specific table if referenced
	targetTable := extractParamValue(normalized, "TABLE_NAME")

	fallbackCols := []string{"table_catalog", "table_schema", "table_name",
		"column_name", "ordinal_position", "data_type", "is_nullable"}
	var rows []map[string]any
	for _, tableName := range tables {
		if targetTable != "" && tableName != targetTable {
			continue
		}
		table, err := c.db.Query(ctx, fmt.Sprintf("DESCRIBE %s", tableName))
		if err != nil {
			continue
		}
		pos := 0
		for _, row := range table.Rows {
			colName, _ := row["column_name"].(string)
			colType, _ := row["type"].(string)
			nullable, _ := row["nullable"].(string)
			if colName == "" || colName == "Partition Keys" {
				continue
			}
			pos++
			isNullable := "YES"
			if nullable == "NO" {
				isNullable = "NO"
			}
			rows = append(rows, map[string]any{
				"table_catalog":    "wadjet",
				"table_schema":     "public",
				"table_name":       tableName,
				"column_name":      colName,
				"ordinal_position": pos,
				"data_type":        pgFormatType(colType),
				"is_nullable":      isNullable,
				"column_default":   nil,
				"udt_name":         nil,
			})
		}
	}
	shape := map[string]any{
		"table_catalog": nil, "table_schema": nil, "table_name": nil,
		"column_name": nil, "ordinal_position": nil, "data_type": nil,
		"is_nullable": nil, "column_default": nil, "udt_name": nil,
	}
	if ans := catalogRowsAnswer(sql, shape, rows, fallbackCols); ans != nil {
		return ans
	}
	ans := &synthAnswer{cols: fallbackCols}
	for _, r := range rows {
		row := make(map[string]any, len(fallbackCols))
		for _, col := range fallbackCols {
			row[col] = r[col]
		}
		ans.rows = append(ans.rows, row)
	}
	return ans
}

// matchInfoSchemaAlerts returns information_schema.alerts data.
func (c *pgConn) matchInfoSchemaAlerts(ctx context.Context) *synthAnswer {
	alerts, err := c.db.Catalog().ListAlerts(ctx)
	if err != nil {
		return nil
	}

	ans := &synthAnswer{cols: []string{"name", "interval_seconds", "enabled", "webhook_url",
		"insert_into_table", "last_evaluated_at"}}
	for _, a := range alerts {
		lastEval := ""
		if !a.LastEvaluatedAt.IsZero() {
			lastEval = a.LastEvaluatedAt.UTC().Format(time.RFC3339)
		}
		ans.rows = append(ans.rows, map[string]any{
			"name":              a.Name,
			"interval_seconds":  fmt.Sprintf("%d", a.IntervalSeconds),
			"enabled":           boolStr(a.Enabled),
			"webhook_url":       a.WebhookURL,
			"insert_into_table": a.InsertIntoTable,
			"last_evaluated_at": lastEval,
		})
	}
	return ans
}

// pgFormatType maps Wadjet type names to PostgreSQL format_type() output.
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
	case "BYTES":
		return "bytea"
	case "UUID":
		return "uuid"
	case "PORT", "PROTOCOL":
		// The catalog must agree with the wire: pgTypeOID declares OID 23 for
		// these, and an introspecting client (DataGrip, Superset, SQLAlchemy)
		// reads BOTH — a pg_attribute row saying `text` beside a
		// RowDescription saying int4 is the contradiction #834 is about.
		return "integer"
	case "DURATION":
		return "bigint"
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
	// Unquoted numeric literal: pgJDBC inlines OIDs bare (attrelid = 16384),
	// which used to be invisible here — so a request for one table's columns
	// was answered with every table's (#305 item 4). Only digits are read: a
	// bare identifier after = is a join condition, not a literal.
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end > 0 && (end == len(rest) || rest[end] == ' ' || rest[end] == ')' || rest[end] == ':' || rest[end] == ';') {
		return rest[:end]
	}
	return ""
}

// extractSelectColumns returns the column labels a SELECT promises, so an
// empty intercepted answer still carries headers a client can read (psycopg2
// crashes on empty tuples; DataGrip reads results by label).
//
// It delegates to selectItems, which scans with paren depth and quote state.
// The hand-rolled split this replaced looked for " FROM " literally, so a
// statement with FROM at the start of a line — every introspection query a BI
// tool formats across lines — never found it and turned the entire remainder
// of the statement into one column name:
//
//	select L.transactionid::varchar::bigint as transaction_id
//	from pg_catalog.pg_locks L ...
//	→ column named "transaction_id\nfrom pg_catalog.pg_locks L\nwhere ..."
func extractSelectColumns(sql string) []string {
	items := selectItems(sql)
	if len(items) == 0 {
		return nil
	}
	cols := make([]string, 0, len(items))
	for _, it := range items {
		cols = append(cols, it.label)
	}
	return cols
}

// isDMLSQL reports whether an UPPERCASED statement is a DML verb.
//
// One predicate, because the simple and the extended protocol must agree
// about which statements are writes. They did not: the simple path had this
// test inline and the extended path had none at all, so every DML statement
// over the extended protocol — the protocol pgx, JDBC, psycopg and every ORM
// use — fell through to the QUERY path and reported `SELECT 1` (#816).
//
// It reads the leading KEYWORD TOKEN, not a text prefix. The prefix version
// tested `HasPrefix(upper, "INSERT ")` with a literal space, so multi-line SQL
// and a leading `/* hint */` — what ORMs and APM layers actually emit — missed
// the branch, and missing it meant Describe EXECUTED the write and Execute ran
// it again (review B3).
func isDMLSQL(sql string) bool {
	switch plansql.LeadingKeyword(sql) {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return true
	}
	return false
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

// fmtCodeAt resolves the result format code for output column i under the
// Bind message's format-code list: an empty list means every column is text,
// a single code applies to all columns, otherwise the list is per-column.
// This is the same resolution sendDataRowFormatted applies to the bytes, so
// the RowDescription's declaration and the DataRow's encoding cannot drift
// apart (#362).
func fmtCodeAt(fmtCodes []int16, i int) int16 {
	switch {
	case len(fmtCodes) == 1:
		return fmtCodes[0]
	case i < len(fmtCodes):
		return fmtCodes[i]
	}
	return 0
}

// sendRowDescription emits an untyped (all-text-OID) RowDescription.
//
// fmtCodes are the result format codes the portal's Bind requested — per the
// protocol a Describe of a PORTAL carries them, while a Describe of a
// STATEMENT (and the simple protocol) passes nil and declares text. Declaring
// 0 for a portal whose DataRows were binary made pgx hand four big-endian
// int4 bytes to its text parser (#362).
func (c *pgConn) sendRowDescription(columns []string, fmtCodes []int16) {
	c.buf = c.buf[:0]

	// Field count (int16)
	c.buf = appendInt16(c.buf, int16(len(columns)))

	for i, col := range columns {
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
		// Format code (int16): what the Bind chose for this column
		c.buf = appendInt16(c.buf, fmtCodeAt(fmtCodes, i))
	}

	c.sendMsg('T', c.buf)
}

// sendTypedRowDescription emits a RowDescription ('T') message with correct
// PostgreSQL type OIDs derived from ColumnMeta. This is critical for JDBC/ODBC
// drivers that use OIDs to determine Java/C types for result columns.
// fmtCodes: see sendRowDescription.
func (c *pgConn) sendTypedRowDescription(metas []wadjet.ColumnMeta, fmtCodes []int16) {
	c.buf = c.buf[:0]
	c.buf = appendInt16(c.buf, int16(len(metas)))

	for i, m := range metas {
		// Field name (null-terminated)
		c.buf = append(c.buf, m.Name...)
		c.buf = append(c.buf, 0)
		// Table OID (int32) = 0
		c.buf = appendInt32(c.buf, 0)
		// Column attr number (int16) = 0
		c.buf = appendInt16(c.buf, 0)
		// Data type OID
		oid := pgColumnOID(m)
		c.buf = appendInt32(c.buf, int32(oid))
		// Data type size
		c.buf = appendInt16(c.buf, pgTypeSize(oid))
		// Type modifier: the DECLARATION a bare OID cannot carry
		c.buf = appendInt32(c.buf, TypeMod(m))
		// Format code (int16): what the Bind chose for this column
		c.buf = appendInt16(c.buf, fmtCodeAt(fmtCodes, i))
	}

	c.sendMsg('T', c.buf)
}

// TypeMod returns the PostgreSQL type modifier (atttypmod) for a result
// column, or -1 for a type that has none. It is exported so the differential
// oracle asserts the typmod a client is HANDED rather than a copy of this
// rule (benchmarks/tpch, ADR-0024 item 5).
//
// The modifier is where PostgreSQL keeps the part of a declaration the OID
// does not carry: numeric's (precision, scale), varchar/bpchar's length,
// time/timestamp/interval's second precision. -1 means "unconstrained", which
// is protocol-legal and is what every unparameterised type sends — so writing
// the constant -1 for everything was less information rather than wrong
// information, and it went unnoticed until DECIMAL started declaring OID 1700
// (#454). What a client loses is ResultSetMetaData.getPrecision()/getScale():
// a column declared DECIMAL(9,2) reports 0 or "unlimited", and a tool that
// sizes a display column or round-trips DDL from a result set gets it wrong.
//
// numeric packs the pair as ((precision << 16) | scale) + VARHDRSZ, exactly as
// PostgreSQL's numerictypmodin does (utils/adt/numeric.c). Precision 0 means
// the declaration did not reach us — a plan-declared schema for a zero-row
// result, an inferred type — and an unconstrained numeric is the honest
// answer there, not a fabricated (0,0).
//
// The switch is keyed on the wadjet TypeID rather than the OID so that a type
// which later gains a parameter (a VARCHAR(n), a TIME(n)) is added here and
// not in the wire writer.
func TypeMod(m wadjet.ColumnMeta) int32 {
	switch m.TypeID {
	case parquet.TypeDecimal:
		// m.WireUnconstrained: an aggregate function's DECIMAL result (MIN/
		// MAX/MIN_BY/MAX_BY/SUM/AVG). live PostgreSQL's \gdesc keeps a
		// numeric(p,s)'s typmod only for a BARE column reference — every
		// aggregate call forgets it, even though m.Precision/m.Scale here
		// still carry the real declaration for a caller that wants it
		// (FIX 2, #457/#458 fold-in).
		if m.Precision <= 0 || m.WireUnconstrained {
			return -1
		}
		return int32((m.Precision<<16)|(m.Scale&0xFFFF)) + pgVarHdrSz
	case parquet.TypeString:
		// A parameterized string destination's LENGTH, which PostgreSQL sends
		// as n + VARHDRSZ under `character varying(n)`. 0 means the engine's
		// unconstrained TypeString, which every unparameterized spelling and
		// every stored column still is (#838).
		if m.StringLength <= 0 {
			return -1
		}
		return int32(m.StringLength) + pgVarHdrSz
	default:
		return -1
	}
}

// pgVarHdrSz is PostgreSQL's VARHDRSZ, the 4 bytes every length-carrying
// typmod is offset by so that -1 can mean "no modifier".
const pgVarHdrSz = 4

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
	case 2950: // uuid
		return 16
	default:
		return -1 // variable length
	}
}

// timestampColumns returns a mask over columns marking those the
// RowDescription declared as TIMESTAMP (OID 1114).
//
// The engine boxes a timestamp as epoch milliseconds — the right thing for
// every compute path that shares that boxing — but a client reads the value
// according to the OID we already told it, so the send path has to convert.
// Without this the wire carried "826727136000" under a declared `timestamp`,
// which psql prints back verbatim and a typed client (pgJDBC, DataGrip,
// SQLAlchemy) fails to parse (#321).
//
// Returns nil when no column is a timestamp, so the common query pays one
// nil check and nothing else. Metas normally arrive in column order; the
// name lookup is the fallback for callers that reorder or rename.
// The same reasoning covers DATE, which the engine boxes as a rendered
// string: under a binary format code those text bytes were written beneath
// the declared OID 1082, whose value is a 4-byte day count, so the client
// decoded whatever the string happened to contain.
//
// Returns nil when no column needs conversion, so the common query pays one
// nil check and nothing else. Metas normally arrive in column order; the
// name lookup is the fallback for callers that reorder or rename.
func sendColumnTypes(columns []string, metas []wadjet.ColumnMeta) []parquet.TypeID {
	if len(metas) == 0 {
		return nil
	}
	var types []parquet.TypeID
	for i, col := range columns {
		var m wadjet.ColumnMeta
		switch {
		case i < len(metas) && metas[i].Name == col:
			m = metas[i]
		default:
			found := false
			for _, cand := range metas {
				if cand.Name == col {
					m, found = cand, true
					break
				}
			}
			if !found {
				continue
			}
		}
		// The list is exactly the types whose BINARY form differs from the
		// text the engine boxes them as. UUID joined it with #839: declaring
		// OID 2950 makes the 36-character text the wrong bytes under a binary
		// format code, the same way declaring 1082 did for a date. The
		// WireProtocol oracle's binary_decode property is what caught the
		// omission when the OID moved and this list did not.
		switch m.TypeID {
		case parquet.TypeTimestamp, parquet.TypeDate, parquet.TypeDecimal, parquet.TypeUUID:
		default:
			continue
		}
		if types == nil {
			types = make([]parquet.TypeID, len(columns))
			for j := range types {
				types[j] = colTypeNone
			}
		}
		types[i] = m.TypeID
	}
	return types
}

// colTypeNone marks a column the send path does not convert. parquet.TypeID
// has no "unknown" member and its zero value is TypeBool, so the absence has
// to be spelled out rather than left implicit.
const colTypeNone = parquet.TypeID(-1)

// columnTypeAt reports the resolved type of output column i, or colTypeNone
// when the caller supplied no metas or the column is not one the send path
// converts.
func columnTypeAt(types []parquet.TypeID, i int) parquet.TypeID {
	if i < len(types) {
		return types[i]
	}
	return colTypeNone
}

// pgEpochOffsetMicros is the gap between the Unix epoch and PostgreSQL's
// timestamp epoch (2000-01-01T00:00:00Z), in microseconds. Binary-format
// `timestamp` values are microseconds relative to the latter.
const pgEpochOffsetMicros = 946684800 * 1_000_000

// cellAt returns the value of output column i, or nil when the row is short.
// A short row is a NULL column, which is what a missing map key meant before
// the send path became positional.
func cellAt(cells []any, i int) any {
	if i < 0 || i >= len(cells) {
		return nil
	}
	return cells[i]
}

// cellsByName boxes a name-keyed row positionally. It is exact only where the
// column names are UNIQUE, which is true of every synthetic catalog answer
// (their column lists are written out by hand) and is why those callers may
// still hold rows as maps.
func cellsByName(columns []string, row map[string]any) []any {
	cells := make([]any, len(columns))
	for i, col := range columns {
		cells[i] = row[col]
	}
	return cells
}

// sendDataRow writes one DataRow. cells are the row's values POSITIONALLY,
// aligned with columns; columns is needed only to name each field for the
// nested-type lookup.
//
// Positional, not keyed by name, because a result may legally carry two
// columns of the same NAME — PostgreSQL answers `SELECT abs(a), abs(b)` with
// two columns called `abs` — and reading a map by name then sent column 0's
// value under column 1's name. A wrong VALUE is strictly worse than a wrong
// name, and the transport is where it has to be prevented: the name is a
// label, the cell is the answer (#513 follow-up).
func (c *pgConn) sendDataRow(columns []string, cells []any, colTypes []parquet.TypeID, nestedSchema *nestedFieldSchema) {
	c.buf = c.buf[:0]

	// Column count (int16)
	c.buf = appendInt16(c.buf, int16(len(columns)))

	for i, col := range columns {
		val := cellAt(cells, i)
		if val == nil {
			// NULL: length = -1
			c.buf = appendInt32(c.buf, -1)
			continue
		}
		s := formatPgValueTyped(val, nestedColumnFor(nestedSchema, col, i))
		if columnTypeAt(colTypes, i) == parquet.TypeTimestamp {
			if ms, ok := val.(int64); ok {
				s = batch.FormatTimestamp(ms)
			}
		}
		c.buf = appendInt32(c.buf, int32(len(s)))
		c.buf = append(c.buf, s...)
	}

	c.sendMsg('D', c.buf)
}

// nestedColumnFor looks up col's declared ROW/ARRAY/MAP structure in
// nestedSchema (sendResultRows resolves it once per result — see
// queryViaCoord's exact answer and nestedColumnSchemas' catalog-lookup
// best-effort one in paraminfer.go), or nil when there is none: unresolved
// is not an error here, just a formatPgValueTyped call that renders without
// a declared field order or element type instead of refusing.
//
// pos is col's index in the row's own output column list — sendDataRow and
// sendDataRowFormatted's loop variable, unchanged from the caller. When the
// name lookup misses, a positional fallback tries nestedSchema.ordered at
// pos: nestedFieldSchema's doc explains why that is sound for the coord
// path's schema (positionally aligned with the output columns) and a no-op
// for the legacy catalog-lookup one (ordered left nil there). Without this,
// a renamed ROW/ARRAY/MAP output column — an alias, or the gather's own
// renamer — lost its declared structure entirely and fell back to
// formatPgComposite's schema-less rendering (sorted keys for a ROW) even
// though the query's real output schema still had it, at the same position
// coordColumnMetas already trusts for its own positional fallback (#471
// resurfacing).
func nestedColumnFor(nestedSchema *nestedFieldSchema, name string, pos int) *parquet.Column {
	if nestedSchema == nil {
		return nil
	}
	if col, ok := nestedSchema.byName[name]; ok {
		return &col
	}
	if nestedSchema.ordered != nil && pos >= 0 && pos < len(nestedSchema.ordered) {
		col := nestedSchema.ordered[pos]
		return &col
	}
	return nil
}

// sendDataRowFormatted sends a DataRow using the format codes from Bind.
// Columns with format code 1 (binary) get binary-encoded values.
// metas provides type info for correct binary encoding (may be nil for text-only).
func (c *pgConn) sendDataRowFormatted(columns []string, cells []any, fmtCodes []int16, colTypes []parquet.TypeID, nestedSchema *nestedFieldSchema) {
	c.buf = c.buf[:0]
	c.buf = appendInt16(c.buf, int16(len(columns)))

	for i, col := range columns {
		val := cellAt(cells, i)
		if val == nil {
			c.buf = appendInt32(c.buf, -1)
			continue
		}

		// ROW/ARRAY/MAP have no PostgreSQL binary wire form of their own —
		// they declare OID 25 (text), same as an unresolved type, and OID
		// 25's "binary" format IS its text bytes — so both formats render
		// the same way here, ahead of the numeric/timestamp/date binary
		// arms below (which do not apply) and appendBinaryValue's generic
		// fallback (which used to reach these via Go's %v and print
		// "map[...]"/"[...]" instead of PostgreSQL's composite/array text).
		switch val.(type) {
		case map[string]any, []any:
			s := formatPgValueTyped(val, nestedColumnFor(nestedSchema, col, i))
			c.buf = appendInt32(c.buf, int32(len(s)))
			c.buf = append(c.buf, s...)
			continue
		}

		// Determine format for this column — the same resolution the
		// RowDescription declared (fmtCodeAt), so declaration and bytes agree.
		binary := fmtCodeAt(fmtCodes, i) == 1

		colType := columnTypeAt(colTypes, i)
		isTS := colType == parquet.TypeTimestamp
		ms, msOK := val.(int64)
		ds, dsOK := val.(string)

		switch {
		case binary && isTS && msOK:
			// Binary `timestamp` is microseconds since 2000-01-01, not the
			// engine's milliseconds since 1970. Emitting the raw int64 kept
			// the declared 8-byte width, so the client parsed it happily
			// and landed ~30000 years off.
			c.buf = appendBinaryTimestamp(c.buf, ms)
		case binary && colType == parquet.TypeDate && dsOK:
			// Binary `date` is a 4-byte day count. The engine boxes a date
			// as its rendered text, which appendBinaryValue would have
			// written verbatim under OID 1082.
			c.buf = appendBinaryDate(c.buf, ds)
		case binary && colType == parquet.TypeDecimal && dsOK:
			// Binary `numeric` is a base-10000 digit vector. A DECIMAL is
			// boxed as its rendered text, and appendBinaryValue's string arm
			// would have written those ASCII bytes verbatim under OID 1700 —
			// the same defect the two arms above exist to prevent, and the
			// one the OID change would otherwise have CREATED (under OID 25
			// the raw bytes were the right binary form of a text column).
			c.buf = appendBinaryNumeric(c.buf, ds)
		case binary && colType == parquet.TypeUUID && dsOK:
			// Binary `uuid` is 16 raw bytes. A UUID is boxed as its canonical
			// 36-character text, which appendBinaryValue's string arm would
			// have written verbatim under OID 2950 — the defect the numeric
			// and date arms above exist to prevent, and the one declaring
			// 2950 would otherwise have CREATED (under OID 25 those text
			// bytes WERE the right binary form).
			c.buf = appendBinaryUUID(c.buf, ds)
		case binary:
			c.buf = appendBinaryValue(c.buf, val)
		default:
			s := formatPgValueTyped(val, nestedColumnFor(nestedSchema, col, i))
			if isTS && msOK {
				s = batch.FormatTimestamp(ms)
			}
			c.buf = appendInt32(c.buf, int32(len(s)))
			c.buf = append(c.buf, s...)
		}
	}

	c.sendMsg('D', c.buf)
}

// appendBinaryTimestamp encodes epoch milliseconds as a PostgreSQL binary
// `timestamp` (OID 1114): int64 microseconds since 2000-01-01T00:00:00Z.
//
// Values whose microsecond form would overflow int64 are sent as NULL rather
// than as a wrapped-around instant: the field is fixed at 8 bytes, so there
// is no way to signal "out of range" other than absence, and a silently
// wrapped date is exactly the failure mode this whole change is closing.
func appendBinaryTimestamp(buf []byte, ms int64) []byte {
	const maxMillis = (math.MaxInt64 - pgEpochOffsetMicros) / 1000
	const minMillis = (math.MinInt64 + pgEpochOffsetMicros) / 1000
	if ms > maxMillis || ms < minMillis {
		return appendInt32(buf, -1)
	}
	us := ms*1000 - pgEpochOffsetMicros
	buf = appendInt32(buf, 8)
	return append(buf, byte(us>>56), byte(us>>48), byte(us>>40), byte(us>>32),
		byte(us>>24), byte(us>>16), byte(us>>8), byte(us))
}

// appendBinaryUUID encodes a canonical 8-4-4-4-12 UUID text into PostgreSQL's
// binary `uuid` (OID 2950): the 16 bytes, in order, with no separators.
//
// A value that does not parse is sent as NULL. The field is a fixed 16 bytes,
// so there is no way to say "not a uuid" other than absence — and writing the
// 36-character text instead, which is what the generic encoder did, hands the
// client 36 bytes to read as 16.
func appendBinaryUUID(buf []byte, s string) []byte {
	var out [16]byte
	n := 0
	for i := 0; i < len(s) && n < 32; i++ {
		ch := s[i]
		if ch == '-' {
			continue
		}
		var d byte
		switch {
		case ch >= '0' && ch <= '9':
			d = ch - '0'
		case ch >= 'a' && ch <= 'f':
			d = ch - 'a' + 10
		case ch >= 'A' && ch <= 'F':
			d = ch - 'A' + 10
		default:
			return appendInt32(buf, -1)
		}
		if n%2 == 0 {
			out[n/2] = d << 4
		} else {
			out[n/2] |= d
		}
		n++
	}
	if n != 32 {
		return appendInt32(buf, -1)
	}
	buf = appendInt32(buf, 16)
	return append(buf, out[:]...)
}

// pgEpochDays is 2000-01-01 expressed in days since the Unix epoch — the
// origin PostgreSQL's binary `date` counts from.
const pgEpochDays = 10957

// appendBinaryDate encodes a date rendered as YYYY-MM-DD into a PostgreSQL
// binary `date` (OID 1082): int32 days since 2000-01-01.
//
// A value that does not parse is sent as NULL. The field is a fixed 4 bytes,
// so there is no way to say "not a date" other than absence — and writing the
// text instead, which is what the generic encoder did, hands the client four
// bytes of ASCII to read as a day count.
func appendBinaryDate(buf []byte, s string) []byte {
	t, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return appendInt32(buf, -1)
	}
	// Midnight UTC is always an exact multiple of a day, so this division is
	// exact on both sides of the epoch.
	days := t.Unix()/86400 - pgEpochDays
	if days > math.MaxInt32 || days < math.MinInt32 {
		return appendInt32(buf, -1)
	}
	buf = appendInt32(buf, 4)
	return appendInt32(buf, int32(days))
}

// PostgreSQL's binary `numeric` is a base-10000 digit vector, not a number:
//
//	int16 ndigits, int16 weight, int16 sign, int16 dscale, int16 digits[ndigits]
//
// with the value being sum(digits[i] * 10000^(weight-i)) under sign, and
// dscale the number of fraction digits to DISPLAY. There is no float in it,
// which is the point — it is why an exact decimal survives the wire.
const (
	pgNumericPos int16 = 0x0000
	pgNumericNeg int16 = 0x4000
)

// appendBinaryNumeric encodes a decimal rendered as [-]ddd[.ddd] into a
// PostgreSQL binary `numeric` (OID 1700).
//
// A value that does not parse is sent as NULL. Unlike `date` and `timestamp`
// the field is variable-length, so writing the text instead would not even be
// caught by a width check — a client would decode ASCII as digit groups and
// get a number with no relation to the value.
func appendBinaryNumeric(buf []byte, s string) []byte {
	digits, weight, sign, dscale, ok := pgNumericDigits(s)
	if !ok {
		return appendInt32(buf, -1)
	}
	buf = appendInt32(buf, int32(8+2*len(digits)))
	buf = appendInt16(buf, int16(len(digits)))
	buf = appendInt16(buf, weight)
	buf = appendInt16(buf, sign)
	buf = appendInt16(buf, dscale)
	for _, d := range digits {
		buf = appendInt16(buf, d)
	}
	return buf
}

// pgNumericDigits splits a rendered decimal into the four header fields and
// the base-10000 digit vector. It works on the DIGITS, never through a float:
// the whole reason a DECIMAL column exists is that float64 cannot hold its
// values, and a wide DECIMAL(38,10) needs 128 bits.
func pgNumericDigits(s string) (digits []int16, weight, sign, dscale int16, ok bool) {
	sign = pgNumericPos
	switch {
	case strings.HasPrefix(s, "-"):
		sign, s = pgNumericNeg, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	// At least one digit somewhere: "", "-" and "." otherwise walked the
	// arithmetic below to a clean ZERO, which is a value, not a refusal.
	if intPart == "" && fracPart == "" {
		return nil, 0, 0, 0, false
	}
	if !allASCIIDigits(intPart) || !allASCIIDigits(fracPart) {
		return nil, 0, 0, 0, false
	}
	if intPart == "" {
		intPart = "0"
	}
	if len(fracPart) > math.MaxInt16 || len(intPart) > math.MaxInt16 {
		return nil, 0, 0, 0, false
	}
	dscale = int16(len(fracPart))

	// Whole base-10000 groups: the integer part pads on the LEFT and the
	// fraction on the RIGHT, because the decimal point is the group boundary.
	if r := len(intPart) % 4; r != 0 {
		intPart = strings.Repeat("0", 4-r) + intPart
	}
	if r := len(fracPart) % 4; r != 0 {
		fracPart += strings.Repeat("0", 4-r)
	}
	all := intPart + fracPart
	weight = int16(len(intPart)/4 - 1)
	digits = make([]int16, 0, len(all)/4)
	for i := 0; i < len(all); i += 4 {
		var d int16
		for _, ch := range []byte(all[i : i+4]) {
			d = d*10 + int16(ch-'0')
		}
		digits = append(digits, d)
	}
	// Leading zero groups shift the weight; trailing ones just shorten the
	// vector. PostgreSQL writes zero as ndigits = 0.
	lead := 0
	for lead < len(digits) && digits[lead] == 0 {
		lead++
	}
	digits = digits[lead:]
	weight -= int16(lead)
	for len(digits) > 0 && digits[len(digits)-1] == 0 {
		digits = digits[:len(digits)-1]
	}
	if len(digits) == 0 {
		weight = 0
	}
	return digits, weight, sign, dscale, true
}

func allASCIIDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
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
	case []byte:
		// bytea's binary form is the value itself — byteasend writes the
		// bytes and nothing else. Without this arm a BYTES column fell to
		// the %v fallback below and shipped Go's slice-of-decimal-bytes
		// debug notation ("[255 254 0 65]") under a declared OID 17, which
		// is the same defect shape appendBinaryTimestamp/appendBinaryDate/
		// appendBinaryNumeric exist to prevent for their own types: bytes a
		// typed client decodes under the OID it was promised (#570).
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

// formatPgValue formats a value for PgWire text output when no column-type
// declaration is available for it — the introspection/catalog-emulation
// rows, and any caller that predates typed rendering. It is
// formatPgValueTyped with no column, so a ROW/ARRAY/MAP value still comes
// out in PostgreSQL's composite/array shape, just without a schema to give
// a ROW its DECLARED field order or an ARRAY/MAP its element type — see
// formatPgValueTyped's doc for what that fallback looks like.
func formatPgValue(val any) string {
	return formatPgValueTyped(val, nil)
}

// formatPgValueTyped formats a value for PgWire text output, the way
// formatPgValue always did for every scalar type, plus PostgreSQL's actual
// composite/array text forms for ROW and ARRAY (#471) using col — the
// column's declared parquet.Column, when sendResultRows was able to resolve
// one (queryViaCoord's exact answer, or nestedColumnSchemas' catalog-lookup
// best effort for the legacy query path) — for a ROW's field order and an
// ARRAY's element type. col may be nil (an unresolved computed expression,
// or any other caller that has none): every nested-type helper below
// degrades to a schema-free rendering instead of refusing, per each
// helper's own doc.
func formatPgValueTyped(val any, col *parquet.Column) string {
	switch tv := val.(type) {
	case []float32:
		// VECTOR: a wadjet extension with no PostgreSQL array/composite
		// form to match (unlike ARRAY/ROW below, which this bracket
		// convention used to also apply, incorrectly — see #471). Left as
		// is: square brackets, no space, the established display for this
		// type.
		var buf strings.Builder
		buf.WriteByte('[')
		for i, f := range tv {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(fmt.Sprintf("%g", f))
		}
		buf.WriteByte(']')
		return buf.String()
	case []any:
		// wadjet's boxing for BOTH ARRAY and MAP (a MAP's entries arrive as
		// one 2-field ROW per key, in the sorted-key order mapEntryRows
		// already put them in) — formatPgArrayOrMap tells them apart using
		// col.Type when it is known.
		return formatPgArrayOrMap(tv, col)
	case map[string]any:
		// wadjet's boxing for ROW.
		return formatPgComposite(tv, col)
	case bool:
		// PostgreSQL text format for bool is t/f, not true/false.
		if tv {
			return "t"
		}
		return "f"
	case []byte:
		// wadjet's boxing for BYTES, and PostgreSQL's text form for the
		// bytea it now declares (OID 17): `\x` then LOWERCASE hex, which is
		// byteaout under the default bytea_output = hex — the setting
		// expr.pgcompat already reports to a client that asks. This arm used
		// to be missing, so the value fell to the %v default and went out as
		// "[255 254 0 65]" (#570).
		//
		// The hex form also removes a hazard the raw bytes carried: a
		// non-UTF-8 value with an embedded NUL cannot appear in a
		// PostgreSQL text-format field at all, and libpq's PQgetvalue —
		// a NUL-terminated char* — truncates at it, so pgx read four bytes
		// where psql read two. Hex is pure ASCII.
		return `\x` + hex.EncodeToString(tv)
	case float64:
		return formatPgFloat(tv, 64)
	case float32:
		return formatPgFloat(float64(tv), 32)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatPgArrayOrMap renders wadjet's []any boxing for ARRAY and MAP.
// col.Type tells them apart when it is known; without it (col is nil, or
// declares neither) this renders as an ARRAY, which is right far more often
// — MAP is the wadjet extension here, ARRAY is the PostgreSQL type.
//
// Review note (N2, adversarial review of #464/#471, not fixed here): this
// default is also a genuine DISPLAY divergence, not just a best-effort
// guess. The exact same MAP value renders two different text shapes
// depending on whether nestedSchema happened to resolve col for this
// position — formatPgMap's "{k1: v1, k2: v2}" when it did, this function's
// ARRAY-shaped "{elem1,elem2}" (each entry's own 2-field ROW rendered as a
// composite, comma-joined) when it did not. A client could see either
// shape for the same query depending on the coord vs. legacy path, or a
// renamed/computed MAP column landing outside both nestedSchemaByName and
// nestedColumnSchemas' resolution. Fixing it needs a way to tell "this is
// unambiguously a MAP, just with no known field names" from "this could be
// either" — which the boxed value alone (a bare []any, identical for both
// types) does not carry, and no col to consult.
func formatPgArrayOrMap(elems []any, col *parquet.Column) string {
	if col != nil && col.Type == parquet.TypeMap {
		return formatPgMap(elems, col)
	}
	var elemCol *parquet.Column
	if col != nil && col.ElementType != nil {
		elemCol = col.ElementType
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range elems {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(formatPgArrayElement(e, elemCol))
	}
	b.WriteByte('}')
	return b.String()
}

// formatPgArrayElement renders one ARRAY element in PostgreSQL's array text
// form: a genuine NULL is the bare, unquoted keyword (never the four
// letters as text — quotePgArray is what quotes THAT case, below); anything
// else is rendered by its own type, recursively, and then quoted if
// quotePgArray says the result needs it.
func formatPgArrayElement(val any, col *parquet.Column) string {
	if val == nil {
		return "NULL"
	}
	return quotePgArray(formatPgValueTyped(val, col))
}

// quotePgArray applies PostgreSQL's array-element quoting: wrap in double
// quotes — backslash-escaping an embedded double quote or backslash — when
// the text is empty, reads case-insensitively as the NULL keyword, or
// contains a character the array parser would otherwise treat as
// structural.
//
// Verified against live PostgreSQL 17 (array_out). This input (a plain
// value, a value needing quoting for each trigger character below, an
// empty string, and the literal text "NULL"):
//
//	ARRAY['plain','has,comma','has"quote','has\backslash','has space',
//	      '','NULL','has{brace}','has(paren)']
//
// renders as:
//
//	{plain,"has,comma","has\"quote","has\\backslash","has space","","NULL",
//	 "has{brace}",has(paren)}
//
// Note parentheses do NOT trigger array quoting (last element, unquoted) —
// that is a COMPOSITE rule, quotePgComposite below, not an array one.
func quotePgArray(s string) string {
	if !pgArrayNeedsQuoting(s) {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		if r == '"' || r == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('"')
	return b.String()
}

func pgArrayNeedsQuoting(s string) bool {
	if s == "" || strings.EqualFold(s, "NULL") {
		return true
	}
	for _, r := range s {
		switch r {
		// PostgreSQL's array_isspace (arrayfuncs.c) treats seven bytes as
		// whitespace: space, tab, newline, CR, vertical tab, and form feed
		// — the last two (\v, \f) were missing here, so a leading or
		// trailing VT/FF silently dropped its quoting on a round trip
		// instead of coming back with it, same as any other array-breaking
		// character would.
		case ',', '{', '}', '"', '\\', ' ', '\t', '\n', '\r', '\v', '\f':
			return true
		}
	}
	return false
}

// formatPgMap renders a MAP's entries — already in the sorted-key order
// mapEntryRows (internal/engine/batch) put them in when the row was
// written, which this function relies on rather than re-deriving: ranging
// a Go map here would reintroduce the exact randomization mapEntryRows
// exists to avoid — as "{k1: v1, k2: v2}". MAP is a wadjet extension with no
// PostgreSQL form to match, so this keeps the bracket convention already in
// use rather than inventing a new one; the fix is the two real defects
// map-range-random order (gone: this walks the incoming SLICE, never a Go
// map) and a NULL value printing as Go's "<nil>" (renders as the unquoted
// NULL keyword now, matching quotePgArray's convention above it).
//
// col, when known, is a TypeMap column: like ARRAY, its per-entry structure
// lives under ElementType — not Fields directly (a MAP column's Fields is
// unused; ElementType points at a TypeRow column carrying the two entry
// fields, the same declaration shape internal/engine/batch and every other
// nested-type schema in this codebase uses) — whose two Fields name which
// of each entry's two keys is "key" and which is "value". mapEntryRows
// writes those same two names, defaulting to "key"/"value" when the schema
// does not override them, which this falls back to as well when col is nil
// or its structure does not resolve.
func formatPgMap(entries []any, col *parquet.Column) string {
	keyName, valName := "key", "value"
	var keyCol, valCol *parquet.Column
	if col != nil && col.ElementType != nil && len(col.ElementType.Fields) == 2 {
		entryFields := col.ElementType.Fields
		keyName, valName = entryFields[0].Name, entryFields[1].Name
		keyCol, valCol = &entryFields[0], &entryFields[1]
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, e := range entries {
		if i > 0 {
			b.WriteString(", ")
		}
		entry, ok := e.(map[string]any)
		if !ok {
			// Not the 2-field entry shape mapEntryRows produces — render
			// whatever it is rather than panic on a type assertion.
			b.WriteString(formatPgValueTyped(e, nil))
			continue
		}
		b.WriteString(formatPgMapEntryField(entry[keyName], keyCol))
		b.WriteString(": ")
		b.WriteString(formatPgMapEntryField(entry[valName], valCol))
	}
	b.WriteByte('}')
	return b.String()
}

func formatPgMapEntryField(val any, col *parquet.Column) string {
	if val == nil {
		return "NULL"
	}
	return formatPgValueTyped(val, col)
}

// formatPgComposite renders a ROW value in PostgreSQL's composite text
// form: parenthesized, comma-separated, in the column's DECLARED field
// order (col.Fields, when known) — with a NULL field as an EMPTY slot
// between commas, never the word NULL (that is an array/MAP convention,
// not a composite one).
//
// Without a declared order (col is nil, or names a different number of
// fields than the value has keys — a computed ROW expression on the legacy
// query path, or a catalog name collision nestedColumnSchemas already
// distrusts for a different reason), the keys are sorted instead: not
// PostgreSQL's answer, but deterministic, which random Go map iteration was
// not.
//
// Verified against live PostgreSQL 17 (record_out) on a 3-field composite:
// ROW(1,NULL,3) renders "(1,,3)"; the NULL middle field is nothing between
// the two commas, not the word NULL.
func formatPgComposite(row map[string]any, col *parquet.Column) string {
	names, fieldCols := compositeFieldOrder(row, col)
	var b strings.Builder
	b.WriteByte('(')
	for i, name := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		v, ok := row[name]
		if !ok || v == nil {
			continue // NULL: an empty slot, not text.
		}
		b.WriteString(quotePgComposite(formatPgValueTyped(v, fieldCols[i])))
	}
	b.WriteByte(')')
	return b.String()
}

// compositeFieldOrder returns row's field names in DISPLAY order, and the
// declared Column for each (nil where col did not cover it) for recursive
// rendering.
func compositeFieldOrder(row map[string]any, col *parquet.Column) ([]string, []*parquet.Column) {
	if col != nil && len(col.Fields) == len(row) {
		names := make([]string, len(col.Fields))
		cols := make([]*parquet.Column, len(col.Fields))
		for i := range col.Fields {
			names[i] = col.Fields[i].Name
			cols[i] = &col.Fields[i]
		}
		return names, cols
	}
	names := make([]string, 0, len(row))
	for k := range row {
		names = append(names, k)
	}
	sort.Strings(names)
	return names, make([]*parquet.Column, len(names))
}

// quotePgComposite applies PostgreSQL's composite-field quoting: wrap in
// double quotes — doubling an embedded double quote, backslash-escaping an
// embedded backslash — when the text is empty or contains a character the
// composite parser would otherwise treat as structural.
//
// Verified against live PostgreSQL 17 (record_out) field by field:
// braces do NOT trigger composite quoting ("has{brace}" prints unquoted,
// unlike an array element with the same content — quotePgArray above) and,
// a genuine PostgreSQL quirk this mirrors rather than improves on, the
// literal text "NULL" is not quoted either — ROW(1,'NULL',3) prints
// "(1,NULL,3)", indistinguishable in the composite's OWN text from what a
// true NULL field renders as ONE LEVEL UP, in an array or a bare column:
// an empty slot here, not this string.
func quotePgComposite(s string) string {
	if !pgCompositeNeedsQuoting(s) {
		return s
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`""`)
		case '\\':
			b.WriteString(`\\`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

func pgCompositeNeedsQuoting(s string) bool {
	if s == "" {
		return true
	}
	for _, r := range s {
		switch r {
		// record_in/record_out's isspace (rowtypes.c, same set as the C
		// library isspace it defers to for this) treats \v and \f as
		// whitespace alongside space/tab/newline/CR — the same gap as
		// pgArrayNeedsQuoting above, and the same fix.
		case ',', '(', ')', '"', '\\', ' ', '\t', '\n', '\r', '\v', '\f':
			return true
		}
	}
	return false
}

// formatPgFloat renders a float the way PostgreSQL's text protocol does:
// plain decimal for ordinary magnitudes, where Go's %v switches to
// e-notation once the exponent reaches the digit count — an epoch like
// 1787049120 came out "1.78704912e+09", which a client reading it as an
// integer rejects. Extreme magnitudes keep e-notation, and the special
// values use PostgreSQL's spellings.
func formatPgFloat(v float64, bits int) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "Infinity"
	case math.IsInf(v, -1):
		return "-Infinity"
	}
	if a := math.Abs(v); v == 0 || (a >= 1e-4 && a < 1e15) {
		return strconv.FormatFloat(v, 'f', -1, bits)
	}
	return strconv.FormatFloat(v, 'e', -1, bits)
}

func (c *pgConn) sendCommandComplete(tag string) {
	payload := append([]byte(tag), 0)
	c.sendMsg('C', payload)
}

func (c *pgConn) sendError(severity, code, message string) {
	// Every field here is NUL-terminated, so a NUL inside one would end it
	// early and let the remainder be read as further fields — a message
	// built from a recovered panic value is attacker-influenced text, so
	// strip them rather than trust the source (#511).
	severity = stripNUL(severity)
	code = stripNUL(code)
	message = stripNUL(message)
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

// stripNUL removes NUL bytes, which terminate a wire field.
func stripNUL(s string) string {
	if !strings.ContainsRune(s, 0) {
		return s
	}
	return strings.ReplaceAll(s, "\x00", "")
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

// commandTag renders the CommandComplete tag for a DML statement.
//
// PostgreSQL's format carries the row count alone — "DELETE 3", "UPDATE 3" —
// and only INSERT prefixes an OID, which is 0 on any modern server:
// "INSERT 0 3". Every command went out in the INSERT form, so psql answered a
// DELETE with "could not interpret result from server: DELETE 0 0" and drivers
// that parse the tag for an affected-row count read the wrong field.
func commandTag(command string, rows int64) string {
	// One renderer for every door (review B8): the HTTP door used to build the
	// tag itself and dropped INSERT's oid field.
	return wadjet.CommandTag(command, rows)
}
