// SPDX-License-Identifier: MIT

// Package pgwire implements the PostgreSQL v3 wire protocol frontend.
// This allows psql, JDBC, ODBC, and any Postgres-compatible client to
// connect to Wadjet and execute SQL queries.
package pgwire

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/planner/syscatalog"
	"github.com/derekmwright/wadjet/internal/queryroute"
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
	router       queryroute.Router // optional; SELECT routes through it when set
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

// SetRouter attaches a query router so SELECT statements stream through
// its ExecuteSQL (the coordinator's native-DAG executor, with batched
// output) instead of the legacy wadjet.DB.Query path which materializes all
// rows into a single CollectSink — root cause of the 2026-04-25 Q18 SF10
// OOM.
//
// When unset, all paths fall back to db.Query. That is not a degraded mode:
// it is the embedded server, pgwire over a wadjet.DB and nothing else, which
// is why the router is an interface this package owns rather than the
// coordinator type it used to be. When set, only SELECT and WITH route
// through it; DDL / DESCRIBE / introspection stay on db.Query.
func (s *Server) SetRouter(router queryroute.Router) {
	s.router = router
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
	// Same attach rule as the HTTP door and the embedded API: a policy set
	// installed against a DB that has a catalog is BOUND to it here, and an
	// unbindable one is remembered so enforcement refuses rather than running
	// on the fold-aware floor alone (ADR-0033 rule 3, #882).
	if db != nil {
		auth.AttachProvider(context.Background(), s.authProvider, db.Catalog(), logger)
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
		router:       s.router,
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
	router       queryroute.Router // optional: routes SELECT away from db.Query when non-nil
	server       *Server           // back-pointer for query admission
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
	preparedSQL  string   // last parsed statement SQL
	preparedOIDs []uint32 // parameter type OIDs Parse declared for it
	portalSQL    string   // last bound portal SQL
	// portalOpen / portalName are whether a portal EXISTS and what it is
	// called. This connection holds one portal; PostgreSQL destroys it at the
	// Sync that ends an implicit transaction, at a simple Query (the unnamed
	// one), at Close, and never creates it when Bind is refused. Execute and
	// Describe of a portal that does not exist answer 34000. Without this an
	// Execute after Sync, with no new Bind, re-ran the previous statement
	// (#1266 review B7).
	portalOpen      bool
	portalName      string
	stmts           map[string]string   // named prepared statements
	stmtOIDs        map[string][]uint32 // their declared parameter type OIDs
	described       bool                // true if Describe was sent for current portal
	describedFields int                 // field count of the RowDescription Describe sent
	resultFmtCodes  []int16             // result format codes from Bind (0=text, 1=binary)
	describeResult  *wadjet.QueryResult // cached Describe result for Execute reuse
	describeStream  queryroute.Stream   // columnar half of describeResult (routed path)
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
		if c.txState != 'T' {
			c.closePortal()
		}
		c.sendReadyForQuery()
	case 'C': // Close (prepared statement or portal)
		c.handleClose(payload)
	case 'd', 'c', 'f': // CopyData / CopyDone / CopyFail outside copy mode
		// Accepted and IGNORED, which is what PostgreSQL does: "we probably
		// got here because a COPY failed, and the frontend is still sending
		// data" (PostgresMain). A COPY that is refused before CopyInResponse
		// leaves a client that had already queued its rows sending them
		// anyway, and routing those into the default case below answered
		// 08P01 AND set skipUntilSync — so a simple-protocol client, which
		// never sends Sync, got no answer to anything it sent afterwards
		// (round-1 review P4). The refusal must end the statement, not the
		// connection.
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
	c.sendParamStatus("server_version", expr.ServerVersionShort+" (Wadjet)")
	c.sendParamStatus("server_encoding", "UTF8")
	c.sendParamStatus("client_encoding", "UTF8")
	c.sendParamStatus("DateStyle", "ISO, MDY")
	c.sendParamStatus("integer_datetimes", "on")
	c.sendParamStatus("standard_conforming_strings", "on")
	c.sendParamStatus("TimeZone", "UTC")
	c.sendParamStatus("IntervalStyle", "postgres")
	// `is_superuser` is what the AUTHORIZER says, never what the role is
	// CALLED. A role named `admin` that holds only `read` reported
	// `is_superuser=on`, and a role named `ops` holding `admin` reported
	// `off` — a psql prompt and every client that branches on this parameter
	// read a privilege nobody granted (#938, ADR-0034: no admin by
	// inference). With no provider (auth disabled) the session is
	// unrestricted, which is what `on` has always meant there.
	c.sendParamStatus("is_superuser", boolParam(c.isAdmin()))

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
		// The trusted environment this statement arrived on (SEC1/#933): the
		// peer address the listener observed, and the protocol this door is.
		// It is attached HERE, not at authentication, because this is the
		// context enforcement reads; the port is stripped by auth.
		ctx = auth.ContextWithEnvironment(ctx, auth.Environment{
			SourceIP: peerAddr(c.conn), Protocol: "pgwire",
		})
	}
	// What the catalog reports about this connection (pg_stat_ssl).
	sess := syscatalog.Session{}
	if tc, ok := c.conn.(*tls.Conn); ok {
		sess.SSL = true
		st := tc.ConnectionState()
		sess.SSLVersion = tls.VersionName(st.Version)
		sess.SSLCipher = tls.CipherSuiteName(st.CipherSuite)
	}
	ctx = syscatalog.WithSession(ctx, sess)
	// Session-level statement_timeout overrides server default
	timeout := c.queryTimeout
	if v, ok := c.sessionVars["statement_timeout"]; ok {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			timeout = time.Duration(ms) * time.Millisecond
		}
	}
	return c.beginStatement(ctx, timeout)
}

// peerAddr is the connection's remote address, or "" for a conn that has
// none (a pipe in a test). The port is stripped where every door's is, in
// auth.ContextWithEnvironment.
func peerAddr(c net.Conn) string {
	if c == nil {
		return ""
	}
	if a := c.RemoteAddr(); a != nil {
		return a.String()
	}
	return ""
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

// copyIdent reads one COPY identifier using the lexer's naming rule (#731):
// unquoted names fold; delimited names preserve bytes and unescape doubled quotes.
// Never merely trim quotes and erase the distinction before resolution.
// parseCopySQL uses it for COPY table [(columns)] FROM STDIN names.
// See docs/internals/pgwire-copy-identifier-folding.md for the design.
func copyIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		// Delimited: the bytes are the name. `""` inside is one quote.
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return batch.FoldIdent(s)
}

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
	table = copyIdent(tableName)

	// Parse optional column list
	if len(rest) > 0 && rest[0] == '(' {
		end := strings.IndexByte(rest, ')')
		if end > 0 {
			colStr := rest[1:end]
			for _, c := range strings.Split(colStr, ",") {
				if col := copyIdent(strings.TrimSpace(c)); col != "" {
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

	// The WRITE decision comes FIRST — before the relation's existence is
	// reported and before its column list is resolved (#938, round-1 review).
	//
	// The 42P01 below and the 42703 further down are metadata about a relation
	// this identity may not touch: asked after them, the authorization refusal
	// left three distinguishable answers where there should be one, so an
	// unauthorized caller could decide whether a relation exists and whether a
	// guessed column exists on it. Asked here, a caller that may not write the
	// relation learns only that, whether or not the relation is real; a caller
	// the decision permits then gets the real 42P01 / 42703 it needs.
	//
	// `auth.TableAccess` is the one effective table decision every door asks
	// (ADR-0034): the evaluator decides where one is installed, and the legacy
	// role rule (HasPermission("write") AND CanAccessTable) where none is.
	if err := auth.TableAccess(ctx, c.authProvider, tableName, auth.ActionWrite); err != nil {
		c.sendQueryError(ctx, "42501", err)
		return
	}

	tableMeta, err := c.db.Catalog().GetTable(ctx, tableName)
	if err != nil {
		c.sendError("ERROR", "42P01", fmt.Sprintf("table %q does not exist", tableName))
		return
	}

	// Determine column ordering
	var columns []string
	if len(copyColumns) > 0 {
		// A COPY column list is a list of REFERENCES, and an unquoted
		// identifier folds to lower case at the lexer (#731) while the
		// catalog keeps the parquet file's spelling. The list is used twice
		// below and BOTH uses are byte-exact against that schema: as the key
		// into `colByName`, where a miss yields the ZERO parquet.Column and
		// so parses every field as a BOOL, and as the key of the row map the
		// ingester reads back with `row[col.Name]`, where a miss writes NULL.
		// The TABLE name two lines above already takes this concession; the
		// column list had none, so `COPY hits (watchid, useragent)` — the
		// spelling a fold-aware client sends — silently filled the table with
		// NULLs. Resolve each name and carry the SCHEMA's spelling forward.
		columns = make([]string, len(copyColumns))
		for i, name := range copyColumns {
			j := batch.ResolveSchemaIndex(tableMeta.Schema.Columns, name)
			if j < 0 {
				// A name that resolves to nothing is 42703 HERE, naming the
				// column, which is what PostgreSQL answers at parse time. It
				// used to fall through to `colByName`, where a miss yields the
				// ZERO parquet.Column — whose Type is TypeBool, the zero of the
				// TypeID iota — so the client got `strconv.ParseBool: parsing
				// "7": invalid syntax` on the first data row: a type error
				// naming the wrong problem, for a column that simply is not
				// there.
				c.sendError("ERROR", "42703",
					fmt.Sprintf("column %q of relation %q does not exist", name, tableName))
				return
			}
			columns[i] = tableMeta.Schema.Columns[j].Name
		}
	} else {
		columns = make([]string, len(tableMeta.Schema.Columns))
		for i, col := range tableMeta.Schema.Columns {
			columns[i] = col.Name
		}
	}

	// COPY must authorize WRITE before CopyInResponse ('G') or constructing an
	// ingester (#938), including the relation decision before exposing existence.
	// Check the column list through EnforceDMLPolicies on a synthesized INSERT,
	// not a COPY-specific policy rule: denied targets get INSERT's 42703.
	// Refuse instead of CopyInResponse; consume no rows and keep the ordinary
	// message loop/ReadyForQuery available.
	// See docs/internals/pgwire-copy-authorization-boundary.md for the design.
	if err := auth.EnforceDMLPolicies(ctx, c.authProvider, c.db.Catalog(), &plansql.ParsedQuery{
		Type: plansql.QueryInsert,
		// The column list as the STATEMENT gave it (resolved to the schema's
		// spelling), not the expansion: an INSERT with no column list names
		// no targets either, and COPY answers what INSERT answers.
		Insert: &plansql.InsertInfo{Table: tableName, Columns: copyTargets(copyColumns, columns)},
	}, "pgwire"); err != nil {
		c.sendQueryError(ctx, "42501", err)
		return
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

// isAdmin reports whether this connection's identity holds the `admin`
// PERMISSION. With no provider or auth disabled every session is
// unrestricted, so it reports true.
func (c *pgConn) isAdmin() bool {
	if c.authProvider == nil || !c.authProvider.Enabled() {
		return true
	}
	if c.identity == nil {
		return false
	}
	authz := c.authProvider.Authorizer()
	return authz != nil && authz.HasPermission(c.identity, "admin")
}

func boolParam(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// copyTargets is the column list the write DECISION sees: the resolved
// spelling when the statement named columns, and nothing when it did not.
//
// `COPY t FROM STDIN` with no list is `INSERT INTO t VALUES (…)` with no list,
// and it earns the same answer — handing the expansion to the column check
// instead would make COPY refuse where INSERT permits, which is a fork of one
// rule into two.
func copyTargets(stated, resolved []string) []string {
	if len(stated) == 0 {
		return nil
	}
	return resolved
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
	if c.portalName == "" {
		c.closePortal() // a simple Query destroys the unnamed portal
	}
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
		return ans.err
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
		if ans.err != nil {
			c.sendSynthRefusal(ans.err)
			return true
		}
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
	if isWriteSQL(sql) {
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
	var stream queryroute.Stream
	var nestedSchema *nestedFieldSchema
	var err error
	if c.router != nil && c.canBypassDB() && shouldRouteToRouter(sql) {
		result, stream, nestedSchema, err = c.queryViaRouter(ctx, sql)
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
	// Bind replaces the portal; a refused Bind leaves none.
	c.closePortal()

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
					// The extended protocol's error state, as for every other
					// refusal here: without it the client's Describe/Execute
					// ran the PREVIOUS portal's SQL (c.portalSQL is untouched)
					// and its CommandComplete followed this error — a refused
					// Bind answered another statement's rows (#1266 census).
					c.skipUntilSync = true
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

	c.portalOpen, c.portalName = true, portal

	// Send BindComplete ('2')
	c.sendMsg('2', nil)
}

// closePortal destroys the connection's portal (see portalOpen).
func (c *pgConn) closePortal() {
	c.portalOpen, c.portalName, c.portalSQL = false, "", ""
}

// refuseMissingPortal answers PostgreSQL's 34000 for an Execute or Describe
// naming a portal that does not exist, and enters the error state, so the
// statement a closed portal held can never run again.
func (c *pgConn) refuseMissingPortal(name string) bool {
	if c.portalOpen && name == c.portalName {
		return false
	}
	c.sendError("ERROR", "34000", fmt.Sprintf("portal \"%s\" does not exist", name))
	c.skipUntilSync = true
	return true
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
		if c.refuseMissingPortal(readCString(payload[1:])) {
			return
		}
		// Portal describe — send RowDescription based on portal SQL, declaring
		// the result format codes the portal's Bind requested (#362).
		c.describeSQL(c.portalSQL, c.resultFmtCodes)
	}
}

// analysisRefusal reports whether a Describe-time failure is one PostgreSQL
// raises during PARSE ANALYSIS — class 42 (a syntax error, a missing
// relation, column or function, a type the operator has no form for), 0A
// (a feature the analyzer refuses), 3D/3F (a missing database or schema) —
// rather than while the statement RUNS, which PostgreSQL reports at Execute
// after describing the portal.
//
// Two exceptions keep today's answer. 42501 is a privilege check, which
// PostgreSQL makes at executor start, after Describe. And a statement whose
// parameters were probed with NULL (substituteNullParams) may fail on the
// NULL where the bound value would not; there only the failures a NULL cannot
// cause — syntax, a missing relation, a missing column — are refused here.
func analysisRefusal(err error, parameterized bool) bool {
	state := sqlerr.StateOf(err)
	if state == "" || state == "42501" {
		return false
	}
	if parameterized {
		switch state {
		case "42601", "42P01", "42703":
			return true
		}
		return false
	}
	switch state[:2] {
	case "42", "0A", "3D", "3F":
		return true
	}
	return false
}

// describeSQL discovers result columns and sends a typed RowDescription,
// NoData, or an analysis-class ErrorResponse. Runtime failures retain the
// Execute-time path; analysisRefusal defines the parameter and 42501 exceptions.
// fmtCodes supplies each field format (ADR-0044).
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
	if isWriteSQL(sql) {
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
		if ans.err != nil {
			// A shape the emulation refuses is refused HERE too, not
			// described and then refused at Execute: a client that holds a
			// RowDescription expects tuples under it.
			c.closeDescribeCache()
			c.sendSynthRefusal(ans.err)
			c.skipUntilSync = true
			return
		}
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
	var stream queryroute.Stream
	var nestedSchema *nestedFieldSchema
	var err error
	if c.router != nil && c.canBypassDB() && shouldRouteToRouter(shapeSQL) {
		result, stream, nestedSchema, err = c.queryViaRouter(ctx, shapeSQL)
	} else {
		result, err = c.db.Query(ctx, shapeSQL)
		if err == nil {
			nestedSchema = c.nestedColumnSchemas(shapeSQL, result.ColumnMetas)
		}
	}
	if err != nil && analysisRefusal(err, parameterized) {
		// A statement PostgreSQL cannot ANALYZE is refused at Describe, with
		// its own SQLSTATE and sentence: that is where the server raises a
		// missing relation, an unknown column or function, a syntax error —
		// during parse analysis, before any portal exists (measured on 17.11
		// for Describe of a statement and of a portal alike). NoData says
		// "this statement returns no rows", which is DDL's answer and DML's
		// without RETURNING, and a client that prepared a typo'd statement
		// read "no columns" first and the error second (#998).
		c.closeDescribeCache()
		c.sendQueryError(ctx, "42000", err)
		c.skipUntilSync = true
		return
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
	if c.refuseMissingPortal(readCString(payload)) {
		return
	}
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
	if isWriteSQL(sql) {
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
		if ans.err != nil {
			c.sendSynthRefusal(ans.err)
			c.skipUntilSync = true
			return
		}
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
	var stream queryroute.Stream
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
		if c.router != nil && c.canBypassDB() && shouldRouteToRouter(sql) {
			result, stream, nestedSchema, err = c.queryViaRouter(ctx, sql)
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
	if len(payload) >= 1 && payload[0] == 'P' && c.portalOpen && readCString(payload[1:]) == c.portalName {
		c.closePortal()
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
	// err REFUSES the statement instead of answering it. The catalog
	// emulation models a bounded set of introspection shapes, and a shape it
	// does not model must say so rather than answer the columns with no rows:
	// an empty answer is indistinguishable from "there is no such relation",
	// which is exactly how `\d` reported a table that exists (#944). Carried
	// on the answer rather than returned beside it because `matchIntrospection`
	// is consulted from four places and every one of them already branches on
	// nil-or-not.
	err error
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

// matchIntrospection answers the handful of statements this layer answers
// itself rather than the engine: SHOW, the bare version() probe, and the
// one-column FROM-less session expressions (matchSyntheticSelect). It returns
// nil for everything else, which the caller runs on the query engine.
//
// The CATALOG is not here. pg_catalog and information_schema are relations the
// engine scans (package syscatalog, ADR-0044): a canned responder that matched
// a statement's TEXT answered its precomputed rows whatever the WHERE, the
// JOIN or the aggregate said (#1251), and no amount of teaching it spellings
// would have made it a query.
func (c *pgConn) matchIntrospection(sql, upper string) *synthAnswer {
	// Comments are not part of the statement: `/* app */ SHOW search_path`
	// is SHOW (see stripSQLComments).
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

	// version(), bare or pg_catalog-qualified. Only the one-expression form
	// is claimed; any richer spelling (an alias, more columns) is a real
	// query the engine answers with version()'s value under the client's own
	// labels.
	if list, ok := selectList(normalized); ok &&
		(list == "VERSION()" || list == "PG_CATALOG.VERSION()") {
		return singleRow([]string{"version"}, map[string]any{
			"version": expr.ServerVersion,
		})
	}

	// SELECT with no FROM clause — the session expressions whose answer is
	// this connection's (current_user is the authenticated identity).
	if strings.HasPrefix(normalized, "SELECT ") && !strings.Contains(normalized, " FROM ") {
		return c.matchSyntheticSelect(normalized)
	}
	return nil
}

// stripSQLComments removes -- line comments and /* block */ comments (nested,
// as PostgreSQL nests them), leaving a space where each stood so tokens do not
// fuse. String literals and quoted identifiers are respected.
//
// This layer decides by the statement's leading word (SHOW, a FROM-less
// SELECT), so it has to read the statement the server will run, not the
// commentary in front of it.
func stripSQLComments(sql string) string {
	var out strings.Builder
	out.Grow(len(sql))
	inSingle, inDouble := false, false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
		case ch == '\'':
			inSingle = true
		case ch == '"':
			inDouble = true
		case ch == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			out.WriteByte(' ')
			if i < len(sql) {
				out.WriteByte('\n')
			}
			continue
		case ch == '/' && i+1 < len(sql) && sql[i+1] == '*':
			depth, j := 1, i+2
			for j < len(sql) && depth > 0 {
				if sql[j] == '/' && j+1 < len(sql) && sql[j+1] == '*' {
					depth++
					j += 2
					continue
				}
				if sql[j] == '*' && j+1 < len(sql) && sql[j+1] == '/' {
					depth--
					j += 2
					continue
				}
				j++
			}
			i = j - 1
			out.WriteByte(' ')
			continue
		}
		out.WriteByte(ch)
	}
	return out.String()
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
	"server_version":              {"server_version", expr.ServerVersionShort},
	"server_version_num":          {"server_version_num", expr.ServerVersionNum},
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

// sendSynthRefusal reports an introspection shape the emulation does not
// model, with the SQLSTATE the refusal carries (0A000 by default, the class
// this branch uses for "not implemented here" the way
// plansql.RefuseUnsupportedStatement does).
func (c *pgConn) sendSynthRefusal(err error) {
	code := sqlerr.StateOf(err)
	if code == "" {
		code = "0A000"
	}
	c.sendError("ERROR", code, err.Error())
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

// pgColumnOID uses the whole meta: TypeString with StringLength != 0 is
// VARCHAR/1043, not text/25 (#838). Positive length is constrained; -1 is
// unconstrained varchar; zero is text. OID follows destination, not length presence.
// Do not send CHAR/bpchar1042: this engine does not implement its padding and
// trailing-blank rules. Varchar states the implemented bounded-character value;
// the padding residual is ADR-0012 item 5.
// See docs/internals/pgwire-string-column-oid.md for the design.
func pgColumnOID(m wadjet.ColumnMeta) int {
	if m.TypeID == parquet.TypeString && m.StringLength != 0 {
		return oidVarchar
	}
	if m.TypeID == parquet.TypeArray {
		return pgArrayColumnOID(m.ElementType)
	}
	return pgTypeOID(m.TypeName)
}

// pgArrayColumnOID is the OID an ARRAY column declares: PostgreSQL's array
// type OF its element (int4[] 1007, int8[] 1016, text[] 1009, float8[] 1022,
// numeric[] 1231, timestamp[] 1115, date[] 1182, uuid[] 2951, bool[] 1000,
// bytea[] 1001).
//
// PostgreSQL has no generic `array`, so declaring OID 25 for every ARRAY
// meant the text a client read was right — `{1,2}` either way — while the
// TYPE it was told was not: JDBC's getArray, pgx's array scanning and
// DataGrip's column typing all key on the OID, so an array column arrived as
// a string and a typed consumer refused or mis-rendered it (#992).
//
// TWO element kinds deliberately keep text, and both are a fact about
// PostgreSQL rather than a gap here:
//
//   - a NESTED array. PostgreSQL's `int4[][]` is still OID 1007 and is
//     RECTANGULAR — one element count per dimension, in the binary header and
//     enforced by its text parser. This engine's nested arrays are ragged in
//     general (`{{1,2},{3}}` is a value here and a syntax error there), so
//     declaring 1007 for one would promise a shape the value may not have.
//   - a ROW or MAP element. PostgreSQL has no MAP at all, and a composite
//     array needs the composite's own registered OID, which a query that
//     CONSTRUCTS a row does not have. ADR-0012 records both.
//
// A nil element is an ARRAY the plan could not type, and it keeps text for
// the same reason a DECIMAL with no (p,s) keeps typmod -1: a declaration
// invented here is one a client cannot tell from a real one.
func pgArrayColumnOID(elem *parquet.Column) int {
	if elem == nil {
		return oidText
	}
	switch elem.Type {
	case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap:
		return oidText
	}
	return pgArrayOID(pgTypeOID(elem.Type.String()))
}

// pgArrayOID maps an element's own declared OID onto PostgreSQL's array OID
// for it. The pairing is pg_type.typarray, read off a live 17.11 catalog.
//
// Keyed on the ELEMENT'S OID rather than on the engine type, so the two can
// never drift: whatever pgTypeOID decides a scalar column of that type
// declares, its array declares the array of exactly that. The types this
// engine renders as text — IPv4, IPv6, CIDR, MAC, VECTOR — therefore land on
// text[] 1009, which is what their scalar declaration already says.
func pgArrayOID(elemOID int) int {
	switch elemOID {
	case 16:
		return 1000 // bool[]
	case 17:
		return 1001 // bytea[]
	case 20:
		return 1016 // int8[]
	case 23:
		return 1007 // int4[]
	case 700:
		return 1021 // float4[]
	case 701:
		return 1022 // float8[]
	case oidVarchar:
		return 1015 // varchar[]
	case 1082:
		return 1182 // date[]
	case 1114:
		return 1115 // timestamp[]
	case 1700:
		return 1231 // numeric[]
	case 2950:
		return 2951 // uuid[]
	}
	return 1009 // text[]
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

// visibleCatalogTables is the relation set every synthetic catalog view is
// built from, filtered to the ones this identity may READ.
//
// It is ONE filter at the source rather than one per view, and that is the
// point: `pg_class`, `pg_tables`, `pg_attribute`, `information_schema.tables`
// and `information_schema.columns` all render from the same list, so a view
// that hid `pg_class` and not `pg_attribute` would leave `\d` — which joins
// them — still answering. `psql`'s `\d`, DataGrip's tree and every BI tool's
// schema discovery come through exactly this route, so it is the door on
// which "metadata follows the effective table decision" (ADR-0034) is worth
// most: before this, `SELECT relname FROM pg_catalog.pg_class WHERE
// relname='secret'` answered `secret` to an identity whose policy denies it,
// and the pg_attribute join handed over its column names and types.
//
// Provider nil / auth disabled: `auth.VisibleTables` returns the list
// unchanged, so nothing changes for the embedded and dev paths.
func (c *pgConn) visibleCatalogTables(ctx context.Context) ([]string, error) {
	tables, err := c.db.ListTables(ctx)
	if err != nil {
		return nil, err
	}
	return auth.VisibleTables(ctx, c.authProvider, tables), nil
}

// isWriteSQL reports whether a statement is a WRITE — one whose answer is a
// command tag over no rows, not a result set.
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
//
// `CREATE TABLE … AS SELECT` is here too (#1024) and a declared `CREATE TABLE`
// is not: the CTAS runs a query and answers `SELECT <n>` or `CREATE TABLE AS`,
// which is exactly the tag #816 was about, while the declared form still takes
// the query path and its long-standing tag.
func isWriteSQL(sql string) bool {
	switch plansql.LeadingKeyword(sql) {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return true
	case "CREATE":
		return plansql.IsCreateTableAsSelect(sql)
	}
	return false
}

func isCommandSQL(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	// The utility statements Execute acknowledges without running (see
	// handleExecute): each describes as NoData, never as a query.
	return strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "CLOSE") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ") ||
		strings.HasPrefix(upper, "DEALLOCATE") ||
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

// TypeMod exposes the actual client typmod for oracle checks (ADR-0024 item 5).
// NUMERIC packs ((precision<<16)|scale)+VARHDRSZ (#454); absent precision or
// WireUnconstrained emits -1, never invented (0,0). VARCHAR length emits n+4;
// unconstrained types emit -1. Key on engine TypeID, not wire OID, when adding
// parameterized types; keep the declaration's information at the wire boundary.
// See docs/internals/pgwire-result-type-modifiers.md for the design.
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

// sendColumnTypes marks outputs whose boxed form needs wire conversion (#321).
// TIMESTAMP boxes epoch milliseconds; DATE boxes text but binary OID1082 needs
// four-byte days. DECIMAL and UUID likewise have distinct binary encodings.
// Match metas by position AND name, then name lookup for reordered columns.
// Return nil if nothing needs conversion so ordinary rows pay only a nil check;
// never send boxed text bytes under an incompatible declared binary OID.
// See docs/internals/pgwire-send-column-conversions.md for the design.
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

		// ROW and MAP have no PostgreSQL binary wire form of their own —
		// they declare OID 25 (text), same as an unresolved type, and OID
		// 25's "binary" format IS its text bytes — so both formats render
		// the same way here, ahead of the numeric/timestamp/date binary
		// arms below (which do not apply) and appendBinaryValue's generic
		// fallback (which used to reach these via Go's %v and print
		// "map[...]"/"[...]" instead of PostgreSQL's composite/array text).
		//
		// An ARRAY is the one that left this arm with #992. It now declares
		// its element's array OID, and under a BINARY format code that OID
		// promises PostgreSQL's array wire form rather than the `{…}` text —
		// the same obligation declaring 1082/1700/2950 created for date,
		// numeric and uuid. An ARRAY still declaring text (a nested or
		// composite element, or one the plan could not type) keeps the text
		// bytes, which under OID 25 is what binary means.
		switch val.(type) {
		case map[string]any, []any:
			decl := nestedColumnFor(nestedSchema, col, i)
			if vals, isArr := val.([]any); isArr && fmtCodeAt(fmtCodes, i) == 1 {
				if elem, ok := binaryArrayElement(decl); ok {
					c.buf = appendBinaryArray(c.buf, vals, elem)
					continue
				}
			}
			s := formatPgValueTyped(val, decl)
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

// binaryArrayElement reports the ELEMENT a binary-format ARRAY is written
// from, and false for every column that keeps OID 25 — a ROW, a MAP, an
// untyped or nested ARRAY. It is pgArrayColumnOID's own test, asked of the
// DECLARATION rather than of the OID, so the bytes and the RowDescription
// cannot come to different conclusions about one column (#992).
func binaryArrayElement(decl *parquet.Column) (parquet.Column, bool) {
	if decl == nil || decl.Type != parquet.TypeArray || decl.ElementType == nil {
		return parquet.Column{}, false
	}
	switch decl.ElementType.Type {
	case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap:
		return parquet.Column{}, false
	}
	return *decl.ElementType, true
}

// appendBinaryArray encodes one ARRAY value in PostgreSQL's array binary
// format: ndim, a has-null flag, the element OID, then one (length, lower
// bound) pair per dimension and the elements themselves, each a length-
// prefixed value or -1 for NULL.
//
// One dimension only, which is the shape binaryArrayElement admits. An EMPTY
// array is ndim 0 with no dimension pair at all — PostgreSQL's own encoding
// for `'{}'::int4[]`, and not the same bytes as a one-dimension array of
// length zero.
//
// The elements go through appendBinaryCell, the same dispatch the scalar
// columns take, so a date, timestamp, numeric or uuid ELEMENT is encoded the
// way that type's own column would be rather than as the text it is boxed as.
func appendBinaryArray(buf []byte, vals []any, elem parquet.Column) []byte {
	elemOID := int32(pgTypeOID(elem.Type.String()))
	var hasNull int32
	for _, v := range vals {
		if v == nil {
			hasNull = 1
			break
		}
	}
	var p []byte
	if len(vals) == 0 {
		p = appendInt32(p, 0)
		p = appendInt32(p, hasNull)
		p = appendInt32(p, elemOID)
	} else {
		p = appendInt32(p, 1)
		p = appendInt32(p, hasNull)
		p = appendInt32(p, elemOID)
		p = appendInt32(p, int32(len(vals)))
		p = appendInt32(p, 1)
		for _, v := range vals {
			p = appendBinaryCell(p, v, elem.Type)
		}
	}
	buf = appendInt32(buf, int32(len(p)))
	return append(buf, p...)
}

// appendBinaryCell writes one length-prefixed binary value under the type its
// column declares — the four conversions sendDataRowFormatted makes inline for
// a top-level column, in one function so an ARRAY's ELEMENTS take exactly the
// same route. A NULL is -1 with no payload, which is what both a NULL column
// and a NULL array element are on the wire.
func appendBinaryCell(buf []byte, val any, colType parquet.TypeID) []byte {
	if val == nil {
		return appendInt32(buf, -1)
	}
	switch colType {
	case parquet.TypeTimestamp:
		if ms, ok := val.(int64); ok {
			return appendBinaryTimestamp(buf, ms)
		}
	case parquet.TypeDate:
		if s, ok := val.(string); ok {
			return appendBinaryDate(buf, s)
		}
	case parquet.TypeDecimal:
		if s, ok := val.(string); ok {
			return appendBinaryNumeric(buf, s)
		}
	case parquet.TypeUUID:
		if s, ok := val.(string); ok {
			return appendBinaryUUID(buf, s)
		}
	}
	return appendBinaryValue(buf, val)
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
// rows. It is batch.FormatPGText with no column: a ROW/ARRAY/MAP value still
// comes out in PostgreSQL's composite/array shape, just without a schema to
// give a ROW its DECLARED field order or an ARRAY/MAP its element type.
func formatPgValue(val any) string {
	return batch.FormatPGText(val, nil)
}

// formatPgValueTyped is PostgreSQL's text output of val under its declared
// column — the ONE renderer every door shares (batch.FormatPGText, arc CW);
// col may be nil.
func formatPgValueTyped(val any, col *parquet.Column) string {
	return batch.FormatPGText(val, col)
}

// formatPgFloat renders a float the way PostgreSQL's text protocol does. It
// is batch.FormatFloat8Text, the ONE renderer this engine has for a float's
// text form — the double/real-to-TEXT assignment and cast sites in the
// embedded engine call the same function, so a DOUBLE prints one text on the
// wire and the identical one once it is stored in a TEXT column (review r5
// P1, #1252).
func formatPgFloat(v float64, bits int) string { return batch.FormatFloat8Text(v, bits) }
func quotePgArray(s string) string             { return batch.QuotePGArrayElement(s) }
func quotePgComposite(s string) string         { return batch.QuotePGCompositeField(s) }

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
