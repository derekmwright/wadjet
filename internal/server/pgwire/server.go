// Package pgwire implements the PostgreSQL v3 wire protocol frontend.
// This allows psql, JDBC, ODBC, and any Postgres-compatible client to
// connect to Caelum and execute SQL queries.
package pgwire

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/derekmwright/caelum/caelum"
)

// Server listens for PostgreSQL wire protocol connections and dispatches
// queries to a Caelum DB instance.
type Server struct {
	db       *caelum.DB
	listener net.Listener
	logger   *slog.Logger
	wg       sync.WaitGroup
	done     chan struct{}
}

// Config holds configuration for the pgwire server.
type Config struct {
	Addr string // listen address, e.g. ":5433"
}

// NewServer creates a new PostgreSQL wire protocol server.
func NewServer(db *caelum.DB, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		db:     db,
		logger: logger,
		done:   make(chan struct{}),
	}
}

// Start begins listening for connections on the given address.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("pgwire listen: %w", err)
	}
	s.listener = ln
	s.logger.Info("PostgreSQL wire protocol server listening", "addr", addr)

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
		conn:   conn,
		db:     s.db,
		logger: s.logger,
		buf:    make([]byte, 0, 4096),
	}
	c.run()
}

// pgConn handles a single PostgreSQL client connection.
type pgConn struct {
	conn   net.Conn
	db     *caelum.DB
	logger *slog.Logger
	buf    []byte
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
	if strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "RESET ") ||
		strings.HasPrefix(upper, "DISCARD ") ||
		strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK") ||
		strings.HasPrefix(upper, "DEALLOCATE") {
		c.sendCommandComplete("SET")
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
	c.sendRowDescription(columns)

	// Send DataRow for each row
	for _, row := range result.Rows {
		c.sendDataRow(columns, row)
	}

	// Send CommandComplete
	c.sendCommandComplete(fmt.Sprintf("SELECT %d", len(result.Rows)))
	c.sendReadyForQuery()
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
	// 'Z' + int32(5) + byte('I' = idle)
	c.conn.Write([]byte{'Z', 0, 0, 0, 5, 'I'})
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
		s := fmt.Sprintf("%v", val)
		c.buf = appendInt32(c.buf, int32(len(s)))
		c.buf = append(c.buf, s...)
	}

	c.sendMsg('D', c.buf)
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
