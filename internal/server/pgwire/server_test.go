package pgwire

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/derekmwright/caelum/caelum"
	"github.com/derekmwright/caelum/internal/storage/ingest"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func setupTestDB(t *testing.T) *caelum.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := caelum.Open(ctx, caelum.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt32},
			{Name: "name", Type: parquet.TypeString},
			{Name: "score", Type: parquet.TypeFloat64},
		},
	}
	if err := db.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"id": int32(1), "name": "alice", "score": 95.5},
		{"id": int32(2), "name": "bob", "score": 87.3},
		{"id": int32(3), "name": "carol", "score": 92.1},
	}
	ing := db.NewIngester("users", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

func startTestServer(t *testing.T, db *caelum.DB) *Server {
	t.Helper()
	srv := NewServer(db, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

// pgClient is a minimal PostgreSQL wire protocol client for testing.
type pgClient struct {
	conn net.Conn
	t    *testing.T
}

func newPGClient(t *testing.T, addr string) *pgClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("connecting to pgwire: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &pgClient{conn: conn, t: t}
}

func (c *pgClient) startup(user, database string) {
	c.t.Helper()
	// Build startup message: int32(len) + int32(196608) + params
	var payload []byte
	payload = binary.BigEndian.AppendUint32(payload, 196608) // protocol 3.0
	payload = append(payload, "user"...)
	payload = append(payload, 0)
	payload = append(payload, user...)
	payload = append(payload, 0)
	payload = append(payload, "database"...)
	payload = append(payload, 0)
	payload = append(payload, database...)
	payload = append(payload, 0)
	payload = append(payload, 0) // terminator

	// Prepend length
	msg := binary.BigEndian.AppendUint32(nil, uint32(len(payload)+4))
	msg = append(msg, payload...)

	if _, err := c.conn.Write(msg); err != nil {
		c.t.Fatalf("writing startup: %v", err)
	}

	// Read responses until ReadyForQuery
	for {
		typ, _, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading startup response: %v", err)
		}
		if typ == 'Z' { // ReadyForQuery
			return
		}
		// Expect R (Auth), S (ParamStatus), K (BackendKeyData)
		if typ != 'R' && typ != 'S' && typ != 'K' {
			c.t.Fatalf("unexpected startup message type: %c", typ)
		}
	}
}

func (c *pgClient) simpleQuery(sql string) (columns []string, rows [][]string, tag string) {
	c.t.Helper()
	// 'Q' + int32(len) + sql\0
	payload := append([]byte(sql), 0)
	c.writeMsg('Q', payload)

	// Read responses
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading query response: %v", err)
		}
		switch typ {
		case 'T': // RowDescription
			columns = c.parseRowDesc(data)
		case 'D': // DataRow
			rows = append(rows, c.parseDataRow(data, len(columns)))
		case 'C': // CommandComplete
			tag = readCString(data)
		case 'I': // EmptyQueryResponse
			tag = "EMPTY"
		case 'E': // ErrorResponse
			msg := c.parseError(data)
			c.t.Logf("pgwire error: %s", msg)
			tag = "ERROR: " + msg
		case 'Z': // ReadyForQuery
			return
		}
	}
}

func (c *pgClient) terminate() {
	c.writeMsg('X', nil)
}

func (c *pgClient) writeMsg(typ byte, payload []byte) {
	c.t.Helper()
	var header [5]byte
	header[0] = typ
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)+4))
	if _, err := c.conn.Write(header[:]); err != nil {
		c.t.Fatalf("writing message header: %v", err)
	}
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			c.t.Fatalf("writing message payload: %v", err)
		}
	}
}

func (c *pgClient) readMsg() (byte, []byte, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var header [5]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	msgLen := int(binary.BigEndian.Uint32(header[1:])) - 4
	if msgLen < 0 {
		return msgType, nil, nil
	}
	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

func (c *pgClient) parseRowDesc(data []byte) []string {
	if len(data) < 2 {
		return nil
	}
	nCols := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]

	cols := make([]string, nCols)
	for i := 0; i < nCols; i++ {
		name := readCString(data)
		cols[i] = name
		// Skip: name\0 + tableOID(4) + colAttr(2) + typeOID(4) + typeSize(2) + typMod(4) + fmtCode(2) = 18 bytes after name
		data = data[len(name)+1+18:]
	}
	return cols
}

func (c *pgClient) parseDataRow(data []byte, nCols int) []string {
	if len(data) < 2 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]

	vals := make([]string, n)
	for i := 0; i < n; i++ {
		colLen := int(int32(binary.BigEndian.Uint32(data[:4])))
		data = data[4:]
		if colLen == -1 {
			vals[i] = "NULL"
		} else {
			vals[i] = string(data[:colLen])
			data = data[colLen:]
		}
	}
	return vals
}

func (c *pgClient) parseError(data []byte) string {
	// Parse error fields
	var msg string
	for len(data) > 0 {
		fieldType := data[0]
		data = data[1:]
		if fieldType == 0 {
			break
		}
		val := readCString(data)
		data = data[len(val)+1:]
		if fieldType == 'M' {
			msg = val
		}
	}
	return msg
}

// Tests

func TestPGWireStartup(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")
	client.terminate()
}

func TestPGWireSimpleQuery(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	columns, rows, tag := client.simpleQuery("SELECT id, name, score FROM users ORDER BY id")
	t.Logf("Columns: %v", columns)
	t.Logf("Tag: %s", tag)
	for i, row := range rows {
		t.Logf("Row %d: %v", i, row)
	}

	if len(columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(columns))
	}
	if columns[0] != "id" || columns[1] != "name" || columns[2] != "score" {
		t.Fatalf("unexpected columns: %v", columns)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if tag != "SELECT 3" {
		t.Errorf("expected tag 'SELECT 3', got %q", tag)
	}

	client.terminate()
}

func TestPGWireAggregateQuery(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	columns, rows, _ := client.simpleQuery("SELECT COUNT(*) as cnt, AVG(score) as avg_score FROM users")
	t.Logf("Columns: %v, Rows: %v", columns, rows)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	client.terminate()
}

func TestPGWireEmptyResult(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Use explicit columns (not *) to get proper RowDescription even with 0 rows
	columns, rows, tag := client.simpleQuery("SELECT id, name FROM users WHERE id = 999")
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
	if tag != "SELECT 0" {
		t.Errorf("expected tag 'SELECT 0', got %q", tag)
	}
	if len(columns) != 2 {
		t.Errorf("expected 2 columns, got %d: %v", len(columns), columns)
	}

	client.terminate()
}

func TestPGWireSQLError(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, _, tag := client.simpleQuery("INVALID SQL QUERY")
	if tag == "" || !containsSubstring(tag, "ERROR") {
		t.Errorf("expected error tag, got %q", tag)
	}

	// Connection should still be usable after error
	columns, rows, tag2 := client.simpleQuery("SELECT id FROM users ORDER BY id LIMIT 1")
	if len(columns) == 0 || len(rows) == 0 {
		t.Error("expected query to succeed after error")
	}
	t.Logf("After error recovery: columns=%v rows=%v tag=%s", columns, rows, tag2)

	client.terminate()
}

func TestPGWireSetCommand(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// These are sent by JDBC/psql during connection setup
	_, _, tag := client.simpleQuery("SET client_encoding = 'UTF8'")
	if tag != "SET" {
		t.Errorf("expected SET tag, got %q", tag)
	}

	_, _, tag = client.simpleQuery("SET extra_float_digits = 3")
	if tag != "SET" {
		t.Errorf("expected SET tag, got %q", tag)
	}

	client.terminate()
}

func TestPGWireMultipleQueries(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Run multiple queries on same connection
	for i := 0; i < 5; i++ {
		_, rows, _ := client.simpleQuery(fmt.Sprintf("SELECT * FROM users WHERE id = %d", i+1))
		expectedRows := 0
		if i < 3 {
			expectedRows = 1
		}
		if len(rows) != expectedRows {
			t.Errorf("query %d: expected %d rows, got %d", i, expectedRows, len(rows))
		}
	}

	client.terminate()
}

func TestPGWireSSLDecline(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send SSL request
	sslReq := binary.BigEndian.AppendUint32(nil, 8) // length
	sslReq = binary.BigEndian.AppendUint32(sslReq, 80877103) // SSL request code
	conn.Write(sslReq)

	// Should get 'N' back (SSL declined)
	var resp [1]byte
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		t.Fatal(err)
	}
	if resp[0] != 'N' {
		t.Fatalf("expected 'N' for SSL decline, got %c", resp[0])
	}
}

func TestPGWireShowTables(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	columns, rows, tag := client.simpleQuery("SHOW TABLES")
	t.Logf("SHOW TABLES: columns=%v rows=%v tag=%s", columns, rows, tag)

	if len(rows) < 1 {
		t.Error("expected at least 1 table in SHOW TABLES")
	}

	client.terminate()
}

func TestPGWireConcurrentConnections(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	// Launch 5 concurrent connections
	var wg = make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			client := newPGClient(t, srv.Addr())
			client.startup("user", "db")
			_, rows, _ := client.simpleQuery("SELECT * FROM users")
			client.terminate()
			if len(rows) != 3 {
				wg <- fmt.Errorf("conn %d: expected 3 rows, got %d", id, len(rows))
			} else {
				wg <- nil
			}
		}(i)
	}

	for i := 0; i < 5; i++ {
		if err := <-wg; err != nil {
			t.Error(err)
		}
	}
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && contains(s, sub))
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
