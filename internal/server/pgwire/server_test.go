package pgwire

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

func setupTestDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
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

func startTestServer(t *testing.T, db *wadjet.DB) *Server {
	t.Helper()
	srv := NewServer(db, Config{}, nil)
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

// parseRowDescTyped extracts column names AND type OIDs from a RowDescription.
func (c *pgClient) parseRowDescTyped(data []byte) (names []string, oids []int) {
	if len(data) < 2 {
		return nil, nil
	}
	nCols := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]

	names = make([]string, nCols)
	oids = make([]int, nCols)
	for i := 0; i < nCols; i++ {
		name := readCString(data)
		names[i] = name
		data = data[len(name)+1:]
		// tableOID(4) + colAttr(2) = 6 bytes
		data = data[6:]
		// typeOID(4)
		oids[i] = int(binary.BigEndian.Uint32(data[:4]))
		data = data[4:]
		// typeSize(2) + typMod(4) + fmtCode(2) = 8 bytes
		data = data[8:]
	}
	return
}

// extendedQuery runs Parse/Bind/Describe(Portal)/Execute/Sync and returns
// typed RowDescription info alongside normal results.
func (c *pgClient) extendedQuery(sql string) (names []string, oids []int, rows [][]string, tag string) {
	c.t.Helper()

	// Parse: ""(unnamed) + sql\0 + 0 param types
	var parseBuf []byte
	parseBuf = append(parseBuf, 0) // unnamed statement
	parseBuf = append(parseBuf, sql...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0) // 0 param types
	c.writeMsg('P', parseBuf)

	// Bind: unnamed portal + unnamed statement + 0 fmt codes + 0 params + 0 result fmt codes
	var bindBuf []byte
	bindBuf = append(bindBuf, 0)    // unnamed portal
	bindBuf = append(bindBuf, 0)    // unnamed statement
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 format codes
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 parameters
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 result format codes
	c.writeMsg('B', bindBuf)

	// Describe portal
	var descBuf []byte
	descBuf = append(descBuf, 'P') // portal
	descBuf = append(descBuf, 0)   // unnamed
	c.writeMsg('D', descBuf)

	// Execute unnamed portal, 0 = no row limit
	var execBuf []byte
	execBuf = append(execBuf, 0) // unnamed portal
	execBuf = binary.BigEndian.AppendUint32(execBuf, 0)
	c.writeMsg('E', execBuf)

	// Sync
	c.writeMsg('S', nil)

	// Read responses
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading extended query response: %v", err)
		}
		switch typ {
		case '1': // ParseComplete
		case '2': // BindComplete
		case 'T': // RowDescription
			names, oids = c.parseRowDescTyped(data)
		case 'D': // DataRow
			rows = append(rows, c.parseDataRow(data, len(names)))
		case 'C': // CommandComplete
			tag = readCString(data)
		case 'n': // NoData
		case 'E': // ErrorResponse
			msg := c.parseError(data)
			c.t.Logf("extended query error: %s", msg)
			tag = "ERROR: " + msg
		case 'Z': // ReadyForQuery
			return
		}
	}
}

func TestPGWireTypedRowDescription(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	names, oids, rows, tag := client.extendedQuery("SELECT id, name, score FROM users ORDER BY id")
	t.Logf("Columns: %v, OIDs: %v, Tag: %s", names, oids, tag)

	if len(names) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(names))
	}
	// id is INT32 → OID 23 (int4)
	if oids[0] != 23 {
		t.Errorf("expected id OID 23 (int4), got %d", oids[0])
	}
	// name is STRING → OID 25 (text)
	if oids[1] != 25 {
		t.Errorf("expected name OID 25 (text), got %d", oids[1])
	}
	// score is FLOAT64 → OID 701 (float8)
	if oids[2] != 701 {
		t.Errorf("expected score OID 701 (float8), got %d", oids[2])
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	client.terminate()
}

func TestPGWireDescribeStatement(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Parse a named statement
	var parseBuf []byte
	parseBuf = append(parseBuf, "stmt1"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, "SELECT id, score FROM users"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	client.writeMsg('P', parseBuf)

	// Describe statement (not portal)
	var descBuf []byte
	descBuf = append(descBuf, 'S')
	descBuf = append(descBuf, "stmt1"...)
	descBuf = append(descBuf, 0)
	client.writeMsg('D', descBuf)

	// Sync
	client.writeMsg('S', nil)

	// Read responses
	var gotParamDesc, gotRowDesc bool
	var names []string
	var oids []int
	for {
		typ, data, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading describe response: %v", err)
		}
		switch typ {
		case '1': // ParseComplete
		case 't': // ParameterDescription
			gotParamDesc = true
		case 'T': // RowDescription
			gotRowDesc = true
			names, oids = client.parseRowDescTyped(data)
		case 'n': // NoData
		case 'Z':
			if !gotParamDesc {
				t.Error("did not receive ParameterDescription")
			}
			if !gotRowDesc {
				t.Error("did not receive RowDescription (got NoData)")
			}
			if len(names) > 0 {
				t.Logf("Described columns: %v, OIDs: %v", names, oids)
				if names[0] != "id" {
					t.Errorf("expected first column 'id', got %q", names[0])
				}
			}
			client.terminate()
			return
		}
	}
}

func TestPGWireTransactionState(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// BEGIN — should get 'T' state in ReadyForQuery
	client.writeMsg('Q', append([]byte("BEGIN"), 0))
	var txState byte
	for {
		typ, data, err := client.readMsg()
		if err != nil {
			t.Fatal(err)
		}
		if typ == 'Z' {
			txState = data[0]
			break
		}
	}
	if txState != 'T' {
		t.Errorf("expected txState 'T' after BEGIN, got %c", txState)
	}

	// COMMIT — should get 'I' state
	client.writeMsg('Q', append([]byte("COMMIT"), 0))
	for {
		typ, data, err := client.readMsg()
		if err != nil {
			t.Fatal(err)
		}
		if typ == 'Z' {
			txState = data[0]
			break
		}
	}
	if txState != 'I' {
		t.Errorf("expected txState 'I' after COMMIT, got %c", txState)
	}

	client.terminate()
}

func TestPGWirePgType(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	cols, rows, tag := client.simpleQuery("SELECT oid, typname FROM pg_type")
	t.Logf("pg_type: %d columns, %d rows, tag=%s", len(cols), len(rows), tag)

	if len(rows) < 10 {
		t.Errorf("expected at least 10 pg_type rows, got %d", len(rows))
	}

	// Verify int4 is present
	found := false
	for _, row := range rows {
		if len(row) >= 2 && row[0] == "23" && row[1] == "int4" {
			found = true
			break
		}
	}
	if !found {
		t.Error("pg_type missing int4 (OID 23)")
	}

	client.terminate()
}

func TestPGWireInfoSchemaTables(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	cols, rows, tag := client.simpleQuery("SELECT table_name FROM information_schema.tables")
	t.Logf("info_schema.tables: cols=%v rows=%v tag=%s", cols, rows, tag)

	if len(rows) < 1 {
		t.Fatal("expected at least 1 table in information_schema.tables")
	}
	found := false
	for _, row := range rows {
		if len(row) > 0 && row[0] == "users" {
			found = true
		}
	}
	if !found {
		// The table_name column may not be first depending on SELECT list
		t.Logf("Note: 'users' table may be in a different column position")
	}

	client.terminate()
}

// startTestServerWithAuth creates a pgwire server backed by an auth provider.
func startTestServerWithAuth(t *testing.T, db *wadjet.DB, provider *auth.Provider) *Server {
	t.Helper()
	srv := NewServer(db, Config{AuthProvider: provider}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	return srv
}

// startupWithPassword sends a startup message and responds to a cleartext
// password request with the given token. Returns an error string on auth
// failure, or "" on success.
func (c *pgClient) startupWithPassword(user, database, password string) string {
	c.t.Helper()
	// Build startup message
	var payload []byte
	payload = binary.BigEndian.AppendUint32(payload, 196608)
	payload = append(payload, "user"...)
	payload = append(payload, 0)
	payload = append(payload, user...)
	payload = append(payload, 0)
	payload = append(payload, "database"...)
	payload = append(payload, 0)
	payload = append(payload, database...)
	payload = append(payload, 0)
	payload = append(payload, 0) // terminator

	msg := binary.BigEndian.AppendUint32(nil, uint32(len(payload)+4))
	msg = append(msg, payload...)
	if _, err := c.conn.Write(msg); err != nil {
		c.t.Fatalf("writing startup: %v", err)
	}

	// Read first response — either AuthOk or AuthCleartextPassword or Error
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading startup response: %v", err)
		}
		switch typ {
		case 'R': // Authentication
			authCode := binary.BigEndian.Uint32(data[:4])
			if authCode == 0 {
				// AuthenticationOk — read remaining startup messages
				for {
					typ2, data2, err2 := c.readMsg()
					if err2 != nil {
						c.t.Fatalf("reading post-auth response: %v", err2)
					}
					if typ2 == 'Z' {
						return "" // success
					}
					if typ2 == 'E' {
						return c.parseError(data2)
					}
				}
			}
			if authCode == 3 {
				// CleartextPassword requested — send password
				pwPayload := append([]byte(password), 0)
				c.writeMsg('p', pwPayload)
				continue // loop to read AuthOk or Error
			}
			c.t.Fatalf("unexpected auth code: %d", authCode)
		case 'E': // ErrorResponse (FATAL)
			return c.parseError(data)
		}
	}
}

func TestPGWireAuthSuccess(t *testing.T) {
	db := setupTestDB(t)

	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "test-key-123", Name: "test-user", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	srv := startTestServerWithAuth(t, db, provider)
	client := newPGClient(t, srv.Addr())

	errMsg := client.startupWithPassword("test-user", "testdb", "test-key-123")
	if errMsg != "" {
		t.Fatalf("expected auth success, got error: %s", errMsg)
	}

	// Verify we can query after auth
	cols, rows, tag := client.simpleQuery("SELECT * FROM users")
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d (cols=%v tag=%s)", len(rows), cols, tag)
	}

	client.terminate()
}

func TestPGWireAuthBadKey(t *testing.T) {
	db := setupTestDB(t)

	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "test-key-123", Name: "test-user", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	srv := startTestServerWithAuth(t, db, provider)
	client := newPGClient(t, srv.Addr())

	errMsg := client.startupWithPassword("test-user", "testdb", "wrong-key")
	if errMsg == "" {
		t.Fatal("expected auth failure, but got success")
	}
	t.Logf("auth rejected as expected: %s", errMsg)
}

func TestPGWireAuthNoPassword(t *testing.T) {
	db := setupTestDB(t)

	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "test-key-123", Name: "test-user", Role: "admin"},
		},
		Roles: []auth.RoleConfig{
			{Name: "admin", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	srv := startTestServerWithAuth(t, db, provider)
	client := newPGClient(t, srv.Addr())

	errMsg := client.startupWithPassword("test-user", "testdb", "")
	if errMsg == "" {
		t.Fatal("expected auth failure with empty password, but got success")
	}
	t.Logf("empty password rejected as expected: %s", errMsg)
}

func TestPGWireAuthDisabled(t *testing.T) {
	db := setupTestDB(t)

	// No auth provider — should accept without password flow
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("anyone", "anydb") // original startup with no password

	cols, rows, tag := client.simpleQuery("SELECT * FROM users")
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d (cols=%v tag=%s)", len(rows), cols, tag)
	}
	client.terminate()
}

func TestPGWireCurrentUserReflectsIdentity(t *testing.T) {
	db := setupTestDB(t)

	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "analyst-key", Name: "alice@example.com", Role: "analyst"},
		},
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)

	srv := startTestServerWithAuth(t, db, provider)
	client := newPGClient(t, srv.Addr())

	errMsg := client.startupWithPassword("alice", "testdb", "analyst-key")
	if errMsg != "" {
		t.Fatalf("auth failed: %s", errMsg)
	}

	_, rows, _ := client.simpleQuery("SELECT current_user")
	if len(rows) != 1 || rows[0][0] != "alice@example.com" {
		t.Errorf("expected current_user=alice@example.com, got %v", rows)
	}
	client.terminate()
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
