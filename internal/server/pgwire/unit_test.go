package pgwire

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

// --- Pure function tests ---

func TestTableOID(t *testing.T) {
	// Deterministic: same name always returns the same OID
	oid1 := tableOID("events")
	oid2 := tableOID("events")
	if oid1 != oid2 {
		t.Errorf("tableOID not deterministic: %d vs %d", oid1, oid2)
	}
	// Different names produce different OIDs
	oid3 := tableOID("users")
	if oid1 == oid3 {
		t.Error("different table names should produce different OIDs")
	}
	// OID should be >= 16384 (user table range)
	if oid1 < 16384 {
		t.Errorf("OID should be >= 16384, got %d", oid1)
	}
}

func TestPgTypeOID(t *testing.T) {
	tests := []struct {
		typeName string
		want     int
	}{
		{"INT32", 23},
		{"INT64", 20},
		{"FLOAT32", 700},
		{"FLOAT64", 701},
		{"BOOLEAN", 16},
		{"BOOL", 16},
		{"TIMESTAMP", 1114},
		{"DATE", 1082},
		{"VARCHAR", 25},   // default = text
		{"STRING", 25},    // default = text
		{"UNKNOWN", 25},   // default = text
		{"int32", 23},     // case insensitive
		{"float64", 701},  // case insensitive
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			got := pgTypeOID(tt.typeName)
			if got != tt.want {
				t.Errorf("pgTypeOID(%q) = %d, want %d", tt.typeName, got, tt.want)
			}
		})
	}
}

func TestPgTypeSize(t *testing.T) {
	tests := []struct {
		oid  int
		want int16
	}{
		{16, 1},    // bool
		{21, 2},    // int2
		{23, 4},    // int4
		{20, 8},    // int8
		{700, 4},   // float4
		{701, 8},   // float8
		{1082, 4},  // date
		{1114, 8},  // timestamp
		{25, -1},   // text (variable)
		{0, -1},    // unknown (variable)
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("oid_%d", tt.oid), func(t *testing.T) {
			got := pgTypeSize(tt.oid)
			if got != tt.want {
				t.Errorf("pgTypeSize(%d) = %d, want %d", tt.oid, got, tt.want)
			}
		})
	}
}

func TestPgFormatType(t *testing.T) {
	tests := []struct {
		typeName string
		want     string
	}{
		{"INT32", "integer"},
		{"INT64", "bigint"},
		{"FLOAT32", "real"},
		{"FLOAT64", "double precision"},
		{"BOOLEAN", "boolean"},
		{"BOOL", "boolean"},
		{"TIMESTAMP", "timestamp without time zone"},
		{"DATE", "date"},
		{"DECIMAL", "numeric"},
		{"VARCHAR", "text"},
		{"UNKNOWN", "text"},
	}
	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			got := pgFormatType(tt.typeName)
			if got != tt.want {
				t.Errorf("pgFormatType(%q) = %q, want %q", tt.typeName, got, tt.want)
			}
		})
	}
}

func TestBoolStr(t *testing.T) {
	if boolStr(true) != "t" {
		t.Errorf("boolStr(true) = %q, want \"t\"", boolStr(true))
	}
	if boolStr(false) != "f" {
		t.Errorf("boolStr(false) = %q, want \"f\"", boolStr(false))
	}
}

func TestIsCommandSQL(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"SET client_encoding = 'UTF8'", true},
		{"RESET ALL", true},
		{"BEGIN", true},
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"  SET x = y", true},
		{"SELECT 1", false},
		{"INSERT INTO t VALUES(1)", false},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got := isCommandSQL(tt.sql)
			if got != tt.want {
				t.Errorf("isCommandSQL(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

func TestExtractParamValue(t *testing.T) {
	tests := []struct {
		name       string
		normalized string
		field      string
		want       string
	}{
		{"quoted_value", "WHERE RELNAME = 'client_traffic' AND FOO", "RELNAME", "client_traffic"},
		{"no_match", "WHERE FOO = 'bar'", "RELNAME", ""},
		{"no_quote", "WHERE RELNAME = abc", "RELNAME", ""},
		{"equals_no_space", "WHERE RELNAME ='test'", "RELNAME", "test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractParamValue(tt.normalized, tt.field)
			if got != tt.want {
				t.Errorf("extractParamValue(%q, %q) = %q, want %q", tt.normalized, tt.field, got, tt.want)
			}
		})
	}
}

func TestExtractSelectColumns(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			"basic", "SELECT a, b, c FROM t",
			[]string{"a", "b", "c"},
		},
		{
			"aliases", "SELECT a AS x, b AS y FROM t",
			[]string{"x", "y"},
		},
		{
			"dotted", "SELECT t.oid, t.name FROM t",
			[]string{"oid", "name"},
		},
		{
			"no_from", "SELECT 1, 'hello'",
			[]string{"1", "'hello'"},
		},
		{
			"not_select", "INSERT INTO t VALUES(1)",
			nil,
		},
		{
			"quoted_alias", `SELECT a AS "MyCol" FROM t`,
			[]string{"MyCol"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractSelectColumns(tt.sql)
			if len(got) != len(tt.want) {
				t.Errorf("expected %d columns, got %d: %v", len(tt.want), len(got), got)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("column %d: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseStartupParams(t *testing.T) {
	// Build params: user\0wadjet\0database\0mydb\0\0
	var data []byte
	data = append(data, "user"...)
	data = append(data, 0)
	data = append(data, "wadjet"...)
	data = append(data, 0)
	data = append(data, "database"...)
	data = append(data, 0)
	data = append(data, "mydb"...)
	data = append(data, 0)
	data = append(data, 0) // terminator

	params := parseStartupParams(data)
	if params["user"] != "wadjet" {
		t.Errorf("expected user=wadjet, got %q", params["user"])
	}
	if params["database"] != "mydb" {
		t.Errorf("expected database=mydb, got %q", params["database"])
	}
}

func TestReadCString(t *testing.T) {
	// With null terminator
	got := readCString([]byte("hello\x00world"))
	if got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
	// Without null terminator (returns entire data)
	got = readCString([]byte("nothinghere"))
	if got != "nothinghere" {
		t.Errorf("expected 'nothinghere', got %q", got)
	}
	// Empty
	got = readCString([]byte{0})
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestAppendBinaryValue(t *testing.T) {
	tests := []struct {
		name string
		val  any
	}{
		{"bool_true", true},
		{"bool_false", false},
		{"int32", int32(42)},
		{"int64", int64(9999)},
		{"int", 123},
		{"float32", float32(3.14)},
		{"float64", float64(2.718)},
		{"string", "hello"},
		{"other", []byte{1, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := appendBinaryValue(nil, tt.val)
			if len(buf) == 0 {
				t.Error("expected non-empty buffer")
			}
			// First 4 bytes are the length
			length := int(int32(binary.BigEndian.Uint32(buf[:4])))
			if length != len(buf)-4 {
				t.Errorf("length prefix %d does not match data length %d", length, len(buf)-4)
			}
		})
	}
}

func TestFormatPgValue(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"string", "hello", "hello"},
		{"int", 42, "42"},
		{"float", 3.14, "3.14"},
		{"bool", true, "true"},
		{"nil_default", nil, "<nil>"},
		{"array", []any{"a", "b", "c"}, "[a, b, c]"},
		{"empty_array", []any{}, "[]"},
		{"nested_array", []any{[]any{1, 2}}, "[[1, 2]]"},
		{"map", map[string]any{"k": "v"}, "{k: v}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatPgValue(tt.val)
			if got != tt.want {
				t.Errorf("formatPgValue(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestAppendInt16(t *testing.T) {
	buf := appendInt16(nil, 258) // 0x0102
	if len(buf) != 2 || buf[0] != 1 || buf[1] != 2 {
		t.Errorf("expected [1,2], got %v", buf)
	}
}

func TestAppendInt32(t *testing.T) {
	buf := appendInt32(nil, 0x01020304)
	if len(buf) != 4 || buf[0] != 1 || buf[1] != 2 || buf[2] != 3 || buf[3] != 4 {
		t.Errorf("expected [1,2,3,4], got %v", buf)
	}
}

// --- Protocol-level tests ---

func setupMultiTypeDB(t *testing.T) *wadjet.DB {
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
			{Name: "active", Type: parquet.TypeBool},
			{Name: "visits", Type: parquet.TypeInt64},
		},
	}
	if err := db.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"id": int32(1), "name": "alice", "score": 95.5, "active": true, "visits": int64(100)},
		{"id": int32(2), "name": "bob", "score": 87.3, "active": false, "visits": int64(42)},
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

// --- Synthetic / introspection query tests ---

func TestPGWireSyntheticSelect1(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	cols, rows, tag := client.simpleQuery("SELECT 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if cols[0] != "?column?" {
		t.Errorf("expected column '?column?', got %q", cols[0])
	}
	if rows[0][0] != "1" {
		t.Errorf("expected '1', got %q", rows[0][0])
	}
	if tag != "SELECT 1" {
		t.Errorf("expected tag 'SELECT 1', got %q", tag)
	}
	client.terminate()
}

func TestPGWireVersionQuery(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	cols, rows, _ := client.simpleQuery("SELECT version()")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if cols[0] != "version" {
		t.Errorf("expected column 'version', got %q", cols[0])
	}
	if !contains(rows[0][0], "Wadjet") {
		t.Errorf("expected version to contain 'Wadjet', got %q", rows[0][0])
	}
	client.terminate()
}

func TestPGWireCurrentSchema(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, rows, _ := client.simpleQuery("SELECT current_schema")
	if len(rows) != 1 || rows[0][0] != "public" {
		t.Errorf("expected current_schema=public, got %v", rows)
	}
	client.terminate()
}

func TestPGWireCurrentDatabase(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, rows, _ := client.simpleQuery("SELECT current_database()")
	if len(rows) != 1 || rows[0][0] != "wadjet" {
		t.Errorf("expected current_database=wadjet, got %v", rows)
	}
	client.terminate()
}

func TestPGWireCurrentUser_NoAuth(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, rows, _ := client.simpleQuery("SELECT current_user")
	if len(rows) != 1 || rows[0][0] != "wadjet" {
		t.Errorf("expected current_user=wadjet (no auth), got %v", rows)
	}
	client.terminate()
}

func TestPGWireSessionUser(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, rows, _ := client.simpleQuery("SELECT session_user")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	client.terminate()
}

func TestPGWireShowStatements(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	tests := []struct {
		sql    string
		column string
		want   string
	}{
		{"SHOW TRANSACTION ISOLATION LEVEL", "transaction_isolation", "read committed"},
		{"SHOW standard_conforming_strings", "standard_conforming_strings", "on"},
		{"SHOW server_version", "server_version", "15.0"},
		{"SHOW server_encoding", "server_encoding", "UTF8"},
		{"SHOW client_encoding", "client_encoding", "UTF8"},
		{"SHOW DateStyle", "DateStyle", "ISO, MDY"},
		{"SHOW some_unknown", "setting", ""},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			cols, rows, _ := client.simpleQuery(tt.sql)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row for %q, got %d", tt.sql, len(rows))
			}
			if cols[0] != tt.column {
				t.Errorf("expected column %q, got %q", tt.column, cols[0])
			}
			if rows[0][0] != tt.want {
				t.Errorf("expected %q=%q, got %q", tt.column, tt.want, rows[0][0])
			}
		})
	}
	client.terminate()
}

func TestPGWireTransactionCommands(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// RESET
	_, _, tag := client.simpleQuery("RESET ALL")
	if tag != "SET" {
		t.Errorf("expected SET for RESET, got %q", tag)
	}

	// DISCARD
	_, _, tag = client.simpleQuery("DISCARD ALL")
	if tag != "SET" {
		t.Errorf("expected SET for DISCARD, got %q", tag)
	}

	// DEALLOCATE
	_, _, tag = client.simpleQuery("DEALLOCATE ALL")
	if tag != "SET" {
		t.Errorf("expected SET for DEALLOCATE, got %q", tag)
	}

	// ROLLBACK
	client.simpleQuery("BEGIN")
	_, _, tag = client.simpleQuery("ROLLBACK")
	if tag != "ROLLBACK" {
		t.Errorf("expected ROLLBACK tag, got %q", tag)
	}

	// END (alias for COMMIT)
	client.simpleQuery("BEGIN")
	_, _, tag = client.simpleQuery("END")
	if tag != "COMMIT" {
		t.Errorf("expected COMMIT tag for END, got %q", tag)
	}

	client.terminate()
}

func TestPGWireEmptyQueryString(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, _, tag := client.simpleQuery("")
	if tag != "EMPTY" {
		t.Errorf("expected EMPTY tag for empty query, got %q", tag)
	}

	// Semicolons only
	_, _, tag = client.simpleQuery("  ;  ")
	if tag != "EMPTY" {
		t.Errorf("expected EMPTY tag for semicolons-only query, got %q", tag)
	}

	client.terminate()
}

func TestPGWireUnsupportedMessageType(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Send an unknown message type (e.g., 'Z' which is ReadyForQuery from server)
	client.writeMsg('Z', []byte{0})

	// Should get error + ReadyForQuery back
	for {
		typ, data, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == 'E' {
			msg := client.parseError(data)
			if !contains(msg, "unsupported") {
				t.Errorf("expected 'unsupported' in error, got %q", msg)
			}
		}
		if typ == 'Z' {
			break
		}
	}

	// Connection should still work
	_, rows, _ := client.simpleQuery("SELECT 1")
	if len(rows) != 1 {
		t.Error("expected query to work after unsupported message type")
	}
	client.terminate()
}

func TestPGWireCancelRequest(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send cancel request (version 80877102)
	var payload []byte
	payload = binary.BigEndian.AppendUint32(payload, 80877102)
	payload = binary.BigEndian.AppendUint32(payload, 0) // process ID
	payload = binary.BigEndian.AppendUint32(payload, 0) // secret key

	msg := binary.BigEndian.AppendUint32(nil, uint32(len(payload)+4))
	msg = append(msg, payload...)
	conn.Write(msg)

	// Server should close the connection
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 100)
	_, err = conn.Read(buf)
	// We expect either EOF or timeout since server closes connection
	if err == nil {
		t.Log("cancel request handled, connection may have been kept open")
	}
}

func TestPGWireInvalidProtocol(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send invalid protocol version
	var payload []byte
	payload = binary.BigEndian.AppendUint32(payload, 99999) // invalid version
	msg := binary.BigEndian.AppendUint32(nil, uint32(len(payload)+4))
	msg = append(msg, payload...)
	conn.Write(msg)

	// Server should close the connection
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 100)
	_, err = conn.Read(buf)
	if err == nil {
		t.Log("invalid protocol handled")
	}
}

func TestPGWireInvalidStartupLength(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	conn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send a startup message with too-small length (3 bytes = payload of -1)
	msg := binary.BigEndian.AppendUint32(nil, 3) // length 3 = payload of -1 (invalid)
	conn.Write(msg)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 100)
	_, err = conn.Read(buf)
	if err == nil {
		t.Log("invalid startup length handled")
	}
}

// --- Extended Query protocol: Close ---

func TestPGWireCloseStatement(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Parse a named statement
	var parseBuf []byte
	parseBuf = append(parseBuf, "stmt_to_close"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, "SELECT 1"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	client.writeMsg('P', parseBuf)

	// Close that named statement
	var closeBuf []byte
	closeBuf = append(closeBuf, 'S')
	closeBuf = append(closeBuf, "stmt_to_close"...)
	closeBuf = append(closeBuf, 0)
	client.writeMsg('C', closeBuf)

	// Sync
	client.writeMsg('S', nil)

	// Read responses: ParseComplete + CloseComplete + ReadyForQuery
	var gotParseComplete, gotCloseComplete bool
	for {
		typ, _, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == '1' {
			gotParseComplete = true
		}
		if typ == '3' {
			gotCloseComplete = true
		}
		if typ == 'Z' {
			break
		}
	}
	if !gotParseComplete {
		t.Error("expected ParseComplete")
	}
	if !gotCloseComplete {
		t.Error("expected CloseComplete")
	}
	client.terminate()
}

// --- Extended Query: Execute with SET/BEGIN ---

func TestPGWireExtendedQuerySetCommand(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Use extended query protocol to execute SET
	names, _, _, tag := client.extendedQuery("SET client_encoding = 'UTF8'")
	if tag != "SET" {
		t.Errorf("expected SET tag, got %q", tag)
	}
	_ = names

	// BEGIN via extended query
	_, _, _, tag = client.extendedQuery("BEGIN")
	if tag != "BEGIN" {
		t.Errorf("expected BEGIN tag, got %q", tag)
	}

	// COMMIT via extended query
	_, _, _, tag = client.extendedQuery("COMMIT")
	if tag != "COMMIT" {
		t.Errorf("expected COMMIT tag, got %q", tag)
	}

	// ROLLBACK via extended query
	client.extendedQuery("BEGIN")
	_, _, _, tag = client.extendedQuery("ROLLBACK")
	if tag != "ROLLBACK" {
		t.Errorf("expected ROLLBACK tag, got %q", tag)
	}

	client.terminate()
}

func TestPGWireExtendedQueryEmpty(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Execute with empty SQL
	_, _, _, tag := client.extendedQuery("")
	if tag != "SELECT 0" {
		t.Errorf("expected 'SELECT 0' for empty, got %q", tag)
	}

	client.terminate()
}

// --- Information schema columns ---

func TestPGWireInfoSchemaColumns(t *testing.T) {
	db := setupMultiTypeDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	cols, rows, tag := client.simpleQuery("SELECT column_name FROM information_schema.columns WHERE table_name = 'users'")
	t.Logf("info_schema.columns: cols=%v rows=%d tag=%s", cols, len(rows), tag)
	if len(rows) < 3 {
		t.Errorf("expected at least 3 columns for users table, got %d", len(rows))
	}
	client.terminate()
}

// --- pg_class queries ---

func TestPGWirePgClassRelkind(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Query that should list table names
	cols, rows, tag := client.simpleQuery("SELECT relname FROM pg_class WHERE relkind = 'r'")
	t.Logf("pg_class: cols=%v rows=%d tag=%s", cols, len(rows), tag)
	if len(rows) < 1 {
		t.Error("expected at least 1 table from pg_class")
	}
	client.terminate()
}

// --- Flush message (no-op) ---

func TestPGWireFlushMessage(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Send Flush message ('H')
	client.writeMsg('H', nil)

	// Send a query to verify connection still works
	_, rows, _ := client.simpleQuery("SELECT 1")
	if len(rows) != 1 {
		t.Error("expected query to work after Flush")
	}
	client.terminate()
}

// --- Addr when no listener ---

func TestServerAddr_NoListener(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, Config{}, nil)
	if srv.Addr() != "" {
		t.Errorf("expected empty addr before Start, got %q", srv.Addr())
	}
}

// --- Named prepared statement via Bind ---

func TestPGWireNamedPreparedStatementBind(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Parse a named statement
	var parseBuf []byte
	parseBuf = append(parseBuf, "my_stmt"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, "SELECT id, name FROM users ORDER BY id"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	client.writeMsg('P', parseBuf)

	// Bind: unnamed portal to named statement
	var bindBuf []byte
	bindBuf = append(bindBuf, 0)        // unnamed portal
	bindBuf = append(bindBuf, "my_stmt"...)
	bindBuf = append(bindBuf, 0)        // statement name
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 format codes
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 parameters
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 result format codes
	client.writeMsg('B', bindBuf)

	// Describe portal
	var descBuf []byte
	descBuf = append(descBuf, 'P')
	descBuf = append(descBuf, 0) // unnamed portal
	client.writeMsg('D', descBuf)

	// Execute
	var execBuf []byte
	execBuf = append(execBuf, 0) // unnamed portal
	execBuf = binary.BigEndian.AppendUint32(execBuf, 0)
	client.writeMsg('E', execBuf)

	// Sync
	client.writeMsg('S', nil)

	var rowCount int
	for {
		typ, _, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == 'D' {
			rowCount++
		}
		if typ == 'Z' {
			break
		}
	}
	if rowCount != 3 {
		t.Errorf("expected 3 rows, got %d", rowCount)
	}
	client.terminate()
}

// --- Extended query with parameter binding ---

func TestPGWireBindWithParameters(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Parse statement with placeholder
	var parseBuf []byte
	parseBuf = append(parseBuf, 0) // unnamed
	parseBuf = append(parseBuf, "SELECT oid FROM pg_type WHERE oid = $1"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	client.writeMsg('P', parseBuf)

	// Bind with parameter value "23" (int4)
	var bindBuf []byte
	bindBuf = append(bindBuf, 0) // unnamed portal
	bindBuf = append(bindBuf, 0) // unnamed statement
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 format codes for params
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 1) // 1 parameter
	paramVal := []byte("23")
	bindBuf = binary.BigEndian.AppendUint32(bindBuf, uint32(len(paramVal)))
	bindBuf = append(bindBuf, paramVal...)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 result format codes
	client.writeMsg('B', bindBuf)

	// Describe portal
	var descBuf []byte
	descBuf = append(descBuf, 'P')
	descBuf = append(descBuf, 0)
	client.writeMsg('D', descBuf)

	// Execute
	var execBuf []byte
	execBuf = append(execBuf, 0)
	execBuf = binary.BigEndian.AppendUint32(execBuf, 0)
	client.writeMsg('E', execBuf)

	// Sync
	client.writeMsg('S', nil)

	var gotRows int
	for {
		typ, _, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == 'D' {
			gotRows++
		}
		if typ == 'Z' {
			break
		}
	}

	if gotRows != 1 {
		t.Errorf("expected 1 row for oid=23, got %d", gotRows)
	}
	client.terminate()
}

// --- pg_attribute queries (handleAttributeQuery) ---

func TestPGWireAttributeQuery(t *testing.T) {
	db := setupMultiTypeDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Query pg_attribute - should return column metadata
	cols, rows, tag := client.simpleQuery("SELECT attname, format_type FROM pg_attribute WHERE attrelid = 'users'")
	t.Logf("pg_attribute: cols=%v rows=%d tag=%s", cols, len(rows), tag)
	if len(rows) < 3 {
		t.Errorf("expected at least 3 attribute rows for users table, got %d", len(rows))
	}
	client.terminate()
}

func TestPGWireAttributeQueryByOID(t *testing.T) {
	db := setupMultiTypeDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Query pg_attribute by OID - use the deterministic OID for "users"
	oid := fmt.Sprintf("%d", tableOID("users"))
	query := fmt.Sprintf("SELECT attname FROM pg_attribute WHERE attrelid = '%s'", oid)
	_, rows, _ := client.simpleQuery(query)
	if len(rows) < 3 {
		t.Errorf("expected at least 3 attribute rows by OID, got %d", len(rows))
	}
	client.terminate()
}

// --- pg_class specific table lookup ---

func TestPGWirePgClassOIDLookup(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Look up OID for a specific table
	_, rows, _ := client.simpleQuery("SELECT oid FROM pg_class WHERE relname = 'users'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for OID lookup, got %d", len(rows))
	}
	oid := rows[0][0]
	if oid == "" {
		t.Error("expected non-empty OID")
	}

	// Reverse OID lookup
	query := fmt.Sprintf("SELECT relname FROM pg_class WHERE oid = '%s'", oid)
	_, rows2, _ := client.simpleQuery(query)
	if len(rows2) != 1 || rows2[0][0] != "users" {
		t.Errorf("expected relname=users from reverse OID lookup, got %v", rows2)
	}

	client.terminate()
}

func TestPGWirePgClassNonexistentTable(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Look up OID for nonexistent table - should return 0 rows
	_, rows, tag := client.simpleQuery("SELECT oid FROM pg_class WHERE relname = 'nonexistent'")
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
	if tag != "SELECT 0" {
		t.Errorf("expected SELECT 0, got %q", tag)
	}
	client.terminate()
}

func TestPGWirePgClassReverseOIDNotFound(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Reverse OID lookup with nonexistent OID
	_, rows, tag := client.simpleQuery("SELECT relname FROM pg_class WHERE oid = '99999999'")
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
	if tag != "SELECT 0" {
		t.Errorf("expected SELECT 0, got %q", tag)
	}
	client.terminate()
}

// --- pg_namespace ---

func TestPGWirePgNamespace(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	cols, rows, tag := client.simpleQuery("SELECT nspname FROM pg_namespace")
	t.Logf("pg_namespace: cols=%v rows=%v tag=%s", cols, rows, tag)
	if len(rows) != 1 || rows[0][0] != "public" {
		t.Errorf("expected nspname=public, got %v", rows)
	}
	client.terminate()
}

// --- pg_index/pg_constraint (empty results) ---

func TestPGWirePgIndex(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, rows, tag := client.simpleQuery("SELECT indexrelid FROM pg_index WHERE indrelid = 12345")
	if len(rows) != 0 {
		t.Errorf("expected 0 rows from pg_index, got %d", len(rows))
	}
	if tag != "SELECT 0" {
		t.Errorf("expected SELECT 0, got %q", tag)
	}
	client.terminate()
}

func TestPGWirePgConstraint(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, rows, tag := client.simpleQuery("SELECT conname FROM pg_constraint WHERE conrelid = 12345")
	if len(rows) != 0 {
		t.Errorf("expected 0 rows from pg_constraint, got %d", len(rows))
	}
	if tag != "SELECT 0" {
		t.Errorf("expected SELECT 0, got %q", tag)
	}
	client.terminate()
}

// --- pg_type specific OID lookup ---

func TestPGWirePgTypeSpecificOID(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Look up a specific type by OID
	_, rows, _ := client.simpleQuery("SELECT oid, typname FROM pg_type WHERE oid = '23'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for oid=23, got %d", len(rows))
	}
	if rows[0][1] != "int4" {
		t.Errorf("expected typname=int4, got %q", rows[0][1])
	}
	client.terminate()
}

// --- DML commands through pgwire ---

func TestPGWireDMLInsert(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, _, tag := client.simpleQuery("INSERT INTO users (id, name, score) VALUES (4, 'dave', 88.0)")
	if !contains(tag, "INSERT") {
		t.Errorf("expected INSERT tag, got %q", tag)
	}
	client.terminate()
}

// --- Describe for empty/command SQL via extended query ---

func TestPGWireDescribeCommandSQL(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Parse a SET command
	var parseBuf []byte
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, "SET client_encoding = 'UTF8'"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	client.writeMsg('P', parseBuf)

	// Describe statement
	var descBuf []byte
	descBuf = append(descBuf, 'S')
	descBuf = append(descBuf, 0) // unnamed
	client.writeMsg('D', descBuf)

	// Sync
	client.writeMsg('S', nil)

	var gotParamDesc, gotNoData bool
	for {
		typ, _, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == '1' { // ParseComplete
			continue
		}
		if typ == 't' { // ParameterDescription
			gotParamDesc = true
		}
		if typ == 'n' { // NoData
			gotNoData = true
		}
		if typ == 'Z' {
			break
		}
	}
	if !gotParamDesc {
		t.Error("expected ParameterDescription for SET command")
	}
	if !gotNoData {
		t.Error("expected NoData for SET command describe")
	}
	client.terminate()
}

// --- Describe for pg_catalog queries via extended query ---

func TestPGWireDescribePgCatalogQuery(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Parse a pg_catalog query
	var parseBuf []byte
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, "SELECT oid, typname FROM pg_type"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	client.writeMsg('P', parseBuf)

	// Bind
	var bindBuf []byte
	bindBuf = append(bindBuf, 0) // unnamed portal
	bindBuf = append(bindBuf, 0) // unnamed statement
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0)
	client.writeMsg('B', bindBuf)

	// Describe portal
	var descBuf []byte
	descBuf = append(descBuf, 'P')
	descBuf = append(descBuf, 0) // unnamed
	client.writeMsg('D', descBuf)

	// Sync
	client.writeMsg('S', nil)

	var gotRowDesc bool
	for {
		typ, _, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == 'T' {
			gotRowDesc = true
		}
		if typ == 'Z' {
			break
		}
	}
	if !gotRowDesc {
		t.Error("expected RowDescription for pg_catalog describe")
	}
	client.terminate()
}

// --- Execute with CLOSE command ---

func TestPGWireExtendedQueryCloseCommand(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	_, _, _, tag := client.extendedQuery("CLOSE ALL")
	if tag != "SET" {
		t.Errorf("expected SET tag for CLOSE, got %q", tag)
	}
	client.terminate()
}

// --- Close portal (not statement) ---

func TestPGWireClosePortal(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Close a portal (type 'P' instead of 'S')
	var closeBuf []byte
	closeBuf = append(closeBuf, 'P')
	closeBuf = append(closeBuf, 0) // unnamed portal
	client.writeMsg('C', closeBuf)

	// Sync
	client.writeMsg('S', nil)

	var gotCloseComplete bool
	for {
		typ, _, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == '3' {
			gotCloseComplete = true
		}
		if typ == 'Z' {
			break
		}
	}
	if !gotCloseComplete {
		t.Error("expected CloseComplete for portal close")
	}
	client.terminate()
}

// --- Bind with NULL parameter ---

func TestPGWireBindWithNullParam(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("testuser", "testdb")

	// Parse
	var parseBuf []byte
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, "SELECT 1"...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	client.writeMsg('P', parseBuf)

	// Bind with a NULL parameter (length = -1)
	var bindBuf []byte
	bindBuf = append(bindBuf, 0) // unnamed portal
	bindBuf = append(bindBuf, 0) // unnamed statement
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 format codes
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 1) // 1 parameter
	// NULL param: length = -1
	bindBuf = binary.BigEndian.AppendUint32(bindBuf, 0xFFFFFFFF) // -1
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 result format codes
	client.writeMsg('B', bindBuf)

	// Sync
	client.writeMsg('S', nil)

	// Just verify no crash
	for {
		typ, _, err := client.readMsg()
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
		if typ == 'Z' {
			break
		}
	}
	client.terminate()
}
