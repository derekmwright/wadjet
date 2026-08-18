package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The extended-protocol invariant, and the bug that motivated it.
//
// DataGrip 2026.1.3 opened a connection and failed with pgJDBC's "Received
// resultset tuples, but no field structure for them". The statement was
// `select current_database(), current_schema(), current_user`. Describe ran
// the query, the query failed (bare current_user parsed as a column
// reference), and the server answered NoData — no result shape. Execute then
// matched the substring CURRENT_SCHEMA and sent a synthetic ONE-column row for
// the THREE-column statement. The driver had been promised nothing and then
// handed tuples.
//
// Two independent decisions produced that: Describe's and Execute's. The tests
// below hold the invariant that closes it — within one Parse/Describe/Bind/
// Execute round the server never sends a DataRow the client has no
// RowDescription for, and the field counts always agree.

// wireMsg is one backend message: its type byte plus whatever the type
// carries that the shape invariant is about.
type wireMsg struct {
	typ    byte
	fields int    // 'T' and 'D': field count; -1 otherwise
	text   string // 'C': command tag; 'E': error message
}

func (m wireMsg) String() string {
	switch m.typ {
	case 'T', 'D':
		return fmt.Sprintf("%c(%d)", m.typ, m.fields)
	case 'C', 'E':
		return fmt.Sprintf("%c(%q)", m.typ, m.text)
	default:
		return string(m.typ)
	}
}

func traceString(trace []wireMsg) string {
	parts := make([]string, len(trace))
	for i, m := range trace {
		parts[i] = m.String()
	}
	return strings.Join(parts, " ")
}

// extendedTrace runs one Parse/Bind/Describe(portal)/Execute/Sync round and
// returns the backend messages in order.
func (c *pgClient) extendedTrace(sql string) []wireMsg {
	c.t.Helper()

	var parseBuf []byte
	parseBuf = append(parseBuf, 0) // unnamed statement
	parseBuf = append(parseBuf, sql...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0) // 0 param types
	c.writeMsg('P', parseBuf)

	var bindBuf []byte
	bindBuf = append(bindBuf, 0)                        // unnamed portal
	bindBuf = append(bindBuf, 0)                        // unnamed statement
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 format codes
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 parameters
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 result format codes
	c.writeMsg('B', bindBuf)

	c.writeMsg('D', []byte{'P', 0}) // Describe portal

	var execBuf []byte
	execBuf = append(execBuf, 0) // unnamed portal
	execBuf = binary.BigEndian.AppendUint32(execBuf, 0)
	c.writeMsg('E', execBuf)

	c.writeMsg('S', nil)

	var trace []wireMsg
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading response for %q: %v", sql, err)
		}
		m := wireMsg{typ: typ, fields: -1}
		switch typ {
		case 'T', 'D':
			if len(data) >= 2 {
				m.fields = int(binary.BigEndian.Uint16(data[:2]))
			}
		case 'C':
			m.text = readCString(data)
		case 'E':
			m.text = c.parseError(data)
		}
		trace = append(trace, m)
		if typ == 'Z' {
			return trace
		}
	}
}

// extendedTraceParams runs Parse/Bind(with parameters)/Describe(statement)/
// Execute/Sync — the shape a JDBC metadata lookup takes, where Describe sees
// the statement with placeholders and Execute sees the bound portal.
func (c *pgClient) extendedTraceParams(sql string, params []string) []wireMsg {
	c.t.Helper()

	var parseBuf []byte
	parseBuf = append(parseBuf, 0) // unnamed statement
	parseBuf = append(parseBuf, sql...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 0)
	c.writeMsg('P', parseBuf)

	// Describe the STATEMENT, before Bind — the placeholders are still in it.
	c.writeMsg('D', []byte{'S', 0})

	var bindBuf []byte
	bindBuf = append(bindBuf, 0)                        // unnamed portal
	bindBuf = append(bindBuf, 0)                        // unnamed statement
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 format codes
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, uint16(len(params)))
	for _, p := range params {
		bindBuf = binary.BigEndian.AppendUint32(bindBuf, uint32(len(p)))
		bindBuf = append(bindBuf, p...)
	}
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 result format codes
	c.writeMsg('B', bindBuf)

	var execBuf []byte
	execBuf = append(execBuf, 0)
	execBuf = binary.BigEndian.AppendUint32(execBuf, 0)
	c.writeMsg('E', execBuf)

	c.writeMsg('S', nil)

	var trace []wireMsg
	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading response for %q: %v", sql, err)
		}
		m := wireMsg{typ: typ, fields: -1}
		switch typ {
		case 'T', 'D':
			if len(data) >= 2 {
				m.fields = int(binary.BigEndian.Uint16(data[:2]))
			}
		case 'C':
			m.text = readCString(data)
		case 'E':
			m.text = c.parseError(data)
		}
		trace = append(trace, m)
		if typ == 'Z' {
			return trace
		}
	}
}

// assertShapeCoherent enforces the invariant on one round's messages.
//
// The result shape for a round is whatever Describe answered: a RowDescription
// ('T') or NoData ('n'). It is answered once. A driver binds its reader to
// that answer, so a later RowDescription is not a second chance — Execute
// contributes tuples, not field structure.
func assertShapeCoherent(t *testing.T, sql string, trace []wireMsg) {
	t.Helper()
	// A statement carrying $N placeholders is the one case where Execute may
	// still contribute the description: Describe runs the statement to learn
	// its shape and a placeholder statement may not run. Everything else must
	// be described in answer to its Describe.
	deferrable := strings.Contains(sql, "$")
	answered := false
	described := -1 // field count; -1 = NoData
	for _, m := range trace {
		switch m.typ {
		case 'T':
			if answered && !(deferrable && described < 0) {
				t.Errorf("%q: a second RowDescription after the shape was already "+
					"answered; trace = %s", sql, traceString(trace))
				return
			}
			answered, described = true, m.fields
		case 'n': // NoData — the statement has no promised result shape
			answered, described = true, -1
		case 'D':
			if !answered || described < 0 {
				t.Errorf("%q: DataRow with no RowDescription for it (pgJDBC: "+
					"\"Received resultset tuples, but no field structure for them\"); trace = %s",
					sql, traceString(trace))
				return
			}
			if m.fields != described {
				t.Errorf("%q: DataRow has %d fields, RowDescription promised %d; trace = %s",
					sql, m.fields, described, traceString(trace))
				return
			}
		}
	}
}

// TestExtendedProtocolShapeCoherence walks the statement shapes a JDBC client
// sends during connection setup and metadata discovery, and asserts that each
// round's tuples fit the field structure the same round described.
func TestExtendedProtocolShapeCoherence(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	statements := []string{
		// The reported statement.
		"select current_database(), current_schema(), current_user",
		// Its one-column relatives, which the introspection layer answers.
		"select current_database()",
		"select current_schema()",
		"select current_user",
		"select version()",
		// Multi-column session queries that route to the engine.
		"select current_database() as a, current_schemas(false) as b",
		"select version() as v",
		"select current_user, current_schema",
		"select current_schema(), 1",
		// DataGrip's uptime probe. It reaches the engine (no FROM, not one of
		// the claimed one-column spellings) and used to fail to parse, which
		// left Describe with NoData.
		"select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time",
		// Real queries.
		"SELECT id, name FROM users ORDER BY id",
		"SELECT COUNT(*) AS cnt FROM users",
		"SELECT 1",
		// Catalog introspection.
		"SELECT relname FROM pg_class WHERE relkind = 'r'",
		"SELECT oid, typname FROM pg_type",
		"SELECT table_name FROM information_schema.tables",
		"SELECT nspname FROM pg_namespace",
		"SELECT datname FROM pg_database",
		"SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database WHERE datallowconn = true ORDER BY datname",
		// Statements with no result set at all.
		"SET extra_float_digits = 3",
		"BEGIN",
		"COMMIT",
		// A statement that cannot be answered by either path.
		"SELECT * FROM no_such_table_at_all",
	}

	for _, sql := range statements {
		t.Run(sql, func(t *testing.T) {
			trace := client.extendedTrace(sql)
			t.Logf("%s", traceString(trace))
			assertShapeCoherent(t, sql, trace)
		})
	}
}

// TestDataGripOpeningSequence replays, verbatim and on one connection, the
// statements DataGrip 2026.1.3 sends when it opens a session. Every one has to
// come back with a well-formed result.
func TestDataGripOpeningSequence(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	type step struct {
		sql      string
		wantCols []string // nil = no result set expected
		wantRows int
	}
	steps := []step{
		{sql: "SET extra_float_digits = 3"},
		{sql: "SET application_name = 'DataGrip 2026.1.3'"},
		{sql: "select version()", wantCols: []string{"version"}, wantRows: 1},
		{
			sql:      "select current_database() as a, current_schemas(false) as b",
			wantCols: []string{"a", "b"},
			wantRows: 1,
		},
		{
			sql:      "select current_database(), current_schema(), current_user",
			wantCols: []string{"current_database", "current_schema", "current_user"},
			wantRows: 1,
		},
		{
			sql:      "SHOW TRANSACTION ISOLATION LEVEL",
			wantCols: []string{"transaction_isolation"},
			wantRows: 1,
		},
		{
			sql:      "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time",
			wantCols: []string{"startup_time"},
			wantRows: 1,
		},
		// The database and schema pickers. Both used to come back empty,
		// which left DataGrip with nothing to select.
		{
			sql:      "SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database WHERE datallowconn = true ORDER BY datname",
			wantCols: []string{"TABLE_CAT"},
			wantRows: 1,
		},
		{
			sql:      "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace ORDER BY TABLE_SCHEM",
			wantCols: []string{"TABLE_SCHEM", "TABLE_CATALOG"},
			wantRows: 1,
		},
	}

	for _, s := range steps {
		t.Run(s.sql, func(t *testing.T) {
			names, _, rows, tag := client.extendedQuery(s.sql)
			t.Logf("cols=%v rows=%v tag=%q", names, rows, tag)
			if strings.HasPrefix(tag, "ERROR") {
				t.Fatalf("statement failed: %s", tag)
			}
			if s.wantCols == nil {
				if len(rows) != 0 {
					t.Errorf("expected no result set, got rows %v", rows)
				}
				return
			}
			if len(names) != len(s.wantCols) {
				t.Fatalf("columns = %v, want %v", names, s.wantCols)
			}
			for i, w := range s.wantCols {
				if names[i] != w {
					t.Errorf("column %d = %q, want %q", i, names[i], w)
				}
			}
			if len(rows) != s.wantRows {
				t.Fatalf("got %d rows, want %d", len(rows), s.wantRows)
			}
			for _, row := range rows {
				if len(row) != len(s.wantCols) {
					t.Errorf("row has %d values, RowDescription promised %d: %v",
						len(row), len(s.wantCols), row)
				}
			}
		})
	}
}

// TestDataGripOpeningSequenceSimpleProtocol runs the same statements through
// the simple protocol, the one psql uses. It shares matchIntrospection with
// the extended path, so the answers must agree between the two.
func TestDataGripOpeningSequenceSimpleProtocol(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, tc := range []struct {
		sql      string
		wantCols []string
		wantRow  []string
	}{
		{
			"select current_database(), current_schema(), current_user",
			[]string{"current_database", "current_schema", "current_user"},
			[]string{"wadjet", "public", "wadjet"},
		},
		{
			"select current_database() as a, current_schemas(false) as b",
			[]string{"a", "b"},
			[]string{"wadjet", "{public}"},
		},
		{
			"select version()",
			[]string{"version"},
			[]string{"PostgreSQL 15.0 (Wadjet analytical query engine)"},
		},
		{
			"SHOW TRANSACTION ISOLATION LEVEL",
			[]string{"transaction_isolation"},
			[]string{"read committed"},
		},
	} {
		cols, rows, tag := client.simpleQuery(tc.sql)
		if strings.HasPrefix(tag, "ERROR") {
			t.Errorf("%q: %s", tc.sql, tag)
			continue
		}
		if len(cols) != len(tc.wantCols) {
			t.Errorf("%q: columns = %v, want %v", tc.sql, cols, tc.wantCols)
			continue
		}
		for i, w := range tc.wantCols {
			if cols[i] != w {
				t.Errorf("%q: column %d = %q, want %q", tc.sql, i, cols[i], w)
			}
		}
		if len(rows) != 1 {
			t.Errorf("%q: got %d rows, want 1", tc.sql, len(rows))
			continue
		}
		for i, w := range tc.wantRow {
			if rows[0][i] != w {
				t.Errorf("%q: value %d = %q, want %q", tc.sql, i, rows[0][i], w)
			}
		}
	}
}

// TestDataGripOpeningSequencePgx drives the same sequence through pgx, which
// speaks the extended protocol the way pgJDBC does — it holds the statement
// description from Describe and decodes DataRows against it, so a shape
// disagreement surfaces as a driver error rather than a silent mismatch.
func TestDataGripOpeningSequencePgx(t *testing.T) {
	_, srv := setupRealDB(t)
	ctx := context.Background()
	addr := srv.Addr()
	connStr := fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		addr[len("127.0.0.1:"):])

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "SET extra_float_digits = 3"); err != nil {
		t.Fatalf("SET extra_float_digits: %v", err)
	}
	if _, err := conn.Exec(ctx, "SET application_name = 'DataGrip 2026.1.3'"); err != nil {
		t.Fatalf("SET application_name: %v", err)
	}

	var version string
	if err := conn.QueryRow(ctx, "select version()").Scan(&version); err != nil {
		t.Fatalf("select version(): %v", err)
	}
	t.Logf("version() = %s", version)

	var a, b string
	err = conn.QueryRow(ctx, "select current_database() as a, current_schemas(false) as b").Scan(&a, &b)
	if err != nil {
		t.Fatalf("select current_database() as a, current_schemas(false) as b: %v", err)
	}
	t.Logf("current_database()=%s current_schemas(false)=%s", a, b)
	if a != "wadjet" {
		t.Errorf("current_database() = %q, want wadjet", a)
	}
	if b != "{public}" {
		t.Errorf("current_schemas(false) = %q, want {public}", b)
	}

	// The reported statement.
	rows, err := conn.Query(ctx, "select current_database(), current_schema(), current_user")
	if err != nil {
		t.Fatalf("DataGrip opening query: %v", err)
	}
	defer rows.Close()
	fds := rows.FieldDescriptions()
	if len(fds) != 3 {
		t.Fatalf("RowDescription has %d fields, want 3", len(fds))
	}
	for i, want := range []string{"current_database", "current_schema", "current_user"} {
		if fds[i].Name != want {
			t.Errorf("field %d = %q, want %q", i, fds[i].Name, want)
		}
	}
	n := 0
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("Values: %v", err)
		}
		if len(vals) != 3 {
			t.Fatalf("row has %d values, want 3: %v", len(vals), vals)
		}
		t.Logf("row: %v", vals)
		if vals[0] != "wadjet" || vals[1] != "public" || vals[2] != "wadjet" {
			t.Errorf("row = %v, want [wadjet public wadjet]", vals)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	rows.Close()
	if n != 1 {
		t.Fatalf("got %d rows, want 1", n)
	}

	var isolation string
	if err := conn.QueryRow(ctx, "SHOW TRANSACTION ISOLATION LEVEL").Scan(&isolation); err != nil {
		t.Fatalf("SHOW TRANSACTION ISOLATION LEVEL: %v", err)
	}
	if isolation != "read committed" {
		t.Errorf("transaction_isolation = %q, want %q", isolation, "read committed")
	}

	// The uptime probe. DataGrip binds it to a number, so the value has to
	// arrive as one — a text column here fails in the driver, not the server.
	const startupSQL = "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time"
	startupRows, err := conn.Query(ctx, startupSQL)
	if err != nil {
		t.Fatalf("%s: %v", startupSQL, err)
	}
	sfds := startupRows.FieldDescriptions()
	if len(sfds) != 1 || sfds[0].Name != "startup_time" {
		startupRows.Close()
		t.Fatalf("startup_time RowDescription = %+v, want one field named startup_time", sfds)
	}
	startupRows.Close()

	var startup float64
	if err := conn.QueryRow(ctx, startupSQL).Scan(&startup); err != nil {
		t.Fatalf("scanning startup_time as a number: %v", err)
	}
	t.Logf("startup_time = %v", startup)
	if delta := float64(time.Now().Unix()) - startup; delta < 0 || delta > 300 {
		t.Errorf("startup_time %v is %vs from now — not this process's start", startup, delta)
	}

	// The database picker. An empty list here is what left DataGrip with no
	// database to select.
	var cat string
	err = conn.QueryRow(ctx,
		"SELECT datname AS TABLE_CAT FROM pg_catalog.pg_database WHERE datallowconn = true ORDER BY datname").
		Scan(&cat)
	if err != nil {
		t.Fatalf("getCatalogs: %v", err)
	}
	if cat != "wadjet" {
		t.Errorf("database list = [%q], want [wadjet]", cat)
	}

	// The schema picker alongside it.
	var schem, schemCat *string
	err = conn.QueryRow(ctx,
		"SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace ORDER BY TABLE_SCHEM").
		Scan(&schem, &schemCat)
	if err != nil {
		t.Fatalf("getSchemas: %v", err)
	}
	if schem == nil || *schem != "public" {
		t.Errorf("schema list = [%v], want [public]", schem)
	}
}

// TestAliasedSessionQueryKeepsClientLabels checks that a session query the
// client labelled itself comes back under those labels. The synthetic answers
// carry fixed column names, so they must only claim the bare spellings — an
// aliased or multi-column statement is a real query and the engine answers it.
func TestAliasedSessionQueryKeepsClientLabels(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, tc := range []struct {
		sql      string
		wantCols []string
		wantVals []string
	}{
		{"select version() as v", []string{"v"}, []string{"PostgreSQL 15.0 (Wadjet analytical query engine)"}},
		{"select current_user as whoami", []string{"whoami"}, []string{"wadjet"}},
		{
			"select current_database() as a, current_schemas(false) as b",
			[]string{"a", "b"},
			[]string{"wadjet", "{public}"},
		},
	} {
		names, _, rows, tag := client.extendedQuery(tc.sql)
		if strings.HasPrefix(tag, "ERROR") {
			t.Errorf("%q: %s", tc.sql, tag)
			continue
		}
		if len(names) != len(tc.wantCols) {
			t.Errorf("%q: columns = %v, want %v", tc.sql, names, tc.wantCols)
			continue
		}
		for i, w := range tc.wantCols {
			if names[i] != w {
				t.Errorf("%q: column %d = %q, want %q", tc.sql, i, names[i], w)
			}
		}
		if len(rows) != 1 {
			t.Errorf("%q: got %d rows, want 1", tc.sql, len(rows))
			continue
		}
		for i, w := range tc.wantVals {
			if rows[0][i] != w {
				t.Errorf("%q: value %d = %q, want %q", tc.sql, i, rows[0][i], w)
			}
		}
	}
}

// TestWriteMentioningCatalogNameExecutes pins that the introspection layer
// only claims statements that read. It matches on statement text, so a write
// whose literal mentions a catalog name was swallowed: the client got an empty
// result set and a success tag, and the row was never written.
func TestWriteMentioningCatalogNameExecutes(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "msg", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "audit", schema, nil); err != nil {
		t.Fatal(err)
	}
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, sql := range []string{
		"INSERT INTO audit (msg) VALUES ('pg_class scan failed')",
		"INSERT INTO audit (msg) VALUES ('information_schema unavailable')",
	} {
		_, _, tag := client.simpleQuery(sql)
		if !strings.HasPrefix(tag, "INSERT") {
			t.Errorf("%q: tag = %q, want an INSERT tag (the write was swallowed)", sql, tag)
		}
	}

	cols, rows, tag := client.simpleQuery("SELECT msg FROM audit ORDER BY msg")
	t.Logf("readback cols=%v rows=%v tag=%q", cols, rows, tag)
	if len(rows) != 2 {
		t.Fatalf("readback returned %d rows, want 2 — writes were lost", len(rows))
	}
}

// TestSessionFunctionLabels pins that each bare session spelling comes back
// under its own label. The synthetic answers reported `current_user` for
// session_user and current_role, and `current_database` for current_catalog,
// which disagrees with both PostgreSQL and the engine's label for the same
// function.
func TestSessionFunctionLabels(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for sql, want := range map[string]string{
		"select current_user":       "current_user",
		"select session_user":       "session_user",
		"select user":               "user",
		"select current_role":       "current_role",
		"select current_catalog":    "current_catalog",
		"select current_schema":     "current_schema",
		"select current_database()": "current_database",
	} {
		names, _, rows, tag := client.extendedQuery(sql)
		if strings.HasPrefix(tag, "ERROR") {
			t.Errorf("%q: %s", sql, tag)
			continue
		}
		if len(names) != 1 || names[0] != want {
			t.Errorf("%q: columns = %v, want [%s]", sql, names, want)
		}
		if len(rows) != 1 {
			t.Errorf("%q: got %d rows, want 1", sql, len(rows))
		}
	}
}

// TestCatalogFallbackDescribesTheStatementNotThePortal covers the last place
// where the two ends of a round could still disagree: the pg_index fallback
// took its columns from the connection's portal instead of the statement it
// was answering. At a statement Describe the portal still holds the PREVIOUS
// query, so the round described one width and executed another — which the
// shape guard reports as an error on a perfectly valid query.
func TestCatalogFallbackDescribesTheStatementNotThePortal(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	// Leave a two-column query in the portal.
	if names, _, _, tag := client.extendedQuery("SELECT id, name FROM users"); len(names) != 2 {
		t.Fatalf("setup query: cols=%v tag=%q", names, tag)
	}

	// Now a one-column pg_index lookup, described as a STATEMENT before Bind.
	trace := client.extendedTraceParams(
		"SELECT i.indexrelid FROM pg_index i WHERE i.indrelid = $1", []string{"users"})
	t.Logf("%s", traceString(trace))
	assertShapeCoherent(t, "pg_index lookup", trace)

	for _, m := range trace {
		if m.typ == 'E' {
			t.Fatalf("pg_index lookup errored: %s; trace = %s", m.text, traceString(trace))
		}
		if m.typ == 'T' && m.fields != 1 {
			t.Errorf("RowDescription has %d fields, want 1 (the statement's own SELECT list); trace = %s",
				m.fields, traceString(trace))
		}
	}
}

// TestExecuteErrorsWhenNothingCanAnswer covers the other side of the
// invariant: a statement that neither the engine nor the introspection layer
// can answer gets NoData at Describe and an ErrorResponse at Execute — never
// tuples.
func TestExecuteErrorsWhenNothingCanAnswer(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	trace := client.extendedTrace("SELECT definitely_not_a_column FROM users")
	t.Logf("%s", traceString(trace))

	var sawNoData, sawError bool
	for _, m := range trace {
		switch m.typ {
		case 'n':
			sawNoData = true
		case 'E':
			sawError = true
		case 'D':
			t.Errorf("DataRow sent for an unanswerable statement; trace = %s", traceString(trace))
		}
	}
	if !sawNoData {
		t.Errorf("expected NoData from Describe; trace = %s", traceString(trace))
	}
	if !sawError {
		t.Errorf("expected ErrorResponse from Execute; trace = %s", traceString(trace))
	}

	// The connection stays usable, and the failure does not leak into the
	// next statement.
	names, _, rows, tag := client.extendedQuery("SELECT id FROM users ORDER BY id LIMIT 1")
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("statement after a failure reported %s", tag)
	}
	if len(names) != 1 || len(rows) != 1 {
		t.Errorf("after a failed statement: cols=%v rows=%v", names, rows)
	}
}

// TestStaleDescribeErrorDoesNotReplay pins the cache-staleness half of the
// report. When Execute answers from the introspection layer it must drop any
// Describe-time failure cached for the connection: the client already got its
// answer, and a leftover error would surface as the *next* statement's error.
func TestStaleDescribeErrorDoesNotReplay(t *testing.T) {
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rc := &recordConn{}
	c := &pgConn{conn: rc, db: db, stmts: map[string]string{}}

	// A Describe-time failure is cached for the connection...
	c.describeErr = errors.New("sentinel: stale describe failure")
	// ...and Execute then answers the portal from the introspection layer.
	c.portalSQL = "select version()"
	c.handleExecute(nil)

	wire := rc.buf.Bytes()
	if got := countMsgs(wire, 'E'); got != 0 {
		t.Errorf("ErrorResponse messages = %d, want 0", got)
	}
	if bytes.Contains(wire, []byte("sentinel")) {
		t.Errorf("stale Describe error replayed into an answered statement; wire = %q", wire)
	}
	if got := countMsgs(wire, 'D'); got != 1 {
		t.Errorf("DataRow messages = %d, want 1", got)
	}
	if c.describeErr != nil {
		t.Errorf("describeErr not cleared by the introspection path: %v", c.describeErr)
	}

	// The next statement on this connection must not inherit anything.
	rc.buf.Reset()
	c.portalSQL = "SELECT current_schema()"
	c.handleExecute(nil)
	if got := countMsgs(rc.buf.Bytes(), 'E'); got != 0 {
		t.Errorf("next statement got %d ErrorResponse messages, want 0; wire = %q",
			got, rc.buf.Bytes())
	}
}

// TestParameterizedCatalogLookupShape covers the case where Describe and
// Execute genuinely see different SQL: Describe answers the statement with its
// placeholders, Bind substitutes the literal, and Execute answers the bound
// portal. The rows must be the bound query's rows and still fit the described
// shape — which is why the Execute path re-resolves a portal whose SQL differs
// from the one Describe cached.
func TestParameterizedCatalogLookupShape(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	trace := client.extendedTraceParams("SELECT oid FROM pg_class WHERE relname = $1", []string{"users"})
	t.Logf("%s", traceString(trace))
	assertShapeCoherent(t, "pg_class OID lookup", trace)

	dataRows := 0
	for _, m := range trace {
		if m.typ == 'D' {
			dataRows++
		}
	}
	if dataRows != 1 {
		t.Errorf("bound lookup returned %d rows, want 1 (the OID of `users`); trace = %s",
			dataRows, traceString(trace))
	}
}

// TestParameterizedStatementDescribesBeforeTuples covers the statement shape a
// JDBC PreparedStatement sends. Describe sees $1 — which does not parse, so
// running the statement to learn its shape fails — and answered NoData; the
// portal then executed and produced tuples the client had no structure for.
// Describe now discovers the shape with NULL in the parameters' place, so the
// RowDescription goes out in answer to the Describe, where a driver that ties
// descriptions to its pending Describe requests (pgJDBC) will pick it up.
func TestParameterizedStatementDescribesBeforeTuples(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	trace := client.extendedTraceParams("SELECT id, name FROM users WHERE name = $1", []string{"bob"})
	t.Logf("%s", traceString(trace))
	assertShapeCoherent(t, "parameterized select", trace)

	// The description answers the Describe, before Bind's BindComplete.
	var describeIdx, bindIdx, dataIdx = -1, -1, -1
	for i, m := range trace {
		switch {
		case m.typ == 'T' && describeIdx < 0:
			describeIdx = i
		case m.typ == '2' && bindIdx < 0:
			bindIdx = i
		case m.typ == 'D' && dataIdx < 0:
			dataIdx = i
		}
	}
	if describeIdx < 0 {
		t.Fatalf("no RowDescription for a parameterized statement; trace = %s", traceString(trace))
	}
	if bindIdx >= 0 && describeIdx > bindIdx {
		t.Errorf("RowDescription came after BindComplete, so it did not answer the Describe; trace = %s",
			traceString(trace))
	}
	if dataIdx < 0 {
		t.Fatalf("bound portal returned no rows; trace = %s", traceString(trace))
	}
	if trace[dataIdx].fields != trace[describeIdx].fields {
		t.Errorf("DataRow has %d fields, RowDescription promised %d",
			trace[dataIdx].fields, trace[describeIdx].fields)
	}

	// And the rows are the bound query's rows, not the shape query's.
	names, _, rows, tag := client.extendedQuery("SELECT id, name FROM users WHERE name = 'bob'")
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("control query: %s", tag)
	}
	if len(rows) != 1 || len(names) != 2 {
		t.Fatalf("control query returned cols=%v rows=%v", names, rows)
	}
}

// TestExecuteSendsNoRowDescription pins that the introspection path obeys the
// extended protocol: Describe carries the RowDescription, Execute carries
// tuples. A second RowDescription at Execute is what a driver that already
// holds a description will not expect.
func TestExecuteSendsNoRowDescription(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, sql := range []string{
		"select version()",
		"select current_user",
		"SHOW TRANSACTION ISOLATION LEVEL",
		"SELECT oid, typname FROM pg_type",
	} {
		trace := client.extendedTrace(sql)
		descriptions := 0
		for _, m := range trace {
			if m.typ == 'T' {
				descriptions++
			}
		}
		if descriptions != 1 {
			t.Errorf("%q: %d RowDescription messages, want exactly 1; trace = %s",
				sql, descriptions, traceString(trace))
		}
	}
}

// TestDescribeAndExecuteAgreeOnColumns checks the pairing directly: for every
// introspection-answered statement, the columns Describe reports are the
// columns Execute produces values for.
func TestDescribeAndExecuteAgreeOnColumns(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, sql := range []string{
		"select version()",
		"select current_user",
		"select current_schema",
		"select current_database()",
		"SELECT 1",
		"SHOW server_version",
		"SELECT relname FROM pg_class WHERE relkind = 'r'",
		"SELECT oid, typname, typlen, typtype, typnamespace FROM pg_type",
		"SELECT table_name FROM information_schema.tables",
		"SELECT attname, format_type FROM pg_attribute WHERE attrelid = 'users'",
	} {
		names, _, rows, tag := client.extendedQuery(sql)
		if strings.HasPrefix(tag, "ERROR") {
			t.Errorf("%q: %s", sql, tag)
			continue
		}
		if len(names) == 0 {
			t.Errorf("%q: no RowDescription", sql)
			continue
		}
		for i, row := range rows {
			if len(row) != len(names) {
				t.Errorf("%q: row %d has %d values, RowDescription named %d columns (%v)",
					sql, i, len(row), len(names), names)
			}
		}
	}
}
