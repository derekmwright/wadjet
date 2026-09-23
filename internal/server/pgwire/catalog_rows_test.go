// SPDX-License-Identifier: MIT

package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// assertOneRow checks a simple-protocol answer against the columns and values
// it should carry, and reports a column/value count disagreement as such —
// the shape invariant this layer is built around.
func assertOneRow(t *testing.T, sqlText string, cols []string, rows [][]string, tag string,
	wantCols []string, wantVals []string) {
	t.Helper()
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("%s: %s", sqlText, tag)
	}
	if len(cols) != len(wantCols) {
		t.Fatalf("%s: columns = %v, want %v", sqlText, cols, wantCols)
	}
	for i, w := range wantCols {
		if cols[i] != w {
			t.Errorf("%s: column %d = %q, want %q", sqlText, i, cols[i], w)
		}
	}
	if len(rows) != 1 {
		t.Fatalf("%s: got %d rows %v, want exactly one", sqlText, len(rows), rows)
	}
	if len(rows[0]) != len(wantCols) {
		t.Fatalf("%s: row has %d values for %d columns: %v", sqlText, len(rows[0]), len(wantCols), rows[0])
	}
	for i, w := range wantVals {
		if w == "" {
			continue // value not pinned, only its presence
		}
		if rows[0][i] != w {
			t.Errorf("%s: value %d = %q, want %q", sqlText, i, rows[0][i], w)
		}
	}
}

// TestPgDatabaseListing covers the database picker's source. pg_database
// queries reached the pg_catalog intercept, matched no branch, and came back
// with zero rows — so the picker rendered an empty database list and nothing
// could be selected (issue #305 item 6).
func TestPgDatabaseListing(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	tests := []struct {
		name     string
		sql      string
		wantCols []string
		wantVals []string
	}{
		{
			// pgJDBC's DatabaseMetaData.getCatalogs(), verbatim. The alias is
			// the point: the answer must be labelled TABLE_CAT, which is what
			// the driver reads the value back under.
			name:     "pgJDBC getCatalogs",
			sql:      `SELECT datname AS "TABLE_CAT" FROM pg_catalog.pg_database WHERE datallowconn = true ORDER BY datname`,
			wantCols: []string{"TABLE_CAT"},
			wantVals: []string{"wadjet"},
		},
		{
			name:     "bare datname listing",
			sql:      "SELECT datname FROM pg_database",
			wantCols: []string{"datname"},
			wantVals: []string{"wadjet"},
		},
		{
			name:     "qualified relation name",
			sql:      "SELECT datname FROM pg_catalog.pg_database",
			wantCols: []string{"datname"},
			wantVals: []string{"wadjet"},
		},
		{
			name:     "table alias on every column",
			sql:      "select d.oid, d.datname, d.datallowconn, d.datistemplate from pg_database d",
			wantCols: []string{"oid", "datname", "datallowconn", "datistemplate"},
			wantVals: []string{"", "wadjet", "t", "f"},
		},
		{
			name: "the wider shape a picker asks for",
			sql: "SELECT datname, datdba, encoding, datcollate, datctype, datistemplate, " +
				"datallowconn FROM pg_database WHERE datname = 'wadjet'",
			wantCols: []string{"datname", "datdba", "encoding", "datcollate", "datctype",
				"datistemplate", "datallowconn"},
			wantVals: []string{"wadjet", "10", "6", "en_US.UTF-8", "en_US.UTF-8", "f", "t"},
		},
		{
			name:     "not a template, connections allowed",
			sql:      "SELECT datistemplate, datallowconn FROM pg_database WHERE NOT datistemplate",
			wantCols: []string{"datistemplate", "datallowconn"},
			wantVals: []string{"f", "t"},
		},
		{
			// A column this server does not model comes back NULL rather than
			// a number it made up — and the shape still agrees.
			name:     "unmodelled column is NULL",
			sql:      "SELECT datname, datfrozenxid FROM pg_database",
			wantCols: []string{"datname", "datfrozenxid"},
			wantVals: []string{"wadjet", "NULL"},
		},
		{
			// PostgreSQL 17's pg_database, every column in its order: the
			// relation is the catalog's, not a vocabulary this layer picked.
			name: "select star names the relation's columns",
			sql:  "SELECT * FROM pg_database",
			wantCols: []string{"oid", "datname", "datdba", "encoding", "datlocprovider",
				"datistemplate", "datallowconn", "dathasloginevt", "datconnlimit", "datfrozenxid",
				"datminmxid", "dattablespace", "datcollate", "datctype", "datlocale",
				"daticurules", "datcollversion", "datacl"},
			wantVals: []string{"", "wadjet", "10", "6", "c", "f", "t", "f", "-1", "NULL",
				"NULL", "1663", "en_US.UTF-8", "en_US.UTF-8", "NULL", "NULL", "NULL", "NULL"},
		},
		{
			name:     "distinct",
			sql:      "SELECT DISTINCT datname FROM pg_database",
			wantCols: []string{"datname"},
			wantVals: []string{"wadjet"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols, rows, tag := client.simpleQuery(tt.sql)
			assertOneRow(t, tt.sql, cols, rows, tag, tt.wantCols, tt.wantVals)
		})
	}
}

// TestPgDatabaseOIDIsStable checks the fake OID is the same on every ask —
// a client that caches a catalog OID and looks it up again must find it.
func TestPgDatabaseOIDIsStable(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	var first string
	for i := 0; i < 3; i++ {
		_, rows, tag := client.simpleQuery("SELECT oid FROM pg_database")
		if strings.HasPrefix(tag, "ERROR") || len(rows) != 1 {
			t.Fatalf("attempt %d: rows=%v tag=%s", i, rows, tag)
		}
		if i == 0 {
			first = rows[0][0]
			continue
		}
		if rows[0][0] != first {
			t.Fatalf("oid changed between calls: %q then %q", first, rows[0][0])
		}
	}

	// And it fits where a client puts it. PostgreSQL OIDs are 32 bit; the
	// hash behind this one used to run to the trillions.
	if oid := tableOID("wadjet"); oid < 16384 || oid >= 1<<31 {
		t.Fatalf("tableOID = %d, outside the OID range", oid)
	}
	if tableOID("a") == tableOID("b") {
		t.Fatal("tableOID collides on distinct names")
	}
}

// TestPgDatabaseAggregatesAreQueries: pg_database is a relation, so an
// aggregate over it is answered as one — `count(*)` is 1, the one database —
// and a function this server does not implement is refused by name. The
// canned responder this replaced answered both with no rows (#1251).
func TestPgDatabaseAggregatesAreQueries(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	cols, rows, tag := client.simpleQuery("SELECT count(*) FROM pg_database")
	assertOneRow(t, "count", cols, rows, tag, []string{"count"}, []string{"1"})

	_, _, tag = client.simpleQuery("SELECT pg_size_pretty(pg_database_size(datname)) FROM pg_database")
	if !strings.HasPrefix(tag, "ERROR") || !strings.Contains(tag, "pg_database_size") {
		t.Errorf("pg_database_size: tag %q, want an error naming the function", tag)
	}
}

// TestPgNamespaceListing is the schema picker's half: pg_namespace lists the
// schemas that exist — pg_catalog, information_schema and public — in the
// client's own shape. It used to be one fixed row, "public", whatever the
// statement asked.
func TestPgNamespaceListing(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	tests := []struct {
		name     string
		sql      string
		wantCols []string
		wantRows [][]string
	}{
		{
			name:     "bare nspname",
			sql:      "SELECT nspname FROM pg_namespace ORDER BY nspname",
			wantCols: []string{"nspname"},
			wantRows: [][]string{{"information_schema"}, {"pg_catalog"}, {"public"}},
		},
		{
			name:     "oid alongside nspname",
			sql:      "SELECT oid, nspname FROM pg_namespace ORDER BY oid",
			wantCols: []string{"oid", "nspname"},
			wantRows: [][]string{{"11", "pg_catalog"}, {"2200", "public"}, {"13317", "information_schema"}},
		},
		{
			// pgJDBC's getSchemas(), its quoted labels verbatim.
			name: "pgJDBC getSchemas",
			sql: `SELECT nspname AS "TABLE_SCHEM", NULL AS "TABLE_CATALOG" FROM pg_catalog.pg_namespace ` +
				`ORDER BY "TABLE_SCHEM"`,
			wantCols: []string{"TABLE_SCHEM", "TABLE_CATALOG"},
			wantRows: [][]string{{"information_schema", "NULL"}, {"pg_catalog", "NULL"}, {"public", "NULL"}},
		},
		{
			name:     "aliased relation, filtered",
			sql:      "select n.oid, n.nspname from pg_namespace n where n.nspname = 'public'",
			wantCols: []string{"oid", "nspname"},
			wantRows: [][]string{{"2200", "public"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cols, rows, tag := client.simpleQuery(tt.sql)
			if strings.HasPrefix(tag, "ERROR") {
				t.Fatalf("%s: %s", tt.sql, tag)
			}
			if strings.Join(cols, ",") != strings.Join(tt.wantCols, ",") {
				t.Fatalf("columns = %v, want %v", cols, tt.wantCols)
			}
			if fmt.Sprint(rows) != fmt.Sprint(tt.wantRows) {
				t.Fatalf("rows = %v, want %v", rows, tt.wantRows)
			}
		})
	}
}

// TestCatalogPickerShapeCoherence runs the picker statements through the
// extended protocol, where a disagreement between what Describe promised and
// what Execute sent is what breaks pgJDBC.
func TestCatalogPickerShapeCoherence(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, sqlText := range []string{
		`SELECT datname AS "TABLE_CAT" FROM pg_catalog.pg_database WHERE datallowconn = true ORDER BY datname`,
		"SELECT datname FROM pg_database",
		"select d.oid, d.datname, d.datallowconn from pg_database d",
		"SELECT * FROM pg_database",
		"SELECT count(*) FROM pg_database",
		"SELECT oid, nspname FROM pg_namespace",
		`SELECT nspname AS "TABLE_SCHEM", NULL AS "TABLE_CATALOG" FROM pg_catalog.pg_namespace ORDER BY "TABLE_SCHEM"`,
	} {
		t.Run(sqlText, func(t *testing.T) {
			trace := client.extendedTrace(sqlText)
			t.Logf("%s", traceString(trace))
			assertShapeCoherent(t, sqlText, trace)
		})
	}
}

// TestPgxGetCatalogs drives the database listing through pgx, which holds the
// Describe-time field descriptions and decodes DataRows against them.
func TestPgxGetCatalogs(t *testing.T) {
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

	var catalog string
	err = conn.QueryRow(ctx,
		`SELECT datname AS "TABLE_CAT" FROM pg_catalog.pg_database WHERE datallowconn = true ORDER BY datname`).
		Scan(&catalog)
	if err != nil {
		t.Fatalf("getCatalogs: %v", err)
	}
	if catalog != "wadjet" {
		t.Fatalf("database list = [%q], want [wadjet]", catalog)
	}

	var schema string
	err = conn.QueryRow(ctx, "SELECT nspname FROM pg_namespace WHERE nspname = 'public'").Scan(&schema)
	if err != nil {
		t.Fatalf("getSchemas: %v", err)
	}
	if schema != "public" {
		t.Fatalf("schema list = [%q], want [public]", schema)
	}

	// The picker's two answers agree with what the session functions report,
	// so a client that opens on current_database() finds it in the list.
	var current string
	if err := conn.QueryRow(ctx, "select current_database()").Scan(&current); err != nil {
		t.Fatalf("current_database(): %v", err)
	}
	if current != catalog {
		t.Fatalf("current_database() = %q but pg_database lists %q", current, catalog)
	}
}

// --- SELECT-list parsing ---
