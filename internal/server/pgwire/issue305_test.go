package pgwire

// Regression coverage for the issue #305 punch list — the pgwire
// introspection-layer compatibility audit. Each test names the item it pins.
// Items already retired by earlier work (typed OIDs, bind-by-type #365,
// ParameterDescription, SELECT-list shaping) are pinned by the existing
// suites; this file covers the residue closed at disposition time.

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setupTwoTableDB extends setupRealDB with a second table so that a
// per-table introspection answer is distinguishable from an all-tables one.
func setupTwoTableDB(t *testing.T) *Server {
	t.Helper()
	db, srv := setupRealDB(t)
	ctx := context.Background()
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "event_id", Type: parquet.TypeInt64},
			{Name: "severity", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("events", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"event_id": int64(1), "severity": "low"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

// Item 4: a numeric, unquoted predicate (pgJDBC inlines attrelid = 16384)
// must scope the pg_attribute answer to that one table — not answer with
// every table's columns as if they were one table's list.
func TestIssue305Item4NumericAttrelidScopesAnswer(t *testing.T) {
	srv := setupTwoTableDB(t)
	db := openPQ(t, srv.Addr())

	oid := tableOID("users")
	rows, err := db.Query(
		"SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = " +
			strconv.Itoa(oid) + " AND attnum > 0")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// users has 5 columns; events has 2. An unscoped answer returns 7.
	if len(names) != 5 {
		t.Fatalf("attrelid = %d answered %d columns %v, want the 5 columns of users",
			oid, len(names), names)
	}
	for _, n := range names {
		if n == "event_id" || n == "severity" {
			t.Fatalf("attrelid = %d answered another table's column %q", oid, n)
		}
	}
}

// Item 7: SHOW consults what SET stored, under the variable's own label, and
// server_version_num is an integer (pgJDBC calls Integer.parseInt on it).
func TestIssue305Item7ShowReflectsSet(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	if _, err := db.Exec(`SET search_path TO analytics, public`); err != nil {
		t.Fatalf("SET: %v", err)
	}
	var val string
	if err := db.QueryRow(`SHOW search_path`).Scan(&val); err != nil {
		t.Fatalf("SHOW search_path: %v", err)
	}
	if val != "analytics, public" {
		t.Errorf("SHOW search_path = %q, want the SET value %q", val, "analytics, public")
	}

	var num string
	if err := db.QueryRow(`SHOW server_version_num`).Scan(&num); err != nil {
		t.Fatalf("SHOW server_version_num: %v", err)
	}
	if _, err := strconv.Atoi(num); err != nil {
		t.Errorf("SHOW server_version_num = %q, not an integer: %v", num, err)
	}

	// An unSET variable with a known default still answers; an unknown one
	// answers empty rather than erroring (kept from the previous behavior).
	var appName string
	if _, err := db.Exec(`SET application_name = 'issue305'`); err != nil {
		t.Fatalf("SET application_name: %v", err)
	}
	if err := db.QueryRow(`SHOW application_name`).Scan(&appName); err != nil {
		t.Fatalf("SHOW application_name: %v", err)
	}
	if appName != "issue305" {
		t.Errorf("SHOW application_name = %q, want %q", appName, "issue305")
	}
}

// Item 8: pg_typeof is a function, not the type catalog. A statement over a
// real table that happens to contain the substring PG_TYPE must not be
// claimed by the introspection layer and silently answered empty — the
// engine answers it (loudly, if the function is unimplemented).
func TestIssue305Item8PgTypeofNotSwallowed(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query(`SELECT pg_typeof(id) FROM users`)
	if err != nil {
		// A loud error is an acceptable answer for an unimplemented
		// function; a silent empty result set is not.
		if !strings.Contains(err.Error(), "pg_typeof") {
			t.Fatalf("error does not name the function: %v", err)
		}
		return
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		if !strings.Contains(err.Error(), "pg_typeof") {
			t.Fatalf("error does not name the function: %v", err)
		}
		return
	}
	if n == 0 {
		t.Fatalf("SELECT pg_typeof(id) FROM users answered 0 rows over a 3-row table: silently swallowed")
	}
}

// Item 6: pg_tables is the user-facing table listing; an empty answer while
// tables exist is a wrong answer.
func TestIssue305Item6PgTablesListsTables(t *testing.T) {
	srv := setupTwoTableDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query(`SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			t.Fatal(err)
		}
		if schema != "public" {
			t.Errorf("schemaname = %q, want public", schema)
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !found["users"] || !found["events"] {
		t.Fatalf("pg_tables listed %v, want users and events", found)
	}
}

// Item 6 (residue): the catalogs this server has nothing in still answer
// with a coherent empty result, not an error.
func TestIssue305Item6EmptyCatalogsAnswer(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	for _, sql := range []string{
		`SELECT schemaname, viewname FROM pg_catalog.pg_views`,
		`SELECT schemaname, matviewname FROM pg_catalog.pg_matviews`,
		`SELECT oid, proname FROM pg_catalog.pg_proc WHERE proname = 'nope'`,
		`SELECT objoid, description FROM pg_catalog.pg_description`,
		`SELECT oid, amname FROM pg_catalog.pg_am`,
	} {
		rows, err := db.Query(sql)
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			continue
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			t.Errorf("%s: %v", sql, err)
		}
		rows.Close()
	}
}

// Item 2 (residue): pg_type answers in the client's own SELECT-list shape —
// pgJDBC's TypeInfoCache reads columns by position from its own list, not
// from a fixed five-column vocabulary.
func TestIssue305Item2PgTypeClientShape(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query(`SELECT typname, oid FROM pg_catalog.pg_type WHERE typname = 'int4'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 || cols[0] != "typname" || cols[1] != "oid" {
		t.Fatalf("columns = %v, want [typname oid] (the client's own order)", cols)
	}
	if !rows.Next() {
		t.Fatalf("typname = 'int4' answered no rows")
	}
	var typname, oid string
	if err := rows.Scan(&typname, &oid); err != nil {
		t.Fatal(err)
	}
	if typname != "int4" || oid != "23" {
		t.Fatalf("got (%q, %q), want (int4, 23)", typname, oid)
	}
	if rows.Next() {
		t.Fatalf("typname = 'int4' answered more than one row")
	}
}

// Item 8 (second half): a query about a table NAMED "alerts" must not be
// answered with the alert listing just because the word appears in its text.
// The information_schema branches select on the relation the statement reads.
func TestIssue305Item8AlertsWordIsNotTheAlertCatalog(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query(
		`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'alerts'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	// The alert listing's vocabulary is name/interval_seconds/enabled/...;
	// the client asked about columns and must get its own two labels back.
	if len(cols) != 2 || cols[0] != "column_name" || cols[1] != "data_type" {
		t.Fatalf("columns = %v, want [column_name data_type]", cols)
	}
	for rows.Next() {
		t.Fatalf("no table named alerts exists; the answer must be empty")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// Item 10: a SELECT from a table that does not exist is an error, not an
// empty result with a success tag.
func TestIssue305Item10MissingTableErrors(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query(`SELECT * FROM no_such_table_305`)
	if err == nil {
		n := 0
		for rows.Next() {
			n++
		}
		rows.Close()
		t.Fatalf("SELECT from a missing table answered %d rows with no error", n)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "no_such_table_305") {
		t.Errorf("error does not name the missing table: %v", err)
	}
}
