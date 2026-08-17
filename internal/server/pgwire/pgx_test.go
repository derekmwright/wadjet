package pgwire

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"

	"github.com/derekmwright/wadjet/wadjet"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setupRealDB creates a test DB with a multi-type schema to exercise type mapping.
func setupRealDB(t *testing.T) (*wadjet.DB, *Server) {
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
		{"id": int32(3), "name": "carol", "score": 92.1, "active": true, "visits": int64(200)},
	}
	ing := db.NewIngester("users", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)

	return db, srv
}

func openPQ(t *testing.T, addr string) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		addr[len("127.0.0.1:"):])
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestLibPQConnect verifies that lib/pq can connect and ping.
func TestLibPQConnect(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

// TestLibPQSimpleQuery runs a SELECT and scans typed results.
func TestLibPQSimpleQuery(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query("SELECT id, name, score FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	type user struct {
		ID    int
		Name  string
		Score float64
	}

	var results []user
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.ID, &u.Name, &u.Score); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		results = append(results, u)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}
	t.Logf("Row 0: %+v", results[0])
	t.Logf("Row 1: %+v", results[1])
	t.Logf("Row 2: %+v", results[2])

	if results[0].Name != "alice" {
		t.Errorf("expected alice, got %q", results[0].Name)
	}
	if results[2].Score != 92.1 {
		t.Errorf("expected 92.1, got %f", results[2].Score)
	}
}

// TestLibPQColumnTypes checks that column type info is reported correctly.
func TestLibPQColumnTypes(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query("SELECT id, name, score, visits FROM users LIMIT 1")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}

	for _, ct := range colTypes {
		t.Logf("Column %q: DatabaseTypeName=%q", ct.Name(), ct.DatabaseTypeName())
	}

	// lib/pq maps OIDs to database type names
	expected := map[string]string{
		"id":     "INT4",
		"name":   "TEXT",
		"score":  "FLOAT8",
		"visits": "INT8",
	}

	for _, ct := range colTypes {
		want, ok := expected[ct.Name()]
		if !ok {
			continue
		}
		got := ct.DatabaseTypeName()
		if got != want {
			t.Errorf("column %q: expected DatabaseTypeName %q, got %q", ct.Name(), want, got)
		}
	}
}

// TestLibPQAggregate runs an aggregate query through lib/pq.
func TestLibPQAggregate(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	var cnt int
	var avgScore float64
	err := db.QueryRow("SELECT COUNT(*) AS cnt, AVG(score) AS avg_score FROM users").Scan(&cnt, &avgScore)
	if err != nil {
		t.Fatalf("QueryRow: %v", err)
	}

	t.Logf("cnt=%d, avg_score=%f", cnt, avgScore)
	if cnt != 3 {
		t.Errorf("expected count 3, got %d", cnt)
	}
	if avgScore < 91.0 || avgScore > 92.0 {
		t.Errorf("avg_score out of range: %f", avgScore)
	}
}

// TestLibPQPreparedStatement tests the extended query protocol via lib/pq Prepare.
func TestLibPQPreparedStatement(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	// lib/pq will use Parse/Describe/Bind/Execute
	stmt, err := db.Prepare("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("stmt.Query: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		t.Logf("id=%d name=%s", id, name)
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows from prepared statement, got %d", count)
	}
}

// TestLibPQTransaction tests BEGIN/COMMIT flow.
func TestLibPQTransaction(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	rows, err := tx.Query("SELECT COUNT(*) AS cnt FROM users")
	if err != nil {
		t.Fatalf("tx.Query: %v", err)
	}
	defer rows.Close()

	var cnt int
	if rows.Next() {
		if err := rows.Scan(&cnt); err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
	rows.Close()

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	t.Logf("Transaction query returned count=%d", cnt)
	if cnt != 3 {
		t.Errorf("expected 3, got %d", cnt)
	}
}

// TestLibPQMultipleStatements runs multiple queries on the same connection.
func TestLibPQMultipleStatements(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	for i := 0; i < 5; i++ {
		var cnt int
		err := db.QueryRow("SELECT COUNT(*) AS cnt FROM users").Scan(&cnt)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if cnt != 3 {
			t.Errorf("iteration %d: expected 3, got %d", i, cnt)
		}
	}
}

// Regression test for GitHub issue #9: SELECT col1, col2 returns NULL values
// through pgx v5 client, but SELECT * works.
func TestPgxColumnProjection(t *testing.T) {
	_, srv := setupRealDB(t)
	addr := srv.Addr()
	ctx := context.Background()

	connStr := fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		addr[len("127.0.0.1:"):])

	// pgx v5 uses extended protocol with binary format by default.
	// Use QueryExecModeSimpleProtocol for simple protocol tests.
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	// Test 1: Simple protocol — SELECT col1, col2 (the reported regression)
	t.Run("simple_protocol_projection", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT name, score FROM users ORDER BY id",
			pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("Values: %v", err)
			}
			fds := rows.FieldDescriptions()
			for i, fd := range fds {
				if vals[i] == nil {
					t.Errorf("row %d col %q: value is nil", count, fd.Name)
				}
			}
			t.Logf("row %d: name=%v score=%v", count, vals[0], vals[1])
			count++
		}
		if count != 3 {
			t.Fatalf("expected 3 rows, got %d", count)
		}
	})

	// Test 2: Simple protocol — SELECT * (should work)
	t.Run("simple_protocol_star", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT * FROM users ORDER BY id",
			pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("Values: %v", err)
			}
			count++
			_ = vals
		}
		if count != 3 {
			t.Fatalf("expected 3 rows, got %d", count)
		}
	})

	// Test 3: Simple protocol with WHERE
	t.Run("simple_protocol_where", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT name, score FROM users WHERE id = 1",
			pgx.QueryExecModeSimpleProtocol)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("Values: %v", err)
			}
			if vals[0] == nil {
				t.Errorf("name is nil")
			}
			if vals[1] == nil {
				t.Errorf("score is nil")
			}
			t.Logf("filtered: name=%v score=%v", vals[0], vals[1])
			count++
		}
		if count != 1 {
			t.Fatalf("expected 1 row, got %d", count)
		}
	})

	// Test 4: Extended protocol (binary format) — SELECT col1, col2
	t.Run("extended_protocol_projection", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT name, score FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Errorf("row %d Values error: %v", count, err)
				count++
				continue
			}
			fds := rows.FieldDescriptions()
			for i, fd := range fds {
				if vals[i] == nil {
					t.Errorf("row %d col %q: value is nil", count, fd.Name)
				}
			}
			t.Logf("ext row %d: name=%v score=%v", count, vals[0], vals[1])
			count++
		}
		if count != 3 {
			t.Fatalf("expected 3 rows, got %d", count)
		}
	})

	// Test 5: Extended protocol — SELECT * (binary format)
	t.Run("extended_protocol_star", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT * FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var count int
		for rows.Next() {
			_, err := rows.Values()
			if err != nil {
				t.Errorf("row %d Values error: %v", count, err)
			}
			count++
		}
		if count != 3 {
			t.Fatalf("expected 3 rows, got %d", count)
		}
	})
}

// TestLibPQQuotedIdentifiers covers the query shape BI tools and JDBC
// metadata-driven builders emit over the wire: identifiers quoted
// defensively, table name included. Before delimited identifiers were lexed,
// the first query such a client sent failed in the lexer.
func TestLibPQQuotedIdentifiers(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	t.Run("quoted_table_and_columns", func(t *testing.T) {
		rows, err := db.Query(`SELECT "name", "score" FROM "users" WHERE "score" > 90 ORDER BY "score" DESC`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("columns: %v", err)
		}
		if len(cols) != 2 || cols[0] != "name" || cols[1] != "score" {
			t.Errorf("column names: got %v, want [name score]", cols)
		}

		var names []string
		var scores []float64
		for rows.Next() {
			var n string
			var s float64
			if err := rows.Scan(&n, &s); err != nil {
				t.Fatalf("scan: %v", err)
			}
			names = append(names, n)
			scores = append(scores, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		if len(names) != 2 || names[0] != "alice" || names[1] != "carol" {
			t.Errorf("names: got %v, want [alice carol]", names)
		}
		if len(scores) != 2 || scores[0] != 95.5 {
			t.Errorf("scores: got %v, want 95.5 first", scores)
		}
	})

	t.Run("quoted_group_by_positional", func(t *testing.T) {
		rows, err := db.Query(`SELECT "name", COUNT(*) AS "cnt" FROM "users" GROUP BY 1 ORDER BY 1 LIMIT 10`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("columns: %v", err)
		}
		if len(cols) != 2 || cols[0] != "name" || cols[1] != "cnt" {
			t.Errorf("column names: got %v, want [name cnt]", cols)
		}

		var got []string
		for rows.Next() {
			var n string
			var c int64
			if err := rows.Scan(&n, &c); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got = append(got, fmt.Sprintf("%s=%d", n, c))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		if len(got) != 3 || got[0] != "alice=1" || got[2] != "carol=1" {
			t.Errorf("groups: got %v, want [alice=1 bob=1 carol=1]", got)
		}
	})

	t.Run("quoted_alias_with_space", func(t *testing.T) {
		rows, err := db.Query(`SELECT "name" AS "user name" FROM "users" ORDER BY "name" LIMIT 1`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("columns: %v", err)
		}
		if len(cols) != 1 || cols[0] != "user name" {
			t.Errorf("column names: got %v, want [\"user name\"]", cols)
		}
	})

	t.Run("unterminated_reports_a_clear_error", func(t *testing.T) {
		_, err := db.Query(`SELECT "name FROM users`)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "unterminated quoted identifier") {
			t.Errorf("error does not name the cause: %v", err)
		}
	})
}
