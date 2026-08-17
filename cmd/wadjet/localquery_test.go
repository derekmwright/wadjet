package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIsCatalogFreeQuery(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want bool
	}{
		{"table function only", `SELECT COUNT(*) FROM read_json('conn.log')`, true},
		{"table function with group by", `SELECT id_orig_h, SUM(orig_bytes) FROM read_json('conn.log') GROUP BY 1 ORDER BY 2 DESC LIMIT 10`, true},
		{"csv and parquet", `SELECT * FROM read_csv('a.csv')`, true},
		{"parquet", `SELECT * FROM read_parquet('a.parquet')`, true},
		{"no source", `SELECT 1`, true},
		{"catalog table", `SELECT * FROM flow_logs`, false},
		{"mixed join", `SELECT * FROM read_json('a.json') j JOIN flow_logs f ON j.id = f.id`, false},
		{"table function join", `SELECT * FROM read_json('a.json') a JOIN read_csv('b.csv') b ON a.id = b.id`, true},
		{"comma join of table functions", `SELECT * FROM read_json('a.json') a, read_csv('b.csv') b`, true},
		{"cte over table function", `WITH c AS (SELECT * FROM read_json('a.json')) SELECT COUNT(*) FROM c`, true},
		{"cte over catalog table", `WITH c AS (SELECT * FROM orders) SELECT COUNT(*) FROM c`, false},
		{"derived table", `SELECT * FROM (SELECT a FROM read_csv('x.csv')) t`, true},
		{"derived table over catalog", `SELECT * FROM (SELECT a FROM orders) t`, false},
		{"subquery in where over catalog", `SELECT * FROM read_json('a.json') WHERE id IN (SELECT id FROM orders)`, false},
		{"union of table functions", `SELECT a FROM read_json('a.json') UNION ALL SELECT a FROM read_json('b.json')`, true},
		{"union with catalog table", `SELECT a FROM read_json('a.json') UNION ALL SELECT a FROM orders`, false},
		{"parse error", `SELECT * FROM read_json(`, false},
		{"empty", ``, false},
		{"not a select", `INSERT INTO t SELECT * FROM read_json('a.json')`, false},
		{"create table as", `CREATE TABLE t AS SELECT * FROM read_json('a.json')`, false},
		{"unknown table function", `SELECT * FROM postgres_scan('conn', 'orders')`, false},
		{"select word in a literal", `SELECT * FROM read_json('a.json') WHERE msg = 'select'`, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCatalogFreeQuery(tc.sql); got != tc.want {
				t.Errorf("isCatalogFreeQuery(%q) = %v, want %v", tc.sql, got, tc.want)
			}
		})
	}
}

func TestCountSelectWords(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"SELECT 1", 1},
		{"select a from t union select b from u", 2},
		{"SELECT selected_col FROM t", 1},
		{"SELECT preselect FROM t", 1},
		{"", 0},
	}
	for _, tc := range cases {
		if got := countSelectWords(tc.in); got != tc.want {
			t.Errorf("countSelectWords(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestQueryCmdTableFunctionNoObjectStore is the #303 regression: a table
// function query must succeed with the object-store endpoint pointed at a
// dead address. Before the fix the command built a MinIO client from these
// flags and failed in catalog init before the SQL was parsed.
func TestQueryCmdTableFunctionNoObjectStore(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "conn.log")
	lines := `{"id_orig_h":"10.0.0.1","orig_bytes":100}
{"id_orig_h":"10.0.0.2","orig_bytes":250}
{"id_orig_h":"10.0.0.1","orig_bytes":50}
`
	if err := os.WriteFile(logPath, []byte(lines), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	withDeadObjectStore(t)

	out := runQueryCmd(t, `SELECT id_orig_h, SUM(orig_bytes) AS total FROM read_json('`+logPath+
		`') GROUP BY id_orig_h ORDER BY total DESC`)

	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		t.Fatalf("parsing output %q: %v", out, err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2: %q", len(rows), out)
	}
	if got := rows[0]["id_orig_h"]; got != "10.0.0.2" {
		t.Errorf("first row id_orig_h = %v, want 10.0.0.2 (%q)", got, out)
	}
	if got := rows[1]["total"]; got != float64(150) {
		t.Errorf("second row total = %v, want 150 (%q)", got, out)
	}
}

// TestQueryCmdCatalogTableStillNeedsStore pins the other half of the
// decision: a catalog-table query keeps the object-store path and so still
// fails when the endpoint is dead.
func TestQueryCmdCatalogTableStillNeedsStore(t *testing.T) {
	withDeadObjectStore(t)

	cmd := queryCmd()
	cmd.SetArgs([]string{`SELECT * FROM flow_logs`})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err == nil {
		t.Fatal("query over a catalog table succeeded with no reachable object store")
	}
}

// withDeadObjectStore points the storage flags at an address nothing
// listens on and restores them when the test ends.
func withDeadObjectStore(t *testing.T) {
	t.Helper()
	prevType, prevEndpoint, prevBucket := storageType, endpoint, bucket
	prevAccess, prevSecret := accessKey, secretKey
	t.Cleanup(func() {
		storageType, endpoint, bucket = prevType, prevEndpoint, prevBucket
		accessKey, secretKey = prevAccess, prevSecret
	})
	storageType = ""
	endpoint = "127.0.0.1:1"
	bucket = "wadjet"
	accessKey = "nobody"
	secretKey = "nothing"
}

// runQueryCmd executes the `query` command and returns what it wrote to
// stdout.
func runQueryCmd(t *testing.T, sqlText string) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating pipe: %v", err)
	}
	prevStdout := os.Stdout
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		io.Copy(&sb, r)
		done <- sb.String()
	}()

	cmd := queryCmd()
	cmd.SetArgs([]string{sqlText})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SilenceUsage = true
	runErr := cmd.Execute()

	w.Close()
	os.Stdout = prevStdout
	out := <-done
	r.Close()

	if runErr != nil {
		t.Fatalf("query failed: %v (output %q)", runErr, out)
	}
	return out
}
