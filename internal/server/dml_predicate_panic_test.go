package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A DML WHERE that cannot be evaluated must reach an HTTP client as a
// SQLSTATE, not as a dropped connection.
//
// `DELETE FROM t WHERE 1/0 = 1` over HTTP returned a transport EOF and a
// goroutine dump: the DML predicate closure called compiled.Eval with no
// recover, and net/http's own recover answers a panic by closing the
// connection and logging a stack (#677). The embedded and pgwire doors both
// survived it, because DB.Execute has a boundary and this executor — its twin
// — did not.
//
// The test drives a REAL httptest server over a REAL socket, because that is
// the only arrangement in which "the connection died" and "the handler
// returned an error" are distinguishable at all. Calling the executor
// directly would have passed on the broken build.
func TestHTTPDMLPredicatePanicReachesTheClientAsASQLSTATE(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "panic_dml", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := ingest.New(cat, "panic_dml", schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "n": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()
	// One client, reused across every case: a door that kills the connection
	// poisons the NEXT request too, which is the blast radius the issue is
	// about.
	client := ts.Client()

	for _, tc := range []struct {
		name  string
		where string
		state string
	}{
		{"division by zero", "1/0 = 1", "22012"},
		{"modulo by zero", "1 % 0 = 1", "22012"},
		{"invalid cast to bool", "CAST('abc' AS BOOL)", "22P02"},
		{"invalid cast to int", "CAST('abc' AS INT) = 1", "22P02"},
	} {
		for _, stmt := range []string{
			"DELETE FROM panic_dml WHERE " + tc.where,
			"UPDATE panic_dml SET n = 2 WHERE " + tc.where,
		} {
			t.Run(tc.name+" "+stmt[:6], func(t *testing.T) {
				before := exec.QueryPanicsRecovered()
				status, body := postSQL(t, client, ts.URL, stmt)
				if status == 0 {
					t.Fatalf("%q killed the connection instead of answering", stmt)
				}
				if status != http.StatusBadRequest {
					t.Errorf("%q answered HTTP %d, want 400 — a statement refused for what it "+
						"CONTAINS is the client's error\n  body: %s", stmt, status, body)
				}
				var resp map[string]string
				if err := json.Unmarshal([]byte(body), &resp); err != nil {
					t.Fatalf("%q answered unparseable body %q: %v", stmt, body, err)
				}
				if resp["sqlstate"] != tc.state {
					t.Errorf("%q reported SQLSTATE %q, want %q\n  body: %s",
						stmt, resp["sqlstate"], tc.state, body)
				}
				if d := exec.QueryPanicsRecovered() - before; d != 0 {
					t.Errorf("%q counted %d recovered panic(s); a fatal EVAL is the designed class "+
						"and keeps its own SQLSTATE rather than becoming an XX000", stmt, d)
				}
			})
		}
	}

	// The server is still serving, and nothing was deleted or updated.
	status, body := postSQL(t, client, ts.URL, "SELECT id, n FROM panic_dml")
	if status != http.StatusOK {
		t.Fatalf("the server is unusable after the refused statements: HTTP %d %s", status, body)
	}
	var q struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal([]byte(body), &q); err != nil {
		t.Fatal(err)
	}
	if len(q.Rows) != 1 {
		t.Fatalf("%d rows survive, want 1: %s", len(q.Rows), body)
	}
	if got := q.Rows[0]["n"]; got != float64(1) {
		t.Errorf("n = %v after statements that all failed, want 1", got)
	}
}

// postSQL returns (0, "") when the request did not complete at all — the
// dropped-connection outcome this test exists to tell apart from an error
// response.
func postSQL(t *testing.T, client *http.Client, base, sql string) (int, string) {
	t.Helper()
	buf, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Post(base+"/v1/queries", "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Logf("POST %q: %v", sql, err)
		return 0, ""
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("reading the body of %q: %v", sql, err)
		return 0, ""
	}
	return resp.StatusCode, string(body)
}
