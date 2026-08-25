package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// newTestServer creates a minimal server with an in-memory catalog for testing
// HTTP endpoints that do not require a coordinator.
func newTestServer(t *testing.T) (*Server, *catalog.Catalog) {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	srv := New(Config{
		Addr:    ":0",
		Catalog: cat,
	}, logger)
	return srv, cat
}

// --- Health endpoint ---

func TestHandleHealth(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %q", body["status"])
	}
}

// --- List Tables endpoint ---

func TestHandleListTables_Empty(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/tables")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	tables := body["tables"].([]any)
	if len(tables) != 0 {
		t.Errorf("expected 0 tables, got %d", len(tables))
	}
}

func TestHandleListTables_WithTables(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}}
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/tables")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	tables := body["tables"].([]any)
	if len(tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(tables))
	}
}

// --- Get Table endpoint ---

func TestHandleGetTable(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/tables/users")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHandleGetTable_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/tables/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// --- Create Table (REST) ---

func TestHandleCreateTable(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"name":"events","columns":[{"name":"id","type":"BIGINT"},{"name":"data","type":"VARCHAR","nullable":true}]}`
	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		var errBody map[string]string
		json.NewDecoder(resp.Body).Decode(&errBody)
		t.Fatalf("expected 201, got %d: %v", resp.StatusCode, errBody)
	}
}

func TestHandleCreateTable_EmptyName(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"name":"","columns":[{"name":"id","type":"BIGINT"}]}`
	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandleCreateTable_NoColumns(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"name":"test","columns":[]}`
	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandleCreateTable_InvalidType(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"name":"test","columns":[{"name":"x","type":"BADTYPE"}]}`
	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandleCreateTable_InvalidBody(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewBufferString("not json"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandleCreateTable_Duplicate(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := cat.CreateTable(ctx, "existing", schema, nil); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"name":"existing","columns":[{"name":"id","type":"BIGINT"}]}`
	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
}

func TestHandleCreateTable_NullableField(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"name":"test_nullable","columns":[{"name":"id","type":"INT64","nullable":false},{"name":"val","type":"VARCHAR"}]}`
	resp, err := http.Post(ts.URL+"/v1/tables", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
}

// --- Delete Table endpoint ---

func TestHandleDeleteTable(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := cat.CreateTable(ctx, "todrop", schema, nil); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/tables/todrop", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHandleDeleteTable_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/tables/nope", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// --- Query endpoint (local execution) ---

func TestHandleQuery_EmptySQL(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":""}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandleQuery_InvalidBody(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString("{invalid"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHandleQuery_InvalidSQL(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"NOT VALID SQL AT ALL $$$$"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid SQL, got %d", resp.StatusCode)
	}
}

func TestHandleQuery_ShowTables(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	cat.CreateTable(ctx, "tbl1", schema, nil)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"SHOW TABLES"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var qr QueryResponse
	json.NewDecoder(resp.Body).Decode(&qr)
	if len(qr.Rows) < 1 {
		t.Error("expected at least 1 table row from SHOW TABLES")
	}
}

func TestHandleQuery_Describe(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}}
	cat.CreateTable(ctx, "mytable", schema, nil)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"DESCRIBE mytable"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var qr QueryResponse
	json.NewDecoder(resp.Body).Decode(&qr)
	if len(qr.Rows) != 2 {
		t.Errorf("expected 2 column rows, got %d", len(qr.Rows))
	}
}

func TestHandleQuery_Describe_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"DESCRIBE nope"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestHandleQuery_Explain(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}}
	cat.CreateTable(ctx, "tbl", schema, nil)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"EXPLAIN SELECT id FROM tbl"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var errBody map[string]string
		json.NewDecoder(resp.Body).Decode(&errBody)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, errBody)
	}
	var qr QueryResponse
	json.NewDecoder(resp.Body).Decode(&qr)
	if len(qr.Rows) == 0 {
		t.Error("expected plan in EXPLAIN output")
	}
}

func TestHandleQuery_CreateTableSQL(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"CREATE TABLE newtable (id BIGINT, name VARCHAR)"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var errBody map[string]string
		json.NewDecoder(resp.Body).Decode(&errBody)
		t.Fatalf("expected 200, got %d: %v", resp.StatusCode, errBody)
	}
}

func TestHandleQuery_DropTableSQL(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	cat.CreateTable(ctx, "dropme", schema, nil)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"DROP TABLE dropme"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHandleQuery_DropTableIfExistsSQL(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"DROP TABLE IF EXISTS nonexistent"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHandleQuery_ShowFunctions(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"SHOW FUNCTIONS"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

// --- writeJSON/writeError ---

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()
	writeError(w, http.StatusNotFound, "not found")
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	var body map[string]string
	json.NewDecoder(w.Body).Decode(&body)
	if body["error"] != "not found" {
		t.Errorf("expected error 'not found', got %q", body["error"])
	}
}

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusOK, map[string]string{"key": "val"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %q", w.Header().Get("Content-Type"))
	}
}

// --- HTTP DML INSERT: PORT/PROTOCOL/DURATION (#495) ---

// TestHandleDMLInsertPortProtocolDuration is the HTTP-path counterpart of
// wadjet/dml_network_types_test.go's TestInsertNetworkTypedColumns (#485).
//
// server.go had its own, independently-maintained copy of dml.go's
// convertValue — convertDMLValue — with the same three gaps #485 fixed in
// the embedded API and never fixed here: PORT and PROTOCOL literals failed
// the INSERT outright ("expected integer, got string", ingest.checkType
// rejecting the raw string convertDMLValue's default arm handed it), and
// DURATION silently wrote int64(0) for every row (checkType accepts a
// string for DURATION, so validateRow let it through, but the writer's
// int64 conversion has no string case for it). Every POST /v1/queries INSERT
// reached this duplicate, not the fixed wadjet.ConvertValue — #485 shipped
// with no test that actually exercised the HTTP path, which is why the
// duplicate went unnoticed.
//
// The fix deletes convertDMLValue and routes both executeDMLInsert and
// executeDMLUpdate through wadjet.ConvertValue directly (one
// implementation), so this test drives the bug's exact repro through the
// real HTTP handler chain (handleQuery -> handleDML -> executeDMLInsert)
// instead of calling a conversion function directly.
func TestHandleDMLInsertPortProtocolDuration(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "dst_port", Type: parquet.TypePort, Nullable: true},
		{Name: "proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "elapsed", Type: parquet.TypeDuration, Nullable: true},
	}}
	if err := cat.CreateTable(ctx, "flow_logs", schema, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	postSQL := func(sql string) QueryResponse {
		t.Helper()
		body, err := json.Marshal(map[string]string{"sql": sql})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST %q: %v", sql, err)
		}
		defer resp.Body.Close()
		var qr QueryResponse
		if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
			t.Fatalf("decoding response for %q: %v", sql, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST %q: status %d, body %+v", sql, resp.StatusCode, qr)
		}
		return qr
	}

	postSQL(`INSERT INTO flow_logs (id, dst_port, proto, elapsed) VALUES (1, 443, 6, 1500000)`)

	qr := postSQL(`SELECT dst_port, proto, elapsed FROM flow_logs WHERE id = 1`)
	if len(qr.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d: %+v", len(qr.Rows), qr.Rows)
	}
	row := qr.Rows[0]
	// JSON numbers decode as float64 into map[string]any.
	want := map[string]float64{"dst_port": 443, "proto": 6, "elapsed": 1_500_000}
	for col, w := range want {
		got, ok := row[col].(float64)
		if !ok {
			t.Errorf("column %q = %#v (%T), want a number", col, row[col], row[col])
			continue
		}
		if got != w {
			t.Errorf("column %q = %v, want %v (elapsed=0 is the #495 silent-DURATION-zero symptom)", col, got, w)
		}
	}

	// executeDMLUpdate's SET-clause values go through the identical
	// wadjet.ConvertValue call (server.go's second convertDMLValue call
	// site) -- verify PORT/PROTOCOL/DURATION convert there too.
	postSQL(`UPDATE flow_logs SET dst_port = 8443, proto = 17, elapsed = 2500000 WHERE id = 1`)
	qr = postSQL(`SELECT dst_port, proto, elapsed FROM flow_logs WHERE id = 1`)
	if len(qr.Rows) != 1 {
		t.Fatalf("after UPDATE: expected 1 row, got %d: %+v", len(qr.Rows), qr.Rows)
	}
	row = qr.Rows[0]
	wantAfterUpdate := map[string]float64{"dst_port": 8443, "proto": 17, "elapsed": 2_500_000}
	for col, w := range wantAfterUpdate {
		got, ok := row[col].(float64)
		if !ok {
			t.Errorf("after UPDATE: column %q = %#v (%T), want a number", col, row[col], row[col])
			continue
		}
		if got != w {
			t.Errorf("after UPDATE: column %q = %v, want %v", col, got, w)
		}
	}
}

// --- defaultMaskValue ---

func TestDefaultMaskValue(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want any
	}{
		{"nil", nil, nil},
		{"string", "secret", "***"},
		{"int", 42, int64(0)},
		{"int32", int32(1), int64(0)},
		{"int64", int64(9), int64(0)},
		{"float32", float32(1.5), float64(0)},
		{"float64", float64(2.5), float64(0)},
		{"bool", true, false},
		{"other", []byte{1, 2}, "***"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := defaultMaskValue(tt.val)
			if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", tt.want) {
				t.Errorf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

// --- resolveQueryLimits ---

func TestResolveQueryLimits(t *testing.T) {
	srv, _ := newTestServer(t)

	// No limits configured
	req := httptest.NewRequest("GET", "/", nil)
	got := srv.resolveQueryLimits(req)
	if got != nil {
		t.Error("expected nil limits when not configured")
	}

	// Global limits only
	global := &config.QueryLimits{MaxScanRows: 100}
	srv.config.QueryLimits = global
	got = srv.resolveQueryLimits(req)
	if got != global {
		t.Error("expected global limits")
	}

	// Per-role limits with matching identity
	roleLimits := &config.QueryLimits{MaxScanRows: 50}
	srv.config.RoleLimits = map[string]*config.QueryLimits{
		"analyst": roleLimits,
	}
	reqWithIdentity := httptest.NewRequest("GET", "/", nil)
	identity := &auth.Identity{Name: "test", Role: "analyst"}
	ctx := auth.ContextWithIdentity(reqWithIdentity.Context(), identity)
	reqWithIdentity = reqWithIdentity.WithContext(ctx)

	got = srv.resolveQueryLimits(reqWithIdentity)
	if got != roleLimits {
		t.Error("expected per-role limits for analyst")
	}

	// Identity with non-matching role falls back to global
	reqAdmin := httptest.NewRequest("GET", "/", nil)
	adminId := &auth.Identity{Name: "admin", Role: "admin"}
	ctx2 := auth.ContextWithIdentity(reqAdmin.Context(), adminId)
	reqAdmin = reqAdmin.WithContext(ctx2)

	got = srv.resolveQueryLimits(reqAdmin)
	if got != global {
		t.Error("expected global limits for non-matching role")
	}
}

// --- logSlowQuery ---

func TestLogSlowQuery(t *testing.T) {
	srv, _ := newTestServer(t)
	// No threshold: should not panic
	srv.logSlowQuery("SELECT 1", time.Millisecond, 0)

	// With threshold: above threshold should log (just verify no panic)
	srv.config.SlowQueryThreshold = time.Millisecond
	srv.logSlowQuery("SELECT *", 2*time.Millisecond, 10)

	// Below threshold: should not log (verify no panic)
	srv.logSlowQuery("SELECT 1", 500*time.Microsecond, 0)
}

// --- getAuthz/getPolicies/getEvaluator ---

func TestGetAuthz_Static(t *testing.T) {
	srv, _ := newTestServer(t)
	// Static: returns srv.authz
	got := srv.getAuthz()
	if got != nil {
		t.Error("expected nil without authz configured")
	}
}

func TestGetPolicies_Static(t *testing.T) {
	srv, _ := newTestServer(t)
	got := srv.getPolicies()
	if got != nil {
		t.Error("expected nil without policies configured")
	}
}

func TestGetEvaluator_Nil(t *testing.T) {
	srv, _ := newTestServer(t)
	got := srv.getEvaluator()
	if got != nil {
		t.Error("expected nil without provider configured")
	}
}

// --- applyColumnPolicies ---

func TestApplyColumnPolicies_EmptyRows(t *testing.T) {
	srv, _ := newTestServer(t)
	identity := &auth.Identity{Name: "user", Role: "reader"}
	got := srv.applyColumnPolicies(identity, "table", nil, nil)
	if got != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestApplyColumnPolicies_ABACMaskAndDeny(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.audit = auth.NewAuditLogger(srv.logger)

	identity := &auth.Identity{Name: "user", Role: "reader"}
	decisions := auth.TableDecisions{
		"secrets": {
			Allowed: true,
			Columns: []auth.ColumnDecision{
				{Column: "ssn", Allowed: false},
				{Column: "name", Allowed: true, MaskFunc: "redact"},
			},
		},
	}

	rows := []map[string]any{
		{"ssn": "123-45-6789", "name": "Alice", "age": 30},
	}

	result := srv.applyColumnPolicies(identity, "secrets", decisions, rows)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if _, exists := result[0]["ssn"]; exists {
		t.Error("ssn column should be denied/removed")
	}
	if result[0]["name"] != "***" {
		t.Errorf("name should be masked, got %v", result[0]["name"])
	}
	if result[0]["age"] != 30 {
		t.Errorf("age should be unchanged, got %v", result[0]["age"])
	}
}

func TestApplyColumnPolicies_NoColumnsDecision(t *testing.T) {
	srv, _ := newTestServer(t)
	identity := &auth.Identity{Name: "user", Role: "reader"}
	decisions := auth.TableDecisions{
		"data": {Allowed: true, Columns: nil},
	}
	rows := []map[string]any{{"col1": "val"}}
	result := srv.applyColumnPolicies(identity, "data", decisions, rows)
	if result[0]["col1"] != "val" {
		t.Error("expected rows unchanged when no column decisions")
	}
}

// --- auditColumnPolicy ---

func TestAuditColumnPolicy_NilAudit(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.audit = nil
	// Should not panic
	srv.auditColumnPolicy(&auth.Identity{Name: "test"}, "table", nil)
}

func TestAuditColumnPolicy_NilPolicy(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.audit = auth.NewAuditLogger(srv.logger)
	// Should not panic
	srv.auditColumnPolicy(&auth.Identity{Name: "test"}, "table", nil)
}

func TestAuditColumnPolicy_WithMaskAndDeny(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.audit = auth.NewAuditLogger(srv.logger)

	policy := &auth.AccessPolicy{
		Columns: map[string]auth.ColumnPolicy{
			"ssn":    auth.ColumnMask,
			"salary": auth.ColumnDeny,
		},
	}
	// Should not panic
	srv.auditColumnPolicy(&auth.Identity{Name: "test"}, "employees", policy)
}

// --- columnDefsToSchema ---

// --- CREATE FUNCTION / DROP FUNCTION / SHOW FUNCTIONS ---

func TestHandleQuery_CreateAndDropFunction(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// Create a UDF
	body := `{"sql":"CREATE FUNCTION double(x) AS x * 2"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CREATE FUNCTION: expected 200, got %d", resp.StatusCode)
	}

	// Show functions to verify
	body2 := `{"sql":"SHOW FUNCTIONS"}`
	resp2, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body2))
	if err != nil {
		t.Fatal(err)
	}
	var qr QueryResponse
	json.NewDecoder(resp2.Body).Decode(&qr)
	resp2.Body.Close()
	found := false
	for _, row := range qr.Rows {
		if row["name"] == "double" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'double' in SHOW FUNCTIONS output")
	}

	// Drop the function
	body3 := `{"sql":"DROP FUNCTION double"}`
	resp3, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body3))
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("DROP FUNCTION: expected 200, got %d", resp3.StatusCode)
	}
}

func TestHandleQuery_CreateFunctionDuplicate(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"CREATE FUNCTION dup_test(x) AS x + 1"}`
	resp, _ := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	resp.Body.Close()

	// Try to create same function again without OR REPLACE
	resp2, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for duplicate function, got %d", resp2.StatusCode)
	}

	// Clean up
	cleanup := `{"sql":"DROP FUNCTION dup_test"}`
	resp3, _ := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(cleanup))
	resp3.Body.Close()
}

func TestHandleQuery_DropFunctionIfExists(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"DROP FUNCTION IF EXISTS nonexistent_fn"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for DROP IF EXISTS, got %d", resp.StatusCode)
	}
}

func TestHandleQuery_DropFunctionNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"DROP FUNCTION nonexistent_fn"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for DROP nonexistent, got %d", resp.StatusCode)
	}
}

// --- getAuthz/getPolicies/getEvaluator with Provider ---

func TestGetAuthz_WithProvider(t *testing.T) {
	srv, _ := newTestServer(t)
	cfg := auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{{Key: "k", Name: "n", Role: "admin"}},
		Roles:   []auth.RoleConfig{{Name: "admin", Tables: []string{"*"}, Allow: []string{"read"}}},
	}
	authn, authz := auth.New(cfg)
	provider := auth.NewProvider(authn, authz, nil, nil)
	srv.provider = provider

	got := srv.getAuthz()
	if got == nil {
		t.Error("expected non-nil authz from provider")
	}
}

func TestGetPolicies_WithProvider(t *testing.T) {
	srv, _ := newTestServer(t)
	provider := auth.NewProvider(nil, nil, nil, nil)
	srv.provider = provider

	got := srv.getPolicies()
	if got != nil {
		t.Error("expected nil policies when provider has none")
	}
}

func TestGetEvaluator_WithProvider(t *testing.T) {
	srv, _ := newTestServer(t)
	provider := auth.NewProvider(nil, nil, nil, nil)
	srv.provider = provider

	got := srv.getEvaluator()
	if got != nil {
		t.Error("expected nil evaluator when provider has none")
	}
}

// --- Drop table SQL not found ---

func TestHandleQuery_DropTableSQL_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	body := `{"sql":"DROP TABLE nonexistent"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// --- New server with nil logger ---

func TestNew_NilLogger(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	srv := New(Config{Catalog: cat}, nil)
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.logger == nil {
		t.Fatal("expected default logger to be set")
	}
}

// TestHandleQuery_ConcurrentRequests verifies that concurrent query requests
// do not cause data races. Before the fix, the server shared a single Planner
// across all requests, causing "concurrent map writes" in buildScan.
func TestHandleQuery_ConcurrentRequests(t *testing.T) {
	srv, cat := newTestServer(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "val", Type: parquet.TypeString},
	}}
	cat.CreateTable(ctx, "conc_test", schema, nil)

	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	const goroutines = 10
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			body := `{"sql":"SELECT id, val FROM conc_test"}`
			resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
			if err != nil {
				errs <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("expected 200, got %d", resp.StatusCode)
				return
			}
			errs <- nil
		}()
	}
	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent request failed: %v", err)
		}
	}
}
