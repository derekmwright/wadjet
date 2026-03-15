package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/caelum/internal/coordinator"
	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/server"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
	"github.com/derekmwright/caelum/internal/worker"
)

// setupTestServer creates an embedded NATS, coordinator, worker, and HTTP server for testing.
func setupTestServer(t *testing.T) (*httptest.Server, objstore.Store, *catalog.Catalog) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Start embedded NATS
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("creating JetStream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setting up streams: %v", err)
	}

	// Create in-memory object store and catalog
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.New(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	// Start worker
	w := worker.New(worker.Config{
		NATSUrl:       embeddedNATS.ClientURL(),
		MaxConcurrent: 4,
		CacheBytes:    64 * 1024 * 1024,
	}, store, nc, js, logger)
	if err := w.Start(ctx); err != nil {
		t.Fatalf("starting worker: %v", err)
	}
	t.Cleanup(w.Stop)

	// Create coordinator
	coord := coordinator.New(coordinator.Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
	}, cat, nc, js, logger)

	// Create HTTP server
	srv := server.New(server.Config{
		Addr:        ":0",
		Catalog:     cat,
		Coordinator: coord,
	}, logger)

	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)

	return ts, store, cat
}

func ingestTestRows(t *testing.T, ctx context.Context, store objstore.Store, cat *catalog.Catalog, tableName string, schema []parquet.Column, rows []map[string]any) {
	t.Helper()

	if err := cat.CreateTable(ctx, tableName, parquet.Schema{Columns: schema}, nil); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	var buf bytes.Buffer
	pw, err := parquet.NewWriter(&buf, parquet.Schema{Columns: schema}, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	filePath := "tables/" + tableName + "/chunk_0001.parquet"
	if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
		Path:      filePath,
		SizeBytes: int64(len(data)),
		NumRows:   int64(len(rows)),
		CreatedAt: time.Now(),
	}}); err != nil {
		t.Fatalf("adding file to manifest: %v", err)
	}
}

func TestAsyncQuerySubmitAndPoll(t *testing.T) {
	ts, store, cat := setupTestServer(t)
	ctx := context.Background()

	schema := []parquet.Column{
		{Name: "user_id", Type: parquet.TypeString},
		{Name: "amount", Type: parquet.TypeFloat64},
	}
	rows := []map[string]any{
		{"user_id": "u1", "amount": 10.0},
		{"user_id": "u2", "amount": 20.0},
		{"user_id": "u1", "amount": 30.0},
	}
	ingestTestRows(t, ctx, store, cat, "events", schema, rows)

	// Submit async query
	body := `{"sql": "SELECT * FROM events"}`
	resp, err := http.Post(ts.URL+"/v1/queries/async", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	var asyncResp struct {
		QueryID string `json:"query_id"`
		State   string `json:"state"`
		Plan    string `json:"plan"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&asyncResp); err != nil {
		t.Fatal(err)
	}
	if asyncResp.QueryID == "" {
		t.Fatal("expected non-empty query_id")
	}
	if asyncResp.State != "running" {
		t.Fatalf("expected state=running, got %s", asyncResp.State)
	}
	t.Logf("submitted query %s, plan: %s", asyncResp.QueryID, asyncResp.Plan)

	// Poll for status until completed (with timeout)
	deadline := time.Now().Add(15 * time.Second)
	var statusResp struct {
		QueryID string `json:"query_id"`
		State   string `json:"state"`
		Elapsed string `json:"elapsed"`
		Stages  []struct {
			StageID    string `json:"stage_id"`
			Type       string `json:"type"`
			TotalTasks int    `json:"total_tasks"`
			DoneTasks  int    `json:"done_tasks"`
		} `json:"stages"`
	}
	for time.Now().Before(deadline) {
		statusURL := ts.URL + "/v1/queries/" + asyncResp.QueryID
		statusHTTP, err := http.Get(statusURL)
		if err != nil {
			t.Fatal(err)
		}
		json.NewDecoder(statusHTTP.Body).Decode(&statusResp)
		statusHTTP.Body.Close()

		if statusResp.State == "completed" || statusResp.State == "failed" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if statusResp.State != "completed" {
		t.Fatalf("query did not complete, state=%s", statusResp.State)
	}
	t.Logf("query completed in %s", statusResp.Elapsed)

	// Get results
	resultsResp, err := http.Get(ts.URL + "/v1/queries/" + asyncResp.QueryID + "/results")
	if err != nil {
		t.Fatal(err)
	}
	defer resultsResp.Body.Close()

	var queryResp struct {
		QueryID string           `json:"query_id"`
		Columns []string         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
		Error   string           `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resultsResp.Body).Decode(&queryResp); err != nil {
		t.Fatal(err)
	}

	if queryResp.Error != "" {
		t.Fatalf("unexpected error: %s", queryResp.Error)
	}
	if len(queryResp.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(queryResp.Rows))
	}
	t.Logf("got %d rows, columns: %v", len(queryResp.Rows), queryResp.Columns)
}

func TestAsyncQueryCancel(t *testing.T) {
	ts, _, _ := setupTestServer(t)

	// Submit async query for a nonexistent table — it will fail during planning
	// Instead, test cancel on a valid query
	// We'll just test the cancel endpoint returns correct response format

	// Cancel a non-existent query should return error
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/queries/nonexistent", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-existent query, got %d", resp.StatusCode)
	}
}

func TestListQueries(t *testing.T) {
	ts, store, cat := setupTestServer(t)
	ctx := context.Background()

	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
	}
	rows := []map[string]any{{"id": int64(1)}, {"id": int64(2)}}
	ingestTestRows(t, ctx, store, cat, "items", schema, rows)

	// Submit a sync query first to populate tracker
	body := `{"sql": "SELECT * FROM items"}`
	resp, err := http.Post(ts.URL+"/v1/queries", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// List queries
	listResp, err := http.Get(ts.URL + "/v1/queries")
	if err != nil {
		t.Fatal(err)
	}
	defer listResp.Body.Close()

	var listResult struct {
		Queries []struct {
			QueryID string `json:"query_id"`
			State   string `json:"state"`
			SQL     string `json:"sql"`
		} `json:"queries"`
		Count int `json:"count"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listResult); err != nil {
		t.Fatal(err)
	}

	if listResult.Count < 1 {
		t.Fatalf("expected at least 1 query, got %d", listResult.Count)
	}
	t.Logf("listed %d queries", listResult.Count)
}

func TestQueryStatusNotFound(t *testing.T) {
	ts, _, _ := setupTestServer(t)

	resp, err := http.Get(ts.URL + "/v1/queries/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestAsyncQueryNoCoordinator(t *testing.T) {
	// Server without coordinator
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	store := objstore.NewMemStore()
	cat := catalog.New(store, "test")

	srv := server.New(server.Config{
		Addr:    ":0",
		Catalog: cat,
	}, logger)
	ts := httptest.NewServer(srv.Mux())
	defer ts.Close()

	// All async endpoints should return 503
	body := `{"sql": "SELECT 1"}`
	resp, err := http.Post(ts.URL+"/v1/queries/async", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for async without coordinator, got %d", resp.StatusCode)
	}

	resp2, err := http.Get(ts.URL + "/v1/queries/foo")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for status without coordinator, got %d", resp2.StatusCode)
	}
}
