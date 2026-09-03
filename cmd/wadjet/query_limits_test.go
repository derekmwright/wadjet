package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/server"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestServeConfigQueryLimitsRejectOverHTTP is the second consumer of #803's
// plumbing: the HTTP server's own planner (the embedded, no-coordinator path).
// The limits arrive the way a deployment's limits arrive — through
// config.Load and Config.EffectiveQueryLimits, the two calls `wadjet serve`
// makes — and the rejection is asserted on the wire.
//
// The coordinator arm, which is what answers in every serve mode, is gated by
// internal/coordinator.TestConfiguredQueryLimitsRejectOnEveryCoordinatorArm.
func TestServeConfigQueryLimitsRejectOverHTTP(t *testing.T) {
	ctx := context.Background()

	cfgPath := writeConfigFile(t, `
query_limits:
  max_scan_bytes: 1
`)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	global, roleLimits := cfg.EffectiveQueryLimits()
	if global == nil || global.MaxScanBytes != 1 {
		t.Fatalf("config extraction: %+v, want MaxScanBytes 1", global)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "msg", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := ingest.New(cat, "events", schema, nil,
		ingest.Config{MaxBufferRows: 4, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "msg": "a"},
		{"id": int64(2), "msg": "b"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	addr := freeAddr(t)
	srv := server.New(server.Config{
		Addr:        addr,
		Catalog:     cat,
		QueryLimits: global,
		RoleLimits:  roleLimits,
	}, logger)
	startServer(t, srv, addr)
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	})

	post := func(sql string) (int, string) {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"sql": sql})
		resp, err := http.Post("http://"+addr+"/v1/queries", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST /v1/queries: %v", err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}

	code, body := post("SELECT id, msg FROM events")
	if code == http.StatusOK {
		t.Fatalf("a query over the configured max_scan_bytes must be rejected; got 200 %s", body)
	}
	if !bytes.Contains([]byte(body), []byte("exceeding limit")) {
		t.Errorf("rejection body %q does not name the limit that was exceeded", body)
	}
}
