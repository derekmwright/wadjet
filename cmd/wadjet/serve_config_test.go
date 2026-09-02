package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/server"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// writeConfigFile writes YAML to a temp file and returns its path.
func writeConfigFile(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// freeAddr reserves and releases a loopback port, returning its address.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func testCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return cat
}

// startServer runs srv.Start() on a goroutine, converting a panic into an
// error on the returned channel so a construction-order defect reports as a
// test failure instead of aborting the whole test binary. Returns once the
// listener accepts a TCP connection.
func startServer(t *testing.T, srv *server.Server, addr string) chan error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("Server.Start panicked: %v", r)
			}
		}()
		errCh <- srv.Start()
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("server did not start: %v", err)
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return errCh
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case err := <-errCh:
		t.Fatalf("server did not start: %v", err)
	default:
	}
	t.Fatalf("server did not begin listening on %s", addr)
	return errCh
}

// TestServeWithProviderConfigStartsAndAuthenticates is the gate for #801: a
// server built from a `--config` file that carries an auth provider must
// START (chi v5 panics when a middleware is installed after the first route
// is registered) and must then actually enforce that middleware.
//
// Both halves matter. Deleting the middleware install would stop the panic
// and leave every route unauthenticated, so the test asserts the 401 as well
// as the 200.
func TestServeWithProviderConfigStartsAndAuthenticates(t *testing.T) {
	cfgPath := writeConfigFile(t, `
mode: standalone
auth:
  enabled: true
  api_keys:
    - key: "analyst-key-abc123"
      name: "analyst"
      role: analyst
  roles:
    - name: analyst
      tables: ["*"]
      allow: [read]
`)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider := buildProviderFromConfig(cfg, logger)
	if !provider.Enabled() {
		t.Fatal("test setup: provider from this config should be enabled")
	}

	addr := freeAddr(t)
	srv := server.New(server.Config{
		Addr:     addr,
		Catalog:  testCatalog(t),
		Provider: provider,
	}, logger)

	startServer(t, srv, addr)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	base := "http://" + addr

	// Health is exempt from auth by design (load balancers, k8s probes).
	resp, err := http.Get(base + "/v1/health")
	if err != nil {
		t.Fatalf("GET /v1/health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /v1/health: got %d, want 200", resp.StatusCode)
	}

	// Unauthenticated: the middleware must be installed and must refuse.
	resp, err = http.Get(base + "/v1/tables")
	if err != nil {
		t.Fatalf("GET /v1/tables (no key): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /v1/tables without a key: got %d, want 401 — auth middleware is not installed", resp.StatusCode)
	}

	// Authenticated: one real request served end to end.
	req, err := http.NewRequest(http.MethodGet, base+"/v1/tables", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer analyst-key-abc123")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /v1/tables (with key): %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /v1/tables with a valid key: got %d (%s), want 200", resp.StatusCode, body)
	}
}
