package dataplane

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestHelloWelcomeRoundTrip is the Phase A smoke test: a client dials a
// server on a localhost port, exchanges Hello/Welcome, and stays
// connected. Validates the entire codegen + plumbing path without
// touching the rest of the worker/coord wiring.
func TestHelloWelcomeRoundTrip(t *testing.T) {
	srv := NewServer(ServerConfig{
		Addr:      "127.0.0.1:0",
		ClusterID: "test-cluster",
	}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "worker-test",
		BuildSHA:         "deadbeef",
		ReconnectBackoff: 50 * time.Millisecond,
	}, nil)
	client.Start(ctx)
	defer client.Stop()

	if !waitFor(t, 2*time.Second, client.Connected) {
		t.Fatal("client never reported Connected=true")
	}
	if got := client.ClusterID(); got != "test-cluster" {
		t.Errorf("ClusterID() = %q, want %q", got, "test-cluster")
	}
	if got := srv.ConnectedWorkers(); len(got) != 1 || got[0] != "worker-test" {
		t.Errorf("ConnectedWorkers() = %v, want [worker-test]", got)
	}
}

// TestClientReconnectAfterServerRestart confirms the worker's run loop
// recovers when coord goes away and comes back.
func TestClientReconnectAfterServerRestart(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "c1"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	addr := srv.Addr()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		CoordAddr:        addr,
		WorkerID:         "worker-1",
		ReconnectBackoff: 25 * time.Millisecond,
	}, nil)
	client.Start(ctx)
	defer client.Stop()

	if !waitFor(t, 2*time.Second, client.Connected) {
		t.Fatal("initial connect: client never reported Connected=true")
	}

	srv.Stop(0)
	if !waitFor(t, 2*time.Second, func() bool { return !client.Connected() }) {
		t.Fatal("after server stop: client never reported Connected=false")
	}

	srv2 := NewServer(ServerConfig{Addr: addr, ClusterID: "c1"}, nil)
	if err := srv2.Start(); err != nil {
		t.Fatalf("server restart: %v", err)
	}
	defer srv2.Stop(0)

	if !waitFor(t, 8*time.Second, client.Connected) {
		t.Fatal("after server restart: client never reconnected")
	}
}

// TestServerRejectsNonHelloFirstMessage exercises the server's defense
// against malformed handshakes. We can't easily inject one through the
// public Client, so we use the lower-level gRPC API directly — but for
// Phase A keeping it light: just check that closing without Hello
// doesn't crash the server.
func TestServerHandlesClientImmediateClose(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "c"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	// Open a raw TCP connection to the gRPC port and immediately close.
	// gRPC will reject it as an invalid HTTP/2 frame; the server's
	// Connect handler is never invoked. Just verifying the server
	// doesn't panic on this.
	for i := 0; i < 5; i++ {
		conn, err := net.Dial("tcp", srv.Addr())
		if err != nil {
			t.Fatalf("dial #%d: %v", i, err)
		}
		_ = conn.Close()
	}
	// Give the server a moment to log/discard.
	time.Sleep(50 * time.Millisecond)

	if got := srv.ConnectedWorkers(); len(got) != 0 {
		t.Errorf("ConnectedWorkers() = %v after garbage conns, want empty", got)
	}
}

// TestResultBatchRoundTrip exercises the Phase B path: server registers
// a handler for a query, client sends ResultBatch messages, server
// dispatches them to the handler.
func TestResultBatchRoundTrip(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "rb-cluster"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "worker-rb",
		ReconnectBackoff: 50 * time.Millisecond,
	}, nil)
	client.Start(ctx)
	defer client.Stop()
	if !waitFor(t, 2*time.Second, client.Connected) {
		t.Fatal("client never connected")
	}

	var received atomic.Int32
	var lastTerm atomic.Bool
	const queryID = "q-test-1"
	srv.RegisterResultHandler(queryID, func(rb *ResultBatch) {
		received.Add(1)
		if rb.Terminal {
			lastTerm.Store(true)
		}
	})
	defer srv.UnregisterResultHandler(queryID)

	for i := 0; i < 3; i++ {
		if err := client.SendResultBatch(ResultBatch{
			QueryID:  queryID,
			WorkerID: "worker-rb",
			RowCount: 100,
			Payload:  []byte("dummy-wshf"),
		}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := client.SendResultBatch(ResultBatch{
		QueryID:  queryID,
		WorkerID: "worker-rb",
		Terminal: true,
	}); err != nil {
		t.Fatalf("send terminal: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool { return received.Load() == 4 }) {
		t.Fatalf("got %d batches, want 4", received.Load())
	}
	if !lastTerm.Load() {
		t.Error("terminal batch was not received with Terminal=true")
	}
}

// TestResultBatchUnknownQueryDropped verifies that ResultBatches for
// unregistered query IDs are silently dropped (matches NATS publish-
// with-no-subscriber semantics).
func TestResultBatchUnknownQueryDropped(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "rb2"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "w",
		ReconnectBackoff: 50 * time.Millisecond,
	}, nil)
	client.Start(ctx)
	defer client.Stop()
	if !waitFor(t, 2*time.Second, client.Connected) {
		t.Fatal("client never connected")
	}

	// No handler registered. Send should still succeed; server drops.
	if err := client.SendResultBatch(ResultBatch{
		QueryID:  "ghost-query",
		WorkerID: "w",
		Terminal: true,
	}); err != nil {
		t.Fatalf("send to unregistered query failed (should be silent drop): %v", err)
	}
	// Connection should remain healthy.
	time.Sleep(50 * time.Millisecond)
	if !client.Connected() {
		t.Error("client disconnected after sending to unregistered query")
	}
}

// waitFor polls cond every 25ms until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return cond()
}

