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

// TestTaskProgressRoundTrip is the Phase E smoke test: worker sends
// TaskProgress envelopes; coord's per-query handler AND global handler
// both fire on each arrival; unregister stops the per-query handler
// while the global continues.
func TestTaskProgressRoundTrip(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "tp"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "w-tp",
		ReconnectBackoff: 50 * time.Millisecond,
	}, nil)
	client.Start(ctx)
	defer client.Stop()
	if !waitFor(t, 2*time.Second, client.Connected) {
		t.Fatal("client never connected")
	}

	var globalCount atomic.Int32
	var perQueryCount atomic.Int32
	var lastRows atomic.Int64

	srv.SetGlobalTaskProgressHandler(func(tp *TaskProgress) {
		globalCount.Add(1)
	})
	srv.RegisterTaskProgressHandler("q-tp-1", func(tp *TaskProgress) {
		perQueryCount.Add(1)
		lastRows.Store(tp.RowsProcessed)
	})
	defer srv.UnregisterTaskProgressHandler("q-tp-1")

	for i := int64(1); i <= 3; i++ {
		if err := client.SendTaskProgress(TaskProgress{
			QueryID:       "q-tp-1",
			StageID:       "s",
			TaskID:        "t",
			WorkerID:      "w-tp",
			RowsProcessed: i * 1000,
		}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return globalCount.Load() == 3 && perQueryCount.Load() == 3
	}) {
		t.Fatalf("counts: global=%d per-query=%d want 3 each",
			globalCount.Load(), perQueryCount.Load())
	}
	if got := lastRows.Load(); got != 3000 {
		t.Errorf("lastRows = %d, want 3000", got)
	}

	// Unregister the per-query handler; further sends still hit global only.
	srv.UnregisterTaskProgressHandler("q-tp-1")
	if err := client.SendTaskProgress(TaskProgress{
		QueryID: "q-tp-1", TaskID: "t", WorkerID: "w-tp", RowsProcessed: 9999,
	}); err != nil {
		t.Fatalf("send post-unregister: %v", err)
	}
	if !waitFor(t, 2*time.Second, func() bool { return globalCount.Load() == 4 }) {
		t.Fatalf("global count after unregister = %d, want 4", globalCount.Load())
	}
	if got := perQueryCount.Load(); got != 3 {
		t.Errorf("per-query count after unregister = %d, want 3 (no further fires)", got)
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

// TestTaskDispatchRoundTrip is the Phase C smoke test: server pushes a
// TaskDispatch envelope at a registered worker; client routes it into
// the registered handler.
func TestTaskDispatchRoundTrip(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "td"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "w-td",
		ReconnectBackoff: 50 * time.Millisecond,
	}, nil)
	client.Start(ctx)
	defer client.Stop()

	var received atomic.Int32
	var lastTaskID atomic.Value
	client.RegisterDispatchHandler(func(td TaskDispatch) {
		received.Add(1)
		lastTaskID.Store(td.TaskID)
	})

	if !waitFor(t, 2*time.Second, client.Connected) {
		t.Fatal("client never connected")
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return len(srv.ConnectedWorkers()) == 1
	}) {
		t.Fatal("server never saw worker register")
	}

	for i := 0; i < 3; i++ {
		if err := srv.SendTaskDispatch("w-td", "task-"+string(rune('a'+i)), "q1", "s1", []byte("blob"), 0); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}

	if !waitFor(t, 2*time.Second, func() bool { return received.Load() == 3 }) {
		t.Fatalf("got %d dispatches, want 3", received.Load())
	}
	if got := lastTaskID.Load(); got != "task-c" {
		t.Errorf("last task id = %v, want task-c", got)
	}
}

// TestTaskDispatchRoundRobin spreads N tasks across 3 workers and
// verifies each worker receives roughly N/3.
func TestTaskDispatchRoundRobin(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "rr"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	counts := make(map[string]*atomic.Int32)
	for _, id := range []string{"w-rr-0", "w-rr-1", "w-rr-2"} {
		counts[id] = &atomic.Int32{}
		client := NewClient(ClientConfig{
			CoordAddr:        srv.Addr(),
			WorkerID:         id,
			ReconnectBackoff: 50 * time.Millisecond,
		}, nil)
		client.Start(ctx)
		defer client.Stop()
		c := counts[id]
		client.RegisterDispatchHandler(func(td TaskDispatch) {
			c.Add(1)
		})
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return len(srv.ConnectedWorkers()) == 3
	}) {
		t.Fatalf("only %d/3 workers connected", len(srv.ConnectedWorkers()))
	}

	const n = 30
	for i := 0; i < n; i++ {
		w, ok := srv.PickWorker()
		if !ok {
			t.Fatalf("PickWorker returned false at i=%d", i)
		}
		if err := srv.SendTaskDispatch(w, "t", "q", "s", []byte{}, 0); err != nil {
			t.Fatalf("dispatch %d to %s: %v", i, w, err)
		}
	}

	if !waitFor(t, 2*time.Second, func() bool {
		total := int32(0)
		for _, c := range counts {
			total += c.Load()
		}
		return total == n
	}) {
		total := int32(0)
		for id, c := range counts {
			t.Logf("worker %s: %d", id, c.Load())
			total += c.Load()
		}
		t.Fatalf("only %d/%d dispatches landed", total, n)
	}
	// Each worker should receive exactly n/3 with strict round-robin.
	for id, c := range counts {
		if got := c.Load(); got != int32(n/3) {
			t.Errorf("worker %s: %d dispatches, want %d", id, got, n/3)
		}
	}
}

// TestPickWorkerEmpty returns false when no workers are connected.
func TestPickWorkerEmpty(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "empty"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	if _, ok := srv.PickWorker(); ok {
		t.Fatal("PickWorker on empty server should return false")
	}
}

// TestSendTaskDispatchUnknownWorker returns ErrNotConnected when the
// named worker isn't currently registered.
func TestSendTaskDispatchUnknownWorker(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "u"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	err := srv.SendTaskDispatch("ghost", "t", "q", "s", nil, 0)
	if err == nil {
		t.Fatal("expected ErrNotConnected for unknown worker")
	}
}

// TestTaskDispatchBackpressureBlocksSend verifies that a slow handler
// applies HTTP/2 flow-control backpressure all the way back to the
// server's Send. We simulate slow consumption with a blocking handler
// and confirm Send eventually blocks once the in-flight window is full.
//
// gRPC's default flow-control window is 64 KiB; sending many small
// messages while the handler blocks fills it. We measure that a
// large-enough fan-out exhibits a measurable Send delay.
func TestTaskDispatchBackpressureBlocksSend(t *testing.T) {
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", ClusterID: "bp"}, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer srv.Stop(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ClientConfig{
		CoordAddr:        srv.Addr(),
		WorkerID:         "w-bp",
		ReconnectBackoff: 50 * time.Millisecond,
	}, nil)
	client.Start(ctx)
	defer client.Stop()

	release := make(chan struct{})
	var received atomic.Int32
	client.RegisterDispatchHandler(func(td TaskDispatch) {
		received.Add(1)
		<-release
	})

	if !waitFor(t, 2*time.Second, client.Connected) {
		t.Fatal("client never connected")
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return len(srv.ConnectedWorkers()) == 1
	}) {
		t.Fatal("server never saw worker")
	}

	// Use a large-ish payload so the flow-control window fills quickly.
	// We don't depend on a specific count; we just verify that Send
	// blocks once the window is full, by watching a sequence of sends
	// in a goroutine and confirming one of them does not complete
	// before we release the handler.
	const payloadKB = 64
	payload := make([]byte, payloadKB*1024)
	sendDone := make(chan int, 64)
	go func() {
		for i := 0; i < cap(sendDone); i++ {
			err := srv.SendTaskDispatch("w-bp", "t", "q", "s", payload, 0)
			if err != nil {
				return
			}
			sendDone <- i
		}
	}()

	// Wait for the producer to stall: at least one send completed (handler
	// received), but progress should stop before the full 64 messages
	// land because the handler is blocked.
	if !waitFor(t, 2*time.Second, func() bool { return received.Load() >= 1 }) {
		t.Fatal("handler never received first message")
	}
	time.Sleep(200 * time.Millisecond)
	if len(sendDone) >= cap(sendDone) {
		t.Fatal("all sends completed despite handler blocking — no backpressure")
	}

	// Release the handler. Producer should drain.
	close(release)
	if !waitFor(t, 3*time.Second, func() bool { return len(sendDone) == cap(sendDone) }) {
		t.Fatalf("after release: %d/%d sends drained", len(sendDone), cap(sendDone))
	}
}

