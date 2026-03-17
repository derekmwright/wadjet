package distributed

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestNewEmbeddedNATS(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1 // random port
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	en, err := NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	defer en.Shutdown()

	url := en.ClientURL()
	if url == "" {
		t.Fatal("ClientURL should not be empty")
	}

	srv := en.Server()
	if srv == nil {
		t.Fatal("Server should not be nil")
	}
}

func TestNewEmbeddedNATSWithNilLogger(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS with nil logger: %v", err)
	}
	defer en.Shutdown()

	if en.ClientURL() == "" {
		t.Fatal("ClientURL should not be empty")
	}
}

func TestNewEmbeddedNATSInvalidStoreDir(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = "/proc/this-should-fail-on-linux"

	_, err := NewEmbeddedNATS(cfg, nil)
	if err == nil {
		t.Fatal("expected error with invalid store dir")
	}
}

func TestConnectInProcess(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("ConnectInProcess: %v", err)
	}
	defer nc.Close()

	if !nc.IsConnected() {
		t.Fatal("expected connected")
	}
}

func TestNewJetStream(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("ConnectInProcess: %v", err)
	}
	defer nc.Close()

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	if js == nil {
		t.Fatal("JetStream should not be nil")
	}
}

func TestSetupStreams(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("ConnectInProcess: %v", err)
	}
	defer nc.Close()

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams: %v", err)
	}

	// Verify streams exist
	stream, err := js.Stream(ctx, StreamTasks)
	if err != nil {
		t.Fatalf("getting tasks stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("getting stream info: %v", err)
	}
	if info.Config.Name != StreamTasks {
		t.Errorf("tasks stream name: got %q, want %q", info.Config.Name, StreamTasks)
	}

	stream2, err := js.Stream(ctx, StreamResults)
	if err != nil {
		t.Fatalf("getting results stream: %v", err)
	}
	info2, err := stream2.Info(ctx)
	if err != nil {
		t.Fatalf("getting stream info: %v", err)
	}
	if info2.Config.Name != StreamResults {
		t.Errorf("results stream name: got %q, want %q", info2.Config.Name, StreamResults)
	}
}

func TestSetupStreamsIdempotent(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	defer en.Shutdown()

	nc, err := ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("ConnectInProcess: %v", err)
	}
	defer nc.Close()

	js, err := NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	ctx := context.Background()

	// Call SetupStreams twice to verify idempotency
	if err := SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams first call: %v", err)
	}
	if err := SetupStreams(ctx, js); err != nil {
		t.Fatalf("SetupStreams second call: %v", err)
	}
}

func TestEmbeddedNATSWithClusterID(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	cfg.ClusterID = "test-cluster"

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	defer en.Shutdown()

	if en.ClientURL() == "" {
		t.Fatal("ClientURL should not be empty")
	}
}

func TestEmbeddedNATSWithLeafRemotes(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	// Use a non-routable address so it won't actually connect
	cfg.LeafRemotes = []string{"nats://192.0.2.1:4222"}

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS with leaf remotes: %v", err)
	}
	defer en.Shutdown()
}

func TestEmbeddedNATSWithInvalidLeafRemote(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	cfg.LeafRemotes = []string{"://invalid-url"}

	_, err := NewEmbeddedNATS(cfg, nil)
	if err == nil {
		t.Fatal("expected error with invalid leaf remote URL")
	}
}

func TestConnectInvalidURL(t *testing.T) {
	_, err := Connect("nats://192.0.2.1:4222")
	if err == nil {
		t.Fatal("expected error connecting to non-existent server")
	}
}

func TestShutdownSequence(t *testing.T) {
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}

	// Shutdown should be safe to call
	en.Shutdown()

	// After shutdown, server should no longer be ready
	if en.Server().ReadyForConnections(100 * time.Millisecond) {
		t.Fatal("server should not be ready after shutdown")
	}
}
