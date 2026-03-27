package distributed

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
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
	_, err := Connect("nats://192.0.2.1:4222", nil)
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

// generateTestCerts creates a CA, server cert, and client cert in dir.
// Returns paths: caCert, serverCert, serverKey, clientCert, clientKey.
func generateTestCerts(t *testing.T, dir string) (string, string, string, string, string) {
	t.Helper()

	// Generate CA key and self-signed cert
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatal(err)
	}

	writeCert := func(name string, certDER []byte, key *ecdsa.PrivateKey) (string, string) {
		certPath := filepath.Join(dir, name+".crt")
		keyPath := filepath.Join(dir, name+".key")

		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
		os.WriteFile(certPath, certPEM, 0o600)

		keyDER, _ := x509.MarshalECPrivateKey(key)
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
		os.WriteFile(keyPath, keyPEM, 0o600)

		return certPath, keyPath
	}

	// Write CA cert
	caCertPath := filepath.Join(dir, "ca.crt")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	os.WriteFile(caCertPath, caPEM, 0o600)

	// Generate and sign server cert
	serverKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverCertDER, _ := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	serverCertPath, serverKeyPath := writeCert("server", serverCertDER, serverKey)

	// Generate and sign client cert
	clientKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "worker-1"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientCertDER, _ := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	clientCertPath, clientKeyPath := writeCert("client", clientCertDER, clientKey)

	return caCertPath, serverCertPath, serverKeyPath, clientCertPath, clientKeyPath
}

func TestNATSmTLS(t *testing.T) {
	dir := t.TempDir()
	caCert, serverCert, serverKey, clientCert, clientKey := generateTestCerts(t, dir)

	// Start NATS server with mTLS
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = filepath.Join(dir, "nats")
	cfg.TLSCert = serverCert
	cfg.TLSKey = serverKey
	cfg.TLSCA = caCert

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS with mTLS: %v", err)
	}
	defer en.Shutdown()

	// Build client TLS config
	clientTLS, err := BuildNATSClientTLS(clientCert, clientKey, caCert)
	if err != nil {
		t.Fatalf("BuildNATSClientTLS: %v", err)
	}

	// Connect with mTLS — nats.Secure() handles TLS negotiation
	nc, err := Connect(en.ClientURL(), clientTLS)
	if err != nil {
		t.Fatalf("Connect with mTLS: %v", err)
	}
	defer nc.Close()

	// Verify connection works by publishing and subscribing
	sub, err := nc.SubscribeSync("test.mtls")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := nc.Publish("test.mtls", []byte("hello")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	if string(msg.Data) != "hello" {
		t.Errorf("got %q, want %q", msg.Data, "hello")
	}
}

func TestNATSmTLS_RejectsNoClientCert(t *testing.T) {
	dir := t.TempDir()
	caCert, serverCert, serverKey, _, _ := generateTestCerts(t, dir)

	// Start NATS server with mTLS
	cfg := DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = filepath.Join(dir, "nats")
	cfg.TLSCert = serverCert
	cfg.TLSKey = serverKey
	cfg.TLSCA = caCert

	en, err := NewEmbeddedNATS(cfg, nil)
	if err != nil {
		t.Fatalf("NewEmbeddedNATS: %v", err)
	}
	defer en.Shutdown()

	// Try to connect without any client cert — should fail
	_, err = Connect(en.ClientURL(), nil)
	if err == nil {
		t.Fatal("expected error connecting without client cert to mTLS server")
	}
}

func TestBuildNATSClientTLS_BadCert(t *testing.T) {
	_, err := BuildNATSClientTLS("/nonexistent/cert.pem", "/nonexistent/key.pem", "/nonexistent/ca.pem")
	if err == nil {
		t.Fatal("expected error with nonexistent cert files")
	}
}
