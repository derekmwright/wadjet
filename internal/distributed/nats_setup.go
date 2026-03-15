package distributed

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSConfig configures the embedded NATS server.
type NATSConfig struct {
	Host       string
	Port       int
	StoreDir   string // JetStream storage directory
	MaxPayload int32  // max message payload in bytes (default 8 MB)
}

// DefaultNATSConfig returns a default NATS configuration.
func DefaultNATSConfig() NATSConfig {
	return NATSConfig{
		Host:       "127.0.0.1",
		Port:       4222,
		StoreDir:   "/tmp/caelum-nats",
		MaxPayload: 8 * 1024 * 1024,
	}
}

// EmbeddedNATS manages an embedded NATS server with JetStream.
type EmbeddedNATS struct {
	server *natsserver.Server
	config NATSConfig
	logger *slog.Logger
}

// NewEmbeddedNATS creates and starts an embedded NATS server.
func NewEmbeddedNATS(cfg NATSConfig, logger *slog.Logger) (*EmbeddedNATS, error) {
	if logger == nil {
		logger = slog.Default()
	}

	opts := &natsserver.Options{
		Host:           cfg.Host,
		Port:           cfg.Port,
		NoLog:          true,
		NoSigs:         true,
		MaxPayload:     cfg.MaxPayload,
		JetStream:      true,
		StoreDir:       cfg.StoreDir,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("creating nats server: %w", err)
	}

	ns.Start()

	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		return nil, fmt.Errorf("nats server failed to become ready")
	}

	logger.Info("embedded NATS started", "host", cfg.Host, "port", cfg.Port)

	return &EmbeddedNATS{
		server: ns,
		config: cfg,
		logger: logger,
	}, nil
}

// ClientURL returns the URL for connecting to this embedded server.
func (e *EmbeddedNATS) ClientURL() string {
	return fmt.Sprintf("nats://%s:%d", e.config.Host, e.config.Port)
}

// Shutdown stops the embedded NATS server.
func (e *EmbeddedNATS) Shutdown() {
	e.server.Shutdown()
	e.server.WaitForShutdown()
	e.logger.Info("embedded NATS shut down")
}

// SetupStreams creates the required JetStream streams for Caelum.
func SetupStreams(ctx context.Context, js jetstream.JetStream) error {
	// Tasks stream: WorkQueue retention so each task is delivered to exactly one worker
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamTasks,
		Subjects:  []string{SubjectTasksAll},
		Retention: jetstream.WorkQueuePolicy,
		MaxAge:    1 * time.Hour,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("creating tasks stream: %w", err)
	}

	// Results stream: Interest retention so coordinators get results while subscribed
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamResults,
		Subjects:  []string{SubjectResultsAll},
		Retention: jetstream.InterestPolicy,
		MaxAge:    1 * time.Hour,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("creating results stream: %w", err)
	}

	return nil
}

// Connect creates a NATS client connection.
func Connect(url string) (*nats.Conn, error) {
	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				slog.Warn("NATS disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			slog.Info("NATS reconnected")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}
	return nc, nil
}

// NewJetStream creates a JetStream context from a connection.
func NewJetStream(nc *nats.Conn) (jetstream.JetStream, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("creating jetstream: %w", err)
	}
	return js, nil
}
