package distributed

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSConfig configures the embedded NATS server.
type NATSConfig struct {
	Host        string
	Port        int
	StoreDir    string // JetStream storage directory
	MaxPayload  int32  // max message payload in bytes (default 8 MB)
	ClusterID   string // unique cluster identifier (e.g., "central", "afb-east")
	LeafRemotes []string // remote NATS URLs for leaf node connections (edge → central)
}

// DefaultNATSConfig returns a default NATS configuration.
// StoreDir defaults to ~/.caelum/nats for persistence across reboots.
func DefaultNATSConfig() NATSConfig {
	storeDir := filepath.Join(os.Getenv("HOME"), ".caelum", "nats")
	return NATSConfig{
		Host:       "127.0.0.1",
		Port:       4222,
		StoreDir:   storeDir,
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

	// Ensure store directory exists for JetStream persistence.
	if err := os.MkdirAll(cfg.StoreDir, 0o750); err != nil {
		return nil, fmt.Errorf("creating NATS store dir %q: %w", cfg.StoreDir, err)
	}

	opts := &natsserver.Options{
		Host:           cfg.Host,
		Port:           cfg.Port,
		NoLog:          true,
		NoSigs:         true,
		MaxPayload:     cfg.MaxPayload,
		JetStream:      true,
		StoreDir:       cfg.StoreDir,
		ServerName:     cfg.ClusterID,
	}

	// Configure leaf node connections to remote clusters
	if len(cfg.LeafRemotes) > 0 {
		var remotes []*natsserver.RemoteLeafOpts
		for _, remote := range cfg.LeafRemotes {
			u, err := url.Parse(remote)
			if err != nil {
				return nil, fmt.Errorf("parsing leaf remote URL %q: %w", remote, err)
			}
			remotes = append(remotes, &natsserver.RemoteLeafOpts{
				URLs: []*url.URL{u},
			})
		}
		opts.LeafNode = natsserver.LeafNodeOpts{
			Remotes: remotes,
		}
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

	logFields := []any{"host", cfg.Host, "port", cfg.Port}
	if cfg.ClusterID != "" {
		logFields = append(logFields, "cluster_id", cfg.ClusterID)
	}
	if len(cfg.LeafRemotes) > 0 {
		logFields = append(logFields, "leaf_remotes", cfg.LeafRemotes)
	}
	logger.Info("embedded NATS started", logFields...)

	return &EmbeddedNATS{
		server: ns,
		config: cfg,
		logger: logger,
	}, nil
}

// ClientURL returns the URL for connecting to this embedded server.
func (e *EmbeddedNATS) ClientURL() string {
	return e.server.ClientURL()
}

// Server returns the underlying NATS server for in-process connections.
func (e *EmbeddedNATS) Server() *natsserver.Server {
	return e.server
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

// Connect creates a NATS client connection over TCP.
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

// ConnectInProcess creates a NATS client connection using in-process communication.
// This avoids TCP overhead when the client is co-located with the embedded NATS server.
func ConnectInProcess(server *natsserver.Server) (*nats.Conn, error) {
	nc, err := nats.Connect(server.ClientURL(),
		nats.InProcessServer(server),
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
		return nil, fmt.Errorf("connecting to NATS in-process: %w", err)
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
