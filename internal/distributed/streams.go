// SPDX-License-Identifier: AGPL-3.0-only

package distributed

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/derekmwright/wadjet/internal/natsconn"
)

// The NATS connection helpers themselves live in internal/natsconn, which
// the embedded CLI links without linking this package. These names stay here
// because every caller in the distributed engine already spells them this
// way; they forward, and add nothing.
type (
	// NATSConfig configures the embedded NATS server.
	NATSConfig = natsconn.NATSConfig
	// EmbeddedNATS is an in-process NATS server with JetStream enabled.
	EmbeddedNATS = natsconn.EmbeddedNATS
)

// DefaultNATSConfig returns a default NATS configuration.
func DefaultNATSConfig() NATSConfig { return natsconn.DefaultNATSConfig() }

// NewEmbeddedNATS starts an embedded NATS server with JetStream enabled.
func NewEmbeddedNATS(cfg NATSConfig, logger *slog.Logger) (*EmbeddedNATS, error) {
	return natsconn.NewEmbeddedNATS(cfg, logger)
}

// Connect creates a NATS client connection over TCP.
func Connect(url string, tlsCfg *tls.Config) (*nats.Conn, error) {
	return natsconn.Connect(url, tlsCfg)
}

// BuildNATSClientTLS creates a TLS config for a NATS client.
func BuildNATSClientTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	return natsconn.BuildNATSClientTLS(certFile, keyFile, caFile)
}

// ConnectInProcess creates a NATS client connection using in-process
// communication.
func ConnectInProcess(server *natsserver.Server) (*nats.Conn, error) {
	return natsconn.ConnectInProcess(server)
}

// NewJetStream creates a JetStream context from a connection.
func NewJetStream(nc *nats.Conn) (jetstream.JetStream, error) {
	return natsconn.NewJetStream(nc)
}

// SetupStreams creates the required JetStream streams for Wadjet.
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

	// Priority task lane: same WorkQueue semantics, separate stream so its
	// consumer filter can't overlap the main tasks consumer (WorkQueue
	// retention forbids overlapping filters).
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamPriTasks,
		Subjects:  []string{SubjectPriTasksAll},
		Retention: jetstream.WorkQueuePolicy,
		MaxAge:    1 * time.Hour,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("creating priority tasks stream: %w", err)
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

	// Dead-letter queue: retains failed tasks for 48 hours for inspection/retry
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      StreamDLQ,
		Subjects:  []string{SubjectDLQAll},
		Retention: jetstream.LimitsPolicy,
		MaxAge:    48 * time.Hour,
		MaxMsgs:   10000,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("creating DLQ stream: %w", err)
	}

	return nil
}
