// SPDX-License-Identifier: MIT

// Package natsconn opens NATS: the embedded server, the client connections
// and the JetStream context. It is the transport a catalog is reached over,
// not the distributed engine that runs on it — internal/distributed keeps
// the subjects, the message envelopes and the streams, and re-exports these
// names so its own callers are unchanged.
//
// It lives on its own so the embedded CLI (`wadjet tables`, `query`, `shell`)
// can open the catalog of a running server without linking the coordinator
// or the worker. See LICENSING.md.
package natsconn

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSConfig configures the embedded NATS server.
type NATSConfig struct {
	Host        string
	Port        int
	StoreDir    string   // JetStream storage directory
	MaxPayload  int32    // max message payload in bytes (default 8 MB)
	ClusterID   string   // unique cluster identifier (e.g., "central", "afb-east")
	LeafRemotes []string // remote NATS URLs for leaf node connections (edge → central)
	TLSCert     string   // TLS certificate file (server cert for coordinator)
	TLSKey      string   // TLS private key file
	TLSCA       string   // CA certificate for verifying client certs (enables mTLS)
}

// DefaultNATSConfig returns a default NATS configuration.
// StoreDir defaults to ~/.wadjet/nats for persistence across reboots.
func DefaultNATSConfig() NATSConfig {
	storeDir := filepath.Join(os.Getenv("HOME"), ".wadjet", "nats")
	return NATSConfig{
		Host:       "127.0.0.1",
		Port:       4222,
		StoreDir:   storeDir,
		MaxPayload: 8 * 1024 * 1024,
	}
}

// natsSlogLogger adapts the nats-server Logger interface onto our slog
// pipeline. We only use this for the embedded NATS server inside coord
// (and tests). Notice/Warn/Error/Fatal map naturally; Debug/Trace map
// to slog.Debug since we leave debug=trace=false at SetLogger time.
type natsSlogLogger struct{ l *slog.Logger }

func newNATSSlogLogger(base *slog.Logger) *natsSlogLogger {
	if base == nil {
		base = slog.Default()
	}
	return &natsSlogLogger{l: base.With("component", "nats")}
}

func (n *natsSlogLogger) Noticef(format string, v ...any) { n.l.Info(fmt.Sprintf(format, v...)) }
func (n *natsSlogLogger) Warnf(format string, v ...any)   { n.l.Warn(fmt.Sprintf(format, v...)) }
func (n *natsSlogLogger) Fatalf(format string, v ...any) {
	n.l.Error(fmt.Sprintf(format, v...), "level", "fatal")
}
func (n *natsSlogLogger) Errorf(format string, v ...any) { n.l.Error(fmt.Sprintf(format, v...)) }
func (n *natsSlogLogger) Debugf(format string, v ...any) { n.l.Debug(fmt.Sprintf(format, v...)) }
func (n *natsSlogLogger) Tracef(format string, v ...any) {
	n.l.Debug(fmt.Sprintf(format, v...), "trace", true)
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
		Host: cfg.Host,
		Port: cfg.Port,
		// NoLog=false so we can install a custom logger after Start(). The
		// SF10 mass-reap pattern (5 workers across 3 hosts going silent
		// within 7s) is consistent with NATS server-side slow-consumer
		// drops, and we historically had NoLog=true so the server's drop
		// warnings were invisible. With this off + the SetLogger call
		// below, slow-consumer events surface in the coord log.
		NoLog:      false,
		NoSigs:     true,
		MaxPayload: cfg.MaxPayload,
		JetStream:  true,
		StoreDir:   cfg.StoreDir,
		ServerName: cfg.ClusterID,
	}

	// Configure mTLS on the NATS server if cert/key/CA are provided.
	if cfg.TLSCert != "" && cfg.TLSKey != "" && cfg.TLSCA != "" {
		caCert, err := os.ReadFile(cfg.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("reading NATS CA file: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("parsing NATS CA certificate from %s", cfg.TLSCA)
		}

		serverCert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("loading NATS server certificate: %w", err)
		}

		opts.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caPool,
			MinVersion:   tls.VersionTLS12,
		}
		opts.TLS = true
		opts.TLSVerify = true // require client certs
		opts.TLSTimeout = 5   // seconds
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

	// Install slog-backed logger BEFORE Start() so any startup messages
	// reach our log instead of stdout. Trace=false keeps volume down;
	// flip to true when chasing a specific NATS-internal issue.
	ns.SetLogger(newNATSSlogLogger(logger), false, false)

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
	if cfg.TLSCert != "" {
		logFields = append(logFields, "tls", true)
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

// asyncErrors is the asynchronous error handler installed on every
// connection this package opens, in place of nats.go's default handler.
//
// The default (nats.go v1.51.0, defaultErrHandler) writes EVERY asynchronous
// error to stderr as "<error> on connection [<cid>]", followed by
// "for subscription on <subject>" when the error concerns a subscription.
// This handler keeps all of that: every asynchronous error is logged at Error
// level through slog's default logger (stderr unless the process installs
// another default; nothing in this repository does), with the error, the
// connection id and the subscription's subject.
//
// A slow-consumer error also carries the subscription's dropped count. What
// that line can and cannot say (repeated in its own attributes and in
// docs/internals/worker-poison-task-delivery-defense.md):
//   - nats.go reports it once per slow-consumer EPISODE (on the transition
//     into slow-consumer state, re-armed by the next message the
//     subscription accepts), not once per dropped message;
//   - dropped_total is the subscription's cumulative count when the line is
//     written, and it counts every dropped frame, heartbeats and 408 status
//     frames included, not only tasks;
//   - for a JetStream pull iterator the subject is the iterator's private
//     inbox (_INBOX.…). The public iterator API does not expose that inbox,
//     so this line alone cannot name the intake lane or the task.
//
// The callback runs on nats.go's asynchronous-callback goroutine. It makes no
// server request and does no I/O beyond the log line. The only lock it takes
// is the subscription's own mutex, briefly, inside Subscription.Dropped (the
// default handler takes the same mutex to read the subject). It does not take
// the connection's lock: the connection id is cached on connect and on
// reconnect instead of read through Conn.GetClientID in the callback.
type asyncErrors struct {
	cid atomic.Uint64
}

// noteConnected caches the server-assigned connection id. It is called after
// the initial connect and from the reconnect handler, never from handle.
func (a *asyncErrors) noteConnected(nc *nats.Conn) {
	if id, err := nc.GetClientID(); err == nil {
		a.cid.Store(id)
	}
}

func (a *asyncErrors) handle(_ *nats.Conn, sub *nats.Subscription, err error) {
	attrs := []any{"error", err, "connection_id", a.cid.Load()}
	if sub != nil {
		attrs = append(attrs, "subject", sub.Subject)
	}
	if !errors.Is(err, nats.ErrSlowConsumer) {
		slog.Error("NATS asynchronous error", attrs...)
		return
	}
	if sub != nil {
		if dropped, derr := sub.Dropped(); derr == nil {
			attrs = append(attrs, "dropped_total", dropped)
		}
	}
	attrs = append(attrs,
		"reported", "once per slow-consumer episode, not per message",
		"dropped_counts", "every dropped frame, heartbeats and 408 status frames included, not only tasks",
		"attribution", "a JetStream pull iterator's subject is its private inbox; this line does not name the lane or the task")
	slog.Error("NATS slow consumer: messages dropped client-side", attrs...)
}

// Connect creates a NATS client connection over TCP.
// If tlsCfg is non-nil, the connection uses TLS (mTLS when client certs are configured).
func Connect(url string, tlsCfg *tls.Config) (*nats.Conn, error) {
	ae := &asyncErrors{}
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				slog.Warn("NATS disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			ae.noteConnected(nc)
			slog.Info("NATS reconnected")
		}),
		nats.ErrorHandler(ae.handle),
	}
	if tlsCfg != nil {
		opts = append(opts, nats.Secure(tlsCfg))
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}
	ae.noteConnected(nc)
	return nc, nil
}

// BuildNATSClientTLS creates a TLS config for a NATS client (worker connecting to coordinator).
// certFile/keyFile are the client certificate and key; caFile is the CA that signed the server cert.
func BuildNATSClientTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("loading NATS client certificate: %w", err)
	}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading NATS CA file: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("parsing NATS CA certificate from %s", caFile)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ConnectInProcess creates a NATS client connection using in-process communication.
// This avoids TCP overhead when the client is co-located with the embedded NATS server.
func ConnectInProcess(server *natsserver.Server) (*nats.Conn, error) {
	ae := &asyncErrors{}
	nc, err := nats.Connect(server.ClientURL(),
		nats.InProcessServer(server),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				slog.Warn("NATS disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			ae.noteConnected(nc)
			slog.Info("NATS reconnected")
		}),
		nats.ErrorHandler(ae.handle),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS in-process: %w", err)
	}
	ae.noteConnected(nc)
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
