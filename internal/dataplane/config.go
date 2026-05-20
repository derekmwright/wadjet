// Package dataplane is the worker↔coord transport for high-rate task
// traffic that NATS JetStream is a poor fit for at scale: task dispatch,
// result batches, gather payloads, and per-task progress. Out of scope
// (stays on NATS): heartbeats, query cancellation, catalog/UDF KV, DLQ.
//
// See project_split_plane_design_2026-05-20.md for the design.
//
// Phase A: connection bootstrap only (Hello/Welcome). The streaming
// RPCs that carry tasks and results are added in Phases B–E.
package dataplane

import (
	"crypto/tls"
	"time"
)

// ServerConfig configures the coord-side gRPC listener.
type ServerConfig struct {
	// Addr is the listen address (e.g. ":9091"). Required.
	Addr string

	// ClusterID identifies this cluster; sent to workers in Welcome.
	ClusterID string

	// TLSConfig enables TLS when non-nil. Default (nil) is plaintext —
	// intra-cluster traffic on a trusted private network. Set for
	// less-trusted deployments. NATS control plane keeps its own mTLS
	// regardless of this setting.
	TLSConfig *tls.Config

	// MaxConcurrentStreams caps connected workers. 0 = unlimited.
	MaxConcurrentStreams uint32
}

// ClientConfig configures the worker-side gRPC dialer.
type ClientConfig struct {
	// CoordAddr is the coord's data-plane endpoint (host:port). Required.
	CoordAddr string

	// WorkerID is sent to coord in Hello. Required.
	WorkerID string

	// BuildSHA is sent to coord for version-skew detection.
	BuildSHA string

	// TLSConfig enables TLS when non-nil. Must match the server's choice.
	TLSConfig *tls.Config

	// ReconnectBackoff is the initial backoff after disconnect.
	// Default: 500ms, doubling up to 10s.
	ReconnectBackoff time.Duration
}
