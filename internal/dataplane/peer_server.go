package dataplane

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	dpv1 "github.com/citc-tech/wadjet/gen/dataplane/v1"
)

// peerChunkBytes is the per-frame payload size for FetchShuffle streams.
// 256 KiB is the proven shuffle write-buffer size (partitioned_shuffle_sink)
// and stays comfortably under gRPC's default 4 MiB message cap.
const peerChunkBytes = 256 * 1024

// peerServeAcquireTimeout bounds how long an incoming fetch may wait for a
// serve slot before being rejected with ResourceExhausted. The consumer
// falls through to S3 on rejection, so bounding the wait server-side keeps
// a saturated producer from silently stalling its consumers — flow control
// past this point is HTTP/2's job, never sleeps.
const peerServeAcquireTimeout = 10 * time.Second

// ShuffleFileResolver maps a fetch request to a local file path. The worker
// implements it over its LocalStageCache + per-query fetch tokens.
// Implementations return ErrPeerDenied for a bad token and ErrPeerNotFound
// for a key the worker doesn't hold; both are terminal for the fetch and the
// consumer falls through to S3.
type ShuffleFileResolver interface {
	ResolveShuffleFile(queryID, key, token string) (string, error)
}

// Sentinel errors for ShuffleFileResolver implementations.
var (
	ErrPeerDenied   = errors.New("peer fetch denied")
	ErrPeerNotFound = errors.New("peer file not found")
)

// PeerServerConfig configures a worker's PeerExchange listener.
type PeerServerConfig struct {
	// Addr is the listen address (e.g. ":9095", or ":0" for tests). Required.
	Addr string

	// AdvertiseAddr is the externally-dialable address peers use, carried in
	// worker heartbeats. When empty it is derived from the bound listener:
	// the listener's host if it is a specific IP, else the first
	// non-loopback unicast IPv4 (falling back to 127.0.0.1).
	AdvertiseAddr string

	// TLSConfig enables TLS when non-nil. Default (nil) is plaintext —
	// matching the coord data plane's intra-cluster trust posture.
	TLSConfig *tls.Config

	// MaxConcurrentFetches caps concurrently-served FetchShuffle streams,
	// protecting the producer's NVMe/NIC from consumer fan-in spikes.
	// 0 = default (16).
	MaxConcurrentFetches int
}

// PeerServer is the worker-side PeerExchange gRPC listener. Serving is a
// resolver lookup + chunked file copy; all state lives in the resolver.
type PeerServer struct {
	dpv1.UnimplementedPeerExchangeServer

	cfg      PeerServerConfig
	resolver ShuffleFileResolver
	logger   *slog.Logger

	grpcSrv *grpc.Server
	lis     net.Listener
	sem     chan struct{}
}

// NewPeerServer constructs a PeerServer. Call Start to begin accepting.
func NewPeerServer(cfg PeerServerConfig, resolver ShuffleFileResolver, logger *slog.Logger) *PeerServer {
	if logger == nil {
		logger = slog.Default()
	}
	maxFetches := cfg.MaxConcurrentFetches
	if maxFetches <= 0 {
		maxFetches = 16
	}
	return &PeerServer{
		cfg:      cfg,
		resolver: resolver,
		logger:   logger.With("component", "dataplane.peer_server"),
		sem:      make(chan struct{}, maxFetches),
	}
}

// Start binds the listener and runs the gRPC server in the background.
func (s *PeerServer) Start() error {
	lis, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("dataplane: peer listen on %s: %w", s.cfg.Addr, err)
	}
	s.lis = lis

	var opts []grpc.ServerOption
	if s.cfg.TLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(s.cfg.TLSConfig)))
	} else {
		opts = append(opts, grpc.Creds(insecure.NewCredentials()))
	}
	s.grpcSrv = grpc.NewServer(opts...)
	dpv1.RegisterPeerExchangeServer(s.grpcSrv, s)

	go func() {
		if err := s.grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.logger.Error("peer serve loop exited", "err", err)
		}
	}()

	s.logger.Info("peer exchange listening",
		"addr", lis.Addr().String(),
		"advertise", s.AdvertiseAddr(),
		"tls", s.cfg.TLSConfig != nil)
	return nil
}

// Stop shuts the server down immediately. In-flight fetches abort; their
// consumers fall through to S3.
func (s *PeerServer) Stop() {
	if s.grpcSrv != nil {
		s.grpcSrv.Stop()
	}
}

// AdvertiseAddr returns the address peers should dial, for the worker to
// carry in its heartbeats. Empty until Start has bound the listener (unless
// an explicit AdvertiseAddr was configured).
func (s *PeerServer) AdvertiseAddr() string {
	if s.cfg.AdvertiseAddr != "" {
		return s.cfg.AdvertiseAddr
	}
	if s.lis == nil {
		return ""
	}
	bound, ok := s.lis.Addr().(*net.TCPAddr)
	if !ok {
		return s.lis.Addr().String()
	}
	host := bound.IP
	if host == nil || host.IsUnspecified() {
		host = firstUnicastIPv4()
	}
	return net.JoinHostPort(host.String(), fmt.Sprintf("%d", bound.Port))
}

// firstUnicastIPv4 returns the first non-loopback unicast IPv4 on any
// interface, falling back to loopback. Best-effort — deployments that need
// a specific address set AdvertiseAddr explicitly.
func firstUnicastIPv4() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipn.IP.To4()
			if ip4 != nil && !ip4.IsLoopback() {
				return ip4
			}
		}
	}
	return net.IPv4(127, 0, 0, 1)
}

// FetchShuffle implements PeerExchangeServer: resolve the key to a local
// file and stream it in peerChunkBytes frames. Backpressure per stream is
// HTTP/2 flow control; the semaphore only bounds fan-in concurrency.
func (s *PeerServer) FetchShuffle(req *dpv1.FetchShuffleRequest, stream grpc.ServerStreamingServer[dpv1.ShuffleChunk]) error {
	ctx := stream.Context()
	acquireTimer := time.NewTimer(peerServeAcquireTimeout)
	defer acquireTimer.Stop()
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-acquireTimer.C:
		return status.Error(codes.ResourceExhausted, "peer fetch concurrency cap held past bound")
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}

	path, err := s.resolver.ResolveShuffleFile(req.GetQueryId(), req.GetKey(), req.GetToken())
	switch {
	case errors.Is(err, ErrPeerDenied):
		return status.Error(codes.PermissionDenied, "peer fetch denied")
	case errors.Is(err, ErrPeerNotFound):
		return status.Error(codes.NotFound, "key not held locally")
	case err != nil:
		return status.Errorf(codes.Internal, "resolving shuffle file: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		// The file was resolved but vanished (e.g. query cleanup raced the
		// fetch). NotFound → the consumer falls through to S3.
		return status.Errorf(codes.NotFound, "opening local file: %v", err)
	}
	defer f.Close()

	buf := make([]byte, peerChunkBytes)
	for {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			if serr := stream.Send(&dpv1.ShuffleChunk{Data: buf[:n]}); serr != nil {
				return serr
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return nil
			}
			return status.Errorf(codes.Internal, "reading local file: %v", rerr)
		}
	}
}
