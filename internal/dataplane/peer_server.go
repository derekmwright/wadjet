package dataplane

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/klauspost/compress/s2"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
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
// consumer falls through to S3. ctx is the fetch stream's context — a
// resolver that does real work (base-table owner read-through) bounds its
// wait on it.
type ShuffleFileResolver interface {
	ResolveShuffleFile(ctx context.Context, queryID, key, token string) (string, error)
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

	// CompressWire s2-compresses raw WSHF payloads on the stream
	// (docs/design/peer-wire-compression.md): the served bytes become a
	// standard WSHC envelope, which every consumer already decodes.
	// Payloads that are already WSHC (or not WSHF at all) pass through
	// untouched. Trades ~1 core-GB/s of producer CPU per stream for
	// ~20% fewer wire bytes (the s2 ratio measured on SF100 shuffle
	// data). Default false pending validation.
	CompressWire bool
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
	// Keepalive: the mirror image of the client-side idle bound
	// (peer_client.go PeerFetchIdleTimeout). A stream's Send blocks on
	// HTTP/2 flow control, so a consumer that dies or stops reading
	// wedges the serving goroutine and permanently leaks its concurrency
	// slot — the semaphore's acquire timeout then rejects every later
	// fetch. Server keepalive reaps dead transports in bounded time,
	// firing the stream context and releasing the slot. The enforcement
	// policy admits the client's 30s pings.
	opts = append(opts,
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}))
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

	path, err := s.resolver.ResolveShuffleFile(ctx, req.GetQueryId(), req.GetKey(), req.GetToken())
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

	if s.cfg.CompressWire {
		// Sniff the payload: raw WSHF is compressed into a WSHC envelope
		// on the stream; anything else (already-WSHC, short, foreign)
		// passes through byte-identical via the head+rest copy below.
		var head [4]byte
		n, herr := io.ReadFull(f, head[:])
		if herr == nil && head == wshfMagic {
			return s.streamCompressed(ctx, io.MultiReader(bytes.NewReader(head[:]), f), stream)
		}
		return s.streamRaw(ctx, io.MultiReader(bytes.NewReader(head[:n]), f), stream)
	}
	return s.streamRaw(ctx, f, stream)
}

// Wire-format magics, mirroring internal/worker shuffle_format.go
// (shuffleMagic / compressedMagic — dataplane cannot import worker without
// a cycle; these four-byte constants ARE the wire contract).
var (
	wshfMagic = [4]byte{'W', 'S', 'H', 'F'}
	wshcMagic = [4]byte{'W', 'S', 'H', 'C'}
)

// streamRaw copies r to the stream in peerChunkBytes frames.
func (s *PeerServer) streamRaw(ctx context.Context, r io.Reader, stream grpc.ServerStreamingServer[dpv1.ShuffleChunk]) error {
	buf := make([]byte, peerChunkBytes)
	for {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		n, rerr := r.Read(buf)
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

// streamCompressed sends a WSHC envelope: the magic, then the s2 stream of
// the raw payload, chunked into peerChunkBytes frames. The consumer's
// existing WSHC decode path (magic sniff in openShuffleFromPeer) handles
// the result; its peer-tier byte ledger counts the compressed wire bytes.
func (s *PeerServer) streamCompressed(ctx context.Context, r io.Reader, stream grpc.ServerStreamingServer[dpv1.ShuffleChunk]) error {
	cw := &chunkStreamWriter{ctx: ctx, stream: stream, buf: make([]byte, 0, peerChunkBytes)}
	if _, err := cw.Write(wshcMagic[:]); err != nil {
		return err
	}
	s2w := s2.NewWriter(cw)
	if _, err := io.Copy(s2w, r); err != nil {
		_ = s2w.Close()
		if serr, ok := status.FromError(err); ok && serr.Code() != codes.Unknown {
			return err
		}
		return status.Errorf(codes.Internal, "compressing stream: %v", err)
	}
	if err := s2w.Close(); err != nil {
		return status.Errorf(codes.Internal, "flushing s2 stream: %v", err)
	}
	return cw.Flush()
}

// chunkStreamWriter adapts stream.Send to io.Writer, buffering to
// peerChunkBytes frames. Send errors (including consumer cancellation)
// surface as write errors and abort the s2 copy.
type chunkStreamWriter struct {
	ctx    context.Context
	stream grpc.ServerStreamingServer[dpv1.ShuffleChunk]
	buf    []byte
}

func (w *chunkStreamWriter) Write(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		if err := w.ctx.Err(); err != nil {
			return 0, status.FromContextError(err).Err()
		}
		room := peerChunkBytes - len(w.buf)
		n := min(room, len(p))
		w.buf = append(w.buf, p[:n]...)
		p = p[n:]
		if len(w.buf) == peerChunkBytes {
			if err := w.Flush(); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func (w *chunkStreamWriter) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	err := w.stream.Send(&dpv1.ShuffleChunk{Data: w.buf})
	w.buf = w.buf[:0]
	return err
}
