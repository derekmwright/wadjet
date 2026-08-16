package dataplane

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	dpv1 "github.com/derekmwright/wadjet/gen/dataplane/v1"
)

// PeerClient dials worker PeerExchange endpoints and streams shuffle files.
// One cached ClientConn per peer address; connections are lazy (grpc.NewClient)
// and RPCs fail fast when a peer is unreachable — the caller falls through to
// S3, so a dead peer costs one failed dial, not a stall.
type PeerClient struct {
	tlsConfig *tls.Config

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPeerClient constructs a PeerClient. tlsConfig nil = plaintext, matching
// the peer server's default intra-cluster posture.
func NewPeerClient(tlsConfig *tls.Config) *PeerClient {
	return &PeerClient{
		tlsConfig: tlsConfig,
		conns:     make(map[string]*grpc.ClientConn),
	}
}

// Close releases every cached connection.
func (c *PeerClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, addr)
	}
}

func (c *PeerClient) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	creds := insecure.NewCredentials()
	if c.tlsConfig != nil {
		creds = credentials.NewTLS(c.tlsConfig)
	}
	// Keepalive: a peer that restarts (or a transport that half-dies)
	// must fail RPCs in bounded time, not strand streams on a dead
	// connection. 30s probe / 10s timeout keeps the probe traffic
	// negligible against shuffle volumes.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}))
	if err != nil {
		return nil, fmt.Errorf("dataplane: peer dial %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

// PeerFetchIdleTimeout bounds the wait for each stream chunk. The server
// streams from local NVMe, so inter-chunk gaps are normally microseconds;
// a stream that stops delivering WITHOUT erroring is a wedged or half-dead
// peer, and it must become an error the tiered read path can fall through
// on — the 2026-08-11 Q21-R2 stall was one such silent stream holding an
// eager hash-join build (and with it the whole cluster's barrier) for
// 228s, invisible to every phase timer and to the tier fallback, which
// only reacts to errors. Var so tests can shrink it.
var PeerFetchIdleTimeout = 15 * time.Second

// FetchShuffle opens a fetch stream for (queryID, key) against the peer at
// addr and returns a reader over the file bytes. The returned reader must be
// closed; closing cancels the stream. Errors — dial failure, PermissionDenied,
// NotFound, mid-stream disconnect, or an inter-chunk gap exceeding
// PeerFetchIdleTimeout — surface from Read; the caller treats any of them
// as a cache miss and falls through to S3.
func (c *PeerClient) FetchShuffle(ctx context.Context, addr, queryID, key, token string) (io.ReadCloser, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := dpv1.NewPeerExchangeClient(conn).FetchShuffle(streamCtx, &dpv1.FetchShuffleRequest{
		QueryId: queryID,
		Key:     key,
		Token:   token,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("dataplane: peer fetch %s from %s: %w", key, addr, err)
	}
	return &peerFetchReader{stream: stream, cancel: cancel, idle: PeerFetchIdleTimeout}, nil
}

// peerFetchReader adapts a FetchShuffle stream to io.ReadCloser.
type peerFetchReader struct {
	stream   grpc.ServerStreamingClient[dpv1.ShuffleChunk]
	cancel   context.CancelFunc
	buf      []byte
	idle     time.Duration // per-Recv inactivity bound; <= 0 disables
	timedOut atomic.Bool   // set by the idle timer goroutine, read after Recv fails
}

func (r *peerFetchReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		var idleTimer *time.Timer
		if r.idle > 0 {
			// Cancel the stream context if the next chunk doesn't arrive
			// within the bound — Recv then returns Canceled, which the
			// caller treats like any other peer failure (fall through to
			// the durable copy). Stopped as soon as the chunk lands, so a
			// healthy stream never sees it.
			idleTimer = time.AfterFunc(r.idle, func() {
				r.timedOut.Store(true)
				r.cancel()
			})
		}
		chunk, err := r.stream.Recv()
		if idleTimer != nil {
			idleTimer.Stop()
		}
		if err != nil {
			// io.EOF (clean end) passes through; every other status —
			// NotFound, PermissionDenied, Unavailable, mid-stream reset —
			// surfaces as a read error the caller treats as a miss.
			if r.timedOut.Load() {
				return 0, fmt.Errorf("dataplane: peer stream idle > %s (wedged peer): %w", r.idle, err)
			}
			return 0, err
		}
		r.buf = chunk.GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *peerFetchReader) Close() error {
	r.cancel()
	return nil
}
