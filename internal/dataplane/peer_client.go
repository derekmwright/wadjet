package dataplane

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	dpv1 "github.com/citc-tech/wadjet/gen/dataplane/v1"
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
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dataplane: peer dial %s: %w", addr, err)
	}
	c.conns[addr] = conn
	return conn, nil
}

// FetchShuffle opens a fetch stream for (queryID, key) against the peer at
// addr and returns a reader over the file bytes. The returned reader must be
// closed; closing cancels the stream. Errors — dial failure, PermissionDenied,
// NotFound, mid-stream disconnect — surface from Read; the caller treats any
// of them as a cache miss and falls through to S3.
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
	return &peerFetchReader{stream: stream, cancel: cancel}, nil
}

// peerFetchReader adapts a FetchShuffle stream to io.ReadCloser.
type peerFetchReader struct {
	stream grpc.ServerStreamingClient[dpv1.ShuffleChunk]
	cancel context.CancelFunc
	buf    []byte
}

func (r *peerFetchReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		chunk, err := r.stream.Recv()
		if err != nil {
			// io.EOF (clean end) passes through; every other status —
			// NotFound, PermissionDenied, Unavailable, mid-stream reset —
			// surfaces as a read error the caller treats as a miss.
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
