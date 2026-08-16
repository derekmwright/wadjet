package dataplane

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	dpv1 "github.com/derekmwright/wadjet/gen/dataplane/v1"
)

// wedgedStream mimics the 2026-08-11 Q21-R2 failure: the server delivers
// some chunks, then stops sending WITHOUT closing the stream. Recv blocks
// until the stream context is cancelled — exactly what a half-dead or
// wedged peer looks like to gRPC without keepalive.
type wedgedStream struct {
	grpc.ClientStream // unused methods panic if called; Read only calls Recv
	ctx    context.Context
	chunks [][]byte
}

func (s *wedgedStream) Recv() (*dpv1.ShuffleChunk, error) {
	if len(s.chunks) > 0 {
		c := s.chunks[0]
		s.chunks = s.chunks[1:]
		return &dpv1.ShuffleChunk{Data: c}, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

// TestPeerFetchIdleTimeout: a stream that goes silent mid-file must
// surface a wedged-peer error from Read within the idle bound — never
// block indefinitely. The tiered read path falls through to the durable
// copy only on error, so silence here held a cluster barrier for 228s.
func TestPeerFetchIdleTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &peerFetchReader{
		stream: &wedgedStream{ctx: ctx, chunks: [][]byte{[]byte("first chunk")}},
		cancel: cancel,
		idle:   50 * time.Millisecond,
	}

	buf := make([]byte, 64)
	n, err := r.Read(buf)
	if err != nil || string(buf[:n]) != "first chunk" {
		t.Fatalf("healthy first chunk: n=%d err=%v", n, err)
	}

	start := time.Now()
	_, err = r.Read(buf)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Read succeeded on a wedged stream; want idle-timeout error")
	}
	if !strings.Contains(err.Error(), "idle") {
		t.Fatalf("error does not mark the wedge: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Read blocked %s on a wedged stream; want ~50ms", elapsed)
	}
}

// TestPeerFetchIdleHealthyRoundTrip: the idle bound must be invisible to a
// healthy fetch — full round-trip through the real server with the
// default timeout armed.
func TestPeerFetchIdleHealthyRoundTrip(t *testing.T) {
	payload := make([]byte, 2*peerChunkBytes+999)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "part-0001.wshf")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	resolver := &fakeResolver{queryID: "q1", key: "queries/q1/stage-1/partition=0001/t.wshf", token: "tok", path: path}
	_, client, addr := startPeerPair(t, resolver)

	rc, err := client.FetchShuffle(context.Background(), addr, "q1", resolver.key, "tok")
	if err != nil {
		t.Fatalf("FetchShuffle: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("reading fetch stream: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}
