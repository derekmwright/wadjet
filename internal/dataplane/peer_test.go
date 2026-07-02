package dataplane

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeResolver serves one (queryID, key, token) triple from a fixed path.
type fakeResolver struct {
	queryID, key, token, path string
}

func (f *fakeResolver) ResolveShuffleFile(queryID, key, token string) (string, error) {
	if token != f.token {
		return "", ErrPeerDenied
	}
	if queryID != f.queryID || key != f.key {
		return "", ErrPeerNotFound
	}
	return f.path, nil
}

func startPeerPair(t *testing.T, resolver ShuffleFileResolver) (*PeerServer, *PeerClient, string) {
	t.Helper()
	srv := NewPeerServer(PeerServerConfig{Addr: "127.0.0.1:0"}, resolver, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("starting peer server: %v", err)
	}
	t.Cleanup(srv.Stop)
	client := NewPeerClient(nil)
	t.Cleanup(client.Close)
	return srv, client, srv.AdvertiseAddr()
}

func TestPeerFetchRoundTrip(t *testing.T) {
	// Payload spanning several 256 KiB chunks, with a non-chunk-aligned tail.
	payload := make([]byte, 3*peerChunkBytes+12345)
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

func TestPeerFetchRejections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "part.wshf")
	if err := os.WriteFile(path, []byte("WSHFdata"), 0o644); err != nil {
		t.Fatal(err)
	}
	resolver := &fakeResolver{queryID: "q1", key: "k1", token: "tok", path: path}
	_, client, addr := startPeerPair(t, resolver)

	cases := []struct {
		name             string
		queryID, key, tk string
		wantCode         codes.Code
	}{
		{"bad token", "q1", "k1", "wrong", codes.PermissionDenied},
		{"empty token", "q1", "k1", "", codes.PermissionDenied},
		{"unknown key", "q1", "nope", "tok", codes.NotFound},
		{"unknown query", "q2", "k1", "tok", codes.NotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := client.FetchShuffle(context.Background(), addr, tc.queryID, tc.key, tc.tk)
			if err != nil {
				t.Fatalf("FetchShuffle returned pre-stream error: %v", err)
			}
			defer rc.Close()
			_, err = io.ReadAll(rc)
			if err == nil {
				t.Fatal("expected a rejection, read succeeded")
			}
			if st, ok := status.FromError(err); !ok || st.Code() != tc.wantCode {
				t.Fatalf("got error %v, want gRPC code %v", err, tc.wantCode)
			}
		})
	}
}

func TestPeerFetchVanishedFile(t *testing.T) {
	// Resolver resolves, but the file is gone (query cleanup raced the
	// fetch) → NotFound, so the consumer falls through to S3.
	resolver := &fakeResolver{queryID: "q1", key: "k1", token: "tok", path: filepath.Join(t.TempDir(), "gone.wshf")}
	_, client, addr := startPeerPair(t, resolver)

	rc, err := client.FetchShuffle(context.Background(), addr, "q1", "k1", "tok")
	if err != nil {
		t.Fatalf("FetchShuffle: %v", err)
	}
	defer rc.Close()
	if _, err = io.ReadAll(rc); err == nil {
		t.Fatal("expected NotFound for vanished file")
	} else if st, _ := status.FromError(err); st.Code() != codes.NotFound {
		t.Fatalf("got %v, want NotFound", err)
	}
}

func TestPeerFetchUnreachablePeer(t *testing.T) {
	client := NewPeerClient(nil)
	t.Cleanup(client.Close)
	// Nothing listens here; the error must surface promptly (fail-fast
	// RPC, no wait-for-ready) rather than hanging until a deadline.
	rc, err := client.FetchShuffle(context.Background(), "127.0.0.1:1", "q", "k", "t")
	if err == nil {
		defer rc.Close()
		if _, rerr := io.ReadAll(rc); rerr == nil {
			t.Fatal("expected an error fetching from unreachable peer")
		}
	}
}

func TestPeerServerAdvertiseAddr(t *testing.T) {
	srv := NewPeerServer(PeerServerConfig{Addr: "127.0.0.1:0"}, &fakeResolver{}, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	addr := srv.AdvertiseAddr()
	if addr == "" {
		t.Fatal("AdvertiseAddr empty after Start")
	}
	if addr[len(addr)-2:] == ":0" {
		t.Fatalf("AdvertiseAddr %q kept the unbound :0 port", addr)
	}
	// Explicit override wins verbatim.
	srv2 := NewPeerServer(PeerServerConfig{Addr: "127.0.0.1:0", AdvertiseAddr: "10.1.2.3:9095"}, &fakeResolver{}, nil)
	if err := srv2.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv2.Stop)
	if got := srv2.AdvertiseAddr(); got != "10.1.2.3:9095" {
		t.Fatalf("AdvertiseAddr override: got %q", got)
	}
}
