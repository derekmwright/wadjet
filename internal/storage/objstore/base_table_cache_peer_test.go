package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// par1 frames data as a plausible parquet payload for the peer-tier magic
// guard (head and tail "PAR1").
func par1(data []byte) []byte {
	out := append([]byte("PAR1"), data...)
	return append(out, []byte("PAR1")...)
}

// fakePeerFetcher implements BaseTablePeerFetcher for cache tests.
type fakePeerFetcher struct {
	body    []byte
	offered bool // false = tier declines (self-owned / no owner)
	errMid  bool // reader fails after half the body
	calls   int
}

func (f *fakePeerFetcher) FetchBaseTable(_ context.Context, _, _ string) (io.ReadCloser, bool) {
	f.calls++
	if !f.offered {
		return nil, false
	}
	if f.errMid {
		return io.NopCloser(io.MultiReader(
			bytes.NewReader(f.body[:len(f.body)/2]),
			&erroringReader{},
		)), true
	}
	return io.NopCloser(bytes.NewReader(f.body)), true
}

type erroringReader struct{}

func (*erroringReader) Read([]byte) (int, error) { return 0, errors.New("peer stream reset") }

func TestBaseTableCachePeerTierServesAndPopulates(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := par1(bytes.Repeat([]byte("peer"), 1000))
	key := "tables/lineitem/chunk_peer.parquet"
	peer := &fakePeerFetcher{body: data, offered: true}
	c.SetPeerFetcher(peer)

	// Miss served entirely from the peer: the inner store is never
	// consulted (it doesn't even hold the object).
	if got := getAll(t, c, "data", key); !bytes.Equal(got, data) {
		t.Fatal("peer-served body mismatch")
	}
	if inner.gets.Load() != 0 {
		t.Fatalf("inner gets = %d, want 0 (peer must serve the miss)", inner.gets.Load())
	}
	s := c.Stats()
	if s.PeerHits != 1 || s.PeerBytes != int64(len(data)) {
		t.Fatalf("peer stats = %+v, want 1 peer hit / %d peer bytes", s, len(data))
	}
	if s.Misses != 0 {
		t.Fatalf("misses = %d, want 0 (peer serves must not count as S3 misses)", s.Misses)
	}

	// The peer copy populated the cache: the second read is a plain local
	// hit and the tier is not consulted again.
	if got := getAll(t, c, "data", key); !bytes.Equal(got, data) {
		t.Fatal("hit body mismatch after peer populate")
	}
	if peer.calls != 1 {
		t.Fatalf("peer calls = %d, want 1", peer.calls)
	}
	if s := c.Stats(); s.Hits != 1 || s.Entries != 1 {
		t.Fatalf("stats after populate = %+v, want 1 hit / 1 entry", s)
	}
}

func TestBaseTableCachePeerTierDeclinedGoesToInner(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := par1([]byte("owned-by-self"))
	key := "tables/nation/chunk_a.parquet"
	putObject(t, inner.Store, "data", key, data)
	c.SetPeerFetcher(&fakePeerFetcher{offered: false})

	if got := getAll(t, c, "data", key); !bytes.Equal(got, data) {
		t.Fatal("body mismatch")
	}
	s := c.Stats()
	if s.Misses != 1 || s.PeerHits != 0 || s.PeerFallthroughs != 0 {
		t.Fatalf("stats = %+v, want plain S3 miss with no peer activity", s)
	}
	if inner.gets.Load() != 1 {
		t.Fatalf("inner gets = %d, want 1", inner.gets.Load())
	}
}

func TestBaseTableCachePeerTierMidStreamFailureFallsThrough(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := par1(bytes.Repeat([]byte("resil"), 500))
	key := "tables/orders/chunk_b.parquet"
	putObject(t, inner.Store, "data", key, data)
	c.SetPeerFetcher(&fakePeerFetcher{body: data, offered: true, errMid: true})

	// The caller sees the S3 copy, undisturbed by the failed peer stream.
	if got := getAll(t, c, "data", key); !bytes.Equal(got, data) {
		t.Fatal("fallthrough body mismatch")
	}
	s := c.Stats()
	if s.PeerFallthroughs != 1 || s.PeerHits != 0 {
		t.Fatalf("stats = %+v, want 1 peer fallthrough / 0 peer hits", s)
	}
	if s.Misses != 1 || inner.gets.Load() != 1 {
		t.Fatalf("stats = %+v inner gets = %d, want the S3 miss path", s, inner.gets.Load())
	}
	// The ordinary tee populated the cache on the fallthrough read.
	if got := getAll(t, c, "data", key); !bytes.Equal(got, data) {
		t.Fatal("hit body mismatch")
	}
	if inner.gets.Load() != 1 {
		t.Fatalf("inner gets = %d, want 1 (tee must have populated)", inner.gets.Load())
	}
}

func TestBaseTableCachePeerTierRejectsNonParquetPayload(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	good := par1([]byte("the-real-bytes"))
	key := "tables/part/chunk_c.parquet"
	putObject(t, inner.Store, "data", key, good)
	// A peer serving garbage (truncated copy, protocol drift) must never be
	// admitted into the parquet read path.
	c.SetPeerFetcher(&fakePeerFetcher{body: bytes.Repeat([]byte("junk"), 100), offered: true})

	if got := getAll(t, c, "data", key); !bytes.Equal(got, good) {
		t.Fatal("caller must see the durable copy")
	}
	s := c.Stats()
	if s.PeerFallthroughs != 1 || s.PeerHits != 0 {
		t.Fatalf("stats = %+v, want the garbage rejected as a fallthrough", s)
	}
}

func TestBaseTableCachePeerLocalPath(t *testing.T) {
	c, inner, _ := newTestCache(t, 1<<20)
	data := par1([]byte("serve-me"))
	key := "tables/supplier/chunk_d.parquet"
	putObject(t, inner.Store, "data", key, data)

	if _, ok := c.PeerLocalPath("data", key); ok {
		t.Fatal("PeerLocalPath must miss before population")
	}
	getAll(t, c, "data", key) // populate via the tee

	path, ok := c.PeerLocalPath("data", key)
	if !ok || path == "" {
		t.Fatal("PeerLocalPath must serve a resident entry")
	}
	s := c.Stats()
	if s.PeerServes != 1 || s.PeerServeBytes != int64(len(data)) {
		t.Fatalf("stats = %+v, want 1 peer serve / %d bytes", s, len(data))
	}
	// Serving a peer is not a local hit: the ledger's hit counters stay
	// untouched by wire serves.
	if s.Hits != 0 {
		t.Fatalf("hits = %d, want 0 (peer serves must not inflate hits)", s.Hits)
	}
}
