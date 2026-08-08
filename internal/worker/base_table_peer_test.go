package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// par1Frame frames data as a plausible parquet payload (head+tail magic)
// for the peer tier's admission guard.
func par1Frame(data []byte) []byte {
	out := append([]byte("PAR1"), data...)
	return append(out, []byte("PAR1")...)
}

// newBaseTableCache builds a BaseTableCache over a MemStore holding the
// given object.
func newBaseTableCache(t *testing.T, bucket, key string, data []byte) *objstore.BaseTableCache {
	t.Helper()
	mem := objstore.NewMemStore()
	ctx := context.Background()
	if err := mem.MakeBucket(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	if data != nil {
		if _, err := mem.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
	}
	btc, err := objstore.NewBaseTableCache(mem, t.TempDir(), 1<<20, nil)
	if err != nil {
		t.Fatal(err)
	}
	return btc
}

// populate pulls the object through the cache so the tee admits it.
func populate(t *testing.T, btc *objstore.BaseTableCache, bucket, key string) {
	t.Helper()
	rc, _, err := btc.Get(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("populate Get: %v", err)
	}
	if _, err := io.Copy(io.Discard, rc); err != nil {
		t.Fatalf("populate read: %v", err)
	}
	rc.Close()
}

func TestBaseTablePeerDirectoryDomain(t *testing.T) {
	d := newBaseTablePeerDirectory("w-self")
	d.record(distributed.WorkerHeartbeat{WorkerID: "w-self", PeerAddr: "10.0.0.1:9095"})
	d.record(distributed.WorkerHeartbeat{WorkerID: "w-b", PeerAddr: "10.0.0.2:9095"})
	d.record(distributed.WorkerHeartbeat{WorkerID: "w-draining", PeerAddr: "10.0.0.3:9095", Draining: true})
	d.record(distributed.WorkerHeartbeat{WorkerID: "w-noaddr"})
	d.record(distributed.WorkerHeartbeat{WorkerID: ""}) // ignored
	// Stale entry: recorded, then aged past the TTL.
	d.record(distributed.WorkerHeartbeat{WorkerID: "w-stale", PeerAddr: "10.0.0.4:9095"})
	d.mu.Lock()
	e := d.peers["w-stale"]
	e.lastSeen = time.Now().Add(-2 * basePeerStaleTTL)
	d.peers["w-stale"] = e
	d.mu.Unlock()

	ids, addrs := d.domain()
	want := []string{"w-b", "w-noaddr", "w-self"}
	if len(ids) != len(want) {
		t.Fatalf("domain = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("domain = %v, want %v (sorted, no draining/stale)", ids, want)
		}
	}
	if addrs["w-b"] != "10.0.0.2:9095" || addrs["w-noaddr"] != "" {
		t.Fatalf("addrs = %v", addrs)
	}
	// The stale entry was pruned, not just skipped.
	d.mu.RLock()
	_, still := d.peers["w-stale"]
	d.mu.RUnlock()
	if still {
		t.Fatal("stale entry must be pruned")
	}
}

func TestResolveBaseTableFileServesResidentOnly(t *testing.T) {
	const bucket, key = "data", "tables/nation/chunk_a.parquet"
	btc := newBaseTableCache(t, bucket, key, par1Frame([]byte("national")))
	e := NewExecutor(btc, NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, t.TempDir())
	e.SetBaseTableCache(btc)

	// Not resident yet → NotFound (the consumer falls through to S3).
	if _, err := e.ResolveShuffleFile("", distributed.BaseTablePeerKey(bucket, key), ""); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("pre-population resolve err = %v, want ErrPeerNotFound", err)
	}
	populate(t, btc, bucket, key)
	path, err := e.ResolveShuffleFile("", distributed.BaseTablePeerKey(bucket, key), "")
	if err != nil || path == "" {
		t.Fatalf("resolve = (%q, %v), want a local path", path, err)
	}

	// Cluster secret, when set, gates the branch.
	restore := basePeerSecret
	basePeerSecret = "s3cret"
	t.Cleanup(func() { basePeerSecret = restore })
	if _, err := e.ResolveShuffleFile("", distributed.BaseTablePeerKey(bucket, key), "wrong"); !errors.Is(err, dataplane.ErrPeerDenied) {
		t.Fatalf("bad-secret resolve err = %v, want ErrPeerDenied", err)
	}
	if _, err := e.ResolveShuffleFile("", distributed.BaseTablePeerKey(bucket, key), "s3cret"); err != nil {
		t.Fatalf("matching-secret resolve err = %v", err)
	}

	// No cache wired → NotFound, never a panic.
	bare := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	bare.SetMemoryBudget(0, t.TempDir())
	if _, err := bare.ResolveShuffleFile("", distributed.BaseTablePeerKey(bucket, key), "s3cret"); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("no-cache resolve err = %v, want ErrPeerNotFound", err)
	}
}

// TestBaseTablePeerTierEndToEnd exercises the full path: a non-owner's
// cache miss dials the owner's real PeerServer, streams the owner's NVMe
// copy, admits it locally, and serves it — without touching the
// consumer's inner store.
func TestBaseTablePeerTierEndToEnd(t *testing.T) {
	const bucket, key = "data", "tables/lineitem/chunk_e2e.parquet"
	body := par1Frame(bytes.Repeat([]byte("wire"), 4096))

	// Owner side: cache holds the file; executor serves it via PeerServer.
	ownerCache := newBaseTableCache(t, bucket, key, body)
	populate(t, ownerCache, bucket, key)
	owner := NewExecutor(ownerCache, NewLRUCache(1<<20), nil)
	owner.SetMemoryBudget(0, t.TempDir())
	owner.SetBaseTableCache(ownerCache)
	srv := dataplane.NewPeerServer(dataplane.PeerServerConfig{Addr: "127.0.0.1:0"}, owner, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("starting peer server: %v", err)
	}
	t.Cleanup(srv.Stop)

	// Choose IDs so the serving side is the rendezvous owner of key.
	ids := []string{"w-1", "w-2"}
	ownerID := distributed.AffinityOwner(key, ids)
	selfID := "w-1"
	if ownerID == "w-1" {
		selfID = "w-2"
	}

	// Consumer side: empty inner store — the peer is the only source.
	consumerCache := newBaseTableCache(t, bucket, key, nil)
	client := dataplane.NewPeerClient(nil)
	t.Cleanup(client.Close)
	dir := newBaseTablePeerDirectory(selfID)
	dir.record(distributed.WorkerHeartbeat{WorkerID: selfID, PeerAddr: "127.0.0.1:1"})
	dir.record(distributed.WorkerHeartbeat{WorkerID: ownerID, PeerAddr: srv.AdvertiseAddr()})
	consumerCache.SetPeerFetcher(&baseTablePeerTier{dir: dir, client: client})

	rc, info, err := consumerCache.Get(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("consumer Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("consumer read: %v", err)
	}
	if !bytes.Equal(got, body) || info.Size != int64(len(body)) {
		t.Fatalf("peer-served body mismatch: %d bytes vs %d", len(got), len(body))
	}
	cs := consumerCache.Stats()
	if cs.PeerHits != 1 || cs.Misses != 0 {
		t.Fatalf("consumer stats = %+v, want 1 peer hit / 0 S3 misses", cs)
	}
	ownStats := ownerCache.Stats()
	if ownStats.PeerServes != 1 || ownStats.PeerServeBytes != int64(len(body)) {
		t.Fatalf("owner stats = %+v, want 1 peer serve of %d bytes", ownStats, len(body))
	}

	// Self-owned files are never peer-fetched: find a key self owns and
	// assert the tier declines it.
	tier := &baseTablePeerTier{dir: dir, client: client}
	for _, cand := range []string{"a.parquet", "b.parquet", "c.parquet", "d.parquet", "e.parquet"} {
		if distributed.AffinityOwner(cand, ids) != selfID {
			continue
		}
		if _, ok := tier.FetchBaseTable(context.Background(), bucket, cand); ok {
			t.Fatalf("tier must decline self-owned file %q", cand)
		}
		return
	}
	t.Fatal("no candidate key hashed to self; extend the candidate list")
}
