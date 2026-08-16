package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
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
	if _, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), ""); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("pre-population resolve err = %v, want ErrPeerNotFound", err)
	}
	populate(t, btc, bucket, key)
	path, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), "")
	if err != nil || path == "" {
		t.Fatalf("resolve = (%q, %v), want a local path", path, err)
	}

	// Cluster secret, when set, gates the branch.
	restore := basePeerSecret
	basePeerSecret = "s3cret"
	t.Cleanup(func() { basePeerSecret = restore })
	if _, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), "wrong"); !errors.Is(err, dataplane.ErrPeerDenied) {
		t.Fatalf("bad-secret resolve err = %v, want ErrPeerDenied", err)
	}
	if _, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), "s3cret"); err != nil {
		t.Fatalf("matching-secret resolve err = %v", err)
	}

	// No cache wired → NotFound, never a panic.
	bare := NewExecutor(objstore.NewMemStore(), NewLRUCache(1<<20), nil)
	bare.SetMemoryBudget(0, t.TempDir())
	if _, err := bare.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), "s3cret"); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("no-cache resolve err = %v, want ErrPeerNotFound", err)
	}
}

func TestResolveBaseTableFileOwnerReadThrough(t *testing.T) {
	const bucket, key = "data", "tables/customer/chunk_rt.parquet"
	body := par1Frame([]byte("owner-fetches-once"))
	btc := newBaseTableCache(t, bucket, key, body)
	e := NewExecutor(btc, NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, t.TempDir())
	e.SetBaseTableCache(btc)
	e.SetBaseTableOwnership(func(string) bool { return true })

	// Not resident, but owned: the resolver populates from the inner store
	// and serves — first-touch single-flight.
	path, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), "")
	if err != nil || path == "" {
		t.Fatalf("owned-miss resolve = (%q, %v), want a read-through serve", path, err)
	}
	s := btc.Stats()
	if s.ReadThroughs != 1 || s.ReadThroughBytes != int64(len(body)) {
		t.Fatalf("stats = %+v, want 1 read-through of %d bytes", s, len(body))
	}
	if s.PeerServes != 1 {
		t.Fatalf("stats = %+v, want the read-through serve on the peer-serve ledger", s)
	}

	// Second fetch is a plain resident serve.
	if _, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), ""); err != nil {
		t.Fatalf("resident resolve err = %v", err)
	}
	if s := btc.Stats(); s.ReadThroughs != 1 || s.PeerServes != 2 {
		t.Fatalf("stats = %+v, want no second read-through", s)
	}
}

func TestResolveBaseTableFileReadThroughGuards(t *testing.T) {
	const bucket, key = "data", "tables/region/chunk_guard.parquet"
	body := par1Frame([]byte("guarded"))

	// Non-owned miss stays NotFound: a divergent membership view must not
	// let a peer make this worker fetch arbitrary objects.
	btc := newBaseTableCache(t, bucket, key, body)
	e := NewExecutor(btc, NewLRUCache(1<<20), nil)
	e.SetMemoryBudget(0, t.TempDir())
	e.SetBaseTableCache(btc)
	e.SetBaseTableOwnership(func(string) bool { return false })
	if _, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), ""); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("non-owned miss err = %v, want ErrPeerNotFound", err)
	}
	if s := btc.Stats(); s.ReadThroughs != 0 || s.ReadThroughFails != 0 {
		t.Fatalf("stats = %+v, want no read-through activity for a non-owned key", s)
	}

	// No ownership func wired (legacy wiring) → same NotFound behavior.
	e.SetBaseTableOwnership(nil)
	if _, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), ""); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("nil-ownership miss err = %v, want ErrPeerNotFound", err)
	}

	// Kill switch: owned miss answers NotFound with read-through disabled.
	restore := basePeerReadThroughEnabled
	basePeerReadThroughEnabled = false
	t.Cleanup(func() { basePeerReadThroughEnabled = restore })
	e.SetBaseTableOwnership(func(string) bool { return true })
	if _, err := e.ResolveShuffleFile(t.Context(), "", distributed.BaseTablePeerKey(bucket, key), ""); !errors.Is(err, dataplane.ErrPeerNotFound) {
		t.Fatalf("kill-switched miss err = %v, want ErrPeerNotFound", err)
	}
	if s := btc.Stats(); s.ReadThroughs != 0 {
		t.Fatalf("stats = %+v, want no read-through under the kill switch", s)
	}
}

// TestBaseTablePeerTierFirstTouchSingleFlight is the cross-worker shape:
// the consumer misses BEFORE the owner ever touched the file. The owner
// reads through to its inner store once and serves; the consumer's own
// store is never consulted.
func TestBaseTablePeerTierFirstTouchSingleFlight(t *testing.T) {
	const bucket, key = "data", "tables/lineitem/chunk_ft.parquet"
	body := par1Frame(bytes.Repeat([]byte("cold"), 4096))

	// Owner side: inner store holds the file, cache does NOT.
	ownerCache := newBaseTableCache(t, bucket, key, body)
	owner := NewExecutor(ownerCache, NewLRUCache(1<<20), nil)
	owner.SetMemoryBudget(0, t.TempDir())
	owner.SetBaseTableCache(ownerCache)
	owner.SetBaseTableOwnership(func(string) bool { return true })
	srv := dataplane.NewPeerServer(dataplane.PeerServerConfig{Addr: "127.0.0.1:0"}, owner, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("starting peer server: %v", err)
	}
	t.Cleanup(srv.Stop)

	ids := []string{"w-1", "w-2"}
	ownerID := distributed.AffinityOwner(key, ids)
	selfID := "w-1"
	if ownerID == "w-1" {
		selfID = "w-2"
	}

	// Consumer side: empty inner store — the owner's read-through is the
	// only possible source.
	consumerCache := newBaseTableCache(t, bucket, key, nil)
	client := dataplane.NewPeerClient(nil)
	t.Cleanup(client.Close)
	dir := newBaseTablePeerDirectory(selfID)
	dir.record(distributed.WorkerHeartbeat{WorkerID: selfID, PeerAddr: "127.0.0.1:1"})
	dir.record(distributed.WorkerHeartbeat{WorkerID: ownerID, PeerAddr: srv.AdvertiseAddr()})
	consumerCache.SetPeerFetcher(&baseTablePeerTier{dir: dir, client: client})

	rc, _, err := consumerCache.Get(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("consumer Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("consumer read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("first-touch body mismatch: %d bytes vs %d", len(got), len(body))
	}
	cs := consumerCache.Stats()
	if cs.PeerHits != 1 || cs.Misses != 0 || cs.PeerFallthroughs != 0 {
		t.Fatalf("consumer stats = %+v, want a clean peer hit on first touch", cs)
	}
	own := ownerCache.Stats()
	if own.ReadThroughs != 1 || own.PeerServes != 1 || own.PeerServeBytes != int64(len(body)) {
		t.Fatalf("owner stats = %+v, want 1 read-through + 1 serve", own)
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
