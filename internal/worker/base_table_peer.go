package worker

import (
	"context"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
)

// Base-table cache peer tier (docs/design/scan-affinity.md §peer tier).
//
// The base-table NVMe cache is per worker, and scan affinity (rendezvous
// file→worker placement) partitions those caches — which is exactly right
// for scan fan-outs and exactly wrong for every OTHER base-table reader:
// late-materialization column gathers inside join tasks, broadcast
// builds, and sub-2×workers tables all read files their worker doesn't
// own, and with partitioned caches those reads miss to S3 where full
// replication used to hit (SF100 steady +12.8%, pair 20260808-{125821,
// 131723}). The peer tier completes the class: a non-owner's cache miss
// fetches the owner's NVMe copy over the PeerExchange wire and populates
// locally, so S3 sees each file once CLUSTER-WIDE regardless of reader
// and convergence runs at NIC speed.
//
// Ownership must agree with the coordinator's placement: both sides hash
// bare object keys via distributed.AffinityOwner over the live,
// non-draining worker set. The worker learns that set the same way the
// coordinator does — the SubjectHeartbeat stream — with the registry's
// 90s staleness TTL. A transiently divergent view costs one NotFound →
// S3 fallthrough, never correctness.
//
// WADJET_BASE_PEER_TIER=0 is the kill switch (both fetching and serving).
// WADJET_BASE_PEER_READTHROUGH=0 kills only the owner read-through
// (first-touch single-flight): a peer fetch for a not-yet-resident owned
// file then answers NotFound as before instead of populating from S3.
// WADJET_PEER_SECRET, when set cluster-wide, gates base-table serving on
// token match; unset (default) matches the peer plane's existing
// intra-cluster trust posture, with TLS as the hardening seam.
var (
	basePeerTierEnabled        = os.Getenv("WADJET_BASE_PEER_TIER") != "0"
	basePeerReadThroughEnabled = os.Getenv("WADJET_BASE_PEER_READTHROUGH") != "0"
	basePeerSecret             = os.Getenv("WADJET_PEER_SECRET")
)

// basePeerStaleTTL mirrors the coordinator registry's default heartbeat
// staleness window (workers.go NewWorkerRegistry) so both sides compute
// rendezvous ownership over the same domain.
const basePeerStaleTTL = 90 * time.Second

// baseTablePeerDirectory is the worker's replica of cluster membership,
// fed by worker heartbeats: workerID → peer address + liveness.
type baseTablePeerDirectory struct {
	self string

	mu    sync.RWMutex
	peers map[string]basePeerEntry
}

type basePeerEntry struct {
	addr     string
	draining bool
	lastSeen time.Time
}

func newBaseTablePeerDirectory(self string) *baseTablePeerDirectory {
	return &baseTablePeerDirectory{self: self, peers: make(map[string]basePeerEntry)}
}

// record folds one heartbeat into the directory.
func (d *baseTablePeerDirectory) record(hb distributed.WorkerHeartbeat) {
	if hb.WorkerID == "" {
		return
	}
	d.mu.Lock()
	d.peers[hb.WorkerID] = basePeerEntry{addr: hb.PeerAddr, draining: hb.Draining, lastSeen: time.Now()}
	d.mu.Unlock()
}

// domain returns the sorted live non-draining worker IDs — the rendezvous
// domain, matching the coordinator's activeWorkerIDs — and each member's
// peer address ("" when the worker serves no fetches). Stale entries are
// pruned in passing.
func (d *baseTablePeerDirectory) domain() (ids []string, addrs map[string]string) {
	cutoff := time.Now().Add(-basePeerStaleTTL)
	d.mu.Lock()
	defer d.mu.Unlock()
	addrs = make(map[string]string, len(d.peers))
	for id, e := range d.peers {
		if e.lastSeen.Before(cutoff) {
			delete(d.peers, id)
			continue
		}
		if e.draining {
			continue
		}
		ids = append(ids, id)
		addrs[id] = e.addr
	}
	sort.Strings(ids)
	return ids, addrs
}

// baseTablePeerTier implements objstore.BaseTablePeerFetcher over the
// heartbeat directory and the worker's shared PeerClient.
type baseTablePeerTier struct {
	dir    *baseTablePeerDirectory
	client *dataplane.PeerClient
}

// FetchBaseTable opens a whole-object stream from the file's rendezvous
// owner. ok=false whenever the tier cannot help — self-owned file, no
// live owner, owner serves no fetches, dial failure — and the cache falls
// through to S3. Mid-stream failures surface from Read and are handled
// the same way by the cache's spool.
func (t *baseTablePeerTier) FetchBaseTable(ctx context.Context, bucket, key string) (io.ReadCloser, bool) {
	if !basePeerTierEnabled || t.client == nil {
		return nil, false
	}
	ids, addrs := t.dir.domain()
	owner := distributed.AffinityOwner(key, ids)
	if owner == "" || owner == t.dir.self {
		return nil, false
	}
	addr := addrs[owner]
	if addr == "" {
		return nil, false
	}
	rc, err := t.client.FetchShuffle(ctx, addr, "", distributed.BaseTablePeerKey(bucket, key), basePeerSecret)
	if err != nil {
		return nil, false
	}
	return rc, true
}

// startBaseTablePeerDirectory subscribes the directory to the heartbeat
// stream and seeds self (our own heartbeat echoes back, but the first
// tick is 10s out and the advertised address is already known). Returns
// the subscription for Stop-time cleanup; nil when the worker has no
// directory wired.
func (w *Worker) startBaseTablePeerDirectory() *nats.Subscription {
	if w.basePeers == nil || w.nc == nil {
		return nil
	}
	w.basePeers.record(distributed.WorkerHeartbeat{
		WorkerID: w.config.WorkerID,
		PeerAddr: w.peerAdvertiseAddr(),
		Draining: w.Draining(),
	})
	sub, err := w.nc.Subscribe(distributed.SubjectHeartbeat, func(msg *nats.Msg) {
		var hb distributed.WorkerHeartbeat
		if err := distributed.Unmarshal(msg.Data, &hb); err != nil {
			return
		}
		w.basePeers.record(hb)
	})
	if err != nil {
		w.logger.Warn("base-table peer tier: heartbeat subscribe failed; tier stays inert", "error", err)
		return nil
	}
	return sub
}
