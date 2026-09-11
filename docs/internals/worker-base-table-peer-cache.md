# Worker base table peer cache

Source: internal/worker/base_table_peer.go — var (, moved 2026-09-11 (#1026)

Base-table cache peer tier (docs/design/scan-affinity.md §peer tier).

The base-table NVMe cache is per worker, and scan affinity (rendezvous
file→worker placement) partitions those caches — which is exactly right
for scan fan-outs and exactly wrong for every OTHER base-table reader:
late-materialization column gathers inside join tasks, broadcast
builds, and sub-2×workers tables all read files their worker doesn't
own, and with partitioned caches those reads miss to S3 where full
replication used to hit (SF100 steady +12.8%, pair 20260808-{125821,
131723}). The peer tier completes the class: a non-owner's cache miss
fetches the owner's NVMe copy over the PeerExchange wire and populates
locally, so S3 sees each file once CLUSTER-WIDE regardless of reader
and convergence runs at NIC speed.

Ownership must agree with the coordinator's placement: both sides hash
bare object keys via distributed.AffinityOwner over the live,
non-draining worker set. The worker learns that set the same way the
coordinator does — the SubjectHeartbeat stream — with the registry's
90s staleness TTL. A transiently divergent view costs one NotFound →
S3 fallthrough, never correctness.

WADJET_BASE_PEER_TIER=0 is the kill switch (both fetching and serving).
WADJET_BASE_PEER_READTHROUGH=0 kills only the owner read-through
(first-touch single-flight): a peer fetch for a not-yet-resident owned
file then answers NotFound as before instead of populating from S3.
WADJET_PEER_SECRET, when set cluster-wide, gates base-table serving on
token match; unset (default) matches the peer plane's existing
intra-cluster trust posture, with TLS as the hardening seam.
