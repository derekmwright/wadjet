package distributed

import (
	"hash/fnv"
	"strings"
)

// AffinityOwner returns the rendezvous (highest-random-weight) owner of
// file among workers: argmax_w fnv64(file, w). Deterministic for a given
// worker set; a joining/leaving worker remaps only the files it wins/held.
//
// This single definition is the contract between the coordinator's scan
// fan-out placement (scan_affinity.go) and the worker's base-table peer
// tier (base_table_peer.go): both sides must hash the same file string —
// the bare object key, no bucket prefix — over the same sorted domain of
// live, non-draining worker IDs, or a non-owner will dial the wrong peer
// and pay a harmless NotFound → S3 fallthrough.
func AffinityOwner(file string, workers []string) string {
	var best string
	var bestScore uint64
	for _, w := range workers {
		h := fnv.New64a()
		h.Write([]byte(file))
		h.Write([]byte{0})
		h.Write([]byte(w))
		if s := h.Sum64(); best == "" || s > bestScore || (s == bestScore && w < best) {
			best, bestScore = w, s
		}
	}
	return best
}

// BaseTablePeerKeyPrefix marks a PeerExchange FetchShuffle key as a
// base-table cache fetch ("basetable:<bucket>/<object key>") rather than a
// query-scratch shuffle file. The prefix can never collide with scratch
// keys (those start "queries/") or bucket names (":" is not legal in S3
// bucket names).
const BaseTablePeerKeyPrefix = "basetable:"

// BaseTablePeerKey encodes a (bucket, key) pair for a peer fetch.
func BaseTablePeerKey(bucket, key string) string {
	return BaseTablePeerKeyPrefix + bucket + "/" + key
}

// CutBaseTablePeerKey decodes a peer-fetch key produced by
// BaseTablePeerKey. ok is false for non-base-table keys.
func CutBaseTablePeerKey(peerKey string) (bucket, key string, ok bool) {
	rest, ok := strings.CutPrefix(peerKey, BaseTablePeerKeyPrefix)
	if !ok {
		return "", "", false
	}
	bucket, key, ok = strings.Cut(rest, "/")
	if !ok || bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, key, true
}
