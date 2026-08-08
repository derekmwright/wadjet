package coordinator

import (
	"hash/fnv"
	"os"
	"sort"
)

// Scan-task file→worker affinity (docs/design/scan-affinity.md).
//
// The base-table NVMe cache is PER WORKER, and scan fan-outs split files
// across tasks with no memory of who cached what — so each worker
// independently first-touches (S3-reads) essentially the whole dataset
// over a cold suite (SF100 2026-08-08 diagnosis: 107 misses / ~26 GB per
// worker = ~3× the dataset from S3), and WHICH query pays each per-worker
// first touch depends on scheduler timing. That distribution is the
// dominant cold-run variance signature: the same ±20 s tax roamed between
// Q04 and Q06 across runs on identical plans.
//
// Rendezvous hashing gives every file one canonical owner among the
// active workers: fan-outs group files by owner, tasks carry the owner as
// AffinityWorkerID, and the scheduler prefers (never requires) that
// placement. First touches then happen once per file CLUSTER-WIDE, later
// scans of the same table are warm on every query, and the per-worker
// cache footprint drops from |dataset| to |dataset|/N.
//
// DEFAULT OFF pending the base-table peer tier (SF100 pair
// 20260808-{125821,131723}): scan affinity alone delivered the cold-run
// win (first-touch misses 327→227, cold suite −11.7%, the roaming Q06
// disturbance 19.1s→2.0s) but non-affine base-table readers — the
// late-materialization column gathers in join tasks, broadcast builds,
// sub-2×workers tables — kept reading files their worker doesn't own,
// and with PARTITIONED caches those reads miss where full replication
// used to hit: run 2 paid ~18 GB of S3 first-touches control didn't
// (steady suite +12.8%). The class completion is a peer tier on the
// base-table cache miss path (non-owners fetch the owner's NVMe copy
// instead of S3), after which every reader converges cheaply and the
// flag flips on. WADJET_SCAN_AFFINITY=1 opts in for A/B arms.
var scanAffinityEnabled = os.Getenv("WADJET_SCAN_AFFINITY") == "1"

// affinityOwner returns the rendezvous (highest-random-weight) owner of
// file among workers: argmax_w fnv64(file, w). Deterministic for a given
// worker set; a joining/leaving worker remaps only the files it wins/held.
func affinityOwner(file string, workers []string) string {
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

// affineFileSets splits files into ~taskCount tasks grouped by rendezvous
// owner: each owner's files are sliced into that owner's proportional
// share of tasks (≥1 when it owns any), preserving the fan-out's total
// parallelism while keeping every task's files on one canonical cache.
// Returns nil when affinity is disabled, the worker set is empty, or the
// fan-out is a degenerate shape affinity would hurt (fewer files than
// workers — e.g. single-file row-group shard fan-outs, where pinning all
// shards to one owner would serialize the stage); callers then fall back
// to the plain splitFilesEvenly path.
func affineFileSets(files []string, workers []string, taskCount int) (fileSets [][]string, owners []string) {
	if !scanAffinityEnabled || len(workers) == 0 || len(files) < 2*len(workers) || taskCount <= 0 {
		return nil, nil
	}
	sorted := append([]string(nil), workers...)
	sort.Strings(sorted)
	byOwner := make(map[string][]string, len(sorted))
	for _, f := range files {
		o := affinityOwner(f, sorted)
		byOwner[o] = append(byOwner[o], f)
	}
	for _, w := range sorted {
		group := byOwner[w]
		if len(group) == 0 {
			continue
		}
		// Proportional task share, floored at 1 so every owner's files
		// stay on their canonical worker rather than merging elsewhere.
		share := (len(group)*taskCount + len(files) - 1) / len(files)
		if share < 1 {
			share = 1
		}
		if share > len(group) {
			share = len(group)
		}
		per := (len(group) + share - 1) / share
		for i := 0; i < len(group); i += per {
			end := i + per
			if end > len(group) {
				end = len(group)
			}
			fileSets = append(fileSets, group[i:end])
			owners = append(owners, w)
		}
	}
	return fileSets, owners
}

// affinityFor returns owners[i] when the fan-out took the affine path,
// "" otherwise (plain splitFilesEvenly fallback).
func affinityFor(owners []string, i int) string {
	if i < len(owners) {
		return owners[i]
	}
	return ""
}

// activeWorkerIDs returns the registry's live worker IDs — the rendezvous
// domain for scan affinity. Order-insensitive (affineFileSets sorts).
func (c *Coordinator) activeWorkerIDs() []string {
	if c.workers == nil {
		return nil
	}
	ws := c.workers.ActiveWorkers()
	ids := make([]string, 0, len(ws))
	for _, w := range ws {
		ids = append(ids, w.WorkerID)
	}
	return ids
}
