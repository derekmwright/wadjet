package coordinator

import (
	"os"
	"sort"

	"github.com/citc-tech/wadjet/internal/distributed"
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
// DEFAULT ON since the base-table peer tier landed (worker
// base_table_peer.go). Affinity alone delivered the cold-run win (SF100
// pair 20260808-{125821,131723}: first-touch misses 327→227, cold suite
// −11.7%, the roaming Q06 disturbance 19.1s→2.0s) but regressed steady
// +12.8%: non-affine base-table readers — late-materialization column
// gathers in join tasks, broadcast builds, sub-2×workers tables — kept
// reading files their worker doesn't own, and with PARTITIONED caches
// those reads missed to S3 where full replication used to hit (~18 GB of
// run-2 first-touches). The peer tier completes the class: a non-owner's
// miss fetches the owner's NVMe copy over the peer wire and populates
// locally, so every reader converges at NIC speed and S3 sees each file
// once cluster-wide. WADJET_SCAN_AFFINITY=0 is the kill switch for the
// placement half; WADJET_BASE_PEER_TIER=0 kills the peer tier half.
var scanAffinityEnabled = os.Getenv("WADJET_SCAN_AFFINITY") != "0"

// affinityOwner returns the rendezvous (highest-random-weight) owner of
// file among workers. The hash lives in distributed.AffinityOwner because
// the worker's base-table peer tier must compute identical ownership.
func affinityOwner(file string, workers []string) string {
	return distributed.AffinityOwner(file, workers)
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
