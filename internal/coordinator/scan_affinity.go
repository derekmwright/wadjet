package coordinator

import (
	"fmt"
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

// Byte-balance shedding on top of affinity grouping
// (docs/design/scan-affinity-byte-balance.md). Rendezvous hashing balances
// file COUNT in expectation, but SF100 tables are coarse (lineitem = 63
// files ≈ 283 MB each), so per-worker byte shares are a per-deploy lottery:
// observed draws range from 36/34/30% (even) to 43/18/39% (2.4× max/min)
// of lineitem, and stage barriers pace every scan stage on the hottest
// owner (2026-08-10 pair: the skewed-draw arm's R2 decode split
// 67.0/47.8/61.3 GB and ran steady 611 s where the even-draw arm ran 517 s
// on identical code). Shedding moves surplus files from over-share owners
// to each file's rendezvous runner-up: the peer tier turns the recipient's
// misses into NIC-speed owner fetches (S3 stays single-flight via owner
// read-through), and because the shed choice is a pure function of
// (files, sizes, workers), the same files shed to the same recipient on
// every stage — the recipient's cache converges warm after one fetch.
var affinityByteBalanceEnabled = os.Getenv("WADJET_AFFINITY_BYTE_BALANCE") != "0"

// affinityBalanceTolerance is the accepted per-worker overshoot above the
// fair byte share (total/N). 10%: tight enough to cap barrier pacing near
// fair share, loose enough that whole-file moves (the shed unit) can land
// inside the band at SF100 file granularity (~283 MB vs ~6 GB shares).
const affinityBalanceTolerance = 0.10

// affinityBalanceMinBytes gates shedding to stages where byte imbalance
// can matter. Below this the stage is latency-bound, not bandwidth-bound
// (same scale as the local fast-path routing threshold), and moving files
// off their canonical cache would cost peer churn for nothing.
const affinityBalanceMinBytes int64 = 64 << 20

// affinityOwner returns the rendezvous (highest-random-weight) owner of
// file among workers. The hash lives in distributed.AffinityOwner because
// the worker's base-table peer tier must compute identical ownership.
func affinityOwner(file string, workers []string) string {
	return distributed.AffinityOwner(file, workers)
}

// affineBalance reports what byte-balance shedding did to one fan-out, for
// the dispatch log line. Nil when no shed happened.
type affineBalance struct {
	TotalBytes int64
	ShedFiles  int
	ShedBytes  int64
	// MaxShareBefore/After: hottest worker's byte share as a fraction of
	// the fair share (1.0 = perfectly even).
	MaxShareBefore float64
	MaxShareAfter  float64
}

// affineFileSets splits files into ~taskCount tasks grouped by rendezvous
// owner: each owner's files are sliced into that owner's proportional
// share of tasks (≥1 when it owns any), preserving the fan-out's total
// parallelism while keeping every task's files on one canonical cache.
//
// sizes, when aligned with files (catalog SizeBytes plumbed through
// physical.Stage.ScanFileSizes), enables byte-balance shedding: owners
// holding more than (1+tolerance)× the fair byte share hand surplus files
// to each file's rendezvous runner-up, and task shares go
// byte-proportional instead of count-proportional. A nil/misaligned sizes
// slice degrades to the count-based behavior unchanged.
//
// Returns nil sets when affinity is disabled, the worker set is empty, or
// the fan-out is a degenerate shape affinity would hurt (fewer files than
// workers — e.g. single-file row-group shard fan-outs, where pinning all
// shards to one owner would serialize the stage); callers then fall back
// to the plain splitFilesEvenly path.
func affineFileSets(files []string, sizes []int64, workers []string, taskCount int) (fileSets [][]string, owners []string, balance *affineBalance) {
	if !scanAffinityEnabled || len(workers) == 0 || len(files) < 2*len(workers) || taskCount <= 0 {
		return nil, nil, nil
	}
	sorted := append([]string(nil), workers...)
	sort.Strings(sorted)
	assignee := make(map[string]string, len(files))
	for _, f := range files {
		assignee[f] = affinityOwner(f, sorted)
	}
	byBytes := len(sizes) == len(files) && affinityByteBalanceEnabled
	var totalBytes int64
	var fileSize map[string]int64
	if byBytes {
		fileSize = make(map[string]int64, len(files))
		for i, f := range files {
			fileSize[f] = sizes[i]
			totalBytes += sizes[i]
		}
		if totalBytes >= affinityBalanceMinBytes {
			balance = shedSurplus(files, fileSize, totalBytes, sorted, assignee)
		}
	}
	byWorker := make(map[string][]string, len(sorted))
	for _, f := range files {
		byWorker[assignee[f]] = append(byWorker[assignee[f]], f)
	}
	groupWeight := func(group []string) int { return len(group) }
	totalWeight := len(files)
	if byBytes && totalBytes > 0 {
		// Byte-proportional task shares: equal-byte tasks keep the
		// scheduler's count-based same-batch anti-stacking cap aligned
		// with byte fairness.
		groupWeight = func(group []string) int {
			var b int64
			for _, f := range group {
				b += fileSize[f]
			}
			// Scale bytes down to keep the share arithmetic in int range.
			return int(b >> 20)
		}
		totalWeight = int(totalBytes >> 20)
		if totalWeight == 0 {
			totalWeight = 1
		}
	}
	for _, w := range sorted {
		group := byWorker[w]
		if len(group) == 0 {
			continue
		}
		// Proportional task share, floored at 1 so every worker's files
		// stay together rather than merging elsewhere.
		share := (groupWeight(group)*taskCount + totalWeight - 1) / totalWeight
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
	return fileSets, owners, balance
}

// shedSurplus rebalances assignee in place: workers holding more than
// (1+tolerance)× the fair byte share move surplus files — largest first,
// fewest moves — to each file's rendezvous runner-up (then third choice,
// and so on) provided the recipient stays inside the band. Pure function
// of its inputs: the same stage shape sheds the same files to the same
// recipients on every query, so shed placements converge warm in the
// recipients' caches. Returns nil when nothing moved.
func shedSurplus(files []string, fileSize map[string]int64, totalBytes int64, sorted []string, assignee map[string]string) *affineBalance {
	load := make(map[string]int64, len(sorted))
	for _, f := range files {
		load[assignee[f]] += fileSize[f]
	}
	fair := float64(totalBytes) / float64(len(sorted))
	band := int64(fair * (1 + affinityBalanceTolerance))
	maxLoad := func() int64 {
		var m int64
		for _, w := range sorted {
			if load[w] > m {
				m = load[w]
			}
		}
		return m
	}
	before := float64(maxLoad()) / fair
	bal := &affineBalance{TotalBytes: totalBytes, MaxShareBefore: before}
	for _, w := range sorted {
		if load[w] <= band {
			continue
		}
		// Largest-first: fewest files moved, fewest duplicate cache
		// copies and peer fetches. Name tie-break keeps it deterministic.
		owned := make([]string, 0, 8)
		for _, f := range files {
			if assignee[f] == w {
				owned = append(owned, f)
			}
		}
		sort.Slice(owned, func(i, j int) bool {
			if fileSize[owned[i]] != fileSize[owned[j]] {
				return fileSize[owned[i]] > fileSize[owned[j]]
			}
			return owned[i] < owned[j]
		})
		for _, f := range owned {
			if load[w] <= band {
				break
			}
			for _, r := range distributed.AffinityRank(f, sorted)[1:] {
				if load[r]+fileSize[f] > band {
					continue
				}
				assignee[f] = r
				load[w] -= fileSize[f]
				load[r] += fileSize[f]
				bal.ShedFiles++
				bal.ShedBytes += fileSize[f]
				break
			}
		}
	}
	if bal.ShedFiles == 0 {
		return nil
	}
	bal.MaxShareAfter = float64(maxLoad()) / fair
	return bal
}

// logAffineBalance records a fan-out's byte-balance shed on the dispatch
// log — the engagement marker the SF100 wlog/clog analysis greps for.
func (c *Coordinator) logAffineBalance(stageID string, bal *affineBalance) {
	if bal == nil {
		return
	}
	c.logger.Info("scan-affinity byte-balance",
		"stage_id", stageID,
		"total_bytes", bal.TotalBytes,
		"shed_files", bal.ShedFiles,
		"shed_bytes", bal.ShedBytes,
		"max_share_before", fmt.Sprintf("%.2f", bal.MaxShareBefore),
		"max_share_after", fmt.Sprintf("%.2f", bal.MaxShareAfter))
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
