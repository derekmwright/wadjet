// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// EstimateCost computes the aggregate cost of a set of stages.
func EstimateCost(stages []Stage, node *logical.Node) physical.QueryCost {
	var cost physical.QueryCost
	for _, s := range stages {
		if s.Type == "scan" {
			cost.TotalBytes += s.EstimatedBytes
			cost.TotalRows += s.EstimatedRows
			cost.TotalFiles += len(s.ScanFiles)
		}
	}
	cost.HasFilter = localPlanFacts.HasFilterOrPartition(node)
	cost.HasLimit = localPlanFacts.HasLimit(node)
	return cost
}

// CanProbeSplit returns the scan alias and file list for probe-split pipeline
// routing. Probe-split distributes the dominant probe table's files across
// workers while each worker scans build tables in full. This enables parallel
// execution for join-heavy queries where compute is the bottleneck.
//
// Returns the probe scan alias, its file list, and true if probe-split is viable.
func CanProbeSplit(stages []Stage, workerCount int) (probeAlias string, probeFiles []string, ok bool) {
	if workerCount <= 1 {
		return "", nil, false
	}

	// Pick the largest scan as the probe-split candidate. No exclusions:
	// the physical planner uses RightSemiJoin/RightAntiJoin to swap the
	// local join's build/probe when the inner table is much larger than
	// the outer. This makes it safe to partition the inner (large) table
	// across workers — each worker builds the small outer as hash table
	// and probes with its partition of the large inner table.
	var bestAlias string
	var bestFiles []string
	var bestBytes int64
	for _, s := range stages {
		if s.Type == "scan" && s.EstimatedBytes > bestBytes {
			bestAlias = s.ScanAlias
			bestFiles = s.ScanFiles
			bestBytes = s.EstimatedBytes
		}
	}

	// Need enough files to give each worker meaningful work. For large
	// datasets (> 1 GB), relax from 2 files/worker to 1 file/worker since
	// each file is substantial. At SF100, tables may have only 3-6 files
	// but each is multi-GB.
	minFiles := workerCount * 2
	if bestBytes > 1<<30 {
		minFiles = workerCount
	}
	if bestAlias == "" || len(bestFiles) < minFiles || bestBytes < ProbeSplitMinBytes {
		return "", nil, false
	}

	return bestAlias, bestFiles, true
}

// fusesIntoACTETerminal detects scan-aggregate fusion that would rewrite a
// recorded CTE terminal from relation rows to one consumer's partial aggregates.
// Decline even if reference counts say zero: scalar-subquery TEXT references are
// planned in later walks sharing the cache (#876). Recording a terminal promises
// that later consumers may read its original stream. This is the shared-producer
// ownership rule for filters/projections (#656), at the cost of scan→aggregate
// materialization. canFuseScanAggregate separately requires all children be scans.
// See docs/internals/cte-terminal-aggregate-fusion-boundary.md for the design.
func (p *StagePlanner) fusesIntoACTETerminal(childStages []Stage) bool {
	if len(p.ctePlannedTerminal) == 0 {
		return false
	}
	terminals := make(map[string]bool, len(p.ctePlannedTerminal))
	for _, id := range p.ctePlannedTerminal {
		terminals[id] = true
	}
	for _, s := range childStages {
		if terminals[s.ID] {
			return true
		}
	}
	return false
}

func canFuseScanAggregate(childStages []Stage) bool {
	if len(childStages) == 0 {
		return false
	}
	for _, s := range childStages {
		if s.Type != "scan" {
			return false
		}
	}
	return true
}

// needsLimitStage gives each LIMIT exactly one owner: coordinator post-gather
// for the plan root, an unclaimed sort's top-N for sorted LIMIT without OFFSET,
// or a dedicated StageLimit for everything else (#478), including OFFSET alone.
// Sort top-N truncates to limit+offset but does not skip. sorted means THIS LIMIT
// owns that sort, not that any sort exists below; stop the backward search at
// an already-claimed sort so an outer LIMIT cannot overwrite an inner one (#525).
// Never also stage the root LIMIT: that would apply OFFSET twice.
func (p *StagePlanner) needsLimitStage(node *logical.Node, sorted bool) bool {
	if node == p.limitStageRoot {
		return false // the coordinator's post-gather pass owns this one
	}
	if sorted && node.OffsetVal == 0 {
		return false // the sort stage's top-N is already the global bound
	}
	// An OFFSET alone still needs a stage: nothing below skips rows.
	return node.LimitVal != logical.NoLimit || node.OffsetVal > 0
}
