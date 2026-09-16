// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import "github.com/derekmwright/wadjet/internal/planner/logical"

// isBroadcastCandidate returns true if the right (build) side of a join is
// small enough to broadcast to all workers. When broadcast, the build side
// is sent to every worker and the probe side is split round-robin across
// workers — no shuffle stages needed for either side.
func (p *StagePlanner) isBroadcastCandidate(joinNode *logical.Node) bool {
	if len(joinNode.Children) < 2 {
		return false
	}
	totalBytes, ok := p.EstimateSubtreeBytes(joinNode.Children[1])
	if !ok {
		return false
	}
	// Broadcast threshold: defaults to 100 MB (legacy behavior). Distributed
	// callers override via BroadcastBytesThreshold to adapt the decision to
	// per-worker pool budget so a moderate build (say 500 MB) on a tight
	// cluster falls back to hash-shuffle instead of multiplying memory
	// pressure N× across worker procs.
	threshold := int64(100 * 1024 * 1024)
	if p.BroadcastBytesThreshold > 0 {
		threshold = p.BroadcastBytesThreshold
	} else if p.BroadcastBytesThreshold < 0 {
		return false // broadcast disabled
	}
	return totalBytes <= threshold
}
