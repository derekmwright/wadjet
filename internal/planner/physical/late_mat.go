package physical

import (
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/exec"
)

// LateMatJoinsPlanned counts local-pipeline hash-join probes planned with
// late materialization enabled. Observability-first, mirroring
// SortMergeJoinsPlanned: dormancy tests assert it stays zero with the flag
// off, and A/B arms use it (with exec.LateMatBatchesEmitted) to prove the
// treatment engaged rather than inferring from wall-clock deltas.
var LateMatJoinsPlanned atomic.Int64

// applyLateMaterialization stamps a planned probe with the planner's
// late-materialization setting and records the engagement marker.
func (p *Planner) applyLateMaterialization(probe *exec.HashJoinProbe) {
	if !p.LateMaterialization {
		return
	}
	probe.LateMaterialize = true
	LateMatJoinsPlanned.Add(1)
}
