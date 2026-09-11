package physical

import (
	"log/slog"
	"os"
	"sync/atomic"
)

// Attach-on-arrival replaces eligible dynamic-filter hard dependencies with
// nonblocking consumes. Blooms are drop-only; joins recheck false positives.
// Require a leaf-scan emitter; join-fed emitters keep the barrier to avoid
// finishing the consumer before filtering. Require real scan-filter fragment
// tasks (filters/projections/fused exchange), not pass-through scans or fused
// scan-aggregates, whose dispatcher has no dynamic-filter plumbing.
// Re-emitting consumers require post-consume keys; late attachment can widen
// the downstream filter and defeat the cascade.
// See docs/internals/dynamic-filter-attach-on-arrival.md for the design.

// DFAttachOnArrival gates applyAttachOnArrival. Kill switch
// WADJET_DF_ATTACH_ON_ARRIVAL=0 restores the barrier on every edge.
var DFAttachOnArrival atomic.Bool

// DFGuardedReemit gates the rule-1 relaxation: a RE-EMITTING consumer
// (cascade mid-scan) may also convert to attach mode when every one of its
// emits is an AtScan accumulator — the emit op then buffers rows scanned
// before the consumed bloom lands and retro-filters them at finalize
// (guarded re-emit), preserving downstream filter quality without the
// start barrier.
//
// DEFAULT OFF (opt-in WADJET_DF_GUARDED_REEMIT=1). The SF100 pair
// 2026-08-07 (ctl 6c173cf 16:07 / trt 944e640 16:29) proved the guard
// mechanism itself sound (guard_wait_ms median 46ms, retro-filter at
// exact dim selectivity) but exposed the relaxation's structural blind
// spot: the barrier also protects the mid-scan's OUTPUT volume. At SF100
// the mid's 1-2s scan always ends before the dim bloom arrives (2-4s),
// so its output ships 100% unfiltered — full supplier (8×240K rows) into
// the broadcast replicate + join build instead of the nation-filtered ~8%
// — costing Q05/Q07/Q21 +33-60% in both guarded arms. Re-emitters keep
// the barrier until a shape exists that starts the scan early WITHOUT
// shipping the unfiltered head (e.g. worker-side scan-start hold on the
// deferred bloom).
var DFGuardedReemit atomic.Bool

func init() {
	DFAttachOnArrival.Store(os.Getenv("WADJET_DF_ATTACH_ON_ARRIVAL") != "0")
	DFGuardedReemit.Store(os.Getenv("WADJET_DF_GUARDED_REEMIT") == "1")
}

// AttachOnArrivalConsumesPlanned counts converted consume edges,
// process-wide. A/B observability, same family as DynamicFiltersPlanned.
var AttachOnArrivalConsumesPlanned atomic.Int64

// applyAttachOnArrival converts eligible consume edges to attach-on-arrival
// mode: sets Consume.AttachOnArrival, marks the matching emit LateAttach
// (the coordinator then always stages the merged artifact), and removes the
// stat-dep edge so the consumer dispatches immediately. Must run AFTER all
// dyn-filter marking passes (their emit/consume state must be final) and
// BEFORE the plan validators (edge changes must be visible to them).
func applyAttachOnArrival(stages []Stage) []Stage {
	if !DFAttachOnArrival.Load() {
		return stages
	}
	byID := make(map[string]int, len(stages))
	for i := range stages {
		byID[stages[i].ID] = i
	}
	for ci := range stages {
		c := &stages[ci]
		if len(c.ConsumeDynamicFilters) == 0 {
			continue
		}
		// Rule 1: terminal consumers convert freely. Re-emitting consumers
		// (the cascade mid-scan shape) convert IFF their emitted key set is
		// provably identical under an AtScan guard: the stage is a scan
		// with NO FilterExprs (its only filtering is the consume itself),
		// so repositioning the emit from the sink (AtOutput) to the scan
		// head + guarding it reproduces the post-consume key set exactly —
		// rows scanned before the consumed bloom installs buffer as
		// (emit-key, guard-column) pairs and retro-filter on settle.
		// Stages with FilterExprs or join-fed AtOutput emits (semi/anti
		// build filters ride non-scan stages) keep the barrier: their
		// emitted set depends on filtering a column-pair guard can't
		// reproduce.
		guarded := false
		if len(c.EmitDynamicFilters) > 0 {
			// No FilterExprs/projections: the scan-head emit position must
			// observe the same rows and columns the sink would (projections
			// could compute or alias the emit key away from the raw scan
			// schema). Cascade mids carry neither. Log the reject reason —
			// a silently-kept barrier here is invisible in an A/B run.
			reason := ""
			switch {
			case !DFGuardedReemit.Load():
				reason = "kill_switch"
			case len(c.FilterExprs) > 0:
				reason = "filter_exprs"
			case len(c.ProjectExprs) > 0:
				reason = "project_exprs"
			case len(c.SecurityProjectExprs) > 0:
				reason = "security_project_exprs"
			}
			if reason != "" {
				slog.Info("dynamic_filter: guarded re-emit rejected",
					"consumer", c.ID, "reason", reason)
				continue
			}
			guarded = true
		}
		// Rule 3: consumer must take the dispatched scan-filter fragment
		// path — the only dispatch shape that wires OpSpec.DynamicFilters
		// into a runtime pipeline (dispatchPipelineStage routing).
		if c.Type != StageScan || len(c.ScanFiles) == 0 {
			continue
		}
		if len(c.FusedAggSpecs) > 0 || len(c.FusedAggGroupBy) > 0 {
			continue
		}
		// Emit-bearing scans are a dispatch-shape witness on their own:
		// their emits only function via dispatchScanFilterStage's fragment
		// plumbing (observed: cascade mid-scans dispatch there with zero
		// FilterExprs), so `guarded` implies the fragment path.
		dispatched := guarded || len(c.FilterExprs) > 0 || len(c.ProjectExprs) > 0 ||
			len(c.SecurityProjectExprs) > 0 ||
			(c.Exchange != nil && len(c.Exchange.Keys) > 0 && c.Exchange.Count > 0)
		if !dispatched {
			continue
		}
		for fi := range c.ConsumeDynamicFilters {
			consume := &c.ConsumeDynamicFilters[fi]
			srcIdx, ok := byID[consume.SourceStageID]
			if !ok {
				continue
			}
			src := &stages[srcIdx]
			// Rule 2: scan-only emitters.
			if src.Type != StageScan || len(src.ScanFiles) == 0 {
				continue
			}
			consume.AttachOnArrival = true
			for ei := range src.EmitDynamicFilters {
				if src.EmitDynamicFilters[ei].FilterID == consume.FilterID {
					src.EmitDynamicFilters[ei].LateAttach = true
				}
			}
			if guarded {
				// Every emit on this stage must retro-filter its buffered
				// head rows through this consume's bloom, and reposition to
				// the scan head (AtScan) where the guard column is still
				// present — equivalent by the no-FilterExprs/no-projection
				// eligibility above.
				for ei := range c.EmitDynamicFilters {
					c.EmitDynamicFilters[ei].GuardConsumes = append(
						c.EmitDynamicFilters[ei].GuardConsumes, consume.FilterID)
					c.EmitDynamicFilters[ei].AtOutput = false
				}
			}
			AttachOnArrivalConsumesPlanned.Add(1)
			slog.Info("dynamic_filter: attach-on-arrival",
				"consumer", c.ID, "source", src.ID, "filter_id", consume.FilterID,
				"guarded_reemit", guarded)
		}
		// Drop stat-dep edges whose every consume from that source converted.
		c.Dependencies = filterAttachedStatDeps(c.Dependencies, c.ConsumeDynamicFilters)
	}
	return stages
}

// filterAttachedStatDeps removes dependency IDs that exist purely as
// stat-dep edges for consumes that all converted to attach mode. A dep is
// kept when any wait-mode consume still references it, or when it is not a
// consume source at all (defense: leaf scans have no other dep kinds today,
// but a future structural dep must never be dropped here).
func filterAttachedStatDeps(deps []string, consumes []DynamicFilterConsume) []string {
	if len(deps) == 0 {
		return deps
	}
	attached := make(map[string]bool, len(consumes))
	for _, cf := range consumes {
		if cf.AttachOnArrival {
			if _, seen := attached[cf.SourceStageID]; !seen {
				attached[cf.SourceStageID] = true
			}
		}
	}
	for _, cf := range consumes {
		if !cf.AttachOnArrival {
			attached[cf.SourceStageID] = false
		}
	}
	out := deps[:0:0]
	for _, d := range deps {
		if attached[d] {
			continue
		}
		out = append(out, d)
	}
	return out
}
