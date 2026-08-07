package physical

import (
	"log/slog"
	"os"
	"sync/atomic"
)

// Attach-on-arrival normalization for dynamic-filter consume edges
// (docs/design/attach-on-arrival-dynamic-filters.md).
//
// Every dyn-filter marking pass wires its consume as a hard stat-dep: the
// consumer's dispatch blocks on the emitter stage completing and merging.
// That barrier is coverage-maximizing but not required for correctness —
// the blooms are drop-only and every false positive is re-verified by the
// downstream join. For edges where the emitter chain is scan-only, the
// barrier costs 2-4s of start serialization per marked query (per-stage
// dispatch round-trips + partial merge) while buying a few percent of head
// filtering coverage. This pass converts exactly those edges to
// non-blocking "attach-on-arrival" consumes.
//
// The mode decision is structural — no thresholds, no query names:
//
//  1. The consumer must not itself emit dynamic filters. A re-emitting
//     consumer (cascade mid-scan B0) derives its emitted key set from its
//     post-consume output; a late-attached consume would silently widen
//     the downstream filter (hop-B would ship every supplier key scanned
//     before nation's bloom landed), destroying the cascade transitively.
//  2. The emitter must be a leaf scan stage. Join-fed emitters (semi/anti
//     build filters) complete only after a multi-stage probe chain; the
//     consumer would finish mostly unfiltered and the downstream shuffle/
//     build volume would revert to raw size. Those edges keep the barrier.
//  3. The consumer must dispatch real fragment tasks via the scan-filter
//     path (pushed filters / projections / fused exchange, and NOT fused
//     scan-aggregate — dispatchScanAggregateStage carries no dyn-filter
//     plumbing). Pass-through scans have no runtime pipeline to attach
//     into.

// DFAttachOnArrival gates applyAttachOnArrival. Kill switch
// WADJET_DF_ATTACH_ON_ARRIVAL=0 restores the barrier on every edge.
var DFAttachOnArrival atomic.Bool

func init() {
	DFAttachOnArrival.Store(os.Getenv("WADJET_DF_ATTACH_ON_ARRIVAL") != "0")
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
		// Rule 1: terminal consumers only.
		if len(c.EmitDynamicFilters) > 0 {
			continue
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
		dispatched := len(c.FilterExprs) > 0 || len(c.ProjectExprs) > 0 ||
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
			AttachOnArrivalConsumesPlanned.Add(1)
			slog.Info("dynamic_filter: attach-on-arrival",
				"consumer", c.ID, "source", src.ID, "filter_id", consume.FilterID)
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
