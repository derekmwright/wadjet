package physical

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
)

// DimensionCascade gates markDimensionCascade. Kill switch
// WADJET_DIMENSION_CASCADE=0.
var DimensionCascade atomic.Bool

func init() {
	DimensionCascade.Store(os.Getenv("WADJET_DIMENSION_CASCADE") != "0")
}

// DimensionCascadesPlanned counts cascade annotations, process-wide.
var DimensionCascadesPlanned atomic.Int64

// Bloom sizing for cascade emits: cardinality-informed and CAPPED SMALL.
// The 2026-08-05 Q04 lesson: probe cost is set by bloom RESIDENCY, not
// theory — an 8 MB bitset costs ~0.35µs/row (cache-hostile), a ≤512 KB
// one costs single-digit ns (L2-resident). A dimension cascade probes the
// fact scan's full row count, so the bloom MUST stay small; a higher FPR
// from undersizing only weakens filtering, never breaks it.
const (
	cascadeBloomMinBits = 1 << 16 // 64 Kbit — tiny dimensions (nation/region)
	cascadeBloomMaxBits = 1 << 22 // 4 Mbit = 512 KB — L2/L3-resident ceiling
	// cascadeMaxEmitterRows bounds the FIRST hop's emitter (the filtered
	// dimension scan). Big filtered scans (orders, customer at SF100)
	// are excluded: their blooms would blow the residency cap and their
	// scan time would serialize the fact scan behind them.
	cascadeMaxEmitterRows = 2_000_000
)

// markDimensionCascade wires the two-hop dimension bloom cascade
// (docs/design/dimension-cascade.md):
//
//	D (tiny filtered dimension scan, e.g. nation σ(name))
//	  --hop A: emit D's join key--> B0 (mid dimension scan, e.g. supplier)
//	  --hop B: emit J's build key--> F (fact probe scan, e.g. lineitem l1)
//
// for an inner-form join stage J whose PRIMARY build is B0's scan and
// which carries a chained/fused join against D keyed on a column that
// ORIGINATES from B0 (BuildColOrigins / B0 column membership — the
// s_nationkey provenance). Hop A's consume forces B0's scan to dispatch
// (EmitDynamicFilters routes it through dispatchScanFilterStage), where
// the row-level bloom source applies the consume and the AtOutput emit
// then observes POST-consume rows — so hop B ships exactly the surviving
// build keys. Classic transitive semijoin reduction; safe for J's probe
// because an inner-form probe row without a matching build key produces
// nothing.
//
// Join-type eligibility mirrors the legacy scan→scan pass: "inner",
// "semi", and "left" (build feeds the inner side under probe-left
// convention). Outer-build shapes are excluded.
func (p *Planner) markDimensionCascade(ctx context.Context, stages []Stage) []Stage {
	if !DimensionCascade.Load() {
		return stages
	}
	byID := make(map[string]int, len(stages))
	for i := range stages {
		byID[stages[i].ID] = i
	}
	var seq int
	for ji := range stages {
		j := &stages[ji]
		if j.Type != StageHashJoin && j.Type != StageBroadcastJoin {
			continue
		}
		switch j.JoinType {
		case "inner", "semi", "left", "":
		default:
			continue
		}
		if len(j.JoinLeftKeys) != 1 || len(j.JoinRightKeys) != 1 {
			continue
		}
		// Primary build B0: walk to its leaf scan.
		b0Idx := findLeafScanStage(j.RightDepStage, stages, byID)
		if b0Idx < 0 {
			continue
		}
		b0 := &stages[b0Idx]
		if b0.TableName == "" || len(b0.ScanFiles) == 0 {
			continue
		}
		// Probe fact scan F.
		fIdx := findLeafScanStage(j.LeftDepStage, stages, byID)
		if fIdx < 0 || fIdx == b0Idx {
			continue
		}
		f := &stages[fIdx]
		// The fact scan must dispatch already (pushed filters) — the
		// consume rides its existing fragment. Pass-through fact scans
		// are exchange-fused shapes handled by the semi/anti pass.
		if len(f.FilterExprs) == 0 {
			continue
		}
		// Find a chained/fused dimension join keyed on a B0-origin column
		// whose build resolves to a TINY FILTERED leaf scan D.
		type dimHit struct {
			dIdx      int
			streamCol string // column of B0 the chained join probes on
			dKey      string // D's join key column
		}
		var hit *dimHit
		checkLeg := func(joinType string, leftKeys, rightKeys []string, buildDep string) {
			if hit != nil || len(leftKeys) != 1 || len(rightKeys) != 1 {
				return
			}
			switch joinType {
			case "inner", "semi", "left", "":
			default:
				return
			}
			streamCol := baseColName(leftKeys[0])
			if !hasColumn(b0.Columns, streamCol) {
				return // provenance: the probed column must live on B0
			}
			dIdx := findLeafScanStage(buildDep, stages, byID)
			if dIdx < 0 || dIdx == b0Idx || dIdx == fIdx {
				return
			}
			d := &stages[dIdx]
			if len(d.FilterExprs) == 0 || d.TableName == "" {
				return // unfiltered dimension = no reduction to ship
			}
			if d.EstimatedRows <= 0 || d.EstimatedRows > cascadeMaxEmitterRows {
				return
			}
			if !hasColumn(d.Columns, baseColName(rightKeys[0])) {
				return
			}
			hit = &dimHit{dIdx: dIdx, streamCol: streamCol, dKey: baseColName(rightKeys[0])}
		}
		for _, cj := range j.ChainedJoins {
			checkLeg(cj.JoinType, cj.JoinLeftKeys, cj.JoinRightKeys, cj.BuildDepStage)
		}
		for _, fj := range j.FusedJoins {
			checkLeg(fj.JoinType, fj.JoinLeftKeys, fj.JoinRightKeys, fj.BuildDepStage)
		}
		if hit == nil {
			continue
		}
		d := &stages[hit.dIdx]
		// Key types: integer class on both hops.
		dKeyType, ok := p.columnIntType(ctx, d.TableName, hit.dKey)
		if !ok {
			continue
		}
		b0KeyType, ok := p.columnIntType(ctx, b0.TableName, baseColName(j.JoinRightKeys[0]))
		if !ok {
			continue
		}
		// Cycle guards for both stat-dep edges.
		if stageReaches(d.ID, b0.ID, stages, byID) || stageReaches(b0.ID, f.ID, stages, byID) {
			continue
		}
		// Idempotence.
		if hasConsumeFor(b0.ConsumeDynamicFilters, d.ID, hit.streamCol) ||
			hasConsumeFor(f.ConsumeDynamicFilters, b0.ID, baseColName(j.JoinLeftKeys[0])) {
			continue
		}

		// Hop A: D --(dKey set)--> B0 on streamCol.
		hopA := fmt.Sprintf("dimc-%s-%d-a", j.ID, seq)
		d.EmitDynamicFilters = append(d.EmitDynamicFilters, DynamicFilterEmit{
			FilterID:  hopA,
			KeyColumn: hit.dKey,
			KeyType:   dKeyType,
			BloomBits: cascadeBloomBits(d.EstimatedRows),
			AtOutput:  true,
		})
		b0.ConsumeDynamicFilters = append(b0.ConsumeDynamicFilters, DynamicFilterConsume{
			FilterID:      hopA,
			SourceStageID: d.ID,
			TargetColumn:  hit.streamCol,
			KeyType:       dKeyType,
		})
		if !containsString(b0.Dependencies, d.ID) {
			b0.Dependencies = append(b0.Dependencies, d.ID)
		}
		// Hop B: B0 --(post-consume build-key set)--> F on the probe key.
		// The B0 emit forces dispatchScanFilterStage, whose sink-side
		// AtOutput emit runs after the row-level hop-A consume.
		hopB := fmt.Sprintf("dimc-%s-%d-b", j.ID, seq)
		b0.EmitDynamicFilters = append(b0.EmitDynamicFilters, DynamicFilterEmit{
			FilterID:  hopB,
			KeyColumn: baseColName(j.JoinRightKeys[0]),
			KeyType:   b0KeyType,
			BloomBits: cascadeBloomBits(b0.EstimatedRows / 8),
			AtOutput:  true,
		})
		f.ConsumeDynamicFilters = append(f.ConsumeDynamicFilters, DynamicFilterConsume{
			FilterID:      hopB,
			SourceStageID: b0.ID,
			TargetColumn:  baseColName(j.JoinLeftKeys[0]),
			KeyType:       b0KeyType,
		})
		if !containsString(f.Dependencies, b0.ID) {
			f.Dependencies = append(f.Dependencies, b0.ID)
		}
		seq++
		DimensionCascadesPlanned.Add(1)
		slog.Info("dimension_cascade: marked",
			"join", j.ID, "dim", d.ID, "mid", b0.ID, "fact", f.ID,
			"hop_a", hit.dKey+"->"+hit.streamCol,
			"hop_b", baseColName(j.JoinRightKeys[0])+"->"+baseColName(j.JoinLeftKeys[0]))
	}
	return stages
}

// cascadeBloomBits sizes a cascade bloom from an emitter-rows estimate,
// clamped to the L2-residency ceiling — see the constants' comment.
func cascadeBloomBits(estRows int64) int {
	if estRows <= 0 {
		estRows = 64 * 1024
	}
	bits := estRows * 10
	n := cascadeBloomMinBits
	for int64(n) < bits && n < cascadeBloomMaxBits {
		n <<= 1
	}
	return n
}
