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
	// FIXPOINT: a mid that gained a cascade consume in one iteration
	// qualifies as the next iteration's D (transitively-filtered dimension
	// — Q05's nation carries no predicate of its own; its key set narrows
	// only through region's bloom). Each iteration marks at most one new
	// segment per join; the chain region→nation→supplier→lineitem needs
	// two. Four iterations bounds any realistic snowflake depth; the
	// existing-consume dedup guarantees termination.
	var seq int
	for iter := 0; iter < 4; iter++ {
		if !p.markDimensionCascadeOnce(ctx, stages, byID, &seq) {
			break
		}
	}
	return stages
}

// markDimensionCascadeOnce runs one marking sweep; reports whether any
// segment was marked (the fixpoint driver's continue signal).
func (p *Planner) markDimensionCascadeOnce(ctx context.Context, stages []Stage, byID map[string]int, seqp *int) bool {
	seq := *seqp
	defer func() { *seqp = seq }()
	anyMarked := false
	// Scans that feed an exchange stage as its only input: a consume on
	// such a pass-through scan is FORWARDED into the exchange's tasks
	// (dispatchShuffleStage ships upstream.DynamicFilters), so it is a
	// legal bloom target even with no pushed filters of its own — Q05's
	// lineitem feeds exchange-repartition as a shuffle build.
	exchangeFed := make(map[int]bool)
	for i := range stages {
		s := &stages[i]
		if (s.Type == StageExchangeRepartition || s.Type == StageExchangeReplicate) && len(s.Dependencies) == 1 {
			if idx, ok := byID[s.Dependencies[0]]; ok && stages[idx].Type == StageScan {
				exchangeFed[idx] = true
			}
		}
	}
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
		// Probe fact scan F: the ROOT of J's probe chain, walked through
		// exchanges AND predecessor inner-form joins (Q21's shape: join-6
		// probes join-2's output through a repartition; the fact scan is
		// join-2's probe). Collect the predecessor joins on the way — a
		// dimension leg on J may pair with a leg from any of them.
		chainJoins := []*Stage{j}
		fIdx := -1
		cur := j.LeftDepStage
		for hops := 0; hops < 12; hops++ {
			idx, ok := byID[cur]
			if !ok {
				break
			}
			s := &stages[idx]
			if s.Type == StageScan {
				fIdx = idx
				break
			}
			if s.Type == StageHashJoin || s.Type == StageBroadcastJoin {
				switch s.JoinType {
				case "inner", "semi", "left", "":
				default:
					fIdx = -1
					hops = 12 // outer join breaks probe-row preservation
					break
				}
				chainJoins = append(chainJoins, s)
				cur = s.LeftDepStage
				continue
			}
			if (s.Type == StageExchangeRepartition || s.Type == StageExchangeReplicate) && len(s.Dependencies) >= 1 {
				cur = s.Dependencies[0]
				continue
			}
			break // aggregate/sort/etc.: stop conservatively
		}
		if fIdx < 0 {
			continue
		}
		f := &stages[fIdx]
		// The fact scan must dispatch already (pushed filters) — the
		// consume rides its existing fragment. Pass-through fact scans
		// are exchange-fused shapes handled by the semi/anti pass.
		if len(f.FilterExprs) == 0 {
			continue
		}
		// Enumerate ALL of J's legs uniformly: the primary build plus
		// every fused/chained leg. The 20260806-110951 ground truth: Q21's
		// join-4 has orders as PRIMARY, supplier as a fused/chained leg,
		// and nation keyed on a column owned by the SUPPLIER leg's build —
		// so D and B0 can each be any leg; anchoring B0 on the primary
		// (the first cut of this pass) matched nothing at SF100.
		type leg struct {
			joinType          string
			leftKey, rightKey string
			buildDep          string
		}
		var legs []leg
		for _, cj := range chainJoins {
			if len(cj.JoinLeftKeys) == 1 && len(cj.JoinRightKeys) == 1 {
				legs = append(legs, leg{cj.JoinType, baseColName(cj.JoinLeftKeys[0]), baseColName(cj.JoinRightKeys[0]), cj.RightDepStage})
			}
			for _, c := range cj.ChainedJoins {
				if len(c.JoinLeftKeys) == 1 && len(c.JoinRightKeys) == 1 {
					legs = append(legs, leg{c.JoinType, baseColName(c.JoinLeftKeys[0]), baseColName(c.JoinRightKeys[0]), c.BuildDepStage})
				}
			}
			for _, fu := range cj.FusedJoins {
				if len(fu.JoinLeftKeys) == 1 && len(fu.JoinRightKeys) == 1 {
					legs = append(legs, leg{fu.JoinType, baseColName(fu.JoinLeftKeys[0]), baseColName(fu.JoinRightKeys[0]), fu.BuildDepStage})
				}
			}
		}
		innerForm := func(t string) bool {
			switch t {
			case "inner", "semi", "left", "":
				return true
			}
			return false
		}
		// Candidate bloom targets: the fact probe root plus every leg's
		// build leaf scan (a leg's probe column can live on another leg's
		// build in build-heavy plans — Q05's lineitem).
		targetCandidates := []int{fIdx}
		for _, l := range legs {
			targetCandidates = append(targetCandidates, findLeafScanStage(l.buildDep, stages, byID))
		}
		marked := false
		// Reject reasons are collected unconditionally and logged when a
		// multi-leg join fails to match: plan-time only, one line per
		// no-match join, bounded. Three SF100 arms were spent (2026-08-06)
		// reconstructing why Q21's shape didn't match from dispatch lines
		// alone; this makes the next miss name itself.
		var rejects []string
		note := func(format string, a ...any) {
			if len(rejects) < 12 {
				rejects = append(rejects, fmt.Sprintf(format, a...))
			}
		}
		for di := range legs {
			if marked {
				break
			}
			legD := legs[di]
			if !innerForm(legD.joinType) {
				continue
			}
			dIdx := findLeafScanStage(legD.buildDep, stages, byID)
			if dIdx < 0 || dIdx == fIdx {
				note("D[%s→%s]: no leaf scan via %s", legD.rightKey, legD.leftKey, legD.buildDep)
				continue
			}
			d := &stages[dIdx]
			// D must be SELECTIVE: either its own pushed predicate, or a
			// cascade consume gained in an earlier fixpoint iteration
			// (transitively-filtered dimension — its emitted key set
			// narrows through the upstream bloom because the AtOutput emit
			// observes post-consume rows).
			selective := len(d.FilterExprs) > 0 || len(d.ConsumeDynamicFilters) > 0
			if !selective || d.TableName == "" ||
				d.EstimatedRows <= 0 || d.EstimatedRows > cascadeMaxEmitterRows ||
				!hasColumn(d.Columns, legD.rightKey) {
				note("D[%s→%s]=%s: filters=%d consumes=%d est=%d hasKey=%t", legD.rightKey, legD.leftKey,
					d.ID, len(d.FilterExprs), len(d.ConsumeDynamicFilters),
					d.EstimatedRows, hasColumn(d.Columns, legD.rightKey))
				continue
			}
			for bi := range legs {
				if bi == di {
					continue
				}
				legB := legs[bi]
				if !innerForm(legB.joinType) {
					continue
				}
				// Hop-B target: the leaf scan OWNING legB's probe column.
				// Usually the fact probe root — but in build-heavy plans
				// (Q05: orders is the probe root, lineitem arrives as a
				// shuffle BUILD) the leg's probe column lives on another
				// leg's build scan. Drop-only stays safe either way: an
				// inner-form row failing the leg produces nothing.
				tIdx := fIdx
				if !hasColumn(f.Columns, legB.leftKey) {
					tIdx = uniqueColumnOwner(legB.leftKey, targetCandidates, stages)
					if tIdx < 0 {
						note("B[%s→%s]: %s owner not unique among fact %s + leg scans", legB.rightKey, legB.leftKey, legB.leftKey, f.ID)
						continue
					}
					// The consume must have somewhere to ride: pushed
					// filters (dispatchScanFilterStage), a fused exchange,
					// or a pass-through scan feeding an exchange (spec
					// forwarding). A DIMENSION-CLASS target is also allowed
					// while undispatchable: an intermediate chain scan
					// (Q05's supplier at segment 1) only becomes an emitter
					// — which forces its dispatch and applies the consume —
					// when the NEXT fixpoint iteration marks the following
					// segment; until then the consume is a harmless no-op.
					t := &stages[tIdx]
					dispatched := len(t.FilterExprs) > 0 ||
						(t.Exchange != nil && len(t.Exchange.Keys) > 0 && t.Exchange.Count > 0) ||
						exchangeFed[tIdx]
					if !dispatched && t.EstimatedRows > cascadeMaxEmitterRows {
						note("B[%s→%s]: target %s undispatchable (no filters/exchange/forwarding) and above dimension class", legB.rightKey, legB.leftKey, t.ID)
						continue
					}
				}
				t := &stages[tIdx]
				if tIdx == dIdx {
					continue
				}
				b0Idx := findLeafScanStage(legB.buildDep, stages, byID)
				if b0Idx < 0 || b0Idx == fIdx || b0Idx == dIdx || b0Idx == tIdx {
					note("B[%s→%s]: no leaf scan via %s", legB.rightKey, legB.leftKey, legB.buildDep)
					continue
				}
				b0 := &stages[b0Idx]
				// The mid becomes an EMITTER: the dimension-class size cap
				// applies to it exactly as to D — emitters bypass the
				// dispatch semaphore and ride the priority lane, whose
				// memory rationale assumes tiny scans, and an oversized
				// mid's bloom saturates the L2-residency ceiling anyway.
				// (The 2-hop matcher got this bound implicitly from join
				// shape; the fixpoint + generalized target must enforce it:
				// customer at SF100 is 15M rows and must not qualify.)
				if b0.EstimatedRows <= 0 || b0.EstimatedRows > cascadeMaxEmitterRows {
					note("B0 via %s: est=%d exceeds emitter cap", legB.buildDep, stages[b0Idx].EstimatedRows)
					continue
				}
				if b0.TableName == "" || len(b0.ScanFiles) == 0 ||
					!hasColumn(b0.Columns, legD.leftKey) ||
					!hasColumn(b0.Columns, legB.rightKey) {
					note("B0=%s: scanFiles=%d hasStream(%s)=%t hasKey(%s)=%t cols=%v", b0.ID,
						len(b0.ScanFiles), legD.leftKey, hasColumn(b0.Columns, legD.leftKey),
						legB.rightKey, hasColumn(b0.Columns, legB.rightKey), b0.Columns)
					continue
				}
				dKeyType, ok := p.columnIntType(ctx, d.TableName, legD.rightKey)
				if !ok {
					continue
				}
				b0KeyType, ok := p.columnIntType(ctx, b0.TableName, legB.rightKey)
				if !ok {
					continue
				}
				if stageReaches(d.ID, b0.ID, stages, byID) || stageReaches(b0.ID, t.ID, stages, byID) {
					continue
				}
				// Segment dedup: this exact hop-B already marked (fixpoint
				// termination). Hop-A dedup is a REUSE, not a skip — chain
				// segments share edges (segment 1's hop-B nation→supplier
				// IS segment 2's hop-A).
				if hasConsumeFor(t.ConsumeDynamicFilters, b0.ID, legB.leftKey) {
					continue
				}
				hopAExists := hasConsumeFor(b0.ConsumeDynamicFilters, d.ID, legD.leftKey)

				if !hopAExists {
					// Hop A: D --(legD.rightKey set)--> B0 on legD.leftKey.
					hopA := fmt.Sprintf("dimc-%s-%d-a", j.ID, seq)
					d.EmitDynamicFilters = append(d.EmitDynamicFilters, DynamicFilterEmit{
						FilterID:  hopA,
						KeyColumn: legD.rightKey,
						KeyType:   dKeyType,
						BloomBits: cascadeBloomBits(d.EstimatedRows),
						AtOutput:  true,
					})
					b0.ConsumeDynamicFilters = append(b0.ConsumeDynamicFilters, DynamicFilterConsume{
						FilterID:      hopA,
						SourceStageID: d.ID,
						TargetColumn:  legD.leftKey,
						KeyType:       dKeyType,
					})
					if !containsString(b0.Dependencies, d.ID) {
						b0.Dependencies = append(b0.Dependencies, d.ID)
					}
				}
				// Hop B: B0 --(post-consume legB.rightKey set)--> T on
				// legB.leftKey. The B0 emit forces dispatchScanFilterStage,
				// whose sink-side AtOutput emit runs after the row-level
				// hop-A consume.
				hopB := fmt.Sprintf("dimc-%s-%d-b", j.ID, seq)
				b0.EmitDynamicFilters = append(b0.EmitDynamicFilters, DynamicFilterEmit{
					FilterID:  hopB,
					KeyColumn: legB.rightKey,
					KeyType:   b0KeyType,
					BloomBits: cascadeBloomBits(b0.EstimatedRows / 8),
					AtOutput:  true,
				})
				t.ConsumeDynamicFilters = append(t.ConsumeDynamicFilters, DynamicFilterConsume{
					FilterID:      hopB,
					SourceStageID: b0.ID,
					TargetColumn:  legB.leftKey,
					KeyType:       b0KeyType,
				})
				if !containsString(t.Dependencies, b0.ID) {
					t.Dependencies = append(t.Dependencies, b0.ID)
				}
				seq++
				DimensionCascadesPlanned.Add(1)
				slog.Info("dimension_cascade: marked",
					"join", j.ID, "dim", d.ID, "mid", b0.ID, "target", t.ID, "fact", f.ID,
					"hop_a", legD.rightKey+"->"+legD.leftKey, "hop_a_reused", hopAExists,
					"hop_b", legB.rightKey+"->"+legB.leftKey)
				marked = true
				anyMarked = true
				break
			}
		}
		if !marked && len(legs) > 1 {
			legStrs := make([]string, 0, len(legs))
			for _, l := range legs {
				legStrs = append(legStrs, fmt.Sprintf("%s→%s(dep=%s,jt=%q)", l.rightKey, l.leftKey, l.buildDep, l.joinType))
			}
			slog.Info("dimension_cascade: debug no-match",
				"join", j.ID, "fact", f.ID, "legs", legStrs, "rejects", rejects)
		}
	}
	return anyMarked
}

// uniqueColumnOwner returns the single candidate scan whose Columns contain
// col, or -1 when zero or multiple candidates match — an ambiguous owner
// must never receive a bloom (a wrong target falsely rejects rows).
func uniqueColumnOwner(col string, candidates []int, stages []Stage) int {
	owner := -1
	for _, idx := range candidates {
		if idx < 0 {
			continue
		}
		if hasColumn(stages[idx].Columns, col) {
			if owner >= 0 && owner != idx {
				return -1
			}
			owner = idx
		}
	}
	return owner
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
