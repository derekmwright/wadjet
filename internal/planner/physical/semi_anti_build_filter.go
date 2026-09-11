package physical

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// SemiAntiBuildFilter gates markSemiAntiBuildFilters. Kill switch
// WADJET_SEMIANTI_BUILD_FILTER=0.
//
// Registered with optswitch rather than kept as a bare atomic.Bool (the
// ExchangeSubsume pattern it used to follow). The pass drops build-side rows
// against a key set collected from another stage, so it can change the ANSWER
// — which is the definition of a switch the invariance oracle must enumerate
// (#287). An ad-hoc env var is invisible to optswitch.All(), so the oracle
// never ran a single corpus query with this optimization off.
var SemiAntiBuildFilter = optswitch.Register("semianti-build-filter", "WADJET_SEMIANTI_BUILD_FILTER",
	"semi/anti build-side filtering: filter a shared build scan by the probe stage's key set")

// SemiAntiBuildFiltersPlanned counts build-filter annotations the pass
// produced, process-wide. Mechanism marker for A/B runs (the
// DynamicFiltersPlanned convention).
var SemiAntiBuildFiltersPlanned atomic.Int64

// Bloom sizing bounds. Keys at the source are unknown at plan time (the
// source is a join or filtered scan); clamp 10 bits/key on the source's
// row estimate into [1 Mbit, 64 Mbit]. An undersized bloom raises FPR —
// weaker filtering, never wrong. 64 Mbit = 8 MB artifact, S3-staged.
const (
	semiAntiBloomMinBits = 1 << 20
	semiAntiBloomMaxBits = 1 << 26
	// semiAntiBloomDefaultRows sizes the bloom when the source stage has
	// no row estimate (joins typically don't) — 4M keys → 40 Mbit → fits
	// TPC-H SF100's observed source cardinalities (Q21 join output ≈ 5-7M
	// rows) with ~1% FPR while staying inside the cap.
	semiAntiBloomDefaultRows = 4_000_000
)

// markSemiAntiBuildFilters filters equi semi/anti hash builds by runtime probe
// keys: probe dependency S emits, base scan B consumes with a stat-dep B ← S.
// False positives retain extra rows; failed/missing emits leave builds unfiltered.
// Require one integer key and a leaf build scan behind a sole-dependency repartition
// or carrying an absorbed Exchange. Probe S must show reduction via join,
// aggregate, or pushed-filter scan over a DIFFERENT table; reject cycles through
// B or its exchange. NULL skipping/dropping relies on null-insensitive equi keys.
// See docs/design/semi-anti-build-dynamic-filters.md and
// docs/internals/semi-anti-build-filter-wiring.md for the design.
func (p *Planner) markSemiAntiBuildFilters(ctx context.Context, stages []Stage) []Stage {
	if !SemiAntiBuildFilter.On() {
		return stages
	}
	byID := make(map[string]int, len(stages))
	for i := range stages {
		byID[stages[i].ID] = i
	}
	consumers := make(map[string][]int, len(stages))
	for i := range stages {
		for _, d := range stages[i].Dependencies {
			consumers[d] = append(consumers[d], i)
		}
	}
	var seq int
	for ji := range stages {
		j := &stages[ji]
		if j.Type != StageHashJoin {
			continue
		}
		if j.JoinType != "semi" && j.JoinType != "anti" {
			continue
		}
		if len(j.JoinLeftKeys) != 1 || len(j.JoinRightKeys) != 1 {
			continue
		}
		buildIdx, exchangeID := semiAntiBuildScan(j.RightDepStage, stages, byID)
		if buildIdx < 0 {
			continue
		}
		bscan := &stages[buildIdx]
		buildKey := baseColName(j.JoinRightKeys[0])
		probeKey := baseColName(j.JoinLeftKeys[0])
		// Key must be a column of B (its name is what the consume filter
		// tests against B's scanned batches).
		if !hasColumn(bscan.Columns, buildKey) {
			continue
		}
		// Integer key class via catalog (bloom hashes int64; int32/date
		// widen losslessly so cross-width probe/build pairs still agree).
		keyType, ok := p.columnIntType(ctx, bscan.TableName, buildKey)
		if !ok {
			continue
		}
		sIdx, ok2 := byID[j.LeftDepStage]
		if !ok2 {
			continue
		}
		// Resolve the emit source THROUGH exchange hops: an exchange only
		// re-partitions its input — the key set is identical — and its
		// shuffle tasks aren't fragment-shaped, so the emit lands on the
		// stage below (whose fragment sink accumulates the same keys).
		for hops := 0; hops < 4; hops++ {
			st := &stages[sIdx]
			if (st.Type != StageExchangeRepartition && st.Type != StageExchangeReplicate) ||
				len(st.Dependencies) != 1 {
				break
			}
			ni, ok := byID[st.Dependencies[0]]
			if !ok {
				break
			}
			sIdx = ni
		}
		s := &stages[sIdx]
		if !semiAntiProbeSourceEligible(s, bscan) {
			continue
		}
		// Both sides, not just the build side — the alignment
		// applyDynamicFilters already had. The build column's type decides
		// the KeyType the emit op hashes with; the EMIT runs over S's
		// output, so it is S's column that DynamicFilterEmitOp indexes, and
		// a cross-class pair (INT64 build, STRING probe) would hash two
		// different things under one filter id.
		//
		// When S names no table — a join output, an aggregate — the catalog
		// cannot answer and this check does not apply. That gap is why the
		// emit op carries its own runtime guard (dynamicFilterIntKey in
		// internal/engine/exec/dynamic_filter_emit.go): a planner check runs
		// at a different time, over the catalog, on a column name that may
		// not be the one that arrives.
		if s.TableName != "" {
			probeType, okProbe := p.columnIntType(ctx, s.TableName, probeKey)
			if !okProbe || probeType != keyType {
				continue
			}
		}
		// Cycle guard: the stat-dep edge B ← S must not close a loop.
		if stageReaches(s.ID, bscan.ID, stages, byID) ||
			(exchangeID != "" && stageReaches(s.ID, exchangeID, stages, byID)) {
			continue
		}
		// The filtered output is SHARED: every consumer of B (and of its
		// exchange) must be a semi/anti hash join whose probe input's key
		// set is a subset of S's output keys — i.e. its probe stage IS S
		// or probe-descends from S (each hop only drops or repeats probe
		// rows, never invents key values). Any other consumer would read
		// build rows filtered against keys it never agreed to.
		if !allConsumersSemiAntiFrom(bscan.ID, exchangeID, s.ID, stages, byID, consumers) {
			continue
		}
		// COST eligibility: the row-level bloom probe taxes EVERY scanned
		// build row (~0.35µs/row against a multi-MB cache-hostile bitset —
		// SF100 Q04 2026-08-04: +137 cpu-s on a 380M-row scan), while the
		// saving is the shuffle write + hash build of REJECTED rows. One
		// cheap key-only build doesn't pay for it (Q04 cold +19s, Q22
		// steady +11s); two or more builds sharing the filtered exchange
		// do (Q21: semi + anti over the same raw lineitem, cold −31%).
		// Count LOGICAL builds: primary semi/anti consumers plus chained
		// semi/anti joins whose build rides the same exchange.
		if countSemiAntiBuilds(bscan.ID, exchangeID, stages, consumers) < 2 {
			continue
		}
		// Dedupe: one consume per (B, source, column). A second semi/anti
		// join sharing B and S reuses the existing filter.
		if hasConsumeFor(bscan.ConsumeDynamicFilters, s.ID, buildKey) {
			continue
		}
		filterID := fmt.Sprintf("sabf-%s-%d", j.ID, seq)
		seq++
		emitSpec := DynamicFilterEmit{
			FilterID:  filterID,
			KeyColumn: probeKey,
			KeyType:   keyType,
			BloomBits: semiAntiBloomBits(s.EstimatedRows),
			AtOutput:  true,
		}
		// Reuse an existing output-emit on S for the same column (two
		// builds filtered from one source).
		reused := false
		for _, e := range s.EmitDynamicFilters {
			if e.AtOutput && e.KeyColumn == probeKey {
				emitSpec = e
				reused = true
				break
			}
		}
		if !reused {
			s.EmitDynamicFilters = append(s.EmitDynamicFilters, emitSpec)
		}
		bscan.ConsumeDynamicFilters = append(bscan.ConsumeDynamicFilters, DynamicFilterConsume{
			FilterID:      emitSpec.FilterID,
			SourceStageID: s.ID,
			TargetColumn:  buildKey,
			KeyType:       keyType,
		})
		if !containsString(bscan.Dependencies, s.ID) {
			bscan.Dependencies = append(bscan.Dependencies, s.ID)
		}
		SemiAntiBuildFiltersPlanned.Add(1)
		slog.Info("semi_anti_build_filter: marked",
			"join", j.ID, "join_type", j.JoinType,
			"build_scan", bscan.ID, "build_exchange", exchangeID,
			"source", s.ID, "filter_id", emitSpec.FilterID,
			"key", buildKey, "bloom_bits", emitSpec.BloomBits)
	}
	return stages
}

// semiAntiBuildScan resolves the build-side base scan for the pass.
// Returns (scan index, exchange stage ID or "") — or (-1, "") when the
// shape is unsupported.
func semiAntiBuildScan(depID string, stages []Stage, byID map[string]int) (int, string) {
	idx, ok := byID[depID]
	if !ok {
		return -1, ""
	}
	s := &stages[idx]
	switch s.Type {
	case StageExchangeRepartition:
		if len(s.Dependencies) != 1 {
			return -1, ""
		}
		si, ok := byID[s.Dependencies[0]]
		if !ok || stages[si].Type != StageScan || len(stages[si].ScanFiles) == 0 {
			return -1, ""
		}
		return si, s.ID
	case StageScan:
		// fuseScanShuffle absorbed the exchange into the scan; the scan's
		// dispatched fragment applies DynamicFilters row-level.
		if len(s.ScanFiles) == 0 {
			return -1, ""
		}
		return idx, ""
	}
	return -1, ""
}

// semiAntiProbeSourceEligible reports whether S's output plausibly covers
// far fewer keys than B's raw rows.
func semiAntiProbeSourceEligible(s, bscan *Stage) bool {
	if s.ID == bscan.ID {
		return false
	}
	switch s.Type {
	case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
		return true
	case StageAggregate, "final_aggregate", "merge_aggregate":
		return true
	case StageScan:
		return len(s.FilterExprs) > 0 && s.TableName != bscan.TableName
	}
	return false
}

// stageReaches reports whether from transitively depends on target.
func stageReaches(from, target string, stages []Stage, byID map[string]int) bool {
	seen := make(map[string]bool)
	var walk func(id string) bool
	walk = func(id string) bool {
		if id == target {
			return true
		}
		if seen[id] {
			return false
		}
		seen[id] = true
		idx, ok := byID[id]
		if !ok {
			return false
		}
		for _, d := range stages[idx].Dependencies {
			if walk(d) {
				return true
			}
		}
		return false
	}
	return walk(from)
}

// allConsumersSemiAntiFrom verifies the shared-output safety condition:
// every consumer of scan B — and, when present, of its exchange — is a
// semi/anti hash join using it as the BUILD side whose probe stage equals
// S or probe-descends from S. (The exchange itself consuming B is the one
// structural exception.)
func allConsumersSemiAntiFrom(scanID, exchangeID, sourceID string, stages []Stage, byID map[string]int, consumers map[string][]int) bool {
	check := func(ids []int) bool {
		for _, ci := range ids {
			c := &stages[ci]
			if c.ID == exchangeID {
				continue // the exchange stage carrying B's payload
			}
			if c.ID == sourceID {
				continue // the stat-dep back-edge target itself
			}
			if c.Type != StageHashJoin || (c.JoinType != "semi" && c.JoinType != "anti") {
				return false
			}
			if c.RightDepStage != scanID && c.RightDepStage != exchangeID {
				return false
			}
			if c.LeftDepStage != sourceID &&
				!probeDescendsFrom(c.LeftDepStage, sourceID, stages, byID) {
				return false
			}
		}
		return true
	}
	if !check(consumers[scanID]) {
		return false
	}
	if exchangeID != "" && !check(consumers[exchangeID]) {
		return false
	}
	return true
}

// probeDescendsFrom walks probe-preserving edges from stage id toward
// source: joins follow LeftDepStage (probe side); exchanges, filters, and
// aggregates follow their single structural dependency. Each such hop only
// drops or repeats probe rows — key values are never invented — so a hit
// means id's key set ⊆ source's output keys.
func probeDescendsFrom(id, source string, stages []Stage, byID map[string]int) bool {
	for hops := 0; hops < 16; hops++ {
		if id == source {
			return true
		}
		idx, ok := byID[id]
		if !ok {
			return false
		}
		s := &stages[idx]
		switch s.Type {
		case StageHashJoin, StageBroadcastJoin, StageSortMergeJoin:
			id = s.LeftDepStage
		case StageExchangeRepartition, StageExchangeReplicate, StageAggregate,
			"final_aggregate", "merge_aggregate", "filter":
			if len(s.Dependencies) == 0 {
				return false
			}
			id = s.Dependencies[0]
		default:
			return false
		}
	}
	return false
}

// countSemiAntiBuilds counts the LOGICAL semi/anti hash builds fed by the
// scan/exchange pair: consumer stages using it as their primary build side
// plus chained semi/anti joins (stage-chain fusion) whose BuildDepStage is
// the exchange or scan.
func countSemiAntiBuilds(scanID, exchangeID string, stages []Stage, consumers map[string][]int) int {
	n := 0
	seen := make(map[int]bool)
	for _, id := range []string{scanID, exchangeID} {
		if id == "" {
			continue
		}
		for _, ci := range consumers[id] {
			if seen[ci] {
				continue
			}
			seen[ci] = true
			c := &stages[ci]
			if c.Type == StageHashJoin && (c.JoinType == "semi" || c.JoinType == "anti") &&
				(c.RightDepStage == scanID || c.RightDepStage == exchangeID) {
				n++
			}
			for _, cj := range c.ChainedJoins {
				if (cj.JoinType == "semi" || cj.JoinType == "anti") &&
					(cj.BuildDepStage == scanID || cj.BuildDepStage == exchangeID) {
					n++
				}
			}
		}
	}
	return n
}

func hasConsumeFor(consumes []DynamicFilterConsume, sourceID, col string) bool {
	for _, c := range consumes {
		if c.SourceStageID == sourceID && c.TargetColumn == col {
			return true
		}
	}
	return false
}

func semiAntiBloomBits(estRows int64) int {
	rows := estRows
	if rows <= 0 {
		rows = semiAntiBloomDefaultRows
	}
	bits := rows * 10
	n := semiAntiBloomMinBits
	for int64(n) < bits && n < semiAntiBloomMaxBits {
		n <<= 1
	}
	return n
}
