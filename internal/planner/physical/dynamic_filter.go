package physical

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Dynamic-filter planner pass. Identifies hash_join stages where the build
// side is selective enough to be worth shipping a bloom of its join keys
// to the probe-side leaf scan, and rewrites the Stage DAG to:
//
//   1. Annotate the build-side leaf scan with EmitDynamicFilters so each
//      task computes a partial KeyRange+Bloom during the scan.
//   2. Annotate the probe-side leaf scan with ConsumeDynamicFilters and
//      append the build-scan stage ID to its Dependencies (the stat-dep
//      edge) — the stage DAG executor serializes them via the existing
//      dependency mechanism, so no new edge type or async broker is needed.
//
// SF100+ scalability:
//   - Eligibility threshold caps build cardinality at dynamicFilterMaxBuildRows
//     so the bloom is bounded (10 bits/key → ≤ 12.5 MB at the ceiling).
//   - Only single-column integer join keys participate in v1.
//   - The bloom RIDES the existing per-task result-upload (sideband S3
//     artifact) — no NATS-message bloat from large filters.

// dynamicFilterMaxBuildRows caps build cardinality for eligibility. Joins
// with estimated build rows above this are skipped — the bloom is too
// large to be a clear win and CBO-driven cost analysis (Tier-1 roadmap
// item #1) is the real fix for that case.
const dynamicFilterMaxBuildRows int64 = 10_000_000

// dynamicFilterBitsPerKey controls bloom sizing: 10 bits/key for ~1% FPR.
const dynamicFilterBitsPerKey = 10

// dynamicFilterMinBloomBits is the floor for very-small builds.
const dynamicFilterMinBloomBits = 1024

// applyDynamicFilters walks stages and adds Emit/Consume annotations + the
// stat-dep edge for every eligible hash_join. Returns the (possibly
// mutated) stages slice. Pure local mutation; no error path because all
// failures are degraded silently (the join simply doesn't get a dynamic
// filter, which is always safe).
//
// Safe to call multiple times — already-annotated stages are detected by
// the presence of EmitDynamicFilters/ConsumeDynamicFilters and skipped.
func (p *Planner) applyDynamicFilters(ctx context.Context, stages []Stage) []Stage {
	if !p.DynamicFiltersEnabled {
		return stages
	}
	// Build an index for O(1) stage lookup by ID.
	byID := make(map[string]int, len(stages))
	for i := range stages {
		byID[stages[i].ID] = i
	}
	// Track FilterID assignments to keep them stable within a query.
	var filterCounter int

	for joinIdx := range stages {
		j := &stages[joinIdx]
		if j.Type != StageHashJoin {
			continue
		}
		// v1: single-column int join keys only. Multi-column composite
		// keys come in a follow-up — the emit op already has scaffolding
		// for them but the row-group prune helpers assume one column.
		if len(j.JoinLeftKeys) != 1 || len(j.JoinRightKeys) != 1 {
			continue
		}
		// Semi/anti joins where the build is the small side benefit most.
		// Inner is fine too. Outer joins are skipped — pruning the OUTER
		// side would drop rows that should match-or-NULL through.
		switch j.JoinType {
		case "inner", "semi", "left":
			// "left" only safe when the OUTER side is the LEFT (probe);
			// our HashJoin probes left, builds right, so left outer means
			// build (right) feeds the inner side — eligible. Same logic
			// as the bloom-filter wiring in the in-process planner.
		default:
			continue
		}
		buildLeafIdx := findLeafScanStage(j.RightDepStage, stages, byID)
		probeLeafIdx := findLeafScanStage(j.LeftDepStage, stages, byID)
		if buildLeafIdx < 0 || probeLeafIdx < 0 {
			continue
		}
		buildLeaf := &stages[buildLeafIdx]
		probeLeaf := &stages[probeLeafIdx]

		// Eligibility: build leaf must produce ≤ threshold rows post-filter.
		// EstimatedRows on the leaf reflects post-pushed-filter cardinality
		// when the optimizer can compute it.
		buildRows := buildLeaf.EstimatedRows
		if buildRows <= 0 || buildRows > dynamicFilterMaxBuildRows {
			continue
		}

		// Resolve column types via catalog. Both sides must be integer.
		buildKeyType, ok := p.columnIntType(ctx, buildLeaf.TableName, j.JoinRightKeys[0])
		if !ok {
			continue
		}
		probeKeyType, ok := p.columnIntType(ctx, probeLeaf.TableName, j.JoinLeftKeys[0])
		if !ok {
			continue
		}
		// Types must match to keep hash semantics consistent — the emit
		// op uses the build key's type for hashing; the probe scan applies
		// the same hash on its column. Casting across int32↔int64 would
		// produce different hashes for the same logical value.
		if buildKeyType != probeKeyType {
			continue
		}

		filterCounter++
		filterID := fmt.Sprintf("df-%s-%d", j.ID, filterCounter)
		bloomBits := computeDynamicFilterBloomBits(buildRows)

		// Idempotence: if this build leaf already emits a filter targeted
		// at the same key, skip rather than double-annotate.
		if hasEmitForColumn(buildLeaf.EmitDynamicFilters, j.JoinRightKeys[0]) {
			continue
		}
		buildLeaf.EmitDynamicFilters = append(buildLeaf.EmitDynamicFilters, DynamicFilterEmit{
			FilterID:  filterID,
			KeyColumn: j.JoinRightKeys[0],
			KeyType:   buildKeyType,
			BloomBits: bloomBits,
		})
		probeLeaf.ConsumeDynamicFilters = append(probeLeaf.ConsumeDynamicFilters, DynamicFilterConsume{
			FilterID:      filterID,
			SourceStageID: buildLeaf.ID,
			TargetColumn:  j.JoinLeftKeys[0],
			KeyType:       buildKeyType,
		})
		// Stat-dep edge — append build leaf to probe leaf's dependencies
		// so execute_stage_dag.go serializes them via the existing
		// dependency-wait mechanism. Idempotent append: skip if already there.
		if !containsString(probeLeaf.Dependencies, buildLeaf.ID) {
			probeLeaf.Dependencies = append(probeLeaf.Dependencies, buildLeaf.ID)
		}
	}
	return stages
}

// findLeafScanStage walks back from depID through exchange-repartition /
// exchange-replicate / filter stages to reach a leaf scan. Returns -1 if
// the walk hits an unsupported shape (chained joins, aggregates, etc.) —
// those break the simple "probe-scan reads same column as join probes"
// invariant and a smarter walk would be needed.
func findLeafScanStage(depID string, stages []Stage, byID map[string]int) int {
	cur := depID
	for hops := 0; hops < 8; hops++ {
		idx, ok := byID[cur]
		if !ok {
			return -1
		}
		s := stages[idx]
		if s.Type == StageScan {
			return idx
		}
		if s.Type != StageExchangeRepartition && s.Type != StageExchangeReplicate {
			// Anything else between the join and the leaf (aggregate, sub-
			// join, sort) means the probe-side column may differ from the
			// scan column — bail out conservatively.
			return -1
		}
		if len(s.Dependencies) != 1 {
			return -1
		}
		cur = s.Dependencies[0]
	}
	return -1
}

func hasEmitForColumn(emits []DynamicFilterEmit, col string) bool {
	for _, e := range emits {
		if e.KeyColumn == col {
			return true
		}
	}
	return false
}

func containsString(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}

// computeDynamicFilterBloomBits picks a bloom size to give ~1% FPR for
// `rows` keys at 10 bits/key, rounded up to a power of two and floored at
// the minimum. Capped by the inline-vs-stage thresholds applied later in
// the coordinator.
func computeDynamicFilterBloomBits(rows int64) int {
	bits := rows * dynamicFilterBitsPerKey
	n := 1
	for int64(n) < bits {
		n <<= 1
	}
	if n < dynamicFilterMinBloomBits {
		n = dynamicFilterMinBloomBits
	}
	return n
}

// columnIntType resolves the integer-shaped type ("int32" | "int64" | "date")
// of a column via the catalog. Returns ok=false for non-integer columns or
// catalog lookup failures — caller skips the filter rather than emitting
// a malformed one.
func (p *Planner) columnIntType(ctx context.Context, table, column string) (string, bool) {
	if p == nil || p.catalog == nil || table == "" || column == "" {
		return "", false
	}
	meta, err := p.catalog.GetTable(ctx, table)
	if err != nil || meta == nil {
		return "", false
	}
	idx := meta.Schema.ColumnIndex(column)
	if idx < 0 {
		return "", false
	}
	switch meta.Schema.Columns[idx].Type {
	case parquet.TypeInt32:
		return "int32", true
	case parquet.TypeInt64:
		return "int64", true
	case parquet.TypeDate:
		return "date", true
	}
	return "", false
}
