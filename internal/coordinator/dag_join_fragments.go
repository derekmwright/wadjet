// This file holds hash-join and sort-merge-join fragment construction.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
)

// buildJoinFragment translates a hash_join / broadcast_join stage's task into
// a fragment pipeline (Operators[]). Pipeline shape:
//
//	[ShuffleSource(probe), BroadcastProbe(fused_0..n), HashJoinProbe|BroadcastProbe(primary),
//	 OpFilter?(residual), chained_0..n (stage-chain fusion), OpSort?(folded sort), <terminalSink>]
//
// Each FusedJoin in wireFused becomes its own BroadcastProbe op (the existing
// chain semantics: every fused entry is a broadcast cache the probe stream
// passes through before reaching the primary). chainedOps are pre-built
// post-primary probe/filter ops from absorbed 1:1 downstream joins
// (docs/design/stage-chain-fusion.md); they run after the primary and its
// residual filter — the order the separate stages executed in.
//
// terminalSink is the caller-supplied sink — OpExchangeSender for the
// fuseJoinShuffle path, OpUnpartitionedSink for the standalone-terminal-join
// path. sorts is non-empty when fuseSortIntoPredecessor folded a downstream
// Singleton sort into this join (post-join sort runs in-process via the
// multi-breaker runner).
func buildJoinFragment(
	stage physical.Stage,
	t *distributed.Task,
	taskInputs map[string][]string,
	wireFused []distributed.FusedJoinSpec,
	chainedOps []distributed.OpSpec,
	sorts []distributed.SortKeySpec,
	terminalSink distributed.OpSpec,
	lateMat bool,
) ([]distributed.OpSpec, error) {
	if t.BuildTableAlias == "" {
		return nil, fmt.Errorf("BuildTableAlias required")
	}
	buildFiles, ok := taskInputs[t.BuildTableAlias]
	if !ok {
		return nil, fmt.Errorf("build alias %q missing from inputs", t.BuildTableAlias)
	}
	var probeAlias string
	var probeFiles []string
	for k, v := range taskInputs {
		if k != t.BuildTableAlias {
			probeAlias = k
			probeFiles = v
			break
		}
	}
	if probeAlias == "" {
		return nil, fmt.Errorf("no probe-side alias (only %q)", t.BuildTableAlias)
	}

	ops := make([]distributed.OpSpec, 0, 2+len(wireFused)+3)
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpShuffleSource,
		InputAlias:  probeAlias,
		InputFiles:  probeFiles,
		InputBucket: t.DataBucket,
	})
	for _, fj := range wireFused {
		ops = append(ops, distributed.OpSpec{
			Type:            distributed.OpBroadcastProbe,
			JoinType:        fj.JoinType,
			LeftKeys:        fj.JoinLeftKeys,
			RightKeys:       fj.JoinRightKeys,
			KeyTypes:        fj.JoinKeyTypes,
			BuildAlias:      fj.BuildTableAlias,
			BuildColOrigins: fj.BuildColOrigins,
			BuildFiles:      fj.BuildFiles,
			BuildBucket:     t.DataBucket,
			JoinFilter:      fj.JoinFilter,
			LateMaterialize: lateMat,
			BuildSchema:     append([]distributed.ColumnSpec(nil), fj.BuildSchema...),
		})
		// The absorbed stage's own predicates, applied where that stage
		// would have applied them: immediately after its probe. The planner
		// carries them on the fused spec and the wire format has the field,
		// but this loop used to copy every field EXCEPT FilterExprs — so
		// fusing a filter-carrying join silently discarded its WHERE clause.
		// `WHERE c_nationkey = s_nationkey` over TPC-H Q05's join survived
		// while its stage stood alone and vanished the moment a fifth table
		// made that stage fusable: COUNT(*) 2450 -> 60000 (exactly the
		// unfiltered count) and Q05 revenues ~25x inflated (#312).
		if len(fj.FilterExprs) > 0 {
			ops = append(ops, distributed.OpSpec{
				Type:       distributed.OpFilter,
				Predicates: append([]string(nil), fj.FilterExprs...),
			})
		}
	}
	primaryType := distributed.OpHashJoinProbe
	if stage.Type == physical.StageBroadcastJoin {
		primaryType = distributed.OpBroadcastProbe
	}
	ops = append(ops, distributed.OpSpec{
		Type:                primaryType,
		JoinType:            t.JoinType,
		LeftKeys:            t.JoinLeftKeys,
		RightKeys:           t.JoinRightKeys,
		KeyTypes:            t.JoinKeyTypes,
		BuildAlias:          t.BuildTableAlias,
		BuildFiles:          buildFiles,
		BuildBucket:         t.DataBucket,
		JoinFilter:          t.JoinFilter,
		NullAwareAnti:       t.NullAwareAnti,
		BuildRowHint:        t.BuildRowHint,
		SemiAntiKeyOnly:     t.SemiAntiKeyOnly,
		BuildFilterExprs:    append([]string(nil), t.BuildFilterExprs...),
		QualifyAllBuildCols: t.QualifyAllBuildCols,
		BuildColOrigins:     t.BuildColOrigins,
		OutputColumns:       append([]string(nil), t.Columns...),
		HiddenColumns:       append([]distributed.HiddenJoinColumn(nil), t.HiddenJoinColumns...),
		EmptyDefaults:       append([]distributed.LateralEmptyDefault(nil), t.LateralEmptyDefaults...),
		PadMarker:           t.LateralPadMarker,
		DropMarker:          t.LateralDropMarker,
		LateMaterialize:     lateMat,
		BuildSchema:         append([]distributed.ColumnSpec(nil), t.JoinBuildSchema...),
		ProbeSchema:         append([]distributed.ColumnSpec(nil), t.JoinProbeSchema...),
	})
	// Residual post-join filters (semi/anti residual or compute-stage
	// HAVING-equivalent) compile to an OpFilter applied AFTER the probe and
	// BEFORE any sort/sink. Mirrors the legacy applyPostFilter step.
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	// Absorbed 1:1 downstream joins (stage-chain fusion) run after the
	// primary and its residual filter, in stage order.
	ops = append(ops, chainedOps...)
	// SELECT-list projection for a gather-terminal join (#288 finding, the
	// #169 class on the join path): computes scalar expressions worker-side.
	// Before the sort — a fused sort may key on a projected alias.
	if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
		ops = append(ops, op)
	}
	if len(sorts) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpSort,
			SortKeySpecs: append([]distributed.SortKeySpec(nil), sorts...),
			SortLimit:    stage.Limit,
			HasSortLimit: stage.HasLimit,
		})
	}
	ops = append(ops, terminalSink)
	return ops, nil
}

// buildSortMergeJoinFragment translates a sort_merge_join stage's task into
// a fragment pipeline:
//
//	[ShuffleSource(probe), OpSortMergeJoin(breaker; build from BuildFiles),
//	 OpFilter?(residual), OpSort?(folded sort), OpUnpartitionedSink]
//
// The probe side streams from this task's partition of the left exchange;
// the build side's partition files ride the op spec (BuildFiles) and are
// drained by the worker's breaker construction — the same co-partitioned
// input shape as buildJoinFragment, with the operator swapped. Fused
// broadcast chains never attach to sort_merge_join stages (every fusion
// pass gates on hash_join/broadcast_join), so wireFused must be empty.
func buildSortMergeJoinFragment(
	stage physical.Stage,
	t *distributed.Task,
	taskInputs map[string][]string,
	wireFused []distributed.FusedJoinSpec,
	sorts []distributed.SortKeySpec,
) ([]distributed.OpSpec, error) {
	if len(wireFused) > 0 {
		return nil, fmt.Errorf("sort_merge_join stage carries %d fused joins; fusion passes must not target it", len(wireFused))
	}
	if t.BuildTableAlias == "" {
		return nil, fmt.Errorf("BuildTableAlias required")
	}
	buildFiles, ok := taskInputs[t.BuildTableAlias]
	if !ok {
		return nil, fmt.Errorf("build alias %q missing from inputs", t.BuildTableAlias)
	}
	var probeAlias string
	var probeFiles []string
	for k, v := range taskInputs {
		if k != t.BuildTableAlias {
			probeAlias = k
			probeFiles = v
			break
		}
	}
	if probeAlias == "" {
		return nil, fmt.Errorf("no probe-side alias (only %q)", t.BuildTableAlias)
	}

	ops := make([]distributed.OpSpec, 0, 5)
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpShuffleSource,
		InputAlias:  probeAlias,
		InputFiles:  probeFiles,
		InputBucket: t.DataBucket,
	})
	ops = append(ops, distributed.OpSpec{
		Type:                distributed.OpSortMergeJoin,
		JoinType:            t.JoinType,
		HiddenColumns:       append([]distributed.HiddenJoinColumn(nil), t.HiddenJoinColumns...),
		EmptyDefaults:       append([]distributed.LateralEmptyDefault(nil), t.LateralEmptyDefaults...),
		PadMarker:           t.LateralPadMarker,
		DropMarker:          t.LateralDropMarker,
		LeftKeys:            t.JoinLeftKeys,
		RightKeys:           t.JoinRightKeys,
		KeyTypes:            t.JoinKeyTypes,
		BuildAlias:          t.BuildTableAlias,
		BuildFiles:          buildFiles,
		BuildBucket:         t.DataBucket,
		QualifyAllBuildCols: t.QualifyAllBuildCols,
		BuildColOrigins:     t.BuildColOrigins,
		OutputColumns:       append([]string(nil), t.Columns...),
	})
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	// SELECT-list projection for a gather-terminal join — same insertion
	// as buildJoinFragment (before the fused sort).
	if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
		ops = append(ops, op)
	}
	if len(sorts) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpSort,
			SortKeySpecs: append([]distributed.SortKeySpec(nil), sorts...),
			SortLimit:    stage.Limit,
			HasSortLimit: stage.HasLimit,
		})
	}
	ops = append(ops, distributed.OpSpec{Type: distributed.OpUnpartitionedSink})
	return ops, nil
}
