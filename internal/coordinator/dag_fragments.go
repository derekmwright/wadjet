// This file holds aggregate, union, window, limit, projection, scan, and sort fragments.
// ADR-0010 governs shuffle transport; ADR-0026 §8 governs ordering across the gather boundary.
package coordinator

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// buildAggregateFragment translates a final_aggregate / merge_aggregate
// stage's task into a fragment Operators[] pipeline:
//
//	[OpShuffleSource, OpHashAggregate(MergeMode=true), OpFilter?(HAVING),
//	 OpSort?, OpUnpartitionedSink | OpGatherSink]
//
// FoldAvg is set only for "final_aggregate" — intermediate "merge_aggregate"
// tasks must keep __avg_sum#X / __avg_count#X synthetics intact for the
// downstream final to fold (see executor_stage.go for the same gate on the
// legacy path). Mirror behavior is byte-equivalent to the legacy
// executeStageAggregate path including the post-aggregate sort folded in
// by fuseSortIntoPredecessor.
//
// OpSort is appended only when the stage carries SortKeys (the planner's
// fuseSortIntoPredecessor pass set them by absorbing a downstream Singleton
// Sort). Stage.Limit propagates through as SortLimit so the Sort operator's
// post-Finalize Truncate fires for top-N. The multi-breaker runner chains
// HashAggregate's drain into Sort's consume in-process — no S3 hop between
// the two breakers.
//
// When gatherReplySubject is non-empty, the terminal sink is OpGatherSink
// streaming directly to the coordinator's NATS reply subscription instead
// of an unpartitioned .wshf upload — fuses the downstream gather stage
// into this fragment, eliminating one S3 PUT/GET hop and one coord round-
// trip. Each task publishes its own terminal marker; coord pre-subscribes
// with expectedTerminals = numTasks.
// inputRowBound is the exact upper bound on the rows this task will read
// (aggregateInputRowBound), or 0 when no exact bound exists. It rides the
// OpHashAggregate spec and decides the group-index layout on the worker.
func buildAggregateFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, aggs []distributed.AggSpec, sorts []distributed.SortKeySpec, gatherReplySubject string, inputRowBound int64) ([]distributed.OpSpec, error) {
	if len(taskInputs) != 1 {
		return nil, fmt.Errorf("aggregate fragment: expected 1 input alias, got %d", len(taskInputs))
	}
	var alias string
	var files []string
	for k, v := range taskInputs {
		alias = k
		files = v
		break
	}
	ops := make([]distributed.OpSpec, 0, 5)
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpShuffleSource,
		InputAlias:  alias,
		InputFiles:  files,
		InputBucket: t.DataBucket,
	})
	// MergeMode is true for stages that re-aggregate already-partial
	// outputs (final_aggregate, merge_aggregate). It's false for the
	// standalone "aggregate" stage, which consumes raw rows from upstream
	// non-scan input (e.g. a join output) and produces a partial — and for
	// a RawInputAggregate final, whose input is raw rows from an exchange
	// hash-partitioned on the group keys (rewireAggOverRawExchange):
	// partition-disjoint keys make single-level raw aggregation exact, so
	// the merge remaps (InputCol→OutputCol, COUNT→SUM) must not run. AVG
	// fold runs only on the final stage; partial / merge_aggregate
	// preserve __avg_sum#X / __avg_count#X synthetics for the downstream
	// final to fold. BuildProject is needed only on non-merge mode where
	// AggSpec.InputExpr might reference a derived expression that the
	// upstream hasn't pre-computed.
	mergeMode := (stage.Type == "final_aggregate" || stage.Type == "merge_aggregate") &&
		!stage.RawInputAggregate
	ops = append(ops, distributed.OpSpec{
		Type:        distributed.OpHashAggregate,
		GroupByCols: append([]string(nil), stage.GroupByCols...),
		// Only where this fragment COMPUTES the keys. A merge reads a
		// partial's output, where the key is already a column under its
		// published name — so the two names are one there by construction,
		// and #794's merge boundary needs no agreement to keep (ADR-0026 §2).
		GroupByResolve: mergeModeResolve(stage, mergeMode),
		GroupByTypes:   wireGroupByTypes(stage.GroupByTypes),
		GroupByDecimal: wireGroupByDecimal(stage.GroupByDecimal),
		Aggregates:     append([]distributed.AggSpec(nil), aggs...),
		GroupByAll:     stage.GroupByAll,
		MergeMode:      mergeMode,
		// GroupByAll (DISTINCT) has no derived-input expressions to project and
		// no AVG synthetics to fold — keep both off regardless of stage role.
		FoldAvg:      stage.Type == "final_aggregate" && !stage.GroupByAll,
		BuildProject: !mergeMode && !stage.GroupByAll,
		// An UNGROUPED final aggregate owes SQL one row over any input,
		// including none — see distributed.OpSpec.EmitEmptyIdentity. It is
		// the only aggregate in the DAG that does: a partial or
		// merge_aggregate that produces nothing is absorbed by the final
		// above it, and this stage is the one the planner distributes as a
		// Singleton, so the row it emits cannot be duplicated across tasks.
		EmitEmptyIdentity: stage.Type == "final_aggregate" &&
			len(stage.GroupByCols) == 0 && !stage.GroupByAll,
		InputRowBound: inputRowBound,
	})
	// INTERSECT/EXCEPT counting stage (#346): the aggregate's drain is one
	// row per distinct result row plus its per-arm counts; the emit operator
	// applies the operation's count rule and drops the count columns. It
	// runs BEFORE the post-filter and any fused sort, both of which name the
	// operation's OUTPUT columns.
	if stage.SetOp != "" {
		ops = append(ops, distributed.OpSpec{
			Type:          distributed.OpSetOpEmit,
			SetOp:         stage.SetOp,
			SetOpAll:      stage.SetOpAll,
			SetOpLeftCol:  physical.SetOpLeftCountCol,
			SetOpRightCol: physical.SetOpRightCountCol,
		})
	}
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	// The SELECT list a Project ABOVE the aggregate would have computed —
	// `SELECT g+1 AS gk, COUNT(*) AS n … GROUP BY g+1` names its group-key
	// output by the alias, and a computed one (`COUNT(*)+1 AS k`) exists
	// nowhere until something evaluates it. It runs AFTER the HAVING filter,
	// which names the aggregate's own outputs, and BEFORE a fused sort —
	// fuseSortIntoPredecessor refuses to fold into a predecessor whose
	// projection does not cover the sort's keys, so a key that reaches here
	// is one this OpProject still emits (#656 shape f, #681).
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
	if gatherReplySubject != "" {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		})
	} else {
		ops = append(ops, distributed.OpSpec{
			Type: distributed.OpUnpartitionedSink,
		})
	}
	return ops, nil
}

// buildUnionFragment translates one arm of a union stage into a fragment:
//
//	[OpShuffleSource(arm's whole output), OpProject(arm → result columns), <sink>]
//
// The projection is what makes the arms concatenable — it narrows each arm to
// the set operation's result columns and names them identically, so the N
// tasks emit one schema between them. A pass-through parquet scan arm reaches
// here carrying every column of its table; without the OpProject the union's
// output would be that arm's raw table width (#346).
func buildUnionFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, armIdx int, gatherReplySubject string) ([]distributed.OpSpec, error) {
	if armIdx < 0 || armIdx >= len(stage.UnionArms) {
		return nil, fmt.Errorf("union fragment: task %d has no arm (stage has %d)", armIdx, len(stage.UnionArms))
	}
	arm := stage.UnionArms[armIdx]
	// The arm's producer is Dependencies[armIdx] — one record, read here
	// rather than from a copy on the arm that a later pass could leave behind
	// (physical.UnionArm, #715).
	dep := stage.UnionArmDep(armIdx)
	files, ok := taskInputs[dep]
	if !ok {
		return nil, fmt.Errorf("union fragment: arm %d input %q missing from task inputs", armIdx, dep)
	}
	projectOp, ok := projectOpFromSpecs(arm.Projections)
	if !ok {
		return nil, fmt.Errorf("union fragment: arm %d carries no projection", armIdx)
	}
	ops := []distributed.OpSpec{
		{
			Type:        distributed.OpShuffleSource,
			InputAlias:  dep,
			InputFiles:  files,
			InputBucket: t.DataBucket,
		},
		projectOp,
	}
	// The arms' agreed DECIMAL(p,s), applied AFTER the projection because it
	// names the set operation's RESULT columns. Without it each arm's file
	// declares its own scale and the reader of both takes the first one's,
	// which is the same unscaled integer read as a different number (#533).
	if len(arm.DecimalCoercions) > 0 {
		cols := make([]distributed.ColumnSpec, 0, len(arm.DecimalCoercions))
		for _, c := range arm.DecimalCoercions {
			cols = append(cols, distributed.ColumnSpec{
				Name: c.Name, Type: int(parquet.TypeDecimal),
				Precision: c.Precision, Scale: c.Scale,
			})
		}
		ops = append(ops, distributed.OpSpec{Type: distributed.OpDecimalCoerce, Coercions: cols})
	}
	// A WHERE above the set operation lands on this stage (walkStages pushes
	// a Filter onto the stage it just emitted). It names the set operation's
	// OUTPUT columns, so it runs after the projection — and it has to run at
	// all: without this op the predicate is silently dropped and the query
	// returns the whole concatenation.
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	if gatherReplySubject != "" {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		})
	} else {
		ops = append(ops, distributed.OpSpec{Type: distributed.OpUnpartitionedSink})
	}
	return ops, nil
}

// buildWindowFragment translates a window stage's task into a fragment:
//
//	[OpShuffleSource, OpWindow, OpFilter?(a predicate above the window), <sink>]
//
// There is no OpSort ahead of the window. A window's ORDER BY defines its
// FRAME, not the stream: exec.Window groups its input by (PARTITION BY,
// ORDER BY) and sorts each group itself, exactly as the single-process
// pipeline's windowSourceAdapter does. Sorting the fragment's input would be
// redundant work, and for a stage carrying two OVER clauses with different
// ORDER BYs it could not serve both anyway.
//
// Every spec field is resolved at plan time (physical.WindowColSpec); this is
// a translation, not a decision — see distributed.WindowColSpec.OutputType.
//
// gatherReplySubject is accepted for symmetry with the other fragment
// builders and is empty today: canFuseGather fuses only aggregate and sort
// deps, so a window stage always writes its output and lets a separate
// gather stage read it.
func buildWindowFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, gatherReplySubject string) ([]distributed.OpSpec, error) {
	if len(taskInputs) != 1 {
		return nil, fmt.Errorf("window fragment: expected 1 input alias, got %d", len(taskInputs))
	}
	if len(stage.WindowCols) == 0 {
		return nil, fmt.Errorf("window fragment: stage carries no window columns")
	}
	var alias string
	var files []string
	for k, v := range taskInputs {
		alias = k
		files = v
		break
	}
	winCols := make([]distributed.WindowColSpec, len(stage.WindowCols))
	for i, wc := range stage.WindowCols {
		var orderBy []distributed.SortKeySpec
		for _, ob := range wc.OrderBy {
			orderBy = append(orderBy, distributed.SortKeySpec{
				Column: ob.Column,
				Desc:   ob.Desc,
				// The POSITION the planner decided, where the producer emits
				// the key's name twice and a name says nothing (#968). The
				// worker already rebuilds an `exec.SortKey` from it; without
				// this line the DAG's window bound by name while the
				// single-process one bound by slot.
				SlotPos:   ob.SlotPos,
				NullsLast: distributed.NullsLastPtr(ob.NullsLast),
			})
		}
		winCols[i] = distributed.WindowColSpec{
			Func:           wc.Func,
			InputCol:       wc.InputCol,
			OutputCol:      wc.OutputCol,
			OutputType:     distributed.WindowTypePtr(int(wc.OutputType)),
			PartitionBy:    append([]string(nil), wc.PartitionBy...),
			OrderBy:        orderBy,
			LagLeadOffset:  wc.LagLeadOffset,
			LagLeadDefault: wc.LagLeadDefault,
			NtileBuckets:   wc.NtileBuckets,
			NthValueN:      wc.NthValueN,
		}
		if wc.Frame != nil {
			winCols[i].Frame = &distributed.WindowFrameSpec{
				Mode:  wc.Frame.Mode,
				Start: distributed.WindowBoundSpec{Type: wc.Frame.Start.Type, Offset: wc.Frame.Start.Offset},
				End:   distributed.WindowBoundSpec{Type: wc.Frame.End.Type, Offset: wc.Frame.End.Offset},
			}
		}
	}
	windowOp := distributed.OpSpec{
		Type:       distributed.OpWindow,
		WindowCols: winCols,
	}
	// The expression keys the fragment has to compute before it can
	// partition on them (#585). projectOpFromSpecs builds the same
	// ProjectSpec list a Stage.ProjectExprs OpProject gets; only the
	// destination differs, because a window's projection APPENDS to the
	// batch rather than narrowing it — the window's output is every input
	// column plus its own.
	if op, ok := projectOpFromSpecs(stage.WindowKeyExprs); ok {
		windowOp.WindowKeyExprs = op.Projections
	}
	ops := []distributed.OpSpec{
		{
			Type:        distributed.OpShuffleSource,
			InputAlias:  alias,
			InputFiles:  files,
			InputBucket: t.DataBucket,
		},
		windowOp,
	}
	// A predicate walkStages pushed onto this stage names the window's
	// OUTPUT columns, so it runs after the operator — and it has to run at
	// all: an unemitted filter is a silently unfiltered answer.
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	// The SELECT list a Project ABOVE the window would have computed. Unlike
	// WindowKeyExprs — which the window operator evaluates BEFORE it
	// partitions, and which APPENDS to the batch — this one narrows the
	// window's output to the projection, so it is an ordinary OpProject
	// placed after the operator. Without it a `SELECT id, UPPER(s) FROM
	// (… ROW_NUMBER() OVER … ) x` came back as the window's raw input plus
	// the window column (#656 shape g).
	if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
		ops = append(ops, op)
	}
	if gatherReplySubject != "" {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		})
	} else {
		ops = append(ops, distributed.OpSpec{Type: distributed.OpUnpartitionedSink})
	}
	return ops, nil
}

// buildLimitFragment translates a limit stage's task into a fragment:
//
//	[OpShuffleSource, OpLimit, OpFilter?(a predicate above the LIMIT), <sink>]
//
// One task, reading every partition of its input, because that is what makes
// the bound GLOBAL — the whole point of the stage (physical.StageLimit,
// #478). There is no per-task split to make here and no ordering decision to
// take: a LIMIT under an ORDER BY reaches this stage only for its OFFSET,
// with the sort stage below already having produced the ordered prefix.
func buildLimitFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, gatherReplySubject string) ([]distributed.OpSpec, error) {
	if len(taskInputs) != 1 {
		return nil, fmt.Errorf("limit fragment: expected 1 input alias, got %d", len(taskInputs))
	}
	if !stage.HasLimit && stage.Offset <= 0 {
		return nil, fmt.Errorf("limit fragment: stage %s carries neither a LIMIT nor an OFFSET", stage.ID)
	}
	var alias string
	var files []string
	for k, v := range taskInputs {
		alias, files = k, v
		break
	}
	ops := []distributed.OpSpec{
		{
			Type:        distributed.OpShuffleSource,
			InputAlias:  alias,
			InputFiles:  files,
			InputBucket: t.DataBucket,
		},
		{
			Type:          distributed.OpLimit,
			LimitCount:    stage.Limit,
			HasLimitCount: stage.HasLimit,
			LimitOffset:   stage.Offset,
		},
	}
	// A predicate walkStages pushed onto this stage names the LIMIT's output
	// columns, so it runs after the operator — and it has to run at all: an
	// unemitted filter is a silently unfiltered answer.
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	// …and the SELECT list a Project above the LIMIT would have computed.
	if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
		ops = append(ops, op)
	}
	if gatherReplySubject != "" {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		})
	} else {
		ops = append(ops, distributed.OpSpec{Type: distributed.OpUnpartitionedSink})
	}
	return ops, nil
}

// buildProjectFragment translates a project stage's task into a fragment:
//
//	[OpShuffleSource, OpFilter?, OpProject?, <sink>]
//
// The stage exists for the shapes where a Project or a Filter has nowhere
// else to go: a predicate above a projection that was itself materialized
// onto the producing fragment (so the producer's own filter slot runs
// UNDERNEATH it), a predicate above a deduped `cte-alias` whose target is
// shared with another reference of the same CTE, and a predicate above a CTE
// body's terminal that other references also read (#656).
//
// Filter BEFORE project, the scan fragment's order: the predicate is written
// against this stage's INPUT — the producer's output columns — and a SELECT
// list attached here is written over that same input and must not narrow it
// away before the filter has run. `WITH c AS (… GROUP BY g+1) SELECT gk*10 AS
// gk10 FROM c WHERE gk > 3` needs exactly that order.
//
// One task, reading every partition of its input: a projection and a filter
// are per-row, so any partitioning would be exact, and Singleton is the
// simple answer for a stage the planner emits only where nothing ran at all
// before.
func buildProjectFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, gatherReplySubject string) ([]distributed.OpSpec, error) {
	if len(taskInputs) != 1 {
		return nil, fmt.Errorf("project fragment: expected 1 input alias, got %d", len(taskInputs))
	}
	if len(stage.ProjectExprs) == 0 && len(t.PostFilterExprs) == 0 {
		return nil, fmt.Errorf("project fragment: stage %s carries neither a projection nor a filter", stage.ID)
	}
	var alias string
	var files []string
	for k, v := range taskInputs {
		alias, files = k, v
		break
	}
	ops := []distributed.OpSpec{
		{
			Type:        distributed.OpShuffleSource,
			InputAlias:  alias,
			InputFiles:  files,
			InputBucket: t.DataBucket,
		},
	}
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
		ops = append(ops, op)
	}
	if gatherReplySubject != "" {
		ops = append(ops, distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		})
	} else {
		ops = append(ops, distributed.OpSpec{Type: distributed.OpUnpartitionedSink})
	}
	return ops, nil
}

// projectOpFromSpecs converts a stage's projection spec list into an
// OpProject OpSpec. ok=false when the list is empty.
func projectOpFromSpecs(specs []physical.ProjectExprSpec) (distributed.OpSpec, bool) {
	if len(specs) == 0 {
		return distributed.OpSpec{}, false
	}
	projections := make([]distributed.ProjectSpec, len(specs))
	for i, p := range specs {
		projections[i] = distributed.ProjectSpec{Expr: p.Expr, Name: p.Name}
		// TypeKnown || Type != 0 mirrors wireAggSpecs' "declared" check
		// (agg_wire.go): a nonzero Type reaches the wire exactly as before
		// even from a caller that never learned about TypeKnown, and a
		// genuinely BOOL Type (TypeKnown, zero value) now reaches it too
		// instead of being silently dropped (#445).
		if p.TypeKnown || p.Type != 0 {
			projections[i].Type = distributed.WindowTypePtr(int(p.Type))
		}
		// A DECIMAL's (p,s) travels with its TypeID or the worker's output
		// vector comes out at scale 0 (ADR-0024 item 2).
		projections[i].Precision, projections[i].Scale = p.Precision, p.Scale
		// The SLOT, where the producer publishes this name twice: a name is
		// not a handle when two columns answer to it (ADR-0026 section 3a).
		if p.SourceSlotSet {
			slot := p.SourceSlot
			projections[i].SourceIdx = &slot
		}
	}
	return distributed.OpSpec{Type: distributed.OpProject, Projections: projections}, true
}

// buildScanAggregateFragment translates a fused scan + partial-aggregate
// stage's task into a fragment Operators[] pipeline:
//
//	[OpScan, OpFilter?(scan-pushed WHERE), OpHashAggregate(partial, BuildProject), terminalSink]
//
// terminalSink is the caller-supplied sink — OpExchangeSender for the
// fuseScanAggregateShuffle case (each task hash-partitions its K aggregate
// rows by group key directly, skipping the standalone exchange-repartition
// stage), OpUnpartitionedSink otherwise (one .wshf per task; downstream
// Singleton final_aggregate reads them all). Output shape under the
// unpartitioned terminal is byte-equivalent to the legacy
// executeStageAggregate path.
//
// BuildProject=true asks the worker's buildAggInputProjection to construct a
// derived-input Project for AggSpecs whose InputCol references an expression
// (e.g. SUM(l_extendedprice * (1 - l_discount))). The worker prepends the
// project to the unary chain ahead of HashAggregate's consume phase.
//
// ScanShardIndex/Count propagate single-file row-group sharding for fused
// scan-aggregate over a single compacted parquet file (e.g. SF10 lineitem
// at 5GB).
func buildScanAggregateFragment(stage physical.Stage, t *distributed.Task, files []string, aggs []distributed.AggSpec, shardIdx, shardCount int, terminalSink distributed.OpSpec) ([]distributed.OpSpec, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("scan-aggregate fragment: empty file list")
	}
	if len(stage.FusedAggGroupBy) == 0 && len(aggs) == 0 {
		return nil, fmt.Errorf("scan-aggregate fragment: at least one of FusedAggGroupBy or AggSpecs required")
	}
	if terminalSink.Type == "" {
		return nil, fmt.Errorf("scan-aggregate fragment: terminalSink.Type required")
	}
	scanOp := distributed.OpSpec{
		Type:        distributed.OpScan,
		InputAlias:  scanAliasForStage(stage),
		InputFiles:  append([]string(nil), files...),
		InputBucket: t.DataBucket,
		Columns:     append([]string(nil), stage.Columns...),
		// Same declaration as the plain scan fragment's: the fused partial
		// aggregate reads the base table too, so a MIN/MAX or a group key
		// over one of the nine inexpressible types needs the catalog's
		// type here as well (#423).
		ColumnTypes: wireColumnSpecs(stage.ScanSchema),
	}
	if shardCount > 1 {
		scanOp.ScanShardIndex = shardIdx
		scanOp.ScanShardCount = shardCount
	}
	ops := make([]distributed.OpSpec, 0, 5)
	ops = append(ops, scanOp)
	if len(stage.FilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), stage.FilterExprs...),
		})
	}
	// ABAC security barrier runs BEFORE the partial aggregate so grouping
	// and aggregation see masked values and never see denied columns — and
	// the user's own predicates run above it, for the reason the scan
	// fragment's copy of this records (#859 round 2).
	if op, ok := projectOpFromSpecs(stage.SecurityProjectExprs); ok {
		ops = append(ops, op)
		if len(stage.PostSecurityFilterExprs) > 0 {
			ops = append(ops, distributed.OpSpec{
				Type:       distributed.OpFilter,
				Predicates: append([]string(nil), stage.PostSecurityFilterExprs...),
			})
		}
	}
	ops = append(ops, distributed.OpSpec{
		Type:           distributed.OpHashAggregate,
		GroupByCols:    append([]string(nil), stage.FusedAggGroupBy...),
		GroupByResolve: wireGroupKeyResolve(stage.GroupByResolve),
		GroupByTypes:   wireGroupByTypes(stage.GroupByTypes),
		GroupByDecimal: wireGroupByDecimal(stage.GroupByDecimal),
		Aggregates:     append([]distributed.AggSpec(nil), aggs...),
		MergeMode:      false,
		BuildProject:   true,
	})
	ops = append(ops, terminalSink)
	return ops, nil
}

// buildSortFragment translates a sort / merge_sort stage's task into a
// fragment Operators[] pipeline:
//
//	[OpShuffleSource, OpSort, OpFilter?, OpProject?, OpUnpartitionedSink | OpGatherSink]
//
// The filter and the projection run ABOVE the sort, which is what makes them
// correct for the shapes that put them there: a WHERE above an `ORDER BY …
// LIMIT` inside a CTE or derived table must see the LIMIT's rows, not the
// pre-limit ones, and OpSort applies its SortLimit truncation in Finalize —
// before any of its output reaches the next operator. Until #656 this builder
// was the only one that dropped `t.PostFilterExprs` on the floor, so the
// predicate walkStages had attached to the stage was never evaluated and the
// query answered as if the WHERE were not there.
//
// Same one-output-file-per-task shape the legacy executeStageSort emitted
// when the terminal sink is OpUnpartitionedSink; downstream consumers
// (gather, further merges) read it identically. Limit is forwarded as
// SortLimit so the sort operator's Truncate fires after Finalize for
// top-N optimization.
//
// When gatherReplySubject is non-empty, the terminal sink is OpGatherSink
// streaming the (single, ordered) sorted output directly to the
// coordinator's NATS reply subscription instead of an unpartitioned .wshf
// upload — fuses the downstream gather stage into this fragment,
// eliminating one S3 PUT/GET hop and one coord round-trip. Only safe
// when the upstream stage is DistSingleton (one task → one ordered
// stream); canFuseGather enforces that.
func buildSortFragment(stage physical.Stage, t *distributed.Task, taskInputs map[string][]string, sorts []distributed.SortKeySpec, gatherReplySubject string) ([]distributed.OpSpec, error) {
	if len(taskInputs) != 1 {
		return nil, fmt.Errorf("sort fragment: expected 1 input alias, got %d", len(taskInputs))
	}
	if len(sorts) == 0 {
		return nil, fmt.Errorf("sort fragment: SortKeys required")
	}
	var alias string
	var files []string
	for k, v := range taskInputs {
		alias = k
		files = v
		break
	}
	terminal := distributed.OpSpec{Type: distributed.OpUnpartitionedSink}
	if gatherReplySubject != "" {
		terminal = distributed.OpSpec{
			Type:         distributed.OpGatherSink,
			ReplySubject: gatherReplySubject,
		}
	}
	ops := []distributed.OpSpec{
		{
			Type:        distributed.OpShuffleSource,
			InputAlias:  alias,
			InputFiles:  files,
			InputBucket: t.DataBucket,
		},
		{
			Type:         distributed.OpSort,
			SortKeySpecs: append([]distributed.SortKeySpec(nil), sorts...),
			SortLimit:    stage.Limit,
			HasSortLimit: stage.HasLimit,
		},
	}
	// A predicate walkStages pushed onto this stage names the sort's OUTPUT
	// columns and must run above its LIMIT (#656 shapes a–d).
	if len(t.PostFilterExprs) > 0 {
		ops = append(ops, distributed.OpSpec{
			Type:       distributed.OpFilter,
			Predicates: append([]string(nil), t.PostFilterExprs...),
		})
	}
	// …and the SELECT list a Project above the sort would have computed.
	if op, ok := projectOpFromSpecs(stage.ProjectExprs); ok {
		ops = append(ops, op)
	}
	ops = append(ops, terminal)
	return ops, nil
}
