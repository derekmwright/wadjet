package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/planner/physical"
)

// executeFragment runs a multi-operator pipeline described by task.Operators[]
// end-to-end on this worker, without intermediate S3 round-trips between
// operators. This is the long-term shape that dissolves the
// per-operator-per-task floor cost (S3 PUT + NATS dispatch + per-task setup).
//
// Pipeline shape: Operators[0] is the source, Operators[len-1] is the sink,
// and the operators in between are unary transforms applied per batch.
//
// Common shapes the planner emits:
//
//	[Scan, Filter?, ExchangeSender]                  — replaces scan + shuffle stages
//	[ShuffleSource, HashJoinProbe, ExchangeSender]   — replaces shuffle-recv + join stages
//	[ShuffleSource, HashAggregate, GatherSink]       — final-aggregate + gather
//
// The legacy executeStageScan/HashJoin/Aggregate/Sort handlers remain for
// tasks the planner hasn't migrated yet; they're equivalent to a single-op
// fragment but go through the per-StageType code path.
func (e *Executor) executeFragment(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	if len(task.Operators) == 0 {
		return fmt.Errorf("fragment task %s: empty Operators", task.ID)
	}
	specs := task.Operators
	sourceSpec := specs[0]
	sinkSpec := specs[len(specs)-1]
	middle := specs[1 : len(specs)-1]

	if !isFragmentSourceOp(sourceSpec.Type) {
		return fmt.Errorf("fragment task %s: first op %q is not a source", task.ID, sourceSpec.Type)
	}
	if !isFragmentSinkOp(sinkSpec.Type) {
		return fmt.Errorf("fragment task %s: last op %q is not a sink", task.ID, sinkSpec.Type)
	}
	// Locate every pipeline-breaker (HashAggregate, Sort) in middle. Reject
	// middle ops that aren't recognised as unary or breaker.
	var breakerIdxs []int
	for i, m := range middle {
		if isFragmentSourceOp(m.Type) || isFragmentSinkOp(m.Type) {
			return fmt.Errorf("fragment task %s: middle op %d is %q, must be unary or breaker", task.ID, i, m.Type)
		}
		if isFragmentBreakerOp(m.Type) {
			breakerIdxs = append(breakerIdxs, i)
		}
	}

	// Empty source files: legitimate "upstream produced nothing" case (e.g.,
	// an aggregate or semi-join filtered every row). Emit zero rows and no
	// result files; downstream stages handle the empty-input shape via their
	// own short-circuits. Mirrors executeStageHashJoin's len(probeFiles)==0
	// check before any S3 I/O happens.
	//
	// Exception: OpGatherSink. The coordinator's gather receiver counts
	// terminal markers, not messages — skipping finalize would leave it
	// hanging until the 10-minute gather timeout. Open + finalize the sink
	// (its Finalize publishes a terminal even with no batches consumed) so
	// the receiver unblocks immediately on empty fragments.
	if len(sourceSpec.InputFiles) == 0 {
		if sinkSpec.Type == distributed.OpGatherSink {
			sink, err := e.openFragmentSink(task, sinkSpec)
			if err != nil {
				return fmt.Errorf("fragment task %s: open gather sink (empty source): %w", task.ID, err)
			}
			defer sink.close()
			if err := sink.finalize(ctx, task, result); err != nil {
				return fmt.Errorf("fragment task %s: finalize gather sink (empty source): %w", task.ID, err)
			}
		}
		return nil
	}

	// Build the source.
	src, err := e.buildFragmentSource(task, sourceSpec)
	if err != nil {
		return fmt.Errorf("fragment task %s: source: %w", task.ID, err)
	}
	if err := src.Init(ctx); err != nil {
		return fmt.Errorf("fragment task %s: source init: %w", task.ID, err)
	}
	defer src.Close()

	// Build the sink. Schema-discovery is lazy for the partitioned/
	// unpartitioned sinks; we capture the first batch's schema and construct
	// on demand.
	sink, err := e.openFragmentSink(task, sinkSpec)
	if err != nil {
		return fmt.Errorf("fragment task %s: sink: %w", task.ID, err)
	}
	defer sink.close()

	if len(breakerIdxs) == 0 {
		// Linear pipeline: source → unary ops → sink.
		preOps, cleanup, err := e.buildUnaryChain(ctx, task, middle)
		if err != nil {
			return err
		}
		defer cleanup()
		return e.runFragmentLinear(ctx, task, src, preOps, sink, result)
	}

	// One or more pipeline-breakers — split middle into N+1 unary segments
	// around them and run a chain of consume/drain phases. No materialization
	// to disk between phases; each breaker holds its state in memory (with
	// optional spill).
	return e.runFragmentWithBreakers(ctx, task, src, middle, breakerIdxs, sink, result)
}

// runFragmentLinear streams batches from src through unary ops into the sink.
// No pipeline-breaker — every batch flows end-to-end before the next is read.
func (e *Executor) runFragmentLinear(ctx context.Context, task distributed.Task, src exec.Source, ops []exec.UnaryOperator, sink fragmentSink, result *distributed.ResultNotification) error {
	progress := exec.ProgressReporterFromContext(ctx)
	var totalRows int64
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		if err := sink.consume(ctx, b); err != nil {
			return err
		}
		n := int64(b.ActiveLen())
		totalRows += n
		if progress != nil {
			progress.AddRows(n)
		}
		return nil
	}
	for {
		b, err := src.Next(ctx)
		if err != nil {
			return fmt.Errorf("fragment task %s: source next: %w", task.ID, err)
		}
		if b == nil {
			break
		}
		cur := b
		for _, op := range ops {
			cur, err = op.Execute(ctx, cur)
			if err != nil {
				return fmt.Errorf("fragment task %s: unary exec: %w", task.ID, err)
			}
			if cur == nil {
				break
			}
		}
		if cur == nil || cur.ActiveLen() == 0 {
			continue
		}
		if err := consume(ctx, cur); err != nil {
			return fmt.Errorf("fragment task %s: sink consume: %w", task.ID, err)
		}
	}
	// Spilled-partition flush. Grace Hash Join probes route rows whose hash
	// partition was evicted to disk; without this drain, those probe rows
	// never re-enter the chain and the join silently drops matches. Mirror
	// of exec.Pipeline.flushSpilledOps. Q05 with shared-spill + multi-probe
	// chain (build pressure → primary's only build partition spills →
	// every probe row routes to disk) returns 0 rows without this drain.
	if err := drainFlushableOps(ctx, ops, false, consume); err != nil {
		return fmt.Errorf("fragment task %s: flush spilled ops: %w", task.ID, err)
	}
	result.NumRows = totalRows
	if err := sink.finalize(ctx, task, result); err != nil {
		return fmt.Errorf("fragment task %s: sink finalize: %w", task.ID, err)
	}
	return nil
}

// drainFlushableOps drains any FlushableOperators (Grace Hash Join probes that
// spilled partitions to disk) through the remaining unary chain into consume.
// Mirrors exec.Pipeline.flushSpilledOps.
//
// snapshotSel matters when the downstream consumer retains batch references
// across calls (Sort, HashAggregate consuming into their internal store) and
// upstream ops in the chain reuse a per-instance Sel buffer (exec.Filter and
// the comparison/expression filters do — their selBuf is shared scratch).
// Without the copy, the next iteration's Filter clobbers the Sel of the
// already-stored batch. Sinks that consume each batch in one shot (S3 PUT,
// NATS publish) don't need the copy.
func drainFlushableOps(ctx context.Context, ops []exec.UnaryOperator, snapshotSel bool, consume func(context.Context, *batch.RecordBatch) error) error {
	for opIdx, op := range ops {
		fo, ok := op.(exec.FlushableOperator)
		if !ok || !fo.HasPendingFlush() {
			continue
		}
		downstream := ops[opIdx+1:]
		for {
			b, err := fo.NextFlush(ctx)
			if err != nil {
				return err
			}
			if b == nil {
				break
			}
			cur := b
			for _, dop := range downstream {
				cur, err = dop.Execute(ctx, cur)
				if err != nil {
					return err
				}
				if cur == nil {
					break
				}
			}
			if cur == nil || cur.ActiveLen() == 0 {
				continue
			}
			if snapshotSel && cur.Sel != nil {
				selCopy := make([]uint32, len(cur.Sel))
				copy(selCopy, cur.Sel)
				cur.Sel = selCopy
			}
			if err := consume(ctx, cur); err != nil {
				return err
			}
		}
	}
	return nil
}

// fragmentBreaker bundles an exec.SinkSource pipeline-breaker (HashAggregate,
// Sort) with the optional pre- and post- hooks that operator type needs.
// PrependOps run as the tail of the consume-phase unary chain (e.g.
// HashAggregate's derived-input Project). DrainXform runs per-batch during
// the drain phase before postOps (e.g. HashAggregate's AVG-fold). PostFinalize
// runs once between Finalize and the first Next (e.g. Sort.Truncate for the
// top-N optimization).
type fragmentBreaker struct {
	Op           exec.SinkSource
	Label        string
	PrependOps   []exec.UnaryOperator
	DrainXform   func(*batch.RecordBatch) (*batch.RecordBatch, error)
	PostFinalize func()
	Cleanup      func()
}

// runFragmentWithBreakers handles fragments containing one or more pipeline-
// breakers (HashAggregate, Sort). middle is split into N+1 unary segments
// around the breaker indices: seg[0] feeds the first breaker, seg[k] sits
// between breaker[k-1] and breaker[k], and seg[N] sits between the last
// breaker and the terminal sink.
//
// Each breaker is consumed entirely (Phase k consume) before its drain
// stream is piped into the next phase's consume. State stays in memory
// (with operator-level spill) — no S3 hop between phases.
//
// Today the planner emits at most one breaker per fragment; the multi-
// breaker path is exercised by tests and ready for future shapes
// (e.g. final_aggregate + sort fused into one fragment when the planner
// stops emitting them as separate Sort stages).
func (e *Executor) runFragmentWithBreakers(ctx context.Context, task distributed.Task, src exec.Source, middle []distributed.OpSpec, breakerIdxs []int, sink fragmentSink, result *distributed.ResultNotification) error {
	progress := exec.ProgressReporterFromContext(ctx)

	breakers := make([]*fragmentBreaker, len(breakerIdxs))
	for i, idx := range breakerIdxs {
		fb, err := e.buildFragmentBreaker(ctx, middle[idx])
		if err != nil {
			return fmt.Errorf("fragment task %s: build breaker %d (%s): %w", task.ID, i, middle[idx].Type, err)
		}
		breakers[i] = fb
		defer fb.Cleanup()
	}

	// Phase 0..N-1: feed the previous source (src for j=0, breakers[j-1].Op
	// otherwise) through middle[prevIdx+1:idx] + breakers[j].PrependOps into
	// breakers[j]. Apply breakers[j-1].DrainXform per-batch when draining a
	// previous breaker.
	currentSrc := src
	for j, idx := range breakerIdxs {
		segStart := 0
		if j > 0 {
			segStart = breakerIdxs[j-1] + 1
		}
		segOps, segCleanup, err := e.buildUnaryChain(ctx, task, middle[segStart:idx])
		if err != nil {
			return err
		}
		defer segCleanup()
		// Append breaker prepend-ops AFTER the segment ops; init each so the
		// pipeline can call Execute without further setup. PrependOps come
		// already-init'd from buildFragmentBreaker (HashAggregate's Project),
		// so don't double-init here.
		phaseOps := append([]exec.UnaryOperator{}, segOps...)
		phaseOps = append(phaseOps, breakers[j].PrependOps...)

		if j == 0 {
			pipe := &exec.Pipeline{Source: currentSrc, Ops: phaseOps, Sink: breakers[j].Op}
			if err := pipe.Run(ctx); err != nil {
				return fmt.Errorf("fragment task %s: %s consume: %w", task.ID, breakers[j].Label, err)
			}
		} else {
			if err := drainThroughBreaker(ctx, currentSrc, breakers[j-1].DrainXform, phaseOps, breakers[j].Op); err != nil {
				return fmt.Errorf("fragment task %s: %s consume: %w", task.ID, breakers[j].Label, err)
			}
			if err := breakers[j].Op.Finalize(ctx); err != nil {
				return fmt.Errorf("fragment task %s: %s finalize: %w", task.ID, breakers[j].Label, err)
			}
		}
		if breakers[j].PostFinalize != nil {
			breakers[j].PostFinalize()
		}
		currentSrc = breakers[j].Op
	}

	// Final phase: drain the last breaker through middle[lastIdx+1:end] into
	// sink. Apply the last breaker's DrainXform per-batch before postOps.
	lastIdx := breakerIdxs[len(breakerIdxs)-1]
	finalOps, finalCleanup, err := e.buildUnaryChain(ctx, task, middle[lastIdx+1:])
	if err != nil {
		return err
	}
	defer finalCleanup()

	last := breakers[len(breakers)-1]
	var totalRows int64
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		if err := sink.consume(ctx, b); err != nil {
			return err
		}
		n := int64(b.ActiveLen())
		totalRows += n
		if progress != nil {
			progress.AddRows(n)
		}
		return nil
	}
	for {
		b, err := currentSrc.Next(ctx)
		if err != nil {
			return fmt.Errorf("fragment task %s: %s next: %w", task.ID, last.Label, err)
		}
		if b == nil {
			break
		}
		cur := b
		if last.DrainXform != nil {
			cur, err = last.DrainXform(cur)
			if err != nil {
				return fmt.Errorf("fragment task %s: %s drain xform: %w", task.ID, last.Label, err)
			}
			if cur == nil {
				continue
			}
		}
		for _, op := range finalOps {
			cur, err = op.Execute(ctx, cur)
			if err != nil {
				return fmt.Errorf("fragment task %s: post-breaker exec: %w", task.ID, err)
			}
			if cur == nil {
				break
			}
		}
		if cur == nil || cur.ActiveLen() == 0 {
			continue
		}
		if err := consume(ctx, cur); err != nil {
			return fmt.Errorf("fragment task %s: sink consume: %w", task.ID, err)
		}
	}
	// Spilled-partition flush for any HashJoin probes in finalOps.
	if err := drainFlushableOps(ctx, finalOps, false, consume); err != nil {
		return fmt.Errorf("fragment task %s: flush spilled finalOps: %w", task.ID, err)
	}
	result.NumRows = totalRows
	if err := sink.finalize(ctx, task, result); err != nil {
		return fmt.Errorf("fragment task %s: sink finalize: %w", task.ID, err)
	}
	return nil
}

// drainThroughBreaker pulls batches from src, applies an optional per-batch
// transform, pipes them through the unary chain, and pushes each non-empty
// result into sink.Consume. Caller is responsible for sink.Finalize.
//
// The Sel snapshot before sink.Consume mirrors the legacy applyPostFilter
// pattern: exec.Filter (and other Sel-emitting ops) reuses its internal
// selBuf across calls. When the sink is a retaining SinkSource (Sort,
// HashAggregate), it stores the batch reference; without the copy, the
// next iteration's Filter execution would overwrite the same selBuf and
// corrupt the previously-stored batch's Sel — a Q07-style bug documented
// at executor_stage.go:applyPostFilter.
func drainThroughBreaker(ctx context.Context, src exec.Source, xform func(*batch.RecordBatch) (*batch.RecordBatch, error), ops []exec.UnaryOperator, sink exec.Sink) error {
	consume := func(ctx context.Context, b *batch.RecordBatch) error {
		if b.Sel != nil {
			selCopy := make([]uint32, len(b.Sel))
			copy(selCopy, b.Sel)
			b.Sel = selCopy
		}
		return sink.Consume(ctx, b)
	}
	for {
		b, err := src.Next(ctx)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		cur := b
		if xform != nil {
			cur, err = xform(cur)
			if err != nil {
				return err
			}
			if cur == nil {
				continue
			}
		}
		for _, op := range ops {
			cur, err = op.Execute(ctx, cur)
			if err != nil {
				return err
			}
			if cur == nil {
				break
			}
		}
		if cur == nil || cur.ActiveLen() == 0 {
			continue
		}
		if err := consume(ctx, cur); err != nil {
			return err
		}
	}
	// Spilled-partition flush — same reason as runFragmentLinear. The
	// downstream sink here is a retaining SinkSource (Sort/HashAggregate),
	// so snapshotSel=true.
	return drainFlushableOps(ctx, ops, true, consume)
}

// buildFragmentBreaker dispatches per-OpType breaker construction. Returns a
// fully-initialised SinkSource plus the optional pre/drain/post hooks the
// breaker needs.
func (e *Executor) buildFragmentBreaker(ctx context.Context, spec distributed.OpSpec) (*fragmentBreaker, error) {
	switch spec.Type {
	case distributed.OpHashAggregate:
		hashAgg, err := e.buildFragmentHashAggregate(spec)
		if err != nil {
			return nil, err
		}
		fb := &fragmentBreaker{
			Op:      hashAgg,
			Label:   "hash_aggregate",
			Cleanup: func() { hashAgg.Close() },
		}
		// Optional derived-input projection. Mirrors executeStageAggregate's
		// buildAggInputProjection — required for partial aggregates whose
		// inputs are SQL expressions (e.g. SUM(l_extendedprice*(1-l_discount))).
		// Skipped for merge mode: the partial stage already computed the
		// derived column under OutputCol.
		if spec.BuildProject && !spec.MergeMode {
			project, _, perr := buildAggInputProjection(spec.GroupByCols, spec.Aggregates, nil)
			if perr != nil {
				return nil, fmt.Errorf("agg input project: %w", perr)
			}
			if project != nil {
				if perr := project.Init(ctx); perr != nil {
					return nil, fmt.Errorf("project init: %w", perr)
				}
				fb.PrependOps = append(fb.PrependOps, project)
			}
		}
		// AVG-fold collapses __avg_sum#X / __avg_count#X synthetics into the
		// single AVG output column on the FINAL stage only. Wrap the slice-
		// taking applyAvgFold helper as a per-batch transform.
		if spec.FoldAvg {
			fb.DrainXform = func(b *batch.RecordBatch) (*batch.RecordBatch, error) {
				folded, ferr := applyAvgFold([]*batch.RecordBatch{b})
				if ferr != nil {
					return nil, ferr
				}
				if len(folded) == 0 {
					return nil, nil
				}
				return folded[0], nil
			}
		}
		return fb, nil

	case distributed.OpSort:
		sorter, err := e.buildFragmentSort(spec)
		if err != nil {
			return nil, err
		}
		if err := sorter.Init(ctx); err != nil {
			sorter.Close()
			return nil, fmt.Errorf("sort init: %w", err)
		}
		fb := &fragmentBreaker{
			Op:      sorter,
			Label:   "sort",
			Cleanup: func() { sorter.Close() },
		}
		// Truncate to the top-N rows after Finalize. Mirrors
		// executeStageSort: Truncate runs ONCE between Finalize and the
		// first Next so the materialized output is bounded.
		if spec.SortLimit > 0 {
			limit := spec.SortLimit
			fb.PostFinalize = func() { sorter.Truncate(limit) }
		}
		return fb, nil

	default:
		return nil, fmt.Errorf("unsupported breaker op %q", spec.Type)
	}
}

// buildFragmentSort constructs an exec.Sort from an OpSpec. Mirrors the
// executeStageSort key conversion (Desc → exec.Descending). Spill is wired
// from the executor's shared spill manager when present.
func (e *Executor) buildFragmentSort(spec distributed.OpSpec) (*exec.Sort, error) {
	if len(spec.SortKeySpecs) == 0 {
		return nil, fmt.Errorf("sort: SortKeySpecs required")
	}
	keys := make([]exec.SortKey, len(spec.SortKeySpecs))
	for i, k := range spec.SortKeySpecs {
		order := exec.Ascending
		if k.Desc {
			order = exec.Descending
		}
		keys[i] = exec.SortKey{Column: k.Column, Order: order}
	}
	sorter := exec.NewSort(keys)
	if e.sharedSpill != nil {
		sorter.Spill = e.sharedSpill
	}
	return sorter, nil
}

// buildFragmentHashAggregate constructs an exec.HashAggregate from an OpSpec.
// In merge mode the spec's Aggregates are rewritten so InputCol = OutputCol
// (the partial-output column) and COUNT becomes SUM (counting partial rows
// re-counts groups, not source rows). Mirrors the rewrite in
// executeStageAggregate.
func (e *Executor) buildFragmentHashAggregate(spec distributed.OpSpec) (*exec.HashAggregate, error) {
	if len(spec.Aggregates) == 0 && len(spec.GroupByCols) == 0 {
		return nil, fmt.Errorf("hash_aggregate: at least one of GroupByCols or Aggregates is required")
	}
	aggCols := make([]exec.AggColumn, len(spec.Aggregates))
	for i, a := range spec.Aggregates {
		inputCol := a.InputCol
		fn := parseAggFuncString(a.Func)
		if spec.MergeMode {
			if a.OutputCol != "" {
				inputCol = a.OutputCol
			}
			if fn == exec.AggCount {
				fn = exec.AggSum
			}
		}
		aggCols[i] = exec.AggColumn{
			Func:       fn,
			InputCol:   inputCol,
			OutputCol:  a.OutputCol,
			OutputType: aggOutputTypeString(a.Func),
		}
	}
	hashAgg := exec.NewHashAggregate(spec.GroupByCols, aggCols)
	if e.sharedSpill != nil {
		hashAgg.Spill = e.sharedSpill
	}
	return hashAgg, nil
}

// buildUnaryChain builds and inits each unary op in specs. Returns a single
// cleanup that closes every op (and any owned resources like build-side hash
// tables) in reverse order.
func (e *Executor) buildUnaryChain(ctx context.Context, task distributed.Task, specs []distributed.OpSpec) ([]exec.UnaryOperator, func(), error) {
	var ops []exec.UnaryOperator
	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	for i, opSpec := range specs {
		opChain, opCleanup, err := e.buildFragmentUnary(ctx, task, opSpec)
		if err != nil {
			cleanup()
			return nil, nil, fmt.Errorf("fragment task %s: unary op %d (%s): %w", task.ID, i, opSpec.Type, err)
		}
		if opCleanup != nil {
			cleanups = append(cleanups, opCleanup)
		}
		for _, op := range opChain {
			if err := op.Init(ctx); err != nil {
				cleanup()
				return nil, nil, fmt.Errorf("fragment task %s: unary op %d init: %w", task.ID, i, err)
			}
		}
		ops = append(ops, opChain...)
	}
	return ops, cleanup, nil
}

func isFragmentSourceOp(t distributed.OpType) bool {
	return t == distributed.OpScan || t == distributed.OpShuffleSource
}

func isFragmentSinkOp(t distributed.OpType) bool {
	return t == distributed.OpExchangeSender || t == distributed.OpUnpartitionedSink || t == distributed.OpGatherSink
}

func isFragmentBreakerOp(t distributed.OpType) bool {
	return t == distributed.OpHashAggregate || t == distributed.OpSort
}

func (e *Executor) buildFragmentSource(task distributed.Task, spec distributed.OpSpec) (exec.Source, error) {
	bucket := spec.InputBucket
	if bucket == "" {
		bucket = task.DataBucket
	}
	if bucket == "" {
		bucket = task.ResultBucket
	}
	if len(spec.InputFiles) == 0 {
		return nil, fmt.Errorf("source %q: empty InputFiles", spec.Type)
	}
	src, err := e.sourceForAliasWithProjection(task.QueryID, bucket, spec.InputAlias, spec.InputFiles, spec.Columns)
	if err != nil {
		return nil, err
	}
	// Row-group sharding for OpScan over a single compacted parquet file.
	if spec.ScanShardCount > 1 {
		if cs, ok := src.(*cachedFileStreamSource); ok {
			cs.SetShard(spec.ScanShardIndex, spec.ScanShardCount)
		} else {
			e.logger.Warn("fragment source: shard params set but source is not cachedFileStreamSource",
				"alias", spec.InputAlias, "shard_idx", spec.ScanShardIndex, "shard_count", spec.ScanShardCount)
		}
	}
	return src, nil
}

func (e *Executor) buildFragmentUnary(ctx context.Context, task distributed.Task, spec distributed.OpSpec) ([]exec.UnaryOperator, func(), error) {
	switch spec.Type {
	case distributed.OpFilter:
		ops, _, err := compileFilterExprs(spec.Predicates)
		if err != nil {
			return nil, nil, err
		}
		return ops, nil, nil

	case distributed.OpHashJoinProbe, distributed.OpBroadcastProbe:
		return e.buildFragmentJoinProbe(ctx, task, spec)

	default:
		return nil, nil, fmt.Errorf("unsupported unary op %q", spec.Type)
	}
}

func (e *Executor) buildFragmentJoinProbe(ctx context.Context, task distributed.Task, spec distributed.OpSpec) ([]exec.UnaryOperator, func(), error) {
	if len(spec.LeftKeys) == 0 || len(spec.RightKeys) == 0 {
		return nil, nil, fmt.Errorf("hash_join_probe: LeftKeys and RightKeys required")
	}
	if len(spec.BuildFiles) == 0 {
		// Empty build → inner/semi joins emit no rows; upstream planner may
		// have decided this fragment should not run, but we treat empty
		// build as "no output" to match executeStageHashJoin's behavior.
		// Returning an op chain that drops every row keeps the pipeline
		// well-formed without special-casing the runner.
		return []exec.UnaryOperator{exec.NewFilter(func(_ *batch.RecordBatch, _ int) bool {
			return false
		})}, nil, nil
	}
	bucket := spec.BuildBucket
	if bucket == "" {
		bucket = task.DataBucket
	}
	if bucket == "" {
		bucket = task.ResultBucket
	}
	buildSrc, err := e.sourceForAlias(task.QueryID, bucket, spec.BuildAlias, spec.BuildFiles)
	if err != nil {
		return nil, nil, fmt.Errorf("build source: %w", err)
	}
	if err := buildSrc.Init(ctx); err != nil {
		buildSrc.Close()
		return nil, nil, fmt.Errorf("build source init: %w", err)
	}

	hj := exec.NewHashJoin(mapJoinTypeString(spec.JoinType), spec.LeftKeys, spec.RightKeys)
	hj.BuildTableAlias = spec.BuildAlias
	hj.QualifyAllBuildCols = spec.QualifyAllBuildCols
	if spec.BuildRowHint > 0 {
		hj.BuildRowHint = spec.BuildRowHint
	}
	hj.SemiAntiKeyOnly = spec.SemiAntiKeyOnly
	if spec.JoinFilter != "" {
		hj.SemiAntiFilter = physical.BuildSemiAntiFilter(spec.JoinFilter)
	}
	if e.sharedSpill != nil {
		hj.Spill = e.sharedSpill
		hj.MemTracker = e.sharedTracker
		// Broadcast probes always force partition-on-arrival to bound peak
		// heap; shuffle-side probes opt in based on observed pool pressure.
		// See executeStageHashJoin for the policy rationale.
		if spec.Type == distributed.OpBroadcastProbe {
			hj.PartitionOnArrival = true
		} else {
			hj.PartitionOnArrival = exec.SharedPoolUnderPressure(e.sharedTracker)
		}
	}
	if err := hj.Build(ctx, buildSrc); err != nil {
		buildSrc.Close()
		return nil, nil, fmt.Errorf("building hash table: %w", err)
	}
	buildSrc.Close()
	hj.FixKeyAssignment()

	probe := hj.Probe()
	if len(spec.OutputColumns) > 0 {
		filter := make(map[string]bool, len(spec.OutputColumns))
		for _, c := range spec.OutputColumns {
			filter[c] = true
		}
		probe.OutputFilter = filter
	}
	cleanup := func() { hj.Close() }
	return []exec.UnaryOperator{probe}, cleanup, nil
}

// fragmentSink is the internal interface every fragment-sink kind implements:
// stream batches in via consume, finalize once at end, then upload / publish.
// Distinct from exec.Sink because Finalize here also handles the sink-specific
// upload + KV cache + LocalStageCache adoption.
type fragmentSink interface {
	consume(ctx context.Context, b *batch.RecordBatch) error
	finalize(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error
	close()
}

func (e *Executor) openFragmentSink(task distributed.Task, spec distributed.OpSpec) (fragmentSink, error) {
	switch spec.Type {
	case distributed.OpExchangeSender:
		if spec.NumPartitions <= 0 {
			return nil, fmt.Errorf("exchange_sender: NumPartitions must be > 0")
		}
		spillDir := stageSpillDir(e, task)
		if err := os.MkdirAll(spillDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating spill dir: %w", err)
		}
		return &fragmentExchangeSink{
			executor:    e,
			spillDir:    spillDir,
			shuffleKeys: spec.ShuffleKeys,
			numParts:    spec.NumPartitions,
		}, nil

	case distributed.OpUnpartitionedSink:
		spillDir := stageSpillDir(e, task)
		if err := os.MkdirAll(spillDir, 0o755); err != nil {
			return nil, fmt.Errorf("creating spill dir: %w", err)
		}
		s := newUnpartitionedStageSink(spillDir, task.ID)
		if err := s.Init(context.Background()); err != nil {
			return nil, fmt.Errorf("unpartitioned sink init: %w", err)
		}
		return &fragmentUnpartitionedSink{executor: e, sink: s, spillDir: spillDir}, nil

	case distributed.OpGatherSink:
		if spec.ReplySubject == "" {
			return nil, fmt.Errorf("gather_sink: ReplySubject required")
		}
		if e.nc == nil {
			return nil, fmt.Errorf("gather_sink: NATS connection required")
		}
		// Schema is captured on first Consume so we don't need to plumb it
		// through openFragmentSink. nil here is the same shape gather uses
		// in writeStageOutput.
		s := newGatherReplySink(e.nc, spec.ReplySubject, "", nil)
		return &fragmentGatherSink{sink: s}, nil

	default:
		return nil, fmt.Errorf("unsupported sink op %q", spec.Type)
	}
}

func stageSpillDir(e *Executor, task distributed.Task) string {
	if e.spillDir == "" {
		return filepath.Join(os.TempDir(), "stage-"+task.ID)
	}
	return filepath.Join(e.spillDir, "stage-"+task.ID)
}

// fragmentExchangeSink wraps partitionedShuffleSink with lazy schema discovery
// (sink construction deferred until the first non-empty batch carries a
// schema).
type fragmentExchangeSink struct {
	executor    *Executor
	spillDir    string
	shuffleKeys []string
	numParts    int
	sink        *partitionedShuffleSink
}

func (s *fragmentExchangeSink) consume(ctx context.Context, b *batch.RecordBatch) error {
	if s.sink == nil {
		s.sink = newPartitionedShuffleSink(s.spillDir, s.shuffleKeys, s.numParts, b.Schema)
		if err := s.sink.Init(ctx); err != nil {
			return fmt.Errorf("exchange sink init: %w", err)
		}
	}
	return s.sink.Consume(ctx, b)
}

func (s *fragmentExchangeSink) finalize(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	defer os.RemoveAll(s.spillDir)
	if s.sink == nil {
		return nil
	}
	if err := s.sink.Finalize(ctx); err != nil {
		return err
	}
	return s.executor.uploadPartitionedShuffleFiles(ctx, task, s.sink, result)
}

func (s *fragmentExchangeSink) close() {
	if s.sink != nil {
		s.sink.Close()
	}
}

type fragmentUnpartitionedSink struct {
	executor *Executor
	sink     *unpartitionedStageSink
	spillDir string
}

func (s *fragmentUnpartitionedSink) consume(ctx context.Context, b *batch.RecordBatch) error {
	return s.sink.Consume(ctx, b)
}

func (s *fragmentUnpartitionedSink) finalize(ctx context.Context, task distributed.Task, result *distributed.ResultNotification) error {
	defer os.RemoveAll(s.spillDir)
	if err := s.sink.Finalize(ctx); err != nil {
		return err
	}
	if s.sink.NumChunks() == 0 {
		return nil
	}
	return s.executor.uploadUnpartitionedSpill(ctx, task, s.sink, result)
}

func (s *fragmentUnpartitionedSink) close() { s.sink.Close() }

type fragmentGatherSink struct {
	sink     *gatherReplySink
	finished bool
}

func (s *fragmentGatherSink) consume(ctx context.Context, b *batch.RecordBatch) error {
	if !s.finished {
		// Init only on first non-empty batch; the sink captures schema lazily
		// from gather's NATS publish path.
		if err := s.sink.Init(ctx); err != nil {
			return err
		}
		s.finished = true
	}
	return s.sink.Consume(ctx, b)
}

func (s *fragmentGatherSink) finalize(ctx context.Context, _ distributed.Task, _ *distributed.ResultNotification) error {
	if !s.finished {
		// No batches consumed: still need to publish a terminal marker so
		// the coord's gather subscriber unblocks.
		if err := s.sink.Init(ctx); err != nil {
			return err
		}
	}
	return s.sink.Finalize(ctx)
}

func (s *fragmentGatherSink) close() {}

