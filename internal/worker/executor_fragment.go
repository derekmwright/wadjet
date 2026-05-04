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
	for i, m := range middle {
		if isFragmentSourceOp(m.Type) || isFragmentSinkOp(m.Type) {
			return fmt.Errorf("fragment task %s: middle op %d is %q, must be unary", task.ID, i, m.Type)
		}
	}

	// Empty source files: legitimate "upstream produced nothing" case (e.g.,
	// an aggregate or semi-join filtered every row). Emit zero rows and no
	// result files; downstream stages handle the empty-input shape via their
	// own short-circuits. Mirrors executeStageHashJoin's len(probeFiles)==0
	// check before any S3 I/O happens.
	if len(sourceSpec.InputFiles) == 0 {
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

	// Build unary operators in order.
	var unaryOps []exec.UnaryOperator
	var unaryCleanups []func()
	defer func() {
		for _, c := range unaryCleanups {
			c()
		}
	}()
	for i, opSpec := range middle {
		opChain, cleanup, err := e.buildFragmentUnary(ctx, task, opSpec)
		if err != nil {
			return fmt.Errorf("fragment task %s: unary op %d (%s): %w", task.ID, i, opSpec.Type, err)
		}
		if cleanup != nil {
			unaryCleanups = append(unaryCleanups, cleanup)
		}
		for _, op := range opChain {
			if err := op.Init(ctx); err != nil {
				return fmt.Errorf("fragment task %s: unary op %d init: %w", task.ID, i, err)
			}
		}
		unaryOps = append(unaryOps, opChain...)
	}

	// Build the sink. Schema-discovery is lazy for the partitioned/unpartitioned
	// sinks; we capture the first batch's schema and construct on demand.
	sink, err := e.openFragmentSink(task, sinkSpec)
	if err != nil {
		return fmt.Errorf("fragment task %s: sink: %w", task.ID, err)
	}
	defer sink.close()

	progress := exec.ProgressReporterFromContext(ctx)
	var totalRows int64

	for {
		b, err := src.Next(ctx)
		if err != nil {
			return fmt.Errorf("fragment task %s: source next: %w", task.ID, err)
		}
		if b == nil {
			break
		}
		cur := b
		for _, op := range unaryOps {
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
		if err := sink.consume(ctx, cur); err != nil {
			return fmt.Errorf("fragment task %s: sink consume: %w", task.ID, err)
		}
		n := int64(cur.ActiveLen())
		totalRows += n
		if progress != nil {
			progress.AddRows(n)
		}
	}
	result.NumRows = totalRows
	if err := sink.finalize(ctx, task, result); err != nil {
		return fmt.Errorf("fragment task %s: sink finalize: %w", task.ID, err)
	}
	return nil
}

func isFragmentSourceOp(t distributed.OpType) bool {
	return t == distributed.OpScan || t == distributed.OpShuffleSource
}

func isFragmentSinkOp(t distributed.OpType) bool {
	return t == distributed.OpExchangeSender || t == distributed.OpUnpartitionedSink || t == distributed.OpGatherSink
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

