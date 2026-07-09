package distributed

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"time"
)

// TaskType identifies the kind of work a task performs.
type TaskType string

const (
	TaskTypePipeline TaskType = "pipeline" // full query executed as standalone pipeline on one worker
	TaskTypeShuffle  TaskType = "shuffle"  // hash-partitions input rows into N output partition files
	TaskTypeGather   TaskType = "gather"   // streams pipeline output to ReplySubject via gatherReplySink
	TaskTypeStage    TaskType = "stage"    // single-operator stage fragment (native-DAG Phase 3)
)

// Task is the unit of distributed work published to NATS JetStream.
type Task struct {
	ID        string   `json:"id"`
	QueryID   string   `json:"query_id"`
	StageID   string   `json:"stage_id"`
	Type      TaskType `json:"type"`
	ClusterID string   `json:"cluster_id,omitempty"` // target cluster for routing
	TableName string   `json:"table_name,omitempty"`
	// Attempt is the 1-based execution attempt for this task ID. Stage
	// inputs are durable S3 files and outputs are overwrite-safe (same
	// TaskID → same key), so the coordinator re-dispatches failed tasks
	// with the same ID and a bumped Attempt (coordinator.taskRetrier).
	// 0 means unset (pre-retry senders); treat as attempt 1.
	Attempt int `json:"attempt,omitempty"`
	// EstimatedBytes is the coordinator's estimate of this task's input
	// footprint, used for memory-aware admission (worker holds the task
	// start until the shared pool has room for it) and coordinator-side
	// bin-packing. 0 = unknown — admission and placement fall back to
	// pressure-threshold / round-robin behavior. Doubled on every
	// re-dispatch (grow-on-retry, the Trino FTE pattern): a task whose
	// worker died possibly-of-memory is only admitted where strictly more
	// headroom exists.
	EstimatedBytes int64 `json:"estimated_bytes,omitempty"`

	// Pipeline-specific (full query on one worker)
	SQLText    string `json:"sql_text,omitempty"`    // SQL query to execute as standalone pipeline
	DataBucket string `json:"data_bucket,omitempty"` // bucket containing source data (tables)

	// Scan-split pipeline: table scans distributed across workers, compute on one worker.
	// Maps scan alias → result file paths from pre-scanned data.
	// Alias is unique per scan node: "table" or "table:N" for self-joins.
	PreScannedInputs map[string][]string `json:"pre_scanned_inputs,omitempty"`

	// Probe-split pipeline: the probe table's files are partitioned across workers
	// while build tables are scanned in full by each worker. Maps scan alias →
	// allowed file paths. Only the probe scan alias has a restricted file list;
	// other scans read all files normally.
	ScanFileFilter map[string][]string `json:"scan_file_filter,omitempty"`

	// Row-group sharding for single-file scans. When ScanShardCount > 1, the
	// worker reads only row groups [idx*N/count, (idx+1)*N/count) of each
	// input parquet file (where N = file's NumRowGroups). The dispatcher
	// uses this to fan out a single compacted file (e.g. SF10 partsupp =
	// one 691 MB file) into multiple parallel scan tasks; without it the
	// downstream broadcast-join chain cascades single-tasked because
	// `broadcastJoinProbeSplit` requires probe upstream to have ≥ 2 files.
	// ScanShardCount = 0 or 1 means no sharding (whole file).
	ScanShardIndex int `json:"scan_shard_index,omitempty"`
	ScanShardCount int `json:"scan_shard_count,omitempty"`

	// PartialAggregate is set on probe-split pipeline tasks to indicate that
	// the top-level Sort and Limit should be stripped. Each worker produces
	// complete partial aggregates; the coordinator merges them.
	PartialAggregate bool `json:"partial_aggregate,omitempty"`

	// Scan-specific
	Files           []string          `json:"files,omitempty"`
	PartitionFilter map[string]string `json:"partition_filter,omitempty"`
	// Columns has dual semantics by stage type:
	//   - scan / shuffle tasks: input projection (columns to read from
	//     parquet/wshf source).
	//   - hash_join / broadcast_join tasks: output projection — the worker
	//     applies these as the probe operator's OutputFilter so the join
	//     emits only what the downstream stage consumes, instead of the
	//     full union of build+probe schemas.
	//   - aggregate / sort tasks: ignored (output schema is determined by
	//     AggSpecs / SortKeys).
	Columns     []string `json:"columns,omitempty"`
	FilterExprs []string `json:"filter_exprs,omitempty"` // SQL filter expressions for pushdown
	// PostFilterExprs are SQL filter expressions applied to the stage's
	// OUTPUT (post-aggregate/post-join) rather than to raw scan input.
	// Native-DAG compute stages use this for HAVING and join residual
	// predicates. FilterExprs vs PostFilterExprs differ in column scope:
	// FilterExprs references scan columns; PostFilterExprs references
	// aggregate output cols or joined-schema cols.
	PostFilterExprs []string `json:"post_filter_exprs,omitempty"`

	// Fused scan-aggregate: partial aggregation done at scan level
	ScanAggGroupBy []string  `json:"scan_agg_group_by,omitempty"`
	ScanAggSpecs   []AggSpec `json:"scan_agg_specs,omitempty"`

	// Aggregate-specific
	GroupByCols []string  `json:"group_by_cols,omitempty"`
	Aggregates  []AggSpec `json:"aggregates,omitempty"`
	InputFiles  []string  `json:"input_files,omitempty"` // results from previous stage

	// Sort-specific
	SortKeys       []SortKeySpec `json:"sort_keys,omitempty"`
	Limit          int           `json:"limit,omitempty"`
	MergePreSorted bool          `json:"merge_pre_sorted,omitempty"` // true for merge_sort: inputs are pre-sorted
	MergePartials  bool          `json:"merge_partials,omitempty"`   // true for final_aggregate: re-aggregate partial results

	// Join-specific
	JoinType        string   `json:"join_type,omitempty"`         // inner, left, right, full, cross
	JoinLeftKeys    []string `json:"join_left_keys,omitempty"`    // probe side key columns
	JoinRightKeys   []string `json:"join_right_keys,omitempty"`   // build side key columns
	BuildFiles      []string `json:"build_files,omitempty"`       // build (right) side input files
	BuildTableAlias string   `json:"build_table_alias,omitempty"` // build-side alias for column disambiguation
	// QualifyAllBuildCols, when true, forces the join executor to emit
	// build-side columns under their qualified name even when no probe-side
	// column has the same base name. Set by the planner for self-join scenarios
	// (Q07's two scans of nation that co-path into the same join chain).
	QualifyAllBuildCols bool   `json:"qualify_all_build_cols,omitempty"`
	JoinFilter          string `json:"join_filter,omitempty"` // semi/anti join inequality filter expression

	// Fused join: additional broadcast joins absorbed into a single task.
	// The worker builds hash tables for each fused join, then chains probes
	// batch-by-batch: probe → join1 → join2 → ... → output.
	FusedJoins []FusedJoinSpec `json:"fused_joins,omitempty"`

	// Window-specific
	WindowCols []WindowColSpec `json:"window_cols,omitempty"`

	// Shuffle-specific
	ShuffleKeys   []string `json:"shuffle_keys,omitempty"`   // columns to hash-partition on
	NumPartitions int      `json:"num_partitions,omitempty"` // number of output partitions
	PartitionID   int      `json:"partition_id,omitempty"`   // which partition this join task handles

	// Dynamic filters carried at the top level for non-fragment task shapes
	// (TaskTypeShuffle). Fragment tasks carry the same data in OpSpec.DynamicFilters
	// per-op, but shuffle tasks use a flat task descriptor and apply the
	// filter against their single implicit scan source. The worker materializes
	// these into the cachedFileStreamSource's bloom+range pushdown.
	DynamicFilters []DynamicFilterSpec `json:"dynamic_filters,omitempty"`

	// Distributed tracing context (W3C Trace Context format)
	TraceID    string `json:"trace_id,omitempty"`    // 32-char hex
	SpanID     string `json:"span_id,omitempty"`     // 16-char hex parent span
	TraceFlags byte   `json:"trace_flags,omitempty"` // 0x01 = sampled

	// Identity context (for access control enforcement at workers)
	IdentityName string `json:"identity_name,omitempty"`
	IdentityRole string `json:"identity_role,omitempty"`

	// ABAC pre-evaluated policy decisions (serialized for worker enforcement)
	PolicyDecisionJSON json.RawMessage `json:"policy_decision,omitempty"`

	// Result destination
	ResultBucket string `json:"result_bucket"`
	ResultPrefix string `json:"result_prefix"`

	// PreComputedAggregates carries pre-computed derived-aggregate results
	// that the worker should substitute for in-plan aggregate subtrees.
	// Matches on (input_table, group_by_cols, aggregate specs); when a
	// logical-plan aggregate node matches a signature here, the worker
	// replaces it with a scan of the provided cache files. Populated by
	// the coordinator when PickAggregateShuffleCandidate + preCompute
	// succeed (spec: 2026-04-18-shuffle-distributed-aggregate.md).
	PreComputedAggregates []PreComputedAggregate `json:"pre_computed_aggregates,omitempty"`

	// Inputs maps scan/alias name → S3 keys for upstream stage output.
	// Generalizes PreScannedInputs: used for both table-scan inputs (legacy)
	// and previous-stage-output inputs (Phase 3 native DAG). Worker source
	// selection inspects file patterns: partition=NNNN/*.wshf → partitionShardSource;
	// *.parquet → streamSource.
	Inputs map[string][]string `json:"inputs,omitempty"`

	// InputLocations maps an input S3 key to the peer-exchange address of
	// the worker that produced the file and still holds it on local disk
	// (streaming exchange Phase A, docs/design/streaming-exchange.md).
	// Best-effort hints: a consumer tries one peer fetch per hinted key and
	// falls through to KV/S3 on any failure — the S3 keys stay canonical,
	// so a task spec re-sent verbatim on retry works with or without the
	// hints. Only populated when the coordinator runs --streaming-exchange.
	InputLocations map[string]string `json:"input_locations,omitempty"`

	// FetchToken authorizes peer-exchange fetches for this task's query.
	// Producers record it (to validate incoming FetchShuffle requests
	// against); consumers present it. Minted per QueryID by the
	// coordinator; empty when streaming exchange is disabled.
	FetchToken string `json:"fetch_token,omitempty"`

	// AsyncUpload (streaming exchange Phase B) tells the worker to report
	// task completion once stage-output files are finalized on local disk
	// (and adopted into the LocalStageCache for peer serving), continuing
	// the S3 upload in the background. The worker publishes UploadComplete
	// when the task's uploads land; until then the S3 copy may not exist
	// and consumers rely on the peer tier (with a bounded S3 re-poll).
	// Only set by the coordinator for native-DAG task types whose outputs
	// are consumed by workers; false = today's synchronous upload.
	AsyncUpload bool `json:"async_upload,omitempty"`

	// Output is the S3 prefix where this task's output is materialized.
	// Shuffle/pipeline-intermediate: worker writes "<Output>partition=NNNN/<taskID>.wshf".
	// Pipeline-final (before Gather): single-partition output at "<Output><taskID>.wshf".
	// Gather: empty; worker streams to ReplySubject.
	Output string `json:"output,omitempty"`

	// ReplySubject is the NATS subject the worker publishes batch chunks to.
	// Only set for TaskTypeGather; enables real-operator Gather semantics.
	ReplySubject string `json:"reply_subject,omitempty"`

	// GatherOrdering (Gather only) — merge-sort keys applied by the coordinator
	// when reassembling output from multiple gather workers. Empty means no
	// ordering; coordinator concatenates streams in arrival order.
	GatherOrdering []SortKeySpec `json:"gather_ordering,omitempty"`

	// GatherLimit (Gather only) — top-N limit applied by the coordinator
	// after ordering. Zero means no limit.
	GatherLimit int `json:"gather_limit,omitempty"`

	// StageType discriminates TaskTypeStage variants: "scan", "hash_join",
	// "broadcast_join", "aggregate", "sort", "merge_sort", "window",
	// "final_aggregate". Matches physical.Stage.Type strings. Empty for
	// non-TaskTypeStage tasks.
	StageType string `json:"stage_type,omitempty"`

	// BuildRowHint is the planner's estimate of build-side row count,
	// used to pre-size the hash table arena. Populated for TaskTypeStage
	// hash_join stages. Zero means no hint (arena grows dynamically).
	BuildRowHint int64 `json:"build_row_hint,omitempty"`

	// SemiAntiKeyOnly is set on semi/anti hash_join stages without a
	// SemiAntiFilter — enables key-only build (skip batch storage).
	SemiAntiKeyOnly bool `json:"semi_anti_key_only,omitempty"`

	// Operators carries a multi-operator pipeline the worker runs end-to-end
	// without inter-operator round-trips through S3+NATS. When set, the worker
	// builds an exec.Pipeline from these specs in order: Operators[0] is the
	// source, Operators[len-1] is the sink, and the operators in between are
	// unary transforms. Single-operator stages can be expressed as a single
	// OpSpec; multi-operator fragments (the long-term shape that dissolves
	// the per-operator S3 round-trip floor) carry the full pipeline.
	//
	// Worker dispatch: Execute → executeStage → if len(Operators) > 0
	// → executeFragment; otherwise fall back to the per-StageType handlers
	// (executeStageScan/HashJoin/Aggregate/Sort). The legacy handlers stay
	// alive until every shape has migrated; mixed routing during migration
	// is intentional and safe.
	Operators []OpSpec `json:"operators,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// OpType identifies an operator within a fragment pipeline.
type OpType string

const (
	// Sources (must be first in Operators).
	OpScan          OpType = "scan"           // read parquet/wshf via cachedFileStreamSource
	OpShuffleSource OpType = "shuffle_source" // read partition=NNNN/*.wshf for one partition

	// Unary transforms (middle of the pipeline; zero or more).
	OpFilter         OpType = "filter"          // FilterExprs predicate chain
	OpHashJoinProbe  OpType = "hash_join_probe" // shuffle-side hash join: build from BuildFiles, probe upstream
	OpBroadcastProbe OpType = "broadcast_probe" // broadcast hash join: small build replicated to every task
	OpSortMergeJoin  OpType = "sort_merge_join" // big-vs-big inner join: both sides sort to runs, two-cursor merge (pipeline-breaker)

	// Pipeline-breaker operators. Consume all input from the upstream chain,
	// then emit results into the downstream chain. Splits the fragment into
	// a consume phase (source → preOps → breaker) and a drain phase
	// (breaker → postOps → sink). At most one breaker per fragment today;
	// chained breakers (e.g. aggregate + sort) need a follow-up extension.
	OpHashAggregate OpType = "hash_aggregate" // group-by + aggregates; partial or merge mode
	OpSort          OpType = "sort"           // ordered sort, optional top-N limit

	// Sinks (must be last in Operators).
	OpExchangeSender    OpType = "exchange_sender"    // partitionedShuffleSink: hash-partition into N output files
	OpUnpartitionedSink OpType = "unpartitioned_sink" // unpartitionedStageSink: single .wshf output
	OpGatherSink        OpType = "gather_sink"        // gatherReplySink: stream batches to ReplySubject

	OpProject OpType = "project" // compute SELECT-list expressions (exec.Project); output = exactly Projections
)

// OpSpec describes one operator within a fragment pipeline. Fields are
// optional — populated only for operators of the matching Type. The flat
// shape avoids the JSON-marshal overhead of a discriminated union; the
// worker's executeFragment branches on Type to read the relevant subset.
type OpSpec struct {
	Type OpType `json:"type"`

	// Source operators (OpScan, OpShuffleSource).
	InputAlias     string   `json:"input_alias,omitempty"`  // logical alias for source-column lookup
	InputFiles     []string `json:"input_files,omitempty"`  // S3 keys to read
	InputBucket    string   `json:"input_bucket,omitempty"` // bucket override; falls back to task.DataBucket
	Columns        []string `json:"columns,omitempty"`      // projection hint (parquet column pruning)
	ScanShardIndex int      `json:"scan_shard_index,omitempty"`
	ScanShardCount int      `json:"scan_shard_count,omitempty"`

	// OpFilter.
	Predicates []string `json:"predicates,omitempty"`

	// OpProject.
	Projections []ProjectSpec `json:"projections,omitempty"`

	// OpHashJoinProbe / OpBroadcastProbe.
	JoinType            string   `json:"join_type,omitempty"`  // inner, left, semi, anti, …
	LeftKeys            []string `json:"left_keys,omitempty"`  // probe-side keys
	RightKeys           []string `json:"right_keys,omitempty"` // build-side keys
	BuildAlias          string   `json:"build_alias,omitempty"`
	BuildFiles          []string `json:"build_files,omitempty"`  // build-side input files
	BuildBucket         string   `json:"build_bucket,omitempty"` // bucket override for build files
	JoinFilter          string   `json:"join_filter,omitempty"`
	BuildRowHint        int64    `json:"build_row_hint,omitempty"`
	SemiAntiKeyOnly     bool     `json:"semi_anti_key_only,omitempty"`
	QualifyAllBuildCols bool     `json:"qualify_all_build_cols,omitempty"`
	OutputColumns       []string `json:"output_columns,omitempty"` // OutputFilter for primary probe
	LateMaterialize     bool     `json:"late_materialize,omitempty"` // emit view-column join output (deferred gather)

	// OpExchangeSender (sink).
	ShuffleKeys   []string `json:"shuffle_keys,omitempty"`
	NumPartitions int      `json:"num_partitions,omitempty"`

	// OpGatherSink (sink).
	ReplySubject string `json:"reply_subject,omitempty"`

	// OpHashAggregate (pipeline-breaker).
	GroupByCols  []string  `json:"group_by_cols,omitempty"` // empty = scalar aggregate
	Aggregates   []AggSpec `json:"aggregates,omitempty"`    // per-column aggregations
	GroupByAll   bool      `json:"group_by_all,omitempty"`  // DISTINCT: group by every input column, key set resolved at runtime
	MergeMode    bool      `json:"merge_mode,omitempty"`    // input is already partial-aggregated; rewrite InputCol → OutputCol and COUNT → SUM
	FoldAvg      bool      `json:"fold_avg,omitempty"`      // collapse __avg_sum#X / __avg_count#X synthetics into AVG output (final aggregate only)
	BuildProject bool      `json:"build_project,omitempty"` // construct a derived-input projection before the aggregate (skipped in merge mode — partial output already has OutputCol)

	// OpSort (pipeline-breaker).
	SortKeySpecs []SortKeySpec `json:"sort_key_specs,omitempty"` // ordered key columns
	SortLimit    int           `json:"sort_limit,omitempty"`     // 0 = no limit; > 0 = top-N truncation after sort

	// OpScan (build-side, dynamic-filter producer). Each Emit makes the scan
	// task compute a partial bloom+range over the named column and upload it
	// as a sideband artifact returned in ResultNotification.DynamicFilterPartials.
	DynamicFilterEmits []DynamicFilterEmit `json:"dynamic_filter_emits,omitempty"`

	// OpScan (probe-side, dynamic-filter consumer). Coordinator-materialized
	// stats from the upstream build-scan stage. Worker wires each into the
	// row-group pruning path before the first S3 fetch.
	DynamicFilters []DynamicFilterSpec `json:"dynamic_filters,omitempty"`
}

// PreComputedAggregate identifies a derived aggregate whose result has
// already been computed and cached, paired with the S3 paths of the cache
// files. The worker's plan-rewrite pass matches a logical Aggregate node
// against this signature and replaces it with a scan of CacheFiles.
//
// Signature semantics: Phase 1 matches only aggregates that are
// GROUP BY GroupByCols over a single scan of InputTable (no filters, no
// nested joins). AggSpecs are checked by OutputCol name so the downstream
// column references (e.g. __scalar_0) resolve against the cached rows
// unchanged.
type PreComputedAggregate struct {
	InputTable  string    `json:"input_table"`
	GroupByCols []string  `json:"group_by_cols"`
	AggSpecs    []AggSpec `json:"agg_specs"`
	CacheFiles  []string  `json:"cache_files"`
}

// ProjectSpec is one output column of an OpProject: Name is the emitted
// column, Expr the SQL expression the worker compiles (bare column
// references become passthrough copies). Type is the plan-time inferred
// parquet.TypeID for computed expressions (0 = resolve from the source
// column) — the worker can't infer it from the input schema because the
// output column doesn't exist there.
type ProjectSpec struct {
	Expr string `json:"expr"`
	Name string `json:"name"`
	Type int    `json:"type,omitempty"`
}

// AggSpec defines an aggregation in a task.
type AggSpec struct {
	Func      string `json:"func"` // sum, count, min, max, avg
	InputCol  string `json:"input_col"`
	OutputCol string `json:"output_col"`
	// InputExpr is the SQL text of a derived input expression, e.g.
	// "l_extendedprice * (1 - l_discount)". Empty for bare-column
	// aggregates. Native-DAG workers compile this into a Project
	// before the aggregate so HashAggregate sees a column named
	// InputCol.
	InputExpr string `json:"input_expr,omitempty"`
}

// GatherBatchMsg is the NATS message body the worker publishes to the
// coordinator's gather reply subject. One message per output RecordBatch,
// terminated by one message with Terminal=true (zero RowCount, any Err set).
//
// Payload is a self-contained WSHF byte stream carrying a single chunk
// (magic + chunk-count=1 + schema header + one row chunk). The coordinator
// decodes each message independently via the worker's shuffleChunkReader.
type GatherBatchMsg struct {
	Terminal bool   `json:"terminal"`
	RowCount int32  `json:"row_count"`
	Payload  []byte `json:"payload,omitempty"` // WSHF-encoded single-chunk batch
	Err      string `json:"err,omitempty"`     // non-empty on terminal failure
	// WorkerID lets coord count gather batches as worker-liveness signals
	// (multi-signal liveness — see WorkerRegistry.MarkWorkerSeen). Optional;
	// older workers leave it empty.
	WorkerID string `json:"worker_id,omitempty"`
}

// SortKeySpec defines a sort key in a task.
type SortKeySpec struct {
	Column string `json:"column"`
	Desc   bool   `json:"desc"`
}

// WindowColSpec defines a window function column in a task.
type WindowColSpec struct {
	Func        string        `json:"func"`      // row_number, rank, dense_rank, sum, count, avg, min, max
	InputCol    string        `json:"input_col"` // for aggregate window functions
	OutputCol   string        `json:"output_col"`
	PartitionBy []string      `json:"partition_by,omitempty"`
	OrderBy     []SortKeySpec `json:"order_by,omitempty"`
}

// FusedJoinSpec describes an additional broadcast join absorbed into a task.
// The worker builds the hash table from BuildFiles, then probes each batch
// through this join before passing it to the next fused join (or output).
type FusedJoinSpec struct {
	JoinType        string   `json:"join_type"`
	JoinLeftKeys    []string `json:"join_left_keys"`  // keys from the probe stream
	JoinRightKeys   []string `json:"join_right_keys"` // keys in build files
	BuildFiles      []string `json:"build_files"`     // build-side files (broadcast)
	BuildTableAlias string   `json:"build_table_alias,omitempty"`
	JoinFilter      string   `json:"join_filter,omitempty"`
	FilterExprs     []string `json:"filter_exprs,omitempty"` // post-join filters for this step
}

// ResultNotification is sent by workers when a task completes.
type ResultNotification struct {
	TaskID   string `json:"task_id"`
	QueryID  string `json:"query_id"`
	StageID  string `json:"stage_id"`
	WorkerID string `json:"worker_id"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`

	// Result location
	ResultPath  string   `json:"result_path,omitempty"`
	ResultFiles []string `json:"result_files,omitempty"` // multi-file output (e.g., shuffle per-partition files)
	NumRows     int64    `json:"num_rows"`
	SizeBytes   int64    `json:"size_bytes"`

	// Small result fast path (< 256 KB): inline result data
	InlineData []byte `json:"inline_data,omitempty"`

	// Distributed tracing context (from originating task)
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"` // worker's span for this task

	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`

	// Task execution stats (populated by worker for debugging)
	TaskStats *TaskStats `json:"task_stats,omitempty"`

	// DynamicFilterPartials, populated when the originating task was a
	// build-scan with DynamicFilterEmit set. One ref per emit. Coordinator
	// fetches+unions before dispatching the downstream probe-scan stage.
	DynamicFilterPartials []DynamicFilterPartialRef `json:"dynamic_filter_partials,omitempty"`

	// MissingInputKey (streaming exchange Phase B), set on failure when the
	// task could not resolve an input file that carried a peer-location
	// hint: peer fetch failed AND the durable copy was absent past the
	// bounded re-poll. The coordinator classifies it against the producing
	// worker's liveness and the key's durability bit — producer dead with
	// the key not durable is ErrInputLost (unrecoverable by task retry).
	MissingInputKey string `json:"missing_input_key,omitempty"`
}

// UploadComplete (streaming exchange Phase B) is published by a worker when
// an async-upload task's background S3 uploads have all landed. The
// coordinator flips the per-key durability bits it uses to classify
// missing-input failures and to gate its own direct reads of stage output
// (scalar-subquery extraction).
type UploadComplete struct {
	RootQueryID string   `json:"root_query_id"`
	TaskID      string   `json:"task_id"`
	WorkerID    string   `json:"worker_id"`
	Keys        []string `json:"keys"`
	// Failed marks uploads abandoned after retries (S3 outage) or
	// cancelled (query terminal). Keys stay non-durable; ErrInputLost
	// remains the backstop if the producer also dies.
	Failed bool `json:"failed,omitempty"`
}

// TaskStats captures per-task execution metrics for debugging.
type TaskStats struct {
	MemUsed       int64          `json:"mem_used"`                 // memory tracker usage at completion
	MemBudget     int64          `json:"mem_budget"`               // memory budget for this task
	SpillFiles    int            `json:"spill_files"`              // number of spill files written
	SpillBytes    int64          `json:"spill_bytes"`              // total bytes spilled to disk
	RSS           int64          `json:"rss"`                      // worker process RSS at task completion
	PeakHeapMB    int64          `json:"peak_heap_mb"`             // per-task peak HeapAlloc in MB, captured by atomic-max sampler
	TrackerPeak   int64          `json:"tracker_peak,omitempty"`   // peak of the per-task memory.Tracker (Reserve-tracked bytes)
	OperatorPeaks []OperatorPeak `json:"operator_peaks,omitempty"` // per-Spillable peak attribution at task end
	// Phase-4 accounting observability (additive, omitempty; the coordinator
	// reads neither — diagnostic only).
	DriftMB   int64 `json:"drift_mb,omitempty"`    // HeapInuse − (operator owned + reservoir actual) at task end
	MmapRSSMB int64 `json:"mmap_rss_mb,omitempty"` // max(0, RSS − HeapInuse): non-heap resident working set
}

// OperatorPeak is one entry in TaskStats.OperatorPeaks. Mirrors
// memory.SpillableSnapshot but lives here to avoid a memory→distributed
// package dependency. Populated for each Spillable registered with the
// task's SpillManager at the moment collectTaskStats fires.
type OperatorPeak struct {
	Name    string `json:"name"`    // operator instance name
	Peak    int64  `json:"peak"`    // high-water mark of OwnedBytes
	Current int64  `json:"current"` // SpillableBytes at snapshot time (reclaimable now)
	// Phase-2 AccountedOperator fields. Additive (omitempty) — gob skips them
	// for old encoders, JSON omits when zero — so this stays wire-compatible
	// with mixed-version peers. The coordinator reads none of OperatorPeaks.
	Owned    int64  `json:"owned,omitempty"`    // OwnedBytes incl. operator overhead
	Retained int64  `json:"retained,omitempty"` // RetainedBytes (detained batches)
	State    string `json:"state,omitempty"`    // OpState string
}

// WorkerHeartbeat is periodically sent by workers.
type WorkerHeartbeat struct {
	WorkerID      string    `json:"worker_id"`
	ClusterID     string    `json:"cluster_id,omitempty"`     // cluster this worker belongs to
	MaxConcurrent int       `json:"max_concurrent,omitempty"` // worker's effective task slot count (after auto-tuning); 0 = unknown
	ActiveTasks   int       `json:"active_tasks"`
	ActiveTaskIDs []string  `json:"active_task_ids,omitempty"` // task IDs currently executing
	MemoryUsed    int64     `json:"memory_used"`
	MemoryTotal   int64     `json:"memory_total"`
	PoolUsed      int64     `json:"pool_used,omitempty"`   // bytes Reserved in the worker's shared memory pool
	PoolBudget    int64     `json:"pool_budget,omitempty"` // shared memory pool capacity in bytes; pressure = PoolUsed/PoolBudget
	RSS           int64     `json:"rss,omitempty"`         // process RSS from /proc/self/status
	NumGoroutines int       `json:"num_goroutines,omitempty"`
	Mallocs       uint64    `json:"mallocs,omitempty"`         // cumulative allocation count from runtime.MemStats
	SpillDiskUsed int64     `json:"spill_disk_used,omitempty"` // bytes used in spill directory
	Draining      bool      `json:"draining,omitempty"`        // true when worker is draining
	// PeerAddr is the worker's dialable peer-exchange (FetchShuffle)
	// address. Empty when the worker doesn't serve peer fetches — the
	// coordinator then simply emits no location hints referencing it,
	// which makes mixed-version/mixed-flag rollouts self-gating.
	PeerAddr  string    `json:"peer_addr,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// TaskProgress is published by a worker from inside a task's hot loop
// to signal forward progress (rows/bytes processed). Coord uses these
// to distinguish a slow-but-healthy task from a wedged task.
//
// Workers emit at most one TaskProgress message per ~2s per task; the
// counters are monotonically increasing across the task's lifetime,
// so the coord can compute throughput and detect "no row progress for
// N seconds" stalls without needing every batch to publish.
type TaskProgress struct {
	QueryID        string    `json:"query_id"`
	StageID        string    `json:"stage_id"`
	TaskID         string    `json:"task_id"`
	WorkerID       string    `json:"worker_id"`
	RowsProcessed  int64     `json:"rows_processed"`
	BytesProcessed int64     `json:"bytes_processed,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

// QueryManifest describes the final results of a query.
type QueryManifest struct {
	QueryID     string   `json:"query_id"`
	ResultFiles []string `json:"result_files"`
	TotalRows   int64    `json:"total_rows"`
	TotalBytes  int64    `json:"total_bytes"`
}

// DLQEntry records a failed task for inspection and potential retry.
type DLQEntry struct {
	EntryID   string    `json:"entry_id"`
	TaskID    string    `json:"task_id"`
	QueryID   string    `json:"query_id"`
	StageID   string    `json:"stage_id"`
	WorkerID  string    `json:"worker_id"`
	TaskType  TaskType  `json:"task_type"`
	Error     string    `json:"error"`
	Reason    string    `json:"reason"`              // "execution_error", "panic", "marshal_error", "publish_error"
	TaskData  []byte    `json:"task_data,omitempty"` // original task JSON for replay
	Timestamp time.Time `json:"timestamp"`
}

// Marshal serializes a message. Uses gob for ResultNotification (avoids
// base64 overhead on InlineData), JSON for everything else.
func Marshal(v any) ([]byte, error) {
	if _, ok := v.(ResultNotification); ok {
		var buf bytes.Buffer
		buf.WriteByte('G') // format tag: gob
		if err := gob.NewEncoder(&buf).Encode(v); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return json.Marshal(v)
}

// Unmarshal deserializes a message. Auto-detects gob vs JSON format.
func Unmarshal(data []byte, v any) error {
	if len(data) > 0 && data[0] == 'G' {
		return gob.NewDecoder(bytes.NewReader(data[1:])).Decode(v)
	}
	return json.Unmarshal(data, v)
}
