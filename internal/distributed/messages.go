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

// UploadPolicy is the shuffle-durability mode a task's stage-output uploads
// run under (docs/design/shuffle-durability.md). Carried per task so the
// policy survives mixed-version clusters: workers that predate the field
// unmarshal it away and upload eagerly, which is always safe.
type UploadPolicy string

const (
	UploadEager UploadPolicy = ""     // background upload starts immediately (default)
	UploadLazy  UploadPolicy = "lazy" // queue unstarted; run on release/drain; elide at query end
	UploadOff   UploadPolicy = "off"  // never upload scratch; rely on peers + whole-query re-execution
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
	// DegradedMemory is a WORKER-LOCAL flag, never serialized: the poison-
	// task defense (#318) sets it before executing a redelivery whose prior
	// attempt coincided with a worker death. The executor then wires the
	// task's operators to a reduced-budget spill view so the never-OOM
	// machinery (ADR-0006: spill early, degrade gracefully) engages far
	// below the heap ceiling that killed the previous attempt.
	DegradedMemory bool `json:"-"`
	// EstimatedBytes is the coordinator's estimate of this task's input
	// footprint, used for memory-aware admission (worker holds the task
	// start until the shared pool has room for it) and coordinator-side
	// bin-packing. 0 = unknown — admission and placement fall back to
	// pressure-threshold / round-robin behavior. Doubled on every
	// re-dispatch (grow-on-retry, the Trino FTE pattern): a task whose
	// worker died possibly-of-memory is only admitted where strictly more
	// headroom exists.
	EstimatedBytes int64 `json:"estimated_bytes,omitempty"`
	// Priority routes the task onto the latency-critical lane
	// (SubjectPriTasksAll → dedicated worker slots outside MaxConcurrent).
	// Set only for dimension-class tasks whose completion unblocks bulk
	// work (dyn-filter emitter scans); their smallness is enforced by the
	// planner passes that mark them.
	Priority bool `json:"priority,omitempty"`
	// PriorityDeep sub-classes the priority lane: emitter tasks that ALSO
	// consume dynamic filters (guarded re-emit mid-scans) ride a slot pool
	// SEPARATE from leaf emitters. A guarded task blocks at finalize until
	// its consumed bloom settles; if it could occupy the slots its own
	// upstream leaf emitter needs, the lane deadlocks until the poll
	// deadline (observed SF100 2026-08-07, trt ead0976: Q07 stalled ~10min,
	// hop-B held both lane slots waiting on the dim task queued behind it).
	// Class-disjoint pools make the circular wait structurally impossible,
	// cross-query included.
	PriorityDeep bool `json:"priority_deep,omitempty"`

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
	Columns []string `json:"columns,omitempty"`
	// ColumnTypes is the CATALOG's declared schema for the relation a
	// SHUFFLE task reads, when that task's Files are base-table parquet (a
	// pass-through leaf scan absorbed into the exchange). Its own footer
	// cannot express nine of this engine's types if it was written before
	// v0.18.0, so without this the shuffle re-encodes an IPv4 column as the
	// INT64 it is stored in and every consumer downstream sees raw storage
	// form (#423). Empty for a shuffle over stage output, which carries its
	// own types in the WSHF payload. Fragment tasks declare this per input
	// instead, on OpSpec.ColumnTypes.
	ColumnTypes []ColumnSpec `json:"column_types,omitempty"`
	FilterExprs []string     `json:"filter_exprs,omitempty"` // SQL filter expressions for pushdown
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

	// RowLimit bounds how many rows this task emits, for a LIMIT with no
	// ORDER BY (physical.Stage.RowLimit). The task stops pulling once
	// satisfied; the coordinator trims the union of tasks to the real limit.
	RowLimit int `json:"row_limit,omitempty"`

	// Sort-specific
	SortKeys       []SortKeySpec `json:"sort_keys,omitempty"`
	Limit          int           `json:"limit,omitempty"`
	MergePreSorted bool          `json:"merge_pre_sorted,omitempty"` // true for merge_sort: inputs are pre-sorted
	MergePartials  bool          `json:"merge_partials,omitempty"`   // true for final_aggregate: re-aggregate partial results

	// Join-specific
	JoinType      string   `json:"join_type,omitempty"`       // inner, left, right, full, cross
	JoinLeftKeys  []string `json:"join_left_keys,omitempty"`  // probe side key columns
	JoinRightKeys []string `json:"join_right_keys,omitempty"` // build side key columns
	// JoinKeyTypes[i] is the resolved COMMON type (parquet.TypeID as int) of
	// the pair (JoinLeftKeys[i], JoinRightKeys[i]) — PostgreSQL's operator
	// resolution over the two sides' declared types. Both sides' key bytes
	// are built at it and the integer / bloom fast paths are gated on it,
	// which is what makes `a.i = b.d` a join rather than a panic (#615,
	// ADR-0023). -1 or absent means "no widening", the answer for every
	// same-type join and for an older coordinator.
	JoinKeyTypes    []int    `json:"join_key_types,omitempty"`
	BuildFiles      []string `json:"build_files,omitempty"`       // build (right) side input files
	BuildTableAlias string   `json:"build_table_alias,omitempty"` // build-side alias for column disambiguation
	// QualifyAllBuildCols, when true, forces the join executor to emit
	// build-side columns under their qualified name even when no probe-side
	// column has the same base name. Set by the planner for self-join scenarios
	// (Q07's two scans of nation that co-path into the same join chain).
	QualifyAllBuildCols bool `json:"qualify_all_build_cols,omitempty"`
	// BuildColOrigins maps bare build-column names (lowercased) to their
	// owning scan alias. Set only for multi-table build subtrees (bushy
	// shapes); the executor qualifies duplicate build columns with the
	// owning alias instead of BuildTableAlias.
	BuildColOrigins map[string]string `json:"build_col_origins,omitempty"`
	JoinFilter      string            `json:"join_filter,omitempty"` // semi/anti join inequality filter expression
	// BuildFilterExprs filter the build input rows before hash-table
	// insertion (exchange subsumption dedup).
	BuildFilterExprs []string `json:"build_filter_exprs,omitempty"`
	// JoinBuildSchema / JoinProbeSchema are the plan-declared columns of the
	// two join sides, used only when a side is empty (see OpSpec.BuildSchema).
	JoinBuildSchema []ColumnSpec `json:"join_build_schema,omitempty"`
	JoinProbeSchema []ColumnSpec `json:"join_probe_schema,omitempty"`

	// Fused join: additional broadcast joins absorbed into a single task.
	// The worker builds hash tables for each fused join, then chains probes
	// batch-by-batch: probe → join1 → join2 → ... → output.
	FusedJoins []FusedJoinSpec `json:"fused_joins,omitempty"`

	// Shuffle-specific
	ShuffleKeys   []string `json:"shuffle_keys,omitempty"`   // columns to hash-partition on
	NumPartitions int      `json:"num_partitions,omitempty"` // number of output partitions
	// ShuffleKeyTypes[i] is the type ShuffleKeys[i] must be HASHED at
	// (parquet.TypeID as int), which for a join's exchange-repartition is
	// the key pair's resolved common type. Hashing at each side's own width
	// sends equal values to different partitions and the shuffle join then
	// matches none of them — the same defect as an unwidened key, one layer
	// down (#615). -1 or absent means "hash at the column's own type".
	ShuffleKeyTypes []int `json:"shuffle_key_types,omitempty"`
	// ComputedCols are expression columns appended to the shuffle payload
	// after the projected scan columns (exchange subsumption dedup: a
	// dropped filtered sibling's filter ships as a computed flag).
	ComputedCols []ComputedColSpec `json:"computed_cols,omitempty"`
	// DropCols are read-only helper columns (ComputedCols expression
	// inputs) removed from the payload after the flags are computed.
	DropCols []string `json:"drop_cols,omitempty"`
	// PartialAggKeys/PartialAggSpecs enable sender-side partial
	// aggregation inside the shuffle task (exchange partial agg): rows
	// are pre-combined on PartialAggKeys with name-preserving
	// SUM/MIN/MAX specs (OutputCol == InputCol) before partitioning.
	// Only set when the planner proved every consumer of the exchange
	// merge-compatible; the reduction is exchange-internal and invisible
	// downstream.
	PartialAggKeys  []string  `json:"partial_agg_keys,omitempty"`
	PartialAggSpecs []AggSpec `json:"partial_agg_specs,omitempty"`
	PartitionID     int       `json:"partition_id,omitempty"` // which partition this join task handles

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

	// AffinityWorkerID (docs/design/scan-affinity.md) is the
	// rendezvous-hash owner of this task's base-table files (scan
	// fan-outs and probe-split broadcast joins): the worker whose NVMe
	// base-table cache canonically holds them. Placement PREFERENCE
	// only — the scheduler falls through to binpack/round-robin when
	// the worker is absent or the same-batch cap bites, and a task
	// placed elsewhere just misses the cache exactly as before.
	AffinityWorkerID string `json:"affinity_worker_id,omitempty"`

	// EagerInputs maps an input alias to its eager manifest-feed
	// descriptor (docs/design/eager-consumer-dispatch.md). When an alias
	// appears here, the worker builds a manifest-fed source for it
	// instead of consuming a frozen file list; other aliases of the same
	// task keep their explicit Inputs entries.
	EagerInputs map[string]EagerInput `json:"eager_inputs,omitempty"`

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

	// UploadPolicy (docs/design/shuffle-durability.md) refines AsyncUpload:
	// how urgently the durable S3 copy of this task's stage outputs must
	// exist. Empty = eager (background upload starts immediately —
	// pre-knob behavior, and what workers that predate the field do).
	// "lazy" = the worker queues the upload jobs unstarted and runs them
	// only on a demand signal (SubjectUploadRelease broadcast or worker
	// drain); jobs still queued when the query completes are elided.
	// "off" = never upload; producer death before consumption degrades to
	// the coordinator's one-shot streaming-disabled re-execution.
	// Only meaningful when AsyncUpload is true; the coordinator keeps
	// stages whose outputs it reads itself (scalar-subquery producers) on
	// eager, because the coordinator has no peer tier.
	UploadPolicy UploadPolicy `json:"upload_policy,omitempty"`

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

	// NullAwareAnti marks an anti-join stage that came from a NOT IN
	// subquery and owes its three-valued rule (#507). See
	// exec.HashJoin.NullAwareAnti.
	NullAwareAnti bool `json:"null_aware_anti,omitempty"`

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

	// DeleteMarkers carries the merge-on-read DELETE state for every
	// base-table parquet file this task reads, under whichever carrier the
	// file arrives on — Files, Inputs, PreScannedInputs, ScanFileFilter,
	// BuildFiles, FusedJoins[].BuildFiles or Operators[].{InputFiles,
	// BuildFiles}. One task-level list rather than a field per carrier,
	// because the marker is a property of the FILE, not of the alias that
	// happens to read it: a self-join reading one file twice must skip the
	// same rows on both sides, and the coordinator cannot enumerate the
	// carriers a future dispatcher will invent. Stamped centrally in
	// Scheduler.PublishTasks from the plan's one manifest read, so every
	// task of a query — retries included — sees ONE catalog revision's
	// delete state (#491).
	//
	// A DELETE records the file-absolute row indices it removed rather than
	// rewriting parquet; a scan that ignores them answers with the deleted
	// rows still in it. The single-process engine reads the manifest
	// itself; the DAG's worker has no business doing so (two tasks of one
	// stage could then read different revisions and a join would see a row
	// on one side and not the other), so the plan declares it — the same
	// argument as ColumnTypes and AggSpec.OutputType.
	//
	// Empty for every task over a table with no deletes, which is the
	// common case and costs nothing on the wire.
	//
	// ADR-0010 WHOLESALE-DEPLOY RULE: a worker that predates this field
	// unmarshals it away and answers with the deleted rows — silently, and
	// only for the fraction of a stage's tasks that landed on the old
	// binary. Coordinator and workers deploy together, never rolling, for
	// exactly the reason the partition-assignment function does.
	DeleteMarkers []DeleteSpec `json:"delete_markers,omitempty"`

	CreatedAt time.Time `json:"created_at"`
}

// DeleteSpec is one base-table file's merge-on-read delete markers, in the
// compact form scan.EncodeDeleteRuns produces: varint (gap, length) pairs
// over the file-absolute deleted row indices, coalesced into runs.
//
// The runs encoding is not decoration. The manifest holds the same set as a
// JSON array of decimal indices (~8 B each); a DELETE over a clustered
// predicate marks contiguous rows, which collapse to 2 varints however many
// rows they cover, and scattered deletes cost ~2 B each. That keeps a task
// spec well under the 8 MB NATS payload cap for any marker set the catalog
// manifest itself can hold — see the spec-size table in
// docs/internals/native-dag-execution.md.
type DeleteSpec struct {
	File string `json:"file"`
	Runs []byte `json:"runs"`
}

// OpType identifies an operator within a fragment pipeline.
type OpType string

const (
	// Sources (must be first in Operators).
	OpScan          OpType = "scan"           // read parquet/wshf via cachedFileStreamSource
	OpShuffleSource OpType = "shuffle_source" // read partition=NNNN/*.wshf for one partition

	// Unary transforms (middle of the pipeline; zero or more).
	OpFilter         OpType = "filter"          // FilterExprs predicate chain
	OpColumnPrune    OpType = "column_prune"    // drop columns not in OutputColumns (exec.ColumnPrune, zero-copy)
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
	// OpWindow computes window function columns (exec.Window). The operator
	// partitions and sorts its own input, so the fragment does NOT need an
	// OpSort ahead of it: a window's ORDER BY defines the frame, not the
	// stream, and exec.Window orders each partition itself.
	OpWindow OpType = "window"

	// Sinks (must be last in Operators).
	OpExchangeSender    OpType = "exchange_sender"    // partitionedShuffleSink: hash-partition into N output files
	OpUnpartitionedSink OpType = "unpartitioned_sink" // unpartitionedStageSink: single .wshf output
	OpGatherSink        OpType = "gather_sink"        // gatherReplySink: stream batches to ReplySubject

	OpProject OpType = "project" // compute SELECT-list expressions (exec.Project); output = exactly Projections

	// OpSetOpEmit turns a grouped-count batch into an INTERSECT/EXCEPT
	// answer (exec.SetOpEmit): each input row is one distinct result row
	// plus its per-arm multiplicities in the two count columns; the
	// operator emits k copies per the operation's rule (distinct forms:
	// 0 or 1; INTERSECT ALL: min(a,b); EXCEPT ALL: max(0, a−b)) and drops
	// the count columns. Sits immediately after the counting
	// OpHashAggregate in a set-operation final_aggregate fragment.
	OpSetOpEmit OpType = "set_op_emit"

	// OpLimit applies OFFSET then LIMIT to the whole stream (exec.Limit).
	// It runs in a StageLimit fragment, which the planner makes Singleton
	// precisely so this operator sees every row: a per-task LIMIT n over N
	// tasks is not a global LIMIT n. See physical.StageLimit (#478).
	OpLimit OpType = "limit"

	// OpDecimalCoerce moves the named columns into one declared
	// DECIMAL(p,s) (exec.DecimalCoerce), rescaling the unscaled carrier
	// rather than reinterpreting it. It runs in a union-stage fragment,
	// right after the projection that puts the arm's columns under the set
	// operation's result names, so every arm's .wshf file declares the same
	// scale for the same column (#533).
	//
	// A worker that does not know this op FAILS the task by name rather
	// than ignoring it, which is the right side of ADR-0010's rolling-deploy
	// test: ignoring it would answer with values off by a power of ten.
	OpDecimalCoerce OpType = "decimal_coerce"
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
	// ColumnTypes is the CATALOG's declared schema for the scanned table —
	// the types beside the names Columns carries (#423).
	//
	// A parquet file cannot express nine of this engine's types (IPv4, IPv6,
	// MAC, UUID, BYTES, PORT, PROTOCOL, DURATION and CIDR), so a scan that
	// takes its types from the FILE reads them back as the plain INT64 /
	// BYTE_ARRAY leaves they are stored in and answers 167772165 where the
	// single-process engine answers 10.0.0.5. Files written from v0.18.0 on
	// carry the declared types in their own footer and need nothing from
	// here; files written before it carry no such key, and this is the only
	// thing that tells the worker what they hold.
	//
	// Set only on a base-table OpScan. The worker applies it through the
	// same admission as the row reader (parquet.Reader.SchemaAs →
	// retypeFromCatalog), so a declaration the file's bytes cannot carry
	// fails the task by name instead of decoding one type as another.
	ColumnTypes []ColumnSpec `json:"column_types,omitempty"`

	// OpFilter.
	Predicates []string `json:"predicates,omitempty"`

	// ScanSchemaFilter marks an OpFilter that reads a BASE-TABLE SCAN's
	// output directly, so its input schema is the catalog's declaration and
	// a predicate naming something that is not in it names nothing at all.
	// The worker turns that into a query error rather than an UNKNOWN on
	// every row (the #147 rule, which only the vectorized filter had — see
	// expr.CheckFilterColumns and #653).
	//
	// It is set ONLY there. A filter above a JOIN cannot be checked the same
	// way: a hash-join partition with an EMPTY build side emits its probe
	// rows with only the join KEYS declared for the missing side, so a
	// legitimately-NULL build column is absent from that batch's schema —
	// TPC-H Q20's `ps_availqty > 0.5 * __scalar_0` over a LEFT join is
	// exactly that shape, right on every task and schema-less on the empty
	// ones. Absent from an older coordinator's spec, which reads as false:
	// the pre-guard behavior.
	ScanSchemaFilter bool `json:"scan_schema_filter,omitempty"`

	// OpProject.
	Projections []ProjectSpec `json:"projections,omitempty"`

	// OpDecimalCoerce. Each entry's Type is always parquet's DECIMAL id;
	// Precision and Scale are the set operation's reconciled output type
	// for that column (#533).
	Coercions []ColumnSpec `json:"coercions,omitempty"`

	// OpHashJoinProbe / OpBroadcastProbe.
	JoinType  string   `json:"join_type,omitempty"`  // inner, left, semi, anti, …
	LeftKeys  []string `json:"left_keys,omitempty"`  // probe-side keys
	RightKeys []string `json:"right_keys,omitempty"` // build-side keys
	// KeyTypes is Task.JoinKeyTypes for this op — see there.
	KeyTypes    []int    `json:"key_types,omitempty"`
	BuildAlias  string   `json:"build_alias,omitempty"`
	BuildFiles  []string `json:"build_files,omitempty"`  // build-side input files
	BuildBucket string   `json:"build_bucket,omitempty"` // bucket override for build files
	// BuildColumnTypes is ColumnTypes for the BUILD side: the catalog's
	// declared schema for BuildFiles when those are base-table parquet — a
	// pass-through leaf scan feeding a join, which reads the table directly
	// instead of an upstream stage's WSHF. Same reason and same admission
	// as ColumnTypes (#423). Empty when the build reads stage output.
	BuildColumnTypes []ColumnSpec `json:"build_column_types,omitempty"`
	JoinFilter       string       `json:"join_filter,omitempty"`
	BuildRowHint     int64        `json:"build_row_hint,omitempty"`
	SemiAntiKeyOnly  bool         `json:"semi_anti_key_only,omitempty"`
	// NullAwareAnti marks an anti-join op that came from a NOT IN and must
	// answer its three-valued rule: a NULL probe key never survives, and a
	// NULL anywhere in the build makes the whole answer empty (#507).
	NullAwareAnti bool `json:"null_aware_anti,omitempty"`
	// BuildFilterExprs filter the BUILD input rows before hash-table
	// insertion (exchange subsumption dedup: the dropped exchange's scan
	// filter — or its computed flag column — applied at build read).
	BuildFilterExprs    []string          `json:"build_filter_exprs,omitempty"`
	QualifyAllBuildCols bool              `json:"qualify_all_build_cols,omitempty"`
	BuildColOrigins     map[string]string `json:"build_col_origins,omitempty"` // bare build col → owning scan alias (multi-table builds only)
	OutputColumns       []string          `json:"output_columns,omitempty"`    // OutputFilter for primary probe
	LateMaterialize     bool              `json:"late_materialize,omitempty"`  // emit view-column join output (deferred gather)
	// BuildSchema / ProbeSchema are the plan-declared columns of each side,
	// read ONLY when that side turns out to be empty — an outer join still
	// owes the rows the empty side shapes and cannot name their columns
	// otherwise. Same idea as AggSpec.OutputType (#329): declare on the wire
	// what the worker can no longer read off a batch. See
	// physical.declaredJoinSchema, exec.HashJoin.BuildSchemaHint.
	BuildSchema []ColumnSpec `json:"build_schema,omitempty"`
	ProbeSchema []ColumnSpec `json:"probe_schema,omitempty"`

	// OpExchangeSender (sink).
	ShuffleKeys   []string `json:"shuffle_keys,omitempty"`
	NumPartitions int      `json:"num_partitions,omitempty"`
	// ShuffleKeyTypes is Task.ShuffleKeyTypes for this op — see there.
	ShuffleKeyTypes []int `json:"shuffle_key_types,omitempty"`

	// OpGatherSink (sink).
	ReplySubject string `json:"reply_subject,omitempty"`

	// OpHashAggregate (pipeline-breaker).
	GroupByCols []string `json:"group_by_cols,omitempty"` // empty = scalar aggregate
	// GroupByTypes carries the plan-time parquet.TypeID (as int) of each
	// DERIVED GROUP BY key expression, keyed by the exact key text in
	// GroupByCols. Carried for AggSpec.InputType's reason (#333): the
	// worker compiles a derived key from its SQL text and has no catalog
	// to resolve the columns in it, so a schema-blind inference typed
	// COALESCE(l_extendedprice, 0) Int64 from the literal alone and the
	// pre-aggregate projection truncated every float price into the key
	// vector — a fifth of the distinct groups silently vanished, on the
	// DAG only (#379). A key absent from the map (bare column keys, older
	// coordinators) keeps the worker's own inference and fallback.
	GroupByTypes map[string]int `json:"group_by_types,omitempty"`
	// GroupByDecimal carries the (precision, scale) of the DECIMAL entries
	// in GroupByTypes, keyed the same way. A bare TypeID is not a type for a
	// DECIMAL: the worker builds the key vector from the declaration alone,
	// and a DECIMAL vector with no scale TRUNCATES every value written into
	// it — `GROUP BY COALESCE(a, b)` over DECIMAL(9,2)/DECIMAL(18,4)
	// collapsed 12.7500 and 12.7501 into one group holding 12, on the DAG
	// only. Exactly #379's shape, one type over (ADR-0024 item 2).
	GroupByDecimal map[string]DecimalMeta `json:"group_by_decimal,omitempty"`
	Aggregates     []AggSpec              `json:"aggregates,omitempty"`    // per-column aggregations
	GroupByAll     bool                   `json:"group_by_all,omitempty"`  // DISTINCT: group by every input column, key set resolved at runtime
	MergeMode      bool                   `json:"merge_mode,omitempty"`    // input is already partial-aggregated; rewrite InputCol → OutputCol and COUNT → SUM
	FoldAvg        bool                   `json:"fold_avg,omitempty"`      // collapse __avg_sum#X / __avg_count#X synthetics into AVG output (final aggregate only)
	BuildProject   bool                   `json:"build_project,omitempty"` // construct a derived-input projection before the aggregate (skipped in merge mode — partial output already has OutputCol)
	// EmitEmptyIdentity marks THE aggregate whose one row is the query's
	// answer for these aggregates: the ungrouped final. SQL gives an
	// ungrouped aggregate exactly one row over any input including none
	// (SUM over the empty set is NULL, COUNT is 0), so when this
	// fragment's input turns out to be empty the worker must still build
	// the pipeline and let the aggregate finalize its identity row
	// instead of short-circuiting to zero output.
	//
	// Set only on the ungrouped final_aggregate, which the planner
	// distributes as a Singleton — exactly one task, so exactly one
	// identity row. Partial and merge_aggregate stages leave it false:
	// their empty output is absorbed by the final above them, and an
	// identity partial among typed siblings is what the merge cannot
	// take (see AggSpec.OutputType).
	EmitEmptyIdentity bool `json:"emit_empty_identity,omitempty"`

	// InputRowBound is an EXACT upper bound on the rows this aggregate task
	// will read: the sum of the upstream stage's reported PartitionRows over
	// the partitions bound to this task. It is not an estimate and not a
	// presize hint — the worker feeds it to
	// exec.HashAggregate.SetInputRowBound, which decides the group-index
	// LAYOUT from it (a flat→bucketed conversion is repaid only by the rows
	// that follow it; see exec/two_level_hash.go twoLevelAmortizeMultiple).
	// 0 = unknown (non-partitioned input, legacy coordinator, a task whose
	// inputs were assigned by skew/probe/round-robin splitting rather than
	// by partition range) — the aggregate keeps the adaptive path.
	InputRowBound int64 `json:"input_row_bound,omitempty"`

	// OpSort (pipeline-breaker). SortLimit is meaningful only when
	// HasSortLimit is true — a companion bool rather than folding "no
	// limit" into SortLimit's own zero, because `omitempty` already drops
	// SortLimit off the wire when it's 0, so a genuine `LIMIT 0` and "no
	// limit at all" were indistinguishable once the worker decoded them
	// (#481: a distributed `ORDER BY ... LIMIT 0` truncated nothing).
	// HasSortLimit's own `omitempty` still elides the common unbounded
	// case from the wire, since false is its zero value.
	SortKeySpecs []SortKeySpec `json:"sort_key_specs,omitempty"` // ordered key columns
	SortLimit    int           `json:"sort_limit,omitempty"`     // top-N truncation after sort; valid iff HasSortLimit
	HasSortLimit bool          `json:"has_sort_limit,omitempty"` // true when SortLimit is a real bound (may be 0)

	// OpWindow (pipeline-breaker). One entry per window column; the
	// operator appends them to its input's columns in this order.
	WindowCols []WindowColSpec `json:"window_cols,omitempty"`

	// OpWindow. The PARTITION BY / window ORDER BY terms the fragment must
	// COMPUTE before the operator can key on them, each named by the term's
	// own text — the same name WindowColSpec.PartitionBy/OrderBy carry, so
	// the projection and the operator need no second naming convention.
	// One list for the whole operator, not one per column: two OVER clauses
	// routinely share a key, and computing it twice would put two columns of
	// one name on the batch.
	//
	// A window key that is an expression named no column anywhere, and
	// exec.Window's name lookup used to SKIP what it could not find — so
	// `PARTITION BY id % 3` ran the window over one partition spanning the
	// input and answered a different query (#585). This is the aggregate's
	// AggSpec.InputExpr route, one operator over.
	WindowKeyExprs []ProjectSpec `json:"window_key_exprs,omitempty"`

	// OpScan (build-side, dynamic-filter producer). Each Emit makes the scan
	// task compute a partial bloom+range over the named column and upload it
	// as a sideband artifact returned in ResultNotification.DynamicFilterPartials.
	DynamicFilterEmits []DynamicFilterEmit `json:"dynamic_filter_emits,omitempty"`

	// OpScan (probe-side, dynamic-filter consumer). Coordinator-materialized
	// stats from the upstream build-scan stage. Worker wires each into the
	// row-group pruning path before the first S3 fetch.
	DynamicFilters []DynamicFilterSpec `json:"dynamic_filters,omitempty"`

	// OpLimit. HasLimitCount discriminates a real `LIMIT 0` from "no LIMIT
	// at all" for the same reason HasSortLimit does (#481): omitempty drops
	// a zero LimitCount off the wire, so the two would decode identically.
	// LimitOffset needs no companion — skipping zero rows and having no
	// OFFSET are the same thing.
	LimitCount    int  `json:"limit_count,omitempty"`
	LimitOffset   int  `json:"limit_offset,omitempty"`
	HasLimitCount bool `json:"has_limit_count,omitempty"`

	// OpSetOpEmit.
	SetOp         string `json:"set_op,omitempty"`          // "intersect" | "except"
	SetOpAll      bool   `json:"set_op_all,omitempty"`      // multiset (ALL) form
	SetOpLeftCol  string `json:"set_op_left_col,omitempty"` // arm-A count column, dropped from output
	SetOpRightCol string `json:"set_op_right_col,omitempty"`
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

// ColumnSpec is one column of a plan-declared schema on the wire: the name
// and the parquet.TypeID as an int, matching AggSpec.OutputType's encoding.
//
// Precision, Scale and Dimension are the parameters a bare TypeID does not
// carry — DECIMAL's two, VECTOR's one. A schema declared for a SCAN needs
// them (OpSpec.ColumnTypes, #423): the reader allocates a VECTOR's storage
// from its dimension and renders a DECIMAL from its scale, so a spec that
// dropped them would declare a type the worker cannot build. They are
// omitempty and zero for every other type, so the join-side declarations
// (BuildSchema / ProbeSchema) encode exactly as they did before.
type ColumnSpec struct {
	Name      string `json:"name"`
	Type      int    `json:"type"`
	Precision int    `json:"precision,omitempty"`
	Scale     int    `json:"scale,omitempty"`
	Dimension int    `json:"dimension,omitempty"`
}

// ProjectSpec is one output column of an OpProject: Name is the emitted
// column, Expr the SQL expression the worker compiles (bare column
// references become passthrough copies). Type is the plan-time inferred
// parquet.TypeID for a computed expression — the worker can't infer it from
// the input schema because the output column doesn't exist there.
//
// A POINTER, like AggSpec.OutputType (#354) and WindowColSpec.OutputType
// (#371): a plain int Type collided with parquet.TypeBool's zero value, so a
// correctly-inferred BOOL expression (a comparison, LIKE, a boolean literal)
// was indistinguishable from "never resolved" and buildSelectProjection
// defaulted it to STRING, boxing the value as the text "true"/"false" on the
// wire (#445). nil means "resolve from the source column" (a bare passthrough,
// which the worker resolves by DirectCopy and never consults this for).
type ProjectSpec struct {
	Expr string `json:"expr"`
	Name string `json:"name"`
	Type *int   `json:"type,omitempty"`
	// Precision and Scale carry a computed DECIMAL's declaration. A bare
	// TypeID is not a type for a DECIMAL: the worker builds the output
	// vector from Type alone, and a DECIMAL vector with no scale reads every
	// value back at 10^0 (ADR-0024 item 2; the same reason ColumnSpec and
	// DecimalCoercion carry the pair). Zero means "not a DECIMAL, or the
	// planner could not resolve one" — the #458 unconstrained sentinel.
	Precision int `json:"precision,omitempty"`
	Scale     int `json:"scale,omitempty"`
}

// DecimalMeta is a DECIMAL's declared (precision, scale) on the wire — the
// two facts a bare TypeID cannot express (ADR-0024 item 2). It is the wire
// twin of logical.DecimalMeta and of ColumnSpec's own Precision/Scale pair,
// for the places that carry a TYPE map rather than a column list.
type DecimalMeta struct {
	Precision int `json:"precision,omitempty"`
	Scale     int `json:"scale,omitempty"`
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
	// OutputType is the plan-time parquet.TypeID of this aggregate's
	// output column, carried as a plain int so the wire package stays free
	// of the storage dependency.
	//
	// Nil means "the planner did not declare one" — either an older
	// coordinator that predates the field, or a MIN/MAX whose input
	// column the planner could not resolve to a catalog type. Workers
	// fall back to deriving the type from Func alone in that case, and
	// any decision that needs a TRUSTWORTHY type (emitting an ungrouped
	// aggregate's identity row over zero input, where there is no input
	// schema to read it from) must decline rather than guess.
	//
	// COUNT-family, SUM and AVG are input-independent in this engine, so
	// the planner always declares them. MIN/MAX follow their input
	// column, and are declared only when it resolves to exactly one
	// catalog column type.
	//
	// A POINTER since #354, for WindowColSpec.OutputType's reason:
	// parquet.TypeID's zero value is BOOL, so the plain int this used to be
	// could not tell a declared BOOL_AND/BOOL_OR output from an absent
	// declaration — the DAG read it as undeclared and fell back to a guess,
	// reinstating #345's silent-drop shape for exactly one type.
	OutputType *int `json:"output_type,omitempty"`
	// OutputPrecision/OutputScale carry a DECIMAL OutputType's (p,s), for
	// InputPrecision/InputScale's reason on the output side: a .wshf header
	// carries half of every DECIMAL value in the file (ADR-0010), and the one
	// output row no input vector can type is the identity row an ungrouped
	// aggregate emits when it consumed nothing — the shape a selective filter
	// produces on a partial task whose files matched no rows. Declaring (0,0)
	// there made the merging aggregate read every other partial's scaled
	// Int128 as unscaled: SUM(a) WHERE id < 5 answered 3824.00 for 38.24, and
	// AVG/MIN/MAX the same 10^scale over (#685). Zero for every other type.
	//
	// THIS IS A WHOLESALE-DEPLOY FIELD (ADR-0010). The first draft of this note
	// said a worker ignoring it "still answers correctly", and that is false in
	// both readings. A partial task that drops it writes DECIMAL(0,0) beside
	// siblings that write DECIMAL(38,2), and the consumer of that stage now
	// REFUSES the read by name (cachedFileStreamSource.checkWSHFDecimalHeader)
	// — so a rolling deploy turns the query into a failed task, not a wrong
	// answer, but not into a correct one either. Without that guard it is the
	// silent 10^scale of #685. Coordinator and workers deploy together.
	OutputPrecision int `json:"output_precision,omitempty"`
	OutputScale     int `json:"output_scale,omitempty"`
	// InputType is the plan-time parquet.TypeID of the vector InputExpr
	// evaluates into, carried the same way and for the same reason: the
	// worker compiles InputExpr from its text and has no catalog to
	// resolve the columns in it. Nil means "not declared" (no derived
	// input, or an older coordinator), and the worker keeps its numeric
	// default. Getting it wrong is silent — MAX(COALESCE(a, b)) over two
	// string columns wrote strings into a Float64 vector and the
	// aggregate saw zeros (#333).
	//
	// A POINTER since #371, for WindowColSpec.OutputType's reason:
	// parquet.TypeID's zero value is BOOL, so the plain int this used to be
	// could not tell BOOL_AND's boolean predicate input from an absent
	// declaration — the DAG read it as undeclared, projected the predicate
	// into a Float64 vector, and the accumulator never saw a true value.
	InputType *int `json:"input_type,omitempty"`
	// InputPrecision/InputScale carry a DECIMAL InputType's (p,s), for the
	// reason OpSpec.GroupByDecimal carries a key's: the materialized input
	// vector is built from the declaration and one with no scale truncates
	// every value — MAX(COALESCE(a, b)) over two DECIMAL columns answered
	// 12 for 12.75 on the DAG (ADR-0024 item 2). Zero for every other type.
	InputPrecision int `json:"input_precision,omitempty"`
	InputScale     int `json:"input_scale,omitempty"`
	// InputCol2 is the second column argument of a two-column aggregate:
	// CORR(x, y), COVAR_SAMP/POP(x, y) and MIN_BY/MAX_BY(value, ordering).
	// Empty for every other function.
	InputCol2 string `json:"input_col2,omitempty"`
	// Separator is STRING_AGG's delimiter literal. Empty means the default
	// "," — the same fallback exec.HashAggregate applies.
	Separator string `json:"separator,omitempty"`
	// Percentile is PERCENTILE_CONT/PERCENTILE_DISC's fraction in [0,1].
	// Zero for every other function (and a legal fraction for those two,
	// which is why nothing reads it as "unset").
	Percentile float64 `json:"percentile,omitempty"`
	// Distinct is SQL's `AGG(DISTINCT x)` for every aggregate but COUNT,
	// which travels as Func "count_distinct". The worker maps it onto
	// exec.AggColumn.Distinct; a plan that dropped it ran a plain SUM under
	// the DISTINCT spelling (#703).
	Distinct bool `json:"distinct,omitempty"`
}

// GatherBatchMsg is the NATS message body the worker publishes to the
// coordinator's gather reply subject. One message per output RecordBatch,
// terminated by one message with Terminal=true (zero RowCount, any Err set).
//
// Payload is a self-contained WSHF byte stream carrying a single chunk
// (magic + chunk-count=1 + schema header + one row chunk). The coordinator
// decodes each message independently via the shared wshf.ChunkReader.
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
	// NullsLast places NULLs after the non-NULL values. Nil means the query
	// wrote no NULLS clause, and takes the engine default — see
	// PlaceNullsLast — which is also the only safe reading of a spec written
	// before the field existed.
	//
	// Without this the DAG sorted NULLs first unconditionally: ascending
	// order came back wrong and an explicit NULLS LAST was dropped, while
	// descending was correct by accident (#330).
	NullsLast *bool `json:"nulls_last,omitempty"`
}

// PlaceNullsLast reports where NULLs belong for this key, applying the engine
// default when the spec does not say.
//
// The default is NULLS LAST in BOTH directions, which is DuckDB's
// default_null_order and the placement this engine has always emitted. It is
// PostgreSQL's rule: NULLS LAST for ASC, NULLS FIRST for DESC. SQL leaves
// the default implementation-defined and DuckDB picks NULLS LAST in both
// directions, but wadjet speaks the PostgreSQL wire protocol and a client
// writing ORDER BY x DESC expects PostgreSQL's placement. The DuckDB gate
// sets default_null_order on the oracle so the comparison still holds.
//
// This default was unreachable before #343: the DESC comparator negated the
// kernel's null handling along with its value comparison, so a nominal NULLS
// FIRST for DESC came out of the engine as NULLS LAST. With that negation
// gone the declaration is what the engine emits.
//
// Must agree with the planner's resolveNullsLast key for key, or the two
// execution paths sort differently.
func (s SortKeySpec) PlaceNullsLast() bool {
	if s.NullsLast != nil {
		return *s.NullsLast
	}
	return !s.Desc
}

// NullsLastPtr returns a pointer suitable for SortKeySpec.NullsLast.
func NullsLastPtr(v bool) *bool { return &v }

// WindowColSpec defines one window function column of an OpWindow fragment
// operator. Every field the operator needs is resolved at PLAN TIME and
// carried here: the worker has no catalog and no logical plan, so anything it
// would have to derive itself (the output type, an offset buried in the
// argument list) is a decision the coordinator already made — the shape
// AggSpec.OutputType (#329) and AggSpec.InputType (#333) settled for
// aggregates.
type WindowColSpec struct {
	Func string `json:"func"` // row_number, rank, dense_rank, sum, count, avg, min, max, lag, lead, …
	// InputCol is the COLUMN the function reads, alone — not the argument
	// list. LAG, LEAD and NTH_VALUE spell their column alongside an offset,
	// a default or an N ("n_name, 2"); those extra arguments are parsed at
	// plan time into the LagLead*/NthValueN/NtileBuckets fields below, so
	// this field is always a name the input batch can be looked up by.
	InputCol  string `json:"input_col,omitempty"`
	OutputCol string `json:"output_col"`
	// OutputType is the plan-time parquet.TypeID of this column's output
	// vector, carried as a plain int so the wire package stays free of the
	// storage dependency.
	//
	// Nil means "the planner did not declare one" — an older coordinator,
	// or a value function whose input column it could not resolve to a
	// catalog type. The worker then keeps the conservative declaration
	// (float64, which is what windowOutputType returns for anything it
	// cannot name), and exec.Window's retypeValueColumns still corrects the
	// five value functions from the vector it actually reads. Declaring it
	// is what covers the functions retyping cannot reach.
	//
	// A POINTER, like AggSpec.OutputType (#354) and AggSpec.InputType
	// (#371), because parquet.TypeID's zero value is BOOL: LAG over a
	// boolean column declares 0, and an int field cannot tell that from an
	// absent declaration. Silently reading a declared BOOL as "undeclared"
	// reinstates #345 for exactly one type, which is the kind of hole that
	// goes unnoticed. SortKeySpec.NullsLast carries a pointer for the same
	// reason.
	OutputType  *int          `json:"output_type,omitempty"`
	PartitionBy []string      `json:"partition_by,omitempty"`
	OrderBy     []SortKeySpec `json:"order_by,omitempty"`
	// Frame is the explicit ROWS/RANGE frame, nil for the SQL default.
	Frame *WindowFrameSpec `json:"frame,omitempty"`
	// Function-specific arguments, parsed out of the SQL argument list by
	// the planner (see InputCol).
	LagLeadOffset  int `json:"lag_lead_offset,omitempty"`
	LagLeadDefault any `json:"lag_lead_default,omitempty"`
	NtileBuckets   int `json:"ntile_buckets,omitempty"`
	NthValueN      int `json:"nth_value_n,omitempty"`
}

// WindowTypePtr returns a pointer suitable for the pointer-typed TypeID
// fields — WindowColSpec.OutputType, AggSpec.InputType (#371), and since
// #354 AggSpec.OutputType — all pointer for the same BOOL-is-zero reason.
func WindowTypePtr(v int) *int { return &v }

// WindowFrameSpec describes a window frame on the wire. Mirrors
// exec.WindowFrameSpec / logical.WindowFrameSpec field for field.
type WindowFrameSpec struct {
	Mode  string          `json:"mode"` // "rows" or "range"
	Start WindowBoundSpec `json:"start"`
	End   WindowBoundSpec `json:"end"`
}

// WindowBoundSpec is one end of a WindowFrameSpec.
type WindowBoundSpec struct {
	Type   string `json:"type"` // unbounded_preceding, preceding, current_row, following, unbounded_following
	Offset int    `json:"offset,omitempty"`
}

// FusedJoinSpec describes an additional broadcast join absorbed into a task.
// The worker builds the hash table from BuildFiles, then probes each batch
// through this join before passing it to the next fused join (or output).
type FusedJoinSpec struct {
	JoinType      string   `json:"join_type"`
	JoinLeftKeys  []string `json:"join_left_keys"`  // keys from the probe stream
	JoinRightKeys []string `json:"join_right_keys"` // keys in build files
	// JoinKeyTypes is Task.JoinKeyTypes for this fused join — see there.
	JoinKeyTypes    []int             `json:"join_key_types,omitempty"`
	BuildFiles      []string          `json:"build_files"` // build-side files (broadcast)
	BuildTableAlias string            `json:"build_table_alias,omitempty"`
	BuildColOrigins map[string]string `json:"build_col_origins,omitempty"` // bare build col → owning scan alias (multi-table builds only)
	JoinFilter      string            `json:"join_filter,omitempty"`
	FilterExprs     []string          `json:"filter_exprs,omitempty"` // post-join filters for this step
	// BuildSchema declares this fused build's columns, read only when its
	// file list turns out to be empty (see OpSpec.BuildSchema).
	BuildSchema []ColumnSpec `json:"build_schema,omitempty"`
}

// ResultNotification is sent by workers when a task completes.
type ResultNotification struct {
	TaskID   string `json:"task_id"`
	QueryID  string `json:"query_id"`
	StageID  string `json:"stage_id"`
	WorkerID string `json:"worker_id"`
	Success  bool   `json:"success"`
	Error    string `json:"error,omitempty"`
	// Panicked marks a failure that crossed the query-scoped panic boundary
	// (ADR-0019) on the worker, set from the recovered error's TYPE
	// (errors.As against *exec.QueryPanic), never from Error's text. Error
	// is free-form: it can embed arbitrary user data (e.g. an invalid CAST
	// argument), and that text can legitimately contain any substring,
	// including one that used to double as the panic marker
	// (exec.queryPanicPrefix) — a SQL statement casting the literal string
	// "internal error in x" reproduced exactly that collision. The retry
	// decision and the SQLSTATE handed to the client key off this field,
	// not off Error.
	Panicked bool `json:"panicked,omitempty"`

	// Result location
	ResultPath  string   `json:"result_path,omitempty"`
	ResultFiles []string `json:"result_files,omitempty"` // multi-file output (e.g., shuffle per-partition files)
	// UploadPendingKeys lists the ResultFiles whose durable (S3) copy was
	// still uploading in the background when this notification was sent
	// (streaming exchange Phase B). The coordinator's reap grace counts
	// these per worker: a silent worker holding the only copy of such
	// keys gets a bounded reap deferral (docs/design/reap-grace.md).
	// Keys leave pending via UploadComplete. Absent (older worker or
	// synchronous upload) = nothing pending — grace disengages.
	UploadPendingKeys []string `json:"upload_pending_keys,omitempty"`
	NumRows           int64    `json:"num_rows"`
	SizeBytes         int64    `json:"size_bytes"`

	// Per-partition output accounting for partition-writing tasks (shuffle
	// tasks and fragment tasks with an exchange-sender sink), indexed by
	// partition id with len == NumPartitions. Rows come from the partitioned
	// sink's per-partition counters; bytes are the on-disk uncompressed
	// .wshf sizes (same unit as SizeBytes). The coordinator reduces these
	// element-wise across a stage's tasks to detect hot partitions at the
	// repartition→join seam. Empty partitions hold zeros; nil = worker
	// didn't report (legacy build or non-partitioned output).
	PartitionRows  []int64 `json:"partition_rows,omitempty"`
	PartitionBytes []int64 `json:"partition_bytes,omitempty"`

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

	// PlanRefused marks a failure the worker attributes to the PLAN it was
	// handed rather than to the machine it ran on: the identical task
	// re-dispatched to another worker reproduces it exactly. Set from the
	// error's TYPE, never from Error's text, for the reason Panicked is
	// (that text is free-form and can carry any substring as user data).
	//
	// Today's one producer is #503's declared-schema refusal: a base-table
	// parquet read whose declaration never rode the plan. Three attempts of
	// it cost a stage its whole retry budget and told the client nothing the
	// first attempt had not, so the retrier treats it the way it treats a
	// recovered panic — terminal on the first failure.
	PlanRefused bool `json:"plan_refused,omitempty"`
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

// ProducerTaskManifest announces one completed producer task's shuffle
// output files to eagerly-dispatched consumers (docs/design/
// eager-consumer-dispatch.md §3.1). Published by the coordinator on
// EagerManifestSubject(root, stage) as each producer task reaches a
// successful terminal state; metadata only.
type ProducerTaskManifest struct {
	StageID  string   `json:"stage_id"`
	TaskID   string   `json:"task_id"`
	Attempt  int      `json:"attempt"` // attempt fencing (memo §5)
	Files    []string `json:"files"`   // keys that EXIST (empty partitions absent)
	WorkerID string   `json:"worker_id"`
	// PeerAddr is the producing worker's peer-exchange address, resolved
	// by the coordinator at publish time. Empty when the worker is not
	// serving peer fetches — consumers fall through to S3 as always.
	PeerAddr string `json:"peer_addr,omitempty"`
	// Final marks the manifest of the producer stage's last terminal
	// task; a consumer that has resolved every candidate and seen Final
	// may EOF its manifest feed.
	Final bool `json:"final,omitempty"`
}

// EagerInput describes one eagerly-fed input alias of a consumer task
// (docs/design/eager-consumer-dispatch.md §3.2): the full candidate set
// is nameable at dispatch (deterministic shuffle keys); existence and
// location stream in as ProducerTaskManifests. Replay carries manifests
// already published before this task was built, so the subscribe-then-
// replay contract never loses a completion.
type EagerInput struct {
	RootQueryID     string                 `json:"root_query_id"`
	StageID         string                 `json:"stage_id"` // producer stage
	ProducerTaskIDs []string               `json:"producer_task_ids"`
	PartitionStart  int                    `json:"partition_start"` // inclusive
	PartitionEnd    int                    `json:"partition_end"`   // inclusive
	Replay          []ProducerTaskManifest `json:"replay,omitempty"`
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
	Closed   int    `json:"closed,omitempty"`   // closed same-Name instances coalesced into this entry (max-peak shown)
}

// WorkerHeartbeat is periodically sent by workers.
type WorkerHeartbeat struct {
	WorkerID      string   `json:"worker_id"`
	ClusterID     string   `json:"cluster_id,omitempty"`     // cluster this worker belongs to
	MaxConcurrent int      `json:"max_concurrent,omitempty"` // worker's effective task slot count (after auto-tuning); 0 = unknown
	ActiveTasks   int      `json:"active_tasks"`
	ActiveTaskIDs []string `json:"active_task_ids,omitempty"` // task IDs currently executing
	MemoryUsed    int64    `json:"memory_used"`
	MemoryTotal   int64    `json:"memory_total"`
	PoolUsed      int64    `json:"pool_used,omitempty"`   // bytes Reserved in the worker's shared memory pool
	PoolBudget    int64    `json:"pool_budget,omitempty"` // shared memory pool capacity in bytes; pressure = PoolUsed/PoolBudget
	RSS           int64    `json:"rss,omitempty"`         // process RSS from /proc/self/status
	NumGoroutines int      `json:"num_goroutines,omitempty"`
	Mallocs       uint64   `json:"mallocs,omitempty"`         // cumulative allocation count from runtime.MemStats
	SpillDiskUsed int64    `json:"spill_disk_used,omitempty"` // bytes used in spill directory
	Draining      bool     `json:"draining,omitempty"`        // true when worker is draining
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

// ComputedColSpec is one appended expression column on a shuffle payload
// (exchange subsumption dedup).
type ComputedColSpec struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
}
