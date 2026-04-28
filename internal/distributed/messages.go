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

	// PartialAggregate is set on probe-split pipeline tasks to indicate that
	// the top-level Sort and Limit should be stripped. Each worker produces
	// complete partial aggregates; the coordinator merges them.
	PartialAggregate bool `json:"partial_aggregate,omitempty"`

	// Scan-specific
	Files           []string          `json:"files,omitempty"`
	PartitionFilter map[string]string `json:"partition_filter,omitempty"`
	Columns         []string          `json:"columns,omitempty"`
	FilterExprs     []string          `json:"filter_exprs,omitempty"` // SQL filter expressions for pushdown
	// PostFilterExprs are SQL filter expressions applied to the stage's
	// OUTPUT (post-aggregate/post-join) rather than to raw scan input.
	// Native-DAG compute stages use this for HAVING and join residual
	// predicates. FilterExprs vs PostFilterExprs differ in column scope:
	// FilterExprs references scan columns; PostFilterExprs references
	// aggregate output cols or joined-schema cols.
	PostFilterExprs []string          `json:"post_filter_exprs,omitempty"`

	// Fused scan-aggregate: partial aggregation done at scan level
	ScanAggGroupBy []string  `json:"scan_agg_group_by,omitempty"`
	ScanAggSpecs   []AggSpec `json:"scan_agg_specs,omitempty"`

	// Aggregate-specific
	GroupByCols []string     `json:"group_by_cols,omitempty"`
	Aggregates  []AggSpec    `json:"aggregates,omitempty"`
	InputFiles  []string     `json:"input_files,omitempty"` // results from previous stage

	// Sort-specific
	SortKeys       []SortKeySpec `json:"sort_keys,omitempty"`
	Limit          int           `json:"limit,omitempty"`
	MergePreSorted bool          `json:"merge_pre_sorted,omitempty"` // true for merge_sort: inputs are pre-sorted
	MergePartials  bool          `json:"merge_partials,omitempty"`   // true for final_aggregate: re-aggregate partial results

	// Join-specific
	JoinType        string   `json:"join_type,omitempty"`        // inner, left, right, full, cross
	JoinLeftKeys    []string `json:"join_left_keys,omitempty"`   // probe side key columns
	JoinRightKeys   []string `json:"join_right_keys,omitempty"`  // build side key columns
	BuildFiles      []string `json:"build_files,omitempty"`      // build (right) side input files
	BuildTableAlias string   `json:"build_table_alias,omitempty"` // build-side alias for column disambiguation
	// QualifyAllBuildCols, when true, forces the join executor to emit
	// build-side columns under their qualified name even when no probe-side
	// column has the same base name. Set by the planner for self-join scenarios
	// (Q07's two scans of nation that co-path into the same join chain).
	QualifyAllBuildCols bool   `json:"qualify_all_build_cols,omitempty"`
	JoinFilter      string   `json:"join_filter,omitempty"`       // semi/anti join inequality filter expression

	// Fused join: additional broadcast joins absorbed into a single task.
	// The worker builds hash tables for each fused join, then chains probes
	// batch-by-batch: probe → join1 → join2 → ... → output.
	FusedJoins []FusedJoinSpec `json:"fused_joins,omitempty"`

	// Window-specific
	WindowCols []WindowColSpec `json:"window_cols,omitempty"`

	// Shuffle-specific
	ShuffleKeys   []string `json:"shuffle_keys,omitempty"`    // columns to hash-partition on
	NumPartitions int      `json:"num_partitions,omitempty"`  // number of output partitions
	PartitionID   int      `json:"partition_id,omitempty"`    // which partition this join task handles

	// Distributed tracing context (W3C Trace Context format)
	TraceID  string `json:"trace_id,omitempty"`  // 32-char hex
	SpanID   string `json:"span_id,omitempty"`   // 16-char hex parent span
	TraceFlags byte  `json:"trace_flags,omitempty"` // 0x01 = sampled

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

	CreatedAt time.Time `json:"created_at"`
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
}

// SortKeySpec defines a sort key in a task.
type SortKeySpec struct {
	Column string `json:"column"`
	Desc   bool   `json:"desc"`
}

// WindowColSpec defines a window function column in a task.
type WindowColSpec struct {
	Func        string        `json:"func"`         // row_number, rank, dense_rank, sum, count, avg, min, max
	InputCol    string        `json:"input_col"`     // for aggregate window functions
	OutputCol   string        `json:"output_col"`
	PartitionBy []string      `json:"partition_by,omitempty"`
	OrderBy     []SortKeySpec `json:"order_by,omitempty"`
}

// FusedJoinSpec describes an additional broadcast join absorbed into a task.
// The worker builds the hash table from BuildFiles, then probes each batch
// through this join before passing it to the next fused join (or output).
type FusedJoinSpec struct {
	JoinType        string   `json:"join_type"`
	JoinLeftKeys    []string `json:"join_left_keys"`    // keys from the probe stream
	JoinRightKeys   []string `json:"join_right_keys"`   // keys in build files
	BuildFiles      []string `json:"build_files"`       // build-side files (broadcast)
	BuildTableAlias string   `json:"build_table_alias,omitempty"`
	JoinFilter      string   `json:"join_filter,omitempty"`
	FilterExprs     []string `json:"filter_exprs,omitempty"` // post-join filters for this step
}

// ResultNotification is sent by workers when a task completes.
type ResultNotification struct {
	TaskID    string `json:"task_id"`
	QueryID   string `json:"query_id"`
	StageID   string `json:"stage_id"`
	WorkerID  string `json:"worker_id"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`

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
}

// TaskStats captures per-task execution metrics for debugging.
type TaskStats struct {
	MemUsed    int64 `json:"mem_used"`    // memory tracker usage at completion
	MemBudget  int64 `json:"mem_budget"`  // memory budget for this task
	SpillFiles int   `json:"spill_files"` // number of spill files written
	SpillBytes int64 `json:"spill_bytes"` // total bytes spilled to disk
	RSS        int64 `json:"rss"`         // worker process RSS at task completion
	PeakHeapMB int64 `json:"peak_heap_mb"` // per-task peak HeapAlloc in MB, captured by atomic-max sampler
}

// WorkerHeartbeat is periodically sent by workers.
type WorkerHeartbeat struct {
	WorkerID      string    `json:"worker_id"`
	ClusterID     string    `json:"cluster_id,omitempty"` // cluster this worker belongs to
	MaxConcurrent int       `json:"max_concurrent,omitempty"`  // worker's effective task slot count (after auto-tuning); 0 = unknown
	ActiveTasks   int       `json:"active_tasks"`
	ActiveTaskIDs []string  `json:"active_task_ids,omitempty"` // task IDs currently executing
	MemoryUsed    int64     `json:"memory_used"`
	MemoryTotal   int64     `json:"memory_total"`
	PoolUsed      int64     `json:"pool_used,omitempty"`   // bytes Reserved in the worker's shared memory pool
	PoolBudget    int64     `json:"pool_budget,omitempty"` // shared memory pool capacity in bytes; pressure = PoolUsed/PoolBudget
	RSS           int64     `json:"rss,omitempty"`             // process RSS from /proc/self/status
	NumGoroutines int       `json:"num_goroutines,omitempty"`
	Mallocs       uint64    `json:"mallocs,omitempty"`         // cumulative allocation count from runtime.MemStats
	SpillDiskUsed int64     `json:"spill_disk_used,omitempty"` // bytes used in spill directory
	Draining      bool      `json:"draining,omitempty"`        // true when worker is draining
	Timestamp     time.Time `json:"timestamp"`
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
	Reason    string    `json:"reason"` // "execution_error", "panic", "marshal_error", "publish_error"
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
