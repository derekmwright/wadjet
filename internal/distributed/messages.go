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
	TaskTypeScan      TaskType = "scan"
	TaskTypeAggregate TaskType = "aggregate"
	TaskTypeJoin      TaskType = "join"
	TaskTypeSort      TaskType = "sort"
	TaskTypeWindow    TaskType = "window"
	TaskTypeShuffle   TaskType = "shuffle"
	TaskTypePipeline  TaskType = "pipeline" // full query executed as standalone pipeline on one worker
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

	// Scan-specific
	Files           []string          `json:"files,omitempty"`
	PartitionFilter map[string]string `json:"partition_filter,omitempty"`
	Columns         []string          `json:"columns,omitempty"`
	FilterExprs     []string          `json:"filter_exprs,omitempty"` // SQL filter expressions for pushdown

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

	// Identity context (for access control enforcement at workers)
	IdentityName string `json:"identity_name,omitempty"`
	IdentityRole string `json:"identity_role,omitempty"`

	// ABAC pre-evaluated policy decisions (serialized for worker enforcement)
	PolicyDecisionJSON json.RawMessage `json:"policy_decision,omitempty"`

	// Result destination
	ResultBucket string `json:"result_bucket"`
	ResultPrefix string `json:"result_prefix"`

	CreatedAt time.Time `json:"created_at"`
}

// AggSpec defines an aggregation in a task.
type AggSpec struct {
	Func      string `json:"func"` // sum, count, min, max, avg
	InputCol  string `json:"input_col"`
	OutputCol string `json:"output_col"`
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

	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// WorkerHeartbeat is periodically sent by workers.
type WorkerHeartbeat struct {
	WorkerID     string    `json:"worker_id"`
	ClusterID    string    `json:"cluster_id,omitempty"` // cluster this worker belongs to
	ActiveTasks  int       `json:"active_tasks"`
	MemoryUsed   int64     `json:"memory_used"`
	MemoryTotal  int64     `json:"memory_total"`
	Timestamp    time.Time `json:"timestamp"`
}

// QueryManifest describes the final results of a query.
type QueryManifest struct {
	QueryID     string   `json:"query_id"`
	ResultFiles []string `json:"result_files"`
	TotalRows   int64    `json:"total_rows"`
	TotalBytes  int64    `json:"total_bytes"`
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
