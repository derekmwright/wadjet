package distributed

import (
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
)

// Task is the unit of distributed work published to NATS JetStream.
type Task struct {
	ID        string   `json:"id"`
	QueryID   string   `json:"query_id"`
	StageID   string   `json:"stage_id"`
	Type      TaskType `json:"type"`
	TableName string   `json:"table_name,omitempty"`

	// Scan-specific
	Files           []string          `json:"files,omitempty"`
	PartitionFilter map[string]string `json:"partition_filter,omitempty"`
	Columns         []string          `json:"columns,omitempty"`

	// Aggregate-specific
	GroupByCols []string     `json:"group_by_cols,omitempty"`
	Aggregates  []AggSpec    `json:"aggregates,omitempty"`
	InputFiles  []string     `json:"input_files,omitempty"` // results from previous stage

	// Sort-specific
	SortKeys []SortKeySpec `json:"sort_keys,omitempty"`
	Limit    int           `json:"limit,omitempty"`

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

// ResultNotification is sent by workers when a task completes.
type ResultNotification struct {
	TaskID    string `json:"task_id"`
	QueryID   string `json:"query_id"`
	StageID   string `json:"stage_id"`
	WorkerID  string `json:"worker_id"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`

	// Result location
	ResultPath string `json:"result_path,omitempty"`
	NumRows    int64  `json:"num_rows"`
	SizeBytes  int64  `json:"size_bytes"`

	// Small result fast path (< 256 KB): inline result data
	InlineData []byte `json:"inline_data,omitempty"`

	Duration  time.Duration `json:"duration"`
	Timestamp time.Time     `json:"timestamp"`
}

// WorkerHeartbeat is periodically sent by workers.
type WorkerHeartbeat struct {
	WorkerID     string    `json:"worker_id"`
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

// Marshal serializes a message to JSON.
func Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal deserializes a message from JSON.
func Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
