package distributed

import (
	"encoding/json"
	"testing"
	"time"
)

// --- Marshal/Unmarshal edge cases ---

func TestMarshalResultNotificationGobFormat(t *testing.T) {
	rn := ResultNotification{
		TaskID:  "t-1",
		Success: true,
	}
	data, err := Marshal(rn)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if len(data) == 0 || data[0] != 'G' {
		t.Fatal("ResultNotification should use gob encoding with 'G' prefix")
	}
}

func TestMarshalPointerResultNotificationUsesJSON(t *testing.T) {
	// A *ResultNotification (pointer) should NOT match the gob path
	// because the type assertion checks for value type, not pointer.
	rn := &ResultNotification{TaskID: "t-ptr"}
	data, err := Marshal(rn)
	if err != nil {
		t.Fatalf("Marshal pointer: %v", err)
	}
	// Pointer doesn't match the value type assertion, so it goes JSON path
	if len(data) > 0 && data[0] == 'G' {
		t.Fatal("pointer to ResultNotification should use JSON, not gob")
	}
	// Verify it's valid JSON
	var decoded ResultNotification
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("should be valid JSON: %v", err)
	}
	if decoded.TaskID != "t-ptr" {
		t.Errorf("TaskID: got %q, want %q", decoded.TaskID, "t-ptr")
	}
}

func TestUnmarshalEmptyData(t *testing.T) {
	var task Task
	err := Unmarshal([]byte{}, &task)
	if err == nil {
		t.Fatal("expected error on empty data")
	}
}

func TestUnmarshalInvalidJSON(t *testing.T) {
	var task Task
	err := Unmarshal([]byte(`{invalid json`), &task)
	if err == nil {
		t.Fatal("expected error on invalid JSON")
	}
}

func TestUnmarshalInvalidGob(t *testing.T) {
	// 'G' prefix followed by garbage
	var rn ResultNotification
	err := Unmarshal([]byte("Gnot-valid-gob-data"), &rn)
	if err == nil {
		t.Fatal("expected error on invalid gob data")
	}
}

func TestMarshalQueryManifest(t *testing.T) {
	qm := QueryManifest{
		QueryID:     "q-42",
		ResultFiles: []string{"file1.parquet", "file2.parquet"},
		TotalRows:   10000,
		TotalBytes:  524288,
	}
	data, err := Marshal(qm)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// Should be JSON (not gob)
	if len(data) > 0 && data[0] == 'G' {
		t.Fatal("QueryManifest should use JSON encoding")
	}

	var decoded QueryManifest
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.QueryID != "q-42" {
		t.Errorf("QueryID: got %q, want %q", decoded.QueryID, "q-42")
	}
	if len(decoded.ResultFiles) != 2 {
		t.Errorf("ResultFiles length: got %d, want 2", len(decoded.ResultFiles))
	}
	if decoded.TotalRows != 10000 {
		t.Errorf("TotalRows: got %d, want 10000", decoded.TotalRows)
	}
	if decoded.TotalBytes != 524288 {
		t.Errorf("TotalBytes: got %d, want 524288", decoded.TotalBytes)
	}
}

// --- Task types round trip ---

func TestTaskWithAllTypesRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		task Task
	}{
		{
			name: "aggregate task",
			task: Task{
				ID:          "agg-1",
				QueryID:     "q-1",
				StageID:     "s-1",
				Type:        TaskTypeAggregate,
				GroupByCols: []string{"region", "product"},
				Aggregates: []AggSpec{
					{Func: "sum", InputCol: "amount", OutputCol: "total"},
					{Func: "count", InputCol: "id", OutputCol: "cnt"},
					{Func: "avg", InputCol: "price", OutputCol: "avg_price"},
				},
				InputFiles:   []string{"input1.parquet", "input2.parquet"},
				ResultBucket: "results",
				ResultPrefix: "queries/q-1/s-1/",
				CreatedAt:    time.Now().UTC().Truncate(time.Second),
			},
		},
		{
			name: "sort task",
			task: Task{
				ID:      "sort-1",
				QueryID: "q-2",
				StageID: "s-0",
				Type:    TaskTypeSort,
				SortKeys: []SortKeySpec{
					{Column: "timestamp", Desc: true},
					{Column: "name", Desc: false},
				},
				Limit:        100,
				InputFiles:   []string{"data.parquet"},
				ResultBucket: "results",
				ResultPrefix: "queries/q-2/s-0/",
				CreatedAt:    time.Now().UTC().Truncate(time.Second),
			},
		},
		{
			name: "join task",
			task: Task{
				ID:            "join-1",
				QueryID:       "q-3",
				StageID:       "s-2",
				Type:          TaskTypeJoin,
				JoinType:      "left",
				JoinLeftKeys:  []string{"user_id"},
				JoinRightKeys: []string{"id"},
				InputFiles:    []string{"probe.parquet"},
				BuildFiles:    []string{"build.parquet"},
				ResultBucket:  "results",
				ResultPrefix:  "queries/q-3/s-2/",
				CreatedAt:     time.Now().UTC().Truncate(time.Second),
			},
		},
		{
			name: "window task",
			task: Task{
				ID:      "win-1",
				QueryID: "q-4",
				StageID: "s-1",
				Type:    TaskTypeWindow,
				WindowCols: []WindowColSpec{
					{
						Func:        "row_number",
						OutputCol:   "rn",
						PartitionBy: []string{"department"},
						OrderBy:     []SortKeySpec{{Column: "salary", Desc: true}},
					},
					{
						Func:        "sum",
						InputCol:    "salary",
						OutputCol:   "running_total",
						PartitionBy: []string{"department"},
						OrderBy:     []SortKeySpec{{Column: "hire_date", Desc: false}},
					},
				},
				InputFiles:   []string{"employees.parquet"},
				ResultBucket: "results",
				ResultPrefix: "queries/q-4/s-1/",
				CreatedAt:    time.Now().UTC().Truncate(time.Second),
			},
		},
		{
			name: "scan with identity and filters",
			task: Task{
				ID:              "scan-1",
				QueryID:         "q-5",
				StageID:         "s-0",
				Type:            TaskTypeScan,
				ClusterID:       "afb-east",
				TableName:       "classified_events",
				Files:           []string{"part-0001.parquet"},
				PartitionFilter: map[string]string{"classification": "secret"},
				Columns:         []string{"id", "event_type", "timestamp"},
				FilterExprs:     []string{"event_type = 'login'"},
				IdentityName:    "analyst-01",
				IdentityRole:    "analyst",
				ResultBucket:    "results",
				ResultPrefix:    "queries/q-5/s-0/",
				CreatedAt:       time.Now().UTC().Truncate(time.Second),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := Marshal(tt.task)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var decoded Task
			if err := Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if decoded.ID != tt.task.ID {
				t.Errorf("ID: got %q, want %q", decoded.ID, tt.task.ID)
			}
			if decoded.Type != tt.task.Type {
				t.Errorf("Type: got %q, want %q", decoded.Type, tt.task.Type)
			}
			if decoded.QueryID != tt.task.QueryID {
				t.Errorf("QueryID: got %q, want %q", decoded.QueryID, tt.task.QueryID)
			}
			if decoded.ClusterID != tt.task.ClusterID {
				t.Errorf("ClusterID: got %q, want %q", decoded.ClusterID, tt.task.ClusterID)
			}
			if decoded.IdentityName != tt.task.IdentityName {
				t.Errorf("IdentityName: got %q, want %q", decoded.IdentityName, tt.task.IdentityName)
			}
			if decoded.IdentityRole != tt.task.IdentityRole {
				t.Errorf("IdentityRole: got %q, want %q", decoded.IdentityRole, tt.task.IdentityRole)
			}
		})
	}
}

// --- ResultNotification error fields ---

func TestResultNotificationErrorRoundTrip(t *testing.T) {
	rn := ResultNotification{
		TaskID:   "t-fail",
		QueryID:  "q-1",
		StageID:  "s-0",
		WorkerID: "w-1",
		Success:  false,
		Error:    "file not found: missing.parquet",
	}
	data, err := Marshal(rn)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded ResultNotification
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Success != false {
		t.Error("expected Success=false")
	}
	if decoded.Error != "file not found: missing.parquet" {
		t.Errorf("Error: got %q, want %q", decoded.Error, "file not found: missing.parquet")
	}
	if decoded.WorkerID != "w-1" {
		t.Errorf("WorkerID: got %q, want %q", decoded.WorkerID, "w-1")
	}
}

// --- Subject construction edge cases ---

func TestTaskSubjectVariousTypes(t *testing.T) {
	tests := []struct {
		taskType string
		queryID  string
		stageID  string
		want     string
	}{
		{"scan", "q1", "s0", "wadjet.tasks.scan.q1.s0"},
		{"aggregate", "q2", "s1", "wadjet.tasks.aggregate.q2.s1"},
		{"sort", "q3", "s2", "wadjet.tasks.sort.q3.s2"},
		{"join", "q4", "s3", "wadjet.tasks.join.q4.s3"},
		{"window", "q5", "s4", "wadjet.tasks.window.q5.s4"},
	}
	for _, tt := range tests {
		t.Run(tt.taskType, func(t *testing.T) {
			got := TaskSubject(tt.taskType, tt.queryID, tt.stageID)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClusterTaskSubjectVariousClusters(t *testing.T) {
	tests := []struct {
		cluster string
		want    string
	}{
		{"central", "wadjet.tasks.central.scan.q1.s0"},
		{"edge-west", "wadjet.tasks.edge-west.scan.q1.s0"},
		{"afb-east", "wadjet.tasks.afb-east.scan.q1.s0"},
	}
	for _, tt := range tests {
		t.Run(tt.cluster, func(t *testing.T) {
			got := ClusterTaskSubject(tt.cluster, "scan", "q1", "s0")
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClusterTasksFilterVariousClusters(t *testing.T) {
	tests := []struct {
		cluster string
		want    string
	}{
		{"central", "wadjet.tasks.central.>"},
		{"edge-1", "wadjet.tasks.edge-1.>"},
	}
	for _, tt := range tests {
		t.Run(tt.cluster, func(t *testing.T) {
			got := ClusterTasksFilter(tt.cluster)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResultSubjectMultipleQueries(t *testing.T) {
	tests := []struct {
		queryID string
		stageID string
		taskID  string
		want    string
	}{
		{"q-abc", "s-0", "t-1", "wadjet.results.q-abc.s-0.t-1"},
		{"q-xyz", "stage-2", "task-99", "wadjet.results.q-xyz.stage-2.task-99"},
	}
	for _, tt := range tests {
		t.Run(tt.queryID, func(t *testing.T) {
			got := ResultSubject(tt.queryID, tt.stageID, tt.taskID)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// --- DefaultNATSConfig ---

func TestDefaultNATSConfig(t *testing.T) {
	cfg := DefaultNATSConfig()
	if cfg.Host != "127.0.0.1" {
		t.Errorf("Host: got %q, want %q", cfg.Host, "127.0.0.1")
	}
	if cfg.Port != 4222 {
		t.Errorf("Port: got %d, want 4222", cfg.Port)
	}
	if cfg.MaxPayload != 8*1024*1024 {
		t.Errorf("MaxPayload: got %d, want %d", cfg.MaxPayload, 8*1024*1024)
	}
	if cfg.StoreDir == "" {
		t.Error("StoreDir should not be empty")
	}
}

// --- Task PolicyDecisionJSON ---

func TestTaskPolicyDecisionJSONRoundTrip(t *testing.T) {
	policyJSON := json.RawMessage(`{"allow_rows":["region='east'"],"mask_cols":["ssn"]}`)
	task := Task{
		ID:                 "task-policy",
		QueryID:            "q-policy",
		StageID:            "s-0",
		Type:               TaskTypeScan,
		PolicyDecisionJSON: policyJSON,
		CreatedAt:          time.Now().UTC().Truncate(time.Second),
	}

	data, err := Marshal(task)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded Task
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if string(decoded.PolicyDecisionJSON) != string(policyJSON) {
		t.Errorf("PolicyDecisionJSON: got %s, want %s", decoded.PolicyDecisionJSON, policyJSON)
	}
}

// --- Gob encoding preserves all result fields ---

func TestResultNotificationAllFieldsRoundTrip(t *testing.T) {
	rn := ResultNotification{
		TaskID:     "t-full",
		QueryID:    "q-full",
		StageID:    "s-2",
		WorkerID:   "w-42",
		Success:    true,
		ResultPath: "queries/q-full/s-2/t-full.parquet",
		NumRows:    99999,
		SizeBytes:  1048576,
		InlineData: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		Duration:   3 * time.Second,
		Timestamp:  time.Date(2026, 3, 15, 12, 30, 0, 0, time.UTC),
	}

	data, err := Marshal(rn)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded ResultNotification
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.TaskID != rn.TaskID {
		t.Errorf("TaskID mismatch")
	}
	if decoded.QueryID != rn.QueryID {
		t.Errorf("QueryID mismatch")
	}
	if decoded.StageID != rn.StageID {
		t.Errorf("StageID mismatch")
	}
	if decoded.WorkerID != rn.WorkerID {
		t.Errorf("WorkerID mismatch")
	}
	if decoded.Success != rn.Success {
		t.Errorf("Success mismatch")
	}
	if decoded.ResultPath != rn.ResultPath {
		t.Errorf("ResultPath mismatch")
	}
	if decoded.NumRows != rn.NumRows {
		t.Errorf("NumRows: got %d, want %d", decoded.NumRows, rn.NumRows)
	}
	if decoded.SizeBytes != rn.SizeBytes {
		t.Errorf("SizeBytes: got %d, want %d", decoded.SizeBytes, rn.SizeBytes)
	}
	if decoded.Duration != rn.Duration {
		t.Errorf("Duration: got %v, want %v", decoded.Duration, rn.Duration)
	}
	if len(decoded.InlineData) != len(rn.InlineData) {
		t.Fatalf("InlineData length: got %d, want %d", len(decoded.InlineData), len(rn.InlineData))
	}
	for i := range decoded.InlineData {
		if decoded.InlineData[i] != rn.InlineData[i] {
			t.Errorf("InlineData[%d]: got %#x, want %#x", i, decoded.InlineData[i], rn.InlineData[i])
		}
	}
}
