package distributed

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTaskJSONRoundTrip(t *testing.T) {
	original := Task{
		ID:        "task-1",
		QueryID:   "q-100",
		StageID:   "s-0",
		Type:      TaskType("scan"),
		TableName: "orders",
		Files:     []string{"part-0000.parquet", "part-0001.parquet"},
		Columns:   []string{"id", "amount"},
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Task should be JSON (no 'G' prefix).
	if len(data) > 0 && data[0] == 'G' {
		t.Fatal("expected JSON encoding for Task, got gob")
	}

	var decoded Task
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.QueryID != original.QueryID {
		t.Errorf("QueryID: got %q, want %q", decoded.QueryID, original.QueryID)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type: got %q, want %q", decoded.Type, original.Type)
	}
	if decoded.TableName != original.TableName {
		t.Errorf("TableName: got %q, want %q", decoded.TableName, original.TableName)
	}
	if len(decoded.Files) != len(original.Files) {
		t.Errorf("Files length: got %d, want %d", len(decoded.Files), len(original.Files))
	}
	if !decoded.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", decoded.CreatedAt, original.CreatedAt)
	}
}

func TestResultNotificationGobRoundTrip(t *testing.T) {
	original := ResultNotification{
		TaskID:     "task-1",
		QueryID:    "q-100",
		StageID:    "s-0",
		WorkerID:   "w-1",
		Success:    true,
		ResultPath: "results/q-100/s-0/task-1.parquet",
		NumRows:    5000,
		SizeBytes:  102400,
		Duration:   250 * time.Millisecond,
		Timestamp:  time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// ResultNotification should use gob ('G' prefix).
	if len(data) == 0 || data[0] != 'G' {
		t.Fatal("expected gob encoding (G prefix) for ResultNotification")
	}

	var decoded ResultNotification
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.TaskID != original.TaskID {
		t.Errorf("TaskID: got %q, want %q", decoded.TaskID, original.TaskID)
	}
	if decoded.Success != original.Success {
		t.Errorf("Success: got %v, want %v", decoded.Success, original.Success)
	}
	if decoded.NumRows != original.NumRows {
		t.Errorf("NumRows: got %d, want %d", decoded.NumRows, original.NumRows)
	}
	if decoded.Duration != original.Duration {
		t.Errorf("Duration: got %v, want %v", decoded.Duration, original.Duration)
	}
}

func TestWorkerHeartbeatJSONRoundTrip(t *testing.T) {
	original := WorkerHeartbeat{
		WorkerID:    "w-42",
		ActiveTasks: 3,
		MemoryUsed:  1 << 30,
		MemoryTotal: 4 << 30,
		Timestamp:   time.Date(2026, 3, 15, 8, 0, 0, 0, time.UTC),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	if len(data) > 0 && data[0] == 'G' {
		t.Fatal("expected JSON encoding for WorkerHeartbeat, got gob")
	}

	var decoded WorkerHeartbeat
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.WorkerID != original.WorkerID {
		t.Errorf("WorkerID: got %q, want %q", decoded.WorkerID, original.WorkerID)
	}
	if decoded.ActiveTasks != original.ActiveTasks {
		t.Errorf("ActiveTasks: got %d, want %d", decoded.ActiveTasks, original.ActiveTasks)
	}
	if decoded.MemoryUsed != original.MemoryUsed {
		t.Errorf("MemoryUsed: got %d, want %d", decoded.MemoryUsed, original.MemoryUsed)
	}
}

func TestUnmarshalAutoDetectsFormat(t *testing.T) {
	// Gob-encoded ResultNotification.
	rn := ResultNotification{TaskID: "t-1", QueryID: "q-1", Success: true}
	gobData, err := Marshal(rn)
	if err != nil {
		t.Fatalf("Marshal gob: %v", err)
	}

	var decodedGob ResultNotification
	if err := Unmarshal(gobData, &decodedGob); err != nil {
		t.Fatalf("Unmarshal gob: %v", err)
	}
	if decodedGob.TaskID != "t-1" {
		t.Errorf("gob TaskID: got %q, want %q", decodedGob.TaskID, "t-1")
	}

	// JSON-encoded Task.
	task := Task{ID: "t-2", QueryID: "q-2", Type: TaskType("aggregate")}
	jsonData, err := Marshal(task)
	if err != nil {
		t.Fatalf("Marshal json: %v", err)
	}

	var decodedJSON Task
	if err := Unmarshal(jsonData, &decodedJSON); err != nil {
		t.Fatalf("Unmarshal json: %v", err)
	}
	if decodedJSON.ID != "t-2" {
		t.Errorf("json ID: got %q, want %q", decodedJSON.ID, "t-2")
	}

	// Verify raw JSON can also be unmarshalled (e.g. from external source).
	raw, _ := json.Marshal(WorkerHeartbeat{WorkerID: "w-ext"})
	var hb WorkerHeartbeat
	if err := Unmarshal(raw, &hb); err != nil {
		t.Fatalf("Unmarshal raw JSON: %v", err)
	}
	if hb.WorkerID != "w-ext" {
		t.Errorf("raw JSON WorkerID: got %q, want %q", hb.WorkerID, "w-ext")
	}
}

func TestResultNotificationInlineDataRoundTrip(t *testing.T) {
	payload := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
	original := ResultNotification{
		TaskID:     "t-inline",
		QueryID:    "q-5",
		StageID:    "s-0",
		WorkerID:   "w-1",
		Success:    true,
		InlineData: payload,
		NumRows:    1,
		Timestamp:  time.Now().UTC().Truncate(time.Second),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded ResultNotification
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if len(decoded.InlineData) != len(payload) {
		t.Fatalf("InlineData length: got %d, want %d", len(decoded.InlineData), len(payload))
	}
	for i, b := range decoded.InlineData {
		if b != payload[i] {
			t.Errorf("InlineData[%d]: got %#x, want %#x", i, b, payload[i])
		}
	}
}

func TestTaskSubject(t *testing.T) {
	got := TaskSubject("scan", "q-100", "s-0")
	want := "wadjet.tasks.scan.q-100.s-0"
	if got != want {
		t.Errorf("TaskSubject: got %q, want %q", got, want)
	}

	got = TaskSubject("aggregate", "q-200", "s-1")
	want = "wadjet.tasks.aggregate.q-200.s-1"
	if got != want {
		t.Errorf("TaskSubject: got %q, want %q", got, want)
	}
}

func TestResultSubject(t *testing.T) {
	got := ResultSubject("q-100", "s-0", "t-1")
	want := "wadjet.results.q-100.s-0.t-1"
	if got != want {
		t.Errorf("ResultSubject: got %q, want %q", got, want)
	}
}

func TestQueryResultSubject(t *testing.T) {
	got := QueryResultSubject("q-100")
	want := "wadjet.results.q-100.>"
	if got != want {
		t.Errorf("QueryResultSubject: got %q, want %q", got, want)
	}
}

func TestCancelSubject(t *testing.T) {
	got := CancelSubject("q-100")
	want := "wadjet.cancel.q-100"
	if got != want {
		t.Errorf("CancelSubject: got %q, want %q", got, want)
	}
}

func TestCancelSubjectAll(t *testing.T) {
	got := CancelSubjectAll()
	want := "wadjet.cancel.>"
	if got != want {
		t.Errorf("CancelSubjectAll: got %q, want %q", got, want)
	}
}

func TestClusterTaskSubject(t *testing.T) {
	got := ClusterTaskSubject("afb-east", "scan", "q-100", "s-0")
	want := "wadjet.tasks.afb-east.scan.q-100.s-0"
	if got != want {
		t.Errorf("ClusterTaskSubject: got %q, want %q", got, want)
	}
}

func TestClusterTasksFilter(t *testing.T) {
	got := ClusterTasksFilter("afb-east")
	want := "wadjet.tasks.afb-east.>"
	if got != want {
		t.Errorf("ClusterTasksFilter: got %q, want %q", got, want)
	}
}

func TestTaskClusterIDRoundTrip(t *testing.T) {
	original := Task{
		ID:        "task-1",
		QueryID:   "q-100",
		StageID:   "s-0",
		Type:      TaskType("scan"),
		ClusterID: "afb-east",
		TableName: "sensor_data",
		CreatedAt: time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var decoded Task
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ClusterID != "afb-east" {
		t.Errorf("ClusterID: got %q, want %q", decoded.ClusterID, "afb-east")
	}
}

func TestHeartbeatClusterIDRoundTrip(t *testing.T) {
	original := WorkerHeartbeat{
		WorkerID:  "w-1",
		ClusterID: "afb-west",
		Timestamp: time.Now().UTC(),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	var decoded WorkerHeartbeat
	if err := Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ClusterID != "afb-west" {
		t.Errorf("ClusterID: got %q, want %q", decoded.ClusterID, "afb-west")
	}
}
