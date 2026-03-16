package physical

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTableFuncReadJSON_Local(t *testing.T) {
	// Write a temp JSONL file
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	data := `{"name":"alice","age":30,"active":true}
{"name":"bob","age":25,"active":false}
{"name":"carol","age":35,"active":true}
`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	source, err := buildTableFunctionSource("read_json", []string{path})
	if err != nil {
		t.Fatal(err)
	}

	if err := source.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	b, err := source.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected non-nil batch")
	}

	rows := b.ToRows()
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify data
	if rows[0]["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", rows[0]["name"])
	}
	if rows[1]["age"] != int64(25) {
		t.Errorf("expected age=25, got %v (%T)", rows[1]["age"], rows[1]["age"])
	}

	// Second Next should return nil (all rows in one batch)
	b2, err := source.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if b2 != nil {
		t.Error("expected nil batch after all rows consumed")
	}
}

func TestTableFuncReadJSON_Array(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	data := `[
		{"id": 1, "value": 10.5},
		{"id": 2, "value": 20.3}
	]`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	source, err := buildTableFunctionSource("read_json", []string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Init(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	b, err := source.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if b == nil {
		t.Fatal("expected batch")
	}
	rows := b.ToRows()
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["id"] != int64(1) {
		t.Errorf("expected id=1, got %v", rows[0]["id"])
	}
}

func TestTableFuncUnknown(t *testing.T) {
	_, err := buildTableFunctionSource("read_excel", []string{"file.xlsx"})
	if err == nil {
		t.Error("expected error for unknown table function")
	}
}

func TestTableFuncReadJSON_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("read_json", nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
}

func TestSplitURL(t *testing.T) {
	tests := []struct {
		url    string
		bucket string
		key    string
	}{
		{"https://example.com/path/to/file.json", "https://example.com", "path/to/file.json"},
		{"http://localhost:8080/data.json", "http://localhost:8080", "data.json"},
		{"https://api.github.com", "https://api.github.com", ""},
	}
	for _, tt := range tests {
		b, k := splitURL(tt.url)
		if b != tt.bucket || k != tt.key {
			t.Errorf("splitURL(%q) = (%q, %q), want (%q, %q)", tt.url, b, k, tt.bucket, tt.key)
		}
	}
}
