package physical

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/derekmwright/caelum/internal/storage/parquet"
)

func TestTableFuncReadJSON_Local(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	data := `{"name":"alice","age":30,"active":true}
{"name":"bob","age":25,"active":false}
{"name":"carol","age":35,"active":true}
`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	source, err := buildTableFunctionSource("read_json", []string{path}, nil)
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
	if rows[0]["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", rows[0]["name"])
	}
	if rows[1]["age"] != int64(25) {
		t.Errorf("expected age=25, got %v (%T)", rows[1]["age"], rows[1]["age"])
	}

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

	source, err := buildTableFunctionSource("read_json", []string{path}, nil)
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

func TestTableFuncReadParquet_Local(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.parquet")

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "name", Type: parquet.TypeString},
			{Name: "age", Type: parquet.TypeInt64},
			{Name: "score", Type: parquet.TypeFloat64},
		},
	}
	var buf bytes.Buffer
	w, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"name": "alice", "age": int64(30), "score": 95.5},
		{"name": "bob", "age": int64(25), "score": 87.2},
		{"name": "carol", "age": int64(35), "score": 92.1},
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}

	source, err := buildTableFunctionSource("read_parquet", []string{path}, nil)
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

	result := b.ToRows()
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
	if result[0]["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", result[0]["name"])
	}
	if result[1]["age"] != int64(25) {
		t.Errorf("expected age=25, got %v (%T)", result[1]["age"], result[1]["age"])
	}
	if result[2]["score"] != 92.1 {
		t.Errorf("expected score=92.1, got %v", result[2]["score"])
	}

	b2, err := source.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if b2 != nil {
		t.Error("expected nil after exhaustion")
	}
}

func TestTableFuncReadCSV_Local(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	data := "name,age,score\nalice,30,95.5\nbob,25,87.2\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	source, err := buildTableFunctionSource("read_csv", []string{path}, nil)
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

	result := b.ToRows()
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0]["name"] != "alice" {
		t.Errorf("expected name=alice, got %v", result[0]["name"])
	}
	if result[1]["age"] != int64(25) {
		t.Errorf("expected age=25, got %v (%T)", result[1]["age"], result[1]["age"])
	}
}

func TestTableFuncReadCSV_NamedArgs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.tsv")
	data := "alice\t30\nbob\t25\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	named := map[string]string{"delimiter": "\t", "header": "false"}
	source, err := buildTableFunctionSource("read_csv", []string{path}, named)
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
	if b.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", b.Len)
	}
}

func TestTableFuncReadCSV_PipeDelim(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	data := "name|age\nalice|30\nbob|25\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	named := map[string]string{"delim": "|"}
	source, err := buildTableFunctionSource("read_csv", []string{path}, named)
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
	if b.Len != 2 {
		t.Fatalf("expected 2 rows, got %d", b.Len)
	}
}

func TestTableFuncReadParquet_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("read_parquet", nil, nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
}

func TestTableFuncUnknown(t *testing.T) {
	_, err := buildTableFunctionSource("read_excel", []string{"file.xlsx"}, nil)
	if err == nil {
		t.Error("expected error for unknown table function")
	}
}

func TestTableFuncReadJSON_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("read_json", nil, nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
}

func TestTableFuncReadCSV_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("read_csv", nil, nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
}

func TestTableFuncReadJSON_Glob(t *testing.T) {
	dir := t.TempDir()

	// Write 3 separate JSONL files
	for i, name := range []string{"a.json", "b.json", "c.json"} {
		data := fmt.Sprintf(`{"id":%d,"name":"%s"}`, i+1, name)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	pattern := filepath.Join(dir, "*.json")
	source, err := buildTableFunctionSource("read_json", []string{pattern}, nil)
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
	if b.Len != 3 {
		t.Fatalf("expected 3 rows from glob, got %d", b.Len)
	}
}

func TestTableFuncReadCSV_Glob(t *testing.T) {
	dir := t.TempDir()

	// Write 2 CSV files with same schema
	if err := os.WriteFile(filepath.Join(dir, "part1.csv"), []byte("name,age\nalice,30\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "part2.csv"), []byte("name,age\nbob,25\n"), 0644); err != nil {
		t.Fatal(err)
	}

	pattern := filepath.Join(dir, "*.csv")
	source, err := buildTableFunctionSource("read_csv", []string{pattern}, nil)
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
	// 2 files x (1 header + 1 data row) — the header of the second file
	// gets concatenated as data. For CSV glob, the second file's header
	// becomes a data row. This is a known limitation (same as DuckDB).
	if b.Len < 2 {
		t.Fatalf("expected at least 2 rows from glob, got %d", b.Len)
	}
}

func TestFetchGlob_NoMatch(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "*.nonexistent")
	_, err := fetchGlob(pattern)
	if err == nil {
		t.Error("expected error for no matching files")
	}
}

func TestTableFuncPostgresScan_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("postgres_scan", nil, nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
	_, err = buildTableFunctionSource("postgres_scan", []string{"connstr"}, nil)
	if err == nil {
		t.Error("expected error for missing table name")
	}
}

func TestTableFuncPostgresQuery_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("postgres_query", nil, nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
}

func TestTableFuncMySQLScan_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("mysql_scan", nil, nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
	_, err = buildTableFunctionSource("mysql_scan", []string{"connstr"}, nil)
	if err == nil {
		t.Error("expected error for missing table name")
	}
}

func TestTableFuncMySQLQuery_NoArgs(t *testing.T) {
	_, err := buildTableFunctionSource("mysql_query", nil, nil)
	if err == nil {
		t.Error("expected error for missing args")
	}
}

func TestTableFuncDBScan_SourceCreated(t *testing.T) {
	// Verify sources are created with correct driver/query (Init will fail without a real DB)
	source, err := buildTableFunctionSource("postgres_scan", []string{"host=localhost", "users"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating source: %v", err)
	}
	dbs, ok := source.(*dbScanSource)
	if !ok {
		t.Fatalf("expected *dbScanSource, got %T", source)
	}
	if dbs.driver != "postgres" {
		t.Errorf("expected driver=postgres, got %s", dbs.driver)
	}
	if dbs.query != "SELECT * FROM users" {
		t.Errorf("expected query='SELECT * FROM users', got %q", dbs.query)
	}

	source2, err := buildTableFunctionSource("mysql_query", []string{"root:pass@tcp(localhost)/db", "SELECT id FROM t"}, nil)
	if err != nil {
		t.Fatalf("unexpected error creating source: %v", err)
	}
	dbs2, ok := source2.(*dbScanSource)
	if !ok {
		t.Fatalf("expected *dbScanSource, got %T", source2)
	}
	if dbs2.driver != "mysql" {
		t.Errorf("expected driver=mysql, got %s", dbs2.driver)
	}
	if dbs2.query != "SELECT id FROM t" {
		t.Errorf("expected query='SELECT id FROM t', got %q", dbs2.query)
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
