package physical

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	csvreader "github.com/derekmwright/caelum/internal/storage/csv"
	jsonreader "github.com/derekmwright/caelum/internal/storage/json"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

// buildTableFunctionSource creates an exec.Source for a table function like
// read_json('path_or_url'). The source reads data on Init and produces
// RecordBatch results via Next.
func buildTableFunctionSource(funcName string, args []string, namedArgs map[string]string) (exec.Source, error) {
	switch funcName {
	case "read_json", "read_json_auto":
		if len(args) < 1 {
			return nil, fmt.Errorf("read_json requires at least 1 argument (path or URL)")
		}
		return &jsonTableFuncSource{path: args[0]}, nil
	case "read_parquet":
		if len(args) < 1 {
			return nil, fmt.Errorf("read_parquet requires at least 1 argument (path or URL)")
		}
		return &parquetTableFuncSource{path: args[0]}, nil
	case "read_csv", "read_csv_auto":
		if len(args) < 1 {
			return nil, fmt.Errorf("read_csv requires at least 1 argument (path or URL)")
		}
		return &csvTableFuncSource{path: args[0], namedArgs: namedArgs}, nil
	default:
		return nil, fmt.Errorf("unknown table function: %s", funcName)
	}
}

// jsonTableFuncSource reads a JSON file (local or HTTP) and produces batches.
// Uses the direct-to-columnar byte scanner (8x faster, 32x fewer allocs than
// the row-oriented reader).
type jsonTableFuncSource struct {
	path   string
	reader *jsonreader.ColumnarReader
}

func (s *jsonTableFuncSource) Init(_ context.Context) error {
	data, err := fetchData(s.path)
	if err != nil {
		return fmt.Errorf("read_json: %w", err)
	}
	r, err := jsonreader.NewColumnarReader(data)
	if err != nil {
		return fmt.Errorf("read_json: parsing: %w", err)
	}
	s.reader = r
	return nil
}

func (s *jsonTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	return s.reader.Next()
}

func (s *jsonTableFuncSource) Close() error { return nil }

// parquetTableFuncSource reads a Parquet file (local or HTTP) and produces batches.
// Uses readBatchDirect for column-at-a-time page reading (no row reconstruction).
type parquetTableFuncSource struct {
	path  string
	batch *batch.RecordBatch
	done  bool
}

func (s *parquetTableFuncSource) Init(_ context.Context) error {
	data, err := fetchData(s.path)
	if err != nil {
		return fmt.Errorf("read_parquet: %w", err)
	}
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("read_parquet: %w", err)
	}
	schema := reader.Schema().Columns
	s.batch = readBatchDirect(reader, schema, nil)
	return nil
}

func (s *parquetTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.done || s.batch == nil {
		return nil, nil
	}
	s.done = true
	return s.batch, nil
}

func (s *parquetTableFuncSource) Close() error { return nil }

// csvTableFuncSource reads a CSV file (local or HTTP) and produces batches.
type csvTableFuncSource struct {
	path      string
	namedArgs map[string]string
	reader    *csvreader.Reader
}

func (s *csvTableFuncSource) Init(_ context.Context) error {
	data, err := fetchData(s.path)
	if err != nil {
		return fmt.Errorf("read_csv: %w", err)
	}
	cfg := csvreader.DefaultConfig()
	if delim, ok := s.namedArgs["delimiter"]; ok && len(delim) > 0 {
		cfg.Delimiter = rune(delim[0])
	}
	if delim, ok := s.namedArgs["delim"]; ok && len(delim) > 0 {
		cfg.Delimiter = rune(delim[0])
	}
	if delim, ok := s.namedArgs["sep"]; ok && len(delim) > 0 {
		cfg.Delimiter = rune(delim[0])
	}
	if hdr, ok := s.namedArgs["header"]; ok {
		cfg.HasHeader = hdr == "true" || hdr == "TRUE" || hdr == "1"
	}
	r, err := csvreader.NewReader(data, cfg)
	if err != nil {
		return fmt.Errorf("read_csv: %w", err)
	}
	s.reader = r
	return nil
}

func (s *csvTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	return s.reader.Next()
}

func (s *csvTableFuncSource) Close() error { return nil }

// fetchData retrieves data from a local file path, glob pattern, or HTTP/HTTPS URL.
// Glob patterns (e.g., "data/*.json") expand to multiple files whose contents
// are concatenated.
func fetchData(path string) ([]byte, error) {
	if isURL(path) {
		return fetchHTTP(path)
	}
	if isGlob(path) {
		return fetchGlob(path)
	}
	return os.ReadFile(path)
}

func isGlob(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

func fetchGlob(pattern string) ([]byte, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", pattern, err)
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("glob %s: no matching files", pattern)
	}
	sort.Strings(matches)

	var buf bytes.Buffer
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}
		buf.Write(data)
		// Ensure newline between files for JSONL/CSV concatenation
		if len(data) > 0 && data[len(data)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}
	return buf.Bytes(), nil
}

func isURL(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

func fetchHTTP(url string) ([]byte, error) {
	store := objstore.NewHTTPStore(objstore.HTTPConfig{})
	// Split URL into bucket (scheme+host) and key (path)
	bucket, key := splitURL(url)
	rc, _, err := store.Get(context.Background(), bucket, key)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", url, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading response from %s: %w", url, err)
	}
	return data, nil
}

// splitURL splits "https://example.com/path/to/file.json" into
// bucket="https://example.com" and key="path/to/file.json".
func splitURL(rawURL string) (bucket, key string) {
	// Find the third slash (after scheme://)
	idx := 0
	slashes := 0
	for i, c := range rawURL {
		if c == '/' {
			slashes++
			if slashes == 3 {
				idx = i
				break
			}
		}
	}
	if idx == 0 {
		return rawURL, ""
	}
	return rawURL[:idx], rawURL[idx+1:]
}

