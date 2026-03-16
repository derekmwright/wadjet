package physical

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/engine/exec"
	jsonreader "github.com/derekmwright/caelum/internal/storage/json"
	"github.com/derekmwright/caelum/internal/storage/objstore"
)

// buildTableFunctionSource creates an exec.Source for a table function like
// read_json('path_or_url'). The source reads data on Init and produces
// RecordBatch results via Next.
func buildTableFunctionSource(funcName string, args []string) (exec.Source, error) {
	switch funcName {
	case "read_json", "read_json_auto":
		if len(args) < 1 {
			return nil, fmt.Errorf("read_json requires at least 1 argument (path or URL)")
		}
		return &jsonTableFuncSource{path: args[0]}, nil
	default:
		return nil, fmt.Errorf("unknown table function: %s", funcName)
	}
}

// jsonTableFuncSource reads a JSON file (local or HTTP) and produces batches.
type jsonTableFuncSource struct {
	path   string
	reader *jsonreader.Reader
}

func (s *jsonTableFuncSource) Init(_ context.Context) error {
	data, err := fetchData(s.path)
	if err != nil {
		return fmt.Errorf("read_json: %w", err)
	}
	r, err := jsonreader.NewReaderFromBytes(data)
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

// fetchData retrieves data from a local file path or an HTTP/HTTPS URL.
func fetchData(path string) ([]byte, error) {
	if isURL(path) {
		return fetchHTTP(path)
	}
	return os.ReadFile(path)
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

