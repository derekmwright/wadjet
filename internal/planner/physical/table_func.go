package physical

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"database/sql"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	csvreader "github.com/derekmwright/wadjet/internal/storage/csv"
	"github.com/derekmwright/wadjet/internal/storage/dbscan"
	jsonreader "github.com/derekmwright/wadjet/internal/storage/json"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/lib/pq"              // PostgreSQL driver
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
		return &jsonTableFuncSource{path: expandHome(args[0])}, nil
	case "read_parquet":
		if len(args) < 1 {
			return nil, fmt.Errorf("read_parquet requires at least 1 argument (path or URL)")
		}
		return &parquetTableFuncSource{path: expandHome(args[0])}, nil
	case "read_csv", "read_csv_auto":
		if len(args) < 1 {
			return nil, fmt.Errorf("read_csv requires at least 1 argument (path or URL)")
		}
		return &csvTableFuncSource{path: expandHome(args[0]), namedArgs: namedArgs}, nil
	case "postgres_scan":
		if len(args) < 2 {
			return nil, fmt.Errorf("postgres_scan requires 2 arguments (connection_string, table_name)")
		}
		return &dbScanSource{driver: "postgres", connStr: args[0], query: fmt.Sprintf("SELECT * FROM %s", args[1])}, nil
	case "postgres_query":
		if len(args) < 2 {
			return nil, fmt.Errorf("postgres_query requires 2 arguments (connection_string, sql_query)")
		}
		return &dbScanSource{driver: "postgres", connStr: args[0], query: args[1]}, nil
	case "mysql_scan":
		if len(args) < 2 {
			return nil, fmt.Errorf("mysql_scan requires 2 arguments (connection_string, table_name)")
		}
		return &dbScanSource{driver: "mysql", connStr: args[0], query: fmt.Sprintf("SELECT * FROM %s", args[1])}, nil
	case "mysql_query":
		if len(args) < 2 {
			return nil, fmt.Errorf("mysql_query requires 2 arguments (connection_string, sql_query)")
		}
		return &dbScanSource{driver: "mysql", connStr: args[0], query: args[1]}, nil
	case "generate_series":
		return newGenerateSeriesSource(args)
	default:
		return nil, fmt.Errorf("unknown table function: %s", funcName)
	}
}

// expandHome resolves a leading "~/" against the user's home directory.
// Table function paths arrive inside a SQL string literal, so no shell ever
// expands them and every user who types a home-relative path would
// otherwise get a bare file-not-found (#303). Only the "~/" form is
// handled — "~user/..." is left alone — and if the home directory cannot
// be resolved the original path is returned so the underlying error still
// names the path the user wrote.
func expandHome(path string) string {
	if !strings.HasPrefix(path, "~/") {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	return filepath.Join(home, path[2:])
}

// jsonTableFuncSource reads JSON (local file, glob, or HTTP) and produces
// batches. Streams through the incremental byte scanner: the previous shape
// fetched the whole input into heap and then eagerly parsed EVERY batch up
// front — a second full-size columnar copy held while the raw bytes were
// still live, ~2-3× the input resident for any pgwire user (issue #130).
type jsonTableFuncSource struct {
	path   string
	reader *jsonreader.StreamReader
	closer io.Closer
}

func (s *jsonTableFuncSource) Init(_ context.Context) error {
	rc, err := openData(s.path)
	if err != nil {
		return fmt.Errorf("read_json: %w", err)
	}
	s.closer = rc
	r, err := jsonreader.NewStreamReader(rc)
	if err != nil {
		rc.Close()
		s.closer = nil
		return fmt.Errorf("read_json: parsing: %w", err)
	}
	s.reader = r
	return nil
}

func (s *jsonTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	return s.reader.Next()
}

func (s *jsonTableFuncSource) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

// parquetTableFuncSource reads a Parquet file (local or HTTP) and produces batches.
// Uses readBatchDirect for column-at-a-time page reading (no row reconstruction).
// For local files, opens the file as io.ReaderAt to avoid loading into memory.
type parquetTableFuncSource struct {
	path   string
	batch  *batch.RecordBatch
	done   bool
	closer io.Closer // file handle to close when done
}

func (s *parquetTableFuncSource) Init(_ context.Context) error {
	var ra io.ReaderAt
	var size int64

	if !isURL(s.path) && !isGlob(s.path) {
		// Local file: open as io.ReaderAt (zero-copy, no memory allocation)
		f, err := os.Open(s.path)
		if err != nil {
			return fmt.Errorf("read_parquet: %w", err)
		}
		s.closer = f
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return fmt.Errorf("read_parquet: stat: %w", err)
		}
		ra = f
		size = fi.Size()
	} else {
		// URLs and globs: fetch into memory
		data, err := fetchData(s.path)
		if err != nil {
			return fmt.Errorf("read_parquet: %w", err)
		}
		ra = bytes.NewReader(data)
		size = int64(len(data))
	}

	reader, err := parquet.NewReader(ra, size)
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

func (s *parquetTableFuncSource) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

// csvTableFuncSource reads a CSV file (local or HTTP) and produces batches.
// For local files, uses streaming to avoid loading the entire file into memory.
type csvTableFuncSource struct {
	path      string
	namedArgs map[string]string
	reader    *csvreader.Reader
	closer    io.Closer // file handle to close when done
}

func (s *csvTableFuncSource) Init(_ context.Context) error {
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

	// Every source shape streams: local files directly, globs through the
	// lazy multi-file reader, HTTP straight off the response body. The
	// URL/glob paths previously buffered the full input (globs 2×) before
	// the CSV reader saw a byte.
	rc, err := openData(s.path)
	if err != nil {
		return fmt.Errorf("read_csv: %w", err)
	}
	s.closer = rc
	r, err := csvreader.NewStreamReader(rc, cfg)
	if err != nil {
		rc.Close()
		s.closer = nil
		return fmt.Errorf("read_csv: %w", err)
	}
	s.reader = r
	return nil
}

func (s *csvTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	return s.reader.Next()
}

func (s *csvTableFuncSource) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

// openData opens a local file path, glob pattern, or HTTP/HTTPS URL as a
// stream. Globs expand to multiple files concatenated lazily (each opened
// only when the previous is exhausted, with a newline injected between
// files for JSONL/CSV continuity — same framing fetchGlob produced, without
// buffering every file at once). The caller owns the ReadCloser.
func openData(path string) (io.ReadCloser, error) {
	if isURL(path) {
		return openHTTP(path)
	}
	if isGlob(path) {
		matches, err := filepath.Glob(path)
		if err != nil {
			return nil, fmt.Errorf("glob %s: %w", path, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("glob %s: no matching files", path)
		}
		sort.Strings(matches)
		return &multiFileReadCloser{paths: matches}, nil
	}
	return os.Open(path)
}

// multiFileReadCloser streams a sorted glob expansion file-by-file. At most
// one file is open at a time; a '\n' is injected after any file that does
// not end with one (matching fetchGlob's concatenation framing).
type multiFileReadCloser struct {
	paths     []string
	idx       int
	cur       *os.File
	hadData   bool
	lastByte  byte
	pendingNL bool
}

func (m *multiFileReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if m.pendingNL {
			m.pendingNL = false
			p[0] = '\n'
			return 1, nil
		}
		if m.cur == nil {
			if m.idx >= len(m.paths) {
				return 0, io.EOF
			}
			f, err := os.Open(m.paths[m.idx])
			if err != nil {
				return 0, fmt.Errorf("reading %s: %w", m.paths[m.idx], err)
			}
			m.idx++
			m.cur = f
			m.hadData = false
		}
		n, err := m.cur.Read(p)
		if n > 0 {
			m.hadData = true
			m.lastByte = p[n-1]
			return n, nil
		}
		if err == io.EOF || err == nil {
			m.cur.Close()
			m.cur = nil
			if m.hadData && m.lastByte != '\n' {
				m.pendingNL = true
			}
			continue
		}
		m.cur.Close()
		m.cur = nil
		return 0, err
	}
}

func (m *multiFileReadCloser) Close() error {
	if m.cur != nil {
		err := m.cur.Close()
		m.cur = nil
		return err
	}
	return nil
}

// openHTTP returns the response body as a stream — no io.ReadAll, so a
// large remote file never lands in heap at once.
func openHTTP(url string) (io.ReadCloser, error) {
	store := objstore.NewHTTPStore(objstore.HTTPConfig{})
	bucket, key := splitURL(url)
	rc, _, err := store.Get(context.Background(), bucket, key)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", url, err)
	}
	return rc, nil
}

// fetchData retrieves data from a local file path, glob pattern, or
// HTTP/HTTPS URL fully into memory. Only read_parquet still uses this for
// URLs/globs — parquet needs random access (io.ReaderAt), so buffering is
// inherent there; JSON/CSV stream via openData.
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

// dbScanSource queries an external SQL database and produces columnar batches.
// Supports PostgreSQL (via lib/pq) and MySQL (via go-sql-driver/mysql).
type dbScanSource struct {
	driver  string // "postgres" or "mysql"
	connStr string
	query   string
	db      *sql.DB
	scanner *dbscan.Scanner
}

func (s *dbScanSource) Init(_ context.Context) error {
	db, err := sql.Open(s.driver, s.connStr)
	if err != nil {
		return fmt.Errorf("%s_scan: connect: %w", s.driver, err)
	}
	s.db = db

	rows, err := db.Query(s.query)
	if err != nil {
		db.Close()
		return fmt.Errorf("%s_scan: query: %w", s.driver, err)
	}

	scanner, err := dbscan.NewScanner(rows)
	if err != nil {
		rows.Close()
		db.Close()
		return fmt.Errorf("%s_scan: %w", s.driver, err)
	}
	s.scanner = scanner
	return nil
}

func (s *dbScanSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	return s.scanner.Next()
}

func (s *dbScanSource) Close() error {
	var firstErr error
	if s.scanner != nil {
		if err := s.scanner.Close(); err != nil {
			firstErr = err
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// generateSeriesSource produces rows for generate_series(start, stop[, step]).
// Emits a single int64 column named "generate_series".
type generateSeriesSource struct {
	start, stop, step int64
	cur               int64
	done              bool
}

func newGenerateSeriesSource(args []string) (*generateSeriesSource, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("generate_series requires 2-3 arguments (start, stop[, step])")
	}
	start, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("generate_series: invalid start %q: %w", args[0], err)
	}
	stop, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("generate_series: invalid stop %q: %w", args[1], err)
	}
	step := int64(1)
	if len(args) >= 3 {
		step, err = strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("generate_series: invalid step %q: %w", args[2], err)
		}
		if step == 0 {
			return nil, fmt.Errorf("generate_series: step cannot be zero")
		}
	}
	if start > stop && step > 0 {
		step = -step
	}
	return &generateSeriesSource{start: start, stop: stop, step: step}, nil
}

func (s *generateSeriesSource) Init(_ context.Context) error {
	s.cur = s.start
	s.done = false
	return nil
}

func (s *generateSeriesSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.done {
		return nil, nil
	}

	schema := []parquet.Column{
		{Name: "generate_series", Type: parquet.TypeInt64},
	}

	// Generate up to DefaultBatchSize rows per batch
	n := 0
	vals := make([]int64, 0, batch.DefaultBatchSize)
	for n < batch.DefaultBatchSize {
		if s.step > 0 && s.cur > s.stop {
			break
		}
		if s.step < 0 && s.cur < s.stop {
			break
		}
		vals = append(vals, s.cur)
		s.cur += s.step
		n++
	}

	if n == 0 {
		s.done = true
		return nil, nil
	}

	b := batch.NewRecordBatch(schema, n)
	copy(b.Columns[0].Int64Data[:n], vals)
	b.Len = n

	// Check if we've exhausted the series
	if s.step > 0 && s.cur > s.stop {
		s.done = true
	}
	if s.step < 0 && s.cur < s.stop {
		s.done = true
	}

	return b, nil
}

func (s *generateSeriesSource) Close() error { return nil }

// unnestSource expands a list of values into rows, one per element.
// Supports: SELECT * FROM unnest(1, 2, 3) AS u(val)
// With ordinality adds a 1-based index column.
type unnestSource struct {
	values         []string
	withOrdinality bool
	colName        string
	ordColName     string
	done           bool
}

func newUnnestSource(args []string, withOrdinality bool, colAliases []string) (*unnestSource, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("unnest requires at least 1 argument")
	}
	colName := "unnest"
	ordColName := "ordinality"
	if len(colAliases) > 0 {
		colName = colAliases[0]
	}
	if len(colAliases) > 1 {
		ordColName = colAliases[1]
	}
	return &unnestSource{
		values:         args,
		withOrdinality: withOrdinality,
		colName:        colName,
		ordColName:     ordColName,
	}, nil
}

func (s *unnestSource) Init(_ context.Context) error {
	s.done = false
	return nil
}

func (s *unnestSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.done {
		return nil, nil
	}
	s.done = true

	n := len(s.values)
	typ := inferUnnestType(s.values)

	schema := []parquet.Column{{Name: s.colName, Type: typ}}
	if s.withOrdinality {
		schema = append(schema, parquet.Column{Name: s.ordColName, Type: parquet.TypeInt64})
	}

	b := batch.NewRecordBatch(schema, n)
	b.Len = n

	// Fill the value column
	for i, raw := range s.values {
		v := strings.TrimSpace(raw)
		switch typ {
		case parquet.TypeInt64:
			val, _ := strconv.ParseInt(v, 10, 64)
			b.Columns[0].Int64Data[i] = val
		case parquet.TypeFloat64:
			val, _ := strconv.ParseFloat(v, 64)
			b.Columns[0].Float64Data[i] = val
		case parquet.TypeString:
			if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
				v = v[1 : len(v)-1]
			}
			b.Columns[0].BytesData.Set(i, []byte(v))
		}
	}

	// Fill ordinality column (1-based)
	if s.withOrdinality {
		for i := range n {
			b.Columns[1].Int64Data[i] = int64(i + 1)
		}
	}

	return b, nil
}

func (s *unnestSource) Close() error { return nil }

// inferUnnestType detects the type from the first value.
func inferUnnestType(vals []string) parquet.TypeID {
	if len(vals) == 0 {
		return parquet.TypeString
	}
	v := strings.TrimSpace(vals[0])
	// Quoted string
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return parquet.TypeString
	}
	// Try integer
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return parquet.TypeInt64
	}
	// Try float
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return parquet.TypeFloat64
	}
	return parquet.TypeString
}

// sampleOperator implements TABLESAMPLE BERNOULLI and SYSTEM sampling.
// BERNOULLI: row-level — each row independently included with probability p.
// SYSTEM: batch-level — each batch included/excluded as a whole with probability p.
type sampleOperator struct {
	method  string  // "BERNOULLI" or "SYSTEM"
	pct     float64 // 0-100
	rng     *rand.Rand
}

func newSampleOperator(method string, pct float64) *sampleOperator {
	return &sampleOperator{
		method: method,
		pct:    pct,
		rng:    rand.New(rand.NewSource(rand.Int63())),
	}
}

func (s *sampleOperator) Init(_ context.Context) error { return nil }

func (s *sampleOperator) Execute(_ context.Context, b *batch.RecordBatch) (*batch.RecordBatch, error) {
	if b == nil || b.Len == 0 {
		return b, nil
	}

	threshold := s.pct / 100.0

	if s.method == "SYSTEM" {
		// Block-level: include or exclude entire batch
		if s.rng.Float64() >= threshold {
			b.Len = 0
			return b, nil
		}
		return b, nil
	}

	// BERNOULLI: row-level sampling via selection vector
	sel := make([]uint32, 0, b.Len)
	for i := range b.Len {
		if s.rng.Float64() < threshold {
			sel = append(sel, uint32(i))
		}
	}
	b.Sel = sel
	b.Len = len(sel)
	return b, nil
}

func (s *sampleOperator) Close() error { return nil }

