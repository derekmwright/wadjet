// SPDX-License-Identifier: MIT

package physical

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"database/sql"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	csvreader "github.com/derekmwright/wadjet/internal/storage/csv"
	"github.com/derekmwright/wadjet/internal/storage/dbscan"
	jsonreader "github.com/derekmwright/wadjet/internal/storage/json"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/lib/pq"              // PostgreSQL driver
)

// withColumnAliases applies a FROM item's COLUMN-ALIAS LIST — `FROM
// read_json(…) [AS] f(k, v)` — to a table function's output, POSITIONALLY:
// fewer names rename a prefix (`unnest(…) WITH ORDINALITY AS u(v)` publishes v
// and ordinality), more names than the relation has is `42P10 table "f" has N
// columns available but M columns specified` (PostgreSQL §7.2.1.4).
//
// It is applied HERE rather than lowered in the parser to the derived-table
// spelling a NAMED relation's list takes (plansql.lowerNamedRelationColumnAliases):
// a READER's column list is not knowable until it has read its input —
// read_json infers it from the file — and that plan-time rename refuses with
// "renames the columns of a `SELECT *` this planner did not expand" for
// exactly that reason. This is the layer with the width FOR A READER; a
// function whose signature declares its columns is renamed at plan time
// instead (applyFuncColumnAliases, #1210). Before this, the list was parsed and
// then dropped for every function but unnest, so `SELECT k FROM read_json(…)
// AS f(k, v)` answered NULL for every row (#1184).
//
// The boundary: a READER that produces NO batch (an empty file) is never
// measured against its list, so an over-long list there answers zero rows
// where PostgreSQL raises 42P10.
func withColumnAliases(src exec.Source, aliases []string, relName string) exec.Source {
	if len(aliases) == 0 {
		return src
	}
	if relName == "" {
		relName = "table"
	}
	return &aliasedTableFuncSource{src: src, aliases: aliases, relName: relName}
}

type aliasedTableFuncSource struct {
	src     exec.Source
	aliases []string
	relName string
}

func (s *aliasedTableFuncSource) Init(ctx context.Context) error { return s.src.Init(ctx) }

func (s *aliasedTableFuncSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	b, err := s.src.Next(ctx)
	if err != nil || b == nil {
		return b, err
	}
	if len(s.aliases) > len(b.Schema) {
		return nil, sqlerr.New("42P10",
			"table %q has %d columns available but %d columns specified",
			s.relName, len(b.Schema), len(s.aliases))
	}
	// The schema is COPIED rather than renamed in place: the source keeps its
	// own schema across calls (the CSV and JSON readers index into it while
	// decoding), and a batch may be pooled and handed back to it.
	sch := make([]parquet.Column, len(b.Schema))
	copy(sch, b.Schema)
	for i, name := range s.aliases {
		sch[i].Name = name
	}
	b.Schema = sch
	return b, nil
}

func (s *aliasedTableFuncSource) Close() error { return s.src.Close() }

// withDeclaredSchema makes a table function that produced NO batch still
// publish its columns, by emitting ONE empty batch carrying the schema its
// signature declares.
//
// A relation with no rows is still a relation: `SELECT * FROM
// generate_series(1,0)` is zero rows of one column `generate_series` on
// PostgreSQL 17.11. Without this the pipeline saw no schema at all and the
// door raised XX000 "the result has no columns at all" — the engine failing to
// describe its own output where the answer is an empty result set. It wraps
// only a function whose columns tableFuncDeclaredSchema knows; one whose
// schema is its input's has nothing to publish when the input is empty, which
// is the boundary recorded on the differences page.
func withDeclaredSchema(src exec.Source, cols []parquet.Column) exec.Source {
	if len(cols) == 0 {
		return src
	}
	return &declaredSchemaSource{src: src, cols: cols}
}

type declaredSchemaSource struct {
	src      exec.Source
	cols     []parquet.Column
	produced bool
	done     bool
}

func (s *declaredSchemaSource) Init(ctx context.Context) error { return s.src.Init(ctx) }

func (s *declaredSchemaSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if s.done {
		return nil, nil
	}
	b, err := s.src.Next(ctx)
	if err != nil {
		return nil, err
	}
	if b != nil {
		s.produced = true
		return b, nil
	}
	s.done = true
	if s.produced {
		return nil, nil
	}
	s.produced = true
	empty := batch.NewRecordBatch(s.cols, 0)
	empty.Len = 0
	return empty, nil
}

func (s *declaredSchemaSource) Close() error { return s.src.Close() }

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
		return &csvTableFuncSource{path: expandHome(args[0]), NamedArgs: namedArgs}, nil
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
}

func (s *jsonTableFuncSource) Init(_ context.Context) error {
	// A glob is a SEQUENCE of files, each its own JSON document (fileinput).
	inputs, err := readerInputs(s.path)
	if err != nil {
		return fmt.Errorf("read_json: %w", err)
	}
	r, err := jsonreader.NewFilesReader(inputs)
	if err != nil {
		return fmt.Errorf("read_json: %w", err)
	}
	s.reader = r
	return nil
}

func (s *jsonTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	b, err := s.reader.Next()
	if err != nil {
		return nil, fmt.Errorf("read_json: %s: %w", s.path, err)
	}
	return b, nil
}

func (s *jsonTableFuncSource) Close() error {
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

// parquetTableFuncSource reads Parquet (a local file, a glob, or HTTP) and
// produces one batch per file. Uses readBatchDirect for column-at-a-time
// page reading (no row reconstruction); a local file is read through
// io.ReaderAt and never loaded whole.
//
// A GLOB is a SEQUENCE of files, each read through its own footer, one at a
// time (#1240: the matched files' bytes were concatenated, and a Parquet
// reader parsed the concatenation's tail as one file's footer — `invalid
// magic`). The relation's columns are the FIRST file's footer's — the same
// declaration the plan-time schema reads (parquetFooterSchema) — and every
// later file is held to it BY NAME: a column it lacks is 42703 and a column
// it declares at another type is 42804, each naming the file. Columns a later
// file adds are not part of the relation. A file's columns may come in any
// order; they are read by name.
type parquetTableFuncSource struct {
	path   string
	files  []string // local inputs, in name order
	next   int
	schema []parquet.Column
	mem    *batch.RecordBatch // an http(s) input, read whole
	done   bool
}

func (s *parquetTableFuncSource) Init(_ context.Context) error {
	if isURL(s.path) {
		// A URL is fetched into memory: a Parquet reader needs random access.
		data, err := fetchHTTP(s.path)
		if err != nil {
			return fmt.Errorf("read_parquet: %w", err)
		}
		reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return fmt.Errorf("read_parquet: %w", err)
		}
		b, err := readBatchDirect(reader, reader.Schema().Columns, nil)
		if err != nil {
			return fmt.Errorf("read_parquet: %w", err)
		}
		s.mem = b
		return nil
	}
	files, err := readerFiles(s.path)
	if err != nil {
		return fmt.Errorf("read_parquet: %w", err)
	}
	s.files = files
	// The relation's columns: the first file's footer. Opening it here also
	// reports an unreadable first file at Init, as a single file always was.
	return s.withFile(s.files[0], func(r *parquet.Reader) error {
		s.schema = r.Schema().Columns
		return nil
	})
}

// withFile opens one input file as a Parquet reader over io.ReaderAt, runs
// fn, and closes it.
func (s *parquetTableFuncSource) withFile(path string, fn func(*parquet.Reader) error) error {
	f, fi, err := openInputFile(path)
	if err != nil {
		return fmt.Errorf("read_parquet: %w", err)
	}
	defer f.Close()
	r, err := parquet.NewReader(f, fi.Size())
	if err != nil {
		if len(s.files) > 1 {
			return fmt.Errorf("read_parquet: %s: %w", path, err)
		}
		return fmt.Errorf("read_parquet: %w", err)
	}
	return fn(r)
}

func (s *parquetTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	if s.mem != nil || s.done {
		b := s.mem
		s.mem, s.done = nil, true
		return b, nil
	}
	for s.next < len(s.files) {
		path := s.files[s.next]
		s.next++
		var out *batch.RecordBatch
		err := s.withFile(path, func(r *parquet.Reader) error {
			if s.next > 1 {
				if err := sameParquetColumns(path, s.schema, r.Schema().Columns); err != nil {
					return err
				}
			}
			b, err := readBatchDirect(r, s.schema, nil)
			if err != nil {
				if len(s.files) > 1 {
					return fmt.Errorf("read_parquet: %s: %w", path, err)
				}
				return fmt.Errorf("read_parquet: %w", err)
			}
			out = b
			return nil
		})
		if err != nil {
			return nil, err
		}
		if out != nil && out.Len > 0 {
			return out, nil
		}
	}
	s.done = true
	return nil, nil
}

func (s *parquetTableFuncSource) Close() error { return nil }

// sameParquetColumns holds a later file of a Parquet glob to the relation's
// columns, which are the first file's: each by NAME, at the same type.
func sameParquetColumns(path string, want, have []parquet.Column) error {
	byName := make(map[string]parquet.Column, len(have))
	for _, c := range have {
		byName[c.Name] = c
	}
	for _, w := range want {
		h, ok := byName[w.Name]
		if !ok {
			return sqlerr.New("42703",
				"read_parquet: %s does not have column %q, which the glob's first file declares", path, w.Name)
		}
		if !sameColumnType(w, h) {
			return sqlerr.New("42804",
				"read_parquet: %s declares column %q as %s where the glob's first file declares %s",
				path, w.Name, parquetColumnTypeName(h), parquetColumnTypeName(w))
		}
	}
	return nil
}

// sameColumnType reports whether two columns carry the same type, nested
// element and field types included (nullability aside).
func sameColumnType(a, b parquet.Column) bool {
	if a.Type != b.Type || a.Precision != b.Precision || a.Scale != b.Scale || a.Dimension != b.Dimension {
		return false
	}
	if (a.ElementType == nil) != (b.ElementType == nil) {
		return false
	}
	if a.ElementType != nil && !sameColumnType(*a.ElementType, *b.ElementType) {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i].Name != b.Fields[i].Name || !sameColumnType(a.Fields[i], b.Fields[i]) {
			return false
		}
	}
	return true
}

func parquetColumnTypeName(c parquet.Column) string {
	if c.Type == parquet.TypeDecimal {
		return fmt.Sprintf("decimal(%d,%d)", c.Precision, c.Scale)
	}
	return strings.ToLower(c.Type.String())
}

// csvTableFuncSource reads a CSV file (local or HTTP) and produces batches.
// For local files, uses streaming to avoid loading the entire file into memory.
type csvTableFuncSource struct {
	path      string
	NamedArgs map[string]string
	reader    *csvreader.Reader
}

func (s *csvTableFuncSource) Init(_ context.Context) error {
	cfg := csvreader.DefaultConfig()
	if delim, ok := s.NamedArgs["delimiter"]; ok && len(delim) > 0 {
		cfg.Delimiter = rune(delim[0])
	}
	if delim, ok := s.NamedArgs["delim"]; ok && len(delim) > 0 {
		cfg.Delimiter = rune(delim[0])
	}
	if delim, ok := s.NamedArgs["sep"]; ok && len(delim) > 0 {
		cfg.Delimiter = rune(delim[0])
	}
	if hdr, ok := s.NamedArgs["header"]; ok {
		cfg.HasHeader = hdr == "true" || hdr == "TRUE" || hdr == "1"
	}

	// Every source shape streams, and a glob is a SEQUENCE of files the
	// reader decodes one at a time — each with its own header and its own
	// record grammar state — never their bytes run together (fileinput).
	inputs, err := readerInputs(s.path)
	if err != nil {
		return fmt.Errorf("read_csv: %w", err)
	}
	r, err := csvreader.NewFilesReader(inputs, cfg)
	if err != nil {
		return fmt.Errorf("read_csv: %w", err)
	}
	s.reader = r
	return nil
}

func (s *csvTableFuncSource) Next(_ context.Context) (*batch.RecordBatch, error) {
	b, err := s.reader.Next()
	if err != nil {
		return nil, fmt.Errorf("read_csv: %s: %w", s.path, err)
	}
	return b, nil
}

func (s *csvTableFuncSource) Close() error {
	if s.reader != nil {
		return s.reader.Close()
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

func isGlob(s string) bool {
	return strings.ContainsAny(s, "*?[")
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
// Emits a single column named "generate_series", declared by
// tableFuncDeclaredSchema — int4 when every argument fits int4 and int8
// otherwise, which is the overload PostgreSQL resolves for the same call.
//
// The step is the caller's, never flipped: a series whose bounds run the
// other way from its step is EMPTY. `generate_series(1,0)` and
// `generate_series(3,1)` answer no rows on 17.11 and answered a descending
// series here, because a positive default step was negated whenever
// start > stop — a wrong ROW SET, and the wrong shape of the function:
// `generate_series(1,0,-1)` is how a descending series is written.
type generateSeriesSource struct {
	start, stop, step int64
	// typ is the DECLARED column type, tableFuncDeclaredSchema's, so the
	// vector this source fills is the one the plan annotated and the wire
	// declares. A source that emitted int8 under an int4 declaration is a
	// carrier the reader cannot read (ADR-0024 §2a).
	typ  parquet.TypeID
	cur  int64
	done bool
	// exhausted records that the NEXT step would leave int64, which is the
	// end of the series on this carrier. Without it `s.cur += s.step` wraps
	// at the edge, the wrapped value sits on the other side of the bound and
	// neither exit is reached: generate_series(9223372036854775805,
	// 9223372036854775807) emitted 2048-row batches forever — every row after
	// the wrap a value the series does not contain — and the embedded query
	// was OOM-killed at 43 s where PostgreSQL 17.11 answers three rows.
	// Measured by the round-1 review.
	exhausted bool
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
			// PostgreSQL's own sentence and its own SQLSTATE (22023
			// invalid_parameter_value), measured on 17.11.
			return nil, sqlerr.New("22023", "step size cannot equal zero")
		}
	}
	typ := parquet.TypeInt64
	if cols, ok := tableFuncDeclaredSchema("generate_series", args, false); ok {
		typ = cols[0].Type
	}
	return &generateSeriesSource{start: start, stop: stop, step: step, typ: typ}, nil
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
		{Name: "generate_series", Type: s.typ},
	}

	// Generate up to DefaultBatchSize rows per batch
	n := 0
	vals := make([]int64, 0, batch.DefaultBatchSize)
	for n < batch.DefaultBatchSize && !s.exhausted {
		if s.step > 0 && s.cur > s.stop {
			break
		}
		if s.step < 0 && s.cur < s.stop {
			break
		}
		vals = append(vals, s.cur)
		n++
		next := s.cur + s.step
		if (s.step > 0 && next < s.cur) || (s.step < 0 && next > s.cur) {
			s.exhausted = true
			break
		}
		s.cur = next
	}

	if n == 0 {
		s.done = true
		return nil, nil
	}

	b := batch.NewRecordBatch(schema, n)
	if s.typ == parquet.TypeInt32 {
		for i, v := range vals {
			b.Columns[0].Int32Data[i] = int32(v)
		}
	} else {
		copy(b.Columns[0].Int64Data[:n], vals)
	}
	b.Len = n

	// Check if we've exhausted the series
	if s.exhausted {
		s.done = true
	}
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

// newUnnestSource builds the source for `unnest(…) [WITH ORDINALITY]`. The
// columns are PostgreSQL's own default names; a FROM item's column-alias list
// is applied over the source by withColumnAliases (#1184) and, since unnest
// declares its columns from the call, over that declaration at plan time by
// applyFuncColumnAliases (#1210).
func newUnnestSource(args []string, withOrdinality bool) (*unnestSource, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("unnest requires at least 1 argument")
	}
	colName := "unnest"
	ordColName := "ordinality"
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
		case parquet.TypeInt32:
			val, _ := strconv.ParseInt(v, 10, 64)
			b.Columns[0].Int32Data[i] = int32(val)
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
	// Try integer. An integer that fits int4 declares `integer`, and one that
	// does not declares `bigint` — the width PostgreSQL 17.11 resolves for
	// `unnest(ARRAY[1,2,3])`, whose column is `integer` and whose SUM is
	// therefore bigint and not numeric (#1211, ADR-0024). The width has to be
	// decided from the ARGUMENTS because it is read at plan time, by
	// tableFuncDeclaredSchema, before anything runs.
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n >= -2147483648 && n <= 2147483647 {
			return parquet.TypeInt32
		}
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
	method string  // "BERNOULLI" or "SYSTEM"
	pct    float64 // 0-100
	rng    *rand.Rand
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
