// SPDX-License-Identifier: MIT

package csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

const defaultBatchSize = 2048

// sampleSize is the number of leading data rows a column's type is
// inferred from. A value in a later row that does not parse as that type is
// a 22P02 (see buildBatch).
const sampleSize = 100

// errNotType and errOutOfRange are writeCSVValue's answers for a field that
// does not parse as the column's type, and for a number that parses but lies
// outside it — PostgreSQL's 22P02 and 22003 for the same text.
var (
	errNotType    = errors.New("value does not parse as the column's type")
	errOutOfRange = errors.New("value is out of range for the column's type")
)

// Locator is implemented by an input that is several files read as one
// stream (read_csv over a glob): Segment names the file holding the input
// offset, where that file starts, and an offset before which every later
// offset still lies in the same file.
type Locator interface {
	Segment(offset int64) (file string, start, next int64)
}

var (
	ipv4Re            = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)
	timestampPatterns = []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
)

// ReaderConfig controls CSV parsing behavior.
type ReaderConfig struct {
	Delimiter rune // field delimiter (default: ',')
	HasHeader bool // whether the first row is a header (default: true)
}

// DefaultConfig returns sensible defaults for CSV reading.
func DefaultConfig() ReaderConfig {
	return ReaderConfig{
		Delimiter: ',',
		HasHeader: true,
	}
}

// Reader reads CSV data into columnar RecordBatches.
type Reader struct {
	schema   []parquet.Column
	colIdx   map[string]int
	rows     [][]string // all data rows (excluding header) — used for []byte path
	offset   int
	readRows int         // data rows already built into batches
	cr       *csv.Reader // streaming csv reader — used for io.Reader path

	// starts are the input offsets of the buffered rows (streaming path).
	// loc, for a glob, names the file an offset lies in; seg* follow the file
	// of the current row so a refusal names that file and its own row.
	starts      []int64
	loc         Locator
	segFile     string
	segStart    int64
	segNext     int64
	segRowsSeen int
}

// NewReader creates a CSV reader from raw bytes with the given config.
func NewReader(data []byte, cfg ReaderConfig) (*Reader, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return &Reader{}, nil
	}

	cr := csv.NewReader(bytes.NewReader(trimmed))
	cr.FieldsPerRecord = -1 // allow variable field counts
	if cfg.Delimiter != 0 {
		cr.Comma = cfg.Delimiter
	}

	allRows, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parsing CSV: %w", err)
	}
	if len(allRows) == 0 {
		return &Reader{}, nil
	}

	var header []string
	var dataRows [][]string

	if cfg.HasHeader {
		header = allRows[0]
		dataRows = allRows[1:]
	} else {
		// Generate column names: col0, col1, ...
		numCols := len(allRows[0])
		header = make([]string, numCols)
		for i := range header {
			header[i] = fmt.Sprintf("col%d", i)
		}
		dataRows = allRows
	}

	if len(dataRows) == 0 {
		// Header only, no data
		schema := make([]parquet.Column, len(header))
		for i, name := range header {
			schema[i] = parquet.Column{Name: name, Type: parquet.TypeString}
		}
		return &Reader{schema: schema, colIdx: makeColIdx(schema)}, nil
	}

	// Infer schema from sample rows
	schema := inferCSVSchema(header, dataRows)
	return &Reader{
		schema: schema,
		colIdx: makeColIdx(schema),
		rows:   dataRows,
	}, nil
}

// NewStreamReader creates a streaming CSV reader from an io.Reader.
// Reads the header and a sample of rows for schema inference, then streams
// remaining rows on demand via Next(). Memory usage is O(batch_size) instead
// of O(total_rows).
func NewStreamReader(r io.Reader, cfg ReaderConfig) (*Reader, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true
	if cfg.Delimiter != 0 {
		cr.Comma = cfg.Delimiter
	}

	// Read header
	firstRow, err := cr.Read()
	if err != nil {
		if err == io.EOF {
			return &Reader{}, nil
		}
		return nil, fmt.Errorf("reading CSV header: %w", err)
	}

	var header []string
	var sampleRows [][]string

	if cfg.HasHeader {
		header = make([]string, len(firstRow))
		copy(header, firstRow)
	} else {
		header = make([]string, len(firstRow))
		for i := range header {
			header[i] = fmt.Sprintf("col%d", i)
		}
		row := make([]string, len(firstRow))
		copy(row, firstRow)
		sampleRows = append(sampleRows, row)
	}

	// Row offsets are kept only for a glob, where they name a row's file.
	loc, _ := r.(Locator)
	var starts []int64
	if loc != nil && !cfg.HasHeader {
		starts = append(starts, 0)
	}
	// Read sample rows for schema inference (up to sampleSize)
	for len(sampleRows) < sampleSize {
		start := cr.InputOffset()
		record, err := cr.Read()
		if err != nil {
			break // EOF or error — use what we have
		}
		row := make([]string, len(record))
		copy(row, record)
		sampleRows = append(sampleRows, row)
		if loc != nil {
			starts = append(starts, start)
		}
	}

	if len(sampleRows) == 0 && len(header) > 0 {
		schema := make([]parquet.Column, len(header))
		for i, name := range header {
			schema[i] = parquet.Column{Name: name, Type: parquet.TypeString}
		}
		return &Reader{schema: schema, colIdx: makeColIdx(schema)}, nil
	}

	schema := inferCSVSchema(header, sampleRows)
	// ReuseRecord was on for sampling; turn off a fresh reader wrapping isn't
	// possible, but the sample rows are already copied. The cr will continue
	// streaming from where it left off.
	cr.ReuseRecord = false

	return &Reader{
		schema: schema,
		colIdx: makeColIdx(schema),
		rows:   sampleRows, // buffered sample rows returned first
		starts: starts,
		cr:     cr, // then stream remaining from here
		loc:    loc,
	}, nil
}

// Schema returns the inferred schema.
func (r *Reader) Schema() []parquet.Column {
	return r.schema
}

// Next returns the next batch of rows as a RecordBatch.
func (r *Reader) Next() (*batch.RecordBatch, error) {
	// If we have buffered rows (from []byte path or streaming sample), use those first
	if r.offset < len(r.rows) {
		end := r.offset + defaultBatchSize
		if end > len(r.rows) {
			end = len(r.rows)
		}
		chunk := r.rows[r.offset:end]
		var starts []int64
		if r.starts != nil {
			starts = r.starts[r.offset:end]
		}
		r.offset = end
		return r.buildBatch(chunk, starts)
	}

	// If we have a streaming csv.Reader, read the next batch from it
	if r.cr != nil {
		chunk, starts, err := r.readStreamBatch()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			return nil, nil // EOF
		}
		return r.buildBatch(chunk, starts)
	}

	return nil, nil
}

// readStreamBatch reads up to defaultBatchSize rows from the streaming csv.Reader.
func (r *Reader) readStreamBatch() ([][]string, []int64, error) {
	var rows [][]string
	var starts []int64
	for len(rows) < defaultBatchSize {
		start := r.cr.InputOffset()
		record, err := r.cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, fmt.Errorf("reading CSV row: %w", err)
		}
		row := make([]string, len(record))
		copy(row, record)
		rows = append(rows, row)
		if r.loc != nil {
			starts = append(starts, start)
		}
	}
	return rows, starts, nil
}

// buildBatch creates a RecordBatch from a slice of string rows.
//
// Every vector of the batch is numRows long (NewRecordBatch) and row ranges
// over the chunk, so no write below indexes past a vector; a field reaches a
// typed vector only through writeCSVValue's parse.
func (r *Reader) buildBatch(chunk [][]string, starts []int64) (*batch.RecordBatch, error) {
	numRows := len(chunk)
	b := batch.NewRecordBatch(r.schema, numRows)

	for row, fields := range chunk {
		if r.loc != nil && starts != nil && starts[row] >= r.segNext {
			r.enterSegment(starts[row], r.readRows+row+1)
		}
		for col, sc := range r.schema {
			if col >= len(fields) {
				b.Columns[col].Nulls.SetNull(row)
				if needsBytesNull(sc.Type) {
					b.Columns[col].BytesData.Set(row, nil)
				}
				continue
			}
			val := fields[col]
			if val == "" {
				b.Columns[col].Nulls.SetNull(row)
				if needsBytesNull(sc.Type) {
					b.Columns[col].BytesData.Set(row, nil)
				}
				continue
			}
			err := writeCSVValue(b.Columns[col], row, val, sc.Type)
			if err == nil {
				continue
			}
			// Inside the sample a field that does not parse keeps the
			// NULL it has always read as (a "true" in a column the sample
			// widened to bigint); past it the field is refused, as
			// PostgreSQL's COPY refuses it.
			if !errors.Is(err, errNotType) && !errors.Is(err, errOutOfRange) {
				return nil, err
			}
			if inputRow := r.readRows + row + 1; inputRow > sampleSize {
				return nil, r.refusal(err, inputRow, sc, val)
			}
		}
	}
	r.readRows += numRows
	return b, nil
}

// enterSegment moves the file tracking to the file holding off, the input
// offset of data row inputRow: a new file's first row makes every earlier
// row belong to earlier files.
func (r *Reader) enterSegment(off int64, inputRow int) {
	file, start, next := r.loc.Segment(off)
	if file != r.segFile || start != r.segStart {
		r.segFile, r.segStart = file, start
		r.segRowsSeen = inputRow - 1
	}
	r.segNext = next
}

// refusal is the error for field val of column sc in data row inputRow past
// the sample, with PostgreSQL's SQLSTATE for the same text in COPY: 22003 for
// a number outside the type, 22007 for a timestamp, 22P02 otherwise. Across
// a glob it names the file and the row within it. The caller (read_csv)
// prefixes the reader and the input.
func (r *Reader) refusal(err error, inputRow int, sc parquet.Column, val string) error {
	where := fmt.Sprintf("row %d column %q", inputRow-r.segRowsSeen, sc.Name)
	sample := fmt.Sprintf("the file's first %d rows", sampleSize)
	if r.segFile != "" {
		where = r.segFile + " " + where
		sample = fmt.Sprintf("the first %d rows of the input", sampleSize)
	}
	if errors.Is(err, errOutOfRange) {
		return sqlerr.New("22003", "%s: value %q is out of range for type %s", where, val, sqlTypeName(sc.Type))
	}
	code := "22P02"
	if sc.Type == parquet.TypeTimestamp {
		code = "22007" // PostgreSQL's invalid_datetime_format
	}
	return sqlerr.New(code, "%s: value %q (%s) is not of type %s (the column's type was inferred from %s)",
		where, val, sqlTypeName(detectStringType(val)), sqlTypeName(sc.Type), sample)
}

// writeCSVValue parses val as typ into vec at row. A field that does not
// parse is set NULL and answers errNotType; the caller decides whether that
// is the NULL (inside the sample) or a refusal (past it).
func writeCSVValue(vec *batch.Vector, row int, val string, typ parquet.TypeID) error {
	switch typ {
	// Bool, bigint and double precision read a field with PostgreSQL's own
	// input grammar for the type (the kernel's, which CAST uses): surrounding
	// whitespace, t/f/y/n/on/off prefixes, 0x/0o/0b and digit underscores,
	// NaN/Infinity, and 22003 for a number the type cannot hold. The plain
	// spelling is tried first so the common field pays one strconv call.
	case parquet.TypeBool:
		switch val {
		case "true", "TRUE", "True":
			vec.BoolData[row] = true
		case "false", "FALSE", "False":
			vec.BoolData[row] = false
		default:
			v, ok := kernel.ParseBoolText(val)
			if !ok {
				vec.Nulls.SetNull(row)
				return errNotType
			}
			vec.BoolData[row] = v
		}
	case parquet.TypeInt64:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			var st kernel.NumConstStatus
			if n, st = kernel.IntLitText(val); st != kernel.NumConstOK {
				vec.Nulls.SetNull(row)
				return numStatusError(st)
			}
		}
		vec.Int64Data[row] = n
	case parquet.TypeFloat64:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil || f == 0 || strings.IndexByte(val, '_') >= 0 {
			// Re-read by PostgreSQL's grammar: a 0 (Go reads a nonzero
			// 1e-400 as 0, PostgreSQL as 22003) and an underscore (Go's
			// float syntax allows 1_000, PostgreSQL's float8in does not).
			var st kernel.NumConstStatus
			if f, st = kernel.FloatLitText(val, 64); st != kernel.NumConstOK {
				vec.Nulls.SetNull(row)
				return numStatusError(st)
			}
		}
		vec.Float64Data[row] = f
	case parquet.TypeIPv4:
		ip := net.ParseIP(val)
		if ip == nil {
			vec.Nulls.SetNull(row)
			return errNotType
		}
		ip4 := ip.To4()
		if ip4 == nil {
			vec.Nulls.SetNull(row)
			return errNotType
		}
		vec.Int64Data[row] = int64(uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3]))
	case parquet.TypeTimestamp:
		if isPGSpace(val[0]) || isPGSpace(val[len(val)-1]) {
			val = strings.Trim(val, " \t\n\v\f\r") // PostgreSQL ignores surrounding whitespace
		}
		for _, layout := range timestampPatterns {
			if t, err := time.Parse(layout, val); err == nil {
				vec.Int64Data[row] = t.UnixMicro()
				return nil
			}
		}
		vec.Nulls.SetNull(row)
		return errNotType
	default: // TypeString and everything else
		vec.BytesData.Set(row, []byte(val))
	}
	return nil
}

// isPGSpace is C isspace, the whitespace PostgreSQL's input functions skip.
func isPGSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

func numStatusError(st kernel.NumConstStatus) error {
	if st == kernel.NumConstRange {
		return errOutOfRange
	}
	return errNotType
}

func inferCSVSchema(header []string, rows [][]string) []parquet.Column {
	sample := rows[:min(sampleSize, len(rows))]

	cols := make([]parquet.Column, len(header))
	for i, name := range header {
		cols[i] = parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true}

		// Detect type from non-empty sample values
		var detected parquet.TypeID
		first := true
		for _, row := range sample {
			if i >= len(row) || row[i] == "" {
				continue
			}
			t := detectStringType(row[i])
			if first {
				detected = t
				first = false
			} else {
				detected = promoteType(detected, t)
			}
		}
		if !first {
			cols[i].Type = detected
		}
	}
	return cols
}

// detectStringType is the inference's type for one sampled field. A number
// is recognised with the SAME PostgreSQL input functions writeCSVValue reads
// a field with (the kernel's int8in/float8in), so a spelling the sample
// types as bigint or double precision is read as that type, to the same
// value, in every later row too: ' 5', 0x1F and 1_000 are bigint; 1e-400
// and 1e400 (outside float8) and 1_000.5 are text. Only the six true/false
// spellings infer boolean — a narrower set than the reader accepts (it takes
// t, yes, on, 1 …), never a wider one, so a 0/1 column stays bigint.
func detectStringType(s string) parquet.TypeID {
	// Try bool
	switch s {
	case "true", "false", "TRUE", "FALSE", "True", "False":
		return parquet.TypeBool
	}

	// Try integer, then float, by PostgreSQL's grammar.
	if _, st := kernel.IntLitText(s); st == kernel.NumConstOK {
		return parquet.TypeInt64
	}
	if _, st := kernel.FloatLitText(s, 64); st == kernel.NumConstOK {
		return parquet.TypeFloat64
	}

	// Try IPv4
	if ipv4Re.MatchString(s) {
		if ip := net.ParseIP(s); ip != nil && ip.To4() != nil {
			return parquet.TypeIPv4
		}
	}

	// Try timestamp
	for _, layout := range timestampPatterns {
		if _, err := time.Parse(layout, s); err == nil {
			return parquet.TypeTimestamp
		}
	}

	return parquet.TypeString
}

func promoteType(a, b parquet.TypeID) parquet.TypeID {
	if a == b {
		return a
	}
	// int64 + float64 -> float64
	if isNumeric(a) && isNumeric(b) {
		return parquet.TypeFloat64
	}
	// bool + numeric -> numeric
	if a == parquet.TypeBool && isNumeric(b) {
		return b
	}
	if b == parquet.TypeBool && isNumeric(a) {
		return a
	}
	// Everything else falls back to string
	return parquet.TypeString
}

// sqlTypeName is the SQL name of a type the reader infers, as the relation
// declares it to a client.
func sqlTypeName(t parquet.TypeID) string {
	switch t {
	case parquet.TypeBool:
		return "boolean"
	case parquet.TypeInt64:
		return "bigint"
	case parquet.TypeFloat64:
		return "double precision"
	case parquet.TypeString:
		return "text"
	case parquet.TypeIPv4:
		return "inet"
	case parquet.TypeTimestamp:
		return "timestamp"
	}
	return strings.ToLower(t.String())
}

func isNumeric(t parquet.TypeID) bool {
	return t == parquet.TypeInt64 || t == parquet.TypeFloat64 ||
		t == parquet.TypeInt32 || t == parquet.TypeFloat32
}

func needsBytesNull(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeString, parquet.TypeBytes, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeUUID:
		return true
	}
	return false
}

func makeColIdx(schema []parquet.Column) map[string]int {
	m := make(map[string]int, len(schema))
	for i, c := range schema {
		m[c.Name] = i
	}
	return m
}
