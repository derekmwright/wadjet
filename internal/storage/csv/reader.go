package csv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"time"

	"github.com/derekmwright/caelum/internal/engine/batch"
	"github.com/derekmwright/caelum/internal/storage/parquet"
)

const defaultBatchSize = 2048

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
	schema []parquet.Column
	colIdx map[string]int
	rows   [][]string // all data rows (excluding header) — used for []byte path
	offset int
	cr     *csv.Reader // streaming csv reader — used for io.Reader path
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

	// Read sample rows for schema inference (up to 100)
	const sampleSize = 100
	for len(sampleRows) < sampleSize {
		record, err := cr.Read()
		if err != nil {
			break // EOF or error — use what we have
		}
		row := make([]string, len(record))
		copy(row, record)
		sampleRows = append(sampleRows, row)
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
		cr:     cr,         // then stream remaining from here
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
		r.offset = end
		return r.buildBatch(chunk), nil
	}

	// If we have a streaming csv.Reader, read the next batch from it
	if r.cr != nil {
		chunk, err := r.readStreamBatch()
		if err != nil {
			return nil, err
		}
		if len(chunk) == 0 {
			return nil, nil // EOF
		}
		return r.buildBatch(chunk), nil
	}

	return nil, nil
}

// readStreamBatch reads up to defaultBatchSize rows from the streaming csv.Reader.
func (r *Reader) readStreamBatch() ([][]string, error) {
	var rows [][]string
	for len(rows) < defaultBatchSize {
		record, err := r.cr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading CSV row: %w", err)
		}
		row := make([]string, len(record))
		copy(row, record)
		rows = append(rows, row)
	}
	return rows, nil
}

// buildBatch creates a RecordBatch from a slice of string rows.
func (r *Reader) buildBatch(chunk [][]string) *batch.RecordBatch {
	numRows := len(chunk)
	b := batch.NewRecordBatch(r.schema, numRows)

	for row, fields := range chunk {
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
			writeCSVValue(b.Columns[col], row, val, sc.Type)
		}
	}
	return b
}

func writeCSVValue(vec *batch.Vector, row int, val string, typ parquet.TypeID) {
	switch typ {
	case parquet.TypeBool:
		switch val {
		case "true", "TRUE", "True", "1", "yes", "YES":
			vec.BoolData[row] = true
		default:
			vec.BoolData[row] = false
		}
	case parquet.TypeInt64:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Int64Data[row] = n
	case parquet.TypeFloat64:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Float64Data[row] = f
	case parquet.TypeIPv4:
		ip := net.ParseIP(val)
		if ip == nil {
			vec.Nulls.SetNull(row)
			return
		}
		ip4 := ip.To4()
		if ip4 == nil {
			vec.Nulls.SetNull(row)
			return
		}
		vec.Int64Data[row] = int64(uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3]))
	case parquet.TypeTimestamp:
		for _, layout := range timestampPatterns {
			if t, err := time.Parse(layout, val); err == nil {
				vec.Int64Data[row] = t.UnixMicro()
				return
			}
		}
		vec.Nulls.SetNull(row)
	default: // TypeString and everything else
		vec.BytesData.Set(row, []byte(val))
	}
}

func inferCSVSchema(header []string, rows [][]string) []parquet.Column {
	sampleSize := 100
	if len(rows) < sampleSize {
		sampleSize = len(rows)
	}
	sample := rows[:sampleSize]

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

func detectStringType(s string) parquet.TypeID {
	// Try bool
	switch s {
	case "true", "false", "TRUE", "FALSE", "True", "False":
		return parquet.TypeBool
	}

	// Try integer
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return parquet.TypeInt64
	}

	// Try float
	if _, err := strconv.ParseFloat(s, 64); err == nil {
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
