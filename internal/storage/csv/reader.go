// SPDX-License-Identifier: MIT

package csv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/fileinput"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

const defaultBatchSize = 2048

// sampleSize is the number of leading data rows a column's type is
// inferred from. A value in a later row that does not parse as that type is
// a 22P02 — 22003 for a number out of range, 22007 for a timestamp (see
// buildBatch).
const sampleSize = 100

// errNotType and errOutOfRange are writeCSVValue's answers for a field that
// does not parse as the column's type, and for a number that parses but lies
// outside it — PostgreSQL's 22P02 and 22003 for the same text.
var (
	errNotType    = errors.New("value does not parse as the column's type")
	errOutOfRange = errors.New("value is out of range for the column's type")
)

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

// record is one data row: its fields, which of them are NULL (nil when
// none is), and where it came from — the file (named only across a glob)
// and its 1-based data row within that file, which is what a refusal names.
type record struct {
	fields []string
	nulls  []bool
	file   string
	row    int
}

// Reader reads CSV data into columnar RecordBatches.
//
// Its input is a SEQUENCE of files (fileinput): read_csv over a glob hands it
// the matched files in name order, and each is decoded on its own — its own
// record grammar state, its own line numbers, its own first record — with
// at most one open at a time. The schema is ONE across them: the header is
// the first file's, and the types are inferred from the first 100 data rows
// of the sequence (which cross into later files when the first is short).
// A later file whose first record repeats the header has it skipped; one
// whose first record does not is read whole, as data (a file split after
// its header). A later file whose values do not fit the inferred types is
// refused past the sample like any other row, naming that file and its row.
type Reader struct {
	schema   []parquet.Column
	colIdx   map[string]int
	rows     []record // buffered rows (the sample, or every row for NewReader)
	offset   int
	readRows int // data rows already built into batches
	named    bool

	cfg     ReaderConfig
	inputs  []fileinput.Input
	nextIn  int
	cur     io.ReadCloser
	curName string
	sc      *recordScanner
	curRows int
	first   bool     // the next record is the current file's first
	header  []string // the header record, once read (HasHeader)
	width   int      // fields per record; 0 until the first record fixes it
	perm    []int    // the current file's field for each header column; nil = in order
	done    bool
}

// NewReader creates a CSV reader from raw bytes with the given config. It
// reads every row up front, so a malformed input is refused here.
func NewReader(data []byte, cfg ReaderConfig) (*Reader, error) {
	trimmed := bytes.TrimSpace(data)
	r, err := NewStreamReader(bytes.NewReader(trimmed), cfg)
	if err != nil {
		return nil, err
	}
	for !r.done {
		rec, err := r.nextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parsing CSV: %w", err)
		}
		r.rows = append(r.rows, rec)
	}
	r.done = true
	return r, nil
}

// NewStreamReader creates a streaming CSV reader over one input. The
// caller keeps ownership of r.
func NewStreamReader(r io.Reader, cfg ReaderConfig) (*Reader, error) {
	return NewFilesReader(fileinput.Reader(r), cfg)
}

// NewFilesReader creates a streaming CSV reader over a sequence of files.
// It reads the header and a sample of rows for schema inference, then
// streams the rest on demand via Next(). Memory is O(batch size) plus one
// open file. Close releases the file it holds.
func NewFilesReader(inputs []fileinput.Input, cfg ReaderConfig) (*Reader, error) {
	r := &Reader{cfg: cfg, inputs: inputs, named: len(inputs) > 1}
	for _, in := range inputs {
		r.named = r.named || in.Name != ""
	}
	var sample []record
	for len(sample) < sampleSize {
		rec, err := r.nextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			r.Close()
			return nil, err
		}
		sample = append(sample, rec)
	}
	header := r.header
	if !cfg.HasHeader && r.width > 0 {
		header = make([]string, r.width)
		for i := range header {
			header[i] = fmt.Sprintf("col%d", i)
		}
	}
	if len(header) == 0 {
		return r, nil // an input with no record: no columns
	}
	if len(sample) == 0 {
		schema := make([]parquet.Column, len(header))
		for i, name := range header {
			schema[i] = parquet.Column{Name: name, Type: parquet.TypeString}
		}
		r.schema, r.colIdx = schema, makeColIdx(schema)
		return r, nil
	}
	r.schema = inferCSVSchema(header, sample)
	r.colIdx = makeColIdx(r.schema)
	r.rows = sample
	return r, nil
}

// nextRecord is the next data record of the sequence, opening the next file
// when one ends; io.EOF after the last.
func (r *Reader) nextRecord() (record, error) {
	for {
		if r.sc == nil {
			if r.done || r.nextIn >= len(r.inputs) {
				r.done = true
				return record{}, io.EOF
			}
			in := r.inputs[r.nextIn]
			r.nextIn++
			rc, err := in.Open()
			if err != nil {
				return record{}, err
			}
			r.cur, r.curName, r.curRows, r.first, r.perm = rc, in.Name, 0, true, nil
			r.sc = newRecordScanner(rc, r.cfg.Delimiter)
		}
		fields, nulls, line, err := r.sc.next()
		if err == io.EOF {
			r.closeCurrent()
			continue
		}
		if err != nil {
			return record{}, r.inFile(err)
		}
		// A BLANK line (one empty unquoted field) is skipped unless the
		// relation has exactly one column, where it is that column's NULL as
		// COPY reads it. COPY refuses it with 22P04 in a wider file; this
		// reader skipped it on every path through v0.24.0 — a trailing empty
		// line ends nearly every exported file — and keeps that (ADR-0012 §5).
		if len(fields) == 1 && nulls != nil && r.width != 1 {
			continue
		}
		if r.first {
			r.first = false
			if r.cfg.HasHeader {
				if r.header == nil {
					r.header = fields
					r.width = len(fields)
					continue
				}
				// A later file that repeats the header has it skipped (#1247).
				if slices.Equal(fields, r.header) {
					continue
				}
				// …and one whose header names the same columns in another
				// ORDER is read by name: its fields are mapped onto the first
				// file's columns. Read positionally, its header was a data row
				// and every value landed in the other column.
				if perm := headerPermutation(r.header, fields); perm != nil {
					r.perm = perm
					continue
				}
				// A first record that names some of the header's columns but
				// not the same set is a header that disagrees: a typed
				// refusal naming the file, as COPY … HEADER MATCH refuses a
				// header that is not the table's. One that names none of
				// them is data — a file split after its header (arc RP).
				if len(fields) == len(r.header) && sharesName(r.header, fields) {
					return record{}, r.inFile(sqlerr.New("22P04",
						"line %d: the header %q does not name the columns of the first file's header %q",
						line, fields, r.header))
				}
			}
		}
		if r.width == 0 {
			r.width = len(fields)
		}
		if len(fields) > r.width && emptyTail(fields, nulls, r.width) {
			// A trailing delimiter (`x,y,`): the extra fields are empty and
			// unquoted, no value is lost, and exporters write it — kept as
			// base read it (ADR-0012 §5). COPY refuses it with 22P04.
			fields = fields[:r.width]
			if nulls != nil {
				nulls = nulls[:r.width]
			}
		}
		if len(fields) != r.width {
			// A SHORT record stays refused although base NULL-padded it: a
			// stray unquoted line break splits one record into two short
			// ones, and padding them answers rows the file does not hold.
			// A LONG one with a value past the header loses that value.
			return record{}, r.inFile(r.widthError(len(fields), line))
		}
		if r.perm != nil {
			fields, nulls = permute(fields, nulls, r.perm)
		}
		r.curRows++
		return record{fields: fields, nulls: nulls, file: r.curName, row: r.curRows}, nil
	}
}

// headerPermutation is, when rec names exactly the header's columns in
// another order (and the header's names are distinct), rec's field index for
// each header column; nil otherwise.
func headerPermutation(header, rec []string) []int {
	if len(rec) != len(header) {
		return nil
	}
	at := make(map[string]int, len(rec))
	for i, name := range rec {
		if _, dup := at[name]; dup {
			return nil
		}
		at[name] = i
	}
	perm := make([]int, len(header))
	for i, name := range header {
		j, ok := at[name]
		if !ok {
			return nil
		}
		perm[i] = j
		delete(at, name)
	}
	return perm
}

func sharesName(header, rec []string) bool {
	for _, f := range rec {
		if slices.Contains(header, f) {
			return true
		}
	}
	return false
}

// permute puts a record's fields in the header's order.
func permute(fields []string, nulls []bool, perm []int) ([]string, []bool) {
	out := make([]string, len(perm))
	var outNulls []bool
	if nulls != nil {
		outNulls = make([]bool, len(perm))
	}
	for i, j := range perm {
		out[i] = fields[j]
		if nulls != nil {
			outNulls[i] = nulls[j]
		}
	}
	return out, outNulls
}

// emptyTail reports whether every field past the first width is an
// unquoted empty field (NULL).
func emptyTail(fields []string, nulls []bool, width int) bool {
	if nulls == nil {
		return false
	}
	for i := width; i < len(fields); i++ {
		if !nulls[i] {
			return false
		}
	}
	return true
}

// widthError is PostgreSQL's COPY refusal of a record whose field count is
// not the relation's: 22P04, as `COPY … (FORMAT csv)` raises it.
func (r *Reader) widthError(n, line int) error {
	if n > r.width {
		return sqlerr.New("22P04", "line %d: extra data after last expected column", line)
	}
	name := fmt.Sprintf("col%d", n)
	if n < len(r.header) {
		name = r.header[n]
	}
	return sqlerr.New("22P04", "line %d: missing data for column %q", line, name)
}

// inFile names the current file in an error about it, across a glob.
func (r *Reader) inFile(err error) error {
	if r.curName == "" {
		return err
	}
	return fmt.Errorf("%s: %w", r.curName, err)
}

func (r *Reader) closeCurrent() {
	if r.cur != nil {
		r.cur.Close()
	}
	r.cur, r.sc = nil, nil
}

// Close releases the file the reader holds open, if any.
func (r *Reader) Close() error {
	r.closeCurrent()
	r.done = true
	return nil
}

// Schema returns the inferred schema.
func (r *Reader) Schema() []parquet.Column {
	return r.schema
}

// Next returns the next batch of rows as a RecordBatch.
func (r *Reader) Next() (*batch.RecordBatch, error) {
	if r.schema == nil {
		return nil, nil
	}
	if r.offset < len(r.rows) {
		end := min(r.offset+defaultBatchSize, len(r.rows))
		chunk := r.rows[r.offset:end]
		r.offset = end
		return r.buildBatch(chunk)
	}
	if r.done {
		return nil, nil
	}
	chunk := make([]record, 0, defaultBatchSize)
	for len(chunk) < defaultBatchSize {
		rec, err := r.nextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		chunk = append(chunk, rec)
	}
	if len(chunk) == 0 {
		return nil, nil
	}
	return r.buildBatch(chunk)
}

// buildBatch creates a RecordBatch from a slice of rows.
//
// Every vector of the batch is numRows long (NewRecordBatch) and row ranges
// over the chunk, so no write below indexes past a vector; a field reaches a
// typed vector only through writeCSVValue's parse. Every record has the
// schema's width (nextRecord refuses one that does not).
func (r *Reader) buildBatch(chunk []record) (*batch.RecordBatch, error) {
	numRows := len(chunk)
	b := batch.NewRecordBatch(r.schema, numRows)

	for row, rec := range chunk {
		for col, sc := range r.schema {
			if rec.nulls != nil && rec.nulls[col] {
				b.Columns[col].Nulls.SetNull(row)
				if needsBytesNull(sc.Type) {
					b.Columns[col].BytesData.Set(row, nil)
				}
				continue
			}
			val := rec.fields[col]
			err := writeCSVValue(b.Columns[col], row, val, sc.Type)
			if err == nil {
				continue
			}
			if !errors.Is(err, errNotType) && !errors.Is(err, errOutOfRange) {
				return nil, err
			}
			// The sample's type reads every field the sample holds
			// (inferCSVSchema types a field with the parse used here, and a
			// mix no one type reads is text), so a field that does not
			// parse is past the sample, and is refused as COPY refuses it.
			// It was a NULL inside the sample through v0.24.0.
			return nil, r.refusal(err, rec, sc, val)
		}
	}
	r.readRows += numRows
	return b, nil
}

// refusal is the error for field val of column sc in rec, a row past the
// sample, with PostgreSQL's SQLSTATE for the same text in COPY: 22003 for
// a number outside the type, 22007 for a timestamp, 22P02 otherwise. Across
// a glob it names the file and the row within it. The caller (read_csv)
// prefixes the reader and the input.
func (r *Reader) refusal(err error, rec record, sc parquet.Column, val string) error {
	where := fmt.Sprintf("row %d column %q", rec.row, sc.Name)
	sample := fmt.Sprintf("the file's first %d rows", sampleSize)
	if r.named {
		if rec.file != "" {
			where = rec.file + " " + where
		}
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

// writeCSVValue parses val as typ into vec at row. A field that cannot parse
// is set NULL and returns errNotType, or errOutOfRange when outside the type.
// The caller returns a typed refusal wherever the record occurs, including
// inside the inference sample (ADR-0039 §3).
func writeCSVValue(vec *batch.Vector, row int, val string, typ parquet.TypeID) error {
	if val == "" && typ != parquet.TypeString {
		// A QUOTED empty field is the empty string, not NULL, and no type
		// but text reads it: PostgreSQL's COPY refuses "" for a bigint.
		vec.Nulls.SetNull(row)
		return errNotType
	}
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
				vec.Int64Data[row] = parquet.WallClockMillis(t)
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

// plainDecimal reports whether s is a number in plain decimal notation:
// optional surrounding whitespace, an optional sign, digits with an
// optional fraction, and an optional exponent — no radix prefix, no
// underscore, at least one digit.
func plainDecimal(s string) bool {
	i, j := 0, len(s)
	for i < j && isPGSpace(s[i]) {
		i++
	}
	for j > i && isPGSpace(s[j-1]) {
		j--
	}
	t := s[i:j]
	if t != "" && (t[0] == '+' || t[0] == '-') {
		t = t[1:]
	}
	digits := 0
	k := 0
	for k < len(t) && t[k] >= '0' && t[k] <= '9' {
		k, digits = k+1, digits+1
	}
	if k < len(t) && t[k] == '.' {
		k++
		for k < len(t) && t[k] >= '0' && t[k] <= '9' {
			k, digits = k+1, digits+1
		}
	}
	if digits == 0 {
		return false
	}
	if k < len(t) && (t[k] == 'e' || t[k] == 'E') {
		k++
		if k < len(t) && (t[k] == '+' || t[k] == '-') {
			k++
		}
		exp := 0
		for k < len(t) && t[k] >= '0' && t[k] <= '9' {
			k, exp = k+1, exp+1
		}
		if exp == 0 {
			return false
		}
	}
	return k == len(t)
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

func inferCSVSchema(header []string, rows []record) []parquet.Column {
	sample := rows[:min(sampleSize, len(rows))]

	cols := make([]parquet.Column, len(header))
	for i, name := range header {
		cols[i] = parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true}

		// Detect the type from the sample's non-NULL values. A quoted empty
		// field is a value (the empty string), and it is text.
		var detected parquet.TypeID
		first := true
		for _, row := range sample {
			if i >= len(row.fields) || (row.nulls != nil && row.nulls[i]) {
				continue
			}
			t := detectStringType(row.fields[i])
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

// detectStringType is the inference's type for one sampled field.
//
// A number is recognised only in its PLAIN decimal spelling (plainDecimal:
// surrounding whitespace, a sign, digits, a fraction, an exponent) or as
// NaN/Infinity, and then typed and range-checked with the SAME PostgreSQL
// input functions writeCSVValue reads later rows with (the kernel's int8in,
// then float8in). So a spelling the sample types is read as that type, to
// the same value, in every later row, and one neither type holds (1e-400,
// 1e400) is text. The prefixes 0x/0o/0b and digit underscores make the
// field text: such columns are usually identifiers or flag strings (a hex
// id, TCP flags 0x12), and reading them as a number would reinterpret what
// the file says — though a number column meeting 0x1F past the sample
// reads 31, as PostgreSQL's COPY into bigint does. Only the six true/false
// spellings infer boolean, a subset of what the reader accepts, so a 0/1
// column stays bigint.
func detectStringType(s string) parquet.TypeID {
	// Try bool
	switch s {
	case "true", "false", "TRUE", "FALSE", "True", "False":
		return parquet.TypeBool
	}

	// Try integer, then float: a plain decimal (or NaN/Infinity), typed by
	// PostgreSQL's grammar.
	if plainDecimal(s) {
		if _, st := kernel.IntLitText(s); st == kernel.NumConstOK {
			return parquet.TypeInt64
		}
		if _, st := kernel.FloatLitText(s, 64); st == kernel.NumConstOK {
			return parquet.TypeFloat64
		}
	} else if _, ok := kernel.FloatSpecialText(s); ok {
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
	// Everything else — a boolean beside a number included — is text: no
	// number type reads `true` (PostgreSQL's COPY refuses it for a bigint),
	// and a column the sample typed must read every sampled field (#1260).
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
