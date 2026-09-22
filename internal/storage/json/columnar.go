// SPDX-License-Identifier: MIT

package json

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ColumnarReader reads JSON directly into columnar vectors using a custom
// tokenizer that avoids encoding/json's per-token boxing overhead.
// Schema inference still uses encoding/json's Token API for correctness,
// but the main parse pass operates on raw bytes.
type ColumnarReader struct {
	schema  []parquet.Column
	colIdx  map[string]int // column name → schema index
	batches []*batch.RecordBatch
	pos     int
}

// NewColumnarReader creates a columnar reader from raw JSON bytes.
func NewColumnarReader(data []byte) (*ColumnarReader, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return &ColumnarReader{}, nil
	}

	isArray := trimmed[0] == '['

	// Phase 1: schema inference using token scanning
	schema, err := inferSchemaTokens(trimmed, isArray, defaultSampleSize)
	if err != nil {
		return nil, fmt.Errorf("schema inference: %w", err)
	}
	if len(schema) == 0 {
		return &ColumnarReader{}, nil
	}

	colIdx := make(map[string]int, len(schema))
	for i, col := range schema {
		colIdx[col.Name] = i
	}

	// Phase 2: direct-to-columnar parse using raw byte scanner
	batches, err := parseColumnarDirect(trimmed, isArray, schema, colIdx)
	if err != nil {
		return nil, fmt.Errorf("columnar parse: %w", err)
	}

	return &ColumnarReader{
		schema:  schema,
		colIdx:  colIdx,
		batches: batches,
	}, nil
}

// NewColumnarReaderFromStream reads all data from r and creates a columnar reader.
func NewColumnarReaderFromStream(r io.Reader) (*ColumnarReader, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading json stream: %w", err)
	}
	return NewColumnarReader(data)
}

// Schema returns the inferred schema.
func (r *ColumnarReader) Schema() []parquet.Column {
	return r.schema
}

// Next returns the next batch. Returns nil when exhausted.
func (r *ColumnarReader) Next() (*batch.RecordBatch, error) {
	if r.pos >= len(r.batches) {
		return nil, nil
	}
	b := r.batches[r.pos]
	r.pos++
	return b, nil
}

// ---------------------------------------------------------------------------
// Direct-to-columnar byte scanner
// ---------------------------------------------------------------------------

// jsonScanner is a minimal JSON scanner that operates on raw bytes.
// It extracts string keys and values without allocating intermediate objects.
type jsonScanner struct {
	data []byte
	pos  int
	// fileRow is the 1-based row of the object being scanned, and sampled
	// the number of leading rows the schema was inferred from: a value in a
	// row past sampled is checked against its column before it is written.
	fileRow int
	sampled int
	// file and fileRowBase name the input file the row sits in when the
	// input is several files read as one stream (a glob): the message's row
	// is fileRow-fileRowBase of that file. Empty for a single input.
	file        string
	fileRowBase int
}

func (s *jsonScanner) skipWhitespace() {
	for s.pos < len(s.data) {
		c := s.data[s.pos]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			s.pos++
		} else {
			return
		}
	}
}

func (s *jsonScanner) peek() byte {
	s.skipWhitespace()
	if s.pos >= len(s.data) {
		return 0
	}
	return s.data[s.pos]
}

func (s *jsonScanner) advance() byte {
	s.skipWhitespace()
	if s.pos >= len(s.data) {
		return 0
	}
	b := s.data[s.pos]
	s.pos++
	return b
}

// readString reads a JSON string value and returns it WITHOUT allocating
// a new string if there are no escape sequences. Uses unsafe.String for
// zero-copy when possible.
func (s *jsonScanner) readString() (string, error) {
	if s.advance() != '"' {
		return "", fmt.Errorf("expected '\"' at pos %d", s.pos-1)
	}
	start := s.pos
	hasEscape := false
	for s.pos < len(s.data) {
		c := s.data[s.pos]
		if c == '\\' {
			hasEscape = true
			s.pos += 2 // skip escaped char
			continue
		}
		if c == '"' {
			val := s.data[start:s.pos]
			s.pos++ // skip closing quote
			if hasEscape {
				// Fall back to stdlib for escape handling
				var unescaped string
				err := json.Unmarshal(append([]byte{'"'}, append(val, '"')...), &unescaped)
				if err != nil {
					return string(val), nil
				}
				return unescaped, nil
			}
			if len(val) == 0 {
				return "", nil // &val[0] of the empty string "" indexes past it
			}
			return unsafe.String(&val[0], len(val)), nil
		}
		s.pos++
	}
	return "", fmt.Errorf("unterminated string at pos %d", start)
}

// readStringBytes is like readString but returns the raw bytes without
// going through a string allocation — used for writing directly to BytesColumn.
func (s *jsonScanner) readStringBytes() ([]byte, bool, error) {
	if s.advance() != '"' {
		return nil, false, fmt.Errorf("expected '\"' at pos %d", s.pos-1)
	}
	start := s.pos
	hasEscape := false
	for s.pos < len(s.data) {
		c := s.data[s.pos]
		if c == '\\' {
			hasEscape = true
			s.pos += 2
			continue
		}
		if c == '"' {
			val := s.data[start:s.pos]
			s.pos++
			return val, hasEscape, nil
		}
		s.pos++
	}
	return nil, false, fmt.Errorf("unterminated string at pos %d", start)
}

// readNumber reads a JSON number and returns the raw bytes.
func (s *jsonScanner) readNumber() []byte {
	start := s.pos
	for s.pos < len(s.data) {
		c := s.data[s.pos]
		if (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E' {
			s.pos++
		} else {
			break
		}
	}
	return s.data[start:s.pos]
}

// readRawValue extracts the raw JSON bytes for an entire value (string,
// number, object, array, bool, null). Used for nested types where we
// delegate parsing to encoding/json.
func (s *jsonScanner) readRawValue() []byte {
	s.skipWhitespace()
	start := s.pos
	c := s.peek()
	switch c {
	case '"':
		s.readStringBytes()
		return s.data[start:s.pos]
	case '{':
		s.advance()
		depth := 1
		for depth > 0 && s.pos < len(s.data) {
			switch s.data[s.pos] {
			case '{':
				depth++
			case '}':
				depth--
			case '"':
				s.pos++
				for s.pos < len(s.data) && s.data[s.pos] != '"' {
					if s.data[s.pos] == '\\' {
						s.pos++
					}
					s.pos++
				}
			}
			s.pos++
		}
		return s.data[start:s.pos]
	case '[':
		s.advance()
		depth := 1
		for depth > 0 && s.pos < len(s.data) {
			switch s.data[s.pos] {
			case '[':
				depth++
			case ']':
				depth--
			case '"':
				s.pos++
				for s.pos < len(s.data) && s.data[s.pos] != '"' {
					if s.data[s.pos] == '\\' {
						s.pos++
					}
					s.pos++
				}
			}
			s.pos++
		}
		return s.data[start:s.pos]
	case 't':
		s.pos += 4
		return s.data[start:s.pos]
	case 'f':
		s.pos += 5
		return s.data[start:s.pos]
	case 'n':
		s.pos += 4
		return s.data[start:s.pos]
	default: // number
		s.readNumber()
		return s.data[start:s.pos]
	}
}

// skipValue skips a JSON value (used for unknown columns).
func (s *jsonScanner) skipValue() {
	c := s.peek()
	switch c {
	case '"':
		s.readString()
	case '{':
		s.advance()
		depth := 1
		for depth > 0 && s.pos < len(s.data) {
			switch s.data[s.pos] {
			case '{':
				depth++
			case '}':
				depth--
			case '"':
				s.pos++
				for s.pos < len(s.data) && s.data[s.pos] != '"' {
					if s.data[s.pos] == '\\' {
						s.pos++
					}
					s.pos++
				}
			}
			s.pos++
		}
	case '[':
		s.advance()
		depth := 1
		for depth > 0 && s.pos < len(s.data) {
			switch s.data[s.pos] {
			case '[':
				depth++
			case ']':
				depth--
			case '"':
				s.pos++
				for s.pos < len(s.data) && s.data[s.pos] != '"' {
					if s.data[s.pos] == '\\' {
						s.pos++
					}
					s.pos++
				}
			}
			s.pos++
		}
	case 't': // true
		s.pos += 4
	case 'f': // false
		s.pos += 5
	case 'n': // null
		s.pos += 4
	default: // number
		s.readNumber()
	}
}

// parseColumnarDirect reads all JSON objects directly into typed column vectors.
func parseColumnarDirect(data []byte, isArray bool, schema []parquet.Column, colIdx map[string]int) (out []*batch.RecordBatch, err error) {
	// This is user data crossing Vector.SetValue (the nested-column path):
	// a JSON value the schema's vector cannot hold raises #361's typed
	// panic, and an ingest of malformed data must answer with a decode
	// error, never take the process down.
	defer func() {
		if r := recover(); r != nil {
			te, ok := r.(*batch.TypeMismatchError)
			if !ok {
				panic(r)
			}
			out, err = nil, fmt.Errorf("decoding JSON row: %w", te)
		}
	}()
	sc := &jsonScanner{data: data, sampled: defaultSampleSize}

	if isArray {
		if sc.advance() != '[' {
			return nil, fmt.Errorf("expected '['")
		}
	}

	var batches []*batch.RecordBatch
	seen := make([]bool, len(schema))

	for {
		if sc.peek() == 0 || sc.peek() == ']' {
			break
		}

		rb := batch.NewRecordBatch(schema, defaultBatchSize)
		row := 0

		for row < defaultBatchSize {
			c := sc.peek()
			if c == 0 || c == ']' {
				break
			}
			if c == ',' {
				sc.advance()
				continue
			}
			if c != '{' {
				break
			}

			sc.fileRow++
			if err := scanObjectInto(sc, rb, row, schema, colIdx, seen); err != nil {
				if sqlerr.StateOf(err) != "" {
					return nil, err // already names its row
				}
				return nil, fmt.Errorf("row %d: %w", sc.fileRow, err)
			}
			row++
		}

		if row == 0 {
			break
		}

		if row < defaultBatchSize {
			rb.Len = row
			for _, col := range rb.Columns {
				col.Len = row
			}
		}
		batches = append(batches, rb)
	}

	return batches, nil
}

// scanObjectInto reads one JSON object and writes values directly to vectors.
func scanObjectInto(sc *jsonScanner, rb *batch.RecordBatch, row int, schema []parquet.Column, colIdx map[string]int, seen []bool) error {
	if sc.advance() != '{' {
		return fmt.Errorf("expected '{'")
	}
	// Every vector of rb is rb.Len long (NewRecordBatch), so this one check
	// bounds every write below. A value only reaches a vector that holds its
	// kind — a string a string-like column, a nested value a nested or text
	// column — which is what kept a string out of a bigint vector's empty
	// BytesData (the index-out-of-range of #1243).
	if row < 0 || row >= rb.Len {
		return fmt.Errorf("JSON row index %d outside batch length %d", row, rb.Len)
	}

	for i := range seen {
		seen[i] = false
	}

	for {
		c := sc.peek()
		if c == '}' {
			sc.advance()
			break
		}
		if c == ',' {
			sc.advance()
			continue
		}

		// Read key
		key, err := sc.readString()
		if err != nil {
			return err
		}

		// Expect ':'
		if sc.advance() != ':' {
			return fmt.Errorf("expected ':'")
		}

		colI, exists := colIdx[key]
		if !exists {
			sc.skipValue()
			continue
		}

		seen[colI] = true
		vec := rb.Columns[colI]
		colType := schema[colI].Type

		// Read value directly into typed vector
		valByte := sc.peek()
		switch {
		case valByte == 'n': // null
			sc.pos += 4
			// WriteNullAt advances offsets/children for bytes AND nested
			// columns — a bare SetNull on a nested column skipped its slot
			// and every later row read back shifted (live repro: a null
			// "meta" object made the next row read "alphabeta").
			vec.WriteNullAt(row)

		case valByte == '"': // string
			// Only a string-like column holds a string; past the sample an
			// inet or timestamp string must also parse, and the write's own
			// parse is the check (the value is re-read only to refuse it).
			if !isStringLike(colType) {
				return sc.refuseString(schema[colI])
			}
			start := sc.pos
			if !writeStringValue(sc, vec, row, colType) && sc.fileRow > sc.sampled {
				sc.pos = start
				return sc.refuseString(schema[colI])
			}

		case valByte == 't': // true
			if colType != parquet.TypeBool && colType != parquet.TypeString && sc.fileRow > sc.sampled {
				return sc.refusal(&mismatch{value: "true", observed: "boolean", want: colType}, schema[colI].Name)
			}
			sc.pos += 4
			vec.Nulls.SetValid(row)
			writeBoolTrue(vec, row, colType)

		case valByte == 'f': // false
			if colType != parquet.TypeBool && colType != parquet.TypeString && sc.fileRow > sc.sampled {
				return sc.refusal(&mismatch{value: "false", observed: "boolean", want: colType}, schema[colI].Name)
			}
			sc.pos += 5
			vec.Nulls.SetValid(row)
			writeBoolFalse(vec, row, colType)

		case valByte == '{' || valByte == '[': // nested object or array
			raw := sc.readRawValue()
			nested := colType == parquet.TypeArray || colType == parquet.TypeRow || colType == parquet.TypeMap
			if !nested && colType != parquet.TypeString {
				kind := "object"
				if valByte == '[' {
					kind = "array"
				}
				return sc.refusal(&mismatch{value: string(raw), observed: kind, want: colType}, schema[colI].Name)
			}
			if nested {
				if sc.fileRow > sc.sampled {
					// Checked on the raw bytes, allocation-free, so the value
					// is still decoded once, below.
					if m := sc.checkNested(raw, schema[colI]); m != nil {
						return sc.refusal(m, schema[colI].Name)
					}
				}
				var decoded any
				if err := json.Unmarshal(raw, &decoded); err != nil {
					vec.WriteNullAt(row)
				} else {
					// Coerced per the inferred schema BEFORE the write:
					// Vector.SetValue guards against values it cannot hold
					// (#361). A value past the sample was checked above; one
					// inside it keeps the NULL coerceToColumn gives a value it
					// cannot hold. Timestamp strings inside nested values
					// parse here exactly as the scalar path parses them.
					decoded = coerceToColumn(decoded, schema[colI])
					if decoded == nil {
						vec.WriteNullAt(row)
					} else {
						vec.Nulls.SetValid(row)
						vec.SetValue(row, decoded)
					}
				}
			} else {
				// A text column holding a nested value stores its JSON text.
				vec.Nulls.SetValid(row)
				vec.BytesData.Set(row, raw)
			}

		default: // number
			numBytes := sc.readNumber()
			vec.Nulls.SetValid(row)
			if !writeNumberValue(vec, row, colType, numBytes) && sc.fileRow > sc.sampled {
				m := checkNumber(numBytes, colType)
				if m == nil { // the write and the check disagree: refuse, never keep the 0
					m = &mismatch{value: string(numBytes), observed: "numeric", want: colType}
				}
				return sc.refusal(m, schema[colI].Name)
			}
		}
	}

	// Set nulls for missing columns
	for i, wasSeen := range seen {
		if !wasSeen {
			rb.Columns[i].WriteNullAt(row)
		}
	}

	return nil
}

// The checks below are the scanner's cold paths, kept out of scanObjectInto
// so the per-value loop stays as small as it was.

// refuseString refuses the string at the scanner for col.
func (sc *jsonScanner) refuseString(col parquet.Column) error {
	value, err := sc.readString()
	if err != nil {
		return fmt.Errorf("column %q: %w", col.Name, err)
	}
	m := checkValue(value, col)
	if m == nil { // a string that detects as the column's type yet did not store
		m = &mismatch{value: displayValue(value), observed: "text", want: col.Type}
	}
	return sc.refusal(m, col.Name)
}

// refusal is the 22P02 (or 22003/22007) for m in column name of the
// scanner's row, naming the file and its own row across a glob.
func (sc *jsonScanner) refusal(m *mismatch, name string) error {
	return m.refusal(name, sc.fileRow-sc.fileRowBase, sc.sampled, sc.file)
}

// checkNested runs checkRaw over raw with the scanner itself pointed at it
// (a scanner of its own would be an allocation per value), then restores it.
func (sc *jsonScanner) checkNested(raw []byte, col parquet.Column) *mismatch {
	data, pos := sc.data, sc.pos
	sc.data, sc.pos = raw, 0
	m := checkRaw(sc, col)
	sc.data, sc.pos = data, pos
	return m
}

// checkRaw checks the JSON value at s against col without decoding it,
// the raw-bytes twin of checkValue for the scanner: an ARRAY's elements and a
// ROW's fields recursively, a number by its spelling (checkNumber), a
// string by the column's own parse. It advances s past what it read; a
// malformed value answers nil and is left to the decode that follows.
func checkRaw(s *jsonScanner, col parquet.Column) *mismatch {
	s.skipWhitespace()
	start := s.pos
	c := s.peek()
	if c == 'n' || c == 0 {
		s.readRawValue()
		return nil
	}
	if col.Type == parquet.TypeString {
		s.readRawValue()
		return nil
	}
	switch c {
	case '[':
		if col.Type != parquet.TypeArray {
			s.readRawValue()
			return &mismatch{value: string(s.data[start:s.pos]), observed: "array", want: col.Type}
		}
		s.advance()
		for {
			s.skipWhitespace()
			switch s.peek() {
			case ']', 0:
				s.advance()
				return nil
			case ',':
				s.advance()
				continue
			}
			if col.ElementType == nil {
				s.readRawValue()
				continue
			}
			if m := checkRaw(s, *col.ElementType); m != nil {
				m.path = " element" + m.path
				return m
			}
		}
	case '{':
		if col.Type != parquet.TypeRow {
			s.readRawValue()
			return &mismatch{value: string(s.data[start:s.pos]), observed: "object", want: col.Type}
		}
		s.advance()
		for {
			s.skipWhitespace()
			switch s.peek() {
			case '}', 0:
				s.advance()
				return nil
			case ',':
				s.advance()
				continue
			}
			key, err := s.readString()
			if err != nil {
				return nil
			}
			s.skipWhitespace()
			if s.advance() != ':' {
				return nil
			}
			field := fieldNamed(col.Fields, key)
			if field == nil {
				s.skipWhitespace()
				s.readRawValue()
				continue
			}
			if m := checkRaw(s, *field); m != nil {
				m.path = fmt.Sprintf(" field %q", key) + m.path
				return m
			}
		}
	case '"':
		value, err := s.readString()
		if err != nil {
			return nil
		}
		return checkValue(value, col)
	case 't', 'f':
		raw := s.readRawValue()
		if col.Type == parquet.TypeBool {
			return nil
		}
		return &mismatch{value: string(raw), observed: "boolean", want: col.Type}
	default:
		return checkNumber(s.readNumber(), col.Type)
	}
}

func fieldNamed(fields []parquet.Column, name string) *parquet.Column {
	for i := range fields {
		if fields[i].Name == name {
			return &fields[i]
		}
	}
	return nil
}

// checkNumber checks a JSON number's spelling against a column type the way
// PostgreSQL's input function for that type reads the same text: a bigint
// takes an integer spelling (1.0 and 1e3 are 22P02) and one past its range
// is 22003; a double precision takes any number, and one it cannot hold
// (1e999, a nonzero 1e-400) is 22003. Any other column refuses a number.
// A number that fits costs one strconv call and no allocation.
func checkNumber(num []byte, want parquet.TypeID) *mismatch {
	if len(num) > 0 {
		text := unsafe.String(&num[0], len(num))
		switch want {
		case parquet.TypeInt64:
			if _, err := strconv.ParseInt(text, 10, 64); err == nil {
				return nil
			}
		case parquet.TypeFloat64:
			if f, err := strconv.ParseFloat(text, 64); err == nil && (f != 0 || floatTextIsZero(num)) {
				return nil
			}
		}
	}
	return numberMismatch(string(num), want)
}

// numberMismatch classifies a number checkNumber refused.
func numberMismatch(text string, want parquet.TypeID) *mismatch {
	observed := sqlTypeName(detectTokenType(json.Number(text)))
	m := &mismatch{value: text, observed: observed, want: want}
	switch want {
	case parquet.TypeInt64:
		if !strings.ContainsAny(text, ".eE") {
			if _, st := kernel.IntLitText(text); st == kernel.NumConstRange {
				m.code = "22003"
			}
		}
	case parquet.TypeFloat64:
		if _, st := kernel.FloatLitText(text, 64); st == kernel.NumConstRange {
			m.observed, m.code = "numeric", "22003"
		}
	}
	return m
}

// floatTextIsZero reports whether a JSON number's digits are all zero.
func floatTextIsZero(num []byte) bool {
	for _, c := range num {
		if c == 'e' || c == 'E' {
			return true
		}
		if c >= '1' && c <= '9' {
			return false
		}
	}
	return true
}

// coerceToColumn makes a json.Unmarshal'd value storable in col's vector,
// or returns nil for NULL. It exists because Vector.SetValue no longer
// swallows a value the vector cannot hold (#361) — before the guard a
// mismatched nested cell silently became 0 AND, for ARRAY/ROW shapes,
// desynced the column's offsets so later rows read back shifted.
//
// Values past the sample are checked before calling this conversion.
// Within the sample, incompatible values retain the existing NULL behavior.
// Values SetValue can already convert pass through untouched.
// TIMESTAMP strings are the one enrichment: they parse with the same
// patterns the scalar path uses; they used to be dropped for 0.
func coerceToColumn(val any, col parquet.Column) any {
	if val == nil {
		return nil
	}
	switch col.Type {
	case parquet.TypeArray:
		elems, ok := val.([]any)
		if !ok {
			return nil
		}
		if col.ElementType == nil {
			return elems
		}
		out := make([]any, len(elems))
		for i, e := range elems {
			out[i] = coerceToColumn(e, *col.ElementType)
		}
		return out
	case parquet.TypeRow:
		m, ok := val.(map[string]any)
		if !ok {
			return nil
		}
		if len(col.Fields) == 0 {
			return m
		}
		out := make(map[string]any, len(m))
		for _, f := range col.Fields {
			if fv, present := m[f.Name]; present {
				out[f.Name] = coerceToColumn(fv, f)
			}
		}
		return out
	case parquet.TypeString, parquet.TypeBytes:
		return val // SetValue coerces anything through its string form
	case parquet.TypeBool:
		switch val.(type) {
		case bool, float64:
			return val
		}
		return nil
	case parquet.TypeTimestamp:
		switch tv := val.(type) {
		case float64:
			return val
		case string:
			for _, layout := range timestampPatterns {
				if t, err := time.Parse(layout, tv); err == nil {
					return t.UnixMicro() // the scalar path's unit
				}
			}
			return nil
		}
		return nil
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64,
		parquet.TypePort, parquet.TypeProtocol, parquet.TypeDuration, parquet.TypeDecimal:
		if _, ok := val.(float64); ok { // every JSON number
			return val
		}
		if col.Type == parquet.TypeDecimal {
			if _, ok := val.(string); ok {
				return val
			}
		}
		return nil
	case parquet.TypeDate, parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR,
		parquet.TypeMAC, parquet.TypeUUID:
		// String forms only: SetValue parses them, and a parse failure is a
		// value-level miss, not a type mismatch. A JSON number has no
		// agreed meaning for any of these.
		if _, ok := val.(string); ok {
			return val
		}
		return nil
	}
	// TypeMap, TypeVector and anything else only arrive via an explicit
	// caller schema; pass through and let the guard name a mismatch.
	return val
}

// writeStringValue stores the string at the scanner into a string-like
// column and reports whether it did: an inet or timestamp column stores only
// a string that parses as one (the value otherwise reads 0, valid — the
// caller refuses it past the sample).
func writeStringValue(sc *jsonScanner, vec *batch.Vector, row int, colType parquet.TypeID) bool {
	vec.Nulls.SetValid(row)

	switch colType {
	case parquet.TypeIPv4:
		str, err := sc.readString()
		if err != nil {
			return false
		}
		if ip := net.ParseIP(str); ip != nil {
			if ip4 := ip.To4(); ip4 != nil {
				vec.Int64Data[row] = int64(binary.BigEndian.Uint32(ip4))
				return true
			}
		}
		return false

	case parquet.TypeTimestamp:
		str, err := sc.readString()
		if err != nil {
			return false
		}
		for _, layout := range timestampPatterns {
			if t, err := time.Parse(layout, str); err == nil {
				vec.Int64Data[row] = t.UnixMicro()
				return true
			}
		}
		return false

	default: // TypeString
		raw, hasEscape, err := sc.readStringBytes()
		if err != nil {
			return false
		}
		if hasEscape {
			var unescaped string
			json.Unmarshal(append([]byte{'"'}, append(raw, '"')...), &unescaped)
			vec.BytesData.Set(row, []byte(unescaped))
		} else {
			vec.BytesData.Set(row, raw)
		}
		return true
	}
}

func writeNumberValue(vec *batch.Vector, row int, colType parquet.TypeID, numBytes []byte) bool {
	if len(numBytes) == 0 {
		return false
	}
	switch colType {
	case parquet.TypeInt64:
		if i, err := strconv.ParseInt(unsafe.String(&numBytes[0], len(numBytes)), 10, 64); err == nil {
			vec.Int64Data[row] = i
			return true
		}
	case parquet.TypeFloat64:
		// Go reads a nonzero number below the smallest denormal as 0 with no
		// error; a 0 is therefore re-read by the caller's check past the
		// sample (checkNumber), which PostgreSQL reads as 22003.
		if f, err := strconv.ParseFloat(unsafe.String(&numBytes[0], len(numBytes)), 64); err == nil {
			vec.Float64Data[row] = f
			return f != 0 || floatTextIsZero(numBytes)
		}
	case parquet.TypeString:
		vec.BytesData.Set(row, numBytes)
		return true
	}
	return false
}

func writeBoolTrue(vec *batch.Vector, row int, colType parquet.TypeID) {
	switch colType {
	case parquet.TypeBool:
		vec.BoolData[row] = true
	case parquet.TypeInt64:
		vec.Int64Data[row] = 1
	case parquet.TypeFloat64:
		vec.Float64Data[row] = 1
	case parquet.TypeString:
		vec.BytesData.Set(row, []byte("true"))
	}
}

func writeBoolFalse(vec *batch.Vector, row int, colType parquet.TypeID) {
	switch colType {
	case parquet.TypeBool:
		vec.BoolData[row] = false
	case parquet.TypeInt64:
		vec.Int64Data[row] = 0
	case parquet.TypeFloat64:
		vec.Float64Data[row] = 0
	case parquet.TypeString:
		vec.BytesData.Set(row, []byte("false"))
	}
}

// ---------------------------------------------------------------------------
// Token-based schema inference (uses encoding/json for correctness)
// ---------------------------------------------------------------------------

func inferSchemaTokens(data []byte, isArray bool, sampleSize int) ([]parquet.Column, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	if isArray {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if d, ok := tok.(json.Delim); !ok || d != '[' {
			return nil, fmt.Errorf("expected '[', got %v", tok)
		}
	}

	var colOrder []string
	colSet := make(map[string]struct{})
	colTypes := make(map[string]parquet.TypeID)
	nestedSchemas := make(map[string]parquet.Column) // full column defs for nested types
	count := 0

	for count < sampleSize && dec.More() {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if d, ok := tok.(json.Delim); !ok || d != '{' {
			break
		}

		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				break
			}
			key, ok := keyTok.(string)
			if !ok {
				break
			}

			if _, exists := colSet[key]; !exists {
				colOrder = append(colOrder, key)
				colSet[key] = struct{}{}
			}

			valTok, err := dec.Token()
			if err != nil {
				break
			}

			if d, ok := valTok.(json.Delim); ok && (d == '{' || d == '[') {
				nestedType := inferNestedType(dec, d)
				if prev, has := colTypes[key]; has {
					if prev != nestedType.Type {
						colTypes[key] = parquet.TypeString // conflicting types fall back to string
					}
				} else {
					colTypes[key] = nestedType.Type
				}
				if _, has := nestedSchemas[key]; !has {
					nestedSchemas[key] = nestedType
				}
				continue
			}

			if valTok == nil {
				continue
			}

			observed := detectTokenType(valTok)
			if prev, has := colTypes[key]; has {
				colTypes[key] = promoteType(prev, observed)
			} else {
				colTypes[key] = observed
			}
		}

		dec.Token() // closing '}'
		count++
	}

	schema := make([]parquet.Column, len(colOrder))
	for i, name := range colOrder {
		if nested, ok := nestedSchemas[name]; ok && (colTypes[name] == parquet.TypeArray || colTypes[name] == parquet.TypeRow || colTypes[name] == parquet.TypeMap) {
			nested.Name = name
			nested.Nullable = true
			schema[i] = nested
		} else {
			typ, ok := colTypes[name]
			if !ok {
				typ = parquet.TypeString
			}
			schema[i] = parquet.Column{
				Name:     name,
				Type:     typ,
				Nullable: true,
			}
		}
	}
	return schema, nil
}

func detectTokenType(tok json.Token) parquet.TypeID {
	switch v := tok.(type) {
	case bool:
		return parquet.TypeBool
	case json.Number:
		s := v.String()
		if !strings.Contains(s, ".") && !strings.Contains(s, "e") && !strings.Contains(s, "E") {
			if _, err := strconv.ParseInt(s, 10, 64); err == nil {
				return parquet.TypeInt64
			}
		}
		// A number float8 cannot hold (1e999, a nonzero 1e-400) infers
		// text, the one type that holds it: typed double precision it read
		// 0 inside the sample and was 22003 past it (checkNumber).
		if _, st := kernel.FloatLitText(s, 64); st != kernel.NumConstOK {
			return parquet.TypeString
		}
		return parquet.TypeFloat64
	case string:
		return detectStringType(v)
	default:
		return parquet.TypeString
	}
}

// inferNestedType determines the schema for a nested JSON value.
// The opening delimiter ('{' or '[') has already been consumed by Token().
func inferNestedType(dec *json.Decoder, delim json.Delim) parquet.Column {
	if delim == '[' {
		return inferArrayType(dec)
	}
	return inferObjectType(dec)
}

// inferArrayType infers ARRAY element type by sampling array elements.
func inferArrayType(dec *json.Decoder) parquet.Column {
	var elemType parquet.TypeID
	var elemCol *parquet.Column
	first := true

	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		if d, ok := tok.(json.Delim); ok {
			nested := inferNestedType(dec, d)
			if first {
				elemType = nested.Type
				elemCol = &nested
				first = false
			}
			continue
		}
		if tok == nil {
			continue
		}
		observed := detectTokenType(tok)
		if first {
			elemType = observed
			first = false
		} else {
			elemType = promoteType(elemType, observed)
		}
	}
	// Consume closing ']'
	dec.Token()

	if first {
		// Empty array
		elemType = parquet.TypeString
	}

	elem := parquet.Column{Name: "element", Type: elemType, Nullable: true}
	if elemCol != nil && (elemType == parquet.TypeRow || elemType == parquet.TypeArray) {
		elemCol.Name = "element"
		elemCol.Nullable = true
		elem = *elemCol
	}
	return parquet.Column{
		Type:        parquet.TypeArray,
		ElementType: &elem,
	}
}

// inferObjectType infers ROW field types from a JSON object.
func inferObjectType(dec *json.Decoder) parquet.Column {
	var fieldOrder []string
	fieldSet := make(map[string]struct{})
	fieldTypes := make(map[string]parquet.TypeID)
	fieldNested := make(map[string]*parquet.Column)

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			break
		}
		key, ok := keyTok.(string)
		if !ok {
			break
		}
		if _, exists := fieldSet[key]; !exists {
			fieldOrder = append(fieldOrder, key)
			fieldSet[key] = struct{}{}
		}

		valTok, err := dec.Token()
		if err != nil {
			break
		}
		if d, ok := valTok.(json.Delim); ok {
			nested := inferNestedType(dec, d)
			fieldTypes[key] = nested.Type
			fieldNested[key] = &nested
			continue
		}
		if valTok == nil {
			continue
		}
		fieldTypes[key] = detectTokenType(valTok)
	}
	// Consume closing '}'
	dec.Token()

	fields := make([]parquet.Column, len(fieldOrder))
	for i, name := range fieldOrder {
		if nc, ok := fieldNested[name]; ok {
			nc.Name = name
			nc.Nullable = true
			fields[i] = *nc
		} else {
			typ := fieldTypes[name]
			if typ == 0 {
				typ = parquet.TypeString
			}
			fields[i] = parquet.Column{Name: name, Type: typ, Nullable: true}
		}
	}

	return parquet.Column{
		Type:   parquet.TypeRow,
		Fields: fields,
	}
}

func drainNestedStatic(dec *json.Decoder) {
	depth := 1
	for depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			return
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
}
