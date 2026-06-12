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

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
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
func parseColumnarDirect(data []byte, isArray bool, schema []parquet.Column, colIdx map[string]int) ([]*batch.RecordBatch, error) {
	sc := &jsonScanner{data: data}

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

			if err := scanObjectInto(sc, rb, row, schema, colIdx, seen); err != nil {
				return nil, fmt.Errorf("row %d: %w", row, err)
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
			writeStringValue(sc, vec, row, colType)

		case valByte == 't': // true
			sc.pos += 4
			vec.Nulls.SetValid(row)
			writeBoolTrue(vec, row, colType)

		case valByte == 'f': // false
			sc.pos += 5
			vec.Nulls.SetValid(row)
			writeBoolFalse(vec, row, colType)

		case valByte == '{' || valByte == '[': // nested object or array
			if colType == parquet.TypeArray || colType == parquet.TypeRow || colType == parquet.TypeMap {
				raw := sc.readRawValue()
				var decoded any
				if err := json.Unmarshal(raw, &decoded); err != nil {
					vec.WriteNullAt(row)
				} else {
					vec.Nulls.SetValid(row)
					vec.SetValue(row, decoded)
				}
			} else {
				// Schema says scalar but JSON has nested — store as JSON string
				raw := sc.readRawValue()
				vec.Nulls.SetValid(row)
				vec.BytesData.Set(row, raw)
			}

		default: // number
			numBytes := sc.readNumber()
			vec.Nulls.SetValid(row)
			writeNumberValue(vec, row, colType, numBytes)
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

func writeStringValue(sc *jsonScanner, vec *batch.Vector, row int, colType parquet.TypeID) {
	vec.Nulls.SetValid(row)

	switch colType {
	case parquet.TypeString:
		raw, hasEscape, err := sc.readStringBytes()
		if err != nil {
			return
		}
		if hasEscape {
			var unescaped string
			json.Unmarshal(append([]byte{'"'}, append(raw, '"')...), &unescaped)
			vec.BytesData.Set(row, []byte(unescaped))
		} else {
			vec.BytesData.Set(row, raw)
		}

	case parquet.TypeIPv4:
		str, err := sc.readString()
		if err != nil {
			return
		}
		if ip := net.ParseIP(str); ip != nil {
			if ip4 := ip.To4(); ip4 != nil {
				vec.Int64Data[row] = int64(binary.BigEndian.Uint32(ip4))
			}
		}

	case parquet.TypeTimestamp:
		str, err := sc.readString()
		if err != nil {
			return
		}
		for _, layout := range timestampPatterns {
			if t, err := time.Parse(layout, str); err == nil {
				vec.Int64Data[row] = t.UnixMicro()
				break
			}
		}

	default:
		raw, hasEscape, err := sc.readStringBytes()
		if err != nil {
			return
		}
		if hasEscape {
			var unescaped string
			json.Unmarshal(append([]byte{'"'}, append(raw, '"')...), &unescaped)
			vec.BytesData.Set(row, []byte(unescaped))
		} else {
			vec.BytesData.Set(row, raw)
		}
	}
}

func writeNumberValue(vec *batch.Vector, row int, colType parquet.TypeID, numBytes []byte) {
	switch colType {
	case parquet.TypeInt64:
		if i, err := strconv.ParseInt(unsafe.String(&numBytes[0], len(numBytes)), 10, 64); err == nil {
			vec.Int64Data[row] = i
		}
	case parquet.TypeFloat64:
		if f, err := strconv.ParseFloat(unsafe.String(&numBytes[0], len(numBytes)), 64); err == nil {
			vec.Float64Data[row] = f
		}
	case parquet.TypeString:
		vec.BytesData.Set(row, numBytes)
	}
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
