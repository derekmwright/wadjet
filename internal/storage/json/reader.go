// Package json reads JSON data (JSONL or JSON array format) and converts it
// to the columnar RecordBatch format used by Wadjet's execution engine.
package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// defaultSampleSize is the number of rows sampled for schema inference.
const defaultSampleSize = 100

// defaultBatchSize is the number of rows per RecordBatch.
const defaultBatchSize = batch.DefaultBatchSize

// timestampPatterns are common timestamp formats tried during type detection.
var timestampPatterns = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// ipv4Re matches dotted-quad IPv4 addresses.
var ipv4Re = regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)

// Reader reads JSON data and produces RecordBatches.
type Reader struct {
	schema []parquet.Column
	rows   []map[string]any
	offset int
}

// NewReaderFromBytes parses JSON data from a byte slice, auto-detecting
// whether the input is JSONL (newline-delimited) or a JSON array.
func NewReaderFromBytes(data []byte) (*Reader, error) {
	rows, err := parseJSON(data)
	if err != nil {
		return nil, err
	}
	schema := inferSchema(rows, defaultSampleSize)
	return &Reader{
		schema: schema,
		rows:   rows,
	}, nil
}

// NewReaderFromStream reads all data from r and parses it as JSON.
func NewReaderFromStream(r io.Reader) (*Reader, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading json stream: %w", err)
	}
	return NewReaderFromBytes(data)
}

// Schema returns the inferred schema.
func (r *Reader) Schema() []parquet.Column {
	return r.schema
}

// Next returns the next batch of rows as a RecordBatch. Returns nil when all
// rows have been consumed.
func (r *Reader) Next() (*batch.RecordBatch, error) {
	if r.offset >= len(r.rows) {
		return nil, nil
	}
	end := r.offset + defaultBatchSize
	if end > len(r.rows) {
		end = len(r.rows)
	}
	chunk := r.rows[r.offset:end]
	r.offset = end
	return batch.FromRows(r.schema, chunk), nil
}

// parseJSON auto-detects JSONL vs JSON array and returns parsed rows.
func parseJSON(data []byte) ([]map[string]any, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	// JSON array: starts with '['
	if trimmed[0] == '[' {
		return parseJSONArray(trimmed)
	}

	// Otherwise treat as JSONL (newline-delimited JSON objects).
	return parseJSONL(trimmed)
}

// parseJSONArray decodes a top-level JSON array of objects.
func parseJSONArray(data []byte) ([]map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	// Read opening bracket.
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("json array: reading opening bracket: %w", err)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		return nil, fmt.Errorf("json array: expected '[', got %v", tok)
	}

	var rows []map[string]any
	for dec.More() {
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			return nil, fmt.Errorf("json array: decoding object at index %d: %w", len(rows), err)
		}
		rows = append(rows, convertNumbers(obj))
	}
	return rows, nil
}

// parseJSONL decodes newline-delimited JSON objects.
func parseJSONL(data []byte) ([]map[string]any, error) {
	var rows []map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	for dec.More() {
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			return nil, fmt.Errorf("jsonl: decoding object at line %d: %w", len(rows)+1, err)
		}
		rows = append(rows, convertNumbers(obj))
	}
	return rows, nil
}

// convertNumbers walks a decoded JSON map and converts json.Number values
// to int64 or float64.
func convertNumbers(m map[string]any) map[string]any {
	for k, v := range m {
		switch val := v.(type) {
		case json.Number:
			if i, err := val.Int64(); err == nil {
				// Check that the number round-trips as an integer.
				if !strings.Contains(val.String(), ".") && !strings.Contains(val.String(), "e") && !strings.Contains(val.String(), "E") {
					m[k] = i
					continue
				}
			}
			if f, err := val.Float64(); err == nil {
				m[k] = f
			}
		}
	}
	return m
}

// inferSchema samples up to sampleSize rows and infers column types. Column
// order is determined by the first occurrence of each key across sampled rows.
func inferSchema(rows []map[string]any, sampleSize int) []parquet.Column {
	if len(rows) == 0 {
		return nil
	}
	limit := sampleSize
	if limit > len(rows) {
		limit = len(rows)
	}

	// Track column order by first appearance.
	var colOrder []string
	colSet := make(map[string]struct{})

	// Collect observed types per column.
	colTypes := make(map[string]parquet.TypeID)

	for i := 0; i < limit; i++ {
		for k, v := range rows[i] {
			if _, exists := colSet[k]; !exists {
				colOrder = append(colOrder, k)
				colSet[k] = struct{}{}
			}
			if v == nil {
				continue
			}
			observed := detectType(v)
			if prev, hasPrev := colTypes[k]; hasPrev {
				colTypes[k] = promoteType(prev, observed)
			} else {
				colTypes[k] = observed
			}
		}
	}

	schema := make([]parquet.Column, len(colOrder))
	for i, name := range colOrder {
		typ, ok := colTypes[name]
		if !ok {
			typ = parquet.TypeString // all-null columns default to string
		}
		schema[i] = parquet.Column{
			Name:     name,
			Type:     typ,
			Nullable: true,
		}
	}
	return schema
}

// detectType determines the Wadjet type for a single Go value.
func detectType(v any) parquet.TypeID {
	switch val := v.(type) {
	case bool:
		return parquet.TypeBool
	case int64:
		return parquet.TypeInt64
	case float64:
		return parquet.TypeFloat64
	case string:
		return detectStringType(val)
	default:
		return parquet.TypeString
	}
}

// detectStringType attempts to recognize well-known formats in a string value.
func detectStringType(s string) parquet.TypeID {
	// Try IPv4.
	if ipv4Re.MatchString(s) {
		if ip := net.ParseIP(s); ip != nil && ip.To4() != nil {
			return parquet.TypeIPv4
		}
	}

	// Try timestamp formats.
	for _, layout := range timestampPatterns {
		if _, err := time.Parse(layout, s); err == nil {
			return parquet.TypeTimestamp
		}
	}

	return parquet.TypeString
}

// promoteType returns the wider of two types when they conflict.
func promoteType(a, b parquet.TypeID) parquet.TypeID {
	if a == b {
		return a
	}

	// Numeric promotions.
	if isNumeric(a) && isNumeric(b) {
		if a == parquet.TypeFloat64 || b == parquet.TypeFloat64 {
			return parquet.TypeFloat64
		}
		// int64 + float32 -> float64, int32 + int64 -> int64, etc.
		if isFloat(a) || isFloat(b) {
			return parquet.TypeFloat64
		}
		// Both integer: promote to larger.
		if a == parquet.TypeInt64 || b == parquet.TypeInt64 {
			return parquet.TypeInt64
		}
		return parquet.TypeInt32
	}

	// If both are string-like enriched types (IPv4, Timestamp), but disagree,
	// fall back to string.
	if isStringLike(a) && isStringLike(b) {
		return parquet.TypeString
	}

	// Bool + numeric -> coerce to wider numeric.
	if a == parquet.TypeBool && isNumeric(b) {
		return b
	}
	if b == parquet.TypeBool && isNumeric(a) {
		return a
	}

	// Anything else: fall back to string.
	return parquet.TypeString
}

// isNumeric returns true for integer and floating point types.
func isNumeric(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64:
		return true
	}
	return false
}

// isFloat returns true for floating-point types.
func isFloat(t parquet.TypeID) bool {
	return t == parquet.TypeFloat32 || t == parquet.TypeFloat64
}

// isStringLike returns true for types that are detected from string values.
func isStringLike(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeString, parquet.TypeIPv4, parquet.TypeTimestamp:
		return true
	}
	return false
}

// coerceValue converts a raw JSON value to the expected type for a column.
// This ensures that, for example, an int64 is promoted to float64 when the
// inferred schema says the column is float64.
func coerceValue(v any, target parquet.TypeID) any {
	if v == nil {
		return nil
	}
	switch target {
	case parquet.TypeFloat64:
		switch val := v.(type) {
		case int64:
			return float64(val)
		case float64:
			return val
		case bool:
			if val {
				return float64(1)
			}
			return float64(0)
		case string:
			return val
		}
	case parquet.TypeInt64:
		switch val := v.(type) {
		case int64:
			return val
		case float64:
			if val == math.Trunc(val) {
				return int64(val)
			}
			return val
		case bool:
			if val {
				return int64(1)
			}
			return int64(0)
		}
	case parquet.TypeString:
		switch val := v.(type) {
		case string:
			return val
		default:
			return fmt.Sprintf("%v", val)
		}
	}
	return v
}

// coerceRows converts raw values in each row to match the inferred schema,
// handling type promotions (e.g., int64 -> float64 when the column was
// inferred as float64).
func coerceRows(rows []map[string]any, schema []parquet.Column) []map[string]any {
	for i := range rows {
		for _, col := range schema {
			if v, ok := rows[i][col.Name]; ok {
				rows[i][col.Name] = coerceValue(v, col.Type)
			}
		}
	}
	return rows
}

// NewReaderFromBytesWithCoercion parses JSON data and coerces values to match
// the inferred schema. This is the recommended entry point when mixed-type
// columns are expected.
func NewReaderFromBytesWithCoercion(data []byte) (*Reader, error) {
	rows, err := parseJSON(data)
	if err != nil {
		return nil, err
	}
	schema := inferSchema(rows, defaultSampleSize)
	rows = coerceRows(rows, schema)
	return &Reader{
		schema: schema,
		rows:   rows,
	}, nil
}
