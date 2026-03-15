package parquet

import (
	"bytes"
	"fmt"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat64, Nullable: true},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5},
		{"id": int64(2), "name": "bob", "score": nil},
		{"id": int64(3), "name": "carol", "score": 78.0},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	if reader.NumRows() != 3 {
		t.Fatalf("expected 3 rows, got %d", reader.NumRows())
	}

	readBack, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(readBack) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(readBack))
	}

	s := reader.Schema()
	t.Logf("Schema: %v", s.ColumnNames())
	for _, row := range readBack {
		t.Logf("  %v", row)
	}
}

func TestCompression(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "data", Type: TypeString},
			{Name: "category", Type: TypeString},
			{Name: "value", Type: TypeFloat64},
		},
	}

	// Generate 10K rows of semi-repetitive data (realistic for log/event data)
	const numRows = 10_000
	rows := make([]map[string]any, numRows)
	categories := []string{"purchase", "refund", "view", "click", "impression"}
	for i := range rows {
		rows[i] = map[string]any{
			"id":       int64(i),
			"data":     fmt.Sprintf("user-%04d performed action at 2026-03-15T%02d:%02d:%02d", i%500, i%24, i%60, i%60),
			"category": categories[i%len(categories)],
			"value":    float64(i%10000) / 100.0,
		}
	}

	codecs := []struct {
		name string
		comp Compression
	}{
		{"none", CompressionNone},
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
		{"gzip", CompressionGzip},
		{"lz4", CompressionLZ4},
	}

	pageSizes := []int{
		4 * 1024,   // 4 KB
		16 * 1024,  // 16 KB
		64 * 1024,  // 64 KB
		256 * 1024, // 256 KB (default)
		1024 * 1024, // 1 MB
		4 * 1024 * 1024, // 4 MB
	}

	// results[pageSize][codec] = file size
	type key struct {
		pageSize int
		codec    string
	}
	results := make(map[key]int)

	for _, pageSize := range pageSizes {
		for _, codec := range codecs {
			name := fmt.Sprintf("page=%dKB/%s", pageSize/1024, codec.name)
			t.Run(name, func(t *testing.T) {
				var buf bytes.Buffer
				cfg := WriterConfig{
					RowGroupSize:   numRows, // single row group to isolate page size effect
					PageBufferSize: pageSize,
					Compression:    codec.comp,
				}
				pw, err := NewWriter(&buf, schema, cfg)
				if err != nil {
					t.Fatal(err)
				}
				if err := pw.WriteRows(rows); err != nil {
					t.Fatal(err)
				}
				if err := pw.Close(); err != nil {
					t.Fatal(err)
				}

				data := buf.Bytes()
				results[key{pageSize, codec.name}] = len(data)

				// Verify round-trip
				reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				if reader.NumRows() != numRows {
					t.Fatalf("expected %d rows, got %d", numRows, reader.NumRows())
				}
				readBack, err := reader.ReadRows(nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(readBack) != numRows {
					t.Fatalf("expected %d rows back, got %d", numRows, len(readBack))
				}
			})
		}
	}

	// Print results table
	t.Logf("")
	t.Logf("Compression × Page Size (%d rows, 4 columns: int64 + 2×string + float64)", numRows)
	t.Logf("")

	// Header
	header := fmt.Sprintf("%-12s", "Page Size")
	for _, codec := range codecs {
		header += fmt.Sprintf(" %14s", codec.name)
	}
	t.Log(header)
	t.Logf("%-12s %14s %14s %14s %14s %14s", "--------", "-----------", "-----------", "-----------", "-----------", "-----------")

	for _, pageSize := range pageSizes {
		label := fmt.Sprintf("%d KB", pageSize/1024)
		line := fmt.Sprintf("%-12s", label)
		noneSize := results[key{pageSize, "none"}]
		for _, codec := range codecs {
			size := results[key{pageSize, codec.name}]
			ratio := float64(noneSize) / float64(size)
			line += fmt.Sprintf(" %7d %5.1fx", size, ratio)
		}
		t.Log(line)
	}
}

func TestParseCompression(t *testing.T) {
	tests := []struct {
		input string
		want  Compression
		err   bool
	}{
		{"snappy", CompressionSnappy, false},
		{"ZSTD", CompressionZstd, false},
		{"Gzip", CompressionGzip, false},
		{"lz4", CompressionLZ4, false},
		{"none", CompressionNone, false},
		{"uncompressed", CompressionNone, false},
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseCompression(tt.input)
			if tt.err {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}
