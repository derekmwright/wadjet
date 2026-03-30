package parquet

import (
	"bytes"
	"testing"
)

func TestReadRows_WithColumnProjection(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat64},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5},
		{"id": int64(2), "name": "bob", "score": 87.0},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Read only "id" and "name"
	result, err := reader.ReadRows([]string{"id", "name"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}

	// Projected columns should be present
	for _, row := range result {
		if _, ok := row["id"]; !ok {
			t.Error("expected 'id' in projected result")
		}
		if _, ok := row["name"]; !ok {
			t.Error("expected 'name' in projected result")
		}
	}
}

func TestReadRows_ProjectionNonexistentColumn(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Request columns that don't exist -- buildProjectedSchema returns nil,
	// so ReadRows should fall back to reading all columns
	result, err := reader.ReadRows([]string{"nonexistent"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
}

func TestNumRowGroups(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
		},
	}

	rows := []map[string]any{
		{"id": int64(1)},
		{"id": int64(2)},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	if reader.NumRowGroups() < 1 {
		t.Fatal("expected at least 1 row group")
	}
}

func TestFile(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
		},
	}

	rows := []map[string]any{{"id": int64(1)}}
	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	if reader.FileReader() == nil {
		t.Fatal("expected non-nil FileReader()")
	}
}

func TestRowGroupStats(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
		},
	}

	rows := []map[string]any{
		{"id": int64(10), "name": "alice"},
		{"id": int64(20), "name": "bob"},
		{"id": int64(30), "name": "carol"},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	stats := reader.RowGroupStats(0)
	if stats.NumRows != 3 {
		t.Fatalf("expected 3 rows in stats, got %d", stats.NumRows)
	}

	if len(stats.Columns) == 0 {
		t.Fatal("expected column stats")
	}

	// Check that id column has stats
	if idStats, ok := stats.Columns["id"]; ok {
		if !idStats.HasStats {
			t.Error("expected id column to have stats")
		}
		// Min should be 10
		if idStats.MinValue != nil {
			if minVal, ok := idStats.MinValue.(int64); ok {
				if minVal != 10 {
					t.Errorf("min = %d, want 10", minVal)
				}
			}
		}
		// Max should be 30
		if idStats.MaxValue != nil {
			if maxVal, ok := idStats.MaxValue.(int64); ok {
				if maxVal != 30 {
					t.Errorf("max = %d, want 30", maxVal)
				}
			}
		}
	}
}

func TestRowGroupStats_OutOfBounds(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
		},
	}

	rows := []map[string]any{{"id": int64(1)}}
	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Negative index
	stats := reader.RowGroupStats(-1)
	if stats.NumRows != 0 {
		t.Fatalf("expected empty stats for index -1, got %d rows", stats.NumRows)
	}

	// Out of range
	stats = reader.RowGroupStats(999)
	if stats.NumRows != 0 {
		t.Fatalf("expected empty stats for index 999, got %d rows", stats.NumRows)
	}
}

func TestReadRowGroup(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice"},
		{"id": int64(2), "name": "bob"},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	result, err := reader.ReadRowGroup(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
}

func TestReadRowGroup_WithProjection(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeString},
			{Name: "score", Type: TypeFloat64},
		},
	}

	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "score": 95.5},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	result, err := reader.ReadRowGroup(0, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
}

func TestReadRowGroup_OutOfBounds(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
		},
	}

	rows := []map[string]any{{"id": int64(1)}}
	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.ReadRowGroup(-1, nil)
	if err == nil {
		t.Fatal("expected error for negative index")
	}

	_, err = reader.ReadRowGroup(999, nil)
	if err == nil {
		t.Fatal("expected error for out-of-range index")
	}
}

func TestNewReader_InvalidData(t *testing.T) {
	_, err := NewReader(bytes.NewReader([]byte("not parquet")), 11)
	if err == nil {
		t.Fatal("expected error for invalid parquet data")
	}
}

func TestCompareNative(t *testing.T) {
	// int64 comparisons
	if CompareNative(int64(1), int64(2)) >= 0 {
		t.Error("1 < 2 should be negative")
	}
	if CompareNative(int64(2), int64(1)) <= 0 {
		t.Error("2 > 1 should be positive")
	}
	if CompareNative(int64(5), int64(5)) != 0 {
		t.Error("5 == 5 should be zero")
	}

	// float64 comparisons
	if CompareNative(float64(1.0), float64(2.0)) >= 0 {
		t.Error("1.0 < 2.0 should be negative")
	}
	if CompareNative(float64(2.0), float64(1.0)) <= 0 {
		t.Error("2.0 > 1.0 should be positive")
	}
	if CompareNative(float64(3.14), float64(3.14)) != 0 {
		t.Error("3.14 == 3.14 should be zero")
	}

	// string comparisons
	if CompareNative("abc", "xyz") >= 0 {
		t.Error("abc < xyz should be negative")
	}
	if CompareNative("xyz", "abc") <= 0 {
		t.Error("xyz > abc should be positive")
	}
	if CompareNative("same", "same") != 0 {
		t.Error("same == same should be zero")
	}

	// Mismatched types return 0
	if CompareNative(int64(1), "string") != 0 {
		t.Error("mismatched types should return 0")
	}
	if CompareNative(int64(1), float64(1.0)) != 0 {
		t.Error("int64 vs float64 should return 0")
	}

	// Unsupported type returns 0
	if CompareNative(true, true) != 0 {
		t.Error("unsupported type should return 0")
	}
}

func TestParquetValueToNative(t *testing.T) {
	// Test via round-trip stats since parquetValueToNative is called by RowGroupStats
	schema := Schema{
		Columns: []Column{
			{Name: "f", Type: TypeFloat64},
			{Name: "s", Type: TypeString},
			{Name: "b", Type: TypeBool, Nullable: true},
		},
	}

	rows := []map[string]any{
		{"f": float64(1.5), "s": "alpha", "b": true},
		{"f": float64(3.5), "s": "zeta", "b": false},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	stats := reader.RowGroupStats(0)
	if stats.NumRows != 2 {
		t.Fatalf("expected 2 rows, got %d", stats.NumRows)
	}

	// Check float stats
	if fStats, ok := stats.Columns["f"]; ok && fStats.HasStats {
		if fStats.MinValue != nil {
			if min, ok := fStats.MinValue.(float64); ok {
				if min != 1.5 {
					t.Errorf("f min = %f, want 1.5", min)
				}
			}
		}
	}

	// Check string stats
	if sStats, ok := stats.Columns["s"]; ok && sStats.HasStats {
		if sStats.MinValue != nil {
			if min, ok := sStats.MinValue.(string); ok {
				if min != "alpha" {
					t.Errorf("s min = %q, want alpha", min)
				}
			}
		}
	}
}

func TestWriterConfig_ZeroRowGroupSize(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
		},
	}

	cfg := WriterConfig{RowGroupSize: 0} // should get set to default
	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows([]map[string]any{{"id": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompressionString(t *testing.T) {
	tests := []struct {
		comp Compression
		want string
	}{
		{CompressionSnappy, "snappy"},
		{CompressionZstd, "zstd"},
		{CompressionGzip, "gzip"},
		{CompressionLZ4, "lz4"},
		{CompressionNone, "none"},
		{Compression(99), "unknown(99)"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.comp.String(); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseUUIDForWrite(t *testing.T) {
	// Valid UUID
	raw := parseUUIDForWrite("550e8400-e29b-41d4-a716-446655440000")
	if raw == nil {
		t.Fatal("expected non-nil result for valid UUID")
	}
	if len(raw) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(raw))
	}

	// Invalid: too short
	raw = parseUUIDForWrite("550e8400")
	if raw != nil {
		t.Fatal("expected nil for short UUID")
	}

	// Invalid: non-hex chars
	raw = parseUUIDForWrite("GGGGGGGG-GGGG-GGGG-GGGG-GGGGGGGGGGGG")
	if raw != nil {
		t.Fatal("expected nil for non-hex UUID")
	}

	// Without dashes
	raw = parseUUIDForWrite("550e8400e29b41d4a716446655440000")
	if raw == nil {
		t.Fatal("expected non-nil for UUID without dashes")
	}
}

func TestParseDateForWrite(t *testing.T) {
	// Epoch itself
	d := parseDateForWrite("1970-01-01")
	if d != 0 {
		t.Fatalf("epoch should be day 0, got %d", d)
	}

	// Some date after epoch
	d = parseDateForWrite("2026-03-15")
	if d <= 0 {
		t.Fatalf("2026-03-15 should be positive days since epoch, got %d", d)
	}

	// Invalid date
	d = parseDateForWrite("not-a-date")
	if d != 0 {
		t.Fatalf("invalid date should return 0, got %d", d)
	}
}

func TestUnhex(t *testing.T) {
	// Digits
	for i := byte('0'); i <= '9'; i++ {
		v := unhex(i)
		if v != i-'0' {
			t.Errorf("unhex(%c) = %d, want %d", i, v, i-'0')
		}
	}

	// Lowercase a-f
	for i := byte('a'); i <= 'f'; i++ {
		v := unhex(i)
		if v != i-'a'+10 {
			t.Errorf("unhex(%c) = %d, want %d", i, v, i-'a'+10)
		}
	}

	// Uppercase A-F
	for i := byte('A'); i <= 'F'; i++ {
		v := unhex(i)
		if v != i-'A'+10 {
			t.Errorf("unhex(%c) = %d, want %d", i, v, i-'A'+10)
		}
	}

	// Invalid
	if unhex('g') != 0xFF {
		t.Error("expected 0xFF for invalid hex char 'g'")
	}
	if unhex('!') != 0xFF {
		t.Error("expected 0xFF for invalid hex char '!'")
	}
}

func TestPrepareRows_TypeConversions(t *testing.T) {
	// Port/Protocol: int, int64, float64 -> int32
	schema := Schema{
		Columns: []Column{
			{Name: "port", Type: TypePort},
			{Name: "proto", Type: TypeProtocol},
			{Name: "dur", Type: TypeDuration},
		},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"port": int(443), "proto": int64(6), "dur": int(1500000)},
		{"port": float64(80), "proto": int(17), "dur": int32(250000)},
		{"port": int64(8080), "proto": float64(6), "dur": float64(5000)},
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
}

func TestPrepareRows_NilAndMissingValues(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "ip", Type: TypeIPv4, Nullable: true},
			{Name: "uuid", Type: TypeUUID, Nullable: true},
			{Name: "date", Type: TypeDate, Nullable: true},
		},
	}

	var buf bytes.Buffer
	pw, err := NewWriter(&buf, schema, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}

	rows := []map[string]any{
		{"ip": nil, "uuid": nil, "date": nil},
	}

	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRoundTrip_BoolType(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "flag", Type: TypeBool},
		},
	}

	rows := []map[string]any{
		{"flag": true},
		{"flag": false},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
}

func TestRoundTrip_Int32Type(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "val", Type: TypeInt32},
		},
	}

	rows := []map[string]any{
		{"val": int32(42)},
		{"val": int32(-100)},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
}

func TestRoundTrip_Float32Type(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "val", Type: TypeFloat32},
		},
	}

	rows := []map[string]any{
		{"val": float32(3.14)},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
}

func TestRoundTrip_BytesType(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "data", Type: TypeBytes},
		},
	}

	rows := []map[string]any{
		{"data": []byte{0xCA, 0xFE}},
	}

	data := writeParquet(t, schema, rows)
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.ReadRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
}

// writeParquet is a test helper that writes rows to parquet and returns the bytes.
func writeParquet(t *testing.T, schema Schema, rows []map[string]any) []byte {
	t.Helper()
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
	return buf.Bytes()
}
